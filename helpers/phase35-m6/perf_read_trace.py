"""Join decoded perf positional reads and switch edges by thread."""

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import threading


ENTER = 'syscalls:sys_enter_pread64:'
EXIT = 'syscalls:sys_exit_pread64:'
NFS = 'nfs:nfs_file_read:'
SWITCH = 'PERF_RECORD_SWITCH'
RECORD = re.compile(r'^\s*.*?\s*(\d+)/(\d+)\s+\[(-01|\d+)\]\s+(\d+)\.(\d{9}):\s+(.*)$')
ARGUMENTS = re.compile(r'fd: 0x[0-9a-f]+, buf: 0x[0-9a-f]+, count: 0x([0-9a-f]+), pos: 0x([0-9a-f]+)')
FILE = re.compile(r'fileid=([0-9a-f]+):([0-9a-f]+):(\d+) .*?\boffset=(\d+) count=(\d+)\b')
RETURNED = re.compile(r'0x[0-9a-f]+')
PERF_TRACE_MAX_BYTES = 256 << 20


@dataclass
class PendingRead:
    start: int
    count: int
    offset: int
    last: int
    matched: bool = False
    saw_nfs: bool = False
    switched_out: int | None = None
    off_cpu: int = 0
    preempted: bool = False
    preempted_off_cpu: int = 0


def analyze_reads(lines, pid, device, inodes, *, split_preempt=False, positional_only=False, on_read=None):
    """Measure only reads joined to caller-approved NFS file identities."""
    if pid <= 1 or not inodes or any(inode <= 0 for inode in inodes):
        raise ValueError('trace requires a target PID and nonempty inode allowlist')
    if positional_only and split_preempt:
        raise ValueError('positional-only replay cannot attribute preemption')
    active = {}
    result = dict(syscalls=0, matched_reads=0, matched_bytes=0, syscall_ns=0, off_cpu_ns=0)
    preempted_off_cpu = 0
    for line in lines:
        if 'PERF_RECORD_LOST' in line:
            raise ValueError('perf reported lost records')
        if not any(marker in line for marker in (ENTER, EXIT, NFS, SWITCH)):
            continue
        record = RECORD.fullmatch(line.rstrip())
        if record is None:
            raise ValueError('unrecognized perf event format')
        process, thread, cpu, seconds, nanos, event = record.groups()
        if positional_only and event.startswith(SWITCH):
            continue
        if cpu == '-01' and not event.startswith(SWITCH):
            raise ValueError('missing CPU on traced syscall or NFS event')
        if int(process) != pid:
            raise ValueError('perf event is outside the target process')
        timestamp = int(seconds) * 1_000_000_000 + int(nanos)
        pending = active.get(thread)
        if event.startswith(ENTER):
            args = ARGUMENTS.fullmatch(event[len(ENTER):].strip())
            if pending is not None or args is None:
                raise ValueError('invalid or overlapping positional syscall entry')
            count, offset = (int(value, 16) for value in args.groups())
            active[thread] = PendingRead(timestamp, count, offset, timestamp)
            continue
        if pending is None:
            if event.startswith(EXIT):
                raise ValueError('positional syscall exit without entry')
            if event.startswith(NFS):
                major, minor, inode, _, _ = file_identity(event)
                if (major, minor) == device and inode in inodes:
                    raise ValueError('allowlisted NFS event without positional syscall entry')
            continue
        if timestamp < pending.last:
            raise ValueError('nonmonotonic events inside positional syscall')
        pending.last = timestamp
        if event.startswith(NFS):
            major, minor, inode, offset, count = file_identity(event)
            if pending.saw_nfs or pending.switched_out is not None or (offset, count) != (pending.offset, pending.count):
                raise ValueError('NFS event does not match positional syscall')
            pending.saw_nfs = True
            pending.matched = (major, minor) == device and inode in inodes
        elif event.startswith(SWITCH):
            edge = event.split()[1:]
            if edge in (['OUT'], ['OUT', 'preempt']) and pending.switched_out is None:
                pending.switched_out = timestamp
                pending.preempted = edge == ['OUT', 'preempt']
            elif edge == ['IN'] and pending.switched_out is not None:
                interval = timestamp - pending.switched_out
                pending.off_cpu += interval
                if pending.preempted:
                    pending.preempted_off_cpu += interval
                pending.switched_out = None
            else:
                raise ValueError('unpaired scheduler edge inside positional syscall')
        elif event.startswith(EXIT):
            status = event[len(EXIT):].strip()
            if not RETURNED.fullmatch(status) or pending.switched_out is not None:
                raise ValueError('invalid positional syscall exit or missing switch-in')
            result['syscalls'] += 1
            del active[thread]
            if pending.matched:
                returned = int(status, 16)
                if not 0 < returned == pending.count:
                    raise ValueError('allowlisted positional read was not complete')
                result['matched_reads'] += 1
                result['matched_bytes'] += returned
                result['syscall_ns'] += timestamp - pending.start
                result['off_cpu_ns'] += pending.off_cpu
                preempted_off_cpu += pending.preempted_off_cpu
            if on_read is not None:
                on_read(int(thread), pending)
        else:
            raise ValueError('unknown event inside positional syscall')
    if active or not result['matched_reads']:
        raise ValueError('incomplete trace or no allowlisted positional reads')
    if split_preempt:
        result['preempted_off_cpu_ns'] = preempted_off_cpu
        result['unclassified_off_cpu_ns'] = result['off_cpu_ns'] - preempted_off_cpu
    if positional_only:
        del result['off_cpu_ns']
    return result


def file_identity(event):
    fields = FILE.match(event[len(NFS):].strip())
    if fields is None:
        raise ValueError('unrecognized NFS file identity')
    major, minor, inode, offset, count = fields.groups()
    return int(major, 16), int(minor, 16), int(inode), int(offset), int(count)


def require_recording_events(attributes):
    required = (ENTER, EXIT, NFS)
    if not all(any(line.startswith(event + ' ') for line in attributes) for event in required) or not any(
            line.startswith((ENTER + ' ', 'dummy:u: ')) and re.search(r'\bcontext_switch: 1\b', line)
            for line in attributes):
        raise ValueError('recording lacks positional/NFS events or task switch recording')


def analyze_recording(path, pid, files, expected=None, *, split_preempt=False, positional_only=False, on_read=None):
    if positional_only and (expected is None or any(value <= 0 for value in expected)):
        raise ValueError('positional-only replay requires positive same-window reference counts')
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or not 0 < info.st_size < PERF_TRACE_MAX_BYTES:
        raise ValueError('perf recording is not a private bounded file')
    identities = [file.lstat() for file in files]
    devices = {identity.st_dev for identity in identities}
    if len(devices) != 1 or any(not stat.S_ISREG(identity.st_mode) for identity in identities):
        raise ValueError('expected regular fixture files on exactly one device')
    device, = devices
    perf = ['/usr/bin/perf', '--no-pager']
    attributes = subprocess.run(perf + ['evlist', '-v', '-i', str(path)], check=True,
                                stdout=subprocess.PIPE, text=True, timeout=10).stdout.splitlines()
    require_recording_events(attributes)
    command = perf + ['script', '--ns', '--show-lost-events', '--show-switch-events',
                      '-F', 'comm,pid,tid,cpu,time,event,trace', '-i', str(path)]
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as decoder:
        expired = threading.Event()

        def expire():
            expired.set()
            decoder.kill()

        timer = threading.Timer(120, expire)
        timer.start()
        try:
            options = {'split_preempt': split_preempt}
            if positional_only:
                options['positional_only'] = True
            if on_read is not None:
                options['on_read'] = on_read
            result = analyze_reads(bounded_lines(decoder.stdout), pid, (os.major(device), os.minor(device)),
                                   {identity.st_ino for identity in identities}, **options)
            if decoder.wait() != 0:
                raise ValueError('perf decoder did not exit successfully')
        except BaseException:
            decoder.kill()
            if expired.is_set():
                raise TimeoutError('perf decode exceeded 120 seconds') from None
            raise
        finally:
            timer.cancel()
            timer.join()
    if expired.is_set():
        raise TimeoutError('perf decode exceeded 120 seconds')
    if expected is not None and (result['matched_reads'], result['matched_bytes']) != expected:
        raise ValueError('matched reads or bytes differ from the same-window reference')
    return dict(result, exact_reference_match=expected is not None)


def bounded_lines(stream):
    while line := stream.readline(16385):
        if len(line) > 16384:
            raise ValueError('oversized perf decoder line')
        yield line


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('trace', type=Path)
    parser.add_argument('--pid', type=int, required=True)
    parser.add_argument('--file', type=Path, action='append', required=True)
    parser.add_argument('--expected-reads', type=int)
    parser.add_argument('--expected-bytes', type=int)
    parser.add_argument('--split-preempt', action='store_true',
                        help='separate preemption-marked off-CPU time from unclassified time')
    args = parser.parse_args()
    expected = args.expected_reads, args.expected_bytes
    if expected == (None, None):
        expected = None
    elif any(value is None or value <= 0 for value in expected):
        parser.error('both same-window reference counts must be positive')
    print(json.dumps(analyze_recording(args.trace, args.pid, args.file, expected, split_preempt=args.split_preempt),
                     sort_keys=True))