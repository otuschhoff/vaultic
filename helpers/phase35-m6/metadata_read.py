from pathlib import Path
import argparse
import ctypes
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time

import pilot


WORKSPACE = Path('/root/proj/vaultic')
PREVIOUS = Path('/volume2/NASDA2/vaultic-test-m6/force-compare-fme-cae1-20261103-10k')
ACCEPTED = Path('/volume2/NASDA2/vaultic-test-m6/critical-path-fme_cae1-20261102-production-cap100k/group-64')
SEED = Path('/ncl1-1-vs-50/fme_dump/amakura/db.begin-readiness-20261001-fuU56R')
CANDIDATE = WORKSPACE / 'bin/phase35-metadata-read/linux-amd64/vaultic'
NATIVE = CANDIDATE.with_name('vaulticdb')
FLOOR = 57481929687040
FIXTURE_CEILING = 110 << 30
RSS_CEILING = 200000000000
MARKER = b'Disposable Phase 35 throughput qualification.\n'


def paths(mode):
    if mode not in ('parallel', 'coalesced', 'catalog'):
        raise ValueError('unknown metadata-read mode')
    return (Path('/volume2/NASDA2/vaultic-test-m6/metadata-read-fme-cae1-20261103-10k-' + mode),
            Path('/run/vaultic-unchanged-metadata-read-' + mode + '-20261103'))


def private_output(path, text=False):
    output = path.open('x' if text else 'xb')
    os.fchmod(output.fileno(), 0o600)
    return output


def prepare(mode):
    root, runtime = paths(mode)
    if root.parent.resolve() != root.parent or root.exists() or runtime.exists():
        raise ValueError('refuse existing or noncanonical capture')
    if shutil.disk_usage(root.parent).free < FLOOR + FIXTURE_CEILING:
        raise ValueError('fixture and free-space reserve unavailable')
    pilot.private_directory(root)
    pilot.private_directory(runtime)
    with private_output(root / '.nobackup') as output:
        output.write(MARKER)
    with private_output(root / 'source.patch') as output:
        subprocess.run(['git', '-C', str(WORKSPACE), 'diff', '--binary'], stdout=output, check=True)
    print(json.dumps({'root': str(root), 'runtime': str(runtime), 'prepared': True}))


def protect():
    library = ctypes.CDLL(None, use_errno=True)
    for path in (SEED, ACCEPTED, Path('/volume2/NASDA2/rustic'), WORKSPACE, PREVIOUS / 'group-64', PREVIOUS / 'input'):
        encoded = os.fsencode(path)
        if library.mount(encoded, encoded, None, 4096, None) != 0 or library.mount(None, encoded, None, 4096 | 32 | 1, None) != 0:
            raise OSError(ctypes.get_errno(), 'private read-only bind mount failed', str(path))
        if not os.statvfs(path).f_flag & os.ST_RDONLY:
            raise ValueError('protected path remains writable: ' + str(path))


def interrupt(process, group=False):
    if process is None:
        return
    if group:
        _, count = pilot.process_group_rss(process.pid)
        if count == 0:
            return
        os.killpg(process.pid, signal.SIGINT)
        deadline = time.monotonic() + 120
        while pilot.process_group_rss(process.pid)[1] != 0:
            if time.monotonic() >= deadline:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=30)
                raise ValueError('owned group required kill fallback; capture rejected')
            if process.poll() is None:
                try:
                    process.wait(timeout=0.25)
                except subprocess.TimeoutExpired:
                    pass
            else:
                threading.Event().wait(0.05)
        return
    else:
        if process.poll() is not None:
            return
        process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=120 if group else 15)
    except subprocess.TimeoutExpired:
        if group:
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        process.wait(timeout=30)


def storage_bytes(root):
    total = 0
    for directory, _, files in os.walk(root):
        for name in files:
            try:
                total += (Path(directory) / name).lstat().st_size
            except FileNotFoundError:
                pass
    return total


def execute(mode):
    root, runtime = paths(mode)
    for path in (root, runtime):
        if path.resolve() != path or path.stat().st_mode & 0o777 != 0o700:
            raise ValueError('unsafe capture root')
    if (root / '.nobackup').read_bytes() != MARKER:
        raise ValueError('ownership marker differs')
    protect()
    pins = [{'path': str(path), 'sha256': pilot.digest(path)} for path in
            (Path(__file__), root / '.nobackup', root / 'qualifier.test', root / 'source.patch', CANDIDATE, NATIVE,
             ACCEPTED / 'result.json', PREVIOUS / 'group-64/result.json', WORKSPACE / 'cmd/vaultic/working_benchmark_test.go',
             WORKSPACE / 'helpers/phase35-m6/observe.py', Path(pilot.__file__))]
    pilot.write_json(root / 'launch-pins.json', {'pins': pins, 'mode': mode, 'entries': 10000,
                     'rss_ceiling_bytes': RSS_CEILING, 'disk_floor_bytes': FLOOR, 'fixture_ceiling_bytes': FIXTURE_CEILING,
                     'managed_ram_limit': '160GiB', 'scratch_limit': '2GiB', 'total_timeout_seconds': 28800,
                     'schedule': ['unforced', 'forced'], 'production_repository_writable': False,
                     'source': str(PREVIOUS / 'input'), 'toolchains': {
                         name: subprocess.check_output(command, text=True).strip() for name, command in
                         [('go', ['go', 'version']), ('rust', ['rustc', '--version']), ('cargo', ['cargo', '--version'])]}})
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(('VAULTIC_', 'VAULTICDB_', 'RESTIC_', 'RUSTIC_'))
                   and key not in ('GOGC', 'GODEBUG', 'GOMEMLIMIT')}
    environment.update({'TMPDIR': str(runtime), 'VAULTIC_TEST_TMPDIR': str(runtime), 'PYTHONDONTWRITEBYTECODE': '1',
                        'VAULTIC_FEATURES': 'slatedb-authoritative=true', 'VAULTIC_PHASE35_METADATA_READ_COMPARE': mode,
                        'VAULTIC_PHASE35_THROUGHPUT_ROOT': str(root), 'VAULTIC_PHASE35_THROUGHPUT_FORCE_COMPARE_RETRY': '1',
                        'VAULTIC_PHASE35_THROUGHPUT_CONTROL_ROOT': str(runtime),
                        'VAULTIC_PHASE35_THROUGHPUT_CANDIDATE_SHA256': pilot.digest(CANDIDATE),
                        'VAULTIC_PHASE35_THROUGHPUT_NATIVE_SHA256': pilot.digest(NATIVE)})
    membership = next(line[3:] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
    cgroup = Path('/sys/fs/cgroup') / membership.lstrip('/')
    driver = root / 'driver'
    pilot.private_directory(driver)
    process = observer = None
    result = {'accepted': False, 'failure': None, 'peak_combined_rss_bytes': 0, 'entries': 10000, 'mode': mode}
    started = time.monotonic()
    try:
        with private_output(driver / 'observer.stdout.log') as observer_output, private_output(driver / 'observer.stderr.log') as observer_errors:
            observer = subprocess.Popen([sys.executable, str(WORKSPACE / 'helpers/phase35-m6/observe.py'), '--root', str(root),
                                         '--cgroup', str(cgroup), '--output', str(root / 'observations.jsonl'), '--interval=1'],
                                        stdout=observer_output, stderr=observer_errors, env=environment)
            deadline = time.monotonic() + 15
            while not (root / 'observations.jsonl').exists() or not (root / 'observations.jsonl').stat().st_size:
                if observer.poll() is not None or time.monotonic() >= deadline:
                    raise ValueError('observer readiness failed')
                threading.Event().wait(0.05)
            with private_output(driver / 'stdout.log') as output, private_output(driver / 'stderr.log') as errors, private_output(driver / 'rss.jsonl', text=True) as samples:
                process = subprocess.Popen([str(root / 'qualifier.test'), '-test.run=^TestPublicationForceComparisonRetry$', '-test.v', '-test.timeout=7h50m'],
                                           stdout=output, stderr=errors, env=environment, cwd=WORKSPACE / 'cmd/vaultic', start_new_session=True)
                last_storage = 0
                while True:
                    rss, count = pilot.process_group_rss(process.pid)
                    elapsed = time.monotonic() - started
                    result['peak_combined_rss_bytes'] = max(result['peak_combined_rss_bytes'], rss)
                    samples.write(json.dumps({'elapsed_seconds': elapsed, 'rss_bytes': rss, 'processes': count}) + '\n')
                    samples.flush()
                    if elapsed - last_storage >= 30:
                        if storage_bytes(root) > FIXTURE_CEILING:
                            raise ValueError('fixture byte ceiling reached')
                        last_storage = elapsed
                    if observer.poll() is not None or rss > RSS_CEILING or elapsed >= 28800 or shutil.disk_usage(root).free < FLOOR:
                        raise ValueError('observer, RSS, time or disk safety limit reached')
                    try:
                        process.wait(timeout=0.25)
                        break
                    except subprocess.TimeoutExpired:
                        pass
                result['exit_code'] = process.returncode
                if process.returncode:
                    raise ValueError('qualifier failed; preserve stage logs')
                for name in ('unforced', 'forced'):
                    measured = json.loads((root / 'group-64' / name / 'result.json').read_text())
                    if measured['entries'] != 10000 or measured['summary']['data_blobs'] != 0 or not all(measured[key] is True for key in ('restore_verified', 'reopen_verified', 'source_unchanged')):
                        raise ValueError('candidate acceptance incomplete')
                if any(pilot.digest(Path(pin['path'])) != pin['sha256'] for pin in pins) or storage_bytes(root) > FIXTURE_CEILING:
                    raise ValueError('pin or fixture ceiling differs')
                result['accepted'] = True
    except BaseException as error:
        result['failure'] = type(error).__name__ + ': ' + str(error)
    finally:
        try:
            try:
                interrupt(process, group=True)
            finally:
                interrupt(observer)
        except BaseException as error:
            result['accepted'] = False
            result['failure'] = 'cleanup: ' + type(error).__name__ + ': ' + str(error)
        finally:
            result['wall_seconds'] = time.monotonic() - started
            result['owned_process_exit_code'] = None if process is None else process.returncode
            pilot.write_json(root / 'outcome.json', result)
    print(json.dumps(result), flush=True)
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', required=True, choices=('parallel', 'coalesced', 'catalog'))
    parser.add_argument('--prepare', action='store_true')
    arguments = parser.parse_args()
    os.umask(0o077)
    if arguments.prepare:
        prepare(arguments.mode)
    else:
        raise SystemExit(execute(arguments.mode))