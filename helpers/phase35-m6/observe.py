#!/usr/bin/env python3.12
import argparse
import json
import os
from pathlib import Path
import threading
import time
import unittest


def optional_text(path):
    try:
        return path.read_text()
    except OSError:
        return None


def process_sample(pid):
    root = Path('/proc') / str(pid)
    text = (root / 'stat').read_text()
    fields = text[text.rfind(')') + 2:].split()
    status = (root / 'status').read_text()
    rss = next((int(line.split()[1]) * 1024 for line in status.splitlines()
                if line.startswith('VmRSS:')), None)
    io = optional_text(root / 'io')
    return {'pid': pid, 'start_ticks': int(fields[19]), 'name': (root / 'comm').read_text().strip(),
            'state': fields[0], 'cpu_ticks': int(fields[11]) + int(fields[12]),
            'threads': int(fields[17]), 'rss_bytes': rss,
            'io': dict((key.rstrip(':'), int(value)) for key, value in
                       (line.split() for line in io.splitlines())) if io is not None else None,
            'schedstat': optional_text(root / 'schedstat')}


def deltas(current, previous, elapsed, ticks):
    if previous is None or elapsed <= 0 or current['start_ticks'] != previous['start_ticks']:
        return {'cpu_cores': None, 'io_bytes_per_second': None}
    cpu = current['cpu_ticks'] - previous['cpu_ticks']
    io = None
    if current['io'] is not None and previous['io'] is not None:
        io = {key: (value - previous['io'][key]) / elapsed
              for key, value in current['io'].items() if key in previous['io']
              and value >= previous['io'][key] and key not in ('syscr', 'syscw')}
    return {'cpu_cores': cpu / ticks / elapsed if cpu >= 0 else None,
            'io_bytes_per_second': io}


def counter_rates(current, previous):
    rates = []
    if previous is None:
        return rates
    if 'components' not in current or 'components' not in previous:
        seconds = current.get('seconds_elapsed', 0) - previous.get('seconds_elapsed', 0)
        if seconds > 0:
            for name in ('bytes_done', 'files_done'):
                now = current.get(name)
                before = previous.get(name)
                if isinstance(now, (int, float)) and isinstance(before, (int, float)) and now >= before:
                    rates.append({'name': name, 'counter_unit': 'logical_bytes' if name == 'bytes_done' else 'files',
                                  'per_second': (now - before) / seconds, 'window_seconds': seconds})
        return rates
    old = {component['component']: component for component in previous['components']}
    for component in current['components']:
        prior = old.get(component['component'])
        if not prior or component.get('stale') or prior.get('stale'):
            continue
        seconds = (component['captured_unix_ms'] - prior['captured_unix_ms']) / 1000
        if seconds <= 0 or component['process_start_id'] != prior['process_start_id']:
            continue
        metrics = {(metric['name'], json.dumps(metric.get('labels', {}), sort_keys=True)): metric
                   for metric in prior.get('metrics', [])}
        for metric in component.get('metrics', []):
            before = metrics.get((metric['name'], json.dumps(metric.get('labels', {}), sort_keys=True)))
            value = metric.get('value')
            earlier = before.get('value') if before else None
            if metric.get('kind') != 'counter' or not isinstance(value, (int, float)) or not isinstance(earlier, (int, float)):
                continue
            if value >= earlier:
                rates.append({'component': component['component'], 'name': metric['name'],
                              'labels': metric.get('labels', {}), 'counter_unit': metric['unit'],
                              'per_second': (value - earlier) / seconds, 'window_seconds': seconds})
    return rates


class TelemetryTail:
    def __init__(self):
        self.offsets = {}

    def read(self, path):
        offset = self.offsets.get(str(path), 0)
        result = None
        with open(path, 'rb') as stream:
            stream.seek(offset)
            for _ in range(100):
                line = stream.readline(2 << 20)
                if not line or not line.endswith(b'\n'):
                    break
                offset += len(line)
                try:
                    value = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if isinstance(value, dict) and 'components' in value:
                    result = value
                elif isinstance(value, dict) and value.get('message_type') in ('status', 'summary'):
                    allowed = ('message_type', 'seconds_elapsed', 'files_done', 'bytes_done',
                               'total_files', 'total_bytes', 'total_files_processed', 'total_bytes_processed',
                               'total_duration', 'data_added', 'data_added_packed')
                    result = {key: value[key] for key in allowed if key in value}
        self.offsets[str(path)] = offset
        return result


def observe(root, cgroup, output, interval):
    os.umask(0o077)
    ticks = os.sysconf('SC_CLK_TCK')
    previous = {}
    previous_time = None
    tail = TelemetryTail()
    previous_telemetry = {}
    stop = threading.Event()
    started = time.monotonic()
    with open(output, 'x') as stream:
        os.chmod(output, 0o600)
        while cgroup.exists():
            now = time.monotonic()
            elapsed = now - previous_time if previous_time is not None else 0
            processes = []
            unavailable = []
            procs = optional_text(cgroup / 'cgroup.procs')
            if procs is None:
                break
            for value in procs.splitlines():
                pid = int(value)
                try:
                    current = process_sample(pid)
                except (OSError, ValueError, StopIteration) as error:
                    unavailable.append({'pid': pid, 'reason': type(error).__name__})
                    continue
                current.update(deltas(current, previous.get(pid), elapsed, ticks))
                processes.append(current)
            telemetry = []
            stages = []
            for mode in ('pebble', 'ram', 'kv', 'auto'):
                cell = root / mode
                if not cell.exists():
                    continue
                stages += [mode + '/' + directory.name for directory in cell.iterdir()
                           if directory.is_dir() and (directory / 'stdout.log').exists()
                           and not (directory / 'result.json').exists()]
                for name in ('import-telemetry.jsonl', 'index-check-telemetry.jsonl', 'backup/stdout.log'):
                    path = cell / name
                    if path.exists():
                        value = tail.read(path)
                        if value is not None:
                            telemetry.append({'source': mode + '/' + name, 'snapshot': value,
                                              'counter_rates': counter_rates(value, previous_telemetry.get(str(path)))})
                            previous_telemetry[str(path)] = value
            record = {'schema': 'phase35-m6-observation-v1', 'unix_seconds': time.time(),
                      'elapsed_seconds': now - started, 'interval_seconds': elapsed,
                      'active_stages': stages, 'processes': processes, 'process_unavailable': unavailable,
                      'cgroup': {name: optional_text(cgroup / name) for name in
                                 ('cpu.stat', 'cpu.pressure', 'io.stat', 'io.pressure', 'memory.current',
                                  'memory.stat', 'memory.events', 'memory.pressure')},
                      'host_shared': {'nfs_rpc': optional_text(Path('/proc/net/rpc/nfs')),
                                      'cpu_pressure': optional_text(Path('/proc/pressure/cpu')),
                                      'io_pressure': optional_text(Path('/proc/pressure/io')),
                                      'memory_pressure': optional_text(Path('/proc/pressure/memory'))},
                      'new_operation_telemetry': telemetry,
                      'limitations': ['NFS RPC/host pressure are shared, not pilot-attributed',
                                      'proc read_bytes/write_bytes do not account for all NFS traffic',
                                      'observer attached after import start; earlier OS samples unavailable',
                                      'missing GC/work counters remain unknown; no inference from RSS']}
            stream.write(json.dumps(record, separators=(',', ':')) + '\n')
            stream.flush()
            previous = {value['pid']: value for value in processes}
            previous_time = now
            stop.wait(interval)


class ObservationTests(unittest.TestCase):
    def test_operation_throughput_and_reset(self):
        def snapshot(timestamp, value, identity='fixture'):
            return {'components': [{'component': 'vaultic', 'captured_unix_ms': timestamp,
                    'process_start_id': identity, 'metrics': [{'name': 'completed_bytes',
                    'kind': 'counter', 'labels': {'stage': 'fixture'}, 'unit': 'bytes', 'value': value}]}]}
        first = snapshot(1000, 100)
        self.assertEqual(counter_rates(snapshot(3000, 500), first)[0]['per_second'], 200)
        self.assertEqual(counter_rates(snapshot(3000, 10), first), [])
        self.assertEqual(counter_rates(snapshot(3000, 500, 'restarted'), first), [])

    def test_counter_rates_and_restart(self):
        previous = {'start_ticks': 1, 'cpu_ticks': 100, 'io': {'rchar': 1000}}
        current = {'start_ticks': 1, 'cpu_ticks': 300, 'io': {'rchar': 5000}}
        self.assertEqual(deltas(current, previous, 2, 100),
                         {'cpu_cores': 1, 'io_bytes_per_second': {'rchar': 2000}})
        current['start_ticks'] = 2
        self.assertIsNone(deltas(current, previous, 2, 100)['cpu_cores'])

    def test_backup_logical_rate(self):
        previous = {'seconds_elapsed': 10, 'bytes_done': 100, 'files_done': 1}
        current = {'seconds_elapsed': 12, 'bytes_done': 500, 'files_done': 3}
        rates = counter_rates(current, previous)
        self.assertEqual(rates[0]['per_second'], 200)
        self.assertEqual(rates[0]['counter_unit'], 'logical_bytes')
        self.assertEqual(rates[1]['per_second'], 1)

    def test_live_process(self):
        sample = process_sample(os.getpid())
        self.assertEqual(sample['pid'], os.getpid())
        self.assertGreater(sample['rss_bytes'], 0)
        self.assertGreater(sample['threads'], 0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--selftest', action='store_true')
    parser.add_argument('--root', type=Path)
    parser.add_argument('--cgroup', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--interval', type=float, default=5)
    args = parser.parse_args()
    if args.selftest:
        unittest.main(argv=[__file__])
    elif args.root and args.cgroup and args.output and args.interval >= 1:
        observe(args.root, args.cgroup, args.output, args.interval)
    else:
        parser.error('provide --root --cgroup --output and interval >= 1s')


if __name__ == '__main__':
    main()