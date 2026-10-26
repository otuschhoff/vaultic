import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import statistics
import subprocess
import sys
import threading
import time
import unittest


ROOT = Path(__file__).resolve().parent
PILOT = runpy.run_path(str(ROOT / 'pilot.py'))


def events_from(path):
    events = []
    with path.open() as stream:
        for line in stream:
            _, marker, encoded = line.partition('M6_EQUAL_WORK ')
            if marker:
                events.append(json.loads(encoded))
    return events


def evaluate(result, events):
    if not result.get('completed') or result.get('exit_code') != 0 or result.get('stop_reason') is not None:
        raise ValueError('component diagnostic did not complete successfully')
    catalog = [event for event in events if event.get('stage') == 'catalog_ready']
    finished = [event for event in events if event.get('stage') == 'finished']
    if len(catalog) != 1 or any(catalog[0].get(key) != expected for key, expected in
                               [('indexes', 10170), ('packs', 419853), ('blobs', 380166821)]):
        raise ValueError('complete frozen native catalog was not established')
    if (len(finished) != 1 or finished[0].get('full_native_catalog') is not True or
            finished[0].get('acceptance') is not False or finished[0].get('reopened_exact_samples') != 45):
        raise ValueError('native reopen evidence is incomplete')
    all_digests = {event.get('fact_sha256') for event in events
                   if event.get('stage') in ('pipeline', 'fact_validation_and_encoding')}
    if (len(all_digests) != 1 or not isinstance(next(iter(all_digests)), str) or
            len(next(iter(all_digests))) != 64 or
            any(character not in '0123456789abcdef' for character in next(iter(all_digests)))):
        raise ValueError('canonical decoded node facts differ')
    ratios = {}
    for stage, work in [('loader', ('items', 'bytes')), ('pipeline', ('items', 'nodes', 'puts', 'fact_sha256'))]:
        selected = [event for event in events if event.get('stage') == stage]
        if len(selected) != 10:
            raise ValueError('exactly five paired rounds are required: ' + stage)
        pairs = {}
        for event in selected:
            key = (event.get('round'), event.get('pooled'))
            if (type(key[0]) is not int or key[0] not in range(5) or type(key[1]) is not bool or
                    key in pairs or type(event.get('elapsed_ns')) is not int or event['elapsed_ns'] <= 0):
                raise ValueError('invalid or duplicated paired round')
            if any(type(event.get(field)) is not int or event[field] <= 0 for field in work if field != 'fact_sha256'):
                raise ValueError('empty or invalid work count')
            pairs[key] = event
        if len({tuple(event[field] for field in work) for event in selected}) != 1:
            raise ValueError('component rounds performed unequal work')
        ratios[stage] = [pairs[(round, False)]['elapsed_ns'] / pairs[(round, True)]['elapsed_ns']
                         for round in range(5)]
    for stage in ('decode', 'fact_validation_and_encoding', 'native_write'):
        rounds = [event for event in events if event.get('stage') == stage]
        if (len(rounds) != 5 or {event.get('round') for event in rounds} != set(range(5)) or
                any(type(event.get('elapsed_ns')) is not int or event['elapsed_ns'] <= 0 for event in rounds)):
            raise ValueError('stage-separated evidence is incomplete: ' + stage)
        fields = ('puts', 'bytes') if stage == 'native_write' else ('nodes',)
        if (any(type(event.get(field)) is not int or event[field] <= 0 for event in rounds for field in fields) or
                len({tuple(event[field] for field in fields) for event in rounds}) != 1):
            raise ValueError('stage-separated rounds performed unequal or empty work: ' + stage)
        pipeline = next(event for event in events if event.get('stage') == 'pipeline')
        if rounds[0]['puts' if stage == 'native_write' else 'nodes'] != pipeline['puts' if stage == 'native_write' else 'nodes']:
            raise ValueError('stage-separated work differs from pipeline: ' + stage)
    pipeline = ratios['pipeline']
    loader = ratios['loader']
    eligible = (statistics.median(pipeline) >= 1.25 and min(pipeline) >= 1.10 and
                sum(ratio >= 1.20 for ratio in pipeline) >= 4 and
                statistics.median(loader) >= 1.50 and min(loader) >= 1.25)
    return {'schema': 'phase35-m6-equal-work-gate-v1', 'eligible_for_fresh_ram': eligible,
            'pipeline_speedup_ratios': pipeline, 'loader_speedup_ratios': loader,
            'median_pipeline_speedup': statistics.median(pipeline),
            'median_loader_speedup': statistics.median(loader),
            'acceptance': False, 'fact_sha256': next(iter(all_digests)),
            'criteria': {'pipeline_median': 1.25, 'pipeline_minimum': 1.10,
                         'pipeline_rounds_at_least_1_20': 4, 'loader_median': 1.50, 'loader_minimum': 1.25},
            'limitations': ['bounded warm-corpus replay is not full import acceptance',
                            'full catalog is populated; residency of every native page is not asserted',
                            'serial paired rounds share storage and OS cache; fixed order alternates',
                            'component timers and CPU profiles are not exclusive wall partitions']}


def verify_pins():
    authorization = json.loads((ROOT / 'AUTHORIZATION.json').read_text())
    manifest = ROOT / 'LAUNCH-SHA256SUMS'
    if PILOT['digest'](manifest) != authorization['launch_sha256']:
        raise ValueError('launch manifest changed')
    for line in manifest.read_text().splitlines():
        expected, relative = line.split('  ', 1)
        path = ROOT / relative
        if not path.resolve().is_relative_to(ROOT) or PILOT['digest'](path) != expected:
            raise ValueError('pinned launch input changed: ' + relative)
    plan = PILOT['load_plan'](ROOT)
    inventory = []
    source = Path(plan['source_repo'])
    for section in ('keys', 'index', 'snapshots'):
        for directory, directories, files in os.walk(source / section, followlinks=False):
            directories.sort()
            for name in sorted(files):
                path = Path(directory) / name
                if path.is_symlink() or not path.is_file():
                    raise ValueError('unexpected original metadata file type')
                inventory.append({'path': str(path.relative_to(source)), 'size': path.stat().st_size})
    if inventory != json.loads((ROOT / 'source-inventory.json').read_text()):
        raise ValueError('original metadata inventory changed')
    pid = int(subprocess.check_output(['systemctl', 'show', 'vaulticdb-rustic.service', '--value', '-p', 'MainPID'], text=True))
    if pid != authorization['production_pid'] or PILOT['digest'](Path('/proc') / str(pid) / 'exe') != plan['daemon_sha256']:
        raise ValueError('production identity changed')
    if PILOT['shutil'].disk_usage(ROOT).free < plan['minimum_disk_free_bytes']:
        raise ValueError('shared NFS reserve unavailable')
    return plan


def protected_paths(plan):
    return [plan['source_repo'], plan['password_file'], str(PILOT['SOURCE_PROFILE'])] + plan['selection']['sources']


def run_diagnostic():
    plan = verify_pins()
    PILOT['verify_read_only']([Path(path) for path in protected_paths(plan)])
    os.umask(0o077)
    cell = ROOT / 'equal-work'
    PILOT['private_directory'](cell)
    for name in ('state', 'run', 'tmp', 'home'):
        PILOT['private_directory'](cell / name)
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(('VAULTIC_', 'VAULTICDB_', 'RESTIC_', 'RUSTIC_'))
                   and key not in ('GOGC', 'GODEBUG', 'GOMEMLIMIT')}
    environment.update({'VAULTIC_M6_EQUAL_WORK': '1', 'VAULTIC_M6_DIAGNOSTIC_STATE': str(cell / 'state'),
                        'VAULTIC_M6_DIAGNOSTIC_REPO': str(ROOT / 'source-view'),
                        'VAULTIC_M6_DIAGNOSTIC_PASSWORD_FILE': plan['password_file'],
                        'VAULTICDB_TEST_BINARY': str(ROOT / 'bin/vaulticdb'),
                        'HOME': str(cell / 'home'), 'TMPDIR': str(cell / 'tmp')})
    command = [str(ROOT / 'bin/equal-work.test'), '-test.run=^TestM6FullCatalogEqualWorkDiagnostic$',
               '-test.count=1', '-test.timeout=12h', '-test.v']
    outcome = PILOT['run_command'](command, cell / 'run', plan['import_timeout_seconds'], environment,
                                    disk_floor=plan['minimum_disk_free_bytes'])
    try:
        gate = evaluate(outcome, events_from(cell / 'run/stdout.log'))
    except (ValueError, KeyError, TypeError) as error:
        gate = {'schema': 'phase35-m6-equal-work-gate-v1', 'eligible_for_fresh_ram': False,
                'acceptance': False, 'failure_type': type(error).__name__, 'reason': str(error)}
    gate['diagnostic_result_sha256'] = PILOT['digest'](cell / 'run/result.json')
    gate['diagnostic_stdout_sha256'] = PILOT['digest'](cell / 'run/stdout.log')
    gate['launch_sha256'] = PILOT['digest'](ROOT / 'LAUNCH-SHA256SUMS')
    PILOT['write_json'](cell / 'gate.json', gate)
    verify_pins()
    PILOT['emit_event'](gate)
    if not gate['eligible_for_fresh_ram']:
        raise ValueError('no fresh acceptance run: bounded material-throughput gate did not pass')


def run_ram():
    verify_pins()
    gate = json.loads((ROOT / 'equal-work/gate.json').read_text())
    run = ROOT / 'equal-work/run'
    if (gate.get('eligible_for_fresh_ram') is not True or
            gate['launch_sha256'] != PILOT['digest'](ROOT / 'LAUNCH-SHA256SUMS') or
            gate['diagnostic_result_sha256'] != PILOT['digest'](run / 'result.json') or
            gate['diagnostic_stdout_sha256'] != PILOT['digest'](run / 'stdout.log') or
            not evaluate(json.loads((run / 'result.json').read_text()), events_from(run / 'stdout.log'))['eligible_for_fresh_ram']):
        raise ValueError('fresh RAM acceptance requires an unchanged passing equal-work gate')
    PILOT['execute'](ROOT, 'ram', False)
    verify_pins()


def run_observed(action):
    plan = verify_pins()
    membership = next(line[3:] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
    cgroup = Path('/sys/fs/cgroup') / membership.lstrip('/')
    output = ROOT / ('observations-' + action + '.jsonl')
    observer = subprocess.Popen(['/usr/bin/python3.12', str(ROOT / 'observer-v2.py'), '--root', str(ROOT),
                                 '--cgroup', str(cgroup), '--output', str(output), '--interval=5'])
    try:
        ready = time.monotonic() + 10
        while not output.exists() or output.stat().st_size == 0:
            if observer.poll() is not None or time.monotonic() >= ready:
                raise ValueError('independent observer did not become ready before workload')
            threading.Event().wait(0.05)
        if action == 'diagnostic':
            run_diagnostic()
        elif action == 'ram':
            run_ram()
        else:
            raise ValueError('only bounded diagnostic and gated fresh RAM are authorized here')
        if observer.poll() is not None:
            raise ValueError('independent observer terminated before workload completed')
    finally:
        observer.terminate()
        try:
            observer.wait(timeout=10)
        except subprocess.TimeoutExpired:
            observer.kill()
            observer.wait()


def launch(action):
    plan = verify_pins()
    unit = 'vaultic-m6-nextsteps-' + action + '-' + ROOT.name.removeprefix('pilot-')
    subprocess.run(['systemd-run', '--unit=' + unit, '--wait', '--pipe', '--collect',
                    '--property=ProtectSystem=strict', '--property=ReadWritePaths=' + str(ROOT),
                    '--property=ReadOnlyPaths=' + ' '.join(protected_paths(plan)),
                    '--property=NoNewPrivileges=yes', '--property=MemoryAccounting=yes',
                    '--property=MemorySwapMax=0', '--property=LimitCORE=0', '--property=UMask=0077',
                    '/usr/bin/python3.12', str(ROOT / 'nextsteps.py'), '--run', action], check=True)


class GateTests(unittest.TestCase):
    def evidence(self, speedup=1.5):
        events = [{'stage': 'catalog_ready', 'indexes': 10170, 'packs': 419853, 'blobs': 380166821},
                  {'stage': 'finished', 'full_native_catalog': True, 'acceptance': False, 'reopened_exact_samples': 45}]
        for round in range(5):
            for pooled in (False, True):
                for stage in ('loader', 'pipeline'):
                    events.append({'stage': stage, 'round': round, 'pooled': pooled,
                                   'elapsed_ns': int(3000 / speedup) if pooled else 3000,
                                   'items': 10, 'nodes': 100, 'puts': 200, 'bytes': 1000,
                                   'fact_sha256': 'a' * 64})
            for stage in ('decode', 'fact_validation_and_encoding', 'native_write'):
                events.append({'stage': stage, 'round': round, 'elapsed_ns': 100,
                               'nodes': 100, 'puts': 200, 'bytes': 1000, 'fact_sha256': 'a' * 64})
        return {'completed': True, 'exit_code': 0, 'stop_reason': None}, events

    def test_material_gain_required(self):
        self.assertTrue(evaluate(*self.evidence())['eligible_for_fresh_ram'])
        self.assertFalse(evaluate(*self.evidence(1.1))['eligible_for_fresh_ram'])

    def test_incomplete_diagnostic_rejected(self):
        result, events = self.evidence()
        result['completed'] = False
        with self.assertRaises(ValueError):
            evaluate(result, events)

    def test_unequal_work_rejected(self):
        result, events = self.evidence()
        events[2]['items'] += 1
        with self.assertRaises(ValueError):
            evaluate(result, events)

    def test_changed_facts_rejected(self):
        result, events = self.evidence()
        next(event for event in events if event.get('stage') == 'pipeline')['fact_sha256'] = 'b' * 64
        with self.assertRaises(ValueError):
            evaluate(result, events)

    def test_missing_pair_or_reopen_rejected(self):
        result, events = self.evidence()
        with self.assertRaises(ValueError):
            evaluate(result, events[:-1])
        events[1]['reopened_exact_samples'] = 0
        with self.assertRaises(ValueError):
            evaluate(result, events)

    def test_wrong_native_scope_rejected(self):
        result, events = self.evidence()
        next(event for event in events if event.get('stage') == 'native_write')['puts'] = 0
        with self.assertRaises(ValueError):
            evaluate(result, events)

    def test_duplicate_pair_rejected(self):
        result, events = self.evidence()
        loader = [event for event in events if event.get('stage') == 'loader']
        loader[-1]['round'], loader[-1]['pooled'] = loader[0]['round'], loader[0]['pooled']
        with self.assertRaises(ValueError):
            evaluate(result, events)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--selftest', action='store_true')
    parser.add_argument('--launch', choices=('diagnostic', 'ram'))
    parser.add_argument('--run', choices=('diagnostic', 'ram'))
    args = parser.parse_args()
    if args.selftest:
        unittest.main(argv=[__file__])
    elif args.launch:
        launch(args.launch)
    elif args.run:
        run_observed(args.run)
    else:
        parser.error('select --selftest, --launch or --run')