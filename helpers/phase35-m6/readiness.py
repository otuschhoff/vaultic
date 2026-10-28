#!/usr/bin/env python3.12
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import subprocess
import threading
import time

import observe
import pilot


SCHEMA = 'indexdb-readiness-v1'
MAX_LINE = 2 << 20
MAX_PROCESSES = 16
MAX_JOBS = 128
RECEIPT_RESERVE = 32 << 20
EXPECTED = {'indexes': 10170, 'packs': 419853, 'blobs': 380166821, 'snapshots': 143}
EVENTS = {'summary', 'reconciliation_stats', 'metadata_lookup_stats',
          'commit_rpc_stats', 'pack_publication_stats', 'working_state_stats',
          'working_memory', 'nfs_source_stats'}
STAGES = ('catalog', 'snapshot-references', 'metadata-check', 'repository-check',
          'activation-check', 'activate', 'backup', 'reconcile-tiers',
          'post-metadata-check', 'post-repository-check', 'restore')


def integer(value, name, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError('invalid integer: ' + name)
    return value


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def private_tree(root):
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(directory) / name
            stat = path.lstat()
            if path.is_symlink() or stat.st_nlink > 1 and path.is_file():
                raise ValueError('shared or redirected writable object: ' + str(path))


def tree_bytes(root):
    total = 0
    for directory, _, files in os.walk(root, followlinks=False):
        for name in files:
            path = Path(directory) / name
            if path.is_symlink():
                raise ValueError('redirected working artifact')
            total += path.stat().st_size
    return total


def retain_process(processes, sample):
    pid = sample['pid']
    if pid in processes and processes[pid]['start_ticks'] != sample['start_ticks']:
        raise ValueError('PID reused during owned action')
    if len(processes) >= MAX_PROCESSES and pid not in processes:
        raise ValueError('process identity capacity exceeded')
    processes[pid] = sample


class EventError(ValueError):
    def __init__(self, message, events):
        super().__init__(message)
        self.events = events


def load_contract(path, value=None):
    if path.stat().st_size > MAX_LINE:
        raise ValueError('oversized contract')
    plan = json.loads(path.read_text()) if value is None else value
    if plan['schema'] != SCHEMA or plan['authorization'] != 'private-target-only':
        raise ValueError('missing private execution authorization')
    root = Path(plan['root']).resolve(strict=True)
    if not root.is_relative_to(pilot.AUTH_ROOT.resolve()) or root == pilot.AUTH_ROOT.resolve():
        raise ValueError('target outside private authorization root')
    if root.stat().st_mode & 0o777 != 0o700:
        raise ValueError('private root permissions changed')
    if str(root) != plan['root'] or Path(plan['root']).is_symlink():
        raise ValueError('noncanonical target')
    actual_identity = {'device': root.stat().st_dev, 'inode': root.stat().st_ino,
                       'repo_device': (root / 'repo').stat().st_dev,
                       'repo_inode': (root / 'repo').stat().st_ino}
    if plan['target_identity'] != actual_identity:
        raise ValueError('private target identity changed')
    for name in ('repo', 'db', 'wal', 'cache', 'working', 'tmp'):
        target = root / name
        if target.is_symlink() or target.resolve().parent != root:
            raise ValueError('unsafe private target: ' + name)
    for name, pin in plan['pins'].items():
        if pilot.digest(Path(pin['path'])) != pin['sha256']:
            raise ValueError('frozen input changed: ' + name)
    required = {'binary', 'daemon', 'profile', 'empty_profile', 'inventory',
                'source_profile', 'supervisor', 'pilot', 'observer', 'password'}
    if not required.issubset(plan['pins']):
        raise ValueError('missing frozen inputs')
    if Path(plan['password_file']).is_symlink():
        raise ValueError('password path must not be a symlink')
    if plan['password_file'] != plan['pins']['password']['path']:
        raise ValueError('unpinned password file')
    for name, actual in (('supervisor', __file__), ('pilot', pilot.__file__), ('observer', observe.__file__)):
        if Path(plan['pins'][name]['path']).resolve() != Path(actual).resolve():
            raise ValueError('unpinned executing helper: ' + name)
    inventory = Path(plan['pins']['inventory']['path'])
    with open(inventory) as stream:
        inventory_counts = {'index': 0, 'snapshots': 0, 'data': 0, 'config': 0, 'keys': 0}
        seen = set()
        for ordinal, line in enumerate(iter(lambda: stream.readline(4097), '')):
            if ordinal >= 500_000 or len(line) > 4096:
                raise ValueError('unbounded source inventory')
            entry = json.loads(line)
            source = Path(entry['path'])
            source_identity = hashlib.sha256(str(source).encode()).digest()
            if source_identity in seen:
                raise ValueError('duplicate source inventory entry')
            seen.add(source_identity)
            if not source.resolve().is_relative_to(pilot.SOURCE_REPO.resolve()):
                raise ValueError('source inventory escapes original repository')
            if source.stat().st_size != integer(entry['size'], 'source object size'):
                raise ValueError('source inventory changed')
            if 'sha256' in entry and pilot.digest(source) != entry['sha256']:
                raise ValueError('source metadata changed')
            relative = source.relative_to(pilot.SOURCE_REPO)
            if relative.parts[0] in inventory_counts:
                inventory_counts[relative.parts[0]] += 1
                if relative.parts[0] != 'data' and 'sha256' not in entry:
                    raise ValueError('unhashed legacy metadata inventory')
            candidate = root / 'repo' / relative
            if candidate.is_symlink() or candidate.stat().st_size != entry['size']:
                raise ValueError('private candidate differs from frozen inventory')
            if 'sha256' in entry and pilot.digest(candidate) != entry['sha256']:
                raise ValueError('private candidate metadata differs from frozen inventory')
        if inventory_counts['keys'] < 1:
            raise ValueError('missing frozen repository keys')
        if {key: value for key, value in inventory_counts.items() if key != 'keys'} != {
            'index': EXPECTED['indexes'], 'snapshots': EXPECTED['snapshots'],
            'data': EXPECTED['packs'], 'config': 1}:
            raise ValueError('incomplete original metadata inventory')
    if pilot.tomllib.loads(Path(plan['pins']['empty_profile']['path']).read_text()):
        raise ValueError('nonempty check profile')
    config = pilot.tomllib.loads(Path(plan['pins']['profile']['path']).read_text())
    selection = pilot.source_selection(config)
    if selection != plan['selection'] or any(not Path(source).is_dir() for source in selection['sources']):
        raise ValueError('source selection changed or unavailable')
    original = pilot.source_selection(pilot.tomllib.loads(Path(plan['pins']['source_profile']['path']).read_text()))
    if selection != original or len(selection['sources']) != 52 or plan['expected'] != EXPECTED:
        raise ValueError('frozen real-input scope changed')
    if not {'!**/.Trash-*/**', '!**/lost+found/**'}.issubset(selection['globs']):
        raise ValueError('frozen exclusions missing')
    if not {'CACHEDIR.TAG', '.nobackup'}.issubset(selection['exclude_if_present']):
        raise ValueError('frozen exclusion markers missing')
    if plan['jobs'] != len(config['backup']['snapshots']) or not 1 <= plan['jobs'] <= MAX_JOBS:
        raise ValueError('invalid snapshot job coverage')
    forbidden = {'stdin', 'stdin-from-command', 'files-from', 'files-from-raw', 'files-from-verbatim',
                 'dry-run', 'skip-if-unchanged', 'experimental', 'pathdiff', 'fsevents', 'hooks'}
    for section in [config, config['backup']] + config['backup']['snapshots']:
        if any(key in forbidden or key.startswith('experimental-') for key in section):
            raise ValueError('unqualified backup mode')
    for original in selection['sources'] + [str(pilot.SOURCE_REPO)]:
        source = Path(original).resolve()
        if root.is_relative_to(source) or source.is_relative_to(root):
            raise ValueError('source and writable target overlap')
    socket = Path(plan['socket'])
    if socket.parent.is_symlink() or not socket.is_absolute() or len(str(socket).encode()) > 100:
        raise ValueError('unsafe runtime socket')
    if socket.parent.stat().st_mode & 0o777 != 0o700:
        raise ValueError('runtime must be private')
    for name in ('indexes', 'packs', 'blobs', 'snapshots'):
        integer(plan['expected'][name], name, 1)
    limits = plan['limits']
    for name in ('artifact_bytes', 'stream_bytes', 'sample_bytes'):
        integer(limits[name], name, MAX_LINE)
    samples = (limits['backup_seconds'] * 4 + 2) * 1024
    stages = len(STAGES) - 1 + len(plan['restores']) + plan['jobs']
    telemetry_bytes = integer(limits['telemetry_bytes'], 'telemetry_bytes', MAX_LINE * (43200 // 5 + 2))
    if limits['sample_bytes'] < samples or limits['artifact_bytes'] < stages * (
            limits['sample_bytes'] + 2 * limits['stream_bytes'] + telemetry_bytes + RECEIPT_RESERVE):
        raise ValueError('insufficient full-run exporter capacity')
    if limits['rss_bytes'] != pilot.RSS_LIMIT or limits['sample_seconds'] != 0.25:
        raise ValueError('RSS safety contract changed')
    if limits['import_seconds'] != 43200 or limits['check_seconds'] != 43200 or limits['backup_seconds'] != 604800:
        raise ValueError('runtime contract changed')
    if limits['disk_free_bytes'] != 57_481_929_687_040:
        raise ValueError('disk reserve contract changed')
    if plan['working_limit'] != '160GiB' or plan['scratch_limit'] != '8GiB':
        raise ValueError('managed resource contract changed')
    if not plan['restores'] or len(plan['restores']) > MAX_JOBS:
        raise ValueError('freeze explicit historical restore identities')
    for item in plan['restores']:
        snapshot = item['snapshot']
        if len(snapshot) != 64 or any(letter not in '0123456789abcdef' for letter in snapshot):
            raise ValueError('invalid frozen historical restore identity')
    return plan


def seal(draft, output):
    if draft.stat().st_size > MAX_LINE:
        raise ValueError('oversized draft')
    plan = json.loads(draft.read_text())
    root = Path(plan['root']).resolve(strict=True)
    if plan['authorization'] != 'private-target-only' or str(root) != plan['root']:
        raise ValueError('explicit canonical private target approval required')
    plan['schema'] = SCHEMA
    plan['expected'] = dict(EXPECTED)
    plan['working_limit'], plan['scratch_limit'] = '160GiB', '8GiB'
    plan['limits'] = {'rss_bytes': pilot.RSS_LIMIT, 'sample_seconds': 0.25,
                      'import_seconds': 43200, 'check_seconds': 43200, 'backup_seconds': 604800,
                      'disk_free_bytes': 57_481_929_687_040, 'artifact_bytes': 8 << 40,
                      'stream_bytes': 512 << 20, 'sample_bytes': 3 << 30, 'telemetry_bytes': 20 << 30}
    helpers = {'supervisor': __file__, 'pilot': pilot.__file__, 'observer': observe.__file__}
    for name, actual in helpers.items():
        plan['pins'][name] = {'path': str(Path(actual).resolve())}
    for pin in plan['pins'].values():
        path = Path(pin['path']).resolve(strict=True)
        actual = pilot.digest(path)
        if pin.get('sha256', actual) != actual:
            raise ValueError('draft pin changed')
        pin.update({'path': str(path), 'sha256': actual})
    config = pilot.tomllib.loads(Path(plan['pins']['profile']['path']).read_text())
    plan['selection'] = pilot.source_selection(config)
    plan['jobs'] = len(config['backup']['snapshots'])
    plan['target_identity'] = {'device': root.stat().st_dev, 'inode': root.stat().st_ino,
                               'repo_device': (root / 'repo').stat().st_dev,
                               'repo_inode': (root / 'repo').stat().st_ino}
    private_tree(root / 'repo')
    load_contract(draft, plan)
    pilot.write_json(output, plan)
    return fingerprint(plan)


def command(plan, stage, restore=None):
    root = Path(plan['root'])
    pins = plan['pins']
    argv = [pins['binary']['path'], '--json', '--repo=' + str(root / 'repo'),
            '--password-file=' + plan['password_file'], '--cache-dir=' + str(root / 'cache'),
            '--use-profile=' + pins['profile' if stage == 'backup' else 'empty_profile']['path']]
    socket = plan['socket']
    if stage in ('catalog', 'snapshot-references', 'activate', 'metadata-check',
                 'activation-check', 'post-metadata-check', 'reconcile-tiers'):
        argv += ['--metadata-daemon-path=' + pins['daemon']['path'],
             '--metadata-daemon-socket=' + socket,
             '--metadata-daemon-data-dir=' + str(root / 'db'),
             '--metadata-daemon-object-store=local']
        action = 'import' if stage in ('catalog', 'snapshot-references', 'activate') else (
            'rebuild-pack-stats' if stage == 'reconcile-tiers' else 'check')
        argv += ['index', action, '--start-daemon', '--daemon-path=' + pins['daemon']['path'],
                 '--daemon-socket=' + socket, '--daemon-data-dir=' + str(root / 'db'),
                 '--daemon-object-store=local']
        if action == 'import':
            argv += ['--from-legacy', '--resume', '--max-errors=100']
            if stage == 'snapshot-references':
                argv += ['--snapshot-metadata-only']
            else:
                argv += ['--snapshot-depth=0']
                if stage == 'activate':
                    argv += ['--activate']
        elif action == 'check':
            argv += ['--check-workers=2', '--check-rpc-concurrency=2', '--max-findings=100',
                     '--fail-on-warning']
        if action != 'rebuild-pack-stats':
            argv += ['--working-memory-mode=ram', '--working-memory-ram-limit=' + plan['working_limit'],
                     '--working-memory-scratch=' + str(root / 'working'),
                     '--working-memory-disk-budget=' + plan['scratch_limit']]
    else:
        argv += ['--metadata-daemon-path=' + pins['daemon']['path'],
                 '--metadata-daemon-socket=' + socket, '--metadata-daemon-data-dir=' + str(root / 'db'),
                 '--metadata-daemon-object-store=local']
        if stage == 'backup':
            argv += ['backup', '--working-memory-mode=ram',
                     '--working-memory-ram-limit=' + plan['working_limit'],
                     '--working-memory-scratch=' + str(root / 'working'),
                     '--working-memory-disk-budget=' + plan['scratch_limit']]
        elif stage in ('repository-check', 'post-repository-check'):
            argv += ['check', '--read-data', '--check-unused']
        elif stage == 'restore':
            argv += ['restore', restore, '--verify', '--target=' + str(root / 'restore' / fingerprint(restore))]
        else:
            raise ValueError('unknown stage')
    return argv


def json_events(path):
    retained = {}
    with open(path, 'rb') as stream:
        while True:
            line = stream.readline(MAX_LINE + 1)
            if not line:
                break
            if len(line) > MAX_LINE or not line.endswith(b'\n'):
                raise EventError('oversized or incomplete command event', retained)
            if not line.lstrip().startswith(b'{'):
                continue
            try:
                value = json.loads(line)
            except ValueError as error:
                raise EventError(str(error), retained) from error
            if not isinstance(value, dict):
                raise ValueError('invalid command event')
            kind = value.get('message_type')
            if 'num_errors' in value:
                retained['repository_check'] = value
            elif kind in EVENTS:
                retained[kind] = value
                if kind in ('summary', 'reconciliation_stats'):
                    key = 'summaries' if kind == 'summary' else 'reconciliations'
                    records = retained.setdefault(key, [])
                    if len(records) >= MAX_JOBS:
                        raise ValueError('snapshot event cardinality exceeded')
                    records.append(value)
                if kind == 'working_memory' and value.get('action') == 'complete':
                    retained['working_memory_complete'] = value
            elif 'indexes_total' in value:
                retained['import'] = value
            elif 'coverage' in value and 'consistency' in value:
                retained['index_check'] = value
    return retained


def validate_stage(plan, stage, events, identity):
    if stage in ('catalog', 'snapshot-references', 'activate'):
        result = events['import']
        for key in ('errors', 'warnings', 'trees_visited', 'nodes_visited', 'crawl_debt_created'):
            if integer(result[key], key) != 0:
                raise ValueError('incomplete or materialized historical import: ' + key)
        if stage == 'snapshot-references':
            if result.get('snapshot_metadata_only') is not True:
                raise ValueError('snapshot references were not explicitly imported')
            for key in ('snapshots_total', 'snapshots_seen', 'snapshots_imported'):
                if integer(result[key], key) != plan['expected']['snapshots']:
                    raise ValueError('snapshot reference coverage mismatch')
        else:
            for key in ('indexes_total', 'indexes_seen'):
                if integer(result[key], key) != plan['expected']['indexes']:
                    raise ValueError('catalog coverage mismatch')
            if stage == 'catalog':
                for key, expected in (('packs_imported', 'packs'), ('blobs_imported', 'blobs')):
                    if integer(result[key], key) != plan['expected'][expected]:
                        raise ValueError('catalog count mismatch: ' + key)
            if not isinstance(result.get('checkpoint'), str) or len(result['checkpoint']) != 64:
                raise ValueError('missing durable import checkpoint')
    elif stage in ('metadata-check', 'activation-check', 'post-metadata-check'):
        result = events['index_check']
        if result['coverage']['complete'] is not True or result['coverage']['mode'] != 'full':
            raise ValueError('incomplete metadata check coverage')
        if integer(result['warnings'], 'warnings') or result.get('findings'):
            raise ValueError('unclean metadata check')
        for key in ('pending_crawl_debt', 'pending_exports', 'failed_exports', 'missing_in_slatedb',
                'missing_in_legacy', 'missing_packs', 'invalid_packs', 'aggregate_mismatches',
                'reverse_edge_mismatches', 'snapshot_mismatches', 'snapshot_commit_mismatches',
                'path_version_mismatches', 'missing_placement_records', 'backend_pack_mismatches',
                'derived_tier_mismatches', 'packs_below_durability', 'verification_state_mismatches',
                    'analytics_mismatches'):
            if integer(result[key], key):
                raise ValueError('unresolved metadata state: ' + key)
        for key in ('plaintext_objects', 'invalid_encrypted_objects'):
            if integer(result.get(key, 0), key):
                raise ValueError('invalid encryption audit: ' + key)
        current = result['consistency']
        if not current.get('session_id') or not current.get('options_digest'):
            raise ValueError('missing read-session or check-option identity')
        integer(current['schema_version'], 'schema_version')
        actual = (current['repository_id'], integer(current['generation'], 'generation', 1),
                  integer(current.get('sequence', 0), 'sequence'), current['options_digest'])
        if actual[0] != plan['repository_id'] or identity is not None and actual[:2] != identity[:2]:
            raise ValueError('metadata identity/generation changed')
        if stage == 'activation-check' and identity is not None and actual[2:] != identity[2:]:
            raise ValueError('metadata or check options changed after checks')
        if integer(result['slatedb_snapshots'], 'slatedb_snapshots') != plan['expected']['snapshots'] + (plan.get('jobs', 1) if stage == 'post-metadata-check' else 0):
            raise ValueError('snapshot catalog count mismatch')
        return actual
    elif stage in ('repository-check', 'post-repository-check'):
        if integer(events['repository_check']['num_errors'], 'num_errors'):
            raise ValueError('repository check failed')
    elif stage == 'backup':
        summaries = events['summaries']
        reconciliations = events['reconciliations']
        if len(summaries) != plan['jobs'] or len(reconciliations) != plan['jobs']:
            raise ValueError('incomplete source job coverage')
        acknowledged = set()
        for summary, event in zip(summaries, reconciliations):
            snapshot = summary.get('snapshot_id')
            if not isinstance(snapshot, str) or len(snapshot) != 64 or any(
                    letter not in '0123456789abcdef' for letter in snapshot) or snapshot in acknowledged:
                raise ValueError('missing or duplicate acknowledged snapshot')
            acknowledged.add(snapshot)
            counters = event
            for key in ('deferred', 'failed', 'failed_canceled'):
                if integer(counters[key], key):
                    raise ValueError('incomplete required reconciliation')
            for key in ('scanned', 'reused', 'changed', 'reconciled'):
                integer(counters[key], key)
            for key in ('total_files_processed', 'total_bytes_processed'):
                integer(summary[key], key)
    elif stage == 'restore':
        summary = events['summary']
        if integer(summary['files_restored'], 'files_restored') != integer(summary['total_files'], 'total_files'):
            raise ValueError('incomplete verified restore')
    return identity


def stop_owned(process, sample=None):
    try:
        os.killpg(process.pid, signal.SIGINT)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if sample is not None:
            count = sample()
        else:
            _, count = pilot.process_group_rss(process.pid)
        if count == 0:
            return {'complete': True, 'forced': False, 'signal': 'SIGINT'}
        threading.Event().wait(0.1)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    return {'complete': False, 'forced': True, 'signal': 'SIGKILL'}


def run_observed(argv, directory, seconds, plan):
    limits = plan['limits']
    started, started_utc = time.monotonic(), utc()
    failure = threading.Event()
    errors = []
    processes = {}
    hashes = {}
    peaks = {'rss_bytes': 0, 'cpu_ticks': 0}
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(('VAULTIC_', 'VAULTICDB_', 'RESTIC_', 'RUSTIC_'))
                   and key not in ('GOGC', 'GODEBUG', 'GOMEMLIMIT')}
    root = Path(plan['root'])
    if 'index' in argv and argv[argv.index('index') + 1] in ('import', 'check'):
        argv = argv + ['--monitor-export-jsonl=' + str(directory / 'telemetry.jsonl')]
    environment.update({'TMPDIR': str(root / 'tmp'), 'HOME': str(root),
                        'VAULTIC_FEATURES': 'slatedb-authoritative=true'})

    def collect(source, stream):
        used = 0
        try:
            while True:
                chunk = source.read1(65536)
                if not chunk:
                    break
                used += len(chunk)
                if used > limits['stream_bytes']:
                    raise ValueError('command output capacity exceeded')
                stream.write(chunk)
                stream.flush()
        except Exception as error:
            errors.append(type(error).__name__ + ': ' + str(error))
            failure.set()
        finally:
            source.close()
            stream.close()

    process = None
    threads = []
    outputs = []
    cleanup = None
    try:
        for name in ('stdout.log', 'stderr.log'):
            outputs.append(open(directory / name, 'xb'))
        with open(directory / 'samples.jsonl', 'x') as samples:
            pilot.process_group_rss(os.getpgrp())
            for path in (root, root / 'working', root / 'tmp'):
                if not path.is_dir() or path.is_symlink():
                    raise ValueError('observation or working directory unavailable')
            pilot.write_json(directory / 'observer-ready.json', {
                'utc': utc(), 'monotonic': time.monotonic(), 'sample_seconds': limits['sample_seconds'],
                'rss_ceiling_bytes': limits['rss_bytes'], 'action_id': directory.name})
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
            process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       cwd=root, env=environment, start_new_session=True)
            for source, stream in zip((process.stdout, process.stderr), outputs):
                thread = threading.Thread(target=collect, args=(source, stream), daemon=True)
                thread.start()
                threads.append(thread)
            used = 0
            previous_sample = time.monotonic()
            diagnostic_at = 0
            scratch_peak = 0
            while True:
                now = time.monotonic()
                if now - previous_sample > 1:
                    raise ValueError('mandatory safety sampling gap exceeded one second')
                previous_sample = now
                rss, count = pilot.process_group_rss(process.pid)
                if count > MAX_PROCESSES:
                    raise ValueError('owned process cardinality exceeded')
                current = []
                for entry in Path('/proc').iterdir():
                    if not entry.name.isdecimal():
                        continue
                    pid = int(entry.name)
                    try:
                        if os.getpgid(pid) != process.pid:
                            continue
                        sample = observe.process_sample(pid)
                    except (FileNotFoundError, ProcessLookupError):
                        continue
                    retain_process(processes, sample)
                    if sample['state'] not in ('Z', 'X', 'x'):
                        try:
                            stat = (entry / 'exe').stat()
                            status = (entry / 'status').read_text()
                        except (FileNotFoundError, ProcessLookupError):
                            continue
                        executable = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
                        if executable not in hashes:
                            try:
                                hashes[executable] = pilot.digest(entry / 'exe')
                            except (FileNotFoundError, ProcessLookupError):
                                continue
                        sample['executable_sha256'] = hashes[executable]
                        pins = plan.get('pins', {})
                        allowed = {pin['sha256'] for name, pin in pins.items() if name in ('binary', 'daemon')}
                        if allowed and hashes[executable] not in allowed:
                            raise ValueError('unapproved owned executable')
                        swapped = next((int(line.split()[1]) for line in status.splitlines()
                                        if line.startswith('VmSwap:')), None)
                        if swapped is None or swapped:
                            raise ValueError('owned swap is present or unavailable')
                    current.append(sample)
                peaks['rss_bytes'] = max(peaks['rss_bytes'], rss)
                peaks['cpu_ticks'] = sum(item['cpu_ticks'] for item in processes.values())
                if now >= diagnostic_at:
                    scratch_peak = max(scratch_peak, tree_bytes(root / 'working') + tree_bytes(root / 'tmp'))
                    if scratch_peak > 8 * 1024 ** 3:
                        raise ValueError('working scratch ceiling exceeded')
                    diagnostic_at = now + 5
                else:
                    current = None
                record = json.dumps({'elapsed_seconds': now - started, 'utc': utc(),
                                     'combined_rss_bytes': rss, 'processes': current,
                                     'disk_free_bytes': shutil.disk_usage(root).free,
                                     'scratch_peak_bytes': scratch_peak}) + '\n'
                used += len(record.encode())
                if used > limits['sample_bytes']:
                    raise ValueError('safety sample capacity exceeded')
                samples.write(record)
                samples.flush()
                if failure.is_set():
                    raise ValueError('output collector failed')
                if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024 > 1 << 30:
                    raise ValueError('separate collector memory ceiling exceeded')
                telemetry = directory / 'telemetry.jsonl'
                if telemetry.exists() and telemetry.stat().st_size > limits.get('telemetry_bytes', limits['stream_bytes']):
                    raise ValueError('telemetry artifact capacity exceeded')
                if rss > limits['rss_bytes'] or now - started >= seconds:
                    raise ValueError('RSS or runtime ceiling exceeded')
                if shutil.disk_usage(root).free < limits['disk_free_bytes']:
                    raise ValueError('disk reserve reached')
                try:
                    process.wait(timeout=limits['sample_seconds'])
                    break
                except subprocess.TimeoutExpired:
                    pass
            if process.returncode != 0:
                raise ValueError('command failed: exit ' + str(process.returncode))
    except Exception as error:
        errors.append(type(error).__name__ + ': ' + str(error))
    finally:
        if process is not None:
            def cleanup_sample():
                rss, count = pilot.process_group_rss(process.pid)
                peaks['rss_bytes'] = max(peaks['rss_bytes'], rss)
                if rss > limits['rss_bytes']:
                    errors.append('cleanup RSS ceiling exceeded')
                sample_path = directory / 'samples.jsonl'
                if sample_path.stat().st_size + 1024 > limits['sample_bytes']:
                    raise ValueError('cleanup sample capacity exceeded')
                with open(sample_path, 'a') as stream:
                    stream.write(json.dumps({'elapsed_seconds': time.monotonic() - started,
                                             'utc': utc(), 'combined_rss_bytes': rss, 'cleanup': True}) + '\n')
                return count
            try:
                cleanup = stop_owned(process, cleanup_sample)
                if not cleanup['complete']:
                    errors.append('owned cleanup required forced termination')
            except Exception as error:
                errors.append('cleanup observation failed: ' + str(error))
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        for thread in threads:
            thread.join(timeout=10)
            if thread.is_alive():
                errors.append('collector failed to join')
        for stream in outputs:
            if not stream.closed:
                stream.close()
    events = {}
    try:
        scratch_peak = max(locals().get('scratch_peak', 0), tree_bytes(root / 'working') + tree_bytes(root / 'tmp'))
        if scratch_peak > 8 * 1024 ** 3:
            errors.append('working scratch ceiling exceeded after cleanup')
    except (OSError, ValueError) as error:
        errors.append('final scratch measurement unavailable: ' + str(error))
    try:
        events = json_events(directory / 'stdout.log')
    except (OSError, ValueError) as error:
        events = getattr(error, 'events', {})
        errors.append('completion events unavailable: ' + str(error))
    result = {'started_utc': started_utc, 'ended_utc': utc(),
              'wall_seconds': time.monotonic() - started, 'peaks': peaks,
              'exit_code': process.returncode if process is not None else None,
              'observation_complete': not errors, 'errors': errors, 'events': events,
              'processes': list(processes.values()), 'clock_ticks_per_second': os.sysconf('SC_CLK_TCK'),
              'argv_sha256': fingerprint(argv), 'scratch_peak_bytes': locals().get('scratch_peak'),
              'cleanup': cleanup,
              'cpu_provenance': 'identity-bound /proc tick samples; terminal exit ticks may be unavailable',
              'collector_peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
              'diagnostic_counters': {name: events.get(name) for name in (
                  'metadata_lookup_stats', 'commit_rpc_stats', 'pack_publication_stats',
                  'working_state_stats', 'nfs_source_stats')},
              'unsupported_metrics': {'actual_source_read_bytes': None, 'source_stat_calls': None,
                                      'chunking_cpu_seconds': None, 'compression_cpu_seconds': None,
                                      'encryption_cpu_seconds': None}}
    pilot.write_json(directory / 'observation.json', result)
    return result


def execute(path):
    plan = load_contract(path)
    root = Path(plan['root'])
    receipt_dir = root / 'readiness'
    pilot.private_directory(receipt_dir)
    receipt = {'schema': SCHEMA, 'contract_sha256': fingerprint(plan), 'started_utc': utc(),
               'historical_materialization': 'explicitly-deferred', 'outcomes': [],
               'backup_committed': False, 'acknowledged_snapshot_ids': [], 'verification_passed': False,
               'observation_complete': False, 'readiness_accepted': False}
    identity = None
    import_started = time.monotonic()
    reserve = receipt_dir / 'receipt.reserve'
    reserve_stream = None
    try:
        private_tree(root / 'repo')
        reserve_stream = open(reserve, 'xb')
        os.posix_fallocate(reserve_stream.fileno(), 0, RECEIPT_RESERVE)
        for stage in STAGES:
            if fingerprint(load_contract(path)) != receipt['contract_sha256']:
                raise ValueError('contract changed during execution')
            protected = [Path(source) for source in plan['selection']['sources']] + [pilot.SOURCE_REPO]
            pilot.verify_read_only(protected)
            work = [None]
            if stage == 'restore':
                work = plan['restores'] + [{'snapshot': snapshot} for snapshot in receipt['acknowledged_snapshot_ids']]
            for ordinal, restore in enumerate(work):
                directory = receipt_dir / (stage + '-' + str(ordinal))
                pilot.private_directory(directory)
                seconds = plan['limits']['backup_seconds' if stage == 'backup' else 'check_seconds']
                if stage in ('catalog', 'snapshot-references'):
                    seconds = plan['limits']['import_seconds'] - (time.monotonic() - import_started)
                if seconds <= 0:
                    raise ValueError('combined import deadline exhausted')
                if 'artifact_bytes' in plan['limits'] and tree_bytes(receipt_dir) > plan['limits']['artifact_bytes'] - RECEIPT_RESERVE:
                    raise ValueError('full-run artifact capacity exhausted')
                argv = command(plan, stage, restore['snapshot'] if restore else None)
                if stage == 'activate':
                    if identity is None:
                        raise ValueError('activation has no validated metadata identity')
                    argv += ['--activation-generation=' + str(identity[1]),
                             '--activation-sequence=' + str(identity[2]),
                             '--activation-snapshots=' + str(plan['expected']['snapshots'])]
                result = run_observed(argv, directory, seconds, plan)
                events = result['events']
                if stage == 'backup':
                    receipt['acknowledged_snapshot_ids'] = [summary['snapshot_id'] for summary in events.get('summaries', [])
                                                            if summary.get('snapshot_id')]
                    receipt['backup_committed'] = bool(receipt['acknowledged_snapshot_ids'])
                if not result['observation_complete']:
                    raise ValueError('mandatory observation failed: ' + '; '.join(result['errors']))
                identity = validate_stage(plan, stage, events, identity)
                outcome = {'stage': stage, 'ordinal': ordinal, 'observation': result,
                           'events': events, 'identity': identity,
                           'artifacts': {artifact.name: pilot.digest(artifact) for artifact in directory.iterdir()
                                         if artifact.is_file()}}
                pilot.write_json(directory / 'verified.json', outcome)
                receipt['outcomes'].append(outcome)
        receipt['verification_passed'] = True
        receipt['observation_complete'] = True
        receipt['readiness_accepted'] = True
    except Exception as error:
        receipt['failure'] = type(error).__name__ + ': ' + str(error)
    finally:
        receipt['ended_utc'] = utc()
        if reserve_stream is not None:
            reserve_stream.close()
        if reserve.exists():
            reserve.unlink()
        receipt['artifacts'] = {str(artifact.relative_to(receipt_dir)): pilot.digest(artifact)
                                for directory in receipt_dir.iterdir() if directory.is_dir()
                                for artifact in directory.iterdir() if artifact.is_file()}
        receipt['wall_seconds'] = time.monotonic() - import_started
        receipt['cost_summary'] = cost_summary(receipt['outcomes'])
        pilot.write_json(receipt_dir / 'receipt.json', receipt)
    return receipt


def cost_summary(outcomes):
    bootstrap = {'catalog', 'snapshot-references', 'metadata-check', 'repository-check',
                 'activation-check', 'activate'}
    backup_seconds = sum(item['observation']['wall_seconds'] for item in outcomes if item['stage'] == 'backup')
    byte_counts = [summary.get('total_bytes_processed') for item in outcomes if item['stage'] == 'backup'
                   for summary in item['events'].get('summaries', [])]
    logical_bytes = sum(byte_counts) if byte_counts and all(type(value) is int and value >= 0 for value in byte_counts) else None
    return {'bootstrap_wall_seconds': sum(item['observation']['wall_seconds'] for item in outcomes
                                           if item['stage'] in bootstrap),
            'backup_wall_seconds': backup_seconds,
            'logical_processed_bytes': logical_bytes,
            'logical_processed_bytes_per_second': logical_bytes / backup_seconds if backup_seconds and logical_bytes is not None else None,
            'peak_combined_rss_bytes': max((item['observation']['peaks']['rss_bytes'] for item in outcomes), default=None),
            'unavailable': ['exact exit CPU ticks', 'local-adapter source read bytes', 'exclusive substage CPU attribution'],
            'bottleneck_conclusions': None}


def main():
    parser = argparse.ArgumentParser(description='Private catalog-first readiness supervisor; never deploys production.')
    parser.add_argument('contract', type=Path)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--seal', type=Path, help='validate and seal an approved draft without execution')
    options = parser.parse_args()
    os.umask(0o077)
    if options.seal:
        if options.execute:
            parser.error('--seal and --execute are mutually exclusive')
        print(json.dumps({'contract_sha256': seal(options.seal, options.contract), 'execution_started': False}))
    elif options.execute:
        result = execute(options.contract)
        print(json.dumps({key: result[key] for key in ('backup_committed', 'verification_passed',
                                                      'observation_complete', 'readiness_accepted')}))
        raise SystemExit(0 if result['readiness_accepted'] else 1)
    plan = load_contract(options.contract)
    print(json.dumps({'contract_sha256': fingerprint(plan), 'execution_started': False,
                      'historical_materialization': 'explicitly-deferred'}))


if __name__ == '__main__':
    main()