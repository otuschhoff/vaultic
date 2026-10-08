#!/usr/bin/env python3.12
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest
from unittest.mock import patch


RSS_LIMIT = 200_000_000_000
AUTH_ROOT = Path('/volume2/NASDA2/vaultic-test-m6')
SOURCE_REPO = Path('/volume2/NASDA2/rustic/repo')
SOURCE_PROFILE = Path('/volume2/NASDA2/rustic/etc/rustic.toml')
PASSWORD_FILE = Path('/volume2/NASDA2/rustic/etc/repo-password.txt')


def digest(path):
    with open(path, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write_json(path, value):
    with open(path, 'x', encoding='utf8') as stream:
        os.chmod(path, 0o600)
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')


def secure_directory(path):
    os.chmod(path, 0o700)
    if path.stat().st_mode & 0o777 != 0o700:
        raise ValueError('filesystem did not preserve private directory permissions')


def private_directory(path):
    path.mkdir(mode=0o700)
    secure_directory(path)


def source_selection(config):
    if 'use-profiles' in config:
        raise ValueError('included profiles require a separately frozen inventory')
    if any('hooks' in section for section in config.values() if isinstance(section, dict)):
        raise ValueError('profile hooks are not authorized by this pilot')
    backup = config['backup']
    patterns = backup['globs']
    if '!**/.snapshot' not in patterns or '!**/.snapshot/**' not in patterns:
        raise ValueError('both directory and descendant snapshot exclusions are required')
    if not all(isinstance(pattern, str) and pattern.startswith('!') for pattern in patterns):
        raise ValueError('positive/mixed rustic glob semantics require separate review')
    sources = []
    for job in backup['snapshots']:
        if 'hooks' in job:
            raise ValueError('backup job hooks are not authorized')
        if not job['sources'] or not job.get('one-file-system'):
            raise ValueError('expected complete one-filesystem source jobs')
        for source in job['sources']:
            path = Path(source)
            if not path.is_absolute() or '.snapshot' in path.parts:
                raise ValueError('unsafe explicit source root')
            sources.append(source)
    if len(set(sources)) != len(sources):
        raise ValueError('duplicate source roots require review')
    return {'sources': sources, 'globs': patterns,
            'exclude_if_present': backup.get('exclude-if-present', []),
            'preserve_original_profile_options': True}


def make_view(source, target):
    private_directory(target)
    shutil.copyfile(source / 'config', target / 'config')
    os.chmod(target / 'config', 0o600)
    private_directory(target / 'locks')
    (target / 'data').symlink_to((source / 'data').resolve(), target_is_directory=True)
    inventory = []
    def walk_error(error):
        raise error
    for section in ('keys', 'index', 'snapshots'):
        private_directory(target / section)
        if not (source / section).is_dir():
            raise ValueError('missing repository metadata directory: ' + section)
        for directory, directories, files in os.walk(source / section, followlinks=False, onerror=walk_error):
            directories.sort()
            for name in directories:
                if name == '.snapshot' or (Path(directory) / name).is_symlink():
                    raise ValueError('unexpected directory in repository metadata inventory')
                relative = (Path(directory) / name).relative_to(source)
                private_directory(target / relative)
            for name in sorted(files):
                original = Path(directory) / name
                if original.is_symlink() or not original.is_file():
                    raise ValueError('unexpected metadata file type')
                relative = original.relative_to(source)
                (target / relative).symlink_to(original.resolve())
                inventory.append({'path': str(relative), 'size': original.stat().st_size})
    return inventory


def prepare(binary, daemon):
    os.umask(0o077)
    profile_bytes = SOURCE_PROFILE.read_bytes()
    config = tomllib.loads(profile_bytes.decode('utf8'))
    selection = source_selection(config)
    missing_sources = []
    for source in selection['sources']:
        if '.snapshot' in Path(source).resolve().parts:
            raise ValueError('source resolves inside a snapshot: ' + source)
        if not Path(source).is_dir():
            missing_sources.append(source)
    root = Path(tempfile.mkdtemp(prefix='pilot-', dir=AUTH_ROOT))
    secure_directory(root)
    private_directory(root / 'bin')
    private_directory(root / 'tmp')
    shutil.copyfile(binary, root / 'bin/vaultic')
    shutil.copyfile(daemon, root / 'bin/vaulticdb')
    for name in ('vaultic', 'vaulticdb'):
        os.chmod(root / 'bin' / name, 0o700)
    (root / 'backup.toml').write_bytes(profile_bytes)
    (root / 'empty.toml').write_text('# Isolated import/check profile.\n')
    shutil.copyfile(Path(__file__), root / 'pilot.py')
    inventory = make_view(SOURCE_REPO, root / 'source-view')
    write_json(root / 'source-inventory.json', inventory)
    capacity = shutil.disk_usage(root)
    plan = {'schema': 'phase35-m6-real-pilot-v1', 'root': str(root),
            'authorization': 'operator conversation 2026-10-06; isolated test writes only',
            'source_repo': str(SOURCE_REPO), 'password_file': str(PASSWORD_FILE),
            'profile_sha256': digest(root / 'backup.toml'), 'view_config_sha256': digest(root / 'source-view/config'),
            'inventory_sha256': digest(root / 'source-inventory.json'),
            'vaultic_sha256': digest(root / 'bin/vaultic'), 'daemon_sha256': digest(root / 'bin/vaulticdb'),
            'runner_sha256': digest(root / 'pilot.py'), 'selection': selection,
            'backup_missing_sources': missing_sources,
            'modes': ['pebble', 'ram', 'kv', 'auto'], 'repetitions': 1,
            'rss_limit_bytes': RSS_LIMIT, 'rss_sample_interval_seconds': 0.25,
            'page_cache_in_rss_limit': False, 'working_limit': '160GiB',
            'import_timeout_seconds': 43200, 'check_timeout_seconds': 43200,
            'backup_timeout_seconds': 604800, 'extension_approved': False,
            'minimum_disk_free_bytes': max(capacity.total // 20, capacity.free // 5),
            'acceptance': 'complete semantics, RSS/time ceilings; compare actual wall and completed-work throughput',
            'latency_is_acceptance_gate': False, 'production_default_or_removal_approved': False,
            'limitations': ['single screening repetition; not repeated production acceptance',
                            'source files may change during backup; do not label follow-up truly unchanged',
                            'RSS ceiling is sampled, not an instantaneous kernel RSS limit',
                            'file-backed mapped RSS counts; unmapped page cache is excluded']}
    write_json(root / 'plan.json', plan)
    return root


def process_group_rss(group):
    total = 0
    count = 0
    for entry in Path('/proc').iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            if os.getpgid(int(entry.name)) != group:
                continue
            status = (entry / 'status').read_text()
        except (FileNotFoundError, ProcessLookupError):
            continue
        for line in status.splitlines():
            if line.startswith('VmRSS:'):
                total += int(line.split()[1]) * 1024
                count += 1
                break
        else:
            if '\nState:\tZ' in status or '\nState:\tX' in status:
                continue
            try:
                stat_fields = (entry / 'stat').read_text().rsplit(')', 1)[1].split()
            except (FileNotFoundError, ProcessLookupError):
                continue
            if stat_fields[0] in ('Z', 'X', 'x') or int(stat_fields[6]) & 4:
                continue
            raise ValueError('RSS unavailable for live pilot process')
    return total, count


def cgroup_memory():
    try:
        membership = next(line[3:] for line in Path('/proc/self/cgroup').read_text().splitlines()
                          if line.startswith('0::'))
        directory = Path('/sys/fs/cgroup') / membership.lstrip('/')
        stats = dict(line.split() for line in (directory / 'memory.stat').read_text().splitlines())
        return {'cgroup_current_bytes': int((directory / 'memory.current').read_text()),
                'cgroup_file_cache_bytes': int(stats['file'])}
    except (OSError, StopIteration, KeyError, ValueError):
        return {'cgroup_current_bytes': None, 'cgroup_file_cache_bytes': None}


def run_command(command, directory, seconds, environment, ceiling=RSS_LIMIT, disk_floor=0):
    started = time.monotonic()
    peak = 0
    reason = None
    with open(directory / 'stdout.log', 'xb') as stdout, open(directory / 'stderr.log', 'xb') as stderr, \
            open(directory / 'rss.jsonl', 'x') as samples:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=environment,
                                   cwd=directory, start_new_session=True)
        try:
            while True:
                try:
                    rss, count = process_group_rss(process.pid)
                except (OSError, ValueError):
                    reason = 'RSS monitoring unavailable'
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=30)
                    break
                elapsed = time.monotonic() - started
                peak = max(peak, rss)
                samples.write(json.dumps({'elapsed_seconds': elapsed, 'rss_bytes': rss,
                                          'processes': count, **cgroup_memory()}) + '\n')
                samples.flush()
                if rss > ceiling:
                    reason = 'RSS ceiling exceeded'
                elif elapsed >= seconds:
                    reason = 'wall timeout exceeded'
                elif disk_floor and shutil.disk_usage(directory).free < disk_floor:
                    reason = 'shared NFS free-space reserve reached'
                if reason:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=30)
                    break
                try:
                    process.wait(timeout=0.25)
                    break
                except subprocess.TimeoutExpired:
                    pass
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    result = {'command': command, 'exit_code': process.returncode, 'wall_seconds': time.monotonic() - started,
              'peak_combined_rss_bytes': peak, 'stop_reason': reason,
              'completed': process.returncode == 0 and reason is None}
    write_json(directory / 'result.json', result)
    return result


def load_plan(root):
    root = root.resolve(strict=True)
    if not root.is_relative_to(AUTH_ROOT.resolve()) or root == AUTH_ROOT.resolve():
        raise ValueError('pilot root is outside the authorized subtree')
    if root.stat().st_mode & 0o777 != 0o700:
        raise ValueError('pilot root must remain private')
    plan = json.loads((root / 'plan.json').read_text())
    if plan['root'] != str(root) or plan['schema'] != 'phase35-m6-real-pilot-v1':
        raise ValueError('wrong pilot identity')
    for field, path in [('profile_sha256', root / 'backup.toml'),
                        ('view_config_sha256', root / 'source-view/config'),
                        ('inventory_sha256', root / 'source-inventory.json'),
                        ('vaultic_sha256', root / 'bin/vaultic'),
                        ('daemon_sha256', root / 'bin/vaulticdb'),
                        ('runner_sha256', Path(__file__))]:
        if digest(path) != plan[field]:
            raise ValueError('frozen pilot artifact changed: ' + field)
    if digest(SOURCE_PROFILE) != plan['profile_sha256']:
        raise ValueError('operator source profile changed')
    if (plan['rss_limit_bytes'] != RSS_LIMIT or plan['extension_approved'] or
            plan['import_timeout_seconds'] != 43200 or plan['check_timeout_seconds'] != 43200 or
            plan['backup_timeout_seconds'] != 604800 or plan['modes'] != ['pebble', 'ram', 'kv', 'auto']):
        raise ValueError('operator resource/time/mode contract changed')
    if source_selection(tomllib.loads((root / 'backup.toml').read_text())) != plan['selection']:
        raise ValueError('frozen source selection changed')
    return plan


def verify_read_only(paths):
    mounts = []
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        fields = line.split()
        mount = Path(fields[4].replace('\\040', ' ').replace('\\134', '\\'))
        mounts.append((mount, set(fields[5].split(','))))
    for original in paths:
        path = Path(original).resolve(strict=True)
        matching = [(mount, flags) for mount, flags in mounts if path.is_relative_to(mount)]
        _, flags = max(matching, key=lambda item: len(item[0].parts))
        if 'ro' not in flags:
            raise ValueError('missing kernel read-only protection: ' + str(path))


def command_for(root, cell, mode, action):
    backup = action in ('init', 'backup')
    profile = root / ('backup.toml' if backup else 'empty.toml')
    repository = cell / ('backup-repo' if backup else 'repo-view')
    command = [str(root / 'bin/vaultic'), '--use-profile=' + str(profile),
               '--repo=' + str(repository), '--password-file=' + str(PASSWORD_FILE),
               '--cache-dir=' + str(cell / 'cache'), '--json']
    if backup or action == 'repository-check':
        command += ['--metadata-daemon-path=' + str(root / 'bin/vaulticdb'),
                    '--metadata-daemon-data-dir=' + str(cell / ('backup-db' if backup else 'index-db')),
                    '--metadata-daemon-socket=' + str(cell / 'runtime' / ('backup.sock' if backup else 'index.sock')),
                    '--metadata-daemon-object-store=local']
    if action == 'init':
        return command + ['init']
    if action == 'backup':
        command += ['backup']
    elif action == 'repository-check':
        command += ['check']
    else:
        command += ['index', 'import' if action == 'import' else 'check', '--start-daemon',
                    '--daemon-path=' + str(root / 'bin/vaulticdb'),
                    '--daemon-data-dir=' + str(cell / 'index-db'), '--daemon-object-store=local',
                    '--daemon-socket=' + str(cell / 'runtime/index.sock')]
        if action == 'import':
            command += ['--force-reset-old-idx', '--activate',
                        '--monitor-export-jsonl=' + str(cell / 'import-telemetry.jsonl')]
        else:
            command += ['--monitor-export-jsonl=' + str(cell / 'index-check-telemetry.jsonl')]
    if mode != 'pebble':
        command += ['--working-memory-mode=' + mode, '--working-memory-ram-limit=160GiB',
                    '--working-memory-scratch=' + str(cell / 'working')]
    return command


def import_scope(directory, expected_indexes, expected_snapshots):
    result = None
    with open(directory / 'stdout.log') as stream:
        for line in stream:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict) and 'indexes_total' in value:
                result = value
    if result is None:
        raise ValueError('missing structured full-import result')
    for prefix, expected in [('indexes', expected_indexes), ('snapshots', expected_snapshots)]:
        if result.get(prefix + '_total') != expected or result.get(prefix + '_seen') != expected:
            raise ValueError('full-import scope did not complete: ' + prefix)
    if result.get('errors') != 0:
        raise ValueError('full import reported source errors')
    return {key: value for key, value in result.items() if isinstance(value, (int, float, bool))}


def execute(root, mode, preflight=False):
    plan = load_plan(root)
    if mode not in plan['modes']:
        raise ValueError('unplanned mode')
    protected = [SOURCE_REPO, SOURCE_PROFILE, PASSWORD_FILE]
    protected += [Path(path) for path in plan['selection']['sources'] if Path(path).is_dir()]
    verify_read_only(protected)
    os.umask(0o077)
    cell = root / ('preflight-' + mode if preflight else mode)
    private_directory(cell)
    for directory in ('runtime', 'working', 'cache', 'home', 'tmp'):
        private_directory(cell / directory)
    shutil.copytree(root / 'source-view', cell / 'repo-view', symlinks=True)
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(('VAULTIC_', 'VAULTICDB_', 'RESTIC_', 'RUSTIC_'))
                   and key not in ('GOGC', 'GODEBUG', 'GOMEMLIMIT')}
    environment.update({'TMPDIR': str(cell / 'tmp'), 'HOME': str(cell / 'home'),
                        'XDG_CACHE_HOME': str(cell / 'cache'), 'PYTHONDONTWRITEBYTECODE': '1'})
    outcomes = []
    actions = ['import'] if preflight else ['import', 'index-check', 'repository-check', 'init', 'backup']
    for action in actions:
        if action in ('init', 'backup') and plan['backup_missing_sources']:
            raise ValueError('backup blocked; configured sources are unavailable, no subset is permitted')
        directory = cell / action
        private_directory(directory)
        command = command_for(root, cell, mode, action)
        if preflight:
            command = [argument for argument in command if argument not in ('--force-reset-old-idx', '--activate')]
            command += ['--dry-run', '--work-budget=1', '--snapshot-work-budget=1']
        seconds = 300 if preflight or action == 'init' else plan[
            'backup_timeout_seconds' if action == 'backup' else
            'import_timeout_seconds' if action == 'import' else 'check_timeout_seconds']
        print(json.dumps({'mode': mode, 'action': action, 'started': True, 'timeout_seconds': seconds}), flush=True)
        outcome = run_command(command, directory, seconds, environment,
                      disk_floor=plan['minimum_disk_free_bytes'])
        outcomes.append({'action': action, **outcome})
        print(json.dumps({'mode': mode, 'action': action, **{key: outcome[key] for key in
              ('exit_code', 'wall_seconds', 'peak_combined_rss_bytes', 'stop_reason', 'completed')}}), flush=True)
        if not outcome['completed']:
            write_json(cell / 'summary.json', {'mode': mode, 'complete': False, 'outcomes': outcomes})
            if preflight and outcome['exit_code'] == 2 and outcome['stop_reason'] is None:
                return
            raise ValueError('pilot stopped on incomplete or failed action: ' + action)
        if action == 'import' and not preflight:
            inventory = json.loads((root / 'source-inventory.json').read_text())
            counters = import_scope(directory,
                                    sum(entry['path'].startswith('index/') for entry in inventory),
                                    sum(entry['path'].startswith('snapshots/') for entry in inventory))
            write_json(directory / 'verified-scope.json', counters)
    write_json(cell / 'summary.json', {'mode': mode, 'complete': not preflight, 'outcomes': outcomes,
                                     'preflight_only': preflight})


class PilotTests(unittest.TestCase):
    def test_rss_exit_race_and_live_unknown(self):
        entry = Path('/proc/12345')
        missing_rss = 'Name:\tfixture\nState:\tR (running)\n'
        for state, flags in [('R', 4), ('Z', 0), ('X', 0), ('x', 0)]:
            with self.subTest(state=state, flags=flags), \
                    patch.object(Path, 'iterdir', return_value=iter([entry])), \
                    patch('os.getpgid', return_value=12345), \
                    patch.object(Path, 'read_text', side_effect=[missing_rss,
                                 f'12345 (fixture) {state} 1 12345 12345 0 -1 {flags}']):
                self.assertEqual(process_group_rss(12345), (0, 0))
        with patch.object(Path, 'iterdir', return_value=iter([entry])), \
                patch('os.getpgid', return_value=12345), \
                patch.object(Path, 'read_text', side_effect=[missing_rss,
                             '12345 (fixture) R 1 12345 12345 0 -1 0']):
            with self.assertRaisesRegex(ValueError, 'RSS unavailable'):
                process_group_rss(12345)

    def test_selection_fail_closed(self):
        config = {'backup': {'globs': ['!**/.snapshot', '!**/.snapshot/**'],
                             'snapshots': [{'sources': ['/source'], 'one-file-system': True}]}}
        self.assertEqual(source_selection(config)['sources'], ['/source'])
        config['backup']['snapshots'][0]['sources'] = ['/source/.snapshot/hourly']
        with self.assertRaises(ValueError):
            source_selection(config)

    def test_rss_failure_retains_result(self):
        with tempfile.TemporaryDirectory() as temporary, \
                patch(__name__ + '.process_group_rss', side_effect=ValueError('unknown RSS')):
            root = Path(temporary)
            result = run_command([sys.executable, '-c', 'import signal; signal.pause()'],
                                 root, 10, dict(os.environ))
            self.assertFalse(result['completed'])
            self.assertEqual(result['stop_reason'], 'RSS monitoring unavailable')
            self.assertIsNotNone(result['exit_code'])
            self.assertEqual(json.loads((root / 'result.json').read_text()), result)

    def test_view_preserves_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'source'
            source.mkdir()
            (source / 'config').write_bytes(b'config')
            for section in ('data', 'keys', 'index', 'snapshots'):
                (source / section).mkdir()
                (source / section / 'object').write_bytes(b'unchanged')
            view = root / 'view'
            inventory = make_view(source, view)
            self.assertEqual(len(inventory), 3)
            (view / 'locks' / 'private-lock').write_bytes(b'lock')
            self.assertFalse((source / 'locks').exists())
            (view / 'config').write_bytes(b'private config')
            self.assertEqual((source / 'config').read_bytes(), b'config')

    def test_timeout_and_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            success = root / 'success'
            success.mkdir()
            result = run_command([sys.executable, '-c', 'print("fixture")'], success, 10, dict(os.environ))
            self.assertTrue(result['completed'])
            timeout = root / 'timeout'
            timeout.mkdir()
            result = run_command([sys.executable, '-c', 'import signal; signal.pause()'], timeout, 0.1, dict(os.environ))
            self.assertEqual(result['stop_reason'], 'wall timeout exceeded')
            self.assertFalse(result['completed'])
            pressure = root / 'pressure'
            pressure.mkdir()
            result = run_command([sys.executable, '-c', 'import signal; signal.pause()'],
                                 pressure, 10, dict(os.environ), ceiling=1)
            self.assertEqual(result['stop_reason'], 'RSS ceiling exceeded')
            self.assertGreater(result['peak_combined_rss_bytes'], 1)
            self.assertFalse(result['completed'])
            disk = root / 'disk'
            disk.mkdir()
            result = run_command([sys.executable, '-c', 'import signal; signal.pause()'],
                                 disk, 10, dict(os.environ), disk_floor=shutil.disk_usage(root).total * 2)
            self.assertEqual(result['stop_reason'], 'shared NFS free-space reserve reached')
            self.assertFalse(result['completed'])

    def test_command_scope(self):
        root = Path('/private/pilot')
        cell = root / 'ram'
        backup = command_for(root, cell, 'ram', 'backup')
        self.assertIn('--use-profile=/private/pilot/backup.toml', backup)
        self.assertIn('--repo=/private/pilot/ram/backup-repo', backup)
        self.assertFalse(any('exclude=' in argument for argument in backup))
        imported = command_for(root, cell, 'pebble', 'import')
        self.assertIn('--repo=/private/pilot/ram/repo-view', imported)
        self.assertIn('--activate', imported)
        self.assertFalse(any('working-memory-mode=' in argument for argument in imported))
        self.assertNotIn('--repo=' + str(SOURCE_REPO), imported)

    def test_full_import_scope(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = {'indexes_total': 2, 'indexes_seen': 2, 'snapshots_total': 3,
                      'snapshots_seen': 3, 'errors': 0}
            (root / 'stdout.log').write_text(json.dumps(result) + '\n')
            self.assertEqual(import_scope(root, 2, 3)['errors'], 0)
            result['snapshots_seen'] = 2
            (root / 'stdout.log').write_text(json.dumps(result) + '\n')
            with self.assertRaises(ValueError):
                import_scope(root, 2, 3)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--selftest', action='store_true')
    parser.add_argument('--prepare', action='store_true')
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--daemon', type=Path)
    parser.add_argument('--run', type=Path)
    parser.add_argument('--mode', choices=['pebble', 'ram', 'kv', 'auto'])
    parser.add_argument('--preflight', action='store_true')
    args = parser.parse_args()
    if args.selftest:
        unittest.main(argv=[sys.argv[0]], exit=True)
    elif args.prepare and args.binary and args.daemon:
        print(prepare(args.binary, args.daemon))
    elif args.run and args.mode:
        execute(args.run, args.mode, args.preflight)
    else:
        parser.error('use --selftest or --prepare --binary PATH --daemon PATH')


if __name__ == '__main__':
    main()