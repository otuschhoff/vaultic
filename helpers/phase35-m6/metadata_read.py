from pathlib import Path
import argparse
import ctypes
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import statistics
import unittest

import pilot


WORKSPACE = Path('/root/proj/vaultic')
PREVIOUS = Path('/volume2/NASDA2/vaultic-test-m6/force-compare-fme-cae1-20261103-10k')
FIFTY = Path('/volume2/NASDA2/vaultic-test-m6/metadata-read-fme-cae1-20261103-50k-coalesced')
SCALE_ROOT = Path('/volume2/NASDA2/vaultic-test-m6/forced-scale-fme-cae1-20261104-200k-retry2')
SCALE_RUNTIME = Path('/run/vaultic-unchanged-scale200k-20261104-retry2')
SCALE_SOURCE = Path('/ncl1-1-vs-50/fme_cae1')
CHUNK_ROOT = Path('/volume2/NASDA2/vaultic-test-m6/chunk-compare-fme-cae1-20261104-200k')
CHUNK_BIN = WORKSPACE / 'bin/phase35-chunk200k/linux-amd64'
CHUNK_SCHEDULE = ('A', 'B', 'C', 'C', 'B', 'A', 'B', 'C', 'A', 'A', 'C', 'B', 'C', 'D', 'D', 'C')
ACCEPTED = Path('/volume2/NASDA2/vaultic-test-m6/critical-path-fme_cae1-20261102-production-cap100k/group-64')
SEED = Path('/ncl1-1-vs-50/fme_dump/amakura/db.begin-readiness-20261001-fuU56R')
CANDIDATE = WORKSPACE / 'bin/phase35-metadata-read/linux-amd64/vaultic'
NATIVE = CANDIDATE.with_name('vaulticdb')
FLOOR = 57481929687040
FIXTURE_CEILING = 110 << 30
RSS_CEILING = 200000000000
MARKER = b'Disposable Phase 35 throughput qualification.\n'
DB_OVERRIDES = {
    'db-control': {},
    'db-crypto32': {'crypto_threads': 32},
    'db-meta2g': {'meta_cache_bytes': 2 << 30},
    'db-block8g': {'block_cache_bytes': 8 << 30},
    'db-batch5ms': {'batch_delay_ns': 5000000},
    'db-rpc64': {'rpc_limit': 64},
    'db-control-end': {},
}
FOLLOWUP_OVERRIDES = {
    'db-scale200k': {'meta_cache_bytes': 2 << 30, 'readers': 128, 'publication_group_size': 16},
    'db-cache2g-start': {'meta_cache_bytes': 2 << 30},
    'db-cache2g-end': {'meta_cache_bytes': 2 << 30},
    'db-cache128': {},
    'db-cache256': {'meta_cache_bytes': 256 << 20},
    'db-cache512': {'meta_cache_bytes': 512 << 20},
    'db-cache1g': {'meta_cache_bytes': 1 << 30},
    'db-cache4g': {'meta_cache_bytes': 4 << 30},
    'db-group64': {'meta_cache_bytes': 2 << 30},
    'db-group64-end': {'meta_cache_bytes': 2 << 30},
    'db-group16': {'meta_cache_bytes': 2 << 30, 'publication_group_size': 16},
    'db-group32': {'meta_cache_bytes': 2 << 30, 'publication_group_size': 32},
    'db-hot-control': {'meta_cache_bytes': 2 << 30},
    'db-hot-control-end': {'meta_cache_bytes': 2 << 30},
    'db-hot-crypto16': {'meta_cache_bytes': 2 << 30, 'crypto_threads': 16},
    'db-hot-crypto32': {'meta_cache_bytes': 2 << 30, 'crypto_threads': 32},
    'db-hot-batch0ms': {'meta_cache_bytes': 2 << 30, 'batch_delay_ns': 0},
    'db-hot-batch2ms': {'meta_cache_bytes': 2 << 30, 'batch_delay_ns': 2000000},
    'db-hot-batch3ms': {'meta_cache_bytes': 2 << 30, 'batch_delay_ns': 3000000},
    'db-hot-batch5ms': {'meta_cache_bytes': 2 << 30, 'batch_delay_ns': 5000000},
    'db-read64-end': {'meta_cache_bytes': 2 << 30},
}
for readers in (64, 96, 128):
    for group in (16, 32, 64):
        FOLLOWUP_OVERRIDES[f'db-read{readers}-g{group}'] = {
            'meta_cache_bytes': 2 << 30, 'readers': readers, 'publication_group_size': group}
        if readers == 64:
            FOLLOWUP_OVERRIDES[f'db-read64-g{group}-end'] = dict(FOLLOWUP_OVERRIDES[f'db-read64-g{group}'])
DB_OVERRIDES.update(FOLLOWUP_OVERRIDES)


def binaries(profile):
    candidate = (WORKSPACE / 'bin/phase35-forced-followup/linux-amd64/vaultic'
                 if profile in FOLLOWUP_OVERRIDES else CANDIDATE)
    return candidate, candidate.with_name('vaulticdb')


def profile_settings(profile):
    if profile in DB_OVERRIDES:
        settings = {'readers': 64, 'rpc_limit': 32, 'meta_cache_bytes': 128 << 20,
                    'block_cache_bytes': 512 << 20, 'crypto_threads': 8, 'batch_delay_ns': 1000000,
                    'publication_group_size': 64}
        settings.update(DB_OVERRIDES[profile])
        return settings
    if profile not in ('baseline', 'rpcs', 'readers'):
        raise ValueError('unapproved tuning profile')
    return {'readers': 64 if profile == 'readers' else 16,
            'rpc_limit': 32 if profile in ('rpcs', 'readers') else 8}


def chunk_settings(variant):
    if variant not in ('A', 'B', 'C', 'D'):
        raise ValueError('unknown chunk variant')
    if variant == 'A':
        return {}
    return {'VAULTICDB_CHUNK_CACHE_BYTES': {'B': 0, 'C': 64 << 20, 'D': 256 << 20}[variant],
            'VAULTICDB_CHUNK_CACHE_ENTRIES': 4096, 'VAULTICDB_CHUNK_FETCH_LIMIT': 64,
            'VAULTICDB_CHUNK_FETCH_BYTES': 16 << 20, 'VAULTICDB_CHUNK_MAX_BYTES': 1 << 20,
            'VAULTICDB_CHUNK_RANGE_LIMIT': 4}


def chunk_paths(label):
    labels = [f'leg-{index:02d}-{variant}' for index, variant in enumerate(CHUNK_SCHEDULE, 1)]
    if label not in labels:
        raise ValueError('unknown scheduled chunk leg')
    return CHUNK_ROOT / label, Path('/run/vaultic-unchanged-chunk200k-' + label)


def observe_db_settings(group, settings, native=NATIVE, chunk_variant=None):
    observed = []
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if os.getpgid(int(entry.name)) != group or os.readlink(entry / 'exe') != str(native):
                continue
            values = dict(item.split(b'=', 1) for item in (entry / 'environ').read_bytes().split(b'\0') if b'=' in item)
            expected = {'VAULTICDB_CRYPTO_THREADS': settings['crypto_threads'],
                        'VAULTICDB_META_CACHE_BYTES': settings['meta_cache_bytes'],
                        'VAULTICDB_BLOCK_CACHE_BYTES': settings['block_cache_bytes']}
            if chunk_variant is not None:
                expected.update(chunk_settings(chunk_variant))
                if chunk_variant == 'A' and any(name.encode() in values for name in chunk_settings('C')):
                    raise ValueError('baseline unexpectedly inherited chunk configuration')
            if any(values.get(name.encode()) != str(value).encode() for name, value in expected.items()):
                raise ValueError('owned daemon tuning environment differs')
            workers = sum(task.joinpath('comm').read_text().strip().startswith('vaulticdb-crypt')
                          for task in entry.joinpath('task').iterdir())
            if workers == settings['crypto_threads']:
                observed.append({'pid': int(entry.name), 'crypto_threads': workers, **expected})
        except (FileNotFoundError, ProcessLookupError):
            continue
    return observed


def paths(mode, inode_limit=10000, profile=None):
    if mode not in ('parallel', 'coalesced', 'catalog'):
        raise ValueError('unknown metadata-read mode')
    if inode_limit == 200000:
        if mode != 'coalesced' or profile != 'db-scale200k':
            raise ValueError('200k requires its explicit forced-only scale profile')
        if SCALE_RUNTIME.parent != Path('/run') or not SCALE_RUNTIME.name.startswith('vaultic-unchanged-'):
            raise ValueError('200k requires the canonical private qualification runtime prefix')
        return SCALE_ROOT, SCALE_RUNTIME
    if profile == 'db-scale200k':
        raise ValueError('scale profile requires exactly 200000 entries')
    if inode_limit not in (10000, 50000) or (inode_limit == 50000 and mode != 'coalesced'):
        raise ValueError('expanded capture requires exactly 50000 coalesced entries')
    if profile is not None and (profile not in ('baseline', 'rpcs', 'readers', *DB_OVERRIDES) or inode_limit != 50000):
        raise ValueError('concurrency profile requires an allowlisted 50000-entry capture')
    label = str(inode_limit // 1000) + 'k'
    runtime_suffix = '' if inode_limit == 10000 else '-50k'
    suffix = '' if profile is None else '-forced-concurrency-' + profile
    if profile in DB_OVERRIDES:
        suffix = '-forced-db-' + profile.removeprefix('db-')
    runtime = Path('/run/vaultic-unchanged-metadata-read-' + mode + runtime_suffix + '-20261103')
    if profile is not None:
        runtime = Path('/run/vaultic-unchanged-force-50k-' + profile + '-20261103')
    if profile in DB_OVERRIDES:
        runtime = Path('/run/vaultic-unchanged-db50k-' + profile.removeprefix('db-') + '-20261103')
    return (Path('/volume2/NASDA2/vaultic-test-m6/metadata-read-fme-cae1-20261103-' + label + '-' + mode + suffix),
            runtime)


def fixture_ceiling(inode_limit):
    return FIXTURE_CEILING if inode_limit == 10000 else 350 << 30


def private_output(path, text=False):
    output = path.open('x' if text else 'xb')
    os.fchmod(output.fileno(), 0o600)
    return output


def prepare(mode, inode_limit=10000, profile=None):
    root, runtime = paths(mode, inode_limit, profile)
    if root.parent.resolve() != root.parent or root.exists() or runtime.exists():
        raise ValueError('refuse existing or noncanonical capture')
    if shutil.disk_usage(root.parent).free < FLOOR + fixture_ceiling(inode_limit):
        raise ValueError('fixture and free-space reserve unavailable')
    pilot.private_directory(root)
    pilot.private_directory(runtime)
    with private_output(root / '.nobackup') as output:
        output.write(MARKER)
    with private_output(root / 'source.patch') as output:
        subprocess.run(['git', '-C', str(WORKSPACE), 'diff', '--binary'], stdout=output, check=True)
    print(json.dumps({'root': str(root), 'runtime': str(runtime), 'prepared': True}))


def protect(profile=None, chunk=False):
    library = ctypes.CDLL(None, use_errno=True)
    protected = [SEED, ACCEPTED, Path('/volume2/NASDA2/rustic'), WORKSPACE, PREVIOUS / 'group-64', PREVIOUS / 'input']
    if profile is not None:
        protected.append(FIFTY)
    if profile == 'db-scale200k':
        protected.append(SCALE_SOURCE)
    if chunk:
        protected.append(SCALE_ROOT)
    for path in protected:
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


def execute(mode, inode_limit=10000, profile=None, chunk_leg=None):
    root, runtime = paths(mode, inode_limit, profile) if chunk_leg is None else chunk_paths(chunk_leg)
    candidate, native = binaries(profile)
    chunk_variant = None
    if chunk_leg is not None:
        if (mode, inode_limit, profile) != ('coalesced', 200000, 'db-scale200k'):
            raise ValueError('chunk comparison requires the fixed forced-only 200k profile')
        parent_namespace = os.environ.get('VAULTIC_CHUNK_PARENT_MOUNT_NS')
        if not parent_namespace or os.readlink('/proc/self/ns/mnt') == parent_namespace:
            raise ValueError('chunk leg must run in a distinct private mount namespace')
        chunk_variant = chunk_leg.rsplit('-', 1)[1]
        candidate = CHUNK_BIN / 'vaultic'
        native = CHUNK_BIN / ('old' if chunk_variant == 'A' else 'new') / 'vaulticdb'
    settings = profile_settings(profile) if profile is not None else None
    db_screen = profile in DB_OVERRIDES
    ceiling = fixture_ceiling(inode_limit)
    for path in (root, runtime):
        if path.resolve() != path or path.stat().st_mode & 0o777 != 0o700:
            raise ValueError('unsafe capture root')
    if (root / '.nobackup').read_bytes() != MARKER:
        raise ValueError('ownership marker differs')
    protect(profile, chunk=chunk_leg is not None)
    if chunk_leg is not None:
        subprocess.run(['sha256sum', '-c', 'FOLLOWUP-SHA256SUMS'], cwd=SCALE_ROOT, check=True)
    if profile is not None:
        subprocess.run(['sha256sum', '-c', 'METADATA-READ-SHA256SUMS'], cwd=FIFTY, check=True)
        retained = json.loads((FIFTY / 'launch-pins.json').read_text())
        expected = {pin['path']: pin['sha256'] for pin in retained['pins']}
        if any(pilot.digest(path) != expected[str(path)] for path in (CANDIDATE, NATIVE)):
            raise ValueError('concurrency screen comparison binaries differ from accepted 50k capture')
    pins = [{'path': str(path), 'sha256': pilot.digest(path)} for path in
            (Path(__file__), root / '.nobackup', root / 'qualifier.test', root / 'source.patch', candidate, native,
             ACCEPTED / 'result.json', PREVIOUS / 'group-64/result.json', WORKSPACE / 'cmd/vaultic/working_benchmark_test.go',
             WORKSPACE / 'helpers/phase35-m6/observe.py', Path(pilot.__file__))]
    if profile is not None:
        pins.extend({'path': str(path), 'sha256': pilot.digest(path)} for path in
                    (FIFTY / 'verified-observation.json', FIFTY / 'METADATA-READ-SHA256SUMS', FIFTY / 'group-64/result.json'))
    pilot.write_json(root / 'launch-pins.json', {'pins': pins, 'mode': mode, 'entries': inode_limit,
                     'concurrency_profile': profile,
                     'chunk_variant': chunk_variant, 'chunk_settings': None if chunk_variant is None else chunk_settings(chunk_variant),
                     'db_tuning': settings if db_screen else None,
                     'forced_only': profile is not None,
                     'read_concurrency': settings['readers'] if settings is not None else None,
                     'metadata_read_concurrency': settings['rpc_limit'] if settings is not None else None,
                     'rss_ceiling_bytes': RSS_CEILING, 'disk_floor_bytes': FLOOR, 'fixture_ceiling_bytes': ceiling,
                     'managed_ram_limit': '160GiB', 'scratch_limit': '2GiB', 'total_timeout_seconds': 28800,
                     'schedule': (['parent-clone', 'forced', 'restore', 'reopen'] if chunk_leg is not None else (['source-freeze', 'parent-setup', 'forced'] if inode_limit == 200000 else (['forced'] if profile is not None else (['unforced', 'forced'] if inode_limit == 10000 else ['parent-setup', 'unforced', 'forced'])))), 'production_repository_writable': False,
                     'source': str(SCALE_ROOT / 'input') if chunk_leg is not None else (str(root / 'input') if inode_limit == 200000 else (str(FIFTY / 'input') if profile is not None else (str(PREVIOUS / 'input') if inode_limit == 10000 else str(ACCEPTED / 'restore/ncl1-1-vs-50/fme_cae1')))), 'toolchains': {
                         name: subprocess.check_output(command, text=True).strip() for name, command in
                         [('go', ['go', 'version']), ('rust', ['rustc', '--version']), ('cargo', ['cargo', '--version'])]}})
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(('VAULTIC_', 'VAULTICDB_', 'RESTIC_', 'RUSTIC_'))
                   and key not in ('GOGC', 'GODEBUG', 'GOMEMLIMIT')}
    environment.update({'TMPDIR': str(runtime), 'VAULTIC_TEST_TMPDIR': str(runtime), 'PYTHONDONTWRITEBYTECODE': '1',
                        'VAULTIC_FEATURES': 'slatedb-authoritative=true', 'VAULTIC_PHASE35_METADATA_READ_COMPARE': mode,
                        'VAULTIC_PHASE35_THROUGHPUT_ROOT': str(root), 'VAULTIC_PHASE35_THROUGHPUT_FORCE_COMPARE_RETRY': '1',
                        'VAULTIC_PHASE35_THROUGHPUT_CONTROL_ROOT': str(runtime),
                        'VAULTIC_PHASE35_THROUGHPUT_CANDIDATE_SHA256': pilot.digest(candidate),
                        'VAULTIC_PHASE35_THROUGHPUT_NATIVE_SHA256': pilot.digest(native)})
    test_name = 'TestPublicationForceComparisonRetry'
    if profile is not None:
        environment.update({'VAULTIC_PHASE35_METADATA_READ_INODE_LIMIT': '50000',
                            'VAULTIC_PHASE35_METADATA_READ_FORCE_ONLY': '1',
                            'VAULTIC_PHASE35_METADATA_READ_CONCURRENCY_PROFILE': profile})
        if db_screen:
            environment['VAULTICDB_CRYPTO_THREADS'] = str(settings['crypto_threads'])
        if inode_limit == 200000:
            test_name = 'TestPublicationScale200kQualification'
            environment.pop('VAULTIC_PHASE35_METADATA_READ_INODE_LIMIT')
            environment.pop('VAULTIC_PHASE35_THROUGHPUT_FORCE_COMPARE_RETRY')
            environment.update({'VAULTIC_PHASE35_SCALE_INODE_LIMIT': '200000',
                                'VAULTIC_PHASE35_THROUGHPUT_ON_DEMAND': '1',
                                'VAULTIC_PHASE35_THROUGHPUT_ORDER': '64',
                                'VAULTIC_PHASE35_THROUGHPUT_SOURCE': str(root / 'input'),
                                'VAULTIC_PHASE35_THROUGHPUT_CANDIDATE': str(candidate),
                                'VAULTIC_PHASE35_THROUGHPUT_NATIVE': str(native)})
    elif inode_limit == 50000:
        test_name = 'TestPublicationBackupQualification'
        environment.pop('VAULTIC_PHASE35_THROUGHPUT_FORCE_COMPARE_RETRY')
        environment.update({'VAULTIC_PHASE35_METADATA_READ_INODE_LIMIT': '50000',
                            'VAULTIC_PHASE35_THROUGHPUT_UNCHANGED': '1', 'VAULTIC_PHASE35_THROUGHPUT_FORCE_COMPARE': '1',
                            'VAULTIC_PHASE35_THROUGHPUT_ON_DEMAND': '1', 'VAULTIC_PHASE35_THROUGHPUT_ORDER': '64',
                            'VAULTIC_PHASE35_THROUGHPUT_CANDIDATE': str(CANDIDATE), 'VAULTIC_PHASE35_THROUGHPUT_NATIVE': str(NATIVE)})
    if chunk_leg is not None:
        test_name = 'TestPublicationChunk200kComparison'
        environment['VAULTIC_PHASE35_CHUNK_VARIANT'] = chunk_variant
        environment.update({name: str(value) for name, value in chunk_settings(chunk_variant).items()})
    membership = next(line[3:] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
    cgroup = Path('/sys/fs/cgroup') / membership.lstrip('/')
    driver = root / 'driver'
    pilot.private_directory(driver)
    process = observer = None
    result = {'accepted': False, 'failure': None, 'peak_combined_rss_bytes': 0, 'entries': inode_limit, 'mode': mode,
              'concurrency_profile': profile, 'forced_only': profile is not None, 'db_settings_verified': False}
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
                process = subprocess.Popen([str(root / 'qualifier.test'), '-test.run=^' + test_name + '$', '-test.v', '-test.timeout=7h50m'],
                                           stdout=output, stderr=errors, env=environment, cwd=WORKSPACE / 'cmd/vaultic', start_new_session=True)
                last_storage = 0
                while True:
                    rss, count = pilot.process_group_rss(process.pid)
                    actual_settings = observe_db_settings(process.pid, settings, native, chunk_variant) if db_screen else []
                    if actual_settings:
                        result['db_settings_verified'] = True
                    elapsed = time.monotonic() - started
                    result['peak_combined_rss_bytes'] = max(result['peak_combined_rss_bytes'], rss)
                    samples.write(json.dumps({'elapsed_seconds': elapsed, 'rss_bytes': rss, 'processes': count,
                                              'db_settings': actual_settings}) + '\n')
                    samples.flush()
                    if elapsed - last_storage >= 30:
                        if storage_bytes(root) > ceiling:
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
                for name in (('forced',) if profile is not None else ('unforced', 'forced')):
                    measured = json.loads((root / 'group-64' / name / 'result.json').read_text())
                    if measured['entries'] != inode_limit or measured['summary']['data_blobs'] != 0 or not all(measured[key] is True for key in ('restore_verified', 'reopen_verified', 'source_unchanged')):
                        raise ValueError('candidate acceptance incomplete')
                    if profile is not None and (measured['forced'] is not True or (root / 'group-64/unforced').exists()):
                        raise ValueError('forced-only screen executed an unforced leg or lacks force semantics')
                if any(pilot.digest(Path(pin['path'])) != pin['sha256'] for pin in pins) or storage_bytes(root) > ceiling:
                    raise ValueError('pin or fixture ceiling differs')
                if db_screen and not result['db_settings_verified']:
                    raise ValueError('actual owned daemon tuning was not verified')
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


def summarize_followup(root, profile, inode_limit=50000, expected_candidate=None):
    outcome = json.loads((root / 'outcome.json').read_text())
    measured = json.loads((root / 'group-64/forced/result.json').read_text())
    settings = profile_settings(profile)
    if not outcome['accepted'] or not outcome['db_settings_verified'] or outcome['exit_code'] != 0:
        raise ValueError('follow-up capture or live settings were not accepted')
    expected_entries, expected_regular, expected_bytes = 50000, 41347, 19493496103
    expected_digest = '8f32883b02c37ae550a0375fa6abfe79ec52d830ef0d9cff1053522687a41e87'
    expected_parent = '60dfb46069fb4e18b603d69caf61dc3ca1db76099089ab18396985aa8c9f8e2b'
    if inode_limit == 200000:
        if profile != 'db-scale200k':
            raise ValueError('200k receipt requires its explicit scale profile')
        source = json.loads((root / 'source-observation.json').read_text())
        parent = json.loads((root / 'group-64/result.json').read_text())
        expected_entries, expected_regular, expected_bytes = source['entries'], source['regular_files'], source['bytes']
        expected_digest, expected_parent = source['sha256'], parent['snapshot']
        if expected_entries != 200000 or expected_bytes > 64 << 30 or not parent['restore_verified'] or not parent['reopen_verified']:
            raise ValueError('200k source dimensions or parent acceptance incomplete')
        if parent['input_sha256'] != expected_digest or parent['files'] != source['files'] or parent['bytes'] != expected_bytes:
            raise ValueError('200k parent source identity differs from the frozen source')
    elif inode_limit != 50000 or profile == 'db-scale200k':
        raise ValueError('unapproved follow-up receipt size')
    if measured['entries'] != expected_entries or measured['regular_files'] != expected_regular or measured['bytes'] != expected_bytes:
        raise ValueError('follow-up source dimensions differ')
    if measured['input_sha256'] != expected_digest:
        raise ValueError('follow-up source digest differs')
    if measured['baseline_parent'] != expected_parent:
        raise ValueError('follow-up parent differs')
    if measured['forced'] is not True or measured['summary']['data_blobs'] != 0 or (root / 'group-64/unforced').exists():
        raise ValueError('follow-up force or unchanged-content semantics differ')
    if not all(measured[name] is True for name in ('restore_verified', 'reopen_verified', 'source_unchanged')):
        raise ValueError('follow-up correctness incomplete')
    if measured['metadata_cache_bytes'] != settings['meta_cache_bytes'] or measured['block_cache_bytes'] != settings['block_cache_bytes']:
        raise ValueError('follow-up actual cache settings differ')
    if measured['crypto_threads_requested'] != str(settings['crypto_threads']):
        raise ValueError('follow-up actual crypto settings differ')
    expected_flags = [f"--read-concurrency={settings['readers']}",
                      f"--metadata-read-concurrency={settings['rpc_limit']}",
                      f"--metadata-batch-delay={settings['batch_delay_ns'] // 1000000}ms",
                      f"--metadata-publication-group-size={settings['publication_group_size']}"]
    if measured['metadata_read_flags'] != expected_flags or measured['candidate'] != str(expected_candidate or binaries(profile)[0]) or measured['engine_multi_get'] is not True:
        raise ValueError('follow-up binary, batching, concurrency or engine settings differ')
    before, after = measured['writer_before'], measured['writer_after']
    if before['process_started_unix_ms'] != after['process_started_unix_ms']:
        raise ValueError('follow-up native process changed during measurement')
    deltas = {}
    for field in ('cache_reads', 'encryption_reads'):
        first, last = before['attribution'][field], after['attribution'][field]
        if field == 'cache_reads':
            deltas[field] = {}
            for kind in ('block', 'index', 'filter', 'stats'):
                deltas[field][kind] = {}
                for name in ('hits', 'misses'):
                    if last[kind][name] < first[kind][name]:
                        raise ValueError('follow-up cache counter reset')
                    deltas[field][kind][name] = last[kind][name] - first[kind][name]
        else:
            if any(last[name] < first[name] for name in first):
                raise ValueError('follow-up encryption counter reset')
            deltas[field] = {name: last[name] - first[name] for name in first}
    critical = measured['critical_path']
    stages = critical['reconciliation']['publication_stages']
    if not isinstance(stages, dict) or set(stages) != {'planning', 'commit'}:
        raise ValueError('follow-up publication stage export missing')
    main = measured['main_seconds']
    if not main > 0:
        raise ValueError('follow-up main interval invalid')
    return {'profile': profile, 'settings': settings, 'main_seconds': main,
            'command_seconds': measured['command_seconds'], 'entries': expected_entries, 'entries_per_second': expected_entries / main,
            'cli_cpu_seconds': measured['cli_cpu_seconds'], 'publication_stages': stages,
            'reconciliation': critical['reconciliation'], 'deltas': deltas,
            'peak_combined_rss_bytes': outcome['peak_combined_rss_bytes'],
            'physical_nfs_traffic_measured': False, 'cache_evictions_measured': False,
            'single_pass_sustained_qualified': main >= 600, 'deployment_qualified': False}


def seal_followup(root, row):
    pilot.write_json(root / 'verified-followup.json', row)
    artifacts = sorted(path for directory in (root, root / 'driver', root / 'group-64', root / 'group-64/forced')
                       for path in directory.glob('*') if path.is_file())
    manifest = root / 'FOLLOWUP-SHA256SUMS'
    with private_output(manifest, text=True) as output:
        for path in artifacts:
            output.write(pilot.digest(path) + '  ' + str(path.relative_to(root)) + '\n')
    subprocess.run(['sha256sum', '-c', manifest.name], cwd=root, check=True)


def chunk_comparison_report(rows):
    if [row['variant'] for row in rows] != list(CHUNK_SCHEDULE):
        raise ValueError('chunk comparison schedule incomplete or reordered')
    for row in rows:
        if row['entries'] != 200000 or not math.isfinite(row['main_seconds']) or row['main_seconds'] <= 0:
            raise ValueError('chunk comparison requires positive exact 200k measurements')
    variants = {}
    for variant in ('A', 'B', 'C', 'D'):
        selected = [row for row in rows if row['variant'] == variant]
        variants[variant] = {'runs': len(selected),
                             'median_main_seconds': statistics.median(row['main_seconds'] for row in selected),
                             'median_entries_per_second': statistics.median(200000 / row['main_seconds'] for row in selected),
                             'min_main_seconds': min(row['main_seconds'] for row in selected),
                             'max_main_seconds': max(row['main_seconds'] for row in selected),
                             'median_peak_rss_bytes': statistics.median(row['peak_combined_rss_bytes'] for row in selected)}
    control = [row['main_seconds'] for row in rows[:12] if row['variant'] == 'A']
    baseline_spread = max(control) / min(control) - 1
    blocks = []
    for start in (0, 6):
        block = rows[start:start + 6]
        means = {variant: statistics.mean(row['main_seconds'] for row in block if row['variant'] == variant)
                 for variant in ('A', 'B', 'C')}
        blocks.append({'schedule': [row['variant'] for row in block], 'mean_main_seconds': means,
                       'coalescing_vs_old_throughput_gain': means['A'] / means['B'] - 1,
                       'retention_vs_coalescing_throughput_gain': means['B'] / means['C'] - 1,
                       'combined_vs_old_throughput_gain': means['A'] / means['C'] - 1})
    capacity = rows[12:]
    means = {variant: statistics.mean(row['main_seconds'] for row in capacity if row['variant'] == variant)
             for variant in ('C', 'D')}
    for row in rows:
        encryption = row['deltas']['encryption_reads']
        requested = encryption['requested_plaintext_bytes']
        row['origin_amplification'] = encryption['ciphertext_origin_bytes'] / requested if requested else None
        row['counter_window'] = 'writer status before backup through snapshot validation; wider than main phase'
    return {'accepted': True, 'schedule': list(CHUNK_SCHEDULE), 'rows': rows, 'variants': variants,
            'balanced_blocks': blocks, 'baseline_elapsed_spread_fraction': baseline_spread,
            'combined_throughput_supported_above_observed_baseline_spread': all(
                block['combined_vs_old_throughput_gain'] > baseline_spread for block in blocks),
            'capacity_bracket': {'schedule': ['C', 'D', 'D', 'C'], 'mean_main_seconds': means,
                                 'larger_cache_throughput_gain': means['C'] / means['D'] - 1},
            'physical_nfs_traffic_measured': False, 'native_cache_gauges_sampled': False,
            'single_pass_sustained_qualified': any(row['main_seconds'] >= 600 for row in rows),
            'formal_statistical_qualification': False, 'deployment_qualified': False,
            'production_modified': False, 'host_caches_dropped': False}


def execute_chunk_series():
    series = CHUNK_ROOT
    if series.exists() or series.parent.resolve() != series.parent:
        raise ValueError('refuse existing or noncanonical chunk comparison series')
    if shutil.disk_usage(series.parent).free < FLOOR + len(CHUNK_SCHEDULE) * fixture_ceiling(200000):
        raise ValueError('full series reservation plus shared free-space floor unavailable')
    pilot.private_directory(series)
    with private_output(series / '.nobackup') as output:
        output.write(MARKER)
    driver = CHUNK_BIN / 'qualifier.test'
    inputs = (driver, CHUNK_BIN / 'vaultic', CHUNK_BIN / 'old/vaulticdb', CHUNK_BIN / 'new/vaulticdb',
              CHUNK_BIN / 'build.json', Path(__file__), Path(pilot.__file__),
              WORKSPACE / 'helpers/phase35-m6/observe.py', WORKSPACE / 'cmd/vaultic/working_benchmark_test.go',
              SCALE_ROOT / 'FOLLOWUP-SHA256SUMS', SCALE_ROOT / 'group-64/result.json', SCALE_ROOT / 'source-observation.json')
    pins = {str(path): pilot.digest(path) for path in inputs}
    pilot.write_json(series / 'launch-pins.json', {'pins': pins, 'schedule': list(CHUNK_SCHEDULE),
                     'parent': str(SCALE_ROOT), 'source_entries': 200000, 'production_modified': False,
                     'baseline_revision': '01835ca98b1b66d48f176b719fffa8f89ac80c67',
                     'candidate_revision': '931e3a5ef29c632fa483e304abe7b3d1a842865b',
                     'single_pass_sustained_required_seconds': 600,
                     'process_cache_state': 'fresh daemon; identical startup, no cross-leg retained cache',
                     'host_cache_state': 'uncontrolled but unchanged; balanced leg ordering',
                     'larger_cache_bracket_bytes': 256 << 20})
    rows = []
    try:
        for index, variant in enumerate(CHUNK_SCHEDULE, 1):
            if any(pilot.digest(Path(path)) != digest for path, digest in pins.items()):
                raise ValueError('chunk series executable, fixture receipt or supervisor pin changed')
            label = f'leg-{index:02d}-{variant}'
            root, runtime = chunk_paths(label)
            if root.exists() or runtime.exists():
                raise ValueError('refuse existing chunk leg or runtime')
            pilot.private_directory(root)
            pilot.private_directory(runtime)
            with private_output(root / '.nobackup') as output:
                output.write(MARKER)
            with private_output(root / 'source.patch') as output:
                subprocess.run(['git', '-C', str(WORKSPACE), 'diff', '--binary'], stdout=output, check=True)
            for source, target in ((driver, root / 'qualifier.test'),
                                   (SCALE_ROOT / 'source-observation.json', root / 'source-observation.json')):
                shutil.copyfile(source, target)
                os.chmod(target, 0o500 if target.name == 'qualifier.test' else 0o600)
            environment = dict(os.environ, VAULTIC_CHUNK_PARENT_MOUNT_NS=os.readlink('/proc/self/ns/mnt'))
            command = ['timeout', '--signal=INT', '--kill-after=150s', '29100s',
                       'unshare', '--mount', '--propagation', 'private', sys.executable, str(Path(__file__)),
                       '--mode=coalesced', '--inode-limit=200000', '--concurrency-profile=db-scale200k', '--chunk-leg=' + label]
            print(json.dumps({'starting': label, 'root': str(root), 'chunk_settings': chunk_settings(variant)}), flush=True)
            with private_output(series / (label + '.stdout.log')) as output, private_output(series / (label + '.stderr.log')) as errors:
                process = subprocess.Popen(command, stdout=output, stderr=errors, env=environment, start_new_session=True)
                try:
                    status = process.wait()
                    if status:
                        raise subprocess.CalledProcessError(status, command)
                except BaseException:
                    if process.poll() is None:
                        process.send_signal(signal.SIGINT)
                        try:
                            process.wait(timeout=150)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=30)
                            raise ValueError('chunk supervisor required kill fallback; capture rejected')
                    raise
            row = summarize_followup(root, 'db-scale200k', 200000, CHUNK_BIN / 'vaultic')
            row.update({'variant': variant, 'leg': label, 'chunk_settings': chunk_settings(variant)})
            row['cache_evictions_measured'] = variant != 'A'
            if variant != 'A' and row['deltas']['encryption_reads']['header_validation_calls'] <= 0:
                raise ValueError('new chunk read path was not exercised')
            seal_followup(root, row)
            rows.append(row)
            shutil.rmtree(runtime)
            print(json.dumps({'accepted': label, 'main_seconds': row['main_seconds'],
                              'entries_per_second': row['entries_per_second'],
                              'encryption': row['deltas']['encryption_reads']}), flush=True)
        report = chunk_comparison_report(rows)
        pilot.write_json(series / 'verified-series.json', report)
        with private_output(series / 'SERIES-SHA256SUMS', text=True) as output:
            artifacts = sorted(path for path in series.iterdir() if path.is_file() and path.name != 'SERIES-SHA256SUMS')
            artifacts.extend(series / row['leg'] / 'FOLLOWUP-SHA256SUMS' for row in rows)
            for path in artifacts:
                output.write(pilot.digest(path) + '  ' + str(path.relative_to(series)) + '\n')
        subprocess.run(['sha256sum', '-c', 'SERIES-SHA256SUMS'], cwd=series, check=True)
        print(json.dumps({'accepted': True, 'variants': report['variants'], 'balanced_blocks': report['balanced_blocks'],
                          'capacity_bracket': report['capacity_bracket'],
                          'single_pass_sustained_qualified': report['single_pass_sustained_qualified']}), flush=True)
    except BaseException as error:
        pilot.write_json(series / 'failed-series.json', {'accepted': False, 'completed_legs': [row['leg'] for row in rows],
                         'failure': type(error).__name__ + ': ' + str(error)})
        raise


class ChunkComparisonTests(unittest.TestCase):
    def test_schedule_and_routes(self):
        for offset in (0, 6):
            self.assertEqual({variant: CHUNK_SCHEDULE[offset:offset + 6].count(variant) for variant in 'ABC'},
                             {'A': 2, 'B': 2, 'C': 2})
        self.assertNotEqual(CHUNK_SCHEDULE[:6], CHUNK_SCHEDULE[6:12])
        for index, variant in enumerate(CHUNK_SCHEDULE, 1):
            root, runtime = chunk_paths(f'leg-{index:02d}-{variant}')
            self.assertEqual(root.parent, CHUNK_ROOT)
            self.assertEqual(runtime.parent, Path('/run'))
        for label in ('leg-01-C', 'leg-17-C', '../retry', 'leg-03-C/retry'):
            with self.assertRaises(ValueError):
                chunk_paths(label)

    def test_variant_isolation(self):
        self.assertEqual(chunk_settings('A'), {})
        self.assertEqual(chunk_settings('B')['VAULTICDB_CHUNK_CACHE_BYTES'], 0)
        for variant in ('B', 'D'):
            self.assertEqual([name for name in chunk_settings('C') if chunk_settings(variant)[name] != chunk_settings('C')[name]],
                             ['VAULTICDB_CHUNK_CACHE_BYTES'])
        with self.assertRaises(ValueError):
            chunk_settings('unknown')

    def test_analysis_rejects_reordering_and_short_gates(self):
        rows = [{'variant': variant, 'entries': 200000, 'main_seconds': {'A': 220, 'B': 210, 'C': 180, 'D': 175}[variant],
                 'peak_combined_rss_bytes': 1000000000,
                 'deltas': {'encryption_reads': {'requested_plaintext_bytes': 100, 'ciphertext_origin_bytes': 200}}}
                for variant in CHUNK_SCHEDULE]
        report = chunk_comparison_report(rows)
        self.assertFalse(report['single_pass_sustained_qualified'])
        self.assertTrue(report['combined_throughput_supported_above_observed_baseline_spread'])
        self.assertEqual(report['variants']['A']['runs'], 4)
        self.assertGreater(report['capacity_bracket']['larger_cache_throughput_gain'], 0)
        with self.assertRaises(ValueError):
            chunk_comparison_report(rows[:-1])
        rows[0], rows[1] = rows[1], rows[0]
        with self.assertRaises(ValueError):
            chunk_comparison_report(rows)


def execute_series():
    series = Path('/volume2/NASDA2/vaultic-test-m6/forced-followup-fme-cae1-20261103')
    if series.exists() or series.parent.resolve() != series.parent:
        raise ValueError('refuse existing or noncanonical follow-up series')
    pilot.private_directory(series)
    driver = WORKSPACE / 'bin/phase35-forced-followup/linux-amd64/qualifier.test'
    pinned = {str(path): pilot.digest(path) for path in (driver, *binaries('db-cache2g-start'), Path(__file__))}
    pilot.write_json(series / 'launch-pins.json', {'pins': pinned, 'production_modified': False,
                     'source_entries': 50000, 'single_pass_sustained_required_seconds': 600})
    rows = []

    def cell(profile):
        if any(pilot.digest(Path(path)) != digest for path, digest in pinned.items()):
            raise ValueError('follow-up series binary or supervisor pin changed')
        prepare('coalesced', 50000, profile)
        root, runtime = paths('coalesced', 50000, profile)
        shutil.copyfile(driver, root / 'qualifier.test')
        os.chmod(root / 'qualifier.test', 0o500)
        command = ['unshare', '--mount', '--propagation', 'private', sys.executable, str(Path(__file__)),
                   '--mode=coalesced', '--inode-limit=50000', '--concurrency-profile=' + profile]
        with private_output(series / (profile + '.stdout.log')) as output, private_output(series / (profile + '.stderr.log')) as errors:
            process = subprocess.Popen(command, stdout=output, stderr=errors, start_new_session=True)
            try:
                status = process.wait()
                if status:
                    raise subprocess.CalledProcessError(status, command)
            except BaseException:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    try:
                        process.wait(timeout=150)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=30)
                        raise ValueError('series supervisor required kill fallback; preserve rejected capture')
                raise
        row = summarize_followup(root, profile)
        seal_followup(root, row)
        rows.append(row)
        pilot.write_json(series / (profile + '.json'), row)
        shutil.rmtree(runtime)
        print(json.dumps({'profile': profile, 'accepted': True, 'main_seconds': row['main_seconds'],
                          'entries_per_second': row['entries_per_second']}), flush=True)
        return row

    try:
        for profile in ('db-cache2g-start', 'db-cache128', 'db-cache256', 'db-cache512', 'db-cache1g', 'db-cache4g', 'db-cache2g-end'):
            cell(profile)
        groups = {}
        for profile in ('db-group64', 'db-group16', 'db-group32', 'db-group64-end'):
            groups[profile] = cell(profile)
        control = (groups['db-group64']['main_seconds'] + groups['db-group64-end']['main_seconds']) / 2
        candidates = {64: control, 16: groups['db-group16']['main_seconds'], 32: groups['db-group32']['main_seconds']}
        best_group = min(candidates, key=candidates.get)
        if candidates[best_group] >= control * 0.95:
            best_group = 64
        pilot.write_json(series / 'publication-selection.json', {'selected_group': best_group,
                         'main_seconds': candidates, 'minimum_screen_improvement': 0.05, 'causally_qualified': False})
        for profile in ('db-hot-control', 'db-hot-crypto16', 'db-hot-crypto32', 'db-hot-batch0ms',
                        'db-hot-batch2ms', 'db-hot-batch3ms', 'db-hot-batch5ms', 'db-hot-control-end'):
            cell(profile)
        for profile in (f'db-read64-g{best_group}', f'db-read96-g{best_group}',
                        f'db-read128-g{best_group}', f'db-read64-g{best_group}-end'):
            cell(profile)
        pilot.write_json(series / 'verified-series.json', {'accepted': True, 'rows': rows,
                         'selected_publication_group': best_group, 'deployment_qualified': False,
                         'single_pass_sustained_qualified': any(row['single_pass_sustained_qualified'] for row in rows),
                         'causally_qualified': False, 'schedule': [row['profile'] for row in rows]})
    except BaseException as error:
        pilot.write_json(series / 'failed-series.json', {'accepted': False, 'completed_profiles': [row['profile'] for row in rows],
                         'failure': type(error).__name__ + ': ' + str(error)})
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', required=True, choices=('parallel', 'coalesced', 'catalog'))
    parser.add_argument('--inode-limit', type=int, choices=(10000, 50000, 200000), default=10000)
    parser.add_argument('--concurrency-profile', choices=('baseline', 'rpcs', 'readers', *DB_OVERRIDES))
    parser.add_argument('--prepare', action='store_true')
    parser.add_argument('--series', action='store_true')
    parser.add_argument('--chunk-series', action='store_true')
    parser.add_argument('--chunk-self-test', action='store_true')
    parser.add_argument('--chunk-leg')
    arguments = parser.parse_args()
    os.umask(0o077)
    if arguments.chunk_self_test:
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ChunkComparisonTests))
        raise SystemExit(0 if result.wasSuccessful() else 1)
    elif arguments.chunk_series or arguments.chunk_leg is not None:
        if (arguments.mode, arguments.inode_limit, arguments.concurrency_profile) != ('coalesced', 200000, 'db-scale200k') or arguments.prepare or arguments.series:
            raise ValueError('chunk series requires its fixed forced-only 200k routing')
        if arguments.chunk_series:
            if arguments.chunk_leg is not None:
                raise ValueError('cannot select series and individual leg together')
            execute_chunk_series()
        else:
            raise SystemExit(execute(arguments.mode, arguments.inode_limit, arguments.concurrency_profile, arguments.chunk_leg))
    elif arguments.series:
        if arguments.mode != 'coalesced' or arguments.inode_limit != 50000 or arguments.prepare or arguments.concurrency_profile is not None:
            raise ValueError('series requires forced-only 50000-entry coalesced routing')
        execute_series()
    elif arguments.prepare:
        prepare(arguments.mode, arguments.inode_limit, arguments.concurrency_profile)
    else:
        status = execute(arguments.mode, arguments.inode_limit, arguments.concurrency_profile)
        if status == 0 and arguments.inode_limit == 200000:
            root, runtime = paths(arguments.mode, arguments.inode_limit, arguments.concurrency_profile)
            try:
                row = summarize_followup(root, arguments.concurrency_profile, arguments.inode_limit)
                seal_followup(root, row)
                shutil.rmtree(runtime)
                print(json.dumps({'accepted': True, 'entries': 200000, 'main_seconds': row['main_seconds'],
                                  'entries_per_second': row['entries_per_second'],
                                  'single_pass_sustained_qualified': row['single_pass_sustained_qualified']}), flush=True)
            except BaseException as error:
                pilot.write_json(root / 'analysis-failure.json', {'accepted': False, 'failure': type(error).__name__ + ': ' + str(error)})
                raise
        raise SystemExit(status)