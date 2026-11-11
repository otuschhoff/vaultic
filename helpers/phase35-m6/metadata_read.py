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
import tempfile
import unittest
from unittest.mock import patch

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
IMMUTABLE_ROOT = Path('/volume2/NASDA2/vaultic-test-m6/immutable-default-fme-cae1-20261105-200k-retry1')
IMMUTABLE_BIN = WORKSPACE / 'bin/phase35-immutable200k-retry1/linux-amd64'
RECONCILE_PARENT = Path('/volume2/NASDA2/vaultic-test-m6/reconcile-parent-fme-cae1-20261105-400k')
RECONCILE_ROOT = Path('/volume2/NASDA2/vaultic-test-m6/reconcile-read-fme-cae1-20261105-400k')
RECONCILE_BIN = WORKSPACE / 'bin/phase35-reconcile400k/linux-amd64'
RECONCILE_SCHEDULE = ('J', 'K', 'K', 'J', 'K', 'L', 'L', 'K', 'K', 'M', 'M', 'K')
CACHE_ROOT = Path('/volume2/NASDA2/vaultic-test-m6/cache-read-fme-cae1-20261107-400k-retry4')
CACHE_BIN = WORKSPACE / 'bin/phase35-cache400k-retry4-guard600/linux-amd64'
CACHE_PARENT = Path('/volume2/NASDA2/vaultic-test-m6/cache-parent-fme-cae1-20261107-400k')
CACHE_VARIANTS = {'N': (512 << 20, 64 << 20), 'O': (2 << 30, 64 << 20),
                  'P': (8 << 30, 64 << 20), 'Q': (512 << 20, 512 << 20),
                  'R': (512 << 20, 2 << 30), 'S': (512 << 20, 4 << 30)}
CACHE_SCHEDULE = ('N', 'O', 'O', 'N', 'N', 'P', 'P', 'N', 'N', 'Q', 'Q', 'N',
                  'N', 'R', 'R', 'N', 'N', 'S', 'S', 'N')
PIPELINE_ROOT = Path('/volume2/NASDA2/vaultic-test-m6/pipeline-read-fme-cae1-20261108-400k')
PIPELINE_BIN = WORKSPACE / 'bin/phase35-pipeline400k-20261108/linux-amd64'
PIPELINE_VARIANTS = {variant: (512 << 20, 64 << 20) for variant in ('T', 'U', 'V', 'W', 'X')}
PIPELINE_VARIANTS['Y'] = (8 << 30, 4 << 30)
PIPELINE_SCHEDULE = ('T', 'U', 'U', 'T', 'U', 'V', 'V', 'U', 'V', 'W', 'W', 'V',
                     'W', 'X', 'X', 'W', 'X', 'Y', 'Y', 'X')
CACHE_VARIANTS.update(PIPELINE_VARIANTS)
RAM16_ROOT = Path('/volume2/NASDA2/vaultic-test-m6/ram16-read-fme-cae1-20261109-400k')
RAM16_BIN = WORKSPACE / 'bin/phase35-ram16400k-20261109/linux-amd64'
RAM16_VARIANTS = {'Z': (512 << 20, 4 << 30), 'AA': (512 << 20, 16 << 30)}
RAM16_SCHEDULE = ('Z', 'AA', 'AA', 'Z', 'AA', 'Z', 'Z', 'AA')
CACHE_VARIANTS.update(RAM16_VARIANTS)
SCALE800K_PARENT = Path('/volume2/NASDA2/vaultic-test-m6/bottleneck-parent-fme-cae1-20261109-800k-retry2')
SCALE800K_ROOT = Path('/volume2/NASDA2/vaultic-test-m6/bottleneck-parent-fme-cae1-20261109-800k-retry3')
SCALE800K_BIN = WORKSPACE / 'bin/phase35-bottleneck800k-20261109-retry3-final/linux-amd64'
SCALE800K_RUNTIME = Path('/run/vaultic-unchanged-bottleneck800k-20261109-retry3')
SCALE800K_CHUNKS = {'VAULTICDB_CHUNK_CACHE_BYTES': 16 << 30, 'VAULTICDB_CHUNK_CACHE_ENTRIES': 65536}
RAM32_ROOT = Path('/volume2/NASDA2/vaultic-test-m6/ram32-read-fme-cae1-20261110-800k')
RAM32_BIN = WORKSPACE / 'bin/phase35-ram32800k-20261110/linux-amd64'
RAM32_SCHEDULE = (16, 32, 32, 16, 32, 16, 16, 32)
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
    'db-scale800k': {'meta_cache_bytes': 2 << 30, 'readers': 128, 'publication_group_size': 64},
    'db-scale400k': {'meta_cache_bytes': 2 << 30, 'readers': 128, 'publication_group_size': 16},
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


def ram32_leg(label):
    for index, gib in enumerate(RAM32_SCHEDULE, 1):
        if label == f'leg-{index:02d}-{gib}g':
            return RAM32_ROOT / label, Path('/run/vaultic-unchanged-ram32800k-' + label), {
                'VAULTICDB_CHUNK_CACHE_BYTES': gib << 30,
                'VAULTICDB_CHUNK_CACHE_ENTRIES': gib * 4096}
    raise ValueError('RAM32 comparison requires its exact scheduled private leg')


def scale800k_chunks():
    label = os.environ.get('VAULTIC_PHASE35_RAM32_LEG')
    return ram32_leg(label)[2] if label else dict(SCALE800K_CHUNKS)


def binaries(profile):
    if os.environ.get('VAULTIC_PHASE35_RAM32_LEG'):
        ram32_leg(os.environ['VAULTIC_PHASE35_RAM32_LEG'])
        if profile != 'db-scale800k':
            raise ValueError('RAM32 comparison requires the exact 800k profile')
        return RAM32_BIN / 'vaultic', RAM32_BIN / 'vaulticdb'
    if profile == 'db-scale800k':
        return SCALE800K_BIN / 'vaultic', SCALE800K_BIN / 'vaulticdb'
    if profile == 'db-scale400k':
        return RECONCILE_BIN / 'vaultic', RECONCILE_BIN / 'vaulticdb'
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
    if variant in CACHE_VARIANTS:
        return dict(chunk_settings('I'), VAULTICDB_CHUNK_CACHE_BYTES=CACHE_VARIANTS[variant][1],
                    VAULTICDB_CHUNK_CACHE_ENTRIES=65536 if variant in RAM16_VARIANTS else 16384)
    if variant in ('J', 'K', 'L', 'M'):
        return chunk_settings('I')
    if variant not in ('A', 'B', 'C', 'D', 'I'):
        raise ValueError('unknown chunk variant')
    if variant == 'A':
        return {}
    settings = {'VAULTICDB_CHUNK_CACHE_BYTES': {'B': 0, 'C': 64 << 20, 'D': 256 << 20, 'I': 64 << 20}[variant],
            'VAULTICDB_CHUNK_CACHE_ENTRIES': 4096, 'VAULTICDB_CHUNK_FETCH_LIMIT': 64,
            'VAULTICDB_CHUNK_FETCH_BYTES': 16 << 20, 'VAULTICDB_CHUNK_MAX_BYTES': 1 << 20,
            'VAULTICDB_CHUNK_RANGE_LIMIT': 4}
    if variant == 'I':
        settings.update({'VAULTICDB_BACKEND_ACCESS_MODE': 'dedicated-force', 'VAULTICDB_SST_FRESHNESS': 'immutable'})
    return settings


def chunk_paths(label):
    if label in [f'leg-{index:02d}-{variant}' for index, variant in enumerate(RAM16_SCHEDULE, 1)]:
        return RAM16_ROOT / label, Path('/run/vaultic-unchanged-ram16400k-' + label)
    if label in [f'leg-{index:02d}-{variant}' for index, variant in enumerate(PIPELINE_SCHEDULE, 1)]:
        return PIPELINE_ROOT / label, Path('/run/vaultic-unchanged-pipeline400k-' + label)
    if label in [f'leg-{index:02d}-{variant}' for index, variant in enumerate(CACHE_SCHEDULE, 1)]:
        return CACHE_ROOT / label, Path('/run/vaultic-unchanged-cache400k-retry4-' + label)
    if label in [f'leg-{index:02d}-{variant}' for index, variant in enumerate(RECONCILE_SCHEDULE, 1)]:
        return RECONCILE_ROOT / label, Path('/run/vaultic-unchanged-reconcile400k-' + label)
    if label in ('leg-01-I', 'leg-02-I', 'leg-03-I'):
        return IMMUTABLE_ROOT / label, Path('/run/vaultic-unchanged-immutable200k-retry1-' + label)
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
            if settings.get('sample_entries') == 800000:
                expected.update(scale800k_chunks())
            if any(values.get(name.encode()) != str(value).encode() for name, value in expected.items()):
                raise ValueError('owned daemon tuning environment differs')
            workers = sum(task.joinpath('comm').read_text().strip().startswith('vaulticdb-crypt')
                          for task in entry.joinpath('task').iterdir())
            if workers == settings['crypto_threads']:
                observed.append({'pid': int(entry.name), 'crypto_threads': workers, **expected})
        except (FileNotFoundError, ProcessLookupError):
            continue
    return observed


def cache_profile_settings(profile, variant=None):
    settings = profile_settings(profile)
    if profile == 'db-scale800k':
        settings['sample_entries'] = 800000
    if variant in CACHE_VARIANTS:
        if profile != 'db-scale400k':
            raise ValueError('cache variant requires the exact 400k profile')
        settings['block_cache_bytes'] = CACHE_VARIANTS[variant][0]
        if variant in RAM16_VARIANTS:
            settings.update(publication_group_size=64, on_demand_cache_bytes=64 << 20)
        if variant in PIPELINE_VARIANTS:
            settings['publication_group_size'] = 16 if variant in ('T', 'U') else 64
            settings['on_demand_cache_bytes'] = 8 << 30 if variant == 'Y' else 64 << 20
            if variant == 'Y':
                settings['meta_cache_bytes'] = 8 << 30
    return settings


def paths(mode, inode_limit=10000, profile=None):
    if mode not in ('parallel', 'coalesced', 'catalog'):
        raise ValueError('unknown metadata-read mode')
    if inode_limit == 800000:
        if (mode, profile) != ('coalesced', 'db-scale800k'):
            raise ValueError('800k requires its explicit forced-only scale profile')
        if os.environ.get('VAULTIC_PHASE35_RAM32_LEG'):
            return ram32_leg(os.environ['VAULTIC_PHASE35_RAM32_LEG'])[:2]
        return SCALE800K_ROOT, SCALE800K_RUNTIME
    if os.environ.get('VAULTIC_PHASE35_RAM32_LEG'):
        raise ValueError('RAM32 comparison requires exactly 800000 entries')
    if profile == 'db-scale800k':
        raise ValueError('800k profile requires exactly 800000 entries')
    if inode_limit == 400000:
        if (mode, profile) != ('coalesced', 'db-scale400k'):
            raise ValueError('400k requires its explicit forced-only scale profile')
        return RECONCILE_PARENT, Path('/run/vaultic-unchanged-scale400k-20261105')
    if profile == 'db-scale400k':
        raise ValueError('400k profile requires exactly 400000 entries')
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


def protect(profile=None, chunk=False, cache=False, extra_roots=()):
    library = ctypes.CDLL(None, use_errno=True)
    protected = [SEED, ACCEPTED, Path('/volume2/NASDA2/rustic'), WORKSPACE, PREVIOUS / 'group-64', PREVIOUS / 'input']
    if profile is not None:
        protected.append(FIFTY)
    if profile in ('db-scale200k', 'db-scale400k', 'db-scale800k'):
        protected.append(SCALE_SOURCE)
    if chunk:
        protected.append(RECONCILE_PARENT if profile == 'db-scale400k' else SCALE_ROOT)
    if cache:
        protected.append(CACHE_PARENT)
    protected.extend(extra_roots)
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
    if root.resolve() != root or not root.is_dir():
        raise ValueError('storage scope must be an existing canonical directory')
    total = 0
    for directory, _, files in os.walk(root):
        for name in files:
            try:
                total += (Path(directory) / name).lstat().st_size
            except FileNotFoundError:
                pass
    return total


class BackupRuntimeGuard:
    def __init__(self, limit=600):
        self.limit = limit
        self.started = None
        self.finished = False

    def observe(self, now, launched, finished_seconds=None):
        if self.finished:
            return
        if launched and self.started is None:
            self.started = now
        if finished_seconds is not None:
            if self.started is None or not math.isfinite(finished_seconds) or finished_seconds < 0:
                raise ValueError('invalid backup runtime completion receipt')
            if finished_seconds > self.limit:
                raise ValueError(f'backup runtime exceeded the {self.limit}-second limit; stop owned process group')
            self.finished = True
        elif self.started is not None and now - self.started >= self.limit:
            raise ValueError(f'backup runtime exceeded the {self.limit}-second limit; stop owned process group')


def runtime_limits(inode_limit):
    return (1200, 57600, '15h40m') if inode_limit == 800000 else (600, 28800, '7h50m')


def validate_cache_progress(event):
    if event.get('message_type') not in ('reconciliation_progress', 'reconciliation_stats'):
        return False
    metrics = event['reconciliation'] if event['message_type'] == 'reconciliation_progress' else event
    for name in ('changed', 'failed', 'deferred', 'revision_allocation_calls', 'atomic_group_calls', 'directory_publication_calls'):
        if metrics[name] != 0:
            raise ValueError('cache reuse workload rejected during execution: ' + name)
    return True


def execute(mode, inode_limit=10000, profile=None, chunk_leg=None):
    root, runtime = paths(mode, inode_limit, profile) if chunk_leg is None else chunk_paths(chunk_leg)
    candidate, native = binaries(profile)
    chunk_variant = None
    backup_limit, total_limit, test_timeout = runtime_limits(inode_limit)
    if chunk_leg is not None:
        expected_profile = 'db-scale400k' if inode_limit == 400000 else 'db-scale200k'
        if mode != 'coalesced' or profile != expected_profile or inode_limit not in (200000, 400000):
            raise ValueError('chunk comparison requires its fixed forced-only scale profile')
        parent_namespace = os.environ.get('VAULTIC_CHUNK_PARENT_MOUNT_NS')
        if not parent_namespace or os.readlink('/proc/self/ns/mnt') == parent_namespace:
            raise ValueError('chunk leg must run in a distinct private mount namespace')
        chunk_variant = chunk_leg.rsplit('-', 1)[1]
        if (chunk_variant in ('J', 'K', 'L', 'M', *CACHE_VARIANTS)) != (inode_limit == 400000):
            raise ValueError('chunk variant and source scope differ')
        bin_root = RAM16_BIN if chunk_variant in RAM16_VARIANTS else (PIPELINE_BIN if chunk_variant in PIPELINE_VARIANTS else (CACHE_BIN if chunk_variant in CACHE_VARIANTS else (RECONCILE_BIN if inode_limit == 400000 else (IMMUTABLE_BIN if chunk_variant == 'I' else CHUNK_BIN))))
        candidate = bin_root / ('control/vaultic' if chunk_variant == 'T' else 'vaultic')
        native = bin_root / ('old' if chunk_variant == 'A' else 'new') / 'vaulticdb'
    settings = cache_profile_settings(profile, chunk_variant) if profile is not None else None
    db_screen = profile in DB_OVERRIDES
    ceiling = fixture_ceiling(inode_limit)
    for path in (root, runtime):
        if path.resolve() != path or path.stat().st_mode & 0o777 != 0o700:
            raise ValueError('unsafe capture root')
    if (root / '.nobackup').read_bytes() != MARKER:
        raise ValueError('ownership marker differs')
    borrowed_roots = (SCALE800K_PARENT / 'input', SCALE800K_PARENT / 'group-64/db',
                      SCALE800K_PARENT / 'group-64/repo') if inode_limit == 800000 else ()
    protect(profile, chunk=chunk_leg is not None, cache=chunk_variant in CACHE_VARIANTS,
            extra_roots=(SCALE800K_PARENT,) if borrowed_roots else ())
    borrowed_bytes = sum(storage_bytes(path) for path in borrowed_roots)
    if storage_bytes(root) + borrowed_bytes > ceiling:
        raise ValueError('borrowed parent exceeds fixture byte ceiling')
    if chunk_leg is not None:
        parent_root = CACHE_PARENT if chunk_variant in CACHE_VARIANTS else (RECONCILE_PARENT if inode_limit == 400000 else SCALE_ROOT)
        subprocess.run(['sha256sum', '-c', 'FOLLOWUP-SHA256SUMS'], cwd=parent_root, check=True)
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
                     'borrowed_read_only_roots': list(map(str, borrowed_roots)), 'borrowed_read_only_bytes': borrowed_bytes,
                     'ram32_leg': os.environ.get('VAULTIC_PHASE35_RAM32_LEG'),
                     'scale800k_chunk_settings': scale800k_chunks() if inode_limit == 800000 else None,
                     'managed_ram_limit': '160GiB', 'scratch_limit': '2GiB', 'total_timeout_seconds': total_limit,
                     'backup_runtime_limit_seconds': backup_limit if chunk_variant in CACHE_VARIANTS or inode_limit == 800000 else None,
                     'backup_runtime_scope': 'CLI backup only; excludes cloning, preflight, restore and reopen',
                     'schedule': (['parent-clone', 'forced', 'restore', 'reopen'] if chunk_leg is not None or inode_limit == 800000 else (['source-freeze', 'parent-setup', 'forced'] if inode_limit in (200000, 400000) else (['forced'] if profile is not None else (['unforced', 'forced'] if inode_limit == 10000 else ['parent-setup', 'unforced', 'forced'])))), 'production_repository_writable': False,
                     'source': str(SCALE800K_PARENT / 'input') if inode_limit == 800000 else (str((RECONCILE_PARENT if inode_limit == 400000 else SCALE_ROOT) / 'input') if chunk_leg is not None else (str(root / 'input') if inode_limit in (200000, 400000) else (str(FIFTY / 'input') if profile is not None else (str(PREVIOUS / 'input') if inode_limit == 10000 else str(ACCEPTED / 'restore/ncl1-1-vs-50/fme_cae1'))))), 'toolchains': {
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
        if inode_limit in (200000, 400000, 800000):
            test_name = f'TestPublicationScale{inode_limit // 1000}kQualification'
            environment.pop('VAULTIC_PHASE35_METADATA_READ_INODE_LIMIT')
            environment.pop('VAULTIC_PHASE35_THROUGHPUT_FORCE_COMPARE_RETRY')
            environment.update({'VAULTIC_PHASE35_SCALE_INODE_LIMIT': str(inode_limit),
                                'VAULTIC_PHASE35_THROUGHPUT_ON_DEMAND': '1',
                                'VAULTIC_PHASE35_THROUGHPUT_ORDER': '64',
                                'VAULTIC_PHASE35_THROUGHPUT_SOURCE': str(root / 'input'),
                                'VAULTIC_PHASE35_THROUGHPUT_CANDIDATE': str(candidate),
                                'VAULTIC_PHASE35_THROUGHPUT_NATIVE': str(native)})
            if inode_limit == 800000:
                test_name = 'TestPublicationForceComparisonRetry'
                environment.update({'VAULTIC_PHASE35_METADATA_READ_INODE_LIMIT': '800000',
                                    'VAULTIC_PHASE35_THROUGHPUT_FORCE_COMPARE_RETRY': '1'})
                environment.update({key: str(value) for key, value in scale800k_chunks().items()})
                if os.environ.get('VAULTIC_PHASE35_RAM32_LEG'):
                    environment['VAULTIC_PHASE35_RAM32_LEG'] = os.environ['VAULTIC_PHASE35_RAM32_LEG']
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
    progress_offset, progress_pending = 0, b''
    backup_guard = BackupRuntimeGuard(backup_limit)
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
                process = subprocess.Popen([str(root / 'qualifier.test'), '-test.run=^' + test_name + '$', '-test.v', '-test.timeout=' + test_timeout],
                                           stdout=output, stderr=errors, env=environment, cwd=WORKSPACE / 'cmd/vaultic', start_new_session=True)
                last_storage = 0
                while True:
                    rss, count = pilot.process_group_rss(process.pid)
                    actual_settings = observe_db_settings(process.pid, settings, native, chunk_variant) if db_screen else []
                    if actual_settings:
                        result['db_settings_verified'] = True
                    elapsed = time.monotonic() - started
                    if chunk_variant in CACHE_VARIANTS or inode_limit == 800000:
                        stage = root / 'group-64/forced'
                        completed = stage / 'backup.runtime-finished.json'
                        seconds = None
                        if not backup_guard.finished and completed.exists() and completed.stat().st_size:
                            if completed.stat().st_size > 4096:
                                raise ValueError('backup runtime receipt exceeds bounded parser limit')
                            seconds = json.loads(completed.read_text())['seconds']
                        backup_guard.observe(time.monotonic(), (stage / 'backup.runtime-start.json').exists(), seconds)
                        progress_path = root / 'group-64/forced/backup.stdout.log'
                        if progress_path.exists():
                            with progress_path.open('rb') as progress:
                                progress.seek(progress_offset)
                                addition = progress.read(65536)
                                progress_offset += len(addition)
                            records = (progress_pending + addition).split(b'\n')
                            progress_pending = records.pop()
                            if len(progress_pending) > 1 << 20:
                                raise ValueError('cache progress record exceeds bounded parser limit')
                            for record in records:
                                event = json.loads(record)
                                if validate_cache_progress(event):
                                    result['cache_progress_observed'] = True
                    result['peak_combined_rss_bytes'] = max(result['peak_combined_rss_bytes'], rss)
                    samples.write(json.dumps({'elapsed_seconds': elapsed, 'rss_bytes': rss, 'processes': count,
                                              'db_settings': actual_settings}) + '\n')
                    samples.flush()
                    if elapsed - last_storage >= 30:
                        if storage_bytes(root) + borrowed_bytes > ceiling:
                            raise ValueError('fixture byte ceiling reached')
                        last_storage = elapsed
                    if observer.poll() is not None or rss > RSS_CEILING or elapsed >= total_limit or shutil.disk_usage(root).free < FLOOR:
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
                if (chunk_variant in CACHE_VARIANTS or inode_limit == 800000) and not backup_guard.finished:
                    raise ValueError('cache backup lacks completed runtime guard receipt')
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
            if chunk_variant in CACHE_VARIANTS or inode_limit == 800000:
                result['backup_runtime_limit_seconds'] = backup_guard.limit
                result['backup_runtime_guard_finished'] = backup_guard.finished
            result['owned_process_exit_code'] = None if process is None else process.returncode
            pilot.write_json(root / 'outcome.json', result)
    print(json.dumps(result), flush=True)
    return 0 if result['accepted'] else 1


def validate_cache_preflight(receipt):
    if (receipt['accepted'] is not True or receipt['sampled_records'] != 64
            or not 0 < receipt['source_device'] <= 0xffffffff
            or receipt['source_fsid'] != receipt['source_device']):
        raise ValueError('cache source current-pointer identity preflight differs')
    if receipt['snapshot_format'] == 'legacy-tree':
        if any(receipt[name] != 0 for name in ('snapshot_fsid', 'snapshot_root_inode', 'snapshot_commit_sequence', 'snapshot_root_revision')):
            raise ValueError('legacy cache snapshot has normalized identity fields')
    elif receipt['snapshot_format'] == 'normalized':
        if receipt['snapshot_commit_sequence'] <= 0 or receipt['snapshot_root_revision'] <= 0:
            raise ValueError('normalized cache snapshot scope is incomplete')
    else:
        raise ValueError('unknown cache snapshot format')


def cache_parent_file_count(parent, source):
    if (parent['entries'] != source['entries'] or parent['regular_files'] != source['regular_files']
            or parent['source_unchanged'] is not True or parent['input_sha256'] != source['sha256']
            or parent['bytes'] != source['bytes'] or not 0 < source['files'] <= source['entries']):
        raise ValueError('cache parent source count binding differs')
    files = parent.get('files', source['files'])
    if files != source['files']:
        raise ValueError('cache parent file count differs')
    return files


def latency_summary(buckets, nanoseconds=None):
    if buckets is None:
        return None
    if not isinstance(buckets, list) or len(buckets) != 8 or any(type(value) is not int or value < 0 for value in buckets):
        raise ValueError('invalid bounded latency histogram')
    count = sum(buckets)
    upper_ms = [0.1, 1, 5, 20, 100, 500, 2000, None]
    result = {'samples': count, 'samples_over_2s': buckets[-1], 'bucket_counts': buckets,
              'mean_ms': nanoseconds / count / 1000000 if count and nanoseconds is not None else None}
    for percentile in (50, 95, 99):
        target, accumulated, bound = math.ceil(count * percentile / 100), 0, None
        if count:
            for index, value in enumerate(buckets):
                accumulated += value
                if accumulated >= target:
                    bound = upper_ms[index]
                    break
        result[f'p{percentile}_upper_bound_ms'] = bound
    return result


def encryption_counter_deltas(first, last):
    result = {}
    for name, before in first.items():
        if name == 'read_windows':
            continue
        after = last[name]
        if name.endswith('latency_buckets'):
            latency_summary(before)
            latency_summary(after)
            if any(now < earlier for now, earlier in zip(after, before)):
                raise ValueError('follow-up latency counter reset')
            result[name] = [now - earlier for now, earlier in zip(after, before)]
        else:
            if after < before:
                raise ValueError('follow-up encryption counter reset')
            result[name] = after - before
    return result


def bottleneck_report(critical):
    encryption, source = critical.get('encryption_reads') or {}, critical.get('source_reads')
    reconciliation = critical.get('reconciliation') or {}
    latencies = {}
    for name, buckets, timing in (('origin_fetch', 'ciphertext_origin_latency_buckets', 'ciphertext_origin_ns'),
                                 ('chunk_result_wait', 'chunk_wait_latency_buckets', 'chunk_wait_ns'),
                                 ('fetch_admission_wait', 'chunk_admission_latency_buckets', 'chunk_admission_wait_ns')):
        latencies[name] = latency_summary(encryption.get(buckets), encryption.get(timing))
    latencies['source_read'] = None if source is None else latency_summary(source.get('latency_buckets'), source.get('read_ns'))
    return {'latency': latencies, 'source_reads': source,
            'origin_logical_bytes': encryption.get('ciphertext_origin_bytes'),
            'chunk_misses': encryption.get('chunk_cache_misses'), 'chunk_evictions': encryption.get('chunk_cache_evictions'),
            'pipeline_seconds': {name: reconciliation[name] / 1000000000 for name in
                                 ('writer_wait_prepared_ns', 'writer_batch_ns', 'stat_worker_ns', 'prepare_blocked_worker_ns')
                                 if name in reconciliation},
            'timing_scope': 'main-interval accumulated worker/caller durations; overlap, not additive wall time',
            'physical_nfs_traffic_measured': False, 'critical_path_blocked_time_qualified': False,
            'limits': ['source bytes count actual reader returns, including retries; not NAS disk bytes',
                       'chunk-result waits include fetch/decrypt and coalesced followers, not storage alone',
                       'NFS mount intervals remain shared; temporal correlation does not prove attribution',
                       'percentiles are fixed-bucket upper bounds; overflow and unavailable bounds are null']}


def bottleneck_windows(path):
    windows, previous, dropped = [], None, 0
    nfs = {name: 0 for name in ('calls', 'rtt_ms', 'execute_ms', 'received_bytes')}
    with path.open() as stream:
        while line := stream.readline(2 << 20):
            if not line.endswith('\n'):
                raise ValueError('observation exceeds bounded parser limit or is incomplete')
            record = json.loads(line)
            for mount in record.get('host_shared', {}).get('nfs_mount_intervals', []):
                read = mount.get('operations', {}).get('READ', {})
                for name in nfs:
                    nfs[name] += read.get(name, 0)
            for event in record.get('new_operation_telemetry', []):
                if event['source'] != 'group-64/forced/backup.stdout.log':
                    continue
                current = event['snapshot']
                seconds = current.get('critical_path_seconds_elapsed')
                if seconds is None or previous is not None and seconds == previous['critical_path_seconds_elapsed']:
                    continue
                if previous is not None:
                    elapsed = seconds - previous['critical_path_seconds_elapsed']
                    if not math.isfinite(elapsed) or elapsed <= 0:
                        raise ValueError('critical-path observation time reset')
                    first, last = previous.get('encryption_reads'), current.get('encryption_reads')
                    encryption = encryption_counter_deltas(first, last) if first is not None and last is not None else {}
                    first, last = previous.get('source_reads'), current.get('source_reads')
                    source = encryption_counter_deltas(first, last) if first is not None and last is not None else None
                    first, last = previous.get('reconciliation', {}), current.get('reconciliation', {})
                    reconciliation = {}
                    for name in ('scanned', 'writer_wait_prepared_ns', 'writer_batch_ns', 'stat_worker_ns', 'prepare_blocked_worker_ns'):
                        if name in first and name in last:
                            if last[name] < first[name]:
                                raise ValueError('reconciliation observation counter reset')
                            reconciliation[name] = last[name] - first[name]
                    window = bottleneck_report({'encryption_reads': encryption, 'source_reads': source, 'reconciliation': reconciliation})
                    window.update(start_seconds=previous['critical_path_seconds_elapsed'], end_seconds=seconds,
                                  interval_seconds=elapsed, scanned_per_second=reconciliation.get('scanned', 0) / elapsed if 'scanned' in reconciliation else None,
                                  shared_nfs_read_mean_rtt_ms=nfs['rtt_ms'] / nfs['calls'] if nfs['calls'] else None,
                                  shared_nfs_read_rpc_bytes=nfs['received_bytes'] if nfs['calls'] else None)
                    if len(windows) == 2048:
                        windows.pop(0)
                        dropped += 1
                    windows.append(window)
                previous = current
                nfs = {name: 0 for name in nfs}
    correlations = {}
    for name, value in (('source_read_mean_ms', lambda window: (window['latency']['source_read'] or {}).get('mean_ms')),
                        ('origin_fetch_mean_ms', lambda window: (window['latency']['origin_fetch'] or {}).get('mean_ms')),
                        ('prepared_wait_seconds', lambda window: window['pipeline_seconds'].get('writer_wait_prepared_ns')),
                        ('shared_nfs_read_mean_rtt_ms', lambda window: window['shared_nfs_read_mean_rtt_ms'])):
        pairs = [(window['scanned_per_second'], value(window)) for window in windows
                 if window['scanned_per_second'] is not None and value(window) is not None]
        correlation = None
        if len(pairs) >= 3:
            try:
                correlation = statistics.correlation([pair[0] for pair in pairs], [pair[1] for pair in pairs])
            except statistics.StatisticsError:
                pass
        correlations[name] = {'paired_windows': len(pairs), 'pearson_r': correlation}
    return {'windows': windows, 'windows_dropped': dropped, 'correlations_with_scanned_rate': correlations,
            'scope': 'approximate observer-aligned intervals; NFS mount counters are shared',
            'causal_attribution_qualified': False}


def summarize_followup(root, profile, inode_limit=50000, expected_candidate=None):
    outcome = json.loads((root / 'outcome.json').read_text())
    measured = json.loads((root / 'group-64/forced/result.json').read_text())
    variant = root.name.rsplit('-', 1)[-1] if inode_limit == 400000 and root.parent in (RECONCILE_ROOT, CACHE_ROOT, PIPELINE_ROOT, RAM16_ROOT) else None
    settings = cache_profile_settings(profile, variant)
    if not outcome['accepted'] or not outcome['db_settings_verified'] or outcome['exit_code'] != 0:
        raise ValueError('follow-up capture or live settings were not accepted')
    expected_entries, expected_regular, expected_bytes = 50000, 41347, 19493496103
    expected_digest = '8f32883b02c37ae550a0375fa6abfe79ec52d830ef0d9cff1053522687a41e87'
    expected_parent = '60dfb46069fb4e18b603d69caf61dc3ca1db76099089ab18396985aa8c9f8e2b'
    if inode_limit in (200000, 400000, 800000):
        if profile != f'db-scale{inode_limit // 1000}k':
            raise ValueError('scale receipt requires its explicit scale profile')
        source = json.loads((root / 'source-observation.json').read_text())
        parent = json.loads((root / 'group-64/result.json').read_text())
        expected_entries, expected_regular, expected_bytes = source['entries'], source['regular_files'], source['bytes']
        expected_digest, expected_parent = source['sha256'], parent['snapshot']
        if expected_entries != inode_limit or expected_bytes > 64 << 30 or not parent['restore_verified'] or not parent['reopen_verified']:
            raise ValueError('200k source dimensions or parent acceptance incomplete')
        parent_files = cache_parent_file_count(parent, source) if variant in CACHE_VARIANTS else parent['files']
        if parent['input_sha256'] != expected_digest or parent_files != source['files'] or parent['bytes'] != expected_bytes:
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
    expected_flags += reconciliation_flags(variant)
    if inode_limit == 800000:
        expected_flags += ['--metadata-reconciliation-batch-size=5000', '--metadata-reconciliation-read-ahead=1']
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
            deltas[field] = encryption_counter_deltas(first, last)
    critical = measured['critical_path']
    if inode_limit == 800000:
        require_cache_reuse(critical['reconciliation'], expected_entries)
        cpu = root / 'group-64/forced/cpu/cpu.pprof'
        if not cpu.is_file() or not 0 < cpu.stat().st_size <= 32 << 20 or cpu.stat().st_mode & 0o777 != 0o600:
            raise ValueError('800k CPU profile lacks private bounded capture')
        if outcome.get('backup_runtime_limit_seconds') != 1200 or outcome.get('backup_runtime_guard_finished') is not True:
            raise ValueError('800k capture lacks accepted 1200-second backup runtime guard')
    if variant in CACHE_VARIANTS:
        require_cache_reuse(critical['reconciliation'], expected_entries)
        preflight = json.loads((root / 'group-64/forced/reuse-preflight.json').read_text())
        validate_cache_preflight(preflight)
        if not outcome.get('cache_progress_observed'):
            raise ValueError('cache capture lacks accepted identity preflight or live reuse monitoring')
        if outcome.get('backup_runtime_limit_seconds') != 600 or outcome.get('backup_runtime_guard_finished') is not True:
            raise ValueError('cache capture lacks accepted 600-second backup runtime guard')
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
            'bottlenecks': bottleneck_report(critical),
            'bottleneck_windows': bottleneck_windows(root / 'observations.jsonl') if inode_limit == 800000 else None,
            'main_encryption': critical.get('encryption_reads'),
            'read_windows': read_window_deltas(before['attribution']['encryption_reads'], critical.get('encryption_reads') or {}),
            'peak_combined_rss_bytes': outcome['peak_combined_rss_bytes'],
            'physical_nfs_traffic_measured': False, 'cache_evictions_measured': False,
            'single_pass_sustained_qualified': main >= 600, 'deployment_qualified': False}


def seal_followup(root, row):
    pilot.write_json(root / 'verified-followup.json', row)
    artifacts = sorted(path for directory in (root, root / 'driver', root / 'group-64', root / 'group-64/forced')
                       for path in directory.glob('*') if path.is_file())
    artifacts.extend(sorted((root / 'group-64/forced/cpu').glob('*.pprof')))
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


def reconciliation_flags(variant):
    if variant in RAM16_VARIANTS:
        return ['--metadata-reconciliation-read-ahead=1']
    if variant in PIPELINE_VARIANTS:
        flags = ['--metadata-reconciliation-read-ahead=1'] if variant in ('W', 'X', 'Y') else []
        if variant in ('X', 'Y'):
            flags.append('--metadata-reconciliation-batch-size=10000')
        if variant == 'Y':
            flags.append('--metadata-cache-mib=8192')
        return flags
    return {'J': ['--metadata-reconciliation-scalar-revision-reads'],
            'L': ['--metadata-reconciliation-read-ahead=1'],
            'M': ['--metadata-reconciliation-batch-size=500']}.get(variant, [])


def read_window_deltas(before, after):
    previous = {window['unix_seconds']: window for window in before.get('read_windows', [])}
    windows = []
    for window in after.get('read_windows', []):
        baseline = previous.get(window['unix_seconds'], {})
        delta = {name: window[name] - baseline.get(name, 0) for name in ('requested_plaintext_bytes', 'ciphertext_origin_bytes')}
        if any(value < 0 for value in delta.values()):
            raise ValueError('read-window counter reset')
        if any(delta.values()):
            windows.append({'unix_seconds': window['unix_seconds'], **delta,
                            'amplification': delta['ciphertext_origin_bytes'] / delta['requested_plaintext_bytes'] if delta['requested_plaintext_bytes'] else None})
    return {'seconds_per_bucket': 10, 'scope': 'writer-before through main-end; boundary buckets partial',
            'dropped': after.get('read_windows_dropped', 0), 'windows': windows}


def reconcile_comparison_report(rows):
    if [row['variant'] for row in rows] != list(RECONCILE_SCHEDULE) or any(row['entries'] != 400000 for row in rows):
        raise ValueError('400k comparison schedule or scope differs')
    contrasts = []
    for offset, label, control, candidate in ((0, 'revision-batching', 'J', 'K'), (4, 'collection-read-ahead', 'K', 'L'), (8, 'collection-batch-500', 'K', 'M')):
        bracket = rows[offset:offset + 4]
        times = {variant: statistics.median(row['main_seconds'] for row in bracket if row['variant'] == variant) for variant in (control, candidate)}
        if any(not math.isfinite(value) or value <= 0 for value in times.values()):
            raise ValueError('invalid comparison duration')
        contrasts.append({'experiment': label, 'control': control, 'candidate': candidate, 'median_main_seconds': times,
                          'observed_speedup': times[control] / times[candidate], 'replicates_per_variant': 2})
    return {'accepted': True, 'case': 'dedicated-force/immutable', 'rows': rows, 'contrasts': contrasts,
            'causal_speedup_claim': False, 'physical_nfs_traffic_measured': False,
            'single_pass_sustained_qualified': all(row['single_pass_sustained_qualified'] for row in rows)}


def require_cache_reuse(metrics, entries):
    if any(metrics.get(name) != entries for name in ('observed', 'scanned', 'reused')):
        raise ValueError('cache screen requires complete metadata-record reuse, not fresh publication')
    if any(metrics.get(name) != 0 for name in ('reconciled', 'changed', 'failed', 'deferred', 'revision_allocation_calls',
                                             'atomic_group_calls', 'directory_publication_calls')):
        raise ValueError('cache screen unexpectedly changed or published metadata records')
    if metrics.get('revision_lookup', {}).get('keys') != entries:
        raise ValueError('cache screen did not exercise the complete revision lookup workload')


def pipeline_comparison_report(rows):
    if [row['variant'] for row in rows] != list(PIPELINE_SCHEDULE):
        raise ValueError('pipeline comparison schedule incomplete or reordered')
    for row in rows:
        if row['entries'] != 400000 or not math.isfinite(row['main_seconds']) or row['main_seconds'] <= 0:
            raise ValueError('pipeline comparison requires exact positive 400k measurements')
        if row['settings'] != cache_profile_settings('db-scale400k', row['variant']) or row['chunk_settings'] != chunk_settings(row['variant']):
            raise ValueError('pipeline comparison settings differ')
        require_cache_reuse(row['reconciliation'], row['entries'])
    contrasts = []
    for offset, (control, candidate, experiment) in enumerate((
            ('T', 'U', 'directory-prefetch-and-configured-width'),
            ('U', 'V', 'publication-group-64'),
            ('V', 'W', 'collection-read-ahead'),
            ('W', 'X', 'collection-batch-10000'),
            ('X', 'Y', 'ram-heavy-caches'))):
        bracket = rows[offset * 4:offset * 4 + 4]
        medians = {}
        for variant in (control, candidate):
            selected = [row for row in bracket if row['variant'] == variant]
            values = {'main_seconds': statistics.median(row['main_seconds'] for row in selected),
                      'pointer_calls': statistics.median(row['reconciliation']['pointer_lookup']['calls'] for row in selected),
                      'revision_calls': statistics.median(row['reconciliation']['revision_lookup']['calls'] for row in selected),
                      'origin_bytes': statistics.median(row['main_encryption']['ciphertext_origin_bytes'] for row in selected),
                      'peak_rss_bytes': statistics.median(row['peak_combined_rss_bytes'] for row in selected)}
            for name in ('writer_wait_prepared_ns', 'writer_wait_fill_ns', 'writer_batch_ns', 'writer_directories_ns', 'existing_record_lookup_ns'):
                values[name] = statistics.median(row['reconciliation'][name] for row in selected)
            values['entries_per_second'] = 400000 / values['main_seconds']
            medians[variant] = values
        contrasts.append({'experiment': experiment, 'control': control, 'candidate': candidate,
                          'replicates_per_variant': 2, 'medians': medians,
                          'observed_speedup': medians[control]['main_seconds'] / medians[candidate]['main_seconds']})
    return {'accepted': True, 'case': 'dedicated-force/immutable/preparation-pipeline', 'rows': rows,
            'contrasts': contrasts, 'performance_target_met': all(400000 / row['main_seconds'] >= 1000 for row in rows),
            'throughput_target_entries_per_second': 1000, 'causal_speedup_claim': False,
            'physical_nfs_traffic_measured': False, 'production_modified': False,
            'larger_database_growth_qualified': False,
            'single_pass_sustained_qualified': all(row['single_pass_sustained_qualified'] for row in rows)}


def cache_comparison_report(rows):
    if [row['variant'] for row in rows] != list(CACHE_SCHEDULE):
        raise ValueError('cache comparison schedule incomplete or reordered')
    for row in rows:
        if row['entries'] != 400000 or not math.isfinite(row['main_seconds']) or row['main_seconds'] <= 0:
            raise ValueError('cache comparison requires positive exact 400k measurements')
        if row['settings'] != cache_profile_settings('db-scale400k', row['variant']) or row['chunk_settings'] != chunk_settings(row['variant']):
            raise ValueError('cache comparison settings differ')
        require_cache_reuse(row['reconciliation'], row['entries'])
    contrasts = []
    for offset, candidate in enumerate(('O', 'P', 'Q', 'R', 'S')):
        bracket = rows[offset * 4:offset * 4 + 4]
        medians = {}
        for variant in ('N', candidate):
            selected = [row for row in bracket if row['variant'] == variant]
            medians[variant] = {'main_seconds': statistics.median(row['main_seconds'] for row in selected),
                                'origin_bytes': statistics.median(row['main_encryption']['ciphertext_origin_bytes'] for row in selected),
                                'chunk_misses': statistics.median(row['main_encryption']['chunk_cache_misses'] for row in selected),
                                'block_misses_wider_window': statistics.median(row['deltas']['cache_reads']['block']['misses'] for row in selected),
                                'peak_rss_bytes': statistics.median(row['peak_combined_rss_bytes'] for row in selected)}
            medians[variant]['entries_per_second'] = 400000 / medians[variant]['main_seconds']
            medians[variant]['target_met_in_each_replicate'] = all(400000 / row['main_seconds'] >= 1000 for row in selected)
        contrasts.append({'candidate': candidate, 'control': 'N', 'replicates_per_variant': 2,
                          'medians': medians,
                          'observed_speedup': medians['N']['main_seconds'] / medians[candidate]['main_seconds'],
                          'origin_byte_reduction_fraction': 1 - medians[candidate]['origin_bytes'] / medians['N']['origin_bytes'] if medians['N']['origin_bytes'] else None})
    return {'accepted': True, 'case': 'dedicated-force/immutable/cache-capacity', 'rows': rows,
            'throughput_target_entries_per_second': 1000,
            'performance_target_met': any(contrast['medians'][contrast['candidate']]['target_met_in_each_replicate'] for contrast in contrasts),
            'contrasts': contrasts, 'causal_speedup_claim': False, 'production_modified': False,
            'deployment_qualified': False, 'physical_nfs_traffic_measured': False,
            'single_pass_sustained_qualified': all(row['single_pass_sustained_qualified'] for row in rows),
            'block_counter_scope': 'writer-before through snapshot validation; wider than exact main encryption counters'}


def prepare_cache_parent():
    previous = Path('/volume2/NASDA2/vaultic-test-m6/cache-read-fme-cae1-20261106-400k-retry1')
    leg = previous / 'leg-01-N'
    captured = leg / 'group-64/forced'
    if CACHE_PARENT.exists() or CACHE_PARENT.parent.resolve() != CACHE_PARENT.parent:
        raise ValueError('refuse existing or noncanonical current-identity parent')
    namespace = os.environ.get('VAULTIC_CHUNK_PARENT_MOUNT_NS')
    if not namespace or os.readlink('/proc/self/ns/mnt') == namespace:
        raise ValueError('cache parent preparation requires a private mount namespace')
    if shutil.disk_usage(CACHE_PARENT.parent).free < FLOOR + fixture_ceiling(400000):
        raise ValueError('cache parent reservation plus shared floor unavailable')
    protect('db-scale400k', chunk=True, extra_roots=(previous,))
    for root, manifest in ((previous, 'STOPPED-SHA256SUMS'), (leg, 'FOLLOWUP-SHA256SUMS')):
        subprocess.run(['sha256sum', '-c', manifest], cwd=root, check=True)
    measured = json.loads((captured / 'result.json').read_text())
    source = json.loads((leg / 'source-observation.json').read_text())
    if (measured['entries'] != 400000 or measured['input_sha256'] != source['sha256']
            or not all(measured[name] is True for name in ('restore_verified', 'reopen_verified', 'source_unchanged'))
            or measured['critical_path']['reconciliation']['changed'] != 400000
            or measured['critical_path']['reconciliation']['failed'] != 0
            or measured['summary']['data_blobs'] != 0):
        raise ValueError('current-identity parent requires the verified post-restart publication capture')
    pilot.private_directory(CACHE_PARENT)
    with private_output(CACHE_PARENT / '.nobackup') as output:
        output.write(MARKER)
    group = CACHE_PARENT / 'group-64'
    pilot.private_directory(group)
    for name in ('db', 'repo'):
        target = group / name
        pilot.private_directory(target)
        with private_output(group / ('copy-' + name + '.stdout.log')) as output, private_output(group / ('copy-' + name + '.stderr.log')) as errors:
            subprocess.run(['rsync', '-a', '--chmod=D700,F600', '--exclude=*.coordination.lock',
                            str(captured / name) + '/', str(target) + '/'], stdout=output, stderr=errors, check=True, timeout=1800)
        with private_output(group / ('checksum-' + name + '.stdout.log')) as output, private_output(group / ('checksum-' + name + '.stderr.log')) as errors:
            subprocess.run(['rsync', '-rcni', '--delete', '--exclude=*.coordination.lock',
                            str(captured / name) + '/', str(target) + '/'], stdout=output, stderr=errors, check=True, timeout=1800)
        if (group / ('checksum-' + name + '.stdout.log')).stat().st_size != 0:
            raise ValueError('cache parent checksum clone differs')
    for original, target in ((leg / 'source-observation.json', CACHE_PARENT / 'source-observation.json'),
                             (RECONCILE_PARENT / 'inode-manifest.json', CACHE_PARENT / 'inode-manifest.json'),
                             (captured / 'backup.stdout.log', group / 'backup.stdout.log'),
                             (captured / 'backup.stderr.log', group / 'backup.stderr.log')):
        shutil.copyfile(original, target)
        os.chmod(target, 0o600)
    pilot.write_json(group / 'result.json', measured)
    pilot.write_json(CACHE_PARENT / 'parent-provenance.json', {
        'capture': str(captured), 'snapshot': measured['snapshot'], 'historical_snapshot': measured['baseline_parent'],
        'source': str(RECONCILE_PARENT / 'input'), 'source_sha256': source['sha256'], 'source_entries': 400000,
        'payload_clone_checksum_verified': True, 'reuse_preflight_required': True,
        'old_parent_modified': False, 'filesystem_ids_rewritten': False})
    manifest = CACHE_PARENT / 'FOLLOWUP-SHA256SUMS'
    with private_output(manifest, text=True) as output:
        for path in sorted(CACHE_PARENT.rglob('*')):
            if path.is_file() and path != manifest:
                output.write(pilot.digest(path) + '  ' + str(path.relative_to(CACHE_PARENT)) + '\n')
    subprocess.run(['sha256sum', '-c', manifest.name], cwd=CACHE_PARENT, check=True)
    print(json.dumps({'prepared_current_identity_parent': str(CACHE_PARENT), 'snapshot': measured['snapshot']}), flush=True)


def ram32_comparison_report(rows):
    if [row['cache_gib'] for row in rows] != list(RAM32_SCHEDULE):
        raise ValueError('RAM32 comparison requires its exact ABBA/BAAB schedule')
    for index, row in enumerate(rows, 1):
        label = f"leg-{index:02d}-{row['cache_gib']}g"
        if (row['entries'] != 800000 or not math.isfinite(row['main_seconds']) or row['main_seconds'] <= 0
                or row['chunk_settings'] != ram32_leg(label)[2]
                or row['settings'] != cache_profile_settings('db-scale800k')):
            raise ValueError('RAM32 comparison changed scope, capacity, or non-cache settings')
    brackets = []
    for block in (rows, rows[:4], rows[4:]):
        medians = {}
        for gib in (16, 32):
            selected = [row for row in block if row['cache_gib'] == gib]
            medians[str(gib)] = {
                'main_seconds': statistics.median(row['main_seconds'] for row in selected),
                'origin_bytes': statistics.median(row['main_encryption']['ciphertext_origin_bytes'] for row in selected),
                'evictions': statistics.median(row['main_encryption']['chunk_cache_evictions'] for row in selected),
                'peak_rss_bytes': statistics.median(row['peak_combined_rss_bytes'] for row in selected),
                'cli_cpu_seconds': statistics.median(row['cli_cpu_seconds'] for row in selected)}
        brackets.append({'schedule': [row['cache_gib'] for row in block], 'medians': medians,
                         'observed_speedup': medians['16']['main_seconds'] / medians['32']['main_seconds']})
    return {'accepted': True, 'rows': rows, 'schedule': list(RAM32_SCHEDULE), 'replicates_per_capacity': 4,
            'overall': brackets[0], 'balanced_brackets': brackets[1:],
            'same_optimized_cli_and_native': True, 'causal_speedup_claim': False,
            'physical_nfs_traffic_measured': False, 'production_modified': False,
            'larger_database_growth_qualified': False,
            'single_pass_sustained_qualified': all(row['main_seconds'] >= 600 for row in rows)}


def execute_ram32_series():
    if RAM32_ROOT.exists() or RAM32_ROOT.parent.resolve() != RAM32_ROOT.parent:
        raise ValueError('refuse existing or noncanonical RAM32 series')
    if shutil.disk_usage(RAM32_ROOT.parent).free < FLOOR + len(RAM32_SCHEDULE) * fixture_ceiling(800000):
        raise ValueError('full RAM32 reservation plus shared free-space floor unavailable')
    subprocess.run(['sha256sum', '-c', 'SHA256SUMS'], cwd=RAM32_BIN, check=True)
    build = json.loads((RAM32_BIN / 'build.json').read_text())
    pins = dict(build['source_files'])
    for name in ('vaultic', 'vaulticdb', 'qualifier.test', 'build.json', 'SHA256SUMS'):
        pins[str(RAM32_BIN / name)] = pilot.digest(RAM32_BIN / name)
    parent_proof = json.loads((RAM32_BIN / 'validation/qualified-parent.json').read_text())
    pins.update({str(SCALE800K_PARENT / name): digest for name, digest in parent_proof['files'].items()})
    pilot.private_directory(RAM32_ROOT)
    with private_output(RAM32_ROOT / '.nobackup') as output:
        output.write(MARKER)
    pilot.write_json(RAM32_ROOT / 'launch-pins.json', {'pins': pins, 'schedule': list(RAM32_SCHEDULE),
                     'parent': str(SCALE800K_PARENT), 'entries': 800000, 'process_cache_state': 'fresh daemon per leg',
                     'host_cache_state': 'uncontrolled; balanced ABBA/BAAB ordering; no host cache dropping',
                     'same_optimized_cli_and_native': True, 'ordinary_backend_policies_unchanged': True,
                     'source_mount': 'NASDA2 frozen copy, not original ncl1 mount',
                     'cpu_profile_scope': 'whole CLI backup command, not exact-main or native CPU'})
    rows = []
    try:
        for index, gib in enumerate(RAM32_SCHEDULE, 1):
            if any(pilot.digest(Path(path)) != expected for path, expected in pins.items()):
                raise ValueError('RAM32 source, artifact or parent receipt pin changed')
            label = f'leg-{index:02d}-{gib}g'
            root, runtime, chunks = ram32_leg(label)
            with patch.dict(os.environ, {'VAULTIC_PHASE35_RAM32_LEG': label}):
                prepare('coalesced', 800000, 'db-scale800k')
            for source, target, mode in ((RAM32_BIN / 'qualifier.test', root / 'qualifier.test', 0o500),
                                         (RAM32_BIN / 'build.json', root / 'build.json', 0o600),
                                         (SCALE800K_PARENT / 'source-observation.json', root / 'source-observation.json', 0o600)):
                shutil.copyfile(source, target)
                os.chmod(target, mode)
            environment = dict(os.environ, VAULTIC_PHASE35_RAM32_LEG=label)
            command = ['timeout', '--signal=INT', '--kill-after=180s', '58100s', 'unshare', '--mount',
                       '--propagation', 'private', sys.executable, str(Path(__file__)), '--mode=coalesced',
                       '--inode-limit=800000', '--concurrency-profile=db-scale800k']
            print(json.dumps({'starting': label, 'root': str(root), 'chunk_settings': chunks}), flush=True)
            with private_output(RAM32_ROOT / (label + '.stdout.log')) as output, private_output(RAM32_ROOT / (label + '.stderr.log')) as errors:
                process = subprocess.Popen(command, env=environment, stdout=output, stderr=errors, start_new_session=True)
                try:
                    status = process.wait()
                    if status:
                        raise subprocess.CalledProcessError(status, command)
                except BaseException:
                    if process.poll() is None:
                        interrupt(process, group=True)
                    raise
            launch = json.loads((root / 'launch-pins.json').read_text())
            if launch['ram32_leg'] != label or launch['scale800k_chunk_settings'] != chunks:
                raise ValueError('RAM32 live capacity receipt differs')
            row = summarize_followup(root, 'db-scale800k', 800000, RAM32_BIN / 'vaultic')
            if row != json.loads((root / 'verified-followup.json').read_text()) or runtime.exists():
                raise ValueError('RAM32 report replay or private-runtime cleanup differs')
            subprocess.run(['sha256sum', '-c', 'FOLLOWUP-SHA256SUMS'], cwd=root, check=True,
                           stdout=subprocess.DEVNULL)
            row.update({'cache_gib': gib, 'leg': label, 'chunk_settings': chunks})
            for mode in ('flat', 'cumulative'):
                command = ['go', 'tool', 'pprof', '-top', '-nodecount=35']
                if mode == 'cumulative':
                    command.append('-cum')
                command += [str(RAM32_BIN / 'vaultic'), str(root / 'group-64/forced/cpu/cpu.pprof')]
                with private_output(RAM32_ROOT / (label + '.cpu-' + mode + '.txt')) as output:
                    subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=True)
            rows.append(row)
            pilot.write_json(RAM32_ROOT / (label + '.verified.json'), row)
            print(json.dumps({'accepted': label, 'main_seconds': row['main_seconds'],
                              'origin_bytes': row['main_encryption']['ciphertext_origin_bytes'],
                              'peak_rss_bytes': row['peak_combined_rss_bytes']}), flush=True)
        if any(pilot.digest(Path(path)) != expected for path, expected in pins.items()):
            raise ValueError('RAM32 pins changed before final report')
        report = ram32_comparison_report(rows)
        pilot.write_json(RAM32_ROOT / 'verified-comparison.json', report)
        with private_output(RAM32_ROOT / 'SERIES-SHA256SUMS', text=True) as output:
            for path in sorted(RAM32_ROOT.iterdir()):
                if path.is_file() and path.name != 'SERIES-SHA256SUMS':
                    output.write(pilot.digest(path) + '  ' + path.name + '\n')
        subprocess.run(['sha256sum', '-c', 'SERIES-SHA256SUMS'], cwd=RAM32_ROOT, check=True)
        print(json.dumps({'accepted': True, 'overall': report['overall'],
                          'balanced_brackets': report['balanced_brackets']}), flush=True)
    except BaseException as error:
        pilot.write_json(RAM32_ROOT / 'failed-series.json', {'accepted': False, 'completed_legs': len(rows),
                         'failure': type(error).__name__ + ': ' + str(error)})
        raise


def ram16_comparison_report(rows):
    if [row['variant'] for row in rows] != list(RAM16_SCHEDULE):
        raise ValueError('RAM16 comparison requires its exact ABBA/BAAB schedule')
    for row in rows:
        if row['entries'] != 400000 or not math.isfinite(row['main_seconds']) or row['main_seconds'] <= 0:
            raise ValueError('RAM16 comparison requires positive exact 400k measurements')
        if row['settings'] != cache_profile_settings('db-scale400k', row['variant']) or row['chunk_settings'] != chunk_settings(row['variant']):
            raise ValueError('RAM16 comparison changed more than ciphertext byte capacity')
    brackets = []
    for block in (rows, rows[:4], rows[4:]):
        medians = {}
        for variant in RAM16_VARIANTS:
            selected = [row for row in block if row['variant'] == variant]
            medians[variant] = {'main_seconds': statistics.median(row['main_seconds'] for row in selected),
                                'origin_bytes': statistics.median(row['main_encryption']['ciphertext_origin_bytes'] for row in selected),
                                'peak_rss_bytes': statistics.median(row['peak_combined_rss_bytes'] for row in selected)}
        brackets.append({'schedule': [row['variant'] for row in block], 'medians': medians,
                         'observed_speedup': medians['Z']['main_seconds'] / medians['AA']['main_seconds']})
    return {'accepted': True, 'rows': rows, 'schedule': list(RAM16_SCHEDULE), 'replicates_per_variant': 4,
            'overall': brackets[0], 'balanced_brackets': brackets[1:],
            'performance_target_met': all(400000 / row['main_seconds'] >= 1000 for row in rows),
            'causal_speedup_claim': False, 'physical_nfs_traffic_measured': False,
            'production_modified': False, 'larger_database_growth_qualified': False,
            'single_pass_sustained_qualified': all(row['main_seconds'] >= 600 for row in rows)}


def execute_chunk_series(immutable=False, reconcile=False, caches=False, pipelines=False, ram16=False):
    pipelines = pipelines or ram16
    series = RAM16_ROOT if ram16 else (PIPELINE_ROOT if pipelines else (CACHE_ROOT if caches else (RECONCILE_ROOT if reconcile else (IMMUTABLE_ROOT if immutable else CHUNK_ROOT))))
    schedule = RAM16_SCHEDULE if ram16 else (PIPELINE_SCHEDULE if pipelines else (CACHE_SCHEDULE if caches else (RECONCILE_SCHEDULE if reconcile else (('I', 'I', 'I') if immutable else CHUNK_SCHEDULE))))
    bin_root = RAM16_BIN if ram16 else (PIPELINE_BIN if pipelines else (CACHE_BIN if caches else (RECONCILE_BIN if reconcile else (IMMUTABLE_BIN if immutable else CHUNK_BIN))))
    scale400k = reconcile or caches or pipelines
    parent_root, inode_limit, profile = (CACHE_PARENT if caches or pipelines else RECONCILE_PARENT, 400000, 'db-scale400k') if scale400k else (SCALE_ROOT, 200000, 'db-scale200k')
    if series.exists() or series.parent.resolve() != series.parent:
        raise ValueError('refuse existing or noncanonical chunk comparison series')
    if shutil.disk_usage(series.parent).free < FLOOR + len(schedule) * fixture_ceiling(inode_limit):
        raise ValueError('full series reservation plus shared free-space floor unavailable')
    pilot.private_directory(series)
    with private_output(series / '.nobackup') as output:
        output.write(MARKER)
    driver = bin_root / 'qualifier.test'
    inputs = (driver, bin_root / 'vaultic', bin_root / 'new/vaulticdb',
              bin_root / 'build.json', Path(__file__), Path(pilot.__file__),
              WORKSPACE / 'helpers/phase35-m6/observe.py', WORKSPACE / 'cmd/vaultic/working_benchmark_test.go',
              SCALE_ROOT / 'FOLLOWUP-SHA256SUMS', SCALE_ROOT / 'group-64/result.json', SCALE_ROOT / 'source-observation.json')
    if not immutable and not scale400k:
        inputs += (bin_root / 'old/vaulticdb',)
    if scale400k:
        inputs = tuple(parent_root / path.relative_to(SCALE_ROOT) if path.is_relative_to(SCALE_ROOT) else path for path in inputs)
    if pipelines and not ram16:
        inputs += (bin_root / 'control/vaultic',)
    if ram16:
        inputs += (WORKSPACE / 'vaulticdb/src/encryption.rs', WORKSPACE / 'vaulticdb/src/encryption/tests.rs')
    pins = {str(path): pilot.digest(path) for path in inputs}
    pilot.write_json(series / 'launch-pins.json', {'pins': pins, 'schedule': list(schedule),
                     'parent': str(parent_root), 'source_entries': inode_limit, 'production_modified': False,
                     'baseline_revision': subprocess.check_output(['git', '-C', str(WORKSPACE), 'rev-parse', 'HEAD'], text=True).strip() if scale400k else '01835ca98b1b66d48f176b719fffa8f89ac80c67',
                     'candidate_revision': subprocess.check_output(['git', '-C', str(WORKSPACE), 'rev-parse', 'HEAD'], text=True).strip() if immutable or scale400k else '931e3a5ef29c632fa483e304abe7b3d1a842865b',
                     'default_case': chunk_settings('T' if pipelines else ('N' if caches else 'I')) if immutable or scale400k else None,
                     'same_instrumented_binaries': scale400k and (not pipelines or ram16),
                     'pipeline_control_and_candidate_pinned_separately': pipelines and not ram16,
                     'cpu_profile_scope': 'whole backup command; equal instrumentation on all pipeline legs' if pipelines else None,
                     'parent_database_bytes': storage_bytes(parent_root / 'group-64/db') if pipelines else None,
                     'reconciliation_flags': {variant: reconciliation_flags(variant) for variant in schedule} if scale400k else None,
                     'cache_variants': {variant: {'settings': cache_profile_settings(profile, variant), 'chunk_settings': chunk_settings(variant)} for variant in (RAM16_VARIANTS if ram16 else (PIPELINE_VARIANTS if pipelines else CACHE_VARIANTS))} if caches or pipelines else None,
                     'single_pass_sustained_required_seconds': 600,
                     'process_cache_state': 'fresh daemon; identical untimed 64-record authenticated reuse preflight for cache series; no cross-leg retained cache',
                     'host_cache_state': 'uncontrolled but unchanged; balanced leg ordering',
                     'larger_cache_bracket_bytes': 256 << 20})
    rows = []
    try:
        for index, variant in enumerate(schedule, 1):
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
                                   (parent_root / 'source-observation.json', root / 'source-observation.json')):
                shutil.copyfile(source, target)
                os.chmod(target, 0o500 if target.name == 'qualifier.test' else 0o600)
            environment = dict(os.environ, VAULTIC_CHUNK_PARENT_MOUNT_NS=os.readlink('/proc/self/ns/mnt'))
            command = ['timeout', '--signal=INT', '--kill-after=150s', '29100s',
                       'unshare', '--mount', '--propagation', 'private', sys.executable, str(Path(__file__)),
                       '--mode=coalesced', '--inode-limit=' + str(inode_limit), '--concurrency-profile=' + profile, '--chunk-leg=' + label]
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
            expected_candidate = bin_root / ('control/vaultic' if pipelines and variant == 'T' else 'vaultic')
            row = summarize_followup(root, profile, inode_limit, expected_candidate)
            row.update({'variant': variant, 'leg': label, 'chunk_settings': chunk_settings(variant)})
            row['cache_evictions_measured'] = variant != 'A'
            if pipelines:
                cpu = root / 'group-64/forced/cpu/cpu.pprof'
                if not cpu.is_file() or cpu.stat().st_size == 0 or cpu.stat().st_size > 32 << 20 or cpu.stat().st_mode & 0o777 != 0o600:
                    raise ValueError('pipeline CPU profile missing, oversized or not private')
                row['cpu_profile_sha256'] = pilot.digest(cpu)
                row['cpu_profile_scope'] = 'whole backup command; not exact-main-only'
            if not (immutable or scale400k) and variant != 'A' and row['deltas']['encryption_reads']['header_validation_calls'] <= 0:
                raise ValueError('new chunk read path was not exercised')
            seal_followup(root, row)
            rows.append(row)
            shutil.rmtree(runtime)
            print(json.dumps({'accepted': label, 'main_seconds': row['main_seconds'],
                              'entries_per_second': row['entries_per_second'],
                              'encryption': row['deltas']['encryption_reads']}), flush=True)
        report = ram16_comparison_report(rows) if ram16 else (pipeline_comparison_report(rows) if pipelines else (cache_comparison_report(rows) if caches else (reconcile_comparison_report(rows) if reconcile else ({'accepted': True, 'case': 'dedicated-force/immutable', 'rows': rows,
               'median_main_seconds': statistics.median(row['main_seconds'] for row in rows),
               'causal_speedup_claim': False, 'physical_nfs_traffic_measured': False,
               'single_pass_sustained_qualified': all(row['single_pass_sustained_qualified'] for row in rows)}
              if immutable else chunk_comparison_report(rows)))))
        pilot.write_json(series / 'verified-series.json', report)
        with private_output(series / 'SERIES-SHA256SUMS', text=True) as output:
            artifacts = sorted(path for path in series.iterdir() if path.is_file() and path.name != 'SERIES-SHA256SUMS')
            artifacts.extend(series / row['leg'] / 'FOLLOWUP-SHA256SUMS' for row in rows)
            for path in artifacts:
                output.write(pilot.digest(path) + '  ' + str(path.relative_to(series)) + '\n')
        subprocess.run(['sha256sum', '-c', 'SERIES-SHA256SUMS'], cwd=series, check=True)
        print(json.dumps(report), flush=True)
    except BaseException as error:
        pilot.write_json(series / 'failed-series.json', {'accepted': False, 'completed_legs': [row['leg'] for row in rows],
                         'failure': type(error).__name__ + ': ' + str(error)})
        raise


class ChunkComparisonTests(unittest.TestCase):
    def test_ram32_report_rejects_reordering_and_capacity_drift(self):
        rows = []
        for index, gib in enumerate(RAM32_SCHEDULE, 1):
            label = f'leg-{index:02d}-{gib}g'
            rows.append({'cache_gib': gib, 'entries': 800000, 'main_seconds': 400 if gib == 16 else 360,
                         'chunk_settings': ram32_leg(label)[2], 'settings': cache_profile_settings('db-scale800k'),
                         'main_encryption': {'ciphertext_origin_bytes': 100, 'chunk_cache_evictions': 10},
                         'peak_combined_rss_bytes': 1, 'cli_cpu_seconds': 2})
        self.assertAlmostEqual(ram32_comparison_report(rows)['overall']['observed_speedup'], 400 / 360)
        with self.assertRaises(ValueError):
            ram32_comparison_report(list(reversed(rows)))
        rows[0]['chunk_settings'] = dict(rows[0]['chunk_settings'], VAULTICDB_CHUNK_CACHE_ENTRIES=131072)
        with self.assertRaises(ValueError):
            ram32_comparison_report(rows)

    def test_ram32_schedule_routes_and_capacity(self):
        self.assertEqual(RAM32_SCHEDULE[:4], (16, 32, 32, 16))
        self.assertEqual(RAM32_SCHEDULE[4:], (32, 16, 16, 32))
        for index, gib in enumerate(RAM32_SCHEDULE, 1):
            label = f'leg-{index:02d}-{gib}g'
            root, runtime, chunks = ram32_leg(label)
            with patch.dict(os.environ, {'VAULTIC_PHASE35_RAM32_LEG': label}):
                self.assertEqual(paths('coalesced', 800000, 'db-scale800k'), (root, runtime))
                self.assertEqual(binaries('db-scale800k'), (RAM32_BIN / 'vaultic', RAM32_BIN / 'vaulticdb'))
                self.assertEqual(scale800k_chunks(), chunks)
                self.assertEqual(chunks['VAULTICDB_CHUNK_CACHE_BYTES'], gib << 30)
                self.assertEqual(chunks['VAULTICDB_CHUNK_CACHE_ENTRIES'], gib * 4096)
                with self.assertRaises(ValueError):
                    paths('coalesced', 400000, 'db-scale400k')
            with self.assertRaises(ValueError):
                ram32_leg(label + '/retry')
        with self.assertRaises(ValueError):
            ram32_leg('leg-09-16g')

    def test_borrowed_storage_is_counted_and_missing_scopes_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            borrowed = root / 'borrowed'
            capture = root / 'capture'
            borrowed.mkdir()
            capture.mkdir()
            (borrowed / 'parent').write_bytes(b'p' * 17)
            (capture / 'clone').write_bytes(b'c' * 19)
            self.assertEqual(storage_bytes(borrowed) + storage_bytes(capture), 36)
            with self.assertRaises(ValueError):
                storage_bytes(root / 'missing')

    def test_bottleneck_windows_align_rates_and_latency(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'observations.jsonl'
            records = []
            for seconds in (5, 10, 15, 20):
                snapshot = {'critical_path_seconds_elapsed': seconds,
                            'reconciliation': {'scanned': seconds * 100, 'writer_wait_prepared_ns': seconds * 1000000},
                            'source_reads': {'bytes': seconds * 1000, 'calls': seconds, 'read_ns': seconds * 1000000,
                                             'latency_buckets': [0, seconds, 0, 0, 0, 0, 0, 0]},
                            'encryption_reads': {'ciphertext_origin_ns': seconds * 1000000,
                                                  'ciphertext_origin_latency_buckets': [0, seconds, 0, 0, 0, 0, 0, 0]}}
                records.append({'new_operation_telemetry': [{'source': 'group-64/forced/backup.stdout.log', 'snapshot': snapshot}],
                                'host_shared': {'nfs_mount_intervals': [{'operations': {'READ': {'calls': 10, 'rtt_ms': 50, 'received_bytes': 1000}}}]}})
            path.write_text(''.join(json.dumps(record) + '\n' for record in records))
            report = bottleneck_windows(path)
            self.assertEqual(len(report['windows']), 3)
            self.assertEqual(report['windows'][0]['scanned_per_second'], 100)
            self.assertEqual(report['windows'][0]['source_reads']['bytes'], 5000)
            self.assertEqual(report['windows'][0]['shared_nfs_read_mean_rtt_ms'], 5)
            self.assertIsNone(report['correlations_with_scanned_rate']['source_read_mean_ms']['pearson_r'])
            self.assertFalse(report['causal_attribution_qualified'])

    def test_latency_deltas_and_bottleneck_report(self):
        first = {'reads': 1, 'ciphertext_origin_latency_buckets': [1, 1, 1, 1, 1, 1, 1, 1]}
        last = {'reads': 3, 'ciphertext_origin_latency_buckets': [2, 2, 2, 2, 2, 2, 2, 2]}
        delta = encryption_counter_deltas(first, last)
        self.assertEqual(delta['ciphertext_origin_latency_buckets'], [1] * 8)
        self.assertEqual(first['ciphertext_origin_latency_buckets'], [1] * 8)
        with self.assertRaises(ValueError):
            encryption_counter_deltas(last, first)
        for invalid in ([], [0] * 7, [0] * 9, [0] * 7 + [-1], [0] * 7 + [True]):
            with self.assertRaises(ValueError):
                latency_summary(invalid)
        summary = latency_summary([0, 50, 45, 4, 1, 0, 0, 0], 100000000)
        self.assertEqual(summary['p50_upper_bound_ms'], 1)
        self.assertEqual(summary['p95_upper_bound_ms'], 5)
        self.assertEqual(summary['p99_upper_bound_ms'], 20)
        self.assertIsNone(latency_summary([0] * 8)['p50_upper_bound_ms'])
        report = bottleneck_report({'encryption_reads': dict(delta, ciphertext_origin_ns=8000000),
                                    'source_reads': {'bytes': 123, 'read_ns': 1000000, 'latency_buckets': [0, 1, 0, 0, 0, 0, 0, 0]},
                                    'reconciliation': {'writer_wait_prepared_ns': 2000000000}})
        self.assertEqual(report['latency']['origin_fetch']['samples'], 8)
        self.assertEqual(report['source_reads']['bytes'], 123)
        self.assertEqual(report['pipeline_seconds']['writer_wait_prepared_ns'], 2)
        self.assertFalse(report['physical_nfs_traffic_measured'])
        self.assertFalse(report['critical_path_blocked_time_qualified'])
        self.assertIsNone(bottleneck_report({})['latency']['origin_fetch'])

    def test_scale800k_routes_and_deadlines(self):
        self.assertEqual(paths('coalesced', 800000, 'db-scale800k'), (SCALE800K_ROOT, SCALE800K_RUNTIME))
        self.assertEqual(binaries('db-scale800k'), (SCALE800K_BIN / 'vaultic', SCALE800K_BIN / 'vaulticdb'))
        self.assertEqual(runtime_limits(800000), (1200, 57600, '15h40m'))
        self.assertEqual(runtime_limits(400000), (600, 28800, '7h50m'))
        for size, profile in ((400000, 'db-scale800k'), (800000, 'db-scale400k')):
            with self.assertRaises(ValueError):
                paths('coalesced', size, profile)
        guard = BackupRuntimeGuard(1200)
        guard.observe(0, True)
        guard.observe(1199, True)
        with self.assertRaisesRegex(ValueError, '1200-second limit'):
            guard.observe(1200, True)
        self.assertEqual(SCALE800K_CHUNKS, {'VAULTICDB_CHUNK_CACHE_BYTES': 17179869184,
                                         'VAULTICDB_CHUNK_CACHE_ENTRIES': 65536})

    def test_ram16_routes_capacity_and_report(self):
        rows = []
        for index, variant in enumerate(RAM16_SCHEDULE, 1):
            root, runtime = chunk_paths(f'leg-{index:02d}-{variant}')
            self.assertEqual(root.parent, RAM16_ROOT)
            self.assertTrue(str(runtime).startswith('/run/vaultic-unchanged-ram16400k-'))
            self.assertEqual(chunk_settings(variant)['VAULTICDB_CHUNK_CACHE_ENTRIES'], 65536)
            self.assertEqual(chunk_settings(variant)['VAULTICDB_CHUNK_CACHE_BYTES'], (4 if variant == 'Z' else 16) << 30)
            self.assertEqual(cache_profile_settings('db-scale400k', variant), cache_profile_settings('db-scale400k', 'W'))
            self.assertEqual(reconciliation_flags(variant), reconciliation_flags('W'))
            rows.append({'variant': variant, 'entries': 400000, 'main_seconds': 200 if variant == 'Z' else 180,
                         'settings': cache_profile_settings('db-scale400k', variant), 'chunk_settings': chunk_settings(variant),
                         'peak_combined_rss_bytes': 48 << 30, 'main_encryption': {'ciphertext_origin_bytes': 40 << 30}})
        report = ram16_comparison_report(rows)
        self.assertEqual(report['replicates_per_variant'], 4)
        self.assertEqual(len(report['balanced_brackets']), 2)
        self.assertFalse(report['larger_database_growth_qualified'])
        with self.assertRaises(ValueError):
            ram16_comparison_report(rows[:-1])
        rows[0]['chunk_settings']['VAULTICDB_CHUNK_CACHE_ENTRIES'] = 16384
        with self.assertRaises(ValueError):
            ram16_comparison_report(rows)

    def test_pipeline_routes_settings_and_report(self):
        rows = []
        for index, variant in enumerate(PIPELINE_SCHEDULE, 1):
            root, runtime = chunk_paths(f'leg-{index:02d}-{variant}')
            self.assertEqual(root.parent, PIPELINE_ROOT)
            self.assertTrue(runtime.name.startswith('vaultic-unchanged-pipeline400k-'))
            settings = cache_profile_settings('db-scale400k', variant)
            self.assertEqual(settings['publication_group_size'], 16 if variant in ('T', 'U') else 64)
            self.assertEqual(settings['meta_cache_bytes'], 8 << 30 if variant == 'Y' else 2 << 30)
            self.assertEqual(settings['on_demand_cache_bytes'], 8 << 30 if variant == 'Y' else 64 << 20)
            self.assertEqual('--metadata-reconciliation-read-ahead=1' in reconciliation_flags(variant), variant in ('W', 'X', 'Y'))
            self.assertEqual('--metadata-reconciliation-batch-size=10000' in reconciliation_flags(variant), variant in ('X', 'Y'))
            metrics = dict(observed=400000, scanned=400000, reused=400000, reconciled=0, changed=0,
                           failed=0, deferred=0, revision_allocation_calls=0, atomic_group_calls=0,
                           directory_publication_calls=0, pointer_lookup={'calls': 100}, revision_lookup={'calls': 100, 'keys': 400000})
            metrics.update({name: 1 for name in ('writer_wait_prepared_ns', 'writer_wait_fill_ns', 'writer_batch_ns', 'writer_directories_ns', 'existing_record_lookup_ns')})
            rows.append({'variant': variant, 'entries': 400000, 'main_seconds': 300,
                         'settings': settings, 'chunk_settings': chunk_settings(variant), 'reconciliation': metrics,
                         'main_encryption': {'ciphertext_origin_bytes': 1}, 'peak_combined_rss_bytes': 1,
                         'single_pass_sustained_qualified': False})
        report = pipeline_comparison_report(rows)
        self.assertEqual(len(report['contrasts']), 5)
        self.assertTrue(report['performance_target_met'])
        self.assertFalse(report['larger_database_growth_qualified'])
        with self.assertRaises(ValueError):
            pipeline_comparison_report(list(reversed(rows)))

    def test_cache_preflight_legacy_and_normalized_identity(self):
        legacy = {'accepted': True, 'sampled_records': 64, 'source_device': 7, 'source_fsid': 7,
                  'snapshot_format': 'legacy-tree', 'snapshot_fsid': 0, 'snapshot_root_inode': 0,
                  'snapshot_commit_sequence': 0, 'snapshot_root_revision': 0}
        validate_cache_preflight(legacy)
        normalized = dict(legacy, snapshot_format='normalized', snapshot_commit_sequence=1, snapshot_root_revision=1)
        validate_cache_preflight(normalized)
        validate_cache_preflight(dict(normalized, snapshot_fsid=8, snapshot_root_inode=1))
        with self.assertRaises(ValueError):
            validate_cache_preflight(dict(normalized, snapshot_commit_sequence=0))
        for name, value in {'accepted': False, 'sampled_records': 0, 'source_fsid': 8, 'source_device': 0,
                            'snapshot_format': 'unknown', 'snapshot_fsid': 7, 'snapshot_root_inode': 1}.items():
            with self.assertRaises(ValueError):
                validate_cache_preflight(dict(legacy, **{name: value}))

    def test_cache_parent_forced_receipt_counts(self):
        source = {'entries': 400000, 'files': 355571, 'regular_files': 355389, 'bytes': 123, 'sha256': 'test'}
        parent = {'entries': 400000, 'regular_files': 355389, 'bytes': 123, 'input_sha256': 'test', 'source_unchanged': True}
        self.assertEqual(cache_parent_file_count(parent, source), 355571)
        for name, value in {'entries': 1, 'regular_files': 1, 'bytes': 0, 'input_sha256': 'changed', 'source_unchanged': False, 'files': 0}.items():
            with self.assertRaises(ValueError):
                cache_parent_file_count(dict(parent, **{name: value}), source)

    def test_backup_runtime_excludes_preparation_and_restore(self):
        guard = BackupRuntimeGuard()
        guard.observe(10000, False)
        self.assertIsNone(guard.started)
        guard.observe(20000, True)
        guard.observe(20599.9, True)
        guard.observe(20600, True, 600)
        guard.observe(50000, True)
        self.assertTrue(guard.finished)

    def test_backup_runtime_stops_hung_and_overbudget_runs(self):
        guard = BackupRuntimeGuard()
        guard.observe(10000, True)
        with self.assertRaisesRegex(ValueError, '600-second limit'):
            guard.observe(10600, True)
        with self.assertRaisesRegex(ValueError, '600-second limit'):
            BackupRuntimeGuard().observe(20000, True, 600.01)
        for seconds in (-1, float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                BackupRuntimeGuard().observe(0, True, seconds)

    def test_cache_progress_stops_publication_early(self):
        metrics = {name: 0 for name in ('changed', 'failed', 'deferred', 'revision_allocation_calls', 'atomic_group_calls', 'directory_publication_calls')}
        self.assertTrue(validate_cache_progress({'message_type': 'reconciliation_progress', 'reconciliation': metrics}))
        self.assertFalse(validate_cache_progress({'message_type': 'status'}))
        for name in metrics:
            with self.assertRaises(ValueError):
                validate_cache_progress({'message_type': 'reconciliation_progress', 'reconciliation': dict(metrics, **{name: 1})})

    def test_cache_reuse_rejects_fresh_publication(self):
        metrics = {'observed': 400000, 'scanned': 400000, 'reconciled': 0, 'reused': 400000,
                   'changed': 0, 'failed': 0, 'deferred': 0, 'revision_allocation_calls': 0,
                   'atomic_group_calls': 0, 'directory_publication_calls': 0,
                   'revision_lookup': {'keys': 400000}}
        require_cache_reuse(metrics, 400000)
        for name in ('reused', 'reconciled', 'changed', 'revision_allocation_calls', 'atomic_group_calls', 'directory_publication_calls'):
            invalid = dict(metrics)
            invalid[name] = 0 if name == 'reused' else 1
            with self.assertRaises(ValueError):
                require_cache_reuse(invalid, 400000)
        with self.assertRaises(ValueError):
            require_cache_reuse(dict(metrics, revision_lookup={'keys': 0}), 400000)

    def test_cache400k_isolated_settings_and_report(self):
        baseline = cache_profile_settings('db-scale400k', 'N')
        baseline_chunks = chunk_settings('N')
        self.assertEqual(baseline_chunks['VAULTICDB_CHUNK_CACHE_ENTRIES'], 16384)
        rows = []
        for index, variant in enumerate(CACHE_SCHEDULE, 1):
            root, runtime = chunk_paths(f'leg-{index:02d}-{variant}')
            self.assertEqual(root.parent, CACHE_ROOT)
            self.assertTrue(runtime.name.startswith('vaultic-unchanged-cache400k-'))
            settings, chunks = cache_profile_settings('db-scale400k', variant), chunk_settings(variant)
            changed = [key for key in settings if settings[key] != baseline[key]]
            changed += [key for key in chunks if chunks[key] != baseline_chunks[key]]
            self.assertEqual(changed, [] if variant == 'N' else (['block_cache_bytes'] if variant in ('O', 'P') else ['VAULTICDB_CHUNK_CACHE_BYTES']))
            self.assertGreaterEqual(chunks['VAULTICDB_CHUNK_CACHE_ENTRIES'] * (256 << 10), chunks['VAULTICDB_CHUNK_CACHE_BYTES'])
            rows.append({'variant': variant, 'entries': 400000, 'main_seconds': 400,
                         'settings': settings, 'chunk_settings': chunks, 'main_encryption': {'ciphertext_origin_bytes': 100, 'chunk_cache_misses': 10},
                         'reconciliation': {'observed': 400000, 'scanned': 400000, 'reconciled': 0, 'reused': 400000,
                                            'changed': 0, 'failed': 0, 'deferred': 0, 'revision_allocation_calls': 0,
                                            'atomic_group_calls': 0, 'directory_publication_calls': 0, 'revision_lookup': {'keys': 400000}},
                         'deltas': {'cache_reads': {'block': {'misses': 10}}}, 'peak_combined_rss_bytes': 1000,
                         'single_pass_sustained_qualified': False})
        report = cache_comparison_report(rows)
        self.assertTrue(report['performance_target_met'])
        self.assertEqual(report['throughput_target_entries_per_second'], 1000)
        self.assertEqual(len(report['contrasts']), 5)
        self.assertFalse(report['causal_speedup_claim'])
        self.assertFalse(report['single_pass_sustained_qualified'])
        rows[1]['settings'] = dict(baseline)
        with self.assertRaises(ValueError):
            cache_comparison_report(rows)
        with self.assertRaises(ValueError):
            cache_profile_settings('db-scale200k', 'N')

    def test_reconcile400k_routes_and_brackets(self):
        self.assertEqual(paths('coalesced', 400000, 'db-scale400k')[0], RECONCILE_PARENT)
        for size, profile in ((200000, 'db-scale400k'), (400000, 'db-scale200k'), (50000, 'db-scale400k')):
            with self.assertRaises(ValueError):
                paths('coalesced', size, profile)
        for index, variant in enumerate(RECONCILE_SCHEDULE, 1):
            root, runtime = chunk_paths(f'leg-{index:02d}-{variant}')
            self.assertEqual(root.parent, RECONCILE_ROOT)
            self.assertTrue(runtime.name.startswith('vaultic-unchanged-reconcile400k-'))
            self.assertEqual(chunk_settings(variant), chunk_settings('I'))
        rows = [{'variant': variant, 'entries': 400000, 'main_seconds': 400,
                 'single_pass_sustained_qualified': False} for variant in RECONCILE_SCHEDULE]
        report = reconcile_comparison_report(rows)
        self.assertEqual(len(report['contrasts']), 3)
        self.assertFalse(report['single_pass_sustained_qualified'])
        rows[0], rows[1] = rows[1], rows[0]
        with self.assertRaises(ValueError):
            reconcile_comparison_report(rows)

    def test_read_window_scope_and_reset(self):
        before = {'read_windows': [{'unix_seconds': 10, 'requested_plaintext_bytes': 3, 'ciphertext_origin_bytes': 8}]}
        after = {'read_windows': [{'unix_seconds': 10, 'requested_plaintext_bytes': 5, 'ciphertext_origin_bytes': 16},
                                  {'unix_seconds': 20, 'requested_plaintext_bytes': 0, 'ciphertext_origin_bytes': 2}]}
        windows = read_window_deltas(before, after)['windows']
        self.assertEqual(windows[0]['amplification'], 4)
        self.assertIsNone(windows[1]['amplification'])
        with self.assertRaises(ValueError):
            read_window_deltas(after, before)

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
    parser.add_argument('--inode-limit', type=int, choices=(10000, 50000, 200000, 400000, 800000), default=800000)
    parser.add_argument('--concurrency-profile', choices=('baseline', 'rpcs', 'readers', *DB_OVERRIDES))
    parser.add_argument('--prepare', action='store_true')
    parser.add_argument('--series', action='store_true')
    parser.add_argument('--chunk-series', action='store_true')
    parser.add_argument('--immutable-series', action='store_true')
    parser.add_argument('--reconcile-series', action='store_true')
    parser.add_argument('--cache-series', action='store_true')
    parser.add_argument('--pipeline-series', action='store_true')
    parser.add_argument('--ram16-series', action='store_true')
    parser.add_argument('--ram32-series', action='store_true')
    parser.add_argument('--prepare-cache-parent', action='store_true')
    parser.add_argument('--chunk-self-test', action='store_true')
    parser.add_argument('--chunk-leg')
    arguments = parser.parse_args()
    if arguments.inode_limit == 800000 and arguments.concurrency_profile is None:
        arguments.concurrency_profile = 'db-scale800k'
    os.umask(0o077)
    if arguments.ram32_series:
        if ((arguments.mode, arguments.inode_limit, arguments.concurrency_profile) != ('coalesced', 800000, 'db-scale800k')
                or any((arguments.prepare, arguments.series, arguments.chunk_series, arguments.immutable_series,
                        arguments.reconcile_series, arguments.cache_series, arguments.pipeline_series,
                        arguments.ram16_series, arguments.prepare_cache_parent, arguments.chunk_self_test,
                        arguments.chunk_leg is not None))):
            raise ValueError('RAM32 series requires its exclusive 800k route')
        execute_ram32_series()
    elif arguments.prepare_cache_parent:
        if (arguments.mode, arguments.inode_limit, arguments.concurrency_profile) != ('coalesced', 400000, 'db-scale400k') or any((arguments.prepare, arguments.series, arguments.chunk_series, arguments.immutable_series, arguments.reconcile_series, arguments.cache_series, arguments.pipeline_series, arguments.ram16_series, arguments.chunk_self_test, arguments.chunk_leg is not None)):
            raise ValueError('cache parent preparation requires its fixed isolated 400k routing')
        prepare_cache_parent()
    elif arguments.chunk_self_test:
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ChunkComparisonTests))
        raise SystemExit(0 if result.wasSuccessful() else 1)
    elif arguments.chunk_series or arguments.immutable_series or arguments.reconcile_series or arguments.cache_series or arguments.pipeline_series or arguments.ram16_series or arguments.chunk_leg is not None:
        expected = ('coalesced', 400000, 'db-scale400k') if arguments.reconcile_series or arguments.cache_series or arguments.pipeline_series or arguments.ram16_series or arguments.inode_limit == 400000 else ('coalesced', 200000, 'db-scale200k')
        if (arguments.mode, arguments.inode_limit, arguments.concurrency_profile) != expected or arguments.prepare or arguments.series:
            raise ValueError('chunk series requires its fixed forced-only 200k routing')
        if arguments.chunk_series or arguments.immutable_series or arguments.reconcile_series or arguments.cache_series or arguments.pipeline_series or arguments.ram16_series:
            if sum((arguments.chunk_series, arguments.immutable_series, arguments.reconcile_series, arguments.cache_series, arguments.pipeline_series, arguments.ram16_series)) != 1:
                raise ValueError('cannot select both series')
            if arguments.chunk_leg is not None:
                raise ValueError('cannot select series and individual leg together')
            if (arguments.reconcile_series or arguments.cache_series or arguments.pipeline_series or arguments.ram16_series) != (arguments.inode_limit == 400000):
                raise ValueError('series kind and inode scope differ')
            execute_chunk_series(immutable=arguments.immutable_series, reconcile=arguments.reconcile_series, caches=arguments.cache_series, pipelines=arguments.pipeline_series, ram16=arguments.ram16_series)
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
        if status == 0 and arguments.inode_limit in (200000, 400000, 800000):
            root, runtime = paths(arguments.mode, arguments.inode_limit, arguments.concurrency_profile)
            try:
                row = summarize_followup(root, arguments.concurrency_profile, arguments.inode_limit)
                seal_followup(root, row)
                shutil.rmtree(runtime)
                print(json.dumps({'accepted': True, 'entries': arguments.inode_limit, 'main_seconds': row['main_seconds'],
                                  'entries_per_second': row['entries_per_second'],
                                  'single_pass_sustained_qualified': row['single_pass_sustained_qualified']}), flush=True)
            except BaseException as error:
                pilot.write_json(root / 'analysis-failure.json', {'accepted': False, 'failure': type(error).__name__ + ': ' + str(error)})
                raise
        raise SystemExit(status)