from pathlib import Path
import math
import os
import shutil
import tempfile

import bounded_pilot
import live_readiness
import readiness


def atomic_install(candidate, candidate_sha256, installed, installed_sha256, rollback):
    candidate, installed, rollback = Path(candidate), Path(installed), Path(rollback)
    for path in (candidate, installed):
        if path.is_symlink() or not path.is_file() or path.parent.resolve() != path.parent:
            raise ValueError('executable path is not a canonical regular file')
    if rollback.parent.resolve() != rollback.parent or rollback.parent.stat().st_mode & 0o777 != 0o700:
        raise ValueError('rollback directory must be canonical and private')
    if (readiness.pilot.digest(candidate) != candidate_sha256 or
            readiness.pilot.digest(installed) != installed_sha256):
        raise ValueError('candidate or installed executable pin changed')
    mode = installed.stat().st_mode & 0o777
    with installed.open('rb') as original, rollback.open('xb') as backup:
        os.fchmod(backup.fileno(), mode)
        shutil.copyfileobj(original, backup)
        backup.flush()
        os.fsync(backup.fileno())
    if readiness.pilot.digest(rollback) != installed_sha256:
        raise ValueError('rollback executable differs from installed pin')
    descriptor = os.open(rollback.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=installed.parent, prefix='.vaultic-install-', delete=False) as output:
            temporary = Path(output.name)
            with candidate.open('rb') as source:
                shutil.copyfileobj(source, output)
            os.fchmod(output.fileno(), mode)
            output.flush()
            os.fsync(output.fileno())
        if (readiness.pilot.digest(temporary) != candidate_sha256 or
                readiness.pilot.digest(installed) != installed_sha256):
            raise ValueError('executable pin changed while staging installation')
        os.replace(temporary, installed)
        temporary = None
        descriptor = os.open(installed.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary is not None:
            temporary.unlink()


def validate_qualification(control, candidate, observation):
    for result in (control, candidate):
        if (result.get('restore_verified') is not True or result.get('reopen_verified') is not True or
                result.get('metadata_on_demand') is not True or result.get('group_size') != 64):
            raise ValueError('publication candidate lacks accepted restore/reopen qualification')
        if any(result.get(key) != observation[expected] for key, expected in
               (('files', 'files'), ('bytes', 'bytes'), ('input_sha256', 'sha256'))):
            raise ValueError('publication qualification input differs from sealed source')
        for key in ('source_tree', 'snapshot', 'tree'):
            identifier = result.get(key)
            if not isinstance(identifier, str) or len(identifier) != 64 or any(
                    letter not in '0123456789abcdef' for letter in identifier):
                raise ValueError('publication qualification lacks valid ' + key)
        seconds = result.get('backup_seconds')
        if not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds <= 0:
            raise ValueError('publication qualification lacks completed backup timing')
    if control['source_tree'] != candidate['source_tree']:
        raise ValueError('publication qualification selected source trees differ')


def backup_command(plan, source):
    if source != str(bounded_pilot.SOURCE) or source not in plan['selection']['sources']:
        raise ValueError('full-source rollout requires the qualified fme_cae1 source')
    patterns = plan['selection']['globs']
    if not patterns or any(not isinstance(pattern, str) or not pattern.startswith('!') or len(pattern) < 2
                           for pattern in patterns):
        raise ValueError('full-source rollout requires explicit negative exclusion patterns')
    if '!**/.snapshot' not in patterns or '!**/.snapshot/**' not in patterns:
        raise ValueError('full-source rollout requires snapshot directory and descendant exclusions')
    base = live_readiness.command(plan, 'ls', [])[:-1]
    profile = '--use-profile=' + plan['pins']['empty_profile']['path']
    if profile not in base or '--use-profile=' + plan['pins']['profile']['path'] in base:
        raise ValueError('full-source rollout cannot load the full-crawl profile')
    root = Path(plan['root'])
    arguments = ['backup', '--force', '--json', '--snapshot-per-source', '--one-file-system', '--no-scan',
                 '--read-concurrency=16', '--metadata-on-demand', '--metadata-cache-mib=64',
                 '--metadata-publication-group-size=64', '--metadata-reconciliation-queue-depth=256',
                 '--working-memory-mode=ram', '--working-memory-ram-limit=160GiB',
                 '--working-memory-scratch=' + str(root / 'working'), '--working-memory-disk-budget=8GiB']
    arguments.extend('--exclude=' + pattern[1:] for pattern in patterns)
    arguments.extend('--exclude-if-present=' + marker for marker in plan['selection']['exclude_if_present'])
    return base + arguments + [source]