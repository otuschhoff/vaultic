from pathlib import Path
import math

import bounded_pilot
import live_readiness


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