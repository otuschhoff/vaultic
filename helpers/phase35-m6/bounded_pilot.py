import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import live_readiness
import readiness


CANDIDATE = Path('/root/proj/vaultic/bin/phase35-critical-path-fill/linux-amd64/vaultic')
CANDIDATE_SHA256 = 'cbff53916df13e6d72a65451a7185c444e779279e1312f78d42489e996901fbf'
SOURCE = Path('/ncl1-1-vs-50/fme_cae1')
INODE_LIMIT = 100000


def content_observation(source, paths):
    digest = hashlib.sha256()
    files = regular = size = 0
    for relative in paths:
        path = source / relative
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            continue
        encoded = os.fsencode(relative)
        if stat.S_ISLNK(info.st_mode):
            target = os.fsencode(os.readlink(path))
            digest.update(b'symlink:%d:' % len(encoded) + encoded + b':%d:' % len(target) + target + b':')
        elif stat.S_ISREG(info.st_mode):
            digest.update(b'%d:' % len(encoded) + encoded + b':%d:' % info.st_size)
            with path.open('rb') as stream:
                while chunk := stream.read(1 << 20):
                    digest.update(chunk)
            regular += 1
            size += info.st_size
        else:
            raise ValueError('unsupported selected inode type')
        files += 1
    return {'sha256': digest.hexdigest(), 'files': files, 'regular_files': regular,
            'bytes': size, 'entries': len(paths)}


def validate_manifest(manifest):
    paths = manifest['paths']
    if manifest['version'] != 1 or manifest['source'] != str(SOURCE) or manifest['inode_limit'] != INODE_LIMIT:
        raise ValueError('unexpected bounded pilot scope')
    if len(paths) != INODE_LIMIT or len(set(paths)) != len(paths) or paths[0] != '.':
        raise ValueError('unexpected bounded pilot cardinality')
    selected = set(paths)
    for relative in paths:
        path = Path(relative)
        if path.is_absolute() or '..' in path.parts or '.snapshot' in path.parts or str(path) != relative:
            raise ValueError('unsafe bounded pilot path')
        if str(path.parent) not in selected:
            raise ValueError('bounded pilot omitted ancestor')


def accepted_baseline(plan, receipt):
    recovered = receipt.get('metadata_validated') is True and receipt.get('preview_only') is False
    piloted = (receipt.get('accepted') is True and receipt.get('partial') is True and
               receipt.get('contract_sha256') == readiness.fingerprint(plan))
    if receipt.get('failure') or not (recovered or piloted):
        raise ValueError('production baseline is not accepted')
    if receipt['outcomes'][-1]['stage'] != 'post-metadata-check':
        raise ValueError('production baseline lacks final metadata check')
    post = receipt['outcomes'][-1]['result']
    if not post['observation_complete'] or post['exit_code'] or post['errors']:
        raise ValueError('production metadata check is not accepted')
    return post


def execute(contract, root, baseline=None):
    plan = live_readiness.load_plan(contract)
    baseline = baseline or Path(plan['root']) / 'readiness' / 'receipt.json'
    baseline_sha256 = readiness.pilot.digest(baseline)
    post = accepted_baseline(plan, json.loads(baseline.read_text()))
    original_identity = readiness.validate_stage(
        plan, 'metadata-check', readiness.json_events(Path(plan['pins']['baseline']['path'])), None)
    baseline_plan = copy.deepcopy(plan)
    baseline_plan['expected']['snapshots'] = post['events']['index_check']['slatedb_snapshots']
    identity = readiness.validate_stage(baseline_plan, 'metadata-check', post['events'], original_identity)
    if readiness.pilot.digest(CANDIDATE) != CANDIDATE_SHA256:
        raise ValueError('qualified candidate changed')
    if root.parent != readiness.pilot.AUTH_ROOT or root.resolve() != root or root.stat().st_mode & 0o777 != 0o700:
        raise ValueError('unsafe bounded pilot root')
    if (root / '.nobackup').read_text() != 'Disposable Phase 35 throughput qualification.\n':
        raise ValueError('missing bounded pilot ownership marker')
    manifest_path = root / 'inode-manifest.json'
    manifest = json.loads(manifest_path.read_text())
    validate_manifest(manifest)
    observation = json.loads((root / 'source-observation.json').read_text())
    if content_observation(SOURCE, manifest['paths']) != observation:
        raise ValueError('sealed source content changed')
    runtime = copy.deepcopy(plan)
    runtime['root'] = str(root)
    runtime['jobs'] = 1
    runtime['expected']['snapshots'] = post['events']['index_check']['slatedb_snapshots']
    runtime['pins']['binary'] = {'path': str(CANDIDATE), 'sha256': CANDIDATE_SHA256}
    runtime['limits']['scratch_bytes'] = 8 << 30
    for name in ('tmp', 'working', 'cache'):
        readiness.pilot.private_directory(root / name)
    directory = root / 'production-pilot'
    readiness.pilot.private_directory(directory)
    receipt = {'partial': True, 'accepted': False, 'snapshot': None, 'outcomes': [],
               'baseline': {'path': str(baseline), 'sha256': baseline_sha256},
               'contract_sha256': readiness.fingerprint(plan), 'candidate_sha256': CANDIDATE_SHA256,
               'manifest_sha256': readiness.pilot.digest(manifest_path), 'observation': observation,
               'started_utc': readiness.utc()}
    readiness.pilot.write_json(directory / 'plan.json', runtime)
    helper_sha256 = readiness.pilot.digest(Path(__file__))

    def run(stage, argv, seconds=14400):
        if (readiness.pilot.digest(CANDIDATE) != CANDIDATE_SHA256 or
                readiness.pilot.digest(Path(__file__)) != helper_sha256 or
                readiness.pilot.digest(baseline) != baseline_sha256 or
                readiness.pilot.digest(manifest_path) != receipt['manifest_sha256'] or
                readiness.fingerprint(live_readiness.load_plan(contract)) != receipt['contract_sha256']):
            raise ValueError('pilot inputs changed')
        stage_directory = directory / stage
        readiness.pilot.private_directory(stage_directory)
        print(json.dumps({'stage': stage, 'started_utc': readiness.utc()}), flush=True)
        result = readiness.run_observed(argv, stage_directory, seconds, runtime)
        receipt['outcomes'].append({'stage': stage, 'result': result})
        if not result['observation_complete'] or result['exit_code'] or result['errors']:
            raise ValueError(stage + ': incomplete or failed observation')
        return result

    try:
        base = live_readiness.command(runtime, 'ls', [])[:-1]
        profile = '--use-profile=' + runtime['pins']['profile']['path']
        if profile in base:
            raise ValueError('full-crawl profile leaked into bounded pilot')
        result = run('backup', base + ['backup', '--force', '--json', '--one-file-system', '--no-scan',
                     '--metadata-on-demand', '--metadata-cache-mib=64', '--metadata-publication-group-size=64',
                     '--metadata-reconciliation-queue-depth=256', '--bounded-inode-manifest=' + str(manifest_path),
                     '--working-memory-mode=ram', '--working-memory-ram-limit=160GiB',
                     '--working-memory-scratch=' + str(root / 'working'), '--working-memory-disk-budget=8GiB', str(SOURCE)])
        readiness.validate_stage(runtime, 'backup', result['events'], None)
        summary = result['events']['summary']
        receipt['snapshot'] = summary['snapshot_id']
        if summary['total_files_processed'] != observation['regular_files']:
            raise ValueError('pilot regular-file count differs')
        snapshot_directory = directory / 'snapshot'
        readiness.pilot.private_directory(snapshot_directory)
        with (snapshot_directory / 'stdout.log').open('xb') as output, (snapshot_directory / 'stderr.log').open('xb') as errors:
            inspected = subprocess.run(base + ['cat', 'snapshot', receipt['snapshot']], stdout=output, stderr=errors,
                                       timeout=60, check=True, env={**os.environ, 'VAULTIC_FEATURES': 'slatedb-authoritative=true'})
        snapshot_path = snapshot_directory / 'stdout.log'
        if snapshot_path.stat().st_size > 64 << 20 or inspected.returncode:
            raise ValueError('invalid snapshot inspection output')
        snapshot = json.loads(snapshot_path.read_text())
        if snapshot.get('paths') != [str(SOURCE)] or snapshot.get('crawl_plan', {}).get('mode') != 'bounded':
            raise ValueError('pilot snapshot does not attest bounded scope')
        restored_result = run('restore', base + ['restore', receipt['snapshot'], '--verify', '--target=' + str(root / 'restore')])
        readiness.validate_stage(runtime, 'restore', restored_result['events'], None)
        restored = root / 'restore' / str(SOURCE).lstrip('/')
        restored_paths = {'.'}
        for parent, directories, files in os.walk(restored, followlinks=False):
            restored_paths.update(str((Path(parent) / name).relative_to(restored)) for name in directories + files)
        if restored_paths != set(manifest['paths']):
            raise ValueError('restored path inventory differs from bounded manifest')
        if content_observation(restored, manifest['paths']) != observation or content_observation(SOURCE, manifest['paths']) != observation:
            raise ValueError('restored or current selected source differs')
        runtime['limits']['scratch_bytes'] = 96 << 30
        checked = run('post-metadata-check', live_readiness.command(runtime, 'post-metadata-check'), 7200)
        readiness.validate_stage(runtime, 'post-metadata-check', checked['events'], identity)
        receipt['accepted'] = True
    except Exception as error:
        receipt['failure'] = type(error).__name__ + ': ' + str(error)
    finally:
        receipt['ended_utc'] = readiness.utc()
        readiness.pilot.write_json(directory / 'receipt.json', receipt)
    print(json.dumps({key: receipt.get(key) for key in ('accepted', 'partial', 'snapshot', 'failure')}), flush=True)
    return 0 if receipt['accepted'] else 1


if __name__ == '__main__':
    os.umask(0o077)
    if len(sys.argv) not in (3, 4):
        raise SystemExit('expected recovery contract, prepared bounded root and optional accepted baseline receipt')
    raise SystemExit(execute(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]) if len(sys.argv) == 4 else None))