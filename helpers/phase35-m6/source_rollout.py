from pathlib import Path
import copy
import fcntl
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile

import bounded_pilot
import live_readiness
import readiness


CANDIDATE = Path('/root/proj/vaultic/bin/phase35-critical-path-paired-reads/linux-amd64/vaultic')
CANDIDATE_SHA256 = '6f18a47e144d7159fe54df2887a0539a8fe161e608751c4fd693a85e786dda41'
CONTROL = bounded_pilot.CANDIDATE
CONTROL_SHA256 = bounded_pilot.CANDIDATE_SHA256


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


def activate(contract, qualification, baseline, root):
    plan = live_readiness.load_plan(contract)
    original_identity = readiness.validate_stage(
        plan, 'metadata-check', readiness.json_events(Path(plan['pins']['baseline']['path'])), None)
    post = bounded_pilot.accepted_baseline(plan, json.loads(baseline.read_text()))
    current = copy.deepcopy(plan)
    current['expected']['snapshots'] = post['events']['index_check']['slatedb_snapshots']
    readiness.validate_stage(current, 'metadata-check', post['events'], original_identity)
    observation = qualification / 'source-observation.json'
    results = [qualification / cell / 'result.json' for cell in ('group-64-baseline', 'group-64')]
    validate_qualification(*(json.loads(path.read_text()) for path in results), json.loads(observation.read_text()))
    for cell, binary, digest in (('group-64-baseline', CONTROL, CONTROL_SHA256),
                                 ('group-64', CANDIDATE, CANDIDATE_SHA256)):
        recorded = json.loads((qualification / cell / 'backup-observation.json').read_text())
        if recorded['candidate'] != str(binary) or readiness.pilot.digest(binary) != digest:
            raise ValueError('qualified executable identity changed')
    if root.parent != readiness.pilot.AUTH_ROOT or root.resolve() != root:
        raise ValueError('unsafe rollout root')
    pinned_paths = [contract, baseline, observation, *results, Path(__file__), CANDIDATE, CONTROL]
    pinned_paths.extend(qualification / cell / 'backup-observation.json'
                        for cell in ('group-64-baseline', 'group-64'))
    pins = [{'path': str(path), 'sha256': readiness.pilot.digest(path)} for path in pinned_paths]
    readiness.pilot.private_directory(root)
    receipt = {'accepted': False, 'started_utc': readiness.utc(), 'pins': pins,
               'original_contract_sha256': readiness.fingerprint(plan), 'failure': None}
    installed = Path(plan['pins']['binary']['path'])
    original_sha256 = plan['pins']['binary']['sha256']
    rollback = root / 'rollback-vaultic'
    fresh_contract = root / 'contract.json'
    try:
        if readiness.fingerprint(live_readiness.load_plan(contract)) != receipt['original_contract_sha256']:
            raise ValueError('original production contract changed before activation')
        if any(readiness.pilot.digest(Path(pin['path'])) != pin['sha256'] for pin in pins):
            raise ValueError('rollout qualification input changed before activation')
        atomic_install(CANDIDATE, CANDIDATE_SHA256, installed, original_sha256, rollback)
        with (root / 'version.stdout.log').open('xb') as output, (root / 'version.stderr.log').open('xb') as errors:
            subprocess.run([str(installed), '--version'], check=True, timeout=30, stdout=output, stderr=errors)
        draft = copy.deepcopy(plan)
        draft['root'] = str(root / 'runtime')
        draft['daemon_pid'] = plan['external_processes'][0]['pid']
        draft['pins']['binary'] = {'path': str(installed), 'sha256': CANDIDATE_SHA256}
        draft['pins']['baseline'] = {'path': str(baseline.parent / 'post-metadata-check' / 'stdout.log')}
        draft_path = root / 'draft.json'
        readiness.pilot.write_json(draft_path, draft)
        live_readiness.seal(draft_path, fresh_contract)
        sealed = live_readiness.load_plan(fresh_contract)
        if (sealed['pins']['binary']['sha256'] != CANDIDATE_SHA256 or
                sealed['expected']['snapshots'] != current['expected']['snapshots']):
            raise ValueError('fresh production contract differs from accepted rollout')
        if any(readiness.pilot.digest(Path(pin['path'])) != pin['sha256'] for pin in pins):
            raise ValueError('rollout qualification input changed during activation')
        receipt.update({'accepted': True, 'contract_sha256': readiness.fingerprint(sealed),
                        'rollback_sha256': original_sha256, 'installed_sha256': CANDIDATE_SHA256})
    except BaseException as error:
        receipt['failure'] = type(error).__name__ + ': ' + str(error)
        if rollback.exists() and readiness.pilot.digest(installed) == CANDIDATE_SHA256:
            try:
                atomic_install(rollback, original_sha256, installed, CANDIDATE_SHA256, root / 'rejected-vaultic')
                receipt['rolled_back'] = True
            except BaseException as rollback_error:
                receipt['rollback_failure'] = type(rollback_error).__name__ + ': ' + str(rollback_error)
        raise
    finally:
        receipt['ended_utc'] = readiness.utc()
        readiness.pilot.write_json(root / 'activation.json', receipt)
    return fresh_contract


def validate_full_snapshot(snapshot):
    crawl = snapshot.get('crawl_plan', {})
    if (snapshot.get('paths') != [str(bounded_pilot.SOURCE)] or crawl.get('mode') != 'full' or
            crawl.get('source') or crawl.get('reused_subtrees', 0) != 0 or crawl.get('changed_paths') or
            any(item.get('mode') != 'full' or item.get('root') != str(bounded_pilot.SOURCE)
                for item in crawl.get('roots', []))):
        raise ValueError('snapshot does not attest one complete nonselective source')


def execute(contract):
    plan = live_readiness.load_plan(contract)
    root = contract.parent
    activation = json.loads((root / 'activation.json').read_text())
    if activation.get('accepted') is not True or activation['contract_sha256'] != readiness.fingerprint(plan):
        raise ValueError('full-source run lacks accepted deployment contract')
    activation_sha256 = readiness.pilot.digest(root / 'activation.json')
    runtime = copy.deepcopy(plan)
    runtime['jobs'] = 1
    directory = Path(plan['root']) / 'readiness'
    readiness.pilot.private_directory(directory)
    readiness.pilot.write_json(directory / 'plan.json', runtime)
    receipt = {'accepted': False, 'partial': False, 'failure': None, 'snapshot': None, 'outcomes': [],
               'contract_sha256': activation['contract_sha256'], 'activation_sha256': activation_sha256,
               'source': str(bounded_pilot.SOURCE), 'started_utc': readiness.utc(),
               'scheduling_enabled': False, 'remaining_gates': ['remaining source baselines', 'scheduling', 'pathdiff']}

    def guard():
        if (readiness.fingerprint(live_readiness.load_plan(contract)) != receipt['contract_sha256'] or
                readiness.pilot.digest(root / 'activation.json') != activation_sha256 or
                any(readiness.pilot.digest(Path(pin['path'])) != pin['sha256'] for pin in activation['pins'])):
            raise ValueError('full-source rollout pins changed')

    def run(stage, argv, seconds, scratch):
        guard()
        runtime['limits']['scratch_bytes'] = scratch
        stage_directory = directory / stage
        readiness.pilot.private_directory(stage_directory)
        print(json.dumps({'stage': stage, 'source': str(bounded_pilot.SOURCE), 'started_utc': readiness.utc()}), flush=True)
        result = readiness.run_observed(argv, stage_directory, seconds, runtime)
        receipt['outcomes'].append({'stage': stage, 'result': result})
        if not result['observation_complete'] or result['exit_code'] or result['errors']:
            raise ValueError(stage + ': incomplete or failed observation')
        guard()
        return result

    try:
        identity = readiness.validate_stage(
            runtime, 'metadata-check', readiness.json_events(Path(plan['pins']['baseline']['path'])), None)
        checked = run('metadata-check', live_readiness.command(runtime, 'metadata-check'),
                      runtime['limits']['check_seconds'], 96 << 30)
        readiness.validate_stage(runtime, 'metadata-check', checked['events'], identity)
        backed = run('backup', backup_command(runtime, str(bounded_pilot.SOURCE)),
                     runtime['limits']['backup_seconds'], 8 << 30)
        readiness.validate_stage(runtime, 'backup', backed['events'], None)
        receipt['snapshot'] = backed['events']['summary']['snapshot_id']
        guard()
        inspected = directory / 'snapshot'
        readiness.pilot.private_directory(inspected)
        base = live_readiness.command(runtime, 'ls', [])[:-1]
        with (inspected / 'stdout.log').open('xb') as output, (inspected / 'stderr.log').open('xb') as errors:
            subprocess.run(base + ['cat', 'snapshot', receipt['snapshot']], check=True, timeout=60,
                           stdout=output, stderr=errors,
                           env={**os.environ, 'VAULTIC_FEATURES': 'slatedb-authoritative=true'})
        snapshot_path = inspected / 'stdout.log'
        if snapshot_path.stat().st_size > 64 << 20:
            raise ValueError('oversized full-source snapshot provenance')
        validate_full_snapshot(json.loads(snapshot_path.read_text()))
        restored = run('restore', base + ['restore', receipt['snapshot'], '--verify',
                       '--target=' + str(directory / 'restored')], runtime['limits']['check_seconds'], 8 << 30)
        readiness.validate_stage(runtime, 'restore', restored['events'], None)
        summary = restored['events']['summary']
        if (readiness.integer(summary['bytes_restored'], 'restored bytes') !=
                readiness.integer(summary['total_bytes'], 'snapshot bytes') or
                not (directory / 'restored' / str(bounded_pilot.SOURCE).lstrip('/')).is_dir()):
            raise ValueError('full-source verified restore is incomplete')
        checked = run('post-metadata-check', live_readiness.command(runtime, 'post-metadata-check'),
                      runtime['limits']['check_seconds'], 96 << 30)
        readiness.validate_stage(runtime, 'post-metadata-check', checked['events'], identity)
        receipt['accepted'] = True
    except BaseException as error:
        receipt['failure'] = type(error).__name__ + ': ' + str(error)
    finally:
        receipt['ended_utc'] = readiness.utc()
        readiness.pilot.write_json(directory / 'receipt.json', receipt)
    print(json.dumps({key: receipt[key] for key in ('accepted', 'partial', 'snapshot', 'failure')}), flush=True)
    return 0 if receipt['accepted'] else 1


if __name__ == '__main__':
    os.umask(0o077)
    if len(sys.argv) != 5:
        raise SystemExit('expected original contract, accepted qualification root, accepted baseline receipt and fresh rollout root')
    lock_root = Path('/run/vaultic-phase35/source-rollout')
    if not lock_root.exists():
        readiness.pilot.private_directory(lock_root)
    if lock_root.resolve() != lock_root or lock_root.stat().st_mode & 0o777 != 0o700:
        raise SystemExit('unsafe rollout lock directory')
    descriptor = os.open(lock_root / 'source-rollout.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = subprocess.run(['systemctl', 'show', 'vaultic-full-crawl-20261101-recovered.service',
                                '--property=ActiveState', '--property=MainPID'], check=True,
                               capture_output=True, text=True, timeout=30)
        fields = dict(line.split('=', 1) for line in state.stdout.splitlines())
        if fields != {'ActiveState': 'inactive', 'MainPID': '0'}:
            raise SystemExit('full-crawl unit must remain inactive during one-source rollout')
        fresh = activate(*(Path(argument) for argument in sys.argv[1:]))
        raise SystemExit(execute(fresh))
    finally:
        os.close(descriptor)