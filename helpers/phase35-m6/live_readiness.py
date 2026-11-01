#!/usr/bin/env python3.12
import argparse
import copy
import json
import os
from pathlib import Path
import stat
import time

import readiness


SCHEMA = 'indexdb-live-crawl-v1'
LIMITS = {'rss_bytes': 256 << 30, 'sample_seconds': 0.25,
          'backup_seconds': 30 * 86400, 'check_seconds': 14 * 86400,
          'disk_free_bytes': 57_481_929_687_040, 'artifact_bytes': 1 << 40,
          'stream_bytes': 4 << 30, 'sample_bytes': 32 << 30, 'telemetry_bytes': 64 << 30}
STAGES = ('metadata-check', 'backup', 'reconcile-tiers', 'post-metadata-check', 'post-repository-check')


def identities(paths):
    result = {}
    for name, path in paths.items():
        path = Path(path)
        if path.is_symlink() or not path.is_dir():
            raise ValueError('redirected or unavailable live directory: ' + name)
        info = path.stat()
        result[name] = {'device': info.st_dev, 'inode': info.st_ino}
    return result


def validate_source_mounts(sources):
    mounts = {}
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        fields = line.split()
        mount = fields[4].replace('\\040', ' ').replace('\\134', '\\')
        mounts[mount] = fields[fields.index('-') + 1]
    for source in sources:
        if mounts.get(str(Path(source).resolve())) not in ('nfs', 'nfs4'):
            raise ValueError('live source is not an active NFS export mount: ' + source)


def load_plan(path, value=None):
    if path.stat().st_size > readiness.MAX_LINE:
        raise ValueError('oversized live plan')
    plan = json.loads(path.read_text()) if value is None else value
    if plan['schema'] != SCHEMA or plan['authorization'] != 'one-off-live-crawl-20261031':
        raise ValueError('missing explicit live crawl authorization')
    root = Path(plan['root']).resolve(strict=True)
    if root == readiness.pilot.AUTH_ROOT.resolve() or not root.is_relative_to(readiness.pilot.AUTH_ROOT.resolve()):
        raise ValueError('live evidence target outside authorization root')
    if str(root) != plan['root'] or root.stat().st_mode & 0o777 != 0o700:
        raise ValueError('noncanonical or nonprivate live evidence directory')
    repository = readiness.pilot.SOURCE_REPO
    if plan['repository'] != str(repository) or plan['limits'] != LIMITS:
        raise ValueError('live repository or limits changed')
    required = {'binary', 'daemon', 'profile', 'source_profile', 'empty_profile', 'password', 'recovery',
                'baseline', 'supervisor', 'readiness', 'pilot', 'observer'}
    if set(plan['pins']) != required:
        raise ValueError('missing or unexpected live pins')
    for name, pin in plan['pins'].items():
        if readiness.pilot.digest(Path(pin['path'])) != pin['sha256']:
            raise ValueError('live input pin changed: ' + name)
    for name, module in (('supervisor', __file__), ('readiness', readiness.__file__),
                         ('pilot', readiness.pilot.__file__), ('observer', readiness.observe.__file__)):
        if plan['pins'][name]['path'] != str(Path(module).resolve()):
            raise ValueError('unpinned executing live helper')
    if plan['pins']['password']['path'] != str(readiness.pilot.PASSWORD_FILE):
        raise ValueError('unexpected live password path')
    recovery = Path(plan['pins']['recovery']['path'])
    if recovery.is_symlink() or recovery.stat().st_mode & 0o077:
        raise ValueError('live recovery file is not private')
    config = readiness.pilot.tomllib.loads(Path(plan['pins']['profile']['path']).read_text())
    selection = readiness.pilot.source_selection(config)
    original = readiness.pilot.source_selection(readiness.pilot.tomllib.loads(
        Path(plan['pins']['source_profile']['path']).read_text()))
    if selection != original or selection != plan['selection'] or len(selection['sources']) != 52:
        raise ValueError('live full source scope changed')
    validate_source_mounts(selection['sources'])
    if readiness.snapshot_jobs(config) != plan['jobs'] or plan['jobs'] != 52:
        raise ValueError('live crawl requires 52 independent snapshots')
    for section in [config['backup']] + config['backup']['snapshots']:
        if any(section.get(key) for key in ('use-pathdiff', 'use-fsevents', 'dry-run',
                                          'skip-if-unchanged', 'allow-deferred-commit',
                                          'files-from', 'files-from-raw', 'files-from-verbatim')):
            raise ValueError('unqualified live crawl mode')
    if readiness.pilot.tomllib.loads(Path(plan['pins']['empty_profile']['path']).read_text()):
        raise ValueError('nonempty live check profile')
    paths = {'evidence': root, 'repository': repository,
             'database': repository.parent / 'db', 'wal': repository.parent / 'wal'}
    paths.update({source: Path(source) for source in selection['sources']})
    if identities(paths) != plan['directory_identity']:
        raise ValueError('live directory or source mount identity changed')
    for source in selection['sources']:
        if repository.is_relative_to(Path(source)) or Path(source).is_relative_to(repository):
            raise ValueError('live source and repository overlap')
    socket = Path(plan['socket'])
    if socket.is_symlink() or not stat.S_ISSOCK(socket.stat().st_mode) or socket.parent.stat().st_mode & 0o777 != 0o700:
        raise ValueError('unsafe live daemon socket')
    owner = plan['external_processes']
    if len(owner) != 1:
        raise ValueError('live crawl requires one existing daemon owner')
    actual = readiness.observe.process_sample(owner[0]['pid'])
    if actual['start_ticks'] != owner[0]['start_ticks'] or actual['state'] in ('Z', 'X', 'x'):
        raise ValueError('live daemon owner changed')
    if readiness.pilot.digest(Path('/proc') / str(actual['pid']) / 'exe') != plan['pins']['daemon']['sha256']:
        raise ValueError('live daemon executable changed')
    baseline = readiness.json_events(Path(plan['pins']['baseline']['path']))
    readiness.validate_stage(plan, 'metadata-check', baseline, None)
    return plan


def seal(draft, output):
    plan = json.loads(draft.read_text())
    root = Path(plan['root'])
    if not root.is_absolute() or root == readiness.pilot.AUTH_ROOT or not root.is_relative_to(readiness.pilot.AUTH_ROOT):
        raise ValueError('invalid live evidence authorization root')
    readiness.pilot.private_directory(root)
    for name in ('tmp', 'working', 'cache'):
        readiness.pilot.private_directory(root / name)
    plan.update({'schema': SCHEMA, 'limits': dict(LIMITS), 'repository': str(readiness.pilot.SOURCE_REPO)})
    for name, module in (('supervisor', __file__), ('readiness', readiness.__file__),
                         ('pilot', readiness.pilot.__file__), ('observer', readiness.observe.__file__)):
        plan['pins'][name] = {'path': str(Path(module).resolve())}
    for pin in plan['pins'].values():
        pin['path'] = str(Path(pin['path']).resolve(strict=True))
        digest = readiness.pilot.digest(Path(pin['path']))
        if pin.get('sha256', digest) != digest:
            raise ValueError('live draft pin changed')
        pin['sha256'] = digest
    config = readiness.pilot.tomllib.loads(Path(plan['pins']['profile']['path']).read_text())
    plan['selection'] = readiness.pilot.source_selection(config)
    plan['jobs'] = readiness.snapshot_jobs(config)
    paths = {'evidence': root, 'repository': readiness.pilot.SOURCE_REPO,
             'database': readiness.pilot.SOURCE_REPO.parent / 'db', 'wal': readiness.pilot.SOURCE_REPO.parent / 'wal'}
    paths.update({source: Path(source) for source in plan['selection']['sources']})
    plan['directory_identity'] = identities(paths)
    baseline = readiness.json_events(Path(plan['pins']['baseline']['path']))['index_check']
    plan['repository_id'] = baseline['consistency']['repository_id']
    plan['expected'] = {'indexes': baseline['legacy_indexes'], 'snapshots': baseline['slatedb_snapshots'],
                        'packs': baseline['imported_packs'], 'blobs': baseline['slatedb_locations']}
    owner = readiness.observe.process_sample(plan.pop('daemon_pid'))
    plan['external_processes'] = [{'pid': owner['pid'], 'start_ticks': owner['start_ticks']}]
    load_plan(draft, plan)
    readiness.pilot.write_json(output, plan)
    return readiness.fingerprint(plan)


def command(plan, stage, extra=None):
    root = Path(plan['root'])
    pins = plan['pins']
    argv = [pins['binary']['path'], '--json', '--repo=' + plan['repository'],
            '--password-file=' + pins['password']['path'], '--cache-dir=' + str(root / 'cache'),
            '--use-profile=' + pins['profile' if stage == 'backup' else 'empty_profile']['path'],
            '--metadata-daemon-path=' + pins['daemon']['path'], '--metadata-daemon-socket=' + plan['socket'],
            '--metadata-daemon-data-dir=' + str(Path(plan['repository']).parent / 'db'),
            '--metadata-daemon-object-store=local', '--metadata-encryption-mode=required',
            '--metadata-passphrase-file=' + pins['recovery']['path']]
    if stage == 'backup':
        argv += ['backup', '--force', '--snapshot-per-source', '--no-scan', '--read-concurrency=16']
    elif stage in ('metadata-check', 'post-metadata-check', 'reconcile-tiers',
                   'preview-pending-exports', 'recover-pending-exports'):
        operation = ('export' if stage.endswith('pending-exports') else
                     'rebuild-pack-stats' if stage == 'reconcile-tiers' else 'check')
        argv += ['index', operation]
        argv += ['--daemon-path=' + pins['daemon']['path'], '--daemon-socket=' + plan['socket'],
                 '--daemon-data-dir=' + str(Path(plan['repository']).parent / 'db'), '--daemon-object-store=local']
        if stage.endswith('pending-exports'):
            argv += ['--pending-only', '--verify']
            argv += extra or []
            if stage == 'preview-pending-exports':
                argv += ['--dry-run']
        elif stage != 'reconcile-tiers':
            argv += ['--check-workers=2', '--check-rpc-concurrency=2', '--max-findings=100', '--fail-on-warning']
            argv += ['--check-memory=' + str(96 << 30), '--check-temp-max-bytes=' + str(96 << 30)]
    elif stage == 'post-repository-check':
        argv += ['check', '--read-data', '--check-unused']
    elif stage == 'ls':
        return argv + ['ls'] + extra
    elif stage == 'restore':
        return argv + ['restore', extra[0], '--verify', '--include=' + extra[1], '--target=' + extra[2]]
    else:
        raise ValueError('unexpected live stage')
    if stage in ('backup', 'metadata-check', 'post-metadata-check'):
        argv += ['--working-memory-mode=' + ('ram' if stage == 'backup' else 'kv'),
                 '--working-memory-ram-limit=' + ('160GiB' if stage == 'backup' else '96GiB'),
                 '--working-memory-scratch=' + str(root / 'working'),
                 '--working-memory-disk-budget=' + ('8GiB' if stage == 'backup' else '96GiB')]
    return argv


def observed_stage(plan, stage, directory, extra=None):
    if readiness.tree_bytes(Path(plan['root']) / 'readiness') > LIMITS['artifact_bytes'] - readiness.RECEIPT_RESERVE:
        raise ValueError('live artifact reserve exhausted')
    readiness.pilot.private_directory(directory)
    runtime = copy.deepcopy(plan)
    runtime['limits']['scratch_bytes'] = (96 if stage in ('metadata-check', 'post-metadata-check') else 8) << 30
    seconds = LIMITS['backup_seconds' if stage == 'backup' else 'check_seconds']
    return readiness.run_observed(command(plan, stage, extra), directory, seconds, runtime)


def restore_sample(plan, snapshot, source, directory):
    pending = [source]
    for ordinal in range(64):
        if not pending:
            break
        parent = pending.pop(0)
        listing = directory / ('listing-' + str(ordinal))
        result = observed_stage(plan, 'ls', listing, [snapshot, parent])
        if not result['observation_complete']:
            raise ValueError('live restore sample listing failed: ' + '; '.join(result['errors']))
        children, selected, selected_size, is_file, snapshot_seen = [], None, 0, False, False
        with open(listing / 'stdout.log', 'rb') as stream:
            for number, line in enumerate(iter(lambda: stream.readline(readiness.MAX_LINE + 1), b'')):
                if number >= 100000 or len(line) > readiness.MAX_LINE:
                    raise ValueError('restore sample listing capacity exceeded')
                node = json.loads(line)
                if node.get('message_type') == 'snapshot':
                    if node.get('id') != snapshot or node.get('paths') != [source]:
                        raise ValueError('restore snapshot/source identity mismatch')
                    snapshot_seen = True
                if node.get('message_type') != 'node' or node['path'] == parent:
                    continue
                path = Path(node['path'])
                if not path.is_relative_to(Path(source)) or '.snapshot' in path.parts:
                    raise ValueError('restore sample escaped source scope')
                children.append(node)
                if node['type'] == 'file' and type(node.get('size')) is int and 0 <= node['size'] <= 128 << 20:
                    if selected is None:
                        selected, selected_size, is_file = node['path'], node['size'], True
                elif node['type'] == 'dir' and len(pending) < 1024:
                    pending.append(node['path'])
        if selected is None and not children:
            selected = parent
        if not snapshot_seen:
            raise ValueError('restore listing omitted snapshot identity')
        if selected is not None:
            result = observed_stage(plan, 'restore', directory / 'restore',
                                    [snapshot, selected, str(directory / 'restored')])
            if not result['observation_complete']:
                raise ValueError('live verified restore failed: ' + '; '.join(result['errors']))
            summary = result['events']['summary']
            total = readiness.integer(summary['total_files'], 'selected restore files')
            restored = readiness.integer(summary['files_restored'], 'restored entries')
            if restored < total or readiness.integer(summary['total_bytes'], 'selected bytes') != selected_size or readiness.integer(
                    summary['bytes_restored'], 'restored bytes') != selected_size:
                raise ValueError('incomplete filtered verified restore')
            output = directory / 'restored' / selected.lstrip('/')
            if is_file and (not output.is_file() or output.stat().st_size != selected_size) or not is_file and not output.is_dir():
                raise ValueError('verified restore output differs from snapshot sample')
            return {'snapshot': snapshot, 'source': source, 'selected_path': selected, 'verified': True}
    raise ValueError('no bounded restore sample found for source: ' + source)


def execute(path):
    plan = load_plan(path)
    root = Path(plan['root'])
    directory = root / 'readiness'
    readiness.pilot.private_directory(directory)
    receipt = {'schema': SCHEMA, 'contract_sha256': readiness.fingerprint(plan),
               'started_utc': readiness.utc(), 'outcomes': [], 'acknowledged_snapshot_ids': [],
               'backup_committed': False, 'crawl_metadata_validated': False,
               'readiness_accepted': False, 'restores': []}
    identity = None
    started = time.monotonic()
    reserve = directory / 'receipt.reserve'
    try:
        with open(reserve, 'xb') as stream:
            os.posix_fallocate(stream.fileno(), 0, readiness.RECEIPT_RESERVE)
        for stage in STAGES:
            if readiness.fingerprint(load_plan(path)) != receipt['contract_sha256']:
                raise ValueError('live contract changed')
            print(json.dumps({'stage': stage, 'started_utc': readiness.utc()}), flush=True)
            result = observed_stage(plan, stage, directory / stage)
            receipt['outcomes'].append({'stage': stage, 'result': result})
            if stage == 'backup':
                receipt['acknowledged_snapshot_ids'] = [item['snapshot_id'] for item in result['events'].get('summaries', [])
                                                        if item.get('snapshot_id')]
                receipt['backup_committed'] = bool(receipt['acknowledged_snapshot_ids'])
            if not result['observation_complete']:
                raise ValueError(stage + ': ' + '; '.join(result['errors']))
            identity = readiness.validate_stage(plan, stage, result['events'], identity)
            readiness.pilot.write_json(directory / stage / 'verified.json', {'stage': stage, 'identity': identity})
        receipt['crawl_metadata_validated'] = True
        for ordinal, (snapshot, source) in enumerate(zip(receipt['acknowledged_snapshot_ids'], plan['selection']['sources'])):
            cell = directory / ('sample-' + str(ordinal))
            readiness.pilot.private_directory(cell)
            receipt['restores'].append(restore_sample(plan, snapshot, source, cell))
        receipt['full_data_check_passed'] = True
        receipt['remaining_gates'] = ['persistent daemon reopen', 'pathdiff qualification']
    except Exception as error:
        receipt['failure'] = type(error).__name__ + ': ' + str(error)
    finally:
        if reserve.exists():
            reserve.unlink()
        receipt['ended_utc'] = readiness.utc()
        receipt['wall_seconds'] = time.monotonic() - started
        readiness.pilot.write_json(directory / 'receipt.json', receipt)
    return receipt


def recover_pending_exports(path, expected_packs, expected_blobs, preview_only=False):
    if type(expected_packs) is not int or not 0 < expected_packs <= 32 or \
            type(expected_blobs) is not int or expected_blobs <= 0:
        raise ValueError('explicit bounded pending export counts required')
    plan = load_plan(path)
    directory = Path(plan['root']) / 'readiness'
    readiness.pilot.private_directory(directory)
    receipt = {'schema': SCHEMA, 'contract_sha256': readiness.fingerprint(plan),
               'started_utc': readiness.utc(), 'outcomes': [], 'readiness_accepted': False,
               'backup_committed': False, 'expected_pending_packs': expected_packs,
               'expected_pending_blobs': expected_blobs, 'preview_only': preview_only}
    stages = ('preview-pending-exports',) if preview_only else (
        'preview-pending-exports', 'recover-pending-exports', 'post-metadata-check')
    reserve = directory / 'receipt.reserve'
    try:
        with open(reserve, 'xb') as stream:
            os.posix_fallocate(stream.fileno(), 0, readiness.RECEIPT_RESERVE)
        for stage in stages:
            if readiness.fingerprint(load_plan(path)) != receipt['contract_sha256']:
                raise ValueError('live contract changed')
            print(json.dumps({'stage': stage, 'started_utc': readiness.utc()}), flush=True)
            extra = (['--expected-pending-packs=' + str(expected_packs),
                      '--expected-pending-blobs=' + str(expected_blobs)]
                     if stage.endswith('pending-exports') else None)
            result = observed_stage(plan, stage, directory / stage, extra)
            receipt['outcomes'].append({'stage': stage, 'result': result})
            if not result['observation_complete']:
                raise ValueError(stage + ': ' + '; '.join(result['errors']))
            if stage.endswith('pending-exports'):
                exported = result['events'].get('index_export', {})
                written = 0 if stage == 'preview-pending-exports' else (expected_packs + 999) // 1000
                if any(type(exported.get(key)) is not int or exported[key] != value for key, value in (
                        ('packs_selected', expected_packs), ('blobs_selected', expected_blobs),
                        ('indexes_written', written))):
                    raise ValueError('pending export scope differs from reviewed counts')
            else:
                identity = readiness.validate_stage(plan, 'metadata-check', result['events'], None)
                readiness.pilot.write_json(directory / stage / 'verified.json', {'identity': identity})
                receipt['metadata_validated'] = True
    except Exception as error:
        receipt['failure'] = type(error).__name__ + ': ' + str(error)
    finally:
        if reserve.exists():
            reserve.unlink()
        receipt['ended_utc'] = readiness.utc()
        readiness.pilot.write_json(directory / 'receipt.json', receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description='Explicitly authorized one-off live crawl; never schedules backups.')
    parser.add_argument('contract', type=Path)
    parser.add_argument('--seal', type=Path)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--preview-pending-exports', action='store_true')
    parser.add_argument('--recover-pending-exports', action='store_true')
    parser.add_argument('--expected-pending-packs', type=int, default=0)
    parser.add_argument('--expected-pending-blobs', type=int, default=0)
    options = parser.parse_args()
    os.umask(0o077)
    if sum(bool(mode) for mode in (options.seal, options.execute, options.preview_pending_exports,
                                  options.recover_pending_exports)) > 1:
        parser.error('seal, execute, preview and recovery are mutually exclusive')
    if options.seal:
        if options.execute:
            parser.error('--seal and --execute are mutually exclusive')
        print(json.dumps({'contract_sha256': seal(options.seal, options.contract), 'execution_started': False}))
    elif options.preview_pending_exports or options.recover_pending_exports:
        result = recover_pending_exports(options.contract, options.expected_pending_packs,
                                         options.expected_pending_blobs, options.preview_pending_exports)
        print(json.dumps(result))
        if result.get('failure'):
            raise SystemExit(1)
    elif options.execute:
        result = execute(options.contract)
        print(json.dumps(result))
        if result.get('failure'):
            raise SystemExit(1)
    else:
        print(json.dumps({'contract_sha256': readiness.fingerprint(load_plan(options.contract)), 'execution_started': False}))


if __name__ == '__main__':
    main()