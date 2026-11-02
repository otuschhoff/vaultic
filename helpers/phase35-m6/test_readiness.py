import json
import os
import shutil
from pathlib import Path
import sys
import subprocess
import tempfile
import contextlib
import signal
import socket
import time
import threading
import unittest
from unittest.mock import patch

import readiness
import live_readiness
import bounded_pilot
import source_rollout


class ReadinessTests(unittest.TestCase):
    def test_rollout_supervisor_requires_complete_restore(self):
        for fail_restore in (False, True):
            with self.subTest(fail_restore=fail_restore), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                runtime = root / 'runtime'
                runtime.mkdir()
                contract = root / 'contract.json'
                contract.write_text('{}')
                plan = {'root': str(runtime), 'jobs': 52, 'pins': {'baseline': {'path': str(contract)}},
                        'limits': {'backup_seconds': 30, 'check_seconds': 30}}
                activation = {'accepted': True, 'contract_sha256': readiness.fingerprint(plan), 'pins': []}
                (root / 'activation.json').write_text(json.dumps(activation))
                stages = []

                def observed(argv, directory, seconds, value):
                    self.assertEqual(value['jobs'], 1)
                    stage = directory.name
                    stages.append(stage)
                    events = {}
                    complete = not (stage == 'restore' and fail_restore)
                    if stage == 'backup':
                        events = {'summary': {'snapshot_id': 'c' * 64}}
                    if stage == 'restore':
                        (runtime / 'readiness' / 'restored' / str(bounded_pilot.SOURCE).lstrip('/')).mkdir(parents=True)
                        events = {'summary': {'total_bytes': 20, 'bytes_restored': 20}}
                    return {'observation_complete': complete, 'exit_code': 0,
                            'errors': [] if complete else ['injected restore failure'], 'events': events}

                def inspect(argv, **kwargs):
                    self.assertEqual(argv[-3:-1], ['cat', 'snapshot'])
                    kwargs['stdout'].write(json.dumps({'paths': [str(bounded_pilot.SOURCE)],
                                                       'crawl_plan': {'mode': 'full'}}).encode())

                with patch.object(live_readiness, 'load_plan', return_value=plan), \
                        patch.object(live_readiness, 'command', side_effect=lambda value, stage, *extra: ['vaultic', stage]), \
                        patch.object(source_rollout, 'backup_command', return_value=['vaultic', 'backup']), \
                        patch.object(readiness, 'run_observed', side_effect=observed), \
                        patch.object(readiness, 'json_events', return_value={}), \
                        patch.object(readiness, 'validate_stage', return_value=('repository', 1, 0, 'options')), \
                        patch.object(source_rollout.subprocess, 'run', side_effect=inspect):
                    self.assertEqual(source_rollout.execute(contract), 1 if fail_restore else 0)
                receipt = json.loads((runtime / 'readiness' / 'receipt.json').read_text())
                self.assertEqual(receipt['accepted'], not fail_restore)
                self.assertFalse(receipt['partial'])
                self.assertFalse(receipt['scheduling_enabled'])
                self.assertEqual(stages, ['metadata-check', 'backup', 'restore'] +
                                 ([] if fail_restore else ['post-metadata-check']))
                self.assertEqual(receipt['snapshot'], 'c' * 64)

    def test_rollout_full_snapshot_requires_nonselective_source(self):
        source = str(bounded_pilot.SOURCE)
        snapshot = {'paths': [source], 'crawl_plan': {'mode': 'full'}}
        source_rollout.validate_full_snapshot(snapshot)
        for crawl in ({'mode': 'bounded'}, {'mode': 'selective'}, {},
                      {'mode': 'full', 'source': 'inode-manifest'},
                      {'mode': 'full', 'reused_subtrees': 1},
                      {'mode': 'full', 'changed_paths': ['changed']},
                      {'mode': 'full', 'roots': [{'root': source, 'mode': 'selective'}]},
                      {'mode': 'full', 'roots': [{'root': '/other', 'mode': 'full'}]}):
            with self.assertRaises(ValueError):
                source_rollout.validate_full_snapshot(dict(snapshot, crawl_plan=crawl))
        with self.assertRaises(ValueError):
            source_rollout.validate_full_snapshot(dict(snapshot, paths=[source, '/other']))

    def test_rollout_activation_rolls_back_on_contract_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            installed, candidate, control = (root / name for name in ('installed', 'candidate', 'control'))
            installed.write_bytes(b'old executable')
            installed.chmod(0o700)
            candidate.write_text('#!/bin/sh\nexit 0\n')
            control.write_bytes(b'qualified control')
            observation = {'files': 10, 'bytes': 20, 'sha256': 'a' * 64}
            result = {'restore_verified': True, 'reopen_verified': True, 'metadata_on_demand': True,
                      'group_size': 64, 'files': 10, 'bytes': 20, 'input_sha256': 'a' * 64,
                      'source_tree': 'b' * 64, 'snapshot': 'c' * 64, 'tree': 'd' * 64, 'backup_seconds': 30}
            qualification = root / 'qualification'
            qualification.mkdir()
            (qualification / 'source-observation.json').write_text(json.dumps(observation))
            for name, binary in (('group-64-baseline', control), ('group-64', candidate)):
                cell = qualification / name
                cell.mkdir()
                (cell / 'result.json').write_text(json.dumps(result))
                (cell / 'backup-observation.json').write_text(json.dumps({'candidate': str(binary)}))
            contract, baseline = root / 'old-contract.json', root / 'baseline.json'
            contract.write_text('{}')
            baseline.write_text('{}')
            plan = {'pins': {'binary': {'path': str(installed), 'sha256': readiness.pilot.digest(installed)},
                             'baseline': {'path': str(baseline)}}, 'expected': {'snapshots': 145},
                    'external_processes': [{'pid': os.getpid()}]}
            with contextlib.ExitStack() as stack:
                for name, value in (('CANDIDATE', candidate), ('CANDIDATE_SHA256', readiness.pilot.digest(candidate)),
                                    ('CONTROL', control), ('CONTROL_SHA256', readiness.pilot.digest(control))):
                    stack.enter_context(patch.object(source_rollout, name, value))
                stack.enter_context(patch.object(readiness.pilot, 'AUTH_ROOT', root))
                loader = stack.enter_context(patch.object(live_readiness, 'load_plan', return_value=plan))
                stack.enter_context(patch.object(readiness, 'json_events', return_value={}))
                stack.enter_context(patch.object(readiness, 'validate_stage', return_value=('repository', 1, 0, 'options')))
                stack.enter_context(patch.object(bounded_pilot, 'accepted_baseline', return_value={
                    'events': {'index_check': {'slatedb_snapshots': 147}}}))
                sealer = stack.enter_context(patch.object(live_readiness, 'seal', side_effect=ValueError('injected seal failure')))
                with self.assertRaisesRegex(ValueError, 'injected seal failure'):
                    source_rollout.activate(contract, qualification, baseline, root / 'rollout')
                self.assertEqual(installed.read_bytes(), b'old executable')

                def seal(draft, output):
                    sealed = json.loads(draft.read_text())
                    sealed['expected']['snapshots'] = 147
                    readiness.pilot.write_json(output, sealed)

                sealer.side_effect = seal
                loader.side_effect = lambda path: plan if path == contract else json.loads(path.read_text())
                fresh = source_rollout.activate(contract, qualification, baseline, root / 'success')
                success = json.loads((root / 'success' / 'activation.json').read_text())
                self.assertTrue(success['accepted'])
                self.assertEqual(success['installed_sha256'], readiness.pilot.digest(candidate))
                self.assertEqual(json.loads(fresh.read_text())['expected']['snapshots'], 147)
                self.assertEqual((root / 'success' / 'rollback-vaultic').read_bytes(), b'old executable')
                self.assertEqual(installed.read_bytes(), candidate.read_bytes())
                self.assertEqual(contract.read_text(), '{}')
            activation = json.loads((root / 'rollout' / 'activation.json').read_text())
            self.assertFalse(activation['accepted'])
            self.assertTrue(activation['rolled_back'])
            self.assertIn('injected seal failure', activation['failure'])

    def test_rollout_atomic_install_and_rollback(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root.chmod(0o700)
            installed, candidate = root / 'installed', root / 'candidate'
            installed.write_bytes(b'old executable')
            installed.chmod(0o700)
            candidate.write_bytes(b'qualified executable')
            original = readiness.pilot.digest(installed)
            qualified = readiness.pilot.digest(candidate)
            with self.assertRaises(ValueError):
                source_rollout.atomic_install(candidate, 'changed', installed, original, root / 'bad')
            self.assertEqual(installed.read_bytes(), b'old executable')
            with self.assertRaises(ValueError):
                source_rollout.atomic_install(candidate, qualified, installed, 'changed', root / 'bad-installed')
            with patch.object(source_rollout.os, 'replace', side_effect=OSError('injected replacement failure')):
                with self.assertRaises(OSError):
                    source_rollout.atomic_install(candidate, qualified, installed, original, root / 'failed-rollback')
            self.assertEqual(readiness.pilot.digest(installed), original)
            self.assertEqual(readiness.pilot.digest(root / 'failed-rollback'), original)
            self.assertEqual(list(root.glob('.vaultic-install-*')), [])
            rollback = root / 'rollback'
            source_rollout.atomic_install(candidate, qualified, installed, original, rollback)
            self.assertEqual(readiness.pilot.digest(installed), qualified)
            self.assertEqual(readiness.pilot.digest(rollback), original)
            self.assertEqual(installed.stat().st_mode & 0o777, 0o700)
            with self.assertRaises(FileExistsError):
                source_rollout.atomic_install(candidate, qualified, installed, qualified, rollback)
            source_rollout.atomic_install(rollback, original, installed, qualified, root / 'replaced')
            self.assertEqual(readiness.pilot.digest(installed), original)
            self.assertEqual(readiness.pilot.digest(root / 'replaced'), qualified)

    def test_rollout_requires_matched_verified_qualification(self):
        observation = {'files': 89326, 'bytes': 20949531016, 'sha256': 'a' * 64}
        result = {'restore_verified': True, 'reopen_verified': True, 'metadata_on_demand': True,
                  'group_size': 64, 'files': observation['files'], 'bytes': observation['bytes'],
                  'input_sha256': observation['sha256'], 'source_tree': 'b' * 64,
                  'snapshot': 'c' * 64, 'tree': 'd' * 64, 'backup_seconds': 3000}
        source_rollout.validate_qualification(result, dict(result), observation)
        for field, value in (('restore_verified', False), ('reopen_verified', False),
                             ('metadata_on_demand', False), ('group_size', 4), ('files', 1),
                             ('bytes', 1), ('input_sha256', 'changed'), ('source_tree', 'e' * 64),
                             ('snapshot', 'bad'), ('tree', ''), ('backup_seconds', 0),
                             ('backup_seconds', float('nan'))):
            with self.assertRaises(ValueError):
                source_rollout.validate_qualification(result, dict(result, **{field: value}), observation)

    def test_full_source_command_preserves_scope_and_exclusions(self):
        source = str(bounded_pilot.SOURCE)
        patterns = ['!**/.snapshot', '!**/.snapshot/**', '!**/.Trash-*/**', '!**/lost+found/**']
        plan = {'root': '/private/evidence',
                'pins': {'profile': {'path': '/private/full.toml'}, 'empty_profile': {'path': '/private/empty.toml'}},
                'selection': {'sources': [source, '/other/source'], 'globs': patterns,
                              'exclude_if_present': ['CACHEDIR.TAG', '.nobackup']}}
        with patch.object(live_readiness, 'command', return_value=['vaultic', '--use-profile=/private/empty.toml', 'ls']):
            arguments = source_rollout.backup_command(plan, source)
            self.assertEqual(arguments[-1], source)
            self.assertEqual(arguments.count(source), 1)
            self.assertNotIn('/other/source', arguments)
            self.assertIn('--snapshot-per-source', arguments)
            self.assertIn('--read-concurrency=16', arguments)
            self.assertFalse(any('bounded-inode' in argument for argument in arguments))
            for pattern in patterns:
                self.assertIn('--exclude=' + pattern[1:], arguments)
            for marker in plan['selection']['exclude_if_present']:
                self.assertIn('--exclude-if-present=' + marker, arguments)
            with self.assertRaises(ValueError):
                source_rollout.backup_command(plan, '/other/source')
            plan['selection']['globs'] = patterns + ['positive']
            with self.assertRaises(ValueError):
                source_rollout.backup_command(plan, source)
        plan['selection']['globs'] = patterns
        with patch.object(live_readiness, 'command', return_value=['vaultic', '--use-profile=/private/full.toml', 'ls']):
            with self.assertRaises(ValueError):
                source_rollout.backup_command(plan, source)

    def test_bounded_pilot_digest_and_scope(self):
        import hashlib
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)
            (source / 'dir').mkdir()
            (source / 'dir' / 'file').write_bytes(b'content')
            (source / 'link').symlink_to('dir/file')
            observed = bounded_pilot.content_observation(source, ['.', 'dir', 'dir/file', 'link'])
            digest = hashlib.sha256(b'8:dir/file:7:contentsymlink:4:link:8:dir/file:').hexdigest()
            self.assertEqual(observed, {'sha256': digest, 'files': 2, 'regular_files': 1, 'bytes': 7, 'entries': 4})
            manifest = {'version': 1, 'source': str(bounded_pilot.SOURCE), 'inode_limit': 100000,
                        'paths': ['.'] + ['file-' + str(index) for index in range(99999)]}
            bounded_pilot.validate_manifest(manifest)
            with self.assertRaises(ValueError):
                bounded_pilot.validate_manifest(dict(manifest, inode_limit=10000))
            with self.assertRaises(ValueError):
                bounded_pilot.validate_manifest(dict(manifest, paths=manifest['paths'][:-1]))
            for unsafe in ('../escape', '/absolute', '.snapshot/file', 'missing/file'):
                changed = dict(manifest, paths=manifest['paths'][:-1] + [unsafe])
                with self.assertRaises(ValueError):
                    bounded_pilot.validate_manifest(changed)

    def test_bounded_pilot_accepted_baseline(self):
        plan = {'repository_id': 'repo'}
        result = {'observation_complete': True, 'exit_code': 0, 'errors': [], 'events': {}}
        receipt = {'accepted': True, 'partial': True, 'contract_sha256': readiness.fingerprint(plan),
                   'outcomes': [{'stage': 'post-metadata-check', 'result': result}]}
        self.assertEqual(bounded_pilot.accepted_baseline(plan, receipt), result)
        for field, value in (('accepted', False), ('partial', False), ('contract_sha256', 'changed'),
                             ('failure', 'failed')):
            with self.assertRaises(ValueError):
                bounded_pilot.accepted_baseline(plan, dict(receipt, **{field: value}))
        for field, value in (('observation_complete', False), ('exit_code', 1), ('errors', ['failed'])):
            changed = dict(receipt, outcomes=[{'stage': 'post-metadata-check', 'result': dict(result, **{field: value})}])
            with self.assertRaises(ValueError):
                bounded_pilot.accepted_baseline(plan, changed)
        with self.assertRaises(ValueError):
            bounded_pilot.accepted_baseline(plan, dict(receipt, outcomes=[{'stage': 'backup', 'result': result}]))

    def clean_live_check(self, snapshots):
        check = {'coverage': {'complete': True, 'mode': 'full'}, 'warnings': 0,
                 'slatedb_snapshots': snapshots, 'legacy_indexes': 2, 'legacy_locations': 5,
                 'slatedb_locations': 5, 'imported_packs': 3,
                 'consistency': {'repository_id': 'repo', 'generation': 1,
                                 'session_id': 'session', 'options_digest': 'digest',
                                 'schema_version': 0, 'sequence': 1}}
        for key in ('pending_crawl_debt', 'pending_exports', 'failed_exports', 'missing_in_slatedb',
                    'missing_in_legacy', 'missing_packs', 'invalid_packs', 'aggregate_mismatches',
                    'reverse_edge_mismatches', 'snapshot_mismatches', 'snapshot_commit_mismatches',
                    'path_version_mismatches', 'missing_placement_records', 'backend_pack_mismatches',
                    'derived_tier_mismatches', 'packs_below_durability', 'verification_state_mismatches',
                    'analytics_mismatches'):
            check[key] = 0
        return check

    def test_live_plan_seal_pins_scope_and_limits(self):
        with self.production_contract() as (contract, fixture):
            base = contract.parent
            profile = Path(fixture['pins']['profile']['path'])
            profile.write_text(profile.read_text().replace('[backup]\n', '[backup]\nsnapshot-per-source = true\n'))
            baseline = base / 'baseline.json'
            baseline.write_text(json.dumps(self.clean_live_check(2)) + '\n')
            original = readiness.pilot.SOURCE_REPO
            for name in ('db', 'wal'):
                (original.parent / name).mkdir(mode=0o700)
            runtime = base / 'runtime'
            runtime.mkdir(mode=0o700)
            server = socket.socket(socket.AF_UNIX)
            server.bind(str(runtime / 'daemon.sock'))
            self.addCleanup(server.close)
            plan = {'authorization': 'one-off-live-crawl-20261031', 'root': str(base / 'live'),
                    'socket': str(runtime / 'daemon.sock'), 'daemon_pid': os.getpid(), 'pins': {}}
            for name in ('binary', 'daemon', 'profile', 'source_profile', 'empty_profile', 'password'):
                plan['pins'][name] = {'path': fixture['pins'][name]['path']}
            plan['pins']['baseline'] = {'path': str(baseline)}
            recovery = base / 'recovery'
            recovery.write_text('fixture recovery\n')
            recovery.chmod(0o600)
            plan['pins']['recovery'] = {'path': str(recovery)}
            draft = base / 'live-draft.json'
            draft.write_text(json.dumps(plan))
            output = base / 'live/contract.json'
            with patch.object(readiness.pilot, 'PASSWORD_FILE', Path(fixture['password_file'])), \
                    patch.object(live_readiness, 'validate_source_mounts'), \
                    patch.object(live_readiness, 'observed_stage') as run:
                digest = live_readiness.seal(draft, output)
                run.assert_not_called()
                sealed = live_readiness.load_plan(output)
                self.assertEqual(digest, readiness.fingerprint(sealed))
                self.assertEqual(sealed['jobs'], 52)
                for stage in ('metadata-check', 'post-metadata-check'):
                    argv = live_readiness.command(sealed, stage)
                    self.assertIn('--working-memory-mode=kv', argv)
                    self.assertIn('--working-memory-ram-limit=96GiB', argv)
                    self.assertIn('--working-memory-disk-budget=96GiB', argv)
                self.assertIn('--working-memory-mode=ram', live_readiness.command(sealed, 'backup'))
                counts = ['--expected-pending-packs=1', '--expected-pending-blobs=6636']
                preview = live_readiness.command(sealed, 'preview-pending-exports', counts)
                recovery = live_readiness.command(sealed, 'recover-pending-exports', counts)
                for argument in counts:
                    self.assertIn(argument, preview)
                    self.assertIn(argument, recovery)
                self.assertIn('--pending-only', preview)
                self.assertIn('--dry-run', preview)
                self.assertIn('--verify', recovery)
                self.assertIn('--pending-only', recovery)
                self.assertNotIn('--dry-run', recovery)
                self.assertNotIn('--full', recovery)
                changed = dict(sealed, limits=dict(sealed['limits'], rss_bytes=1 << 40))
                with self.assertRaisesRegex(ValueError, 'limits changed'):
                    live_readiness.load_plan(output, changed)
                profile.write_text(profile.read_text() + '\n# changed\n')
                with self.assertRaisesRegex(ValueError, 'pin changed'):
                    live_readiness.load_plan(output)

    def test_pending_export_recovery_fails_closed_and_retains_receipt(self):
        for scenario, stages in (('success', 3), ('preview', 1), ('scope', 1),
                                 ('observation', 1), ('export', 2), ('check', 3)):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                plan = {'root': temporary}
                def observed(current, stage, directory, extra=None):
                    self.assertEqual(extra, ['--expected-pending-packs=1', '--expected-pending-blobs=6636']
                                     if stage.endswith('pending-exports') else None)
                    exported = {'packs_selected': 1, 'blobs_selected': 6636,
                                'indexes_written': 0 if stage == 'preview-pending-exports' else 1}
                    if scenario == 'scope' or (scenario == 'export' and stage == 'recover-pending-exports'):
                        exported['packs_selected'] = 2
                    result = {'observation_complete': scenario != 'observation',
                              'errors': ['sampling failure'] if scenario == 'observation' else [],
                              'events': {'index_export': exported}}
                    directory.mkdir(mode=0o700)
                    return result
                with patch.object(live_readiness, 'load_plan', return_value=plan), \
                        patch.object(live_readiness, 'observed_stage', side_effect=observed) as run, \
                        patch.object(readiness, 'validate_stage', return_value={'repository_id': 'repo'}) as validate:
                    if scenario == 'check':
                        validate.side_effect = ValueError('metadata mismatch')
                    receipt = live_readiness.recover_pending_exports(root / 'contract.json', 1, 6636,
                                                                     scenario == 'preview')
                    self.assertEqual(run.call_count, stages)
                    self.assertEqual(len(receipt['outcomes']), stages)
                    self.assertEqual('failure' in receipt, scenario not in ('success', 'preview'))
                    self.assertEqual(receipt.get('metadata_validated', False), scenario == 'success')
                    self.assertFalse(receipt['readiness_accepted'])
                    self.assertFalse(receipt['backup_committed'])
                    self.assertEqual(json.loads((root / 'readiness' / 'receipt.json').read_text()), receipt)
                    self.assertEqual(validate.call_count, int(scenario in ('success', 'check')))
                    if validate.called:
                        self.assertEqual(validate.call_args.args[1], 'metadata-check')
        with patch.object(live_readiness, 'load_plan') as load:
            for packs, blobs in ((0, 1), (33, 1), (True, 1), (1, 0), (1, True)):
                with self.assertRaisesRegex(ValueError, 'bounded pending'):
                    live_readiness.recover_pending_exports(Path('unused'), packs, blobs)
            load.assert_not_called()
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'export.jsonl'
            output.write_text(json.dumps({'packs_selected': 1, 'blobs_selected': 6636,
                                          'indexes_written': 0}) + '\n')
            self.assertEqual(readiness.json_events(output)['index_export']['blobs_selected'], 6636)

    def test_pending_export_recovery_requires_unchanged_snapshot_count(self):
        for snapshots in (145, 197):
            with self.subTest(snapshots=snapshots), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                plan = {'root': temporary, 'repository_id': 'repo', 'jobs': 52,
                        'expected': {'snapshots': 145}}
                def observed(current, stage, directory, extra=None):
                    directory.mkdir(mode=0o700)
                    events = ({'index_check': self.clean_live_check(snapshots)} if stage == 'post-metadata-check'
                              else {'index_export': {'packs_selected': 1, 'blobs_selected': 6636,
                                      'indexes_written': int(stage == 'recover-pending-exports')}})
                    return {'observation_complete': True, 'errors': [], 'events': events}
                with patch.object(live_readiness, 'load_plan', return_value=plan), \
                        patch.object(live_readiness, 'observed_stage', side_effect=observed):
                    receipt = live_readiness.recover_pending_exports(root / 'contract.json', 1, 6636)
                self.assertEqual(receipt.get('metadata_validated', False), snapshots == 145)
                if snapshots == 197:
                    self.assertIn('snapshot catalog count mismatch', receipt['failure'])
                else:
                    self.assertNotIn('failure', receipt)
                self.assertFalse(receipt['backup_committed'])
                self.assertFalse(receipt['readiness_accepted'])

    def test_live_contract_rejects_unmounted_source_directories(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, 'active NFS export'):
                live_readiness.validate_source_mounts([temporary])

    def test_live_failure_preserves_committed_ids_and_never_accepts_readiness(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = {'root': str(root), 'expected': {'snapshots': 2}, 'repository_id': 'repo', 'jobs': 1}
            baseline = {'observation_complete': True, 'events': {'index_check': self.clean_live_check(2)}}
            backup = {'observation_complete': False, 'errors': ['lost final sample'],
                      'events': {'summaries': [{'snapshot_id': 'a' * 64}]}}
            results = [baseline, backup]
            def stage_result(plan, stage, directory):
                directory.mkdir()
                return results.pop(0)
            with patch.object(live_readiness, 'load_plan', return_value=plan), \
                    patch.object(live_readiness, 'observed_stage', side_effect=stage_result) as run:
                result = live_readiness.execute(root / 'contract.json')
            self.assertTrue(result['backup_committed'])
            self.assertEqual(result['acknowledged_snapshot_ids'], ['a' * 64])
            self.assertFalse(result['readiness_accepted'])
            self.assertEqual(run.call_count, 2)
            self.assertTrue((root / 'readiness/receipt.json').is_file())

    @unittest.skipUnless(os.environ.get('VAULTIC_READINESS_TEST_BINARY') and os.environ.get('VAULTICDB_TEST_BINARY'),
                         'explicit current CLI and native binaries required')
    def test_native_live_crawl_and_snapshot_based_verified_restores(self):
        with tempfile.TemporaryDirectory(prefix='live-fixture-') as temporary:
            root = Path(temporary)
            os.chmod(root, 0o700)
            for name in ('source', 'other', 'tmp', 'working', 'cache', 'runtime'):
                (root / name).mkdir(mode=0o700)
            for name in ('source', 'other'):
                (root / name / 'file').write_text(name + ' fixture bytes\n')
            (root / 'password').write_text('fixture password\n')
            (root / 'passphrase').write_text('fixture recovery passphrase\n')
            (root / 'passphrase').chmod(0o600)
            (root / 'empty.toml').write_text('')
            sources = [str(root / 'source'), str(root / 'other')]
            (root / 'backup.toml').write_text('[backup]\nsnapshot-per-source = true\n'
                'globs = ["!**/.snapshot", "!**/.snapshot/**"]\n[[backup.snapshots]]\n'
                'name = "fixture"\none-file-system = true\nsources = ' + json.dumps(sources) + '\n')
            binary, daemon = os.environ['VAULTIC_READINESS_TEST_BINARY'], os.environ['VAULTICDB_TEST_BINARY']
            environment = {key: value for key, value in os.environ.items()
                           if not key.startswith(('VAULTIC_', 'VAULTICDB_', 'RESTIC_', 'RUSTIC_'))}
            base = [binary, '--json', '--repo=' + str(root / 'repo'), '--password-file=' + str(root / 'password'),
                    '--use-profile=' + str(root / 'empty.toml'), '--no-cache']
            def invoke(arguments):
                result = subprocess.run(base + arguments, env=environment, capture_output=True, timeout=60)
                self.assertEqual(result.returncode, 0, result.stderr.decode()[-4000:])
                return result.stdout
            invoke(['init'])
            invoke(['backup'] + sources)
            repository_id = json.loads(invoke(['cat', 'config']))['id']
            environment.update({'VAULTICDB_SOCKET': str(root / 'runtime/d.sock'),
                'VAULTIC_FEATURES': 'slatedb-authoritative=true',
                'VAULTICDB_REPOSITORY_ID': repository_id, 'VAULTICDB_OBJECT_STORE': 'local',
                'VAULTICDB_TOPOLOGY_SOURCE': 'external',
                'VAULTICDB_DATA_DIR': str(root / 'db'), 'VAULTICDB_ENCRYPTION': 'initialize',
                'VAULTICDB_ENCRYPTION_PASSPHRASE_FILE': str(root / 'passphrase')})
            with open(root / 'daemon.stderr', 'wb') as errors:
                owner = subprocess.Popen([daemon], env=environment, stderr=errors, stdout=subprocess.DEVNULL)
                try:
                    deadline = time.monotonic() + 30
                    while not (root / 'runtime/d.sock').exists() and owner.poll() is None and time.monotonic() < deadline:
                        threading.Event().wait(0.05)
                    self.assertIsNone(owner.poll(), (root / 'daemon.stderr').read_text())
                    metadata = ['--metadata-daemon-path=' + daemon, '--metadata-daemon-socket=' + str(root / 'runtime/d.sock'),
                                '--metadata-daemon-data-dir=' + str(root / 'db'), '--metadata-daemon-object-store=local']
                    index_options = ['--daemon-path=' + daemon, '--daemon-socket=' + str(root / 'runtime/d.sock'),
                                     '--daemon-data-dir=' + str(root / 'db'), '--daemon-object-store=local']
                    invoke(metadata + ['index', 'import', '--from-legacy', '--resume', '--snapshot-depth=0'] + index_options)
                    invoke(metadata + ['index', 'import', '--from-legacy', '--resume', '--snapshot-metadata-only'] + index_options)
                    for dry_run in (['--dry-run'], []):
                        exported = json.loads(invoke(metadata + ['index', 'export', '--pending-only', '--verify'] +
                                                     dry_run + index_options))
                        self.assertEqual(exported['packs_selected'], 0)
                        self.assertEqual(exported['indexes_written'], 0)
                    guarded = subprocess.run(base + metadata + ['index', 'export', '--pending-only',
                        '--expected-pending-packs=1', '--expected-pending-blobs=1'] + index_options,
                        env=environment, capture_output=True, timeout=60)
                    self.assertNotEqual(guarded.returncode, 0)
                    self.assertIn('pending export scope differs', guarded.stderr.decode())
                    self.assertEqual(json.loads(guarded.stdout)['indexes_written'], 0)
                    checked = json.loads(invoke(metadata + ['index', 'check', '--check-workers=2', '--check-rpc-concurrency=2'] + index_options))
                    identity = checked['consistency']
                    invoke(metadata + ['index', 'import', '--from-legacy', '--resume', '--snapshot-depth=0', '--activate',
                        '--activation-generation=' + str(identity['generation']), '--activation-sequence=' + str(identity.get('sequence', 0)),
                        '--activation-snapshots=1'] + index_options)
                    plan = {'root': str(root), 'repository': str(root / 'repo'), 'repository_id': repository_id,
                            'socket': str(root / 'runtime/d.sock'), 'jobs': 2, 'expected': {'snapshots': 1},
                            'selection': {'sources': sources}, 'limits': dict(live_readiness.LIMITS), 'pins': {},
                            'external_processes': [{'pid': owner.pid, 'start_ticks': readiness.observe.process_sample(owner.pid)['start_ticks']}]}
                    for name, path in (('binary', binary), ('daemon', daemon), ('profile', root / 'backup.toml'),
                                       ('empty_profile', root / 'empty.toml'), ('password', root / 'password'),
                                       ('recovery', root / 'passphrase')):
                        plan['pins'][name] = {'path': str(path), 'sha256': readiness.pilot.digest(Path(path))}
                    limits = dict(live_readiness.LIMITS, disk_free_bytes=0, backup_seconds=60, check_seconds=60)
                    plan['limits'] = limits
                    with patch.object(live_readiness, 'load_plan', return_value=plan), \
                            patch.object(live_readiness, 'LIMITS', limits):
                        result = live_readiness.execute(root / 'fixture.json')
                    diagnostics = '\n'.join(path.read_text()[-4000:] for path in (root / 'readiness').rglob('stderr.log'))
                    self.assertIsNone(result.get('failure'), str(result.get('failure')) + '\n' + diagnostics[-4000:])
                    self.assertEqual(len(result['acknowledged_snapshot_ids']), 2)
                    self.assertEqual(len(result['restores']), 2)
                    self.assertTrue(result['crawl_metadata_validated'])
                    self.assertTrue(result['full_data_check_passed'])
                    self.assertFalse(result['readiness_accepted'])
                    self.assertIsNone(owner.poll())
                finally:
                    if owner.poll() is None:
                        owner.send_signal(signal.SIGINT)
                    owner.wait(timeout=30)

    def test_snapshot_job_modes_and_inheritance(self):
        config = {'backup': {'snapshots': [{'sources': ['a', 'b']}, {'sources': ['c']}]}}
        self.assertEqual(readiness.snapshot_jobs(config), 2)
        config['backup']['snapshot-per-source'] = True
        self.assertEqual(readiness.snapshot_jobs(config), 3)
        config['backup']['snapshots'][0].update({'snapshot-per-source': False, 'coalesce-sources': True})
        self.assertEqual(readiness.snapshot_jobs(config), 2)
        config['backup']['snapshots'][0]['snapshot-per-source'] = True
        with self.assertRaisesRegex(ValueError, 'conflicting'):
            readiness.snapshot_jobs(config)
        config['backup']['snapshots'][0]['snapshot-per-source'] = 'false'
        with self.assertRaisesRegex(ValueError, 'booleans'):
            readiness.snapshot_jobs(config)

    @contextlib.contextmanager
    def production_contract(self):
        with tempfile.TemporaryDirectory(prefix='r1c-') as temporary:
            base = Path(temporary)
            root = base / 'private'
            root.mkdir(mode=0o700)
            (root / 'repo').mkdir(mode=0o700)
            original = base / 'original'
            original.mkdir()
            sources = []
            for number in range(52):
                source = base / ('source-' + str(number))
                source.mkdir()
                sources.append(str(source))
            profile = base / 'backup.toml'
            profile.write_text('[backup]\nglobs = ["!**/.snapshot", "!**/.snapshot/**", '
                               '"!**/.Trash-*/**", "!**/lost+found/**"]\n'
                               'exclude-if-present = ["CACHEDIR.TAG", ".nobackup"]\n'
                               '[[backup.snapshots]]\nname = "all"\none-file-system = true\nsources = '
                               + json.dumps(sources) + '\n')
            empty = base / 'empty.toml'
            empty.write_text('')
            password = base / 'password'
            password.write_text('test-only\n')
            entries = []
            for section in ('index', 'snapshots'):
                (original / section).mkdir()
                (root / 'repo' / section).mkdir()
                for number in range(2):
                    source = original / section / str(number)
                    source.write_text(section + str(number))
                    shutil.copyfile(source, root / 'repo' / section / str(number))
                    entries.append({'path': str(source), 'size': source.stat().st_size,
                                    'sha256': readiness.pilot.digest(source)})
            inventory = base / 'inventory.jsonl'
            inventory.write_text(''.join(json.dumps(entry) + '\n' for entry in entries))
            expected = {'indexes': 2, 'snapshots': 2, 'packs': 3, 'blobs': 5}
            for name in ('config', 'keys/key', 'data/pack1', 'data/pack2', 'data/pack3'):
                source = original / name
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text(name)
                candidate = root / 'repo' / name
                candidate.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, candidate)
                entries.append({'path': str(source), 'size': source.stat().st_size,
                                'sha256': readiness.pilot.digest(source)})
            inventory.write_text(''.join(json.dumps(entry) + '\n' for entry in entries))
            plan = {'schema': readiness.SCHEMA, 'authorization': 'private-target-only',
                    'root': str(root), 'target_identity': {
                        'device': root.stat().st_dev, 'inode': root.stat().st_ino,
                        'repo_device': (root / 'repo').stat().st_dev, 'repo_inode': (root / 'repo').stat().st_ino},
                    'expected': expected, 'password_file': str(password), 'pins': {}, 'jobs': 1,
                    'selection': readiness.pilot.source_selection(readiness.pilot.tomllib.loads(profile.read_text())),
                    'socket': str(root / 'd.sock'), 'working_limit': '160GiB', 'scratch_limit': '8GiB',
                    'restores': [{'snapshot': 'a' * 64}],
                    'limits': {'rss_bytes': readiness.pilot.RSS_LIMIT, 'sample_seconds': 0.25,
                               'import_seconds': 43200, 'check_seconds': 43200, 'backup_seconds': 604800,
                               'disk_free_bytes': 57_481_929_687_040, 'artifact_bytes': 8 << 40,
                               'stream_bytes': 32 << 20, 'sample_bytes': 3 << 30, 'telemetry_bytes': 20 << 30}}
            for name, path in (('binary', Path(sys.executable)), ('daemon', Path(sys.executable)),
                               ('profile', profile), ('source_profile', profile), ('empty_profile', empty),
                               ('password', password), ('inventory', inventory), ('supervisor', Path(readiness.__file__)),
                               ('pilot', Path(readiness.pilot.__file__)), ('observer', Path(readiness.observe.__file__))):
                plan['pins'][name] = {'path': str(path), 'sha256': readiness.pilot.digest(path)}
            contract = base / 'contract.json'
            contract.write_text(json.dumps(plan))
            with patch.object(readiness.pilot, 'AUTH_ROOT', base), \
                    patch.object(readiness.pilot, 'SOURCE_REPO', original), \
                    patch.object(readiness, 'EXPECTED', expected):
                yield contract, plan

    def test_contract_input_scope_identity_and_capacity(self):
        with self.production_contract() as (contract, plan):
            self.assertEqual(readiness.load_contract(contract), plan)
            for field, invalid in (('jobs', 0), ('expected', {}), ('target_identity', {}),
                                   ('authorization', 'production'), ('restores', [{'snapshot': 'latest'}])):
                changed = dict(plan, **{field: invalid})
                contract.write_text(json.dumps(changed))
                with self.subTest(field=field), self.assertRaises((ValueError, KeyError)):
                    readiness.load_contract(contract)
            changed = dict(plan, limits=dict(plan['limits'], sample_bytes=2 << 20))
            contract.write_text(json.dumps(changed))
            with self.assertRaisesRegex(ValueError, 'capacity'):
                readiness.load_contract(contract)
            contract.write_text(json.dumps(plan))
            Path(plan['pins']['profile']['path']).write_text('changed')
            with self.assertRaisesRegex(ValueError, 'frozen input changed'):
                readiness.load_contract(contract)

    def test_writable_target_redirects_and_shared_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'file').write_text('private')
            (root / 'redirect').symlink_to('/tmp')
            with self.assertRaises(ValueError):
                readiness.private_tree(root)
            (root / 'redirect').unlink()
            os.link(root / 'file', root / 'shared')
            with self.assertRaises(ValueError):
                readiness.private_tree(root)

    def test_seal_is_create_only_and_never_executes(self):
        with self.production_contract() as (contract, plan):
            output = contract.parent / 'sealed.json'
            with patch.object(readiness, 'run_observed') as run:
                digest = readiness.seal(contract, output)
                run.assert_not_called()
            self.assertEqual(digest, readiness.fingerprint(readiness.load_contract(output)))
            with self.assertRaises(FileExistsError):
                readiness.seal(contract, output)

    def test_acknowledgment_survives_malformed_tail(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'stdout'
            output.write_text(json.dumps({'message_type': 'summary', 'snapshot_id': 'a' * 64}) + '\n{partial')
            with self.assertRaises(readiness.EventError) as caught:
                readiness.json_events(output)
            self.assertEqual(caught.exception.events['summaries'][0]['snapshot_id'], 'a' * 64)

    def test_pid_reuse_and_identity_capacity(self):
        processes = {}
        readiness.retain_process(processes, {'pid': 10, 'start_ticks': 100})
        with self.assertRaisesRegex(ValueError, 'PID reused'):
            readiness.retain_process(processes, {'pid': 10, 'start_ticks': 101})
        for pid in range(11, 11 + readiness.MAX_PROCESSES - 1):
            readiness.retain_process(processes, {'pid': pid, 'start_ticks': 100})
        with self.assertRaisesRegex(ValueError, 'capacity'):
            readiness.retain_process(processes, {'pid': 99, 'start_ticks': 100})

    def test_output_capacity_and_runtime_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ('tmp', 'working', 'capacity', 'timeout'):
                (root / name).mkdir()
            plan = {'root': str(root), 'limits': {'stream_bytes': 128, 'sample_bytes': 4 << 20,
                    'rss_bytes': 1 << 30, 'sample_seconds': 0.25, 'disk_free_bytes': 0}}
            result = readiness.run_observed([sys.executable, '-c', 'print("x" * 1024)'], root / 'capacity', 10, plan)
            self.assertFalse(result['observation_complete'])
            self.assertTrue(any('capacity' in error for error in result['errors']))
            self.assertLessEqual((root / 'capacity/stdout.log').stat().st_size, 128)
            result = readiness.run_observed([sys.executable, '-c', 'import signal; signal.pause()'],
                                            root / 'timeout', 0.01, plan)
            self.assertFalse(result['observation_complete'])
            self.assertTrue(any('ceiling' in error for error in result['errors']))

    def test_external_process_identity_is_accounted_but_never_stopped(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ('tmp', 'working', 'valid', 'invalid'):
                (root / name).mkdir()
            sample = readiness.observe.process_sample(os.getpid())
            plan = {'root': str(root), 'limits': {'stream_bytes': 1 << 20, 'sample_bytes': 4 << 20,
                    'rss_bytes': 1 << 30, 'sample_seconds': 0.25, 'disk_free_bytes': 0},
                    'external_processes': [{'pid': os.getpid(), 'start_ticks': sample['start_ticks']}]}
            result = readiness.run_observed([sys.executable, '-c', 'print("ok")'], root / 'valid', 10, plan)
            self.assertTrue(result['observation_complete'], result['errors'])
            self.assertGreaterEqual(result['peaks']['rss_bytes'], sample['rss_bytes'])
            plan['external_processes'][0]['start_ticks'] += 1
            result = readiness.run_observed([sys.executable, '-c', 'import signal; signal.pause()'],
                                            root / 'invalid', 10, plan)
            self.assertFalse(result['observation_complete'])
            self.assertTrue(any('identity changed' in error for error in result['errors']))

    def test_committed_backup_survives_later_observation_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = {'root': str(root), 'selection': {'sources': []}, 'restores': [],
                    'limits': {'import_seconds': 43200, 'check_seconds': 43200, 'backup_seconds': 604800}}
            observation = {'events': {'summaries': [{'snapshot_id': 'a' * 64}]},
                           'observation_complete': False, 'errors': ['lost final safety sample']}
            with patch.object(readiness, 'load_contract', return_value=plan), \
                    patch.object(readiness.pilot, 'verify_read_only'), \
                    patch.object(readiness, 'STAGES', ('backup', 'restore')), \
                    patch.object(readiness, 'command', return_value=['fixture']), \
                    patch.object(readiness, 'run_observed', return_value=observation) as run:
                receipt = readiness.execute(root / 'plan')
            self.assertTrue(receipt['backup_committed'])
            self.assertEqual(receipt['acknowledged_snapshot_ids'], ['a' * 64])
            self.assertFalse(receipt['observation_complete'])
            self.assertFalse(receipt['verification_passed'])
            self.assertFalse(receipt['readiness_accepted'])
            self.assertEqual(run.call_count, 1)

    def test_strict_counters(self):
        for value in (True, -1, 1.5, '1', None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                readiness.integer(value, 'counter')

    def test_cost_summary_does_not_invent_rates_or_bottlenecks(self):
        empty = readiness.cost_summary([])
        self.assertIsNone(empty['logical_processed_bytes_per_second'])
        self.assertIsNone(empty['peak_combined_rss_bytes'])
        self.assertIsNone(empty['bottleneck_conclusions'])
        summary = readiness.cost_summary([{'stage': 'backup', 'observation': {
            'wall_seconds': 2, 'peaks': {'rss_bytes': 123}},
            'events': {'summaries': [{'total_bytes_processed': 10}]}}])
        self.assertEqual(summary['logical_processed_bytes_per_second'], 5)
        self.assertEqual(summary['peak_combined_rss_bytes'], 123)
        invalid = readiness.cost_summary([{'stage': 'backup', 'observation': {
            'wall_seconds': 2, 'peaks': {'rss_bytes': 123}},
            'events': {'summaries': [{'total_bytes_processed': None}]}}])
        self.assertIsNone(invalid['logical_processed_bytes'])
        self.assertIsNone(invalid['logical_processed_bytes_per_second'])

    def test_json_event_bounds(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'events'
            for content in ('{bad}\n', '{}', 'x' * (readiness.MAX_LINE + 1)):
                output.write_text(content)
                with self.assertRaises(ValueError):
                    readiness.json_events(output)
            output.write_text(json.dumps({'message_type': 'reconciliation_stats', 'failed': 0}) + '\n')
            self.assertEqual(readiness.json_events(output)['reconciliation_stats']['failed'], 0)

    def test_catalog_scope_and_omitted_history(self):
        plan = {'expected': {'indexes': 2, 'packs': 3, 'blobs': 5, 'snapshots': 4}}
        catalog = {'errors': 0, 'warnings': 0, 'trees_visited': 0, 'nodes_visited': 0,
                   'crawl_debt_created': 0, 'indexes_total': 2, 'indexes_seen': 2,
                   'indexes_imported': 2, 'packs_imported': 3, 'blobs_imported': 5}
        catalog['checkpoint'] = 'a' * 64
        readiness.validate_stage(plan, 'catalog', {'import': catalog}, None)
        for field in catalog:
            invalid = dict(catalog)
            invalid.pop(field)
            with self.subTest(field=field), self.assertRaises((KeyError, ValueError)):
                readiness.validate_stage(plan, 'catalog', {'import': invalid}, None)
        with self.assertRaises((KeyError, ValueError)):
            readiness.validate_stage(plan, 'snapshot-references', {'import': catalog}, None)

    def test_resumed_catalog_and_snapshot_completion(self):
        plan = {'expected': {'indexes': 2, 'packs': 3, 'blobs': 5, 'snapshots': 4}}
        result = {'errors': 0, 'warnings': 0, 'trees_visited': 0, 'nodes_visited': 0,
                  'crawl_debt_created': 0, 'indexes_total': 2, 'indexes_seen': 2,
                  'indexes_imported': 0, 'indexes_resumed': 2, 'packs_imported': 0,
                  'blobs_imported': 0, 'checkpoint': 'a' * 64,
                  'snapshot_metadata_only': True, 'snapshots_total': 4, 'snapshots_seen': 4,
                  'snapshots_imported': 1, 'snapshots_resumed': 3}
        readiness.validate_stage(plan, 'catalog', {'import': result}, None)
        readiness.validate_stage(plan, 'snapshot-references', {'import': result}, None)
        for field in ('indexes_resumed', 'snapshots_resumed'):
            invalid = dict(result, **{field: 1})
            stage = 'catalog' if field == 'indexes_resumed' else 'snapshot-references'
            with self.subTest(field=field), self.assertRaises(ValueError):
                readiness.validate_stage(plan, stage, {'import': invalid}, None)

    def test_live_crawl_uses_current_baseline_without_granting_readiness(self):
        config = {'backup': {'snapshot-per-source': True, 'snapshots': [
            {'sources': ['/a', '/b'], 'one-file-system': True}],
            'globs': ['!**/.snapshot', '!**/.snapshot/**'], 'exclude-if-present': []}}
        check = {'coverage': {'complete': True, 'mode': 'full'}, 'warnings': 0,
                 'slatedb_snapshots': 145, 'legacy_indexes': 10171,
                 'legacy_locations': 380166898, 'slatedb_locations': 380166898,
                 'imported_packs': 419854,
                 'consistency': {'repository_id': 'repo', 'generation': 1,
                                 'session_id': 'session', 'options_digest': 'digest',
                                 'schema_version': 0, 'sequence': 1}}
        for key in ('pending_crawl_debt', 'pending_exports', 'failed_exports', 'missing_in_slatedb',
                    'missing_in_legacy', 'missing_packs', 'invalid_packs', 'aggregate_mismatches',
                    'reverse_edge_mismatches', 'snapshot_mismatches', 'snapshot_commit_mismatches',
                    'path_version_mismatches', 'missing_placement_records', 'backend_pack_mismatches',
                    'derived_tier_mismatches', 'packs_below_durability', 'verification_state_mismatches',
                    'analytics_mismatches'):
            check[key] = 0
        events = {'summaries': [{'snapshot_id': letter * 64, 'total_files_processed': 1,
                                 'total_bytes_processed': 1} for letter in ('a', 'b')],
                  'reconciliations': [dict.fromkeys(
                      ('deferred', 'failed', 'failed_canceled', 'scanned', 'reused', 'changed', 'reconciled'), 0)] * 2}
        final = dict(check, slatedb_snapshots=147)
        result = readiness.validate_live_crawl(config, {'index_check': check}, events, {'index_check': final})
        self.assertTrue(result['crawl_metadata_validated'])
        self.assertFalse(result['readiness_accepted'])
        for changed in (dict(final, slatedb_snapshots=146), dict(final, imported_packs=1)):
            with self.assertRaises(ValueError):
                readiness.validate_live_crawl(config, {'index_check': check}, events, {'index_check': changed})

    def test_generation_and_full_coverage(self):
        plan = {'repository_id': 'repo', 'expected': {'snapshots': 4}}
        check = {'coverage': {'complete': True, 'mode': 'full'}, 'warnings': 0,
                 'pending_crawl_debt': 0, 'pending_exports': 0, 'failed_exports': 0,
                 'slatedb_snapshots': 4,
                 'consistency': {'repository_id': 'repo', 'generation': 1}}
        check['consistency'].update({'session_id': 'session', 'options_digest': 'digest',
                             'schema_version': 1, 'sequence': 0})
        for key in ('missing_in_slatedb', 'missing_in_legacy', 'missing_packs', 'invalid_packs',
                    'aggregate_mismatches', 'reverse_edge_mismatches', 'snapshot_mismatches',
                    'snapshot_commit_mismatches', 'path_version_mismatches', 'missing_placement_records',
                    'backend_pack_mismatches', 'derived_tier_mismatches', 'packs_below_durability',
                    'verification_state_mismatches', 'analytics_mismatches', 'plaintext_objects',
                    'invalid_encrypted_objects'):
            check[key] = 0
        self.assertEqual(readiness.validate_stage(plan, 'metadata-check', {'index_check': check}, None), ('repo', 1, 0, 'digest'))
        for identity in (('repo', 2), ('other', 1)):
            with self.subTest(identity=identity), self.assertRaises(ValueError):
                readiness.validate_stage(plan, 'activation-check', {'index_check': check}, identity)
        with self.assertRaisesRegex(ValueError, 'changed after checks'):
            readiness.validate_stage(plan, 'activation-check', {'index_check': check}, ('repo', 1, 7, 'digest'))
        check['coverage']['complete'] = False
        with self.assertRaises(ValueError):
            readiness.validate_stage(plan, 'metadata-check', {'index_check': check}, None)

    def test_no_downstream_action_or_receipt_reuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = {'root': str(root), 'selection': {'sources': []}, 'restores': [],
                    'limits': {'import_seconds': 43200, 'check_seconds': 43200},
                    'pins': {}}
            with patch.object(readiness, 'load_contract', return_value=plan), \
                    patch.object(readiness.pilot, 'verify_read_only'), \
                    patch.object(readiness, 'command', return_value=['fixture']), \
                    patch.object(readiness, 'run_observed', side_effect=ValueError('lost safety observer')) as run:
                receipt = readiness.execute(root / 'plan.json')
                self.assertEqual(run.call_count, 1)
                self.assertFalse(receipt['readiness_accepted'])
                self.assertFalse(receipt['backup_committed'])
                self.assertIn('lost safety observer', receipt['failure'])
                with self.assertRaises(FileExistsError):
                    readiness.execute(root / 'plan.json')

    def test_real_child_success_and_collector_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'tmp').mkdir()
            (root / 'working').mkdir()
            plan = {'root': str(root), 'limits': {'stream_bytes': 4 << 20,
                    'sample_bytes': 4 << 20, 'rss_bytes': 1 << 30,
                    'sample_seconds': 0.25, 'disk_free_bytes': 0}}
            success = root / 'success'
            success.mkdir()
            result = readiness.run_observed([sys.executable, '-c', 'print("ok")'], success, 10, plan)
            self.assertTrue(result['observation_complete'])
            self.assertTrue((success / 'samples.jsonl').stat().st_size)
            fail = root / 'fail'
            fail.mkdir()
            with patch.object(readiness.pilot, 'process_group_rss', side_effect=ValueError('unavailable')):
                result = readiness.run_observed([sys.executable, '-c', 'print("never")'], fail, 10, plan)
                self.assertFalse(result['observation_complete'])
            self.assertFalse(json.loads((fail / 'observation.json').read_text())['observation_complete'])
            scratch = root / 'scratch'
            scratch.mkdir()
            with patch.object(readiness, 'tree_bytes', return_value=5 * 1024 ** 3):
                result = readiness.run_observed([sys.executable, '-c', 'print("bounded")'], scratch, 10, plan)
            self.assertFalse(result['observation_complete'])
            self.assertIn('ValueError: working scratch ceiling exceeded', result['errors'])
            expanded = root / 'expanded'
            expanded.mkdir()
            plan['limits']['scratch_bytes'] = 96 << 30
            with patch.object(readiness, 'tree_bytes', return_value=5 << 30):
                result = readiness.run_observed([sys.executable, '-c', 'print("bounded")'], expanded, 10, plan)
            self.assertTrue(result['observation_complete'], result['errors'])
            self.assertEqual(result['scratch_peak_bytes'], 10 << 30)

    def test_slow_filesystem_diagnostics_do_not_block_safety_sampling(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ('tmp', 'working', 'observation'):
                (root / name).mkdir()
            plan = {'root': str(root), 'limits': {'stream_bytes': 4 << 20,
                    'sample_bytes': 4 << 20, 'rss_bytes': 1 << 30,
                    'sample_seconds': 0.1, 'disk_free_bytes': 0}}
            usage = shutil.disk_usage(root)
            def delayed_usage(path):
                threading.Event().wait(1.2)
                return usage
            with patch.object(readiness.shutil, 'disk_usage', side_effect=delayed_usage):
                result = readiness.run_observed([sys.executable, '-c',
                    'import threading; threading.Event().wait(2)'], root / 'observation', 10, plan)
            self.assertTrue(result['observation_complete'], result['errors'])
            records = [json.loads(line) for line in (root / 'observation/samples.jsonl').read_text().splitlines()]
            elapsed = [record['elapsed_seconds'] for record in records if not record.get('cleanup')]
            self.assertGreater(len(elapsed), 10)
            self.assertLess(max(right - left for left, right in zip(elapsed, elapsed[1:])), 1)
            sampler = readiness.safety_process_sample
            def delayed_sampler(*args):
                threading.Event().wait(1.2)
                return sampler(*args)
            for name, override in (
                    ('sampler-stall', patch.object(readiness, 'safety_process_sample', side_effect=delayed_sampler)),
                    ('queue-full', patch.object(readiness.queue, 'Queue', return_value=readiness.queue.Queue(maxsize=1)))):
                target = root / name
                target.mkdir()
                with override, patch.object(readiness.shutil, 'disk_usage', side_effect=delayed_usage):
                    result = readiness.run_observed([sys.executable, '-c',
                        'import threading; threading.Event().wait(5)'], target, 10, plan)
                self.assertFalse(result['observation_complete'])
                expected = ('mandatory safety sampling gap exceeded one second' if name == 'sampler-stall'
                            else 'safety sample queue capacity exceeded')
                self.assertTrue(any(expected in error for error in result['errors']), result['errors'])
                self.assertTrue(result['cleanup']['complete'])

    def test_mandatory_sampling_does_not_read_optional_io_diagnostics(self):
        sample = readiness.observe.process_sample(os.getpid(), diagnostics=False)
        self.assertIsNone(sample['io'])
        self.assertIsNone(sample['schedstat'])
        with patch.object(readiness.observe, 'optional_text', side_effect=AssertionError('optional I/O on safety path')):
            rss, processes = readiness.safety_process_sample(os.getpgrp(), {}, {}, {}, {})
        self.assertGreater(rss, 0)
        self.assertTrue(any(item['pid'] == os.getpid() for item in processes))
        diagnostics = readiness.process_diagnostics({os.getpid(): sample})
        self.assertEqual(diagnostics[0]['start_ticks'], sample['start_ticks'])
        self.assertIsNotNone(diagnostics[0]['io'])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ('tmp', 'working', 'observation'):
                (root / name).mkdir()
            plan = {'root': str(root), 'limits': {'stream_bytes': 4 << 20,
                    'sample_bytes': 4 << 20, 'rss_bytes': 1 << 30,
                    'sample_seconds': 0.1, 'disk_free_bytes': 0}}
            collector = readiness.process_diagnostics
            def blocked_diagnostics(processes):
                threading.Event().wait(1.2)
                return collector(processes)
            with patch.object(readiness, 'process_diagnostics', side_effect=blocked_diagnostics):
                result = readiness.run_observed([sys.executable, '-c',
                    'import threading; threading.Event().wait(2)'], root / 'observation', 10, plan)
            self.assertTrue(result['observation_complete'], result['errors'])
            records = [json.loads(line) for line in (root / 'observation/samples.jsonl').read_text().splitlines()]
            elapsed = [record['elapsed_seconds'] for record in records if not record.get('cleanup')]
            self.assertGreater(len(elapsed), 10)
            self.assertLess(max(right - left for left, right in zip(elapsed, elapsed[1:])), 1)

    def test_scratch_measurement_tolerates_deletion_but_rejects_redirection(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / 'run'
            run.write_bytes(b'1234')
            stable = root / 'stable'
            stable.write_bytes(b'123')
            original = Path.lstat
            def removed_during_measurement(path):
                if path == run:
                    path.unlink()
                return original(path)
            with patch.object(Path, 'lstat', autospec=True, side_effect=removed_during_measurement):
                self.assertEqual(readiness.tree_bytes(root), 3)
            for target in (stable, root):
                link = root / 'redirected'
                link.symlink_to(target)
                with self.assertRaisesRegex(ValueError, 'redirected working artifact'):
                    readiness.tree_bytes(root)
                link.unlink()
            def denied_measurement(path):
                if path == stable:
                    raise PermissionError('fixture denied')
                return original(path)
            with patch.object(Path, 'lstat', autospec=True, side_effect=denied_measurement), \
                    self.assertRaises(PermissionError):
                readiness.tree_bytes(root)

    def test_backup_cannot_hide_debt_or_uncertainty(self):
        summary = {'snapshot_id': 'a' * 64, 'total_files_processed': 2, 'total_bytes_processed': 3}
        stats = {'failed': 0, 'deferred': 0, 'failed_canceled': 0, 'scanned': 3,
             'reused': 1, 'changed': 2, 'reconciled': 2}
        readiness.validate_stage({'jobs': 1}, 'backup', {'summaries': [summary], 'reconciliations': [stats]}, None)
        for key in ('failed', 'deferred', 'failed_canceled'):
            invalid = dict(stats, **{key: 1})
            with self.subTest(key=key), self.assertRaises(ValueError):
                readiness.validate_stage({'jobs': 1}, 'backup', {'summaries': [summary], 'reconciliations': [invalid]}, None)
        summary.pop('snapshot_id')
        with self.assertRaises(ValueError):
            readiness.validate_stage({'jobs': 1}, 'backup', {'summaries': [summary], 'reconciliations': [stats]}, None)

    def test_multiple_snapshot_jobs_and_repository_summary(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'events'
            events = []
            for snapshot in ('a', 'b'):
                events += [{'message_type': 'reconciliation_stats',
                    'failed': 0, 'deferred': 0, 'failed_canceled': 0, 'scanned': 3,
                    'changed': 2, 'reused': 1, 'reconciled': 2},
                    {'message_type': 'summary', 'snapshot_id': snapshot * 64,
                     'total_files_processed': 2, 'total_bytes_processed': 3}]
            output.write_text(''.join(json.dumps(event) + '\n' for event in events))
            parsed = readiness.json_events(output)
            readiness.validate_stage({'jobs': 2}, 'backup', parsed, None)
            parsed['reconciliations'].pop()
            with self.assertRaises(ValueError):
                readiness.validate_stage({'jobs': 2}, 'backup', parsed, None)
            output.write_text(json.dumps({'message_type': 'summary', 'num_errors': 0}) + '\n')
            readiness.validate_stage({}, 'repository-check', readiness.json_events(output), None)

    @unittest.skipUnless(os.environ.get('VAULTIC_READINESS_TEST_BINARY') and os.environ.get('VAULTICDB_TEST_BINARY'),
                         'explicit current CLI and native binaries required')
    def test_real_catalog_first_supervisor(self):
        with tempfile.TemporaryDirectory(prefix='r1-', dir=os.environ.get('VAULTIC_TEST_TMPDIR')) as temporary, \
            tempfile.TemporaryDirectory(prefix='r1s-', dir='/tmp') as runtime:
            root = Path(temporary)
            os.chmod(root, 0o700)
            for name in ('source', 'tmp', 'working', 'cache'):
                (root / name).mkdir(mode=0o700)
            (root / 'source/file').write_text('legacy and live content\n')
            (root / 'password').write_text('fixture-password\n')
            (root / 'empty.toml').write_text('')
            (root / 'backup.toml').write_text('[backup]\nglobs = ["!**/.snapshot", "!**/.snapshot/**"]\n'
                                             '[[backup.snapshots]]\nname = "fixture"\nsources = [' + json.dumps(str(root / 'source')) + ']\n'
                                             'one-file-system = true\n')
            binary = os.environ['VAULTIC_READINESS_TEST_BINARY']
            daemon = os.environ['VAULTICDB_TEST_BINARY']
            base = [binary, '--repo=' + str(root / 'repo'), '--password-file=' + str(root / 'password'),
                    '--use-profile=' + str(root / 'empty.toml'), '--no-cache', '--json']
            environment = {key: value for key, value in os.environ.items() if not key.startswith('VAULTIC_')}
            def invoke(arguments):
                result = subprocess.run(base + arguments, env=environment, capture_output=True, timeout=60)
                self.assertEqual(result.returncode, 0, result.stderr.decode()[-8000:])
                return result.stdout.decode()
            invoke(['init'])
            invoke(['backup', str(root / 'source')])
            indexes = [path for path in (root / 'repo/index').iterdir() if path.is_file()]
            snapshots = [path.name for path in (root / 'repo/snapshots').iterdir() if path.is_file()]
            packs = []
            for index in indexes:
                packs += json.loads(invoke(['cat', 'index', index.name]))['packs']
            repository_id = json.loads(invoke(['cat', 'config']))['id']
            plan = {'root': str(root), 'pins': {}, 'password_file': str(root / 'password'),
                    'socket': str(Path(runtime) / 'd.sock'), 'jobs': 1, 'repository_id': repository_id,
                    'working_limit': '160GiB', 'scratch_limit': '8GiB',
                    'selection': {'sources': [str(root / 'source')]},
                    'restores': [{'snapshot': snapshot} for snapshot in snapshots],
                    'expected': {'indexes': len(indexes), 'packs': len(packs),
                                 'blobs': sum(len(pack['blobs']) for pack in packs), 'snapshots': len(snapshots)},
                    'limits': {'import_seconds': 60, 'check_seconds': 60, 'backup_seconds': 60,
                               'stream_bytes': 16 << 20, 'sample_bytes': 16 << 20,
                               'sample_seconds': 0.25, 'rss_bytes': readiness.pilot.RSS_LIMIT,
                               'disk_free_bytes': 0}}
            for name, path in (('binary', binary), ('daemon', daemon), ('profile', root / 'backup.toml'),
                               ('empty_profile', root / 'empty.toml')):
                plan['pins'][name] = {'path': str(path), 'sha256': readiness.pilot.digest(Path(path))}
            observed = readiness.run_observed
            def guarded_observation(argv, directory, seconds, contract):
                if '--activate' in argv:
                    for flag in ('activation-generation', 'activation-sequence', 'activation-snapshots'):
                        invalid = list(argv)
                        offset = next(number for number, argument in enumerate(invalid) if argument.startswith('--' + flag + '='))
                        invalid[offset] = '--' + flag + '=' + str(int(invalid[offset].split('=')[1]) + 1)
                        rejected = root / ('rejected-' + flag)
                        rejected.mkdir()
                        result = observed(invalid, rejected, seconds, contract)
                        self.assertFalse(result['observation_complete'], flag)
                        self.assertNotEqual(result['exit_code'], 0, flag)
                        self.assertIn('catalog-first activation', (rejected / 'stderr.log').read_text())
                return observed(argv, directory, seconds, contract)
            with patch.object(readiness, 'load_contract', return_value=plan), \
                    patch.object(readiness.pilot, 'verify_read_only'), \
                    patch.object(readiness, 'run_observed', side_effect=guarded_observation):
                receipt = readiness.execute(root / 'fixture-contract.json')
            failures = []
            for directory in (root / 'readiness').iterdir():
                if directory.is_dir() and not (directory / 'verified.json').exists():
                    for name in ('stdout.log', 'stderr.log'):
                        if (directory / name).exists():
                            failures.append(directory.name + '/' + name + ': ' + (directory / name).read_text()[-8000:])
            evidence = os.environ.get('VAULTIC_READINESS_EVIDENCE_ROOT')
            if evidence:
                target = Path(evidence) / root.name
                shutil.copytree(root, target, ignore=shutil.ignore_patterns('*.sock', '*.lock'))
                readiness.pilot.write_json(target / 'fixture-identity.json', {
                    'source_path': str(root / 'source'), 'source_device': (root / 'source').stat().st_dev,
                    'native_sha256': readiness.pilot.digest(Path(daemon)),
                    'cli_sha256': readiness.pilot.digest(Path(binary))})
            self.assertTrue(receipt['readiness_accepted'], str(receipt.get('failure')) + '\n' + '\n'.join(failures))
            self.assertEqual([outcome['stage'] for outcome in receipt['outcomes']], list(readiness.STAGES) + ['restore'])
            self.assertTrue(receipt['backup_committed'])
            self.assertEqual(len(receipt['acknowledged_snapshot_ids']), 1)


if __name__ == '__main__':
    unittest.main()