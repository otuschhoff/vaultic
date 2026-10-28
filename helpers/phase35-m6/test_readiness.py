import json
import os
import shutil
from pathlib import Path
import sys
import subprocess
import tempfile
import contextlib
import unittest
from unittest.mock import patch

import readiness


class ReadinessTests(unittest.TestCase):
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
                   'packs_imported': 3, 'blobs_imported': 5}
        catalog['checkpoint'] = 'a' * 64
        readiness.validate_stage(plan, 'catalog', {'import': catalog}, None)
        for field in catalog:
            invalid = dict(catalog)
            invalid.pop(field)
            with self.subTest(field=field), self.assertRaises((KeyError, ValueError)):
                readiness.validate_stage(plan, 'catalog', {'import': invalid}, None)
        with self.assertRaises((KeyError, ValueError)):
            readiness.validate_stage(plan, 'snapshot-references', {'import': catalog}, None)

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