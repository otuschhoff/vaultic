import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess


ROOT = Path(__file__).resolve().parent
PARENT = Path('/volume2/NASDA2/vaultic-test-m6/pilot-associative-12d7raqg')
SOURCE = Path('/run/vaultic-diagnostics/m6-nextsteps-lNVpPX')
WORKSPACE = Path('/root/proj/vaultic')
BASE = '2c5061887600ac31c4a4e76c4cc065915b6644dc'
PILOT = runpy.run_path(str(ROOT / 'pilot.py'))
CHANGED = [
    'cmd/vaultic/indexcmd/legacy_import_monitor.go',
    'cmd/vaultic/indexcmd/legacy_import_monitor_test.go',
    'internal/backend/cache/backend.go', 'internal/backend/cache/cache.go',
    'internal/backend/cache/file.go', 'internal/backend/cache/file_test.go',
    'internal/backend/cache/pack_read_session.go',
    'internal/index/legacyimport/import_metrics.go', 'internal/index/legacyimport/import_test.go',
    'internal/index/legacyimport/tree.go', 'internal/index/legacyimport/tree_cache.go',
    'internal/index/legacyimport/snapshot_diagnostic.go', 'internal/index/legacyimport/snapshot_mechanism.go',
    'internal/index/legacyimport/snapshot_equal_work_test.go', 'internal/telemetry/monitor_schema.go',
]


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def pin():
    os.umask(0o077)
    assert not (ROOT / 'LAUNCH-SHA256SUMS').exists()
    assert not any((ROOT / name).exists() for name in ('equal-work', 'pebble', 'ram', 'kv', 'auto'))
    for manifest in ('LAUNCH-SHA256SUMS', 'RAM-TIMEOUT-SHA256SUMS'):
        subprocess.run(['sha256sum', '--check', '--status', manifest], cwd=PARENT, check=True)
        shutil.copyfile(PARENT / manifest, ROOT / ('PARENT-' + manifest))
    plan = json.loads((PARENT / 'plan.json').read_text())
    assert plan['minimum_disk_free_bytes'] == 57481929687040
    assert plan['rss_limit_bytes'] == 200000000000
    assert len(plan['selection']['sources']) == 52
    assert all(Path(path).is_dir() for path in plan['selection']['sources'])
    assert shutil.disk_usage(ROOT).free >= plan['minimum_disk_free_bytes']
    for name in ('backup.toml', 'empty.toml', 'source-inventory.json'):
        shutil.copyfile(PARENT / name, ROOT / name)
    inventory = PILOT['make_view'](Path(plan['source_repo']), ROOT / 'source-view')
    assert inventory == json.loads((ROOT / 'source-inventory.json').read_text())
    assert len(inventory) == 10314
    patch_parts = []
    hashes = {}
    for relative in CHANGED:
        before = ROOT / 'source-base' / relative
        after = ROOT / 'source-candidate' / relative
        before.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        after.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        original = subprocess.run(['git', '-C', str(WORKSPACE), 'show', BASE + ':' + relative], capture_output=True)
        if original.returncode == 0:
            before.write_bytes(original.stdout)
            before_path = str(before)
        else:
            assert not subprocess.run(['git', '-C', str(WORKSPACE), 'ls-tree', BASE, '--', relative], capture_output=True, check=True).stdout
            before_path = '/dev/null'
        shutil.copyfile(SOURCE / relative, after)
        hashes[relative] = digest(after)
        difference = subprocess.run(['git', 'diff', '--no-index', before_path, str(after)], capture_output=True, text=True)
        assert difference.returncode in (0, 1), difference.stderr
        lines = []
        for line in difference.stdout.splitlines(keepends=True):
            if line.startswith(('diff --git ', '--- ', '+++ ')):
                line = line.replace('a' + str(ROOT / 'source-base') + '/', 'a/')
                line = line.replace('a' + str(ROOT / 'source-candidate') + '/', 'a/')
                line = line.replace('b' + str(ROOT / 'source-candidate') + '/', 'b/')
            lines.append(line)
        patch_parts.append(''.join(lines))
    (ROOT / 'source.patch').write_text(''.join(patch_parts))
    subprocess.run(['git', 'apply', '--check', str(ROOT / 'source.patch')], cwd=ROOT / 'source-base', check=True)
    subprocess.run(['git', 'apply', str(ROOT / 'source.patch')], cwd=ROOT / 'source-base', check=True)
    assert all(digest(ROOT / 'source-base' / relative) == expected for relative, expected in hashes.items())
    assert all(digest(SOURCE / relative) == expected for relative, expected in hashes.items())
    for script, log in [('pilot.py', 'runner-gate.log'), ('observer-v2.py', 'observer-gate.log'), ('nextsteps.py', 'controller-gate.log')]:
        with (ROOT / log).open('x') as stream:
            subprocess.run(['/usr/bin/python3.12', str(ROOT / script), '--selftest'], stdout=stream, stderr=subprocess.STDOUT, check=True)
    for log in ('equal-work-native-race.log', 'reader-session-race.log', 'package-gate.log'):
        text = (ROOT / log).read_text()
        assert 'FAIL' not in text and 'SKIP' not in text
    native_gate = (ROOT / 'equal-work-native-race.log').read_text()
    assert sum(line.startswith('--- PASS: TestM6Snapshot') for line in native_gate.splitlines()) == 13
    for name in ('cache-admitted-false', 'cache-admitted-true'):
        assert '--- PASS: TestM6SnapshotDebtBatchNativeReopen/' + name in native_gate
    assert '--- PASS: TestM6EqualWorkCorpusDeterminism' in native_gate
    for relative in ('vaultic', 'vaulticdb', 'equal-work.test'):
        os.chmod(ROOT / 'bin' / relative, 0o700)
    assert digest(ROOT / 'bin/vaulticdb') == plan['daemon_sha256']
    plan.update(root=str(ROOT), vaultic_sha256=digest(ROOT / 'bin/vaultic'), runner_sha256=digest(ROOT / 'pilot.py'))
    PILOT['write_json'](ROOT / 'plan.json', plan)
    assert PILOT['load_plan'](ROOT) == plan
    status = subprocess.check_output(['systemctl', 'show', 'vaulticdb-rustic.service', '--property=ActiveState,SubState,MainPID'], text=True)
    production = dict(line.split('=', 1) for line in status.splitlines())
    assert production['ActiveState'] == 'active' and production['SubState'] == 'running'
    pid = int(production['MainPID'])
    assert pid == 522 and digest(Path('/proc') / str(pid) / 'exe') == plan['daemon_sha256']
    contract = {
        'schema': 'phase35-m6-scoped-pack-reader-candidate-v1',
        'authorization': 'operator: implement all five recommendations; preserve existing input scope, exclusions and limits',
        'source_base_commit': BASE, 'source_file_sha256': hashes, 'source_patch_sha256': digest(ROOT / 'source.patch'),
        'isolated_source': str(SOURCE), 'main_wip_head_not_used': True,
        'go_version': subprocess.check_output(['go', 'version'], text=True).strip(),
        'cli_sha256': plan['vaultic_sha256'], 'native_sha256': plan['daemon_sha256'],
        'diagnostic_sha256': digest(ROOT / 'bin/equal-work.test'), 'production_pid': pid,
        'implementation': {'snapshot_scope_only': True, 'linux_only': True, 'maximum_retained_descriptors': 64,
                           'managed_reservation_bytes': 64 * 8192, 'reservation_denied_fallback': True,
                           'positional_independent_readers': True, 'active_readers_pinned': True,
                           'byte_checks_and_hash_checks_unchanged': True, 'save_remove_invalidation': True,
                           'release_after_last_active_reader': True, 'native_engine_unchanged': True},
        'diagnostic': {'fresh_complete_source_and_native_catalog': True, 'five_paired_rounds': True,
                       'order_alternates': True, 'maximum_trees': 512, 'maximum_complete_tree_nodes': 50000,
                       'maximum_encoded_corpus_bytes': 64 << 20, 'working_reservation_bytes': 1 << 30,
                       'component_measurement_deadline_seconds': 1800, 'whole_run_deadline_seconds': 43200,
                       'separate_loader_decode_encoding_native_pipeline': True,
                       'exact_node_digest_and_debt_reopen_required': True, 'diagnostic_is_not_acceptance': True,
                       'pipeline_gain_required_median': 1.25, 'pipeline_gain_required_minimum': 1.10,
                       'at_least_four_pipeline_pairs_require': 1.20,
                       'fully_resident_native_page_cache_not_asserted': True},
        'runner': {'closed_console_safe': True, 'result_summary_flush_fsync': True,
                   'failure_summary_before_console_reporting': True, 'scope_failure_summary': True,
                   'fresh_state_only': True, 'historical_exit120_cause_proven': False},
        'acceptance_order': ['fresh RAM import', 'index check', 'repository check', 'init', 'all-source backup'],
        'no_full_acceptance_until_material_pipeline_gain': True, 'kv_auto_require_ram_evaluation': True,
        'limits_unchanged': True, 'source_selection_unchanged': True, 'no_snapshot_source_omissions': True,
        'no_failed_db_reuse': True, 'production_or_default_changes': False, 'acceptance_claimed': False,
    }
    PILOT['write_json'](ROOT / 'variant-contract.json', contract)
    sealed = ['bin/vaultic', 'bin/vaulticdb', 'bin/equal-work.test', 'plan.json', 'backup.toml', 'empty.toml',
              'source-inventory.json', 'source-view/config', 'pilot.py', 'observer-v2.py', 'nextsteps.py',
              'pin-candidate.py', '.nobackup', 'source.patch', 'variant-contract.json', 'REPAIR-CONTRACT.md',
              'equal-work-native-race.log', 'reader-session-race.log', 'package-gate.log', 'runner-gate.log',
              'observer-gate.log', 'controller-gate.log', 'cache-range-prototype.log',
              'reader-session-nfs-benchmark.log', 'PARENT-LAUNCH-SHA256SUMS', 'PARENT-RAM-TIMEOUT-SHA256SUMS']
    sealed.extend('source-candidate/' + relative for relative in CHANGED)
    with (ROOT / 'LAUNCH-SHA256SUMS').open('x') as stream:
        for relative in sealed:
            stream.write(digest(ROOT / relative) + '  ' + relative + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    PILOT['write_json'](ROOT / 'AUTHORIZATION.json', {
        'launch_sha256': digest(ROOT / 'LAUNCH-SHA256SUMS'), 'production_pid': pid,
        'authorized_actions': ['bounded full-catalog equal-work diagnostic', 'fresh RAM workflow only after passing gate'],
        'kv_auto_automatic': False, 'acceptance_already_established': False,
    })
    print(json.dumps({'root': str(ROOT), 'source_files': len(CHANGED), 'sealed_inputs': len(sealed),
                      'launch_sha256': digest(ROOT / 'LAUNCH-SHA256SUMS'), 'cli_sha256': plan['vaultic_sha256']}))


if __name__ == '__main__':
    pin()