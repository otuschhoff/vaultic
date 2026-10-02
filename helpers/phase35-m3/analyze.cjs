const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');

function events(text) {
  return text.split('\n').filter(line => line.startsWith('{')).map(line => JSON.parse(line));
}

function requireCase(records, name) {
  assert.ok(records.some(record => record.Action === 'pass' && record.Test === name), `missing passed test: ${name}`);
  assert.ok(!records.some(record => record.Test === name && ['skip','fail'].includes(record.Action)), `skipped/failed test: ${name}`);
}

function analyze(root) {
  const read = name => fs.readFileSync(path.join(root,name),'utf8');
  const race = events(read('logs/race.jsonl'));
  assert.ok(!race.some(record => record.Action === 'fail'));
  const packages = ['workingkv','telemetry','crawl','index','index/maintenance','index/legacyimport'];
  for (const package of [...packages,'../cmd/vaultic/backupcmd','../cmd/vaultic/indexcmd']) {
    const suffix = package.startsWith('../') ? package.slice(3) : `internal/${package}`;
    assert.ok(race.some(record => record.Action === 'pass' && !record.Test && record.Package.endsWith(suffix)), `missing package: ${package}`);
  }
  for (const name of ['TestM3SecureMapParity','TestM3ManifestParity','TestM3OverlayParity','TestM3LookupCapacity','TestM3ImportParity','TestM3SplitImportParity','TestM3CheckParity','TestM3SorterParity']) {
    for (const mode of ['ram','kv']) requireCase(race, `${name}/${mode}`);
  }
  for (const name of ['TestM3RAMSpillRejection','TestM3SorterReservationTransfer']) requireCase(race,name);
  const daemon = events(read('logs/daemon.jsonl'));
  for (const mode of ['ram','kv']) requireCase(daemon,`TestM3FreshImportPolicyFallback/${mode}`);
  const native = events(read('logs/native.jsonl'));
  for (const mode of ['ram','kv']) requireCase(native,`TestCachedAuthoritativeLookupWithPublishedOverlay/${mode}`);
  const archiver = read('logs/archiver.log');
  assert.match(archiver,/^PASS$/m);
  assert.doesNotMatch(archiver,/--- FAIL:/);
  for (const name of ['TestM3BackupParity/ram','TestM3BackupParity/kv','TestM3BackupExhaustionDoesNotFallback','TestScannerCWalkSuppressedErrorFallsBack']) assert.ok(archiver.includes(`--- PASS: ${name}`), `missing archiver gate: ${name}`);
  for (const package of packages.filter(package => package !== 'telemetry')) {
    const label = package.replaceAll('/','-');
    for (const mode of ['ram','kv']) {
      const log = read(`logs/${label}-${mode}.log`);
      assert.match(log,/^PASS$/m);
      assert.match(log,/--- PASS: TestM3/,'no selected test executed');
      const trace = read(`logs/${label}-${mode}.trace`);
      assert.match(trace,/execve\(/);
      assert.match(trace,/\+\+\+ exited with 0 \+\+\+/);
      if (mode === 'ram') assert.doesNotMatch(trace, /(?:vaultic-(?:written|markers|cwalk|check)-[^"\n]*|state\.db)/,'RAM accessed participating scratch');
    }
  }
  for (const [label,pattern] of [['index',/vaultic-written-/],['archiver',/vaultic-markers-/],['archiver',/vaultic-cwalk-/],['index-maintenance',/vaultic-check-/]]) assert.match(read(`logs/${label}-kv.trace`),pattern,'forced KV file boundary not exercised');
  const blocked = [];
  for (const target of ['linux/amd64','linux/arm64','linux/386','linux/arm','darwin/amd64','darwin/arm64','windows/amd64','windows/arm64','freebsd/amd64','freebsd/arm64','openbsd/amd64','openbsd/arm64']) {
    assert.ok(read('core-matrix.txt').includes(`PASS ${target} CGO_ENABLED=0`));
    if (read('build-matrix.txt').includes(`PASS ${target} CGO_ENABLED=0`)) continue;
    assert.ok(['linux/386','linux/arm'].includes(target));
    assert.ok(read('build-matrix.txt').includes(`BASELINE_BLOCKED ${target} MaxPackSize_overflow`));
    const label = target.replace('/','-');
    assert.match(read(`logs/build-${label}.log`),/MaxPackSize.*overflows|overflows.*MaxPackSize/);
    assert.equal(read(`logs/baseline-${label}.diff`),'');
    blocked.push(target);
  }
  assert.match(read('logs/native-386.log'),/^PASS$/m);
  assert.equal(fs.readdirSync(path.join(root,'scratch')).length,0);
  return {milestone:'35-M3',cli_policy:false,auto_fallback:false,participating_ram_files:0,race_cases:race.filter(record=>record.Test&&record.Action==='pass').length,native_authority_modes:['ram','kv'],core_build_targets:12,cli_build_targets:12-blocked.length,baseline_blocked_cli_targets:blocked};
}

if (process.argv[2] === '--self-test') {
  assert.throws(()=>requireCase([], 'missing'));
  assert.throws(()=>requireCase([{Action:'skip',Test:'missing'}],'missing'));
  console.log('PASS M3 analyzer rejects missing/skipped gates');
} else {
  const result = analyze(process.argv[2]);
  fs.writeFileSync(path.join(process.argv[2],'analysis.json'),JSON.stringify(result,null,2)+'\n');
  console.log('PASS M3 parity, restore, native authority, strict no-spill, race, traces, cleanup and portability');
}