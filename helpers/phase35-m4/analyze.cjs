const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');

function events(text) {
  return text.split('\n').filter(line => line.startsWith('{')).map(line => JSON.parse(line));
}
function requireCase(records, name) {
  assert.ok(records.some(record => record.Action === 'pass' && record.Test === name), `missing passed test: ${name}`);
  assert.ok(!records.some(record => record.Test === name && ['skip', 'fail'].includes(record.Action)), `skipped/failed test: ${name}`);
}
function analyze(root) {
  const read = name => fs.readFileSync(path.join(root, name), 'utf8');
  const race = events(read('logs/race.jsonl'));
  for (const name of ['race','cli','daemon','native']) assert.ok(!events(read(`logs/${name}.jsonl`)).some(record => record.Action === 'fail'), `failed ${name} gate`);
  const packages = ['workingkv','workingstate','telemetry','crawl','index','index/maintenance','index/legacyimport','repository/index','configfile','global'];
  for (const suffix of [...packages.map(name=>`internal/${name}`),'cmd/vaultic/backupcmd','cmd/vaultic/indexcmd']) {
    assert.ok(race.some(record => record.Action === 'pass' && !record.Test && record.Package.endsWith(suffix)), `missing package: ${suffix}`);
  }
  for (const name of ['TestM3SecureMapParity','TestM3ManifestParity','TestM3OverlayParity','TestM3LookupCapacity','TestM3ImportParity','TestM3SplitImportParity','TestM3CheckParity','TestM3SorterParity']) {
    for (const mode of ['ram','kv','auto']) requireCase(race, `${name}/${mode}`);
  }
	for (const name of ['TestM4LeafReservationOwnership','TestM4JSONPreflightOwnership']) requireCase(race,name);
  for (const name of ['TestM4AtomicMigration','TestM4MigrationFailurePreservesRAM','TestM4SharedMigrationHeadroom','TestM4ByteGrammarAndResources','TestM4BoundedPressureAndStop','TestM4AggregateDiskBudget','TestM4ConcurrentMigration','TestM4AuthenticatedMigrationRejectsCorruption','TestM4FaultCausesAndCleanup','TestM4CancelledHandoffWait','TestM4CancelledAggregateMigrationWait','TestM4InvalidRequestsNeverFallback','TestM4PressureHysteresis','TestM4IndexDecodeOwnership','TestM4LookupPressureReleasesOnlyCache','TestM4FindingsAdmissionAndReplacement','TestM3RAMSpillRejection']) requireCase(race,name);
  for (const mode of ['ram','auto']) requireCase(race,`TestM4SorterAggregatePressure/${mode}`);
  const cli = events(read('logs/cli.jsonl'));
  for (const mode of ['ram','kv','auto']) requireCase(cli,`TestM4WorkingMemoryCommandRoundTrip/${mode}`);
  for (const name of ['TestM4WorkingMemoryEnvironmentPrecedence','TestM4WorkingMemoryProfilePrecedence','TestM4WorkingMemoryProfileConflictAndStop']) requireCase(cli,name);
  const daemon = events(read('logs/daemon.jsonl'));
  requireCase(daemon,'TestM4RPCAdmissionPreservesOutcome');
	for (const mode of ['ram','kv','auto']) requireCase(daemon,`TestM4ExhaustionStillRollsBack/${mode}`);
  requireCase(daemon,'TestClientCanceledBeginSkipsAdmission');
  const native = events(read('logs/native.jsonl'));
  for (const mode of ['ram','kv','auto']) {
    requireCase(daemon,`TestM3FreshImportPolicyFallback/${mode}`);
    requireCase(native,`TestCachedAuthoritativeLookupWithPublishedOverlay/${mode}`);
  }
  const archiver = read('logs/archiver.log');
  assert.match(archiver,/^PASS$/m); assert.doesNotMatch(archiver,/--- FAIL:/);
  for (const mode of ['ram','kv','auto']) assert.ok(archiver.includes(`--- PASS: TestM3BackupParity/${mode}`));
  assert.ok(archiver.includes('--- PASS: TestM3BackupExhaustionDoesNotFallback'));
  for (const package of ['workingkv','index','crawl','archiver','index-maintenance','index-legacyimport']) {
    for (const mode of ['ram','kv','auto']) {
      const log = read(`logs/${package}-${mode}.log`);
      assert.match(log,/^PASS$/m); assert.match(log,/--- PASS: TestM3/);
      const trace = read(`logs/${package}-${mode}.trace`);
      assert.match(trace,/execve\(/); assert.match(trace,/\+\+\+ exited with 0 \+\+\+/);
      if (mode==='ram') assert.doesNotMatch(trace,/(?:vaultic-(?:written|markers|cwalk|check)-[^"\n]*|state\.db)/,'RAM accessed participating scratch');
    }
  }
  assert.match(read('logs/migration.log'),/--- PASS: TestM4ConcurrentMigration/);
  assert.match(read('logs/migration.trace'),/state\.db/);
  const cgroup = read('logs/cgroup.log');
  assert.match(cgroup,/--- PASS: TestM4IsolatedCgroupPressure/);
  assert.match(cgroup,/M4_CGROUP limit=536870912 /);
  assert.match(cgroup,/status=0\/SUCCESS|code=exited, status=0|code=exited\/status=0/);
  const blocked = [];
  for (const target of ['linux/amd64','linux/arm64','linux/386','linux/arm','darwin/amd64','darwin/arm64','windows/amd64','windows/arm64','freebsd/amd64','freebsd/arm64','openbsd/amd64','openbsd/arm64']) {
    assert.ok(read('core-matrix.txt').includes(`PASS ${target} CGO_ENABLED=0`));
    if (read('build-matrix.txt').includes(`PASS ${target} CGO_ENABLED=0`)) continue;
    assert.ok(['linux/386','linux/arm'].includes(target));
    const label=target.replace('/','-');
    for (const prefix of ['build','baseline']) assert.match(read(`logs/${prefix}-${label}.log`),/MaxPackSize.*overflows|overflows.*MaxPackSize/);
    assert.ok(read('build-matrix.txt').includes(`BASELINE_BLOCKED ${target} MaxPackSize_overflow`));
    blocked.push(target);
  }
  assert.match(read('logs/native-386.log'),/^PASS$/m);
  assert.equal(fs.readdirSync(path.join(root,'scratch')).length,0);
  return {milestone:'35-M4',auto_fallback:true,cli_policy:true,race_cases:race.filter(record=>record.Test&&record.Action==='pass').length,cli_cases:cli.filter(record=>record.Test&&record.Action==='pass').length,daemon_cases:daemon.filter(record=>record.Test&&record.Action==='pass').length,core_build_targets:12,cli_build_targets:12-blocked.length,baseline_blocked_cli_targets:blocked,isolated_cgroup_bytes:512<<20,production_trial:false};
}
if (process.argv[2]==='--self-test') {
  assert.throws(()=>requireCase([], 'missing'));
  assert.throws(()=>requireCase([{Action:'skip',Test:'missing'}], 'missing'));
  assert.throws(()=>requireCase([{Action:'pass',Test:'missing'},{Action:'fail',Test:'missing'}], 'missing'));
  console.log('PASS M4 analyzer rejects missing, skipped and failed gates');
} else {
  fs.writeFileSync(path.join(process.argv[2],'analysis.json'),JSON.stringify(analyze(process.argv[2]),null,2)+'\n');
  console.log('PASS M4 atomic fallback, CLI, pressure, authority, exact parity, no-spill and build gates');
}