'use strict';
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
const os = require('node:os');
const {spawnSync} = require('node:child_process');
const read = filename => JSON.parse(fs.readFileSync(filename, 'utf8'));
const digest = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
const frozenGates = {gc_share_increase: .02, assist_share_increase: .02, pause_floor_seconds: .01,
  pause_ratio: 1.25, operation_p99_ratio: 1.10, minimum_tail_observations: 100};

async function hashFile(filename) {
  const hash = crypto.createHash('sha256');
  for await (const chunk of fs.createReadStream(filename)) hash.update(chunk);
  return hash.digest('hex');
}

async function loadCapture(root, expected) {
  assert.match(expected, /^[a-f0-9]{64}$/);
  root = fs.realpathSync(root);
  const manifest = fs.readFileSync(path.join(root, 'SHA256SUMS'));
  assert.equal(digest(manifest), expected, 'capture manifest identity changed');
  const files = new Set();
  for (const line of manifest.toString('utf8').trimEnd().split('\n')) {
    const match = /^([a-f0-9]{64}) [ *](.+)$/.exec(line);
    assert.ok(match, 'unsupported or malformed checksum line');
    const filename = fs.realpathSync(match[2]);
    assert.ok(filename.startsWith(root + path.sep), 'manifest escapes owned capture');
    assert.ok(!files.has(filename), 'duplicate manifest entry');
    files.add(filename);
    assert.equal(await hashFile(filename), match[1], 'changed capture artifact: ' + filename);
  }
  for (const name of ['analysis.json', 'plan.json', 'plan.sha256', 'analysis.sha256',
    'analysis-reproduced-verified.log', 'source-verified.log', 'core-matrix.txt',
    'cgroup-before.events', 'cgroup-after.events', 'cgroup-limits.txt', 'overlap-monitor.json']) {
    assert.ok(files.has(path.join(root, name)), 'unsealed required artifact: ' + name);
  }
  const plan = read(path.join(root, 'plan.json'));
  const analysis = read(path.join(root, 'analysis.json'));
  assert.equal(digest(fs.readFileSync(path.join(root, 'analysis.json'))),
    fs.readFileSync(path.join(root, 'analysis.sha256'), 'utf8').trim().split(/\s+/)[0], 'analysis digest changed');
  assert.equal(digest(fs.readFileSync(path.join(root, 'plan.json'))),
    fs.readFileSync(path.join(root, 'plan.sha256'), 'utf8').trim());
  assert.deepEqual(plan.gates, frozenGates, 'frozen performance gates changed');
  assert.ok(plan.repetitions >= 3);
  assert.equal(analysis.semantic_cases, plan.cases.length);
  assert.equal(analysis.cases.length, plan.cases.length);
  assert.equal(new Set(analysis.cases.map(entry => entry.id)).size, plan.cases.length);
  assert.equal(analysis.accepted + analysis.rejected + analysis.incomplete, analysis.cases.length);
  for (const classification of ['accepted', 'rejected', 'incomplete']) {
    assert.equal(analysis[classification], analysis.cases.filter(entry => entry.classification === classification).length);
  }
  assert.equal(analysis.production_recommendation_approved, false);
  assert.equal(analysis.production_trial, false);
  const observations = new Map(analysis.cases.map(entry => [entry.id, entry]));
  for (const entry of plan.cases) {
    assert.ok(observations.has(entry.id), 'missing frozen case');
    for (const name of ['measurement.json', 'semantic.json']) {
      assert.ok(files.has(path.join(root, 'results', entry.id, name)), 'unsealed case');
    }
  }
  return {root, manifest_sha256: expected, plan, analysis, observations};
}

function operatorTemplate() {
  return {schema: 'phase35-m6-operator-plan-v1', authorization: {
    scope: 'disposable-fixtures-only', reviewer: null, approval_reference: null,
    source_roots: [], destination_root: null, scratch_root: null},
    workload_slos: [], rollback: {stop_conditions: [], recovery_procedure: null},
    runtime_tuning_allowed: false, default_change_allowed: false, pebble_removal_allowed: false};
}

function pilotBlockers(operator) {
  const blockers = [];
  assert.equal(operator.schema, 'phase35-m6-operator-plan-v1');
  const authorization = operator.authorization;
  assert.ok(Array.isArray(authorization.source_roots), 'source roots must be an explicit list');
  for (const root of authorization.source_roots) {
    assert.ok(typeof root === 'string' && path.isAbsolute(root), 'source roots must be absolute paths');
  }
  for (const key of ['destination_root', 'scratch_root']) {
    assert.ok(authorization[key] === null || (typeof authorization[key] === 'string' && path.isAbsolute(authorization[key])),
      'destination and scratch must be explicit absolute paths');
    if (authorization[key] !== null) {
      const target = path.resolve(authorization[key]);
      for (const root of authorization.source_roots) {
        const source = path.resolve(root);
        const overlaps = (parent, child) => {
          const relative = path.relative(parent, child);
          return relative === '' || (!relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative));
        };
        assert.ok(!overlaps(source, target) && !overlaps(target, source), 'source and writable pilot roots overlap');
      }
    }
  }
  for (const key of ['reviewer', 'approval_reference']) {
    assert.ok(authorization[key] === null || (typeof authorization[key] === 'string' && authorization[key].trim().length),
      'review identity and authorization reference must be explicit');
  }
  assert.ok(Array.isArray(operator.workload_slos), 'workload SLOs must be an explicit list');
  assert.ok(Array.isArray(operator.rollback.stop_conditions) &&
    operator.rollback.stop_conditions.every(condition => typeof condition === 'string' && condition.trim().length));
  assert.ok(operator.rollback.recovery_procedure === null ||
    (typeof operator.rollback.recovery_procedure === 'string' && operator.rollback.recovery_procedure.trim().length));
  assert.ok(['disposable-fixtures-only', 'read-only-real-inputs', 'production-pilot'].includes(operator.authorization.scope));
  assert.equal(operator.runtime_tuning_allowed, false, 'runtime tuning requires separate review');
  assert.equal(operator.default_change_allowed, false, 'default change requires separate review');
  assert.equal(operator.pebble_removal_allowed, false, 'dependency removal requires separate review');
  if (!operator.authorization.reviewer || !operator.authorization.approval_reference) blockers.push('operator authorization is not reviewed');
  if (operator.authorization.scope === 'disposable-fixtures-only') blockers.push('real workload inputs and production pilot are not authorized');
  if (operator.authorization.scope === 'read-only-real-inputs') blockers.push('production pilot is not authorized');
  if (!operator.authorization.source_roots.length || !operator.authorization.destination_root || !operator.authorization.scratch_root) {
    blockers.push('exact authorized source/destination/scratch roots are missing');
  }
  if (!operator.workload_slos.length) blockers.push('operator workload SLOs and representative distribution are missing');
  for (const slo of operator.workload_slos) {
    assert.ok(typeof slo.workload === 'string' && slo.workload.length);
    assert.ok(typeof slo.storage === 'string' && slo.storage.length);
    for (const key of ['maximum_wall_seconds', 'minimum_throughput_bytes_per_second', 'maximum_rss_bytes',
      'minimum_headroom_bytes', 'maximum_gc_cpu_share', 'maximum_assist_cpu_share',
      'maximum_pause_p99_seconds', 'maximum_operation_p99_seconds']) {
      assert.ok(Number.isFinite(slo[key]) && slo[key] > 0, 'missing/invalid operator SLO: ' + key);
    }
    assert.ok(slo.maximum_gc_cpu_share <= 1 && slo.maximum_assist_cpu_share <= 1);
    assert.ok(Number.isInteger(slo.repetitions) && slo.repetitions >= 3);
    assert.ok(Number.isInteger(slo.minimum_tail_observations) && slo.minimum_tail_observations >= 100);
    assert.ok(typeof slo.distribution_manifest_sha256 === 'string');
    assert.match(slo.distribution_manifest_sha256, /^[a-f0-9]{64}$/);
  }
  if (!operator.rollback.stop_conditions.length || !operator.rollback.recovery_procedure) blockers.push('reviewed stop/rollback procedure is missing');
  return blockers;
}

function summarize(capture) {
  return capture.analysis.groups.map(group => {
    const entries = capture.plan.cases.filter(entry => entry.kind === group.kind && entry.scenario === group.scenario &&
      entry.limit === group.limit && entry.mode === group.mode);
    assert.equal(entries.length, capture.plan.repetitions);
    const cases = entries.map(entry => capture.observations.get(entry.id));
    assert.ok(cases.every(entry => entry && ['accepted', 'rejected', 'incomplete'].includes(entry.classification)));
    const measurements = entries.map(entry => read(path.join(capture.root, 'results', entry.id, 'measurement.json')));
    const semantics = entries.map(entry => read(path.join(capture.root, 'results', entry.id, 'semantic.json')));
    const complete = semantics.every(entry => entry.complete && entry.scratch_clean && !entry.typed_exhaustion);
    const accepted = cases.every(entry => entry.classification === 'accepted');
    const finiteRange = key => {
      const values = cases.map(entry => entry[key]);
      return values.every(Number.isFinite) ? {minimum: Math.min(...values), maximum: Math.max(...values)} : null;
    };
    return {...group, storage: capture.plan.storage, capture_manifest_sha256: capture.manifest_sha256,
      per_repeat: cases.map((observation, index) => ({...observation, repeat: entries[index].repeat,
        completed_workload: semantics[index].complete && !semantics[index].typed_exhaustion,
        achieved_payload_bytes: group.kind === 'working' ? measurements[index].working_state.achieved_payload_bytes : null,
        managed_memory: group.kind === 'working' ? measurements[index].working_state.policy.memory : null,
        observed_safety_headroom_bytes: group.kind === 'working'
          ? measurements[index].working_state.policy.resolution.safety_headroom : null,
        migration_operation_seconds: group.kind === 'working'
          ? measurements[index].working_state.migration_operation_seconds : null})),
      completed_repetitions: semantics.filter(entry => entry.complete && !entry.typed_exhaustion).length,
      disposition: group.mode === 'pebble' ? 'historical-baseline-only' : !complete || cases.some(entry => entry.classification === 'rejected')
        ? 'not-recommended-on-this-evidence' : accepted ? 'eligible-for-disposable-fixture-comparison' : 'insufficient-performance-evidence',
      reasons: [...new Set(cases.flatMap(entry => entry.reasons))].sort(),
      gc_cpu_share: finiteRange('gc_cpu_share'), assist_cpu_share: finiteRange('assist_cpu_share'),
      operation_p99_seconds: finiteRange('operation_p99_seconds'),
      throughput_payload_bytes_per_second: finiteRange('throughput_payload_bytes_per_second'),
      managed_retained_bytes: group.kind === 'working'
        ? Math.max(...measurements.map(entry => entry.working_state.policy.memory.Used)) : null,
      managed_peak_charge_bytes: group.kind === 'working'
        ? Math.max(...measurements.map(entry => entry.working_state.policy.memory.Peak)) : null,
      mapping_capacity_bytes: group.kind === 'working'
        ? Math.max(...measurements.map(entry => entry.working_state.mapping_capacity_bytes || 0)) : null,
      interpretation: 'frozen tail/GC acceptance is not a total-throughput SLO or whole-process memory guarantee'};
  });
}

function validateOutput(output, captures) {
  const target = path.join(fs.realpathSync(path.dirname(output)), path.basename(output));
  for (const capture of captures) {
    assert.ok(target !== capture.root && !target.startsWith(capture.root + path.sep), 'report must not modify a sealed capture');
  }
  assert.ok(!fs.existsSync(target), 'refuse to overwrite retained report');
  return target;
}

function report(captures, operator) {
  const blockers = pilotBlockers(operator);
  return {schema: 'phase35-m6-preparation-v1', production_recommendation_approved: false,
    production_pilot_executed: false, default_change_approved: false, pebble_removal_approved: false,
    frozen_gates: frozenGates, operator_plan: operator, production_pilot_blockers: blockers,
    review_required_even_with_complete_operator_plan: true,
    captures: captures.map(capture => ({root: capture.root, manifest_sha256: capture.manifest_sha256,
      profile: capture.plan.profile, semantic_cases: capture.analysis.semantic_cases,
      accepted: capture.analysis.accepted, rejected: capture.analysis.rejected, incomplete: capture.analysis.incomplete,
      storage: capture.plan.storage, limitations: capture.analysis.pending})),
    observations: captures.flatMap(summarize)};
}

async function selftest() {
  const operator = operatorTemplate();
  assert.ok(pilotBlockers(operator).length >= 4);
  assert.throws(() => pilotBlockers({...operator, default_change_allowed: true}));
  assert.throws(() => pilotBlockers({...operator, workload_slos: [{workload: 'backup', storage: 'NFS'}]}));
  assert.throws(() => pilotBlockers({...operator, authorization: {...operator.authorization, source_roots: '/source'}}));
  assert.throws(() => pilotBlockers({...operator, authorization: {...operator.authorization,
    source_roots: ['/source'], destination_root: '/source/writable'}}), /overlap/);
  assert.throws(() => pilotBlockers({...operator, authorization: {...operator.authorization,
    source_roots: ['/source/child'], scratch_root: '/source'}}), /overlap/);
  const reviewed = operatorTemplate();
  reviewed.authorization = {scope: 'read-only-real-inputs', reviewer: 'test-only', approval_reference: 'test-only',
    source_roots: ['/fixture/source'], destination_root: '/fixture/destination', scratch_root: '/fixture/scratch'};
  reviewed.rollback = {stop_conditions: ['test-only-stop'], recovery_procedure: 'test-only-recovery'};
  reviewed.workload_slos = [{workload: 'test-only', storage: 'test-only', distribution_manifest_sha256: '0'.repeat(64),
    repetitions: 3, minimum_tail_observations: 100, maximum_wall_seconds: 1, minimum_throughput_bytes_per_second: 1,
    maximum_rss_bytes: 1, minimum_headroom_bytes: 1, maximum_gc_cpu_share: 1, maximum_assist_cpu_share: 1,
    maximum_pause_p99_seconds: 1, maximum_operation_p99_seconds: 1}];
  assert.deepEqual(pilotBlockers(reviewed), ['production pilot is not authorized']);
  reviewed.authorization.scope = 'production-pilot';
  assert.deepEqual(pilotBlockers(reviewed), []);
  assert.equal(report([], reviewed).production_recommendation_approved, false);
  assert.equal(report([], reviewed).production_pilot_executed, false);
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'vaultic-m6-selftest-'));
  try {
    const file = path.join(directory, 'artifact');
    fs.writeFileSync(file, 'owned evidence\n');
    assert.equal(await hashFile(file), digest('owned evidence\n'));
    const manifest = Buffer.from(`${await hashFile(file)}  ${file}\n`);
    fs.writeFileSync(path.join(directory, 'SHA256SUMS'), manifest);
    await assert.rejects(loadCapture(directory, '0'.repeat(64)), /identity changed/);
    fs.writeFileSync(file, 'changed evidence\n');
    await assert.rejects(loadCapture(directory, digest(manifest)), /changed capture artifact/);
    assert.equal(report([], operator).production_recommendation_approved, false);
    assert.equal(report([], operator).production_pilot_executed, false);
    assert.throws(() => validateOutput(path.join(directory, 'report.json'), [{root: directory}]), /sealed capture/);
    assert.throws(() => validateOutput(file, []), /overwrite/);
    const invocation = spawnSync(process.execPath, [__filename, path.join(directory, 'report.json'), '-',
      path.join(directory, 'missing-capture'), '0'.repeat(64)], {encoding: 'utf8'});
    assert.equal(invocation.status, 1);
    assert.match(invocation.stderr, /ENOENT/);
    assert.ok(!invocation.stderr.includes('usage:'), 'valid capture-pair invocation rejected');
  } finally {
    fs.rmSync(directory, {recursive: true, force: true});
  }
  console.log('PASS M6 preparation pinned-evidence, missing-SLO and unauthorized-change regressions');
}

async function main() {
  if (process.argv[2] === '--selftest') return selftest();
  const args = process.argv.slice(2);
  assert.ok(args.length >= 4 && (args.length - 2) % 2 === 0,
    'usage: node prepare.cjs OUTPUT OPERATOR_PLAN_OR_- CAPTURE_ROOT MANIFEST_SHA256 [CAPTURE_ROOT MANIFEST_SHA256 ...]');
  const [output, operatorFile, ...inputs] = args;
  const operator = operatorFile === '-' ? operatorTemplate() : read(operatorFile);
  const captures = [];
  for (let index = 0; index < inputs.length; index += 2) captures.push(await loadCapture(inputs[index], inputs[index + 1]));
  const target = validateOutput(output, captures);
  fs.writeFileSync(target, JSON.stringify(report(captures, operator), null, 2) + '\n', {flag: 'wx', mode: 0o600});
  console.log('PASS verified M6 preparation report; production decisions require separate review');
}

if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; });
module.exports = {operatorTemplate, pilotBlockers, report, loadCapture};