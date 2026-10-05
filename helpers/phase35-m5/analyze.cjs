'use strict';
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
const hash = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
const read = filename => JSON.parse(fs.readFileSync(filename, 'utf8'));
const write = (filename, value) => fs.writeFileSync(filename, JSON.stringify(value, null, 2) + '\n');
const median = values => {
  assert.ok(values.length > 0 && values.every(Number.isFinite));
  const ordered = [...values].sort((left, right) => left - right);
  const middle = Math.floor(ordered.length / 2);
  return ordered.length % 2 ? ordered[middle] : (ordered[middle - 1] + ordered[middle]) / 2;
};

function overlappingExecutable(root, executable) {
  const owned = path.relative(path.join(root, 'bin'), executable);
  if (owned && !owned.startsWith('..' + path.sep) && !path.isAbsolute(owned)) return false;
  return /^(vaultic|go|cargo|rustc|compile|asm|link|.*\.test)$/.test(path.basename(executable));
}

function competingCapture(root, executable, cwd) {
  const relative = path.relative(root, cwd);
  return overlappingExecutable(root, executable) &&
    (!relative || (!relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative)));
}

function overlapCheck(root) {
  const external = {};
  for (const pid of fs.readdirSync('/proc')) {
    if (!/^\d+$/.test(pid)) continue;
    try {
      const executable = fs.readlinkSync(`/proc/${pid}/exe`).replace(/ \(deleted\)$/, '');
      if (!overlappingExecutable(root, executable)) continue;
      const cwd = fs.readlinkSync(`/proc/${pid}/cwd`);
      assert.ok(!competingCapture(root, executable, cwd), `competing capture process: ${pid} ${path.basename(executable)}`);
      const name = path.basename(executable).endsWith('.test') ? 'test' : path.basename(executable);
      external[name] = (external[name] || 0) + 1;
    } catch (error) {
      if (!['ENOENT', 'ESRCH', 'EINVAL'].includes(error.code)) throw error;
    }
  }
  return external;
}

function watchOverlap(root) {
  let checks = 0;
  const externalPeak = {};
  const inspect = () => {
    try {
      const external = overlapCheck(root);
      for (const [name, count] of Object.entries(external)) externalPeak[name] = Math.max(externalPeak[name] || 0, count);
      checks++;
    }
    catch (error) {
      write(path.join(root, 'overlap-violation.json'), {checks, reason: error.message});
      process.stderr.write(error.message + '\n');
      process.exit(1);
    }
  };
  inspect();
  setInterval(inspect, 100);
  process.on('SIGTERM', () => {
    write(path.join(root, 'overlap-monitor.json'), {checks, interval_milliseconds: 100, violation: false,
      scope: 'owned capture; unrelated host activity allowed by operator', external_process_peak_counts: externalPeak});
    process.exit(0);
  });
}

function plan(root, emit = true, profile = process.env.PHASE35_M5_PROFILE || 'reduced') {
  assert.ok(['reduced', 'extended'].includes(profile));
  const repetitions = Number(process.env.PHASE35_M5_REPETITIONS || 4);
  assert.ok(Number.isInteger(repetitions) && repetitions >= 3 && repetitions <= 12);
  const orders = [['pebble', 'ram', 'kv', 'auto'], ['ram', 'auto', 'pebble', 'kv'],
    ['kv', 'pebble', 'auto', 'ram'], ['auto', 'kv', 'ram', 'pebble']];
  const groups = [];
  const scenarios = ['initial', 'unchanged', 'churn', 'high-live-backup', 'import-indexes', 'check-full', 'check-legacy'];
  if (profile === 'extended') scenarios.push('import-full');
  for (const scenario of scenarios) {
    for (const limit of profile === 'extended' ? ['32MiB', '512MiB', '8GiB'] : ['32MiB', '128MiB', '512MiB']) groups.push({kind: 'workflow', scenario, limit});
  }
  const working = profile === 'extended' ? [['high-live-10GB', '32GiB', 10000000000], ['high-live-100GB', '140GiB', 100000000000]] :
    [['below', '32MiB', 1048576], ['near', '32MiB', 6291456], ['above', '16MiB', 50331648], ['high-live', '8GiB', 1000000000]];
  for (const [scenario, limit, payload] of working) {
    groups.push({kind: 'working', scenario, limit, payload});
  }
  const cases = [];
  for (let repeat = 1; repeat <= repetitions; repeat++) {
    const orderedGroups = repeat % 2 ? groups : [...groups].reverse();
    for (const group of orderedGroups) for (const mode of orders[(repeat - 1) % orders.length]) {
      const id = `${group.kind}-${group.scenario}-${group.limit}-${repeat}-${mode}`;
      cases.push({...group, mode, repeat, id});
    }
  }
  const result = {milestone: '35-M5', profile, repetitions, scenario_groups: groups.length, gomaxprocs: 4, gc: 'default', soft_limit: 'default',
    storage: profile === 'extended' ? 'operator-authorized disposable NFS source/repository/scratch; warm cache; no global cache drop' : 'actual host VM vdisk; warm file cache; no global cache drop',
    isolated_memory_max: profile === 'extended' ? '200G' : '12G', isolated_memory_max_bytes: profile === 'extended' ? 214748364800 : 12884901888,
    test_timeout: profile === 'extended' ? '24h' : '5m', isolated_swap_max: '0', sampler_milliseconds: 50,
    baseline: 'archived M4 parent, policy omitted, identical measurement-only overlay',
    baseline_revision: 'c567dd4561d512162d823ccbfcb3d8578c9690a1',
    activity_scope: 'owned capture; unrelated host activity allowed by operator',
    gates: {gc_share_increase: .02, assist_share_increase: .02, pause_floor_seconds: .01,
      pause_ratio: 1.25, operation_p99_ratio: 1.10, minimum_tail_observations: 100},
    exclusions: ['Pack staging and ordinary repository cache are not working-state spills',
      'Index-only timing excludes snapshot JSON setup/verification and full tree/path traversal',
      'Synthetic high-live probe is not an actual concurrent production backup'],
    pending: [
      ...(profile === 'reduced' ? [{case: 'full-snapshot-path-import', classification: 'incomplete', reason: 'covered separately by extended matrix'},
        {case: 'NFS-source-and-scratch', classification: 'incomplete', reason: 'covered separately by extended matrix'},
        {case: '10GB-and-100GB-retained-state', classification: 'incomplete', reason: 'covered separately by extended matrix'}] : []),
      {case: 'HDD-scratch', classification: 'incomplete', reason: 'VM vdisk and NFS are measured; physical HDD media identity is not known'},
      {case: 'cold-cache', classification: 'incomplete', reason: 'no global kernel cache manipulation authorized; cloned/read inputs are warm'},
      {case: 'real-legacy-distribution-damaged-input-duplicates-and-mismatch-scopes', classification: 'incomplete', reason: 'semantic regression fixtures retained separately; generated repository timing is not a real production distribution'},
      {case: 'production-operator-SLO', classification: 'incomplete', reason: 'no supplied operator SLO or production trial authorization'}
    ], cases};
  write(path.join(root, 'plan.json'), result);
  fs.writeFileSync(path.join(root, 'plan.sha256'), hash(fs.readFileSync(path.join(root, 'plan.json'))) + '\n');
  if (emit) for (const entry of cases) process.stdout.write([entry.id, entry.kind, entry.mode, entry.scenario, entry.limit, entry.payload || 0].join('\t') + '\n');
  return result;
}

function analyze(root) {
  const frozen = read(path.join(root, 'plan.json'));
  assert.equal(hash(fs.readFileSync(path.join(root, 'plan.json'))), fs.readFileSync(path.join(root, 'plan.sha256'), 'utf8').trim(), 'frozen plan changed');
  assert.ok(frozen.repetitions >= 3 && frozen.cases.length === frozen.scenario_groups * 4 * frozen.repetitions);
  assert.equal(new Set(frozen.cases.map(entry => entry.id)).size, frozen.cases.length);
  assert.equal(fs.readFileSync(path.join(root, 'baseline-revision.txt'), 'utf8').trim(), frozen.baseline_revision);
  const events = filename => Object.fromEntries(fs.readFileSync(path.join(root, filename), 'utf8').trim().split('\n').map(line => line.split(' ')));
  const beforeEvents = events('cgroup-before.events'), afterEvents = events('cgroup-after.events');
  for (const name of ['oom', 'oom_kill', 'max']) assert.equal(Number(afterEvents[name]), Number(beforeEvents[name]), 'cgroup pressure/OOM gate: ' + name);
  assert.equal(fs.readFileSync(path.join(root, 'cgroup-limits.txt'), 'utf8'), `${frozen.isolated_memory_max_bytes}\n0\n`);
  assert.ok(!fs.existsSync(path.join(root, 'overlap-violation.json')), 'competing workload observed');
  const monitor = read(path.join(root, 'overlap-monitor.json'));
  assert.ok(monitor.checks > 0 && monitor.interval_milliseconds === 100 && monitor.violation === false);
  assert.equal(monitor.scope, frozen.activity_scope);
  const targets = ['linux/amd64', 'linux/arm64', 'linux/386', 'linux/arm', 'darwin/amd64', 'darwin/arm64',
    'windows/amd64', 'windows/arm64', 'freebsd/amd64', 'freebsd/arm64', 'openbsd/amd64', 'openbsd/arm64'];
  assert.deepEqual(fs.readFileSync(path.join(root, 'core-matrix.txt'), 'utf8').trim().split('\n'),
    targets.map(target => `PASS ${target} CGO_ENABLED=0`), 'incomplete core build matrix');
  for (const filename of ['measurement-race.jsonl', 'semantic-regressions.jsonl']) {
    const records = fs.readFileSync(path.join(root, 'logs', filename), 'utf8').trim().split('\n').map(line => JSON.parse(line));
    assert.ok(records.some(record => record.Action === 'pass' && record.Test));
    assert.ok(!records.some(record => record.Action === 'fail' || record.Action === 'skip'));
    const names = new Set(records.filter(record => record.Action === 'pass').map(record => record.Test));
    const required = filename === 'measurement-race.jsonl'
      ? ['TestM5ProbeOwnershipAndBounds', 'TestM5LargeLatencyHistogram', 'TestM5WorkingRuntimeIntervals', 'TestWorkingRuntimeAvailabilityAndPrivacy', 'TestM5WorkingPolicyMeasurementBoundary',
        'TestRAMFreeListAndIncrementalAccounting', 'TestRAMPackingAtEncryptedBoundary', 'TestKVLargeGrowthWithinBudget',
        'TestPolicyResourceRecoveryBounded', 'TestPolicyOwnedCacheAcrossRemap', 'TestPolicyCacheFileOwnership',
        'TestWorkingScratchInventoryConcurrentRemoval']
      : ['TestM3ImportParity', 'TestM3SplitImportParity', 'TestM3CheckParity', 'TestCheckValidatesDeclaredReadSession', 'TestCheckTelemetryIncludesFinalReadSessionValidation', 'TestM4FindingsAdmissionAndReplacement', 'TestM4SorterAggregatePressure',
        'TestCheckKVSpoolSortsDuplicatesAcrossMergePasses', 'TestLocationMultisetSpoolPreservesDuplicatesAcrossMergePasses',
        'TestLocationSpoolRejectsScratchOverflowAndCorruption', 'TestSnapshotTraversalLimitsLeaveNoCheckpoint'];
    for (const name of required) assert.ok(names.has(name), 'missing mandatory regression gate: ' + name);
  }
  const observations = new Map();
  const cases = [];
  for (const entry of frozen.cases) {
    const directory = path.join(root, 'results', entry.id);
    const log = fs.readFileSync(path.join(root, 'logs', entry.id + '.log'), 'utf8');
    const test = entry.kind === 'workflow' ? 'TestM5Workflow' : 'TestM5WorkingSet';
    assert.ok(log.includes(`--- PASS: ${test} (`), 'required named test absent: ' + entry.id);
    assert.ok(!/--- (SKIP|FAIL):|panic:|test timed out|WARNING: DATA RACE/.test(log), 'invalid case: ' + entry.id);
    const semantic = read(path.join(directory, 'semantic.json'));
    const metric = read(path.join(directory, 'measurement.json'));
    const checkNumbers = value => {
      for (const [name, field] of Object.entries(value)) {
        if (typeof field === 'number') assert.ok(Number.isFinite(field) && field >= 0, 'invalid observation: ' + name);
        else if (field && typeof field === 'object') checkNumbers(field);
      }
    };
    checkNumbers(metric);
    for (const name of ['cpu_seconds', 'logical_read_bytes', 'logical_write_bytes', 'storage_read_bytes', 'storage_write_bytes', 'read_calls', 'write_calls']) {
      assert.ok(typeof metric.os_before[name] === 'number' && typeof metric.os_after[name] === 'number');
      assert.ok(metric.os_after[name] >= metric.os_before[name], 'OS interval counter reset: ' + name);
    }
    assert.equal(semantic.complete, true);
    assert.equal(semantic.scratch_clean, true);
    assert.ok(metric.samples > 0 && metric.wall_seconds > 0 && metric.sampled_os_peak.rss_bytes > 0);
    assert.equal(metric.sampled_os_peak.swap_bytes, 0);
    assert.equal(metric.semantic_sha256, semantic.input_sha256);
    for (const profile of ['cpu.pprof', 'heap.pprof']) {
      assert.ok(fs.statSync(path.join(directory, profile)).size > 0);
      assert.ok(fs.readFileSync(path.join(directory, profile + '.top'), 'utf8').includes('Type:'), 'undecodable profile');
    }
    if (entry.kind === 'workflow') {
      assert.ok(semantic.restore_verified || semantic.exact_check_verified || (semantic.typed_exhaustion && semantic.exhaustion_verified));
      if (entry.scenario === 'import-full' && semantic.completed_workload) {
        assert.equal(semantic.traversal_verified, true);
        assert.match(semantic.traversal_sha256, /^[a-f0-9]{64}$/);
        assert.equal(semantic.timed_import_scope, 'full legacy indexes, snapshots, trees and paths');
      }
      if (entry.scenario === 'unchanged' && semantic.completed_workload) {
        const summary = metric.working_state.backup_summary;
        assert.equal(summary.files_new, 0);
        assert.equal(summary.files_changed, 0);
        assert.equal(summary.files_unmodified, semantic.files);
      }
      if (entry.scenario === 'high-live-backup') {
        assert.equal(metric.working_state.ordinary_live_bytes, 1000000000);
        assert.equal(metric.working_state.concurrent_allocated_bytes, 3 * 1024 ** 3);
        assert.ok(metric.runtime_delta.gc_cycles > 0, 'no real high-live backup GC observed');
      }
      if (entry.mode !== 'pebble') {
        assert.ok(metric.working_state.events.length > 0, 'no actual policy report: ' + entry.id);
        for (const event of metric.working_state.events) {
          assert.equal(event.requested_mode, entry.mode);
          assert.ok(event.resolution.effective_limit <= event.resolution.configured_limit);
          assert.ok(event.memory.Used <= event.memory.Limit && event.disk.Used <= event.disk.Limit);
        }
      }
    } else {
      assert.equal(semantic.sequence_verified, true);
      assert.ok(semantic.completed_workload || semantic.typed_exhaustion);
      if (entry.mode !== 'pebble') {
        const state = metric.working_state.policy;
        assert.ok(state.memory.Used <= state.memory.Limit && state.memory.Peak <= state.resolution.configured_limit);
        assert.ok(state.disk.Used <= state.disk.Limit);
      }
      if (entry.mode === 'ram') assert.equal(metric.working_state.backend, 'ram');
      if (entry.scenario === 'near' && entry.mode === 'auto') {
        assert.equal(metric.working_state.backend, 'bbolt');
        assert.ok(metric.working_state.migration_operation_seconds > 0);
      }
      if (entry.scenario === 'above' && entry.mode === 'ram') assert.equal(semantic.typed_exhaustion, true);
      if (entry.scenario.startsWith('high-live') && semantic.completed_workload) {
        assert.ok(metric.working_state.achieved_payload_bytes >= entry.payload);
        assert.equal(metric.working_state.concurrent_allocated_bytes, 3 * 1024 ** 3);
        assert.equal(metric.lookup_latency.count, 1536, 'missing interleaved high-live lookups');
        assert.ok(metric.runtime_delta.gc_cycles > 0, 'no high-live store GC observed');
        if (entry.payload > 1000000000) {
          assert.equal(metric.latency_histogram, true);
          assert.equal(metric.working_state.payload_size, 32768);
          assert.equal(metric.working_state.batch_entries, 31);
          assert.ok(metric.latency_p99_lower_seconds <= metric.latency_p99_seconds);
        }
      }
    }
    const key = [entry.kind, entry.scenario, entry.limit, entry.repeat].join('/');
    const group = observations.get(key) || [];
    group.push({entry, semantic, metric});
    observations.set(key, group);
  }
  for (const group of observations.values()) {
    assert.equal(group.length, 4);
    const baseline = group.find(item => item.entry.mode === 'pebble');
    assert.ok(baseline);
    for (const item of group) {
      assert.equal(item.semantic.input_sha256, baseline.semantic.input_sha256, 'different matched inputs');
      if (item.entry.kind === 'workflow') {
        assert.equal(item.semantic.files, baseline.semantic.files);
        assert.equal(item.semantic.bytes, baseline.semantic.bytes);
        if (!item.semantic.typed_exhaustion) assert.equal(item.semantic.snapshots, baseline.semantic.snapshots);
      } else if (item.semantic.completed_workload) {
        assert.equal(item.semantic.retained_sha256, baseline.semantic.retained_sha256);
      }
      const metric = item.metric;
      const cpu = metric.os_after.cpu_seconds - metric.os_before.cpu_seconds;
      const baselineCPU = baseline.metric.os_after.cpu_seconds - baseline.metric.os_before.cpu_seconds;
      const missing = [...(metric.runtime_delta.unavailable || []), ...(metric.os_before.unavailable || []),
        ...(metric.os_after.unavailable || []), ...(metric.sampled_os_peak.unavailable || []),
        ...(baseline.metric.runtime_delta.unavailable || []), ...(baseline.metric.sampled_os_peak.unavailable || []),
        ...(baseline.metric.os_before.unavailable || []), ...(baseline.metric.os_after.unavailable || [])];
      const reasons = [];
      let classification = 'accepted';
      if (item.semantic.typed_exhaustion) {
        classification = 'rejected';
        reasons.push('controlled typed exhaustion: incomplete workload is not a successful performance result');
      } else {
        const tailAvailable = metric.latency_count >= frozen.gates.minimum_tail_observations &&
          baseline.metric.latency_count >= frozen.gates.minimum_tail_observations;
        const lookupRequired = item.entry.kind === 'working' && item.entry.scenario.startsWith('high-live');
        const lookupAvailable = !lookupRequired || (metric.lookup_latency.count >= frozen.gates.minimum_tail_observations &&
          baseline.metric.lookup_latency.count >= frozen.gates.minimum_tail_observations);
        const cpuAvailable = cpu > 0 && baselineCPU > 0 && !missing.some(name =>
          name === 'process_cpu' || name === 'runtime_counter_reset' || name.startsWith('/cpu/classes/gc/'));
        const pauseAvailable = !missing.includes('/sched/pauses/total/gc:seconds');
        const gc = metric.runtime_delta.gc_cpu_seconds / cpu;
        const assist = metric.runtime_delta.gc_assist_cpu_seconds / cpu;
        const baselineGC = baseline.metric.runtime_delta.gc_cpu_seconds / baselineCPU;
        const baselineAssist = baseline.metric.runtime_delta.gc_assist_cpu_seconds / baselineCPU;
        if (cpuAvailable && gc > baselineGC + frozen.gates.gc_share_increase) reasons.push('GC CPU share exceeds frozen paired baseline');
        if (cpuAvailable && assist > baselineAssist + frozen.gates.assist_share_increase) reasons.push('assist CPU share exceeds frozen paired baseline');
        if (pauseAvailable && metric.runtime_delta.gc_pause_p99_upper_seconds > Math.max(frozen.gates.pause_floor_seconds,
          frozen.gates.pause_ratio * baseline.metric.runtime_delta.gc_pause_p99_upper_seconds)) reasons.push('pause p99 exceeds frozen paired baseline');
        if (pauseAvailable && metric.after.gc_pause_p99_seconds_cumulative > Math.max(frozen.gates.pause_floor_seconds,
          frozen.gates.pause_ratio * baseline.metric.after.gc_pause_p99_seconds_cumulative)) reasons.push('cumulative pause p99 exceeds original M0 gate');
        const lower = metric.latency_histogram ? metric.latency_p99_lower_seconds : metric.latency_p99_seconds;
        const baselineLower = baseline.metric.latency_histogram ? baseline.metric.latency_p99_lower_seconds : baseline.metric.latency_p99_seconds;
        const uncertainTail = tailAvailable && metric.latency_p99_seconds > frozen.gates.operation_p99_ratio * baselineLower &&
          lower <= frozen.gates.operation_p99_ratio * baseline.metric.latency_p99_seconds;
        if (tailAvailable && lower > frozen.gates.operation_p99_ratio * baseline.metric.latency_p99_seconds) reasons.push('operation p99 exceeds frozen paired baseline');
        if (lookupRequired && lookupAvailable && metric.lookup_latency.p99_seconds >
          frozen.gates.operation_p99_ratio * baseline.metric.lookup_latency.p99_seconds) reasons.push('concurrent lookup p99 exceeds frozen paired baseline');
        if (reasons.length) classification = 'rejected';
        if (missing.length || !cpuAvailable || !tailAvailable || !pauseAvailable || !lookupAvailable || uncertainTail) {
          if (classification !== 'rejected') classification = 'incomplete';
          reasons.push('unsupported/coarse metrics or insufficient operation-tail observations');
        }
      }
      cases.push({id: item.entry.id, classification, reasons, missing_metrics: [...new Set(missing)],
        wall_seconds: metric.wall_seconds, process_cpu_seconds: cpu, peak_rss_bytes: metric.sampled_os_peak.rss_bytes,
        peak_heap_bytes: metric.sampled_heap_peak_bytes, peak_scannable_bytes: metric.sampled_scan_peak_bytes,
        peak_mapped_rss_bytes: metric.sampled_os_peak.mapped_rss_bytes, peak_scratch_bytes: metric.sampled_os_peak.scratch_bytes,
        allocation_bytes_per_second: metric.runtime_delta.allocated_bytes / metric.wall_seconds,
        gc_cycles: metric.runtime_delta.gc_cycles, gc_cpu_share: cpu > 0 ? metric.runtime_delta.gc_cpu_seconds / cpu : null,
        assist_cpu_share: cpu > 0 ? metric.runtime_delta.gc_assist_cpu_seconds / cpu : null,
        gc_pause_p99_upper_seconds: metric.runtime_delta.gc_pause_p99_upper_seconds,
        operation_p99_seconds: metric.latency_p99_seconds,
        concurrent_lookup_p99_seconds: metric.lookup_latency?.p99_seconds ?? null,
        throughput_payload_bytes_per_second: item.entry.kind === 'working' ? metric.working_state.achieved_payload_bytes / metric.wall_seconds : null,
        throughput_retained_entries_per_second: item.entry.kind === 'working' ? metric.working_state.committed_entries / metric.wall_seconds : null,
        logical_source_bytes_per_second: item.entry.kind === 'workflow' ? item.semantic.bytes / metric.wall_seconds : null,
        baseline_wall_ratio: metric.wall_seconds / baseline.metric.wall_seconds,
        logical_io_read_bytes: metric.os_after.logical_read_bytes - metric.os_before.logical_read_bytes,
        logical_io_write_bytes: metric.os_after.logical_write_bytes - metric.os_before.logical_write_bytes,
        storage_io_read_bytes: metric.os_after.storage_read_bytes - metric.os_before.storage_read_bytes,
        storage_io_write_bytes: metric.os_after.storage_write_bytes - metric.os_before.storage_write_bytes,
        io_read_calls: metric.os_after.read_calls - metric.os_before.read_calls,
        io_write_calls: metric.os_after.write_calls - metric.os_before.write_calls});
    }
  }
  cases.sort((left, right) => left.id.localeCompare(right.id));
  const groups = [];
  for (const entry of frozen.cases.filter(item => item.repeat === 1)) {
    const matched = cases.filter(item => item.id.startsWith(`${entry.kind}-${entry.scenario}-${entry.limit}-`) && item.id.endsWith('-' + entry.mode));
    const walls = matched.map(item => item.wall_seconds);
    groups.push({kind: entry.kind, scenario: entry.scenario, limit: entry.limit, mode: entry.mode, repetitions: matched.length,
      wall_median_seconds: median(walls), wall_min_seconds: Math.min(...walls), wall_max_seconds: Math.max(...walls),
      classifications: matched.map(item => item.classification), peak_rss_bytes: Math.max(...matched.map(item => item.peak_rss_bytes)),
      median_baseline_wall_ratio: median(matched.map(item => item.baseline_wall_ratio))});
  }
  const result = {milestone: '35-M5', semantic_cases: cases.length, repetitions: frozen.repetitions,
    actual_near_budget_migrations: frozen.cases.filter(entry => entry.scenario === 'near' && entry.mode === 'auto').length,
    accepted: cases.filter(item => item.classification === 'accepted').length,
    rejected: cases.filter(item => item.classification === 'rejected').length,
    incomplete: cases.filter(item => item.classification === 'incomplete').length,
    production_recommendation_approved: false, production_trial: false, pending: frozen.pending, groups, cases};
  write(path.join(root, 'analysis.json'), result);
  process.stdout.write(`PASS M5 ${cases.length} sealed semantic cases, balanced repetitions and explicit frozen-gate classifications\n`);
  return result;
}

function selftest() {
  assert.equal(competingCapture('/owned', '/usr/bin/go', '/owned'), true);
  assert.equal(competingCapture('/owned', '/usr/bin/rustc', '/owned/scratch'), true);
  assert.equal(competingCapture('/owned', '/usr/bin/rustc', '/unrelated'), false);
  assert.equal(competingCapture('/owned', '/usr/bin/go', '/owned-other'), false);
  assert.equal(competingCapture('/owned', '/owned/bin/candidate-working.test', '/owned/scratch'), false);
  assert.equal(overlappingExecutable('/owned', '/owned/bin/candidate-workflow.test'), false);
  assert.equal(overlappingExecutable('/owned', '/owned/bin-foreign/candidate-workflow.test'), true);
  assert.equal(overlappingExecutable('/owned', '/usr/bin/cargo'), true);
  assert.equal(overlappingExecutable('/owned', '/usr/bin/node'), false);
  assert.equal(median([3, 1, 2]), 2);
  assert.equal(median([4, 1, 3, 2]), 2.5);
  assert.throws(() => median([]));
  const root = fs.mkdtempSync(path.join(require('node:os').tmpdir(), 'vaultic-m5-analyzer-selftest-'));
  try {
    const frozen = plan(root, false, 'reduced');
    fs.writeFileSync(path.join(root, 'baseline-revision.txt'), frozen.baseline_revision + '\n');
    fs.mkdirSync(path.join(root, 'logs'));
    fs.mkdirSync(path.join(root, 'results'));
    fs.writeFileSync(path.join(root, 'cgroup-before.events'), 'max 0\noom 0\noom_kill 0\n');
    fs.copyFileSync(path.join(root, 'cgroup-before.events'), path.join(root, 'cgroup-after.events'));
    fs.writeFileSync(path.join(root, 'cgroup-limits.txt'), '12884901888\n0\n');
    write(path.join(root, 'overlap-monitor.json'), {checks: 100, interval_milliseconds: 100, violation: false, scope: frozen.activity_scope});
    fs.writeFileSync(path.join(root, 'core-matrix.txt'), ['linux/amd64', 'linux/arm64', 'linux/386', 'linux/arm',
      'darwin/amd64', 'darwin/arm64', 'windows/amd64', 'windows/arm64', 'freebsd/amd64', 'freebsd/arm64',
      'openbsd/amd64', 'openbsd/arm64'].map(target => `PASS ${target} CGO_ENABLED=0\n`).join(''));
    const gates = ['TestM5ProbeOwnershipAndBounds', 'TestM5LargeLatencyHistogram', 'TestM5WorkingRuntimeIntervals', 'TestWorkingRuntimeAvailabilityAndPrivacy', 'TestM5WorkingPolicyMeasurementBoundary',
      'TestRAMFreeListAndIncrementalAccounting', 'TestRAMPackingAtEncryptedBoundary', 'TestKVLargeGrowthWithinBudget',
      'TestPolicyResourceRecoveryBounded', 'TestPolicyOwnedCacheAcrossRemap', 'TestPolicyCacheFileOwnership',
      'TestWorkingScratchInventoryConcurrentRemoval',
      'TestM3ImportParity', 'TestM3SplitImportParity', 'TestM3CheckParity', 'TestCheckValidatesDeclaredReadSession', 'TestCheckTelemetryIncludesFinalReadSessionValidation', 'TestM4FindingsAdmissionAndReplacement', 'TestM4SorterAggregatePressure',
      'TestCheckKVSpoolSortsDuplicatesAcrossMergePasses', 'TestLocationMultisetSpoolPreservesDuplicatesAcrossMergePasses',
      'TestLocationSpoolRejectsScratchOverflowAndCorruption', 'TestSnapshotTraversalLimitsLeaveNoCheckpoint'];
    for (const filename of ['measurement-race.jsonl', 'semantic-regressions.jsonl']) {
      fs.writeFileSync(path.join(root, 'logs', filename), gates.map(Test => JSON.stringify({Action: 'pass', Test})).join('\n') + '\n');
    }
    for (const entry of frozen.cases) {
      const directory = path.join(root, 'results', entry.id);
      fs.mkdirSync(directory);
      const semantic = {complete: true, scratch_clean: true, input_sha256: 'synthetic-input', retained_sha256: 'synthetic-input',
        restore_verified: true, sequence_verified: true, completed_workload: true, files: 1, bytes: 1, snapshots: 1};
      if (entry.scenario === 'above' && entry.mode === 'ram') { semantic.completed_workload = false; semantic.typed_exhaustion = true; }
      const counters = {cpu_seconds: 0, logical_read_bytes: 0, logical_write_bytes: 0, storage_read_bytes: 0, storage_write_bytes: 0, read_calls: 0, write_calls: 0};
      const policy = {resolution: {configured_limit: 100, effective_limit: 100}, memory: {Used: 50, Limit: 100, Peak: 50}, disk: {Used: 0, Limit: 100}};
      const metric = {samples: 3, wall_seconds: 1, semantic_sha256: 'synthetic-input', latency_count: 100, latency_p99_seconds: .1,
        lookup_latency: {count: 1536, p99_seconds: .1},
        sampled_os_peak: {rss_bytes: 50, swap_bytes: 0, mapped_rss_bytes: 0, scratch_bytes: 0},
        sampled_heap_peak_bytes: 50, sampled_scan_peak_bytes: 5, os_before: counters, os_after: {...counters, cpu_seconds: 1},
        after: {gc_pause_p99_seconds_cumulative: .001},
        runtime_delta: {allocated_bytes: 100, gc_cpu_seconds: .01, gc_assist_cpu_seconds: 0, gc_cycles: 1, gc_pause_p99_upper_seconds: .001},
        working_state: {policy, backend: entry.mode === 'ram' ? 'ram' : 'bbolt', migration_operation_seconds: .1,
          achieved_payload_bytes: 1000000000, committed_entries: 244141, concurrent_allocated_bytes: 3 * 1024 ** 3, ordinary_live_bytes: 1000000000,
          backup_summary: {files_new: 0, files_changed: 0, files_unmodified: 1},
          events: [{...policy, requested_mode: entry.mode}]}};
      write(path.join(directory, 'semantic.json'), semantic);
      write(path.join(directory, 'measurement.json'), metric);
      for (const profile of ['cpu.pprof', 'heap.pprof']) {
        fs.writeFileSync(path.join(directory, profile), 'synthetic selftest fixture');
        fs.writeFileSync(path.join(directory, profile + '.top'), 'Type: synthetic selftest fixture\n');
      }
      fs.writeFileSync(path.join(root, 'logs', entry.id + '.log'), `--- PASS: ${entry.kind === 'workflow' ? 'TestM5Workflow' : 'TestM5WorkingSet'} (1.00s)\n`);
    }
    const initial = analyze(root);
    assert.ok(initial.accepted > 0 && initial.rejected > 0);
    const chosen = frozen.cases[0];
    const altered = (filename, replace) => {
      const original = fs.readFileSync(filename);
      fs.writeFileSync(filename, replace(original));
      assert.throws(() => analyze(root));
      fs.writeFileSync(filename, original);
    };
    altered(path.join(root, 'logs', chosen.id + '.log'), () => '--- SKIP: TestM5Workflow (0s)\n');
    altered(path.join(root, 'results', chosen.id, 'semantic.json'), original => JSON.stringify({...JSON.parse(original), complete: false}));
    altered(path.join(root, 'results', chosen.id, 'measurement.json'), original => {
      const value = JSON.parse(original); value.os_after.cpu_seconds = -1; return JSON.stringify(value);
    });
    altered(path.join(root, 'results', chosen.id, 'cpu.pprof.top'), () => 'undecodable');
    altered(path.join(root, 'cgroup-after.events'), () => 'max 1\noom 1\noom_kill 1\n');
    altered(path.join(root, 'plan.json'), original => JSON.stringify({...JSON.parse(original), repetitions: 1}));
    altered(path.join(root, 'logs', 'measurement-race.jsonl'), () => JSON.stringify({Action: 'pass', Test: 'WrongTest'}) + '\n');
    altered(path.join(root, 'core-matrix.txt'), () => 'PASS linux/amd64 CGO_ENABLED=0\n');
    altered(path.join(root, 'overlap-monitor.json'), () => JSON.stringify({checks: 100, interval_milliseconds: 100, violation: true}));
    const candidate = frozen.cases.find(entry => entry.mode === 'kv' && entry.scenario === 'initial');
    const measurementPath = path.join(root, 'results', candidate.id, 'measurement.json');
    const originalMeasurement = fs.readFileSync(measurementPath);
    const partial = JSON.parse(originalMeasurement);
    partial.latency_count = 1;
    partial.runtime_delta.gc_cpu_seconds = .5;
    write(measurementPath, partial);
    const classified = analyze(root).cases.find(entry => entry.id === candidate.id);
    assert.equal(classified.classification, 'rejected');
    assert.ok(classified.reasons.includes('GC CPU share exceeds frozen paired baseline'));
    assert.ok(classified.reasons.includes('unsupported/coarse metrics or insufficient operation-tail observations'));
    fs.writeFileSync(measurementPath, originalMeasurement);
    const uncertain = JSON.parse(originalMeasurement);
    uncertain.latency_histogram = true;
    uncertain.latency_p99_lower_seconds = .109;
    uncertain.latency_p99_seconds = .111;
    write(measurementPath, uncertain);
    assert.equal(analyze(root).cases.find(entry => entry.id === candidate.id).classification, 'incomplete');
    fs.writeFileSync(measurementPath, originalMeasurement);
    const high = frozen.cases.find(entry => entry.kind === 'working' && entry.scenario === 'high-live' && entry.mode === 'kv');
    const highPath = path.join(root, 'results', high.id, 'measurement.json');
    const originalHigh = fs.readFileSync(highPath);
    const slowerReads = JSON.parse(originalHigh);
    slowerReads.lookup_latency.p99_seconds = .2;
    write(highPath, slowerReads);
    const highResult = analyze(root).cases.find(entry => entry.id === high.id);
    assert.equal(highResult.classification, 'rejected');
    assert.ok(highResult.reasons.includes('concurrent lookup p99 exceeds frozen paired baseline'));
    fs.writeFileSync(highPath, originalHigh);
    const repeated = analyze(root);
    assert.deepEqual(repeated, initial);
    process.stdout.write('PASS M5 analyzer named-gate, malformed/skip/profile/semantic/OOM/plan rejection and deterministic reproduction selftests\n');
  } finally {
    fs.rmSync(root, {recursive: true, force: true});
  }
}

if (process.argv[2] === '--plan') plan(process.argv[3]);
else if (process.argv[2] === '--selftest') selftest();
else if (process.argv[2] === '--overlap-check') overlapCheck(process.argv[3]);
else if (process.argv[2] === '--watch-overlap') watchOverlap(process.argv[3]);
else analyze(process.argv[2]);