const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');

function parseRun(text, benchmark) {
  assert.match(text, /^PASS$/m, 'benchmark process did not pass');
  const escaped = benchmark.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const matches = [...text.matchAll(new RegExp(`^${escaped}-\\d+\\s+\\d+\\s+([\\d.]+) ns/op`, 'gm'))];
  assert.equal(matches.length, 1, 'benchmark missing, duplicated, or wrong filter');
  const nanos = Number(matches[0][1]);
  assert.ok(Number.isFinite(nanos) && nanos > 0);
  const observations = [...text.matchAll(/phase35_m0=(\{[^\n]+\})/g)].map(match => JSON.parse(match[1]));
  const digests = [...text.matchAll(/input_inventory_sha256=([a-f0-9]{64}) result_sha256=([a-f0-9]{64})/g)];
  return {nanos, observation: observations.at(-1), digests: digests.at(-1)?.slice(1)};
}

function validateObservation(value, kind) {
  assert.equal(value.state.kind, kind);
  assert.equal(value.state.reservation_enforced, false, 'M0 must not claim RAM enforcement');
  assert.equal(value.state.activated, true);
  assert.equal(value.state.observed_buffer_bytes, 0);
  for (const phase of ['before', 'after']) {
    assert.ok(value[phase].heap_bytes > 0);
    assert.ok(value[phase].allocated_bytes_total > 0);
    assert.ok(Array.isArray(value[phase].unavailable || []));
  }
  for (const key of ['allocated_bytes_total', 'gc_cycles_total', 'gc_cpu_seconds_total', 'gc_assist_cpu_seconds_total']) {
    assert.ok(value.after[key] >= value.before[key], `nonmonotonic runtime observation ${key}`);
  }
  assert.equal(/"(?:path|key|value|repository|credential)"/.test(JSON.stringify(value)), false);
  if (kind === 'written_blobs') {
    assert.equal(value.state.committed_entries, 8192);
    assert.equal(value.state.committed_encoded_bytes, 8192 * 148);
  }
  if (kind === 'markers') assert.ok(value.state.committed_entries >= 8192);
  if (kind === 'directories') assert.equal(value.state.committed_entries, 513);
  if (kind === 'import') assert.ok(value.state.peak_observed_buffer_bytes > 0);
  if (kind === 'check_spool') {
    assert.equal(value.state.scratch_reservation_enforced, true);
    assert.ok(value.state.peak_reserved_scratch_bytes > 0);
    assert.match(value.result_sha256, /^[a-f0-9]{64}$/);
  }
}

function median(values) {
  const ordered = [...values].sort((left, right) => left - right);
  const middle = Math.floor(ordered.length / 2);
  return ordered.length % 2 ? ordered[middle] : (ordered[middle - 1] + ordered[middle]) / 2;
}

function validateEvidence(root, stem) {
  for (const suffix of ['cpu', 'heap']) {
    const profile = path.join(root, 'profiles', `${stem}.${suffix}`);
    assert.ok(fs.statSync(profile).size > 0, `empty profile ${profile}`);
    const decoded = fs.readFileSync(`${profile}.top`, 'utf8');
    assert.match(decoded, /^Type: /m, `undecoded profile ${profile}`);
    assert.match(decoded, /^Showing nodes/m, `missing profile samples ${profile}`);
  }
  const timing = fs.readFileSync(path.join(root, 'logs', `${stem}.time`), 'utf8');
  assert.match(timing, /Exit status: 0/);
  const rss = timing.match(/Maximum resident set size \(kbytes\): (\d+)/);
  assert.ok(rss && Number(rss[1]) > 0, `missing RSS evidence ${stem}`);
}

function analyze(root, repetitions) {
  assert.ok(Number.isSafeInteger(repetitions) && repetitions >= 3);
  const summary = {schema_version: 1, repetitions, production_validation: false, comparisons: {}, probes: {}};
  const cases = {overlay: 'BenchmarkWrittenBlobLookup/true', import: 'BenchmarkImportPackTransactionSizes/packs-8', check: 'BenchmarkCheckWithOptions/synthetic-1x/workers=4'};
  for (const [label, benchmark] of Object.entries(cases)) {
    const runs = {};
    const digests = [];
    for (const variant of ['control', 'candidate']) {
      runs[variant] = [];
      for (let repeat = 1; repeat <= repetitions; repeat++) {
        const text = fs.readFileSync(path.join(root, 'logs', `${variant}-${label}-${repeat}.log`), 'utf8');
        const run = parseRun(text, benchmark);
        validateEvidence(root, `${variant}-${label}-${repeat}`);
        runs[variant].push(run.nanos);
        if (label === 'check') {
          assert.equal(run.digests?.length, 2, 'check parity digest missing');
          digests.push(run.digests.join(':'));
        }
      }
    }
    const ratio = median(runs.candidate) / median(runs.control);
    summary.comparisons[label] = {...runs, candidate_control_median_ratio: ratio, review_required: ratio > 1.15};
    if (label === 'check') {
      assert.equal(new Set(digests).size, 1, 'control/candidate check logical parity differs');
      summary.comparisons[label].parity = digests[0];
    }
  }
  const probes = {overlay: ['WrittenBlobs', 'written_blobs'], markers: ['Markers', 'markers'], directories: ['Directories', 'directories'], import: ['Import', 'import'], check: ['Check', 'check_spool']};
  for (const [label, [suffix, kind]] of Object.entries(probes)) {
    const observations = [];
    for (let repeat = 1; repeat <= repetitions; repeat++) {
      const text = fs.readFileSync(path.join(root, 'logs', `candidate-probe-${label}-${repeat}.log`), 'utf8');
      const run = parseRun(text, `BenchmarkPhase35M0${suffix}`);
      validateObservation(run.observation, kind);
      validateEvidence(root, `candidate-probe-${label}-${repeat}`);
      observations.push(run);
    }
    if (kind === 'check_spool') assert.equal(new Set(observations.map(run => run.observation.result_sha256)).size, 1);
    summary.probes[label] = observations;
  }
  for (const [label, owner] of Object.entries({index: 'written', archiver: 'markers', crawl: 'cwalk'})) {
    const trace = fs.readFileSync(path.join(root, 'logs', `files-${label}.trace`), 'utf8');
    assert.ok(trace.includes(`vaultic-${owner}`), `working files absent from ${label} trace`);
    assert.match(trace, /O_CREAT|mkdir/, `creation absent from ${label} trace`);
  }
  for (const name of ['dependency-directories.txt', 'dependency-working-files.txt', 'baseline-revision.txt', 'candidate-revision.txt', 'candidate-untracked.tar', 'runtime-settings.txt', 'artifact-filesystem.txt']) {
    assert.ok(fs.statSync(path.join(root, name)).size > 0, `missing capture metadata ${name}`);
  }
  assert.equal(fs.readdirSync(path.join(root, 'scratch')).length, 0, 'scratch not empty');
  return summary;
}

if (process.argv[2] === '--self-test') {
  assert.equal(parseRun('BenchmarkFixture-4 1 12 ns/op\nPASS\n', 'BenchmarkFixture').nanos, 12);
  assert.throws(() => parseRun('PASS\n', 'BenchmarkFixture'));
  assert.throws(() => parseRun('BenchmarkFixture-4 1 12 ns/op\nFAIL\n', 'BenchmarkFixture'));
  assert.throws(() => parseRun('BenchmarkOther-4 1 12 ns/op\nPASS\n', 'BenchmarkFixture'));
  assert.throws(() => parseRun('BenchmarkFixture-4 1 12 ns/op\nBenchmarkFixture-4 1 12 ns/op\nPASS\n', 'BenchmarkFixture'));
  assert.throws(() => validateEvidence('/nonexistent-phase35-m0-fixture', 'missing'));
  const runtime = {heap_bytes: 1, allocated_bytes_total: 2, gc_cycles_total: 0, gc_cpu_seconds_total: 0, gc_assist_cpu_seconds_total: 0};
  const value = {state: {kind: 'written_blobs', activated: true, reservation_enforced: false, observed_buffer_bytes: 0, committed_entries: 8192, committed_encoded_bytes: 8192 * 148}, before: runtime, after: runtime};
  validateObservation(value, 'written_blobs');
  assert.throws(() => validateObservation({...value, state: {...value.state, reservation_enforced: true}}, 'written_blobs'));
  assert.throws(() => validateObservation({...value, state: {...value.state, committed_entries: 1}}, 'written_blobs'));
  assert.throws(() => validateObservation({...value, after: {...runtime, allocated_bytes_total: 1}}, 'written_blobs'));
  assert.throws(() => validateObservation({...value, path: 'sensitive'}, 'written_blobs'));
  assert.equal(median([3, 1, 2]), 2);
  assert.equal(median([1, 4, 2, 3]), 2.5);
  console.log('PASS: M0 analyzer self-tests');
} else {
  const result = analyze(process.argv[2], Number(process.argv[3]));
  fs.writeFileSync(path.join(process.argv[2], 'analysis.json'), JSON.stringify(result, null, 2) + '\n');
  console.log('PASS: isolated baseline/probe counts, observations, parity, decoded profiles, file traces, capture metadata, and scratch cleanup');
}
