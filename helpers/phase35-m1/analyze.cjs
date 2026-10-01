const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');

function percentile(values, fraction) {
  assert.ok(values.length > 0);
  assert.ok(values.every(value => Number.isSafeInteger(value) && value > 0));
  const ordered = [...values].sort((left, right) => left-right);
  return ordered[Math.ceil(ordered.length*fraction)-1];
}

function parse(text, backend, workload) {
  assert.match(text, /^PASS$/m);
  const benchmark = new RegExp(`^BenchmarkReplay/${backend}/${workload}-\\d+\\s+1\\s+[\\d.]+ ns/op$`, 'gm');
  assert.equal([...text.matchAll(benchmark)].length, 1, 'wrong/absent/duplicate benchmark');
  const records = [...text.matchAll(/phase35_m1=(\{[^\n]+\})/g)];
  assert.equal(records.length, 1, 'misaligned runtime observations');
  const record = JSON.parse(records[0][1]);
  assert.equal(record.backend, backend);
  assert.equal(record.workload, workload);
  assert.equal(record.entries, workload === 'directories' ? 529 : 8192);
  assert.match(record.input_sha256, /^[a-f0-9]{64}$/);
  assert.match(record.result_sha256, /^[a-f0-9]{64}$/);
  assert.equal(record.expected_result_sha256, record.result_sha256);
  assert.equal(record.latencies_ns.put.length, {overlay: 2, markers: 4, directories: 5}[workload]);
  assert.equal(record.latencies_ns.open.length, 1);
  assert.equal(record.latencies_ns.close.length, 1);
  assert.equal(record.latencies_ns.get.length, record.entries);
  assert.equal(record.latencies_ns.scan.length, Math.ceil(record.entries/8)+1);
  for (const phase of ['runtime_before', 'runtime_after']) {
    assert.equal((record[phase].unavailable || []).length, 0);
    assert.ok(record[phase].heap_bytes > 0);
  }
  for (const phase of ['resources_before', 'resources_after', 'sampled_peak']) assert.equal((record[phase].unavailable || []).length, 0, `resource availability ${phase}`);
  for (const metric of ['allocated_bytes_total', 'gc_cycles_total', 'gc_cpu_seconds_total', 'gc_assist_cpu_seconds_total']) assert.ok(record.runtime_after[metric] >= record.runtime_before[metric]);
  for (const metric of ['cpu_ticks', 'read_bytes', 'write_bytes', 'read_calls', 'write_calls']) assert.ok(record.resources_after[metric] >= record.resources_before[metric]);
  assert.ok(record.sampled_peak.rss_bytes > 0);
  return record;
}

function analyze(root, repetitions) {
  assert.ok(Number.isSafeInteger(repetitions) && repetitions >= 3);
  const read = name => fs.readFileSync(path.join(root, name), 'utf8');
  const summary = {repetitions, production_validation: false, candidates: {}};
  const digests = {};
  for (const backend of ['pebble', 'bbolt', 'badger']) {
    const candidate = {eligible: backend !== 'pebble', workloads: {}};
    for (const workload of ['overlay', 'markers', 'directories']) {
      const runs = [];
      for (let repeat=1; repeat<=repetitions; repeat++) {
        const stem = `${backend}-${workload}-${repeat}`;
        const record = parse(read(`logs/${stem}.log`), backend, workload);
        digests[workload] ||= new Set();
        digests[workload].add(`${record.input_sha256}:${record.result_sha256}`);
        const latency = {};
        for (const [operation, values] of Object.entries(record.latencies_ns)) latency[operation] = {count: values.length, p50_ns: percentile(values, .5), p99_ns: percentile(values, .99), max_ns: percentile(values, 1)};
        const eligible = record.sampled_peak.rss_bytes < 512*1024*1024 && record.sampled_peak.mapped_store_bytes < 512*1024*1024 && record.sampled_peak.scratch_bytes < record.encoded_bytes*8+64*1024*1024 && latency.put.p99_ns < 100e6 && latency.get.p99_ns < 10e6 && latency.scan.p99_ns < 500e6;
        candidate.eligible &&= eligible;
        for (const suffix of ['cpu', 'heap']) {
          assert.ok(fs.statSync(path.join(root, `profiles/${stem}.${suffix}`)).size > 0);
          assert.match(read(`profiles/${stem}.${suffix}.top`), /^Showing nodes/m);
        }
        const timing = read(`logs/${stem}.time`);
        assert.match(timing, /Exit status: 0/);
        assert.match(timing, /Maximum resident set size \(kbytes\): [1-9][0-9]*/);
        runs.push({eligible, latency, runtime_before: record.runtime_before, runtime_after: record.runtime_after, resources_before: record.resources_before, resources_after: record.resources_after, sampled_peak: record.sampled_peak, encoded_bytes: record.encoded_bytes, digest: record.result_sha256});
      }
      candidate.workloads[workload] = runs;
    }
    assert.match(read(`logs/enospc-${backend}.trace`), /ENOSPC.*INJECTED/);
    if (backend !== 'pebble') {
      assert.equal(read(`logs/enospc-${backend}.exit`).trim(), '0');
      assert.match(read(`logs/enospc-${backend}.log`), /^PASS$/m);
    } else assert.match(read('logs/enospc-pebble.log'), /MANIFEST flush failed:.*no space left on device/);
    assert.match(read(`logs/permission-${backend}.log`), /^PASS$/m);
    if (backend !== 'pebble') {
      assert.match(read(`logs/close-${backend}.trace`), /EIO.*INJECTED/);
      const status = read(`logs/close-${backend}.exit`).trim();
      candidate.close_cause_preserved = status === '0';
      if (status === '0') assert.match(read(`logs/close-${backend}.log`), /^PASS$/m);
      else {
        assert.equal(backend, 'badger', 'selected candidate close fault failed');
        assert.equal(status, '1');
        assert.match(read('logs/close-badger.log'), /expected close EIO/);
        assert.match(read('logs/close-badger.log'), /DB.Close err: close .*input\/output error/);
        candidate.eligible = false;
        candidate.rejection = 'pinned close wrapper discards original errno cause';
      }
    }
    summary.candidates[backend] = candidate;
  }
  for (const digest of Object.values(digests)) assert.equal(digest.size, 1, 'candidate/repeat semantic parity differs');
  const targets = ['linux/amd64','linux/arm64','linux/386','linux/arm','darwin/amd64','darwin/arm64','windows/amd64','windows/arm64','freebsd/amd64','freebsd/arm64','openbsd/amd64','openbsd/arm64'];
  for (const target of targets) assert.ok(read('build-matrix.txt').includes(`PASS ${target} CGO_ENABLED=0`));
  assert.match(read('logs/race.log'), /^PASS$/m);
  assert.match(read('logs/growth-bbolt.log'), /^--- PASS: TestGrowthFailureProcess/m);
  assert.match(read('logs/growth-bbolt.trace'), /EFBIG/);
  assert.equal(fs.readdirSync(path.join(root, 'scratch')).length, 0);
  assert.ok(summary.candidates.bbolt.eligible, 'selected bbolt failed selection gates; M2 blocked');
  summary.selected_backend = 'bbolt';
  summary.selected_version = 'v1.5.0';
  return summary;
}

if (process.argv[2] === '--self-test') {
  assert.equal(percentile([4,1,3,2], .5), 2);
  assert.equal(percentile([4,1,3,2], .99), 4);
  assert.throws(() => percentile([], .5));
  assert.throws(() => percentile([NaN], .5));
  assert.throws(() => parse('PASS\n', 'bbolt', 'overlay'));
  assert.throws(() => parse('BenchmarkReplay/pebble/overlay-4 1 12 ns/op\nPASS\n', 'bbolt', 'overlay'));
  console.log('PASS M1 analyzer guards');
} else {
  const result = analyze(process.argv[2], Number(process.argv[3]));
  fs.writeFileSync(path.join(process.argv[2], 'analysis.json'), JSON.stringify(result, null, 2)+'\n');
  console.log('PASS M1 parity, safety gates, profiles, filesystem faults, builds and cleanup');
}