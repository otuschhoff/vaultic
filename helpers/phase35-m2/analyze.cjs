const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');

function parse(text, count) {
  assert.match(text, /^PASS$/m);
  const benchmark = new RegExp(`^BenchmarkRAMScaling/entries-${count}-\\d+\\s+1\\s+[\\d.]+ ns/op$`, 'gm');
  assert.equal([...text.matchAll(benchmark)].length, 1, 'missing/wrong/duplicate scaling filter');
  const records = [...text.matchAll(/phase35_m2=(\{[^\n]+\})/g)];
  assert.equal(records.length, 1);
  const record = JSON.parse(records[0][1]);
  assert.equal(record.entries, count);
  assert.equal(record.state.Entries, count);
  assert.equal(record.state.IndexChunks, Math.ceil(count/256));
  assert.equal(record.state.AccountedRetainedBytes, record.state.Budget.Used);
  assert.ok(record.state.Budget.Peak <= record.state.Budget.Limit);
  for (const phase of ['Before', 'After', 'Released']) assert.equal((record[phase].unavailable || []).length, 0);
  assert.ok(record.After.scannable_heap_bytes <= record.Before.scannable_heap_bytes+1048576+record.state.MetadataCapacityBytes*4);
  assert.ok(record.After.heap_objects <= record.Before.heap_objects+record.state.ArenaChunks+record.state.IndexChunks+2048);
  assert.ok(record.After.heap_bytes <= record.Before.heap_bytes+record.state.AccountedRetainedBytes+1048576);
  assert.ok(record.Released.heap_bytes <= record.Before.heap_bytes+1048576);
  return record;
}

function analyze(root) {
  const read = name => fs.readFileSync(path.join(root, name), 'utf8');
  const runs = [];
  for (const count of [4096,65536,262144]) for (let repeat=1; repeat<=3; repeat++) {
    const stem = `entries-${count}-${repeat}`;
    runs.push(parse(read(`logs/${stem}.log`), count));
    assert.match(read(`logs/${stem}.time`), /Exit status: 0/);
    assert.match(read(`logs/${stem}.time`), /Maximum resident set size \(kbytes\): [1-9][0-9]*/);
    for (const suffix of ['cpu','heap']) {
      assert.ok(fs.statSync(path.join(root, `profiles/${stem}.${suffix}`)).size > 0);
      assert.match(read(`profiles/${stem}.${suffix}.top`), /^Showing nodes/m);
    }
  }
  const race = read('logs/race.log');
  for (const name of ['TestRAMConformanceAndBudget','TestRAMReplacementAccounting','TestRAMAdmissionBoundariesAndSharedRaces','TestRAMTreeOrderingAndPointerLayout','TestRAMEncodedConsumerReplay','TestRAMReadAdmissionAndCancelledReservations','TestRAMFailedGrowthPreservesState','TestRAMSharedWriteReservations','TestRAMCancelledQueuedRead']) assert.ok(race.includes(`--- PASS: ${name}`), `missing ${name}`);
  assert.match(race, /^PASS$/m);
  const forced = read('logs/forced-ram.log');
  assert.match(forced, /^PASS$/m);
  for (const kind of ['overlay','markers','directories']) assert.ok(forced.includes(`--- PASS: TestRAMEncodedConsumerReplay/${kind}`));
  const trace = read('logs/forced-ram.trace');
  assert.match(trace, /execve\(/, 'missing syscall capture');
  assert.match(trace, /\+\+\+ exited with 0 \+\+\+/, 'traced process did not complete');
  assert.doesNotMatch(trace, /O_CREAT|O_WRONLY|O_RDWR|mkdir|rename|unlink|truncate/, 'forced RAM performed working-state filesystem writes');
  assert.doesNotMatch(trace, /vaultic-(written|markers|cwalk)-|state\.db|MANIFEST|\.vlog/, 'forced RAM accessed a working database');
  assert.match(read('logs/native-386.log'), /^PASS$/m);
  const targets = ['linux/amd64','linux/arm64','linux/386','linux/arm','darwin/amd64','darwin/arm64','windows/amd64','windows/arm64','freebsd/amd64','freebsd/arm64','openbsd/amd64','openbsd/arm64'];
  for (const target of targets) assert.ok(read('build-matrix.txt').includes(`PASS ${target} CGO_ENABLED=0`));
  assert.equal(fs.readdirSync(path.join(root,'scratch')).length,0);
  return {milestone: '35-M2', production_integrated: false, filesystem_working_writes: 0, runs};
}

if (process.argv[2] === '--self-test') {
  assert.throws(() => parse('PASS\n',4096));
  assert.throws(() => parse('BenchmarkRAMScaling/entries-65536-4 1 12 ns/op\nPASS\n',4096));
  console.log('PASS M2 analyzer guards');
} else {
  const result = analyze(process.argv[2]);
  fs.writeFileSync(path.join(process.argv[2],'analysis.json'),JSON.stringify(result,null,2)+'\n');
  console.log('PASS M2 RAM/KV conformance, budgets, layout/GC scaling, zero working files, profiles, builds and release');
}