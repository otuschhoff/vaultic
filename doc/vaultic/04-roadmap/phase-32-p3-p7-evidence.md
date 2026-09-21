# Phase 32 P3-P7 Experiment and Acceptance Evidence

This record reports the currently executable Phase 32 work as of 2026-09-21.
P3a, P3b, P7a, matched NFS/RGW/native-RADOS sampling, native RADOS lifecycle
tests, live RADOS-WAL latency, and P4 are validated. P3c remains partial: its
memory-WAL role pilots pass, but the complete response matrix is not yet run.
P4 decouples ordered reduction with a bounded worker and coordinator-owned
acknowledgements. A current telemetry baseline selects P5 read amplification,
not P6 writer concurrency, as the next optimization area. The first P5 cache
fill-budget experiment was rejected. The current uncapped import and recovered
activation now provide final repository-scale P7b acceptance.

Importer-owned Phase 34 export is now implemented for future full runs. It emits
the scheduler and Go runtime component together with VaulticDB queue/service,
WAL, cache, LSM and role-specific object-store measurements at a bounded interval,
then makes a bounded final snapshot attempt after the lifecycle action completes. Export failures
and replace-oldest drops are explicit metrics and never block the import. The
new representative results below are accepted only as bounded diagnostic
evidence; the uncapped acceptance result is reported separately under P7.

## Current Telemetry Baseline and P5 Selection

The valid current-revision HDD baseline is retained at
`phase32-telemetry-20260920-hdd-30m-r2`. Vaultic SHA-256 was
`84510e9ea3e91849769153cfe3ada8b62e6cb09050712ef26b7f1dbf92bc3da2` and
VaulticDB SHA-256 was
`b888cf2d0d6366213b85267361ca32414f57e3d776a8fb2b3ec0ee5420540087`.
The 30m01.985s capture contains 361 schema-valid JSONL snapshots at mode `0600`:
359 exact VaulticDB samples, one startup-unavailable sample, one stale sample,
one process identity, and zero exporter failures or drops. The timeout exit 124
is the intended diagnostic boundary, not full-import completion evidence.

At the boundary the importer had processed 1,905 of 10,019 indexes, 100,143
packs, and 80,622,391 blobs at 44,819.9 blobs/s. The wrapper used 319% CPU and
19,901,000 KiB maximum RSS. VaulticDB averaged about 2.54 CPU cores, reached
20.11 GB RSS, read 40.63 GB and wrote 43.54 GB. Its writer queue had p95 depth
zero and maximum depth two, with no queue rejects, backpressure, or L0 stalls.
Transaction-map and slot-lock waits averaged 0.315 and 0.137 microseconds.
Those observations reject P6 as the first experiment: the sequential writer was
not saturated and had substantial memory and CPU headroom.

P5 is selected because planning and reduction accumulated 16m41.765s and
13m55.638s of concurrent worker time while database reads grew with the data
set. VaulticDB issued 325,336 database GETs carrying 263.98 GB, with 3,467.9s
of concurrent GET latency. The 64 GiB memory cache ended at only 389.2 MB used:
94,322 hits, 215,651 misses, 231,002 origin reads, 8,111 admissions, and 200,600
admission rejections. These overlapping worker, RPC, and background totals are
diagnostic concurrency measures and are not summed as wall-clock percentages.

The first P5 experiment increased only the fresh-import cache in-flight fill
budget from 256 MiB to 2 GiB. Its frozen Vaultic SHA-256 was
`2963a34a04f80e5fd2bfcd40c154a2f0f28c9072738c7ee00efe0fc60b0a4192`; the
VaulticDB binary and all other import settings matched the baseline. The bounded
candidate is retained at `phase32-p5-cache-20260920-hdd-30m-r1` and produced 360
valid snapshots with zero exporter failures or drops. It increased admissions
from 8,111 to 13,372 and final cache occupancy from 389.2 MB to 538.1 MB, but
rejections remained 197,141. Database GET count fell only 0.2%, GET bytes rose
3.3% to 272.73 GB, process read/write bytes more than doubled, wrapper CPU rose
from 319% to 355%, maximum RSS rose to 22,839,700 KiB, and throughput fell 0.9%
to 44,432.2 blobs/s. The experiment is rejected and the 256 MiB default is
retained.

A second 30-minute run, retained at
`phase32-cache-reasons-20260920-hdd-30m-r1`, kept the accepted 256 MiB byte
budget and added reason-specific counters. Of 184,545 rejections, 184,538
(99.996%) were background-task-limit failures, seven were reservation failures,
and none were byte-budget failures. The run produced 360 valid snapshots with
zero exporter failures or drops and used 317% CPU and 20,341,756 KiB maximum
RSS. This identifies the existing derived 16-task limit as the immediate
admission gate without claiming that admitting more entries improves runtime.

The third bounded experiment raised only that task limit from 16 to 128 while
retaining the 256 MiB byte budget. Frozen binaries were Vaultic
`0aa32b9df9d53277889d953b1250c946ec0ca099d871780e5e93f62ecde9156d` and
VaulticDB
`66f7b6b544731287813ad53c6aa2d0be7a6f4f834bfc03a04aa208e404940638`;
artifacts are retained at `phase32-p5-cache-tasks-20260921-hdd-30m-r1`.
Admissions rose from 6,820 to 12,958 and origin reads fell 5.1%, but throughput
fell 0.9% from 42,733.9 to 42,343.7 blobs/s. Database GET bytes rose 1.6%, GET
latency rose 9.3%, wrapper CPU rose from 317% to 349%, and process read/write
bytes approximately doubled. The 128-task profile is therefore rejected. The
fresh-import default remains the prior derived 16 tasks; an optional bounded
task setting remains available for controlled diagnostics. The next P5
experiment must improve cache admission selectivity or reuse rather than merely
increase fill concurrency.

The accepted bounded P5 candidate combines the reducer's aggregate and history planning reads
into one transactional `MultiGet` after the receipt idempotency check. Stage 2
and Stage 3 output-equivalence tests pass, and telemetry now requires exactly
one planning read RPC per reduced batch. Process-local importer distributions
are exported in the same aligned monitor snapshot as VaulticDB, including the
new `reduce_prefetch` I/O stage and separate local aggregate/history planning
stages. Existing SlateDB GET-key and point-filter positive, negative and false-
positive counters are also exported with explicit mixed-version availability.

Three matched 30-minute HDD runs are retained at
`phase32-p5-reducer-prefetch-20260921-hdd-30m-r1` through `r3`. They processed
83,961,394, 81,485,471, and 83,576,762 blobs; the median 46,451 blobs/s is 8.8%
above the retained accepted-default baseline's 42,709 blobs/s. Planning reads
fell from approximately three RPCs per reduced batch to one. Median reducer
service fell 21.0% and reducer wait fell 42.9%. Every run produced 361 valid
private snapshots with 359 exact VaulticDB samples, one stale sample, one
startup-unavailable sample, and zero exporter failures or drops. No run
reported retries, conflicts, backpressure, or L0 stalls.

The throughput gain costs more available resources: median wrapper CPU rose
from 317% to 355%, maximum RSS from 20,341,756 KiB to 23,638,476 KiB, filesystem
input by 25.6%, and filesystem output by 28.6%. At a matched blob prefix in the
first repetition, database GET count was effectively flat, GET bytes were 1.9%
lower, and cumulative GET latency was 1.6% lower, so the larger terminal totals
primarily reflect more work and deeper LSM progress. The speed improvement is
accepted for bounded P5 use because no measured capacity limit was approached;
resource efficiency remains a later tuning target. These timeout runs do not
prove full-import completion or finalization acceptance.

SlateDB multi-get instrumentation is now pinned at revision
`f549d4af9e8a7e41f46c45f4f31095cc743a703a`. It reports calls, input and unique
keys, SST visits, candidate keys, needed blocks and bytes, active coalesced reads
and bytes, plus exact request/byte projections for gaps 8, 32 and 128. A separate
`engine_multi_get_metrics_available` capability prevents mixed-version daemons
from presenting absent counters as exact zero. Three matched 30-minute baselines
at `phase32-p5-multiget-telemetry-20260921-hdd-30m-r1` through `r3` produced
44,055.5, 47,253.0 and 46,352.1 blobs/s.

The frozen projection run at
`phase32-p6-coalescing-projection-20260921-hdd-30m-r1` used Vaultic SHA-256
`e498c7cd352fb5422135f08ecd2aa1587fa4fea099bfaaa412ec1ab8a9940e38` and
VaulticDB SHA-256
`2cc115d3124b7cb79f84f5820349e97910ff4b8450beafa669c2c2ddcb86b48c`.
It reached 46,870.5 blobs/s with 361 valid snapshots. Against the active gap 2,
gap 8 reduced requests by 2.16% but increased bytes by 11.59%; gap 32 reduced
requests by 9.84% but increased bytes by 163%; gap 128 reduced requests by
30.90% but increased bytes by 17.25 times. All larger gaps are rejected, gap 2
is retained, and no post-change repetitions are required because no policy
change was promoted.

## Ten-Minute Full-Import Experiments

The session-local positive-record cache candidate is rejected. Run
`phase32-p9-record-cache-20260921-hdd-10m-r1` reused only 1,164 records and
reduced catalog keys by 3.32%, while throughput fell 1.91% versus the deferred
cleanup baseline. Early throughput was effectively flat (+0.63%), late
throughput fell 7.60%, and planning, blob-read, and reducer-prefetch time all
increased.

Cross-index transaction batching is provisionally accepted for matched
confirmation. The corrected candidate
`phase32-p10-cross-index-20260921-hdd-10m-r2`, Vaultic SHA-256
`d188984d386e191191ed851bf8d4c72c3fd5868ad9224120a848e1fb84925bc7`, produced
89,308 blobs/s over 591.2 seconds versus 80,921 blobs/s for deferred-cleanup r4,
a 10.36% gain. Early throughput improved 11.89% and late throughput improved
9.70%. Cleanup calls fell from 1,114 to 308, cleanup time per million blobs fell
72%, read bytes per blob fell 5.4%, write bytes per blob fell 4.5%, and sampled
peak RSS fell from 12.08 to 11.75 GiB. There were no retries, conflicts,
backpressure events, or L0 stalls before timeout-bound cancellation.

The candidate does not remove the shared runtime decay: throughput fell 21.96%
from the early to late windows versus 20.41% for r4. Late planning remained
23.18 ms per batch, blob reads 11.72 ms per batch, and reducer prefetch rose to
7.03 ms per batch. SlateDB multi-get SST visits increased from 1.96 to 6.13 per
call between windows. Ingest wait remained the largest scheduler phase at
405.7 seconds, followed by schedule work at 129.3 seconds and dependency wait
at 33.6 seconds. The next optimization should target growing-state reads rather
than publication lanes or write backpressure.

Doubling the L0 SST target from 256 MiB to 512 MiB is rejected. Run
`phase32-p11-l0-512m-20260921-hdd-10m-r1` produced 86,828 blobs/s, 2.78% below
the cross-index r2 candidate. Early throughput fell 6.74% and late throughput
fell 5.97%; decay improved only from 21.96% to 21.32%. The larger target cut
read bytes per blob by 35.9% and write bytes per blob by 33.8%, but sampled peak
RSS rose from 11.75 to 12.45 GiB and commit time per million blobs rose 18.0%.
It also failed the read-amplification hypothesis: late multi-get SST visits rose
from 6.13 to 6.72 per call, needed bytes per key rose from 913 to 1,006, late
blob-read time rose from 11.72 to 14.64 ms per batch, and late reducer-prefetch
time rose from 7.03 to 11.46 ms per batch. There were no retries, conflicts,
backpressure events, or L0 stalls before timeout-bound cancellation. Keep the
256 MiB L0 SST target.

## Frozen Inputs and Build

The real-daemon fixture contains four ordered indexes, 128 preselected packs,
and 65,536 blobs. Import runs used `GOMAXPROCS=4`, `GOMEMLIMIT=8GiB`, and three
measured iterations; latency sweeps used 100 operations per point. Fresh import
used memory WAL unless explicitly labeled persistent local WAL. All runs used
the optimized Linux amd64 daemon with SHA-256
`6e83382b752e372c6434496973fc48d1a635f77fa759118fdfe8113d6b4648a5`.
The daemon uses SlateDB fork revision
`fc68f09a25defb128edfd722ec82696492dbb692` and Rust `1.98.1`.

The representative NFS runs used the profile binaries built from
`2b30207fc77a2711fe83a09bfc4cdc208961ee2e`: Vaultic SHA-256
`ffb4383328ca5dd97958aa068f0465cdd421674593092506c2fba5d6fb3d51e1` and
VaulticDB SHA-256
`dad411c615b4f530f82fffbfde19f3299c0b963eadadcb8fde10ebb209810d83`.
They ran on 32 logical Intel Xeon Gold 5217 CPUs with 292 GiB RAM. The SSD path
resolved to `/ncl1-1-vs-50/fme_dump/amakura/db-test` on
`ncl1-1-vs-55.eu.socionext.com:/fme_dump`, NFSv3/TCP with 64 KiB reads/writes.
The HDD path remained on `172.21.33.209:/volume2/NASDA2`, NFSv3/TCP with 128 KiB
reads/writes. Results are comparisons of those deployed paths, not media-only
microbenchmarks.

The native RADOS comparison used the working tree based on
`668e08a939f9c346a19889a8cbb1ee46479c7f65`, including direct main-store RADOS
support and atomic object attributes required by SlateDB retry verification.
Vaultic SHA-256 was
`47f3eb608c207ce5e1bddc3ae9107ca45a3cbf4aca7319381a39c4d4c267dd64`; the
RADOS-enabled VaulticDB SHA-256 was
`3a430bd05057c4df275608c46669395de562b0e60218d76847876e49eb45b8ad`.
Main data used pool `db-sst`; WAL used `db-wal`. Both used namespace
`vaultic-perf` with fresh, independent `phase32/668e08a93-*-45m-v2` prefixes.

The P4 comparison used the working tree based on
`36c396c19c466456dcc981f59e216afa0f5bd2c4`. Vaultic SHA-256 was
`d063ed0595c5296f735ed25551b8a08e3f533e10230d559903660d58607d2593`;
the RADOS-enabled VaulticDB SHA-256 remained
`3a430bd05057c4df275608c46669395de562b0e60218d76847876e49eb45b8ad`.
Main data used `db-sst`, WAL used `db-wal`, and fresh prefixes were
`phase32/p4-20260917-main-45m-v2` and
`phase32/p4-20260917-wal-45m-v2` in namespace `vaultic-perf`.

The versioned object-delay profile is accepted only by a build with
`test-failpoints`, requires `target=isolated` and the process-test capability,
and validates its role, operation, and bounded delay before startup. Delay is
inside the selected object-store PUT future, below SlateDB. The client response
delay is a separate typed test option and starts only after a successful commit
RPC has returned, so it cannot be mistaken for server persistence time.

## P3 Results

Attribution enabled and disabled runs had identical input, result, checkpoints,
close, handoff, reopen, and validation. Enabled time was 693.320 ms/op versus
690.433 ms/op disabled, a 0.42% difference. This is bounded smoke-fixture
overhead, not repository-scale performance evidence.

The persistent local-WAL pilot used one mutation and one explicit durable commit
per operation with a 1 ms flush interval. Each transaction issued exactly two
WAL PUTs and one durability wait:

| Added WAL PUT delay | Commit p95 | Commit p99 |
|---:|---:|---:|
| 0 ms | 2.539 ms | 2.788 ms |
| 10 ms | 25.43 ms | 25.55 ms |
| 50 ms | 105.7 ms | 105.8 ms |
| 200 ms | 406.5 ms | 406.6 ms |

The approximately `2 * delay` slope reaches `durable_wait`; queue and writer
service remain small at 3.23-11.02 us/op and 7.07-26.22 us/op respectively. The
matched post-success response sweep leaves storage fast:

| Added response delay | Client commit p95 | Client commit p99 |
|---:|---:|---:|
| 0 ms | 2.614 ms | 2.711 ms |
| 1 ms | 3.776 ms | 4.022 ms |
| 8 ms | 11.31 ms | 11.43 ms |
| 25 ms | 28.58 ms | 28.82 ms |
| 100 ms | 103.6 ms | 103.6 ms |
| 250 ms | 254.1 ms | 254.3 ms |

Across that sweep, server commit stayed at 1.486-2.140 ms and durability wait at
1.315-1.815 ms. A separate deadline test withholds a successful response beyond
the caller timeout and proves idempotent recovery with no duplicate mutation or
lost committed state. The exact benchmark lines are retained in
[Phase 32 P3 latency raw output](phase-32-p3-latency-raw.txt).

Isolated fresh-import role pilots varied one dependency at a time:

| Profile | End-to-end | Import throughput | Finalize |
|---|---:|---:|---:|
| No injection | 693.320 ms/op | 156,892 blobs/s | 0.2756 s |
| Source load +10 ms/index | 693.156 ms/op | 148,502 blobs/s | 0.2518 s |
| Main PUT +10 ms | 847.687 ms/op | 153,931 blobs/s | 0.4219 s |
| Coordination PUT +10 ms | 747.073 ms/op | 156,029 blobs/s | 0.3270 s |

Source delay is hidden at this scale. Main and coordination PUT delays affect
the mandatory finalization tail while import-only throughput remains broadly
flat. The fixture does not show sustained eligible-work blockage that justifies
P4, a dominant Go/RPC cost for P5, or a specific SlateDB function for P6.
Increasing lanes also has no benefit. The isolated pilot alone therefore did
not select P4-P6; two lanes, deferred cleanup opt-in, and all durability checks
were retained pending representative evidence.

Representative storage sampling did reopen that decision. Matched runs used
the same source, fresh candidate settings, 32 pack workers, two publication
lanes, deferred cleanup, and a 45-minute interrupt boundary:

| Main metadata path | Boundary | Indexes | Packs | Blobs | Blobs/s | Peak prepared queue | Max RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| SSD NFS export | 45m02s | 2,369 | 126,229 | 100,525,966 | 37,200 | 13,565,280 B | 38.9 GB |
| HDD NFS export | 44m59s | 2,241 | 119,381 | 94,882,587 | 35,151 | 13,565,280 B | 24.3 GB |
| Native RADOS (`db-sst`; WAL `db-wal`) | 44m42s | 1,114 | 58,226 | 47,651,281 | 17,766 | 13,565,280 B | 20,017,120 KiB |
| RGW/S3 | 44m55s | 404 | 20,481 | 17,158,835 | 6,367 | 13,442,816 B | 4,555,272 KiB |

The selected checkpoints drained the prepared queue to zero. Before their
planned boundaries, no run reported a retry, conflict, reduction failure, or
filter fallback. Timeout cancellation caused one expected interrupted
ingest/recovery read for HDD and one reduction failure/recovery read for RADOS
while draining; exit 124 and final `context canceled` records are comparison
boundaries, not data findings. The SSD row is an in-process checkpoint from its
longer run; the HDD importer stopped at 44m59.315s within a 45m02.46s wrapper.
The RADOS wrapper ended at 45m03.08s and its final scheduler state was
`finished`, with no pending reduction or retained bytes.

Native RADOS delivered 47.8% of SSD-NFS and 50.5% of HDD-NFS blob throughput,
and 2.79 times RGW throughput. The NFS and RGW rows used revision `2b30207fc`;
RADOS required the current working tree above, so this is a matched workload
and boundary comparison rather than a single-revision backend isolation.

The scheduler evidence is decisive for P4:

| Main metadata path | Eligible-ready during reduction | Reduction blocking |
|---|---:|---:|
| SSD NFS export | 10,198 observations / 7m49.316s | 11,861 / 20m41.020s |
| HDD NFS export | 9,907 observations / 9m35.565s | 11,059 / 22m11.127s |
| Native RADOS | 6,273 observations / 12m49.730s | 4,644 / 27m11.354s |
| RGW/S3 | 2,325 observations / 15m28.918s | 1,481 / 28m42.631s |

This satisfies P4's prerequisite of eligible work waiting during synchronous
ordered reduction on representative storage. It does not prove that decoupling
will improve end-to-end throughput, so P4 remains an experiment with the
roadmap's stop condition rather than an accepted optimization. The evidence
does not isolate a P5 client/RPC target or P6 SlateDB function.

## P4 Results

One bounded reducer worker now consumes successful ingests in ordinal order
while the coordinator continues receiving completions and refilling independent
lanes. Only coordinator acknowledgement releases dependencies and prepared
bytes or publishes committed counters and checkpoints. Tests cover blocked
refill, `A, A+B, B`, `A, A, C-fails`, delayed earliest completion, adaptive
children, reducer and ingest failures, caller cancellation, a full reducer
outcome channel, exact retained bytes, and final-checkpoint acknowledgement.

The frozen real-daemon fixture used the same baseline revision, input, daemon,
`GOMAXPROCS=4`, `GOMEMLIMIT=8GiB`, two lanes, deferred cleanup, and three
repetitions per candidate:

| Candidate | Median import blobs/s | Median end-to-end blobs/s | Median finalize |
|---|---:|---:|---:|
| Synchronous baseline | 151,963 | 90,849 | 0.2850 s |
| P4 bounded reducer | 158,744 | 90,807 | 0.3089 s |

Import-only throughput improved 4.5%. End-to-end throughput was flat on this
small fixture because finalization dominates; all 60 final scheduler snapshots
per candidate drained ready batches, pending reductions, retained bytes, and
unreduced bytes to zero. Higher lane counts remained flat, so two lanes remain
the accepted setting.

The representative P4 run reused the native-RADOS workload and 45-minute
boundary with 32 pack workers, two lanes, deferred cleanup, `db-sst` main data,
and `db-wal` WAL:

| Candidate | Boundary | Indexes | Packs | Blobs | Blobs/s | Peak prepared queue | Max RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| Synchronous baseline | 44m42s | 1,114 | 58,226 | 47,651,281 | 17,766 | 13,565,280 B | 20,017,120 KiB |
| P4 bounded reducer | 44m58s | 1,140 | 60,165 | 48,618,467 | 18,018.6 | 13,565,280 B | 20,413,300 KiB |

P4 improved blob throughput 1.42%, packs 3.33%, and indexes 2.33%. Coordinator
`reducer_wait` was 7m06.858s while measured reducer service was 32m29.674s,
compared with 27m11.354s of synchronous reduction blocking in the baseline.
The final scheduler state was `finished` with zero active lanes, ready batches,
pending reductions, retained bytes, and unreduced bytes. Before the interrupt
there were no failures, retries, conflicts, recovery reads, or filter fallback.
The boundary caused the expected one ingest failure, one reduction failure, and
two recovery reads while cancellation drained; exit 124 and `context canceled`
are the planned comparison boundary, not data findings.

Artifacts are `import-45m-v2.console.log` (SHA-256
`8b199c89c137c47c0304b4f41443d38764575218c3c394e454b01f5aee8427a0`) and
`import-45m-v2.time` (SHA-256
`8c96845590a23a44430318855a2004633f76163296f4b507e6f19347ee17bb02`) under
`/volume2/NASDA2/rustic/db.test/rados/phase32-p4/log`. The wrapper elapsed
45m02.91s and exited 124. P4 therefore passes its boundedness, correctness, and
matched-performance gates as a modest optimization. It does not justify more
lanes or select P5/P6.

Native RADOS access was verified against the supplied three-monitor cluster,
sealed FSID, and isolated `vaultic-perf` namespace. The Go backend's live
create/CAS/range/read/reopen/delete test passed against both `db-sst` (0.44s)
and `db-wal` (0.47s), including cleanup. VaulticDB was then built in release
mode with its `rados` feature against an isolated Ceph 20.2.4 runtime; binary
SHA-256 was
`357b68761590edf492302aadb8ca31356eae7f3619c22d85af8cd0e54e2a211a`.
Its native smoke test passed. A 100-commit `db-wal` benchmark produced exactly
two WAL PUTs and one durability wait per commit:

| Commit p95 | Commit p99 | WAL PUT/op | Durable wait/op | Queue/op | Service/op |
|---:|---:|---:|---:|---:|---:|
| 4.858 ms | 18.83 ms | 2.271 ms | 3.833 ms | 3.72 us | 8.25 us |

The live benchmark is gated as `BenchmarkProcessDurableCommitRADOSWAL`, requires
an explicit RADOS-enabled binary, endpoint/pool/namespace/prefix/client
variables, and a protected `VAULTICDB_BENCH_RADOS_KEY_FILE`; ordinary test runs
skip it.

RGW compatibility was verified separately before the main-store comparison.
VaulticDB's live S3 durability/reopen test passed against both supplied RGW
endpoints, `http://172.21.33.25:7480` in 4.09s and
`http://172.21.33.24:7480` in 3.77s, using isolated prefixes in
`vaultic-phase32-perf`. Each test covered durable write, close/reopen/replay,
repository isolation, and cleanup. Credentials were loaded from a protected
local file and are not retained in commands, logs, or this record. These
preflights establish compatibility only; the 45-minute RGW main-store sample is
reported separately below.

The matched RGW main-store run used endpoint `172.21.33.25:7480`, bucket
`vaultic-phase32-perf`, and isolated prefix
`phase32/2b30207fc/import-45m`; its planned interrupt boundary is included in
the consolidated comparison above.

The timeout wrapper ended at 45m00.82s with exit 124. Before cancellation there
were no ingest failures, retries, conflicts, or filter fallback. Cancellation
interrupted one reduction and triggered one recovery read; the final queue was
drained to zero. Scheduler totals were 2,325 eligible-ready observations over
15m28.918s and 1,481 reduction-blocking observations over 28m42.631s. Commit
p50/p95/p99 were all bounded by 268.435 ms; mutation RPC p95/p99 were bounded by
536.871 ms.

The otherwise idle Ceph cluster's total-operation dashboard showed sustained
roughly 250-330 read operations/s with low bandwidth, plus infrequent read and
write bursts up to approximately 84/77 MB/s. Those are cluster-internal Ceph
operations, not one-for-one RGW requests: three-copy replication and RGW
bucket-index/metadata work amplify writes. Simultaneous high-bandwidth read and
write bursts are consistent with SST compaction. VaulticDB leaves SlateDB's
10-second manifest polling default unchanged, which cannot explain hundreds of
operations per second by itself. The evidence therefore points to remote
transaction/LSM read amplification and compaction, not a tight polling loop;
that attribution remains observational and was not independently isolated.

The principal benchmark commands were:

```console
GOMAXPROCS=4 VAULTICDB_FAILURE_TEST_BINARY="$PWD/vaulticdb/target/release/vaulticdb" \
  go test -v ./internal/index/daemon -run '^$' \
  -bench '^BenchmarkProcessDurableCommitWALLatency$' -benchtime=100x -count=1

GOMAXPROCS=4 VAULTICDB_FAILURE_TEST_BINARY="$PWD/vaulticdb/target/release/vaulticdb" \
  go test -v ./internal/index/daemon -run '^$' \
  -bench '^BenchmarkProcessDurableCommitResponseLatency$' -benchtime=100x -count=1

GOMAXPROCS=4 GOMEMLIMIT=8GiB \
  VAULTICDB_TEST_BINARY="$PWD/vaulticdb/target/release/vaulticdb" \
  VAULTICDB_TEST_EXPECTED_SHA256=6e83382b752e372c6434496973fc48d1a635f77fa759118fdfe8113d6b4648a5 \
  go test -v ./internal/index/legacyimport -run '^$' \
  -bench '^BenchmarkImportStage3Daemon$' -benchtime=3x -count=1
```

The source pilot adds `VAULTICDB_BENCH_SOURCE_LOAD_DELAY=10ms`; object pilots
add `VAULTICDB_BENCH_OBJECT_DELAY_PROFILE` with the documented version-one JSON
and respectively `role=main` or `role=coordination`, `operation=put`, and
`delay_ms=10`.

## P7 Local Validation

The current-revision frozen comparison completed all five variants:

| Variant | Time | End-to-end throughput |
|---|---:|---:|
| Stage 2 equivalent: lanes=1, deferred=false | 1,179.861 ms/op | 55,546 blobs/s |
| lanes=2, deferred=false | 2,667.902 ms/op | 24,565 blobs/s |
| lanes=2, deferred=true | 688.510 ms/op | 95,185 blobs/s |
| lanes=4, deferred=true | 690.035 ms/op | 94,975 blobs/s |
| lanes=8, deferred=true | 688.808 ms/op | 95,144 blobs/s |

The accepted two-lane deferred candidate improves end-to-end time by 41.6%
against the Stage 2 equivalent on this fixture and exceeds the 20% local gate.
Four and eight lanes are flat and are not promoted. This does not replace the
roadmap's required repository-scale comparison.

Correctness and regression gates passed:

```console
go test -race ./internal/index/legacyimport ./internal/index/daemon -count=1
rustup run stable cargo test --manifest-path vaulticdb/Cargo.toml --lib
rustup run stable cargo test --manifest-path vaulticdb/Cargo.toml \
  --bin vaulticdb -- --test-threads=1
```

The Go race suites passed both packages. Rust passed 102 library and 166 binary
tests. A default-parallel binary invocation had 164 passes and two failures in
existing process-global `CloseCache` failpoint tests; the established serial
invocation passed all 166, including the new torn-close and object-role gates.
The focused SIGKILL, post-flush/pre-handoff, role-placement, delayed durable
response, and timeout recovery tests also pass.

## Scale and External Limits

A historical uncapped run at revision `8bc9cd7cb` successfully imported 10,019
indexes, 419,530 packs, and 379,934,385 blobs through close, handoff, and reopen
in 1:54:32. It proves that revision's completion path only; it does not certify
the current changes. A current SSD run reached 6,049 imported indexes, 327,065
packs, and 254,818,044 blobs in 2h52m39s before an operator cancellation; it is
not a completion result. The matched 45-minute checkpoints above are the
accepted comparison boundary.

The current uncapped HDD run is retained at
`phase32-p7-uncapped-current-20260921-hdd-r1`. It imported all 10,019 indexes,
419,530 packs, and 379,934,385 blobs in 2:37:13, using 82,479 successful ingest,
reduction, and commit batches with zero failures, retries, or conflicts. The
memory-WAL completion handoff, persistent-WAL reopen, and checkpoint validation
completed in 5.564 seconds. The wrapper ran for 2:37:34 at 359% CPU and reached
35,004,800 KiB maximum RSS. Its 1,889 five-second monitor records are retained
with the run artifacts.

That first command exited 1 only after data completion because its final daemon
shutdown inherited the ordinary 10-second RPC deadline; close failed after
10.009 seconds with `context deadline exceeded`, and the fallback terminated
the owned daemon. Shutdown now has a one-minute default while explicit caller
deadlines remain authoritative. A no-reset recovery scanned all 10,019 durable
checkpoints without republishing a pack or blob. The feature-gated activation
then exited 0 in 1:23.18, closed the daemon in 8.349 milliseconds, and left no
daemon process or socket. A separate fresh-process `index stats` read through
the activated SlateDB authority returned exactly 419,530 packs and 379,934,385
blobs. This recovered completion is accepted as P7b because the original data,
handoff, and reopen were durable, while recovery performed only checkpoint
validation, authority activation, and clean shutdown.

Representative NFS, RADOS, and RGW/S3 are no longer external blockers. Full
current-revision import, close/handoff/reopen, activation, and post-import
validation now satisfy P7b. P4 is accepted with the two-lane setting; do not increase lanes. The
current aligned telemetry selects P5 read amplification over P6 writer
concurrency. Cache fill-budget and task-count experiments remain rejected, while
the combined reducer prefetch is accepted as an 8.8% median bounded-throughput
improvement. Candidate SST fanout, needed-block and coalesced-range telemetry is
complete and rejects larger coalescing gaps.