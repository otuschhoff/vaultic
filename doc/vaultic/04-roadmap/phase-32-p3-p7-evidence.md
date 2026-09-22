# Phase 32 P3-P7 Experiment and Acceptance Evidence

[Phase 32 status and document map](phase-32-scalable-legacy-metadata-bulk-import.md) |
[Telemetry coverage](phase-32-vaulticdb-critical-path-telemetry.md)

This is the run-level evidence ledger as of 2026-09-21. It retains historical
settings and results rather than applying today's binary or duration policy to
older runs. The roadmap owns current defaults and remaining execution gates;
the telemetry document owns metric coverage and interpretation.

## Decision Summary

| Work | Decision and scope | Evidence |
|---|---|---|
| P3a/P3b attribution and latency pilots | Validated; P3c/P3d's full response/scenario matrix remains partial | [P3 results](#p3-results) |
| P4 ordered reducer worker | Accepted; keep two ingestion lanes | [P4 results](#p4-results) |
| P5 cache fill budget / fill tasks | Rejected; retain 256 MiB fill budget and derived 16-task limit | [Telemetry experiments](#current-telemetry-baseline-and-p5-selection) |
| P5 combined reducer prefetch | Accepted bounded result: +8.8% median throughput over its matched baseline | [Telemetry experiments](#current-telemetry-baseline-and-p5-selection) |
| Larger MultiGet coalescing gaps | Rejected; retain gap 2 | [Telemetry experiments](#current-telemetry-baseline-and-p5-selection) |
| Deferred cleanup / cross-index grouping | Provisional ten-minute performance results; confirmation and candidate-specific full lifecycle remain open | [Ten-minute experiments](#ten-minute-full-import-experiments) |
| Positive-record cache / 512 MiB L0 target | Rejected; record cache removed and 256 MiB L0 retained | [Ten-minute experiments](#ten-minute-full-import-experiments) |
| P7a / P7b | Local validation and repository-scale recovered completion accepted for the recorded builds; not blanket acceptance of later candidates or every backend scenario | [Local validation](#p7-local-validation), [scale and recovery](#scale-and-external-limits) |

A timeout boundary is not completion evidence. Concurrent worker, RPC, and
background totals are attribution measures, not additive wall-clock percentages.
Compare each candidate with its named baseline; percentages across different
profiles or input prefixes are not interchangeable.

## Current Telemetry Baseline and P5 Selection

This section records the earlier 30-minute campaign, before the ten-minute cap.
Its historical baseline is retained at
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
task setting remains available for controlled diagnostics. This ruled out
merely increasing fill concurrency; the subsequent experiment targeted reducer
read amortization instead.

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

These are capped samples of the full-import workload, not completed full
imports. The current [test policy](phase-32-scalable-legacy-metadata-bulk-import.md#validation-and-profiling)
also caps confirmation runs at ten minutes. The profile used `GOMAXPROCS=32`,
`GOMEMLIMIT=160GiB`, 32 connections, 32 pack workers, two lanes, memory WAL,
local object storage on HDD NFS, disabled read-cache tiers, five-second JSONL,
and a 595-second SIGINT boundary plus five-second hard-kill allowance. Deferred
cleanup was enabled except in the explicitly named no-defer controls.

Artifacts below are under `/volume2/NASDA2/rustic/db.test/nfs/hdd/`. Imports used
the disposable `phase32-p8-defer-cleanup-20260921-hdd-10m-r3/legacy-source` view,
not the protected `/volume2/NASDA2/rustic/repo`. No benchmark authorizes changes
to that protected repository. Rates below use sampled counter deltas; matched
window endpoints can differ slightly from whole-run or first-to-last rates.

### Deferred Cleanup Baseline

Run `phase32-p8-defer-cleanup-20260921-hdd-10m-r4` is the provisional baseline
for subsequent experiments. Against the fresh no-defer control
`phase32-p8-baseline-20260921-hdd-10m-r5`, the matched-window rate was 81.17k
versus 53.95k blobs/s (+50.4%), and cleanup fell from 227.1 to 11.3 seconds.
Both slowed about 20.5% from early to late windows, so cleanup did not explain
the shared decay. Sampled peak RSS rose 66.7%, read bytes/blob 59.1%, and write
bytes/blob 49.1% versus that control. Deferred cleanup remains opt-in; bounded
throughput does not certify its final completion/reopen durability.

### Experiment 1: Positive-Record Reuse

The session-local positive-record cache candidate is rejected. Run
`phase32-p9-record-cache-20260921-hdd-10m-r1` reused only 1,164 records and
reduced catalog keys by 3.32%, while throughput fell 1.91% versus the deferred
cleanup baseline. Early throughput was effectively flat (+0.63%), late
throughput fell 7.60%, and planning, blob-read, and reducer-prefetch time all
increased.

### Experiment 2: Cross-Index Grouping

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

The first `phase32-p10-cross-index-20260921-hdd-10m-r1` failed before useful
measurement with a checkpoint source-index mismatch; it is not performance
evidence. The fix makes final ingest receipts checkpoint-aware, includes
checkpoint metadata in their hash, and atomically reduces all per-index tallies.
Focused regression and package/race tests passed. The implementation was
committed in `8aa37b04e`; matched confirmation is still pending.

### Experiment 3: Larger L0 SSTs

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

Experiment 3 changed only `--daemon-l0-sst-size-bytes=536870912`. It reused
experiment 2's Vaultic binary; both runs used VaulticDB SHA-256
`3a8b8278046cc6656e8a68fcdf89bbf968437a8a4afeedaf67fc3d8e2bbeefe5`.
The run exited with the expected timeout status 124 after 9m56.71s, emitted
120 monitor records, and left no daemon process. The final ingest/reduce
failure counters appeared only at cancellation. Ingest wait remained the
largest scheduler phase at 408.5 seconds, followed by schedule work at 125.6
seconds, dependency wait at 34.9 seconds, and reducer wait at 11.5 seconds.

### Experiment 4: Larger SlateDB Metadata Cache

A 1 GiB metadata cache is accepted as the baseline for the next import
experiments. It is not yet a general query-time or remote-backend default. Runs
`phase32-p9-meta-cache-1g-20260921-hdd-10m-r1` and `r2` retained the
512 MiB block cache, two lanes, 256 MiB L0 target, memory WAL, and disabled
external cache. They produced 99,180 and 99,587 blobs/s, respectively, only
0.41% apart and averaging 11.28% above cross-index r2's 89,308 blobs/s. The
first run reached the control's terminal blob count in 525.0 seconds, 11.20%
sooner. Early-to-late decay narrowed from 21.30% in the control to 14.92% and
13.10%.

The physical-read evidence supports the cache hypothesis. Database GET
operations fell 21.9% per blob and GET-body bytes fell from 2,806.5 to 174.3
bytes per blob, a 93.8% reduction. Blob-read time fell from 8.54 to 3.50 ms per
batch and reducer-prefetch time from 6.69 to 1.37 ms per reduced batch. Logical
fanout was not fixed: late multi-get SST visits rose from 6.11 to 7.35 per call,
partly because the candidate reached a larger database state in the same wall
time. Plan-build and commit time per batch changed little.

The memory cost is material. Sampled VaulticDB peak RSS rose from 11.75 GiB to
17.69 and 17.61 GiB, much more than the 896 MiB configured-capacity increase.
Both runs exited with expected timeout status 124 after about 9m57s, emitted
119 usable monitor records, left no process, and reported exact effective
capacities of 512 MiB block and 1 GiB metadata. Socket/control files remained
as the same harmless shutdown residue seen in prior runs. Vaultic SHA-256 was
`082d091a4dbf7866f931dabc29c6afab53fb1b137bb6ba2d7dac39f8c7284527`;
VaulticDB SHA-256 was
`12e8b472570449133e7ca454ca50c49df6ef3a78f3caef82a7d0254b40aeac06`.
Test block-cache size independently from this baseline. Internal cache
hit/miss/eviction telemetry is still needed to explain the RSS multiplier and
choose a smaller metadata capacity if possible.

### Experiment 5: Larger SlateDB Block Cache

An 8 GiB block cache is rejected for import. Run
`phase32-p10-block-cache-8g-20260921-hdd-10m-r1` kept the accepted 1 GiB
metadata cache and changed only the block cache from 512 MiB to 8 GiB. It
produced 98,250 blobs/s, 1.14% below the 99,384 blobs/s mean of the two matched
metadata-cache runs. Early-to-late throughput declined 17.12%, also worse than
the two controls' 14.92% and 13.10%.

The extra capacity did not reduce physical data reads: database GET-body bytes
changed by only -0.10%, while database GET operations fell 2.72%. Sampled peak
VaulticDB RSS rose from a 17.65 GiB control mean to 36.12 GiB, a 104.7%
increase. The run exited with expected timeout status 124 after 9m59.39s,
emitted 119 usable monitor records, left no process, and reported exact 8 GiB
block and 1 GiB metadata capacities. Keep the 512 MiB block cache for import;
do not test 16 or 32 GiB without cache hit/eviction evidence from a workload
that can plausibly reuse those blocks.

### Experiment 6: Four Publication Lanes

Four publication lanes are accepted for the next import baseline. Runs
`phase32-p12-publication-lanes-4-20260921-hdd-10m-r1` and `r2` kept the
accepted 1 GiB metadata cache, 512 MiB block cache, 256 MiB L0 target, memory
WAL, and deferred cleanup, changing only publication lanes from two to four.
They produced 113,794 and 114,609 blobs/s, only 0.71% apart, and averaged
114,201 blobs/s: 14.91% above the 99,384 blobs/s mean of the two metadata-cache
controls. Mean early throughput improved 10.80%, mean late throughput improved
16.85%, and early-to-late decay narrowed from 14.01% to 9.32%. Sampled peak
VaulticDB RSS averaged 18.10 GiB, only 2.54% above the 17.65 GiB control mean.

The additional concurrency moved rather than removed the bottleneck. Engine
batch service changed little, from 7.53 to 7.86 ms per commit, while engine
queue time rose from 2.36 to 5.94 and 6.01 ms and commit-request time from
14.68 to 20.76 and 20.81 ms. Scheduler ingest wait fell from about 405 seconds
to 201 and 205 seconds, but schedule work rose from about 142 seconds to 266
and 265 seconds, dependency wait from 29 seconds to 96 and 93 seconds, and
reducer wait from 5 seconds to 16 seconds in both runs. Completed ingest work
waited 177 and 179 seconds in aggregate for scheduler receipt, and the single
ordered reducer consumed 185 and 188 seconds. There were no pre-cancellation
conflicts, retries, backpressure events, or L0 stalls. The next ceiling is
scheduler/reducer serialization plus engine commit queueing, not transaction
identity allocation.

Physical I/O remained secondary but became somewhat less efficient at the
larger reached state. Process read bytes per blob rose 2.30%, process write
bytes per blob rose 4.51%, and database GET-body bytes per blob rose 17.27%.
Both runs exited with expected timeout status 124 after 9m56.52s, emitted 119
usable monitor records, reached all four lanes, and left no process. Their
final ingest/reduction failures occurred only during timeout cancellation.
Socket residue matched prior clean shutdowns.

### Experiment 7: Eight Publication Lanes

Eight publication lanes are rejected. Run
`phase32-p13-publication-lanes-8-20260922-hdd-10m-r1` retained the accepted
cache, L0, WAL, batching, worker, and cleanup settings and changed only
publication lanes from four to eight. It produced 102,709 blobs/s, 10.06% below
the 114,201 blobs/s mean of the two confirmed four-lane runs. Early throughput
fell 13.89% and late throughput fell 8.44%. Sampled peak VaulticDB RSS was
18.15 GiB, only 0.28% above the four-lane mean, so memory capacity does not
explain the regression.

The engine service rate had already reached its useful concurrency point.
Batch service remained effectively flat at 7.87 ms per commit versus 7.85 ms
for four lanes, but queue time rose 126% from a 5.97 ms mean to 13.50 ms and
commit-request time rose 41.8% from 20.78 to 29.47 ms. Scheduler work consumed
368.4 seconds, 62.0% of measured wall time, while dependency wait consumed
121.9 seconds, 20.5%. Completed ingest work waited 415.4 seconds in aggregate
for scheduler receipt. All eight lanes were reached, but useful concurrency was
not sustained: the scheduler reported 297.0 seconds with no active lane and
only 58.2 seconds with all eight active.

There were no pre-cancellation retries, conflicts, backpressure events, or L0
stalls. The final six ingest failures and one reduction failure appeared only
after the timeout signal. The run exited with expected status 124 after
9m57.05s, emitted 119 usable monitor records, and left no process. Keep four
publication lanes and target scheduler completion handling, ordered reduction,
dependency-release latency, and commit queueing before reconsidering wider
publication.

### Experiment 8: Eager Scheduler Completion Draining

Eager scheduler completion draining is accepted for the next import baseline.
Matched normal-build runs `phase32-p14-scheduler-drain-20260922-hdd-10m-r2`
and `r3` retained four lanes and every accepted cache, L0, WAL, batching,
worker, and cleanup setting. They produced 120,971 and 120,677 blobs/s, only
0.24% apart, and averaged 120,824 blobs/s: 5.80% above the 114,201 blobs/s
four-lane control mean. Mean early throughput improved 8.57%. Mean late
throughput was 6.40% below the controls because the candidates reached a larger
database state; early-to-late decay was 21.82% versus 9.32%. At equal work, r2
reached the controls' terminal blob counts 36 to 51 seconds sooner. Sampled
peak VaulticDB RSS was effectively unchanged at -0.28%.

The scheduler change affected the intended path. Aggregate completion pickup
delay fell 34.72%, from 178.0 to 116.2 seconds, while scheduler work fell 6.93%,
from 265.5 to 247.1 seconds. Average completion pickup was 7.08 ms. Dependency
wait was nearly flat at 96.0 seconds; the new attribution assigned means of
47.5 seconds to batches directly blocked by in-flight dependencies and 43.7
seconds to mixed direct and order-propagated blocking. No pure ordered-only
wait was observed. Ingest wait rose 7.65% because the scheduler spent less time
processing completions and reached more transaction work in the same interval.

The next measured coordinator bottleneck initially appeared to be ordered
reduction dispatch. Ingested batches waited an average 255.0 ms from completion
to reducer dispatch, while reducer service averaged about 12.2 ms per batch.
Engine service remained flat at 7.82 ms per commit versus 7.85 ms for the
controls. Queue time rose only 3.70%, from 5.97 to 6.19 ms, and commit-request
time rose 1.67%, from 20.78 to 21.13 ms. Neither run reported pre-cancellation
retries, conflicts, backpressure, or L0 stalls.

Batch-boundary telemetry was stable across both runs. Batches averaged 5.44
packs; 57.4% ended at the eight-pack limit, 40.0% at the 8,000-mutation limit,
2.6% at an index-tail/end-of-input boundary, and none at the 8 MiB byte limit.
A packs-per-transaction increase can affect only the pack-limited portion unless
the mutation limit also changes, so prioritize ordered reduction before testing
16 packs.

The candidates reached a larger database state and paid more physical I/O per
blob: process write bytes rose 21.71% and database GET-body bytes rose 21.95%,
while process read bytes were approximately flat. This is a remaining full-run
and equal-state concern, not evidence that the scheduler optimization caused
additional logical writes. Both matched runs exited with expected status 124
after about 9m57s, emitted 119 usable monitor records, and left no process. A
preceding `r1` used profile-tagged binaries and is diagnostic only; it is
excluded from acceptance calculations.

### Experiment 9: Ordered Dispatch Attribution

Runs `phase32-p15-reducer-attribution-20260922-hdd-10m-r1` and
`phase32-p16-reducer-blocker-20260922-hdd-10m-r1` added behavior-neutral timing
only. They produced 120,240 and 121,138 blobs/s; the latter was 0.26% above the
accepted p14 mean. Both exited at the expected boundary with valid telemetry,
clean process shutdown, and only cancellation-time failure. P16 reported no
retry, conflict, backpressure, or L0 stall. Its engine service was 7.91 ms per
commit and its batch mix remained 57.4% pack-limited, 40.0% mutation-limited,
and 0% byte-limited. The diagnostics therefore represent the accepted workload.

The earlier 255 ms interpretation was queue-age amplification, not scheduler
execution time. P15 attributed 99.53% of aggregate completion-to-dispatch age
to predecessor ordering; once an ordinal was eligible, dispatch took only 1.22
ms on average. P16 reproduced this with 99.58% order wait and 1.07 ms average
ready-to-dispatch delay. Do not optimize the coordinator dispatch loop further.

P16 split 287.2 seconds of blocked-dispatch wall time into 168.1 seconds
(58.5%) waiting for the next ordinal's ingest to complete and 119.1 seconds
(41.5%) waiting behind active reducer service. Reduction cannot simply run out
of order or in parallel: each receipt transaction reads and rewrites shared
aggregate records and the global history sequence, and existing failure
semantics stop later reduction after an earlier failure. Extra reducer workers
would introduce transaction conflicts and visibility-order risk.

Run `phase32-p17-ingest-unblock-20260922-hdd-10m-r1` then split ingest service
by whether completion directly made an idle reducer eligible. Those 3,340
reducer-unblocking ingests averaged 99.26 ms versus 85.73 ms for 12,780 other
ingests, 15.8% slower; their p50 moved from the 67 ms bucket to 134 ms while
p95 and p99 remained in the same buckets. The blocker split reproduced p16 at
164.4 seconds (56.8%) ingest and 125.1 seconds (43.2%) reducer service. P17 was
healthy but produced 118,417 blobs/s, 1.99% below the p14 mean, so use it as
diagnostic rather than performance evidence. Because direct-unblock selection
naturally favors slow ordinals, this result does not by itself justify admission
changes.

The next diagnostic partitions missing-next-ordinal wall time between an
ordinal already active in ingest and one not yet admitted. If active ingest
dominates, optimize transaction service variance or amortization. If admission
dominates, test an ordinal-aware admission preference while retaining dependency
exclusion and the invariant that an earlier dependency waiter prevents later
overlapping work from bypassing it.

Run `phase32-p18-ingest-state-20260922-hdd-10m-r1` resolved that choice. Of
161.1 seconds waiting for a missing next ordinal, 151.2 seconds (93.9%) waited
for an already-active ingest and only 9.85 seconds (6.1%) waited for admission.
The overall split remained stable at 56.5% ingest and 43.5% reducer service.
P18 produced 119,962 blobs/s, 0.71% below the p14 mean, with no retries,
conflicts, backpressure, or L0 stalls before expected cancellation. Do not add
ordinal admission priority: its maximum observed opportunity is small and it
cannot shorten the dominant active transaction.

The first active-service candidate reused one sorted unique pack/blob ID set
across hints, transaction planning, post-commit filter publication, counters,
and retries. Runs `phase32-p19-id-reuse-20260922-hdd-10m-r1` and `r2` reduced
combined hint plus post-commit work by about 7%, but produced 118,016 and
119,036 blobs/s. Their 118,526 blobs/s mean was 1.90% below p14, while engine
latency varied adversely. Reject and revert this local CPU optimization because
it did not reduce end-to-end runtime.

Runs `phase32-p20-packs16-20260922-hdd-10m-r1` and `r2` then tested 16 packs
with the 8,000-mutation and 8 MiB limits unchanged. They produced 118,270 and
117,968 blobs/s, a tightly repeated 118,119 blobs/s mean that was 2.24% below
p14 and 1.54% below p18. The intended coalescing occurred: average packs per
ingest batch increased 38.7%, ingest batches fell about 29%, pack-limited
flushes fell from 57.3% to 33.1%, and mutation-limited flushes rose from 40.2%
to 63.4%. Neither run reported adaptive splits, retries, conflicts,
backpressure, or L0 stalls before cancellation.

The lower transaction count did not shorten total runtime. Larger active
ingests increased missing-next-ordinal wall time from 161.1 seconds in p18 to
176.8 seconds on average, while blocker-service wall time fell from about
124 seconds to 97.7 seconds and aggregate reducer service fell to 167.0
seconds. Normalized process writes also rose to about 354.4 bytes/blob versus
352.0 in p18. Reject 16-pack ingest transactions: the saved fixed transaction
and reducer overhead did not offset longer ingest head-of-line occupancy. Keep
eight packs, 8,000 mutations, and 8 MiB as the import baseline.

Runs `phase32-p21-grouped-reduce-20260922-hdd-10m-r1` and `r2` atomically
reduced the contiguous successful receipts already available at dispatch while
retaining eight-pack ingest transactions. They produced 126,723 and 125,352
blobs/s, a 126,038 blobs/s mean that was 4.32% above p14 and 5.07% above p18;
the repeat spread was 1.09%. Accept grouped ordered reduction as the new import
baseline.

The intended transaction amortization was repeatable. The runs reduced 17,195
and 17,009 receipts through 7,284 and 7,168 transactions, respectively: about
2.37 receipts per transaction and 57.8% fewer reducer transactions. Aggregate
reducer service fell to about 116.7 seconds and reducer-blocked wall time to
45.6 seconds, versus about 196 and 119 seconds before grouping. Both runs
preserved ordered acknowledgement and dependency release and reported no
pre-cancellation retries, conflicts, backpressure, L0 stalls, or adaptive
splits. Sampled peak RSS remained within the prior range.

The remaining dominant ordered blocker is active ingest. Missing-next-ordinal
wall time averaged 223.8 seconds, of which 207.1 seconds (92.5%) was active
ingest and 15.8 seconds (7.1%) admission. Grouping shifted completed receipts
out of reducer service but did not remove ingest head-of-line variance.
Normalized process writes improved to about 338.6 bytes/blob from 349.6 in p14,
while reads rose to about 289.2 bytes/blob from 251.3; retain the read increase
as an equal-state/full-run concern. Do not address the remaining wait by
reordering visible history, raising ingest transaction size, or returning to
eight publication lanes.

## Frozen Inputs and Build

This section describes the original P3/P4/P7 fixture and storage comparisons
below, not the later telemetry or ten-minute campaigns above. Those campaigns
carry their own build identities and profiles.

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

The recorded P7 frozen comparison completed all five variants:

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
later changes. A separate SSD run reached 6,049 imported indexes, 327,065
packs, and 254,818,044 blobs in 2h52m39s before an operator cancellation; it is
not a completion result. The matched 45-minute checkpoints above are the
accepted comparison boundary.

The 2026-09-21 uncapped HDD run, before the later ten-minute experiments, is retained at
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

Representative NFS, RADOS, and RGW/S3 resources are no longer external blockers.
The recorded import, durable close/handoff/reopen, recovered activation, and
fresh-process validation satisfy the repository-scale P7b lifecycle gate.
This does not complete the remaining P3c/P3d scenario matrix or certify later
cross-index grouping. Current optimization decisions are summarized at the top
of this record; retain two lanes and require candidate-specific acceptance.