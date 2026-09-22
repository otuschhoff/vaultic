# Phase 32: Next Steps for Remote SST Performance

[Phase 32 status](phase-32-scalable-legacy-metadata-bulk-import.md) |
[Experiment evidence](phase-32-p3-p7-evidence.md) |
[Telemetry coverage](phase-32-vaulticdb-critical-path-telemetry.md) |
[Read-cache contract](phase-29-slatedb-read-cache-tiers.md)

**Assessment: 2026-09-21.** This is a proposed experiment sequence, not an
accepted configuration or evidence of gains. The host budget is 32 CPU cores,
approximately 300 GB RAM, and high-bandwidth networking. There is no local
SSD/NVMe: available local storage is NFS-HDD or RADOS. The expected production
topology is an authoritative database in Azure S3-compatible object storage,
with abundant RAM as the primary low-latency cache and RADOS as the only
persistent local cache candidate. The database will predominantly serve queries
after import.

## Objectives and Decision Rules

Reduce total import runtime through marker, close/flush, handoff, reopen,
activation, and clean shutdown. Also protect the resulting database's read-mostly
performance: query throughput, p50/p95/p99 latency, and remote requests/bytes per
useful query. A faster import that leaves an expensive-to-query SST layout is
not automatically the preferred production configuration.

Report import, any post-import compaction or cache warming, and steady-state
queries separately. Include any mandatory preparation in time-to-ready; do not
hide work after the timed import. Import-only settings may differ from query
settings, but demonstrate the supported transition and its cost. SST layout and
filter choices persist beyond a process configuration change.

Use extra CPU, memory, and network when they produce repeatable elapsed-time or
query-latency gains. Do not maximize utilization for its own sake. For S3, report
the marginal API/transfer cost alongside every performance gain; do not choose
a cost-versus-latency tradeoff without an explicit deployment budget. RADOS has
no inherent S3-style per-request tariff, but OSD operations, replication,
CPU, bandwidth, and tail latency are still finite shared resources.

## What the Evidence Supports

- Cross-index grouping reached 89.31k blobs/s, +10.36% over deferred-cleanup r4,
  but remains provisional. Late throughput still declined about 22%, with
  MultiGet SST visits increasing from 1.96 to 6.13 per call.
- The 512 MiB L0 target reduced process I/O but regressed throughput 2.78% and
  worsened late reads. Keep 256 MiB as the control; process I/O is not S3 billing
  traffic, and this result does not establish the best query-time SST layout.
- Larger external-cache fill budgets/task counts, positive-record caching, and
  wider coalescing were rejected on the measured workloads. Do not repeat these
  without new workload-specific evidence. Historical coalescing projections
  showed request savings with substantial extra bytes, not actual S3 cost gains.
- Recent ten-minute runs used HDD NFS and disabled external cache tiers. They
  do not establish native RADOS or S3 performance, nor warm-query cache benefit.
- VaulticDB's current database builder does not override the pinned SlateDB
  engine-cache defaults: 512 MiB data blocks and 128 MiB index/filter metadata.
  These caches are distinct from the external tiers and the rejected mutable
  record cache. Available host RAM makes targeted resizing worth testing.

## Cache Architecture to Measure

| Layer | Purpose | Experiment and constraint |
|---|---|---|
| SlateDB metadata cache in RAM | Reuse SST indexes/filters and avoid repeated metadata fetch/decode | Test 128 MiB to 1 GiB independently; measure occupancy, hits, misses, and evictions by entry class |
| SlateDB data-block cache in RAM | Serve hot decoded blocks without repeated origin/cache I/O | Test 512 MiB to 8 GiB independently; consider 16/32 GiB because RAM is plentiful, but only when reuse and RSS data support it |
| Persistent RADOS cache | Retain immutable SST ranges across daemon restarts and absorb a working set larger than practical RAM | Reuse Phase 29 with RADOS as the cache store, initially ciphertext; size from measured reuse and shared-cluster capacity |
| Authoritative Azure S3-compatible storage | Durable database storage and cache-miss service | Measure provider-visible requests, bytes, retries, latency, and cost separately from RAM and RADOS hits |
| NFS-HDD | Current benchmark fixture or fallback storage, not a low-latency production cache | Use for matched historical comparisons; do not infer Azure S3 plus RADOS-cache behavior from it |

The current import baseline is 1 GiB of SlateDB metadata cache and 512 MiB of
data-block cache with four publication lanes and eager scheduler completion
draining. Two matched scheduler runs averaged 120.82k blobs/s, 5.80% above the
prior four-lane controls, with effectively unchanged sampled VaulticDB RSS.
Completion pickup delay fell 34.72% and scheduler work fell 6.93%. Follow-up
attribution showed the apparent 255 ms reducer dispatch delay was 99.6%
predecessor-order queue age; eligible work reached the reducer in about 1.1 ms.
Blocked-dispatch wall time was 58.5% next-ordinal ingest wait and 41.5% active
reducer service. It did not reveal an ID-allocation bottleneck, write
backpressure, or L0 stalls. Eight lanes remains rejected because it regressed
throughput 10.06% before this scheduler change and raised engine queue time
126%. Keep four lanes; do not spend more work on the coordinator dispatch loop.

Raising only the block cache to 8 GiB reduced throughput 1.14%, left database
GET-body bytes effectively flat, and doubled sampled VaulticDB RSS. Do not test
larger block caches until hit/eviction data or a query workload demonstrates
reusable data blocks. Abundant RAM remains valuable, but capacity is assigned
by observed reuse rather than availability.

The existing external cache admits tagged foreground compacted-SST reads;
WAL, manifests, fencing, coordination, conditional reads, and retry reads bypass
it. Do not assume it covers every L0 access or compaction read. Classify actual
misses and bypasses before claiming caching addresses import read amplification.

Its quota/policy machinery uses uncached coordination CAS/read/list operations.
Include this traffic, admission side effects, and restart reconciliation in the
latency and S3 cost model. A local hit is not proof the overall cache system
generates zero remote requests. Preserve current lease, quota, identity,
integrity, and fencing guarantees; optimize bookkeeping only after attribution.

RAM is the preferred resource to spend before adding another network storage
hop. Test larger engine caches aggressively but incrementally; abundant RAM does
not remove the need to measure effective capacity, allocator overhead, and RSS.
Do not allocate the nominal 160 GiB Go limit plus arbitrarily large Rust caches
against the same 300 GB host. Budget observed Go/Rust heaps, engine caches,
memtables, preparation, in-flight reads, cache metadata, OS page cache, and other
services together, using effective cgroup limits as well as host RAM. Avoid
swap and leave explicit safety headroom. Account for duplicate cached bytes
across decoded blocks, RADOS-cached ciphertext, and OS page cache.

## Minimum Additional Telemetry

Reuse existing counters and exporters; add only missing decision-critical fields.
Keep labels bounded and report counter resets and availability explicitly.

- Internal cache occupancy/capacity, hit/miss/eviction rates, and miss latency,
  separating data blocks from index/filter metadata.
- Remote request counts/bytes/latencies by backend, operation, and role: SST,
  WAL, coordination, cache fill, and cache bookkeeping where attribution exists.
  Distinguish foreground query/import traffic from compaction and background
  warming. Mark unavailable distinctions rather than guessing them.
- Useful returned bytes versus fetched range bytes, cache-fill bytes later
  reused, admission/bypass reasons, and remote operations avoided. Existing
  SST-visit and needed-byte counters do not alone count billable requests.
- Batch termination reason and actual packs, mutations, and bytes are now exposed
  as `legacy_import_batch_flushes{reason}`, `legacy_import_batch_packs`,
  `legacy_import_batch_mutations`, and `legacy_import_batch_bytes`. Use their
  deltas to decide whether pack count, the byte cap, or the mutation cap controls
  batching. Adaptive splits and commits per million imported blobs remain to be
  added. Split commit authority checks, optional idempotency lookup, transaction
  extraction, engine submission, and response overhead.
- `completion_to_receive_active_<lanes>`,
  `ingest_completion_to_reduce_dispatch`, and
  `dependency_wait_{inflight,mixed,ordered}` now separate scheduler pickup,
  ordered reducer dispatch, and direct versus order-propagated dependency
  blocking. `reduce_dispatch_wait_{order,ready}` and
  `reduce_dispatch_blocked_{ingest,service}` further separate amplified queue
  age from wall-time causes. `ingest_service_{reducer_unblock,other}` separates
  ingests that directly make an idle reducer eligible from other lane service.
  `reduce_dispatch_blocked_ingest_{active,admission}` then separates active
  transaction service from dependency/admission delay. The matched diagnostics
  show completion pickup and ready dispatch are no longer targets;
  next-ordinal ingest variance and ordered reducer service are.
- Query throughput and tail latency alongside process CPU/RSS, actual interface
  traffic, local disk latency, engine queues, backpressure, and compaction.

## Ranked Experiment Sequence

### 1. Establish Remote and Query Controls

Use the accepted four-lane eager-draining profile for matched ten-minute
repetitions. Repeat it first on authoritative Azure S3-compatible storage
without a persistent cache, then with RADOS caching, while freezing source
order, dataset, encryption, resource limits, and WAL/coordination placement.
Native RADOS remains a useful backend control, but it is not the expected
authoritative production topology. RGW compatibility or latency is not evidence
of Azure's pricing or network behavior; retain separate identities for each
deployment.

Use an existing completed, isolated database for read tests where possible.
Include random point hits/misses, batched lookups, and realistic range/history
queries, with both uniform and hot-set distributions. Fix the query trace and
test low and concurrent demand. Cover external-cache disabled, cold, warm, and
daemon-restart states; label each cache layer's state separately. Do not clear
host-wide caches on a shared machine. Never use a ten-minute partial import as
proof of full-sized read performance.

### 2. Spend RAM on the Engine Working Set

Run the metadata-cache and then data-block-cache capacity experiments above,
changing one capacity at a time and leaving external tiers unchanged. Since RAM
is plentiful and no fast local disk exists, complete this sweep before tuning a
persistent cache. Reuse SlateDB's supported cache constructors rather than
implementing a new cache. Accept only if reduced misses translate into import
throughput or query latency benefits with bounded RSS. Flat hit rates, physical
read bytes, or latency falsify the hypothesis; more occupancy alone is not
success.

### 3. Make RADOS Persistent Caching Pay for Itself

Compare the selected RAM profile against authoritative Azure S3-compatible
storage with and without a RADOS cache tier. There is no SSD/NVMe candidate.
Measure cold fill, warm service, and restart/reconciliation independently.
Vary admission/range granularity only after identifying which fetched ranges
are reused: avoid filling large regions for isolated random reads and avoid
evicting the hot query working set with scans or one-time import/compaction work.
Use existing foreground-only policy as the control, not speculative warming.

Account for fill requests/bytes and coordination work against origin operations
saved over repeated query windows. Report a break-even query volume for warming
or persistent caching; useful warm hits do not guarantee a net cost saving.
Test missing/corrupt/slow RADOS-cache fallback and unchanged results. RADOS must
remain disposable in this role: it never becomes authoritative or substitutes
for Azure S3 close/flush durability.

### 4. Amortize Import Transactions and Preparation

Grouped ordered reducer transactions are accepted. P21 grouped only contiguous
successful receipts already available at dispatch, preserving receipt
idempotency, aggregate/history order, adaptive children, final checkpoints,
failure precedence, and acknowledgement-gated dependency and byte release. Two
matched runs averaged 126.04k blobs/s, 4.32% above p14, with a 1.09% spread.
They averaged 2.37 receipts per reducer transaction, reducing reducer
transaction count by 57.8%, reducer service to about 117 seconds, and
reducer-blocked wall time to about 46 seconds. Keep this behavior with four
publication lanes. Do not add parallel reducer workers: reductions rewrite
shared aggregate keys and the global history sequence, so concurrency would
add conflicts and visibility-order risk.

Treat the remaining 168 seconds of blocked-dispatch wall time as next-ordinal
ingest variance. P17 found reducer-unblocking ingests averaged 99.3 ms versus
85.7 ms for other ingests, but that post-completion classification selects for
slow ordinals. P18 then attributed 93.9% of missing-ordinal wall time to an
already-active ingest and only 6.1% to admission. Do not add ordinal admission
priority. Optimize active transaction service first, beginning with reuse of
the unique pack/blob ID sets across hints, planning, filter publication, and
counters. That candidate reduced its combined local work by about 7% but its
two-run throughput mean regressed 1.90%, so it is rejected. The subsequent
16-pack experiment also failed: despite about 29% fewer ingest transactions,
its tightly repeated 118.12k blobs/s mean was 2.24% below the accepted p14
mean. Larger transactions shifted the batch mix from 57.3% pack-limited and
40.2% mutation-limited to 33.1% and 63.4%, respectively, but increased active
ingest head-of-line wall time. Keep eight packs, 8,000 mutations, and 8 MiB;
do not raise the mutation limit without new evidence that longer active ingests
will not worsen ordered blocking.

P21 retained the accepted ingest limits and reported no pre-cancellation
retries, conflicts, backpressure, L0 stalls, or adaptive splits. It also left
active ingest as the dominant ordered blocker: about 207 seconds of the 224
seconds waiting for a missing next ordinal. Continue to treat this as active
transaction variance, not an admission or reducer-parallelism problem. P21
reduced normalized process writes but increased reads per blob about 15%; carry
that tradeoff into equal-state and full-import validation.

Next reuse immutable ID/canonical preparation across hashing, hints, planning,
filter publication, and counters. Parallelize pure preparation only after a
profile demonstrates a serial cost; retain ordered dependency admission and
state-dependent retry reads. Spare cores can hide independent work, not justify
parallel commit visibility or stale authority checks.

### 5. Tune Remote Reads and Persistent Layout Only from Evidence

If cold queries still fetch many irrelevant SSTs/blocks, evaluate filter quality,
lookup fanout, and compaction/layout before raising concurrency. Distinguish
true hits from filter false positives. Stronger SST filters trade build CPU,
RAM, and stored bytes for fewer remote reads; test on absent and present keys.
Changing a persisted filter/layout requires rebuilding comparable candidates.

Do not rerun wider coalescing merely because S3 bills requests. First calculate
whether avoided requests outweigh extra bytes and latency for the measured
query distribution. A query-specific result may justify a separate candidate;
the rejected import result remains valid for its original workload.

Only test bounded miss-read or flush/compaction concurrency when aligned queue,
latency, backend-load, and CPU data show benefit is plausible. More compaction
can improve long-lived reads but also adds RADOS load or S3 GET/PUT/transfer
costs. Keep foreground tail latency and time-to-ready visible. Do not change
ingestion lanes, cache size, layout, and concurrency in one run.

## S3 Cost Accounting

Use the actual provider, region, storage class, and billing units; do not insert
assumed universal S3 prices. For a stated workload horizon, estimate:

```text
remote cost = sum(billable operation count * price per operation)
            + billable retrieval bytes * retrieval price
            + billable transfer bytes * transfer price
            + stored byte-time * storage price
```

Include GET/range GET, HEAD, PUT/multipart, LIST, retries, and coordination/cache
operations as applicable to the provider's billing rules. Apply free allowances,
minimum storage durations, and other class-specific charges where relevant;
use provider counters/billing to validate application-side estimates. Do not
count logical MultiGet keys or Ceph internal replication operations as S3 GETs.

Report cost per completed import and per million useful queries, plus bytes,
requests, and latency. Compare an agreed lifecycle horizon: one import plus
expected query volume and maintenance. A one-time extra compaction or warmup
may be worthwhile over many reads, but its break-even point must be explicit.
For RADOS, report the analogous resource demand rather than inventing API fees.

## Execution and Acceptance

Follow the [Phase 32 duration policy](phase-32-scalable-legacy-metadata-bulk-import.md#validation-and-profiling):
all exploratory and confirmation runs are at most ten minutes, with no overlap;
import screening uses 595 seconds plus a five-second hard-kill allowance.
Use matched repetitions and fixed query counts/traces or clearly reported timed
windows. Do not combine warm and cold results or infer late-life behavior from
a fresh tiny database. Use existing scale fixtures without silently authorizing
a new long-running import or preconditioning job.

Every result records exact binaries, backend placement, cache state, input/query
identity, useful throughput, latency tails, resource peaks, and estimated remote
cost. Promote neither defaults nor cache/layout policies on one favorable run.
Explicitly authorized uncapped acceptance must verify complete metadata,
recovery, final durability, query correctness, and the supported import-to-query
transition. Preserve the protected source repository and all unrelated work.