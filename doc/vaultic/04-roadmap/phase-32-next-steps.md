# Phase 32: Next Steps for Remote SST Performance

[Phase 32 status](phase-32-scalable-legacy-metadata-bulk-import.md) |
[Experiment evidence](phase-32-p3-p7-evidence.md) |
[Telemetry coverage](phase-32-vaulticdb-critical-path-telemetry.md) |
[Read-cache contract](phase-29-slatedb-read-cache-tiers.md)

**Assessment: 2026-09-21.** This is a proposed experiment sequence, not an
accepted configuration or evidence of gains. The host budget is 32 CPU cores,
approximately 300 GB RAM, and high-bandwidth networking. Authoritative SSTs
will reside on native Ceph RADOS and/or S3 and will predominantly serve queries
after import. Local SSD/NVMe capacity and performance must be measured before
assigning a persistent-cache budget.

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
| SlateDB data-block cache in RAM | Serve hot decoded blocks without repeated origin/cache I/O | Test 512 MiB to 8 GiB independently; consider 16/32 GiB only if reuse and eviction data support it |
| Persistent local SSD/NVMe tier | Retain immutable SST ranges across daemon restarts and absorb a larger read working set | Reuse Phase 29, initially ciphertext; size to a measured working set and actual free disk space |
| Authoritative RADOS/S3 | Durable SST storage and cache-miss service | Measure real remote requests, bytes, retries, and latency separately from local hits |

The existing external cache admits tagged foreground compacted-SST reads;
WAL, manifests, fencing, coordination, conditional reads, and retry reads bypass
it. Do not assume it covers every L0 access or compaction read. Classify actual
misses and bypasses before claiming caching addresses import read amplification.

Its quota/policy machinery uses uncached coordination CAS/read/list operations.
Include this traffic, admission side effects, and restart reconciliation in the
latency and S3 cost model. A local hit is not proof the overall cache system
generates zero remote requests. Preserve current lease, quota, identity,
integrity, and fencing guarantees; optimize bookkeeping only after attribution.

Do not allocate the nominal 160 GiB Go limit plus arbitrarily large Rust caches
against the same 300 GB host. Budget observed Go/Rust heaps, engine caches,
memtables, preparation, in-flight reads, cache metadata, OS page cache, and other
services together, using effective cgroup limits as well as host RAM. Avoid
swap and leave explicit safety headroom. Account for duplicate cached bytes
across decoded blocks, ciphertext disk cache, and OS page cache.

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
- Batch termination reason, actual mutations/bytes, adaptive splits, and commits
  per million imported blobs. Split commit authority checks, optional idempotency
  lookup, transaction extraction, engine submission, and response overhead.
- Query throughput and tail latency alongside process CPU/RSS, actual interface
  traffic, local disk latency, engine queues, backpressure, and compaction.

## Ranked Experiment Sequence

### 1. Establish Remote and Query Controls

Confirm the cross-index candidate with matched ten-minute repetitions. Keep two
lanes, 256 MiB L0 SSTs, and the existing safety/durability settings as controls.
Repeat the control on authorized native RADOS and S3 targets with source order,
dataset, encryption, resource limits, and WAL/coordination placement frozen.
RGW compatibility or latency is not evidence of a public S3 provider's pricing
or network behavior; retain separate identities for each deployment.

Use an existing completed, isolated database for read tests where possible.
Include random point hits/misses, batched lookups, and realistic range/history
queries, with both uniform and hot-set distributions. Fix the query trace and
test low and concurrent demand. Cover external-cache disabled, cold, warm, and
daemon-restart states; label each cache layer's state separately. Do not clear
host-wide caches on a shared machine. Never use a ten-minute partial import as
proof of full-sized read performance.

### 2. Spend RAM on the Engine Working Set

Run the metadata-cache and then data-block-cache capacity experiments above,
changing one capacity at a time and leaving external tiers unchanged. Reuse
SlateDB's supported cache constructors rather than implementing a new cache.
Accept only if reduced misses translate into import throughput or query latency
benefits with bounded memory. Flat hit rates or latency falsify the hypothesis;
more occupancy alone is not success.

### 3. Make Local Persistent Caching Pay for Itself

Compare the selected RAM profile with and without one local SSD/NVMe tier.
Measure cold fill, warm service, and restart/reconciliation independently.
Vary admission/range granularity only after identifying which fetched ranges
are reused: avoid filling large regions for isolated random reads and avoid
evicting the hot query working set with scans or one-time import/compaction work.
Use existing foreground-only policy as the control, not speculative warming.

Account for fill requests/bytes and coordination work against origin operations
saved over repeated query windows. Report a break-even query volume for warming
or persistent caching; useful warm hits do not guarantee a net cost saving.
Test missing/corrupt/slow cache fallback and unchanged results. A local cache
never becomes authoritative or substitutes for close/flush durability.

### 4. Amortize Import Transactions and Preparation

Test 8 to 16 packs per transaction while holding byte/mutation limits fixed.
If the 8,000-mutation cap already ends most batches, the pack-count change is
not a useful experiment; a separate bounded mutation-limit experiment would
need correctness and adaptive-split validation. Measure remote fencing and
receipt overhead per blob, not just transaction count.

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