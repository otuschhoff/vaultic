# Phase 32: Scalable legacy metadata bulk import

[← Back to roadmap index](00-overview.md)

[← Phase 31](phase-31-read-only-nfsv3-snapshot-server.md) · [Phase 33 →](phase-33-operational-monitoring-and-metrics-export.md)

[CLI and operations architecture](../02-architecture/04-cli-and-operations.md) · [Operational monitoring](phase-33-operational-monitoring-and-metrics-export.md)

**Status: Stages 1-3 implemented; Stage 3 split-session importer is enabled only for fresh imports (`--force-reset-old-idx`) with deterministic content-derived root receipt IDs, bounded cleanup, and resume-safe receipt replay. Live testing on 2026-09-16 exposed dependency inversion and oversized-pack rejection, fixed in `902b94389`. Full-run completion and controlled Stage 3 performance acceptance remain pending.**

**Goal:** make a fresh legacy JSON-index rebuild sustain useful throughput as the SlateDB candidate grows, without weakening duplicate-location preservation, aggregate accuracy, history ordering, transaction atomicity, resumability, or metadata durability. Pack preparation remains parallel, publication amortizes fixed transaction work across bounded multi-pack batches, and fresh-import lookup avoidance keeps a stable false-positive rate for repositories containing hundreds of millions of blob IDs.

## Motivation and measured baseline

The pre-Phase-32 importer started multiple pack workers, but `SchemaStore.ImportLegacyPack` admitted publication through a one-slot gate. Serialization was required because every pack transaction updated shared aggregate and history keys; unrestricted optimistic transactions repeatedly invalidated one another and could exhaust conflict retries. The worker count therefore overlapped pack `Stat` and Go-side record construction, but could not create concurrent successful publications.

Each admitted pack paid for a transaction begin, pack and duplicate-blob lookups, aggregate reads and rewrites, history marker and sequence reads, one or more mutation RPCs, and a commit. As the candidate gained SSTs, false-positive duplicate lookups and LSM read/compaction work increased the latency of that single publication lane. More workers could not compensate, so the producer waited while most host CPUs remained idle.

A controlled fresh import with memory WAL, a 16 GiB unflushed limit, 256 MiB L0 SSTs, 32 pack workers, snapshots disabled, and a 20-million-record work budget established this baseline:

| Measurement | Result |
|---|---:|
| Imported blobs | 19,999,995 |
| Imported packs | 24,942 |
| Completed indexes | 470 |
| Elapsed time | 9m28s |
| Initial pack rate | 61-70 packs/s |
| Final observed pack rate | 29-31 packs/s |
| Mean blob rate | 35,219 blobs/s |
| Final candidate size | 3.9 GiB in 17 SSTs |
| Typical host idle CPU | more than 90% |

The database backend in that experiment was NFS and showed queued latency during SST flush bursts, but neither CPU nor sustained backend bandwidth was saturated. Enabling a large SlateDB read cache in earlier runs did not remove the slowdown. This phase treats storage latency as an amplifier rather than the root utilization limit.

## Scope

This phase changes only forced or explicitly selected legacy metadata import. Normal backup publication retains the existing per-pack transaction path.

In scope:

- a bounded `ImportLegacyPacks` API and planner;
- a producer/preparer/writer pipeline with bounded memory;
- one aggregate update and one history-sequence allocation per committed batch;
- duplicate pack and blob coalescing within a batch;
- adaptive batch limits based on mutation count and encoded bytes;
- a scalable fresh-import membership filter with a bounded false-positive rate;
- index checkpoint integration and crash-safe replay;
- import-specific metrics and repeatable performance gates.

Stage 3 extends this scope to bounded concurrent VaulticDB ingestion, ordered
metadata reduction, and daemon transaction scheduling. Its objective is to hide
existing backend latency, not to reduce the latency of the backend itself.
Legacy index discovery, download, decoding, and source-pack preparation are not
optimization targets for this extension. The ordered selected-input stream is
its input contract.

Out of scope for the first implementation:

- changing SlateDB or its transaction isolation model;
- allowing arbitrary concurrent legacy pack commits;
- weakening metadata encryption, writer fencing, or schema validation;
- requiring local NVMe, a particular NFS implementation, or a read cache for acceptable behavior;
- changing normal `PublishPack` semantics;
- activating an incomplete candidate.

## Required invariants

1. A committed batch is equivalent to applying its input packs one at a time in deterministic order, except that aggregate update sequences and history sequence allocation are amortized over the batch. Adaptive splits are reduced as physical child receipts; metrics must label those as physical reductions rather than one logical wall-time batch.
2. Every physical blob location present in any input survives, including duplicate blob IDs in different packs and repeated pack IDs in different legacy indexes.
3. A retry after an aborted transaction replans from current database state. It never reuses mutations derived from a stale transaction snapshot.
4. Fresh-import membership state is updated only after a successful commit. False positives may cause reads; false negatives must never suppress a required read.
5. A completed index checkpoint is never visible unless every selected pack from that index is committed. A missing checkpoint may cause idempotent replay but not lost metadata.
6. Work-budget and error-limit behavior remains deterministic at pack boundaries. A batch never imports records beyond the selected input prefix.
7. Cancellation and timeout remain effective while preparing, waiting for publication, issuing transaction RPCs, retrying conflicts, and awaiting the final checkpoint.
8. Mutation count, encoded bytes, transaction lifetime, queued prepared bytes, and retry work have independent controls. Adaptive item/byte targets admit one indivisible oversized pack; this exception does not relax per-RPC limits, record validation, transaction timeouts, or the single-pack preparation floor.
9. Failed, interrupted, or partial fresh imports never authorize memory-WAL handoff or candidate activation.

## Import pipeline

Split import into three bounded stages:

1. **Index reader.** Decode one legacy index, apply resume and work-budget selection, and emit ordered pack jobs.
2. **Pack preparers.** Run backend `Stat` and construct validated `LegacyPackImport` values concurrently. They do not call VaulticDB.
3. **Batch writer.** Restore deterministic input order, collect a bounded batch, and call `SchemaStore.ImportLegacyPacks` through one publication lane.

A bounded prepared-result channel provides backpressure. Track both item count and estimated retained bytes so a few unusually large packs cannot exceed the memory budget. Cancellation closes admission, lets in-flight preparation finish or abort, and prevents a later batch from committing after the first terminal error.

Pack preparation timeout and database batch timeout are separate. `--pack-timeout` continues to bound source `Stat` and preparation for one pack. Add `--import-batch-timeout` for one database batch, with a default derived from the existing transaction timeout and an explicit upper bound. `--packs-per-transaction` is capped at 256 because one transaction's pack count is also the minimum in-flight preparation floor needed to guarantee progress under byte backpressure.

## Multi-pack transaction API

Add a bulk-only method without changing the normal publication API:

```go
func (store *SchemaStore) ImportLegacyPacks(
    ctx context.Context,
    imports []LegacyPackImport,
    finalCheckpoint *Mutation,
) error
```

`finalCheckpoint` is non-nil only when the batch completes a legacy index. Keeping it in the same transaction makes index completion atomic with the final packs. Earlier batches from a large index may remain committed without a checkpoint; replay merges them idempotently.

The method acquires the existing legacy-import gate once, validates all inputs before opening a transaction, derives fresh-import lookup hints, and retries aborted transactions with bounded exponential backoff. Every retry begins a new transaction and rebuilds the complete plan. After commit, it updates the membership filter for every committed pack and blob.

Keep `ImportLegacyPack` as a compatibility wrapper around a one-element `ImportLegacyPacks` call. This leaves normal callers and focused tests intact while ensuring the two paths cannot drift semantically.

## Batch planner

The planner operates on final states, not a concatenation of per-pack plans.

### Canonicalization and coalescing

Validate and canonicalize every input first. Preserve original order for deterministic error reporting and history ordering, then build:

- one accumulator per pack ID containing all source index IDs, locations, placement data, debt, and lineage inputs;
- one accumulator per blob ID containing all distinct physical locations from every pack in the batch;
- ordered history events representing the same logical transitions as the current per-pack path.

A repeated pack ID is loaded once and merged through the existing pack-record rules. A repeated blob ID is loaded once and all incoming locations are merged in deterministic pack and location order. Conflicting facts that the existing one-pack path would reject must still reject the batch with the responsible pack ID.

### Existing-state reads

Use fresh-import membership hints to divide pack and blob IDs into definitely absent and possibly present sets. Issue bounded `MultiGet` calls only for possibly present IDs, deduplicating keys across the whole batch. Apply database values to the in-memory accumulators once.

The planner must not rely on transaction read-your-write behavior between individual pack plans. It computes each final blob and pack value in memory and emits at most one mutation per key.

### Aggregates and history

Read `aggregateKeys()` once. For each unique pack, calculate the delta from its pre-batch record to its final record and apply all deltas in memory. Preserve the existing publication contract by incrementing every type aggregate and every existing tier aggregate once per batch; missing unused tiers remain sparse. Emit at most one mutation per aggregate key. This removes the principal shared-key conflict and repeated read/write amplification.

Read the history-enabled marker and next-event sequence once. Allocate one contiguous range for all successfully encoded events, emit deterministic event keys, and update the next sequence once. Preserve the current rule that an unencodable advisory history event does not fail catalog publication.

Debt and placement mutations are coalesced by key. If two inputs produce incompatible values for one mutable key, reject the batch rather than depending on mutation order.

### Mutation publication

Sort mutations by key for deterministic requests and tests. Use the existing `writeTransactionBatches` helper to respect advertised per-RPC item and message limits while retaining one transaction. Commit with deferred durability only under the existing destructive fresh-import precondition.

A server-side transaction may span several mutation RPCs, so client RPC limits do not by themselves bound transaction memory or lifetime. The client therefore applies stricter batch admission limits before `Begin`.

## Adaptive batch limits

Add explicit import controls:

```text
--packs-per-transaction N
--import-transaction-bytes BYTES
--prepared-import-bytes BYTES
--import-batch-timeout DURATION
```

Zero selects derived defaults. Start conservatively with:

- at most 8 packs;
- at most 8,000 estimated unique mutations;
- at most 8 MiB of estimated encoded keys and values;
- at most 256 MiB of queued prepared pack data.

Whichever limit is reached first closes the batch. An oversized multi-pack plan returns `ErrLegacyImportBatchTooLarge` and is split adaptively. A single pack is indivisible and may exceed these planning targets, matching Stage 2: keep its catalog and receipt atomic in one transaction, using multiple bounded mutation RPCs as necessary. Per-RPC message/item limits, record validation, and transaction timeouts still apply; this is not unlimited admission of multiple packs. Report oversized single-pack count and maximum actual transaction bytes/mutations separately. The estimate includes protobuf framing headroom, receipt/update overhead, pack records, aggregates, history, debt, placement, and checkpoint mutations. Record actual values and tighten the estimator if an RPC must split unexpectedly often.

Do not reinterpret the existing `--batch-size`; it limits mutations per daemon transaction RPC. The new pack and byte controls bound the larger logical transaction.

## Scalable fresh-import membership filter

The current 64 MiB filter uses four bit positions per ID. For $m$ bits, $k=4$ probes, and $n$ inserted IDs, its approximate false-positive probability is:

$$
p = \left(1-e^{-kn/m}\right)^k
$$

At 20 million IDs the false-positive rate is negligible, but it is approximately 4% at 80 million, 8% at 100 million, 36% at 200 million, and 81% at 400 million. A false positive is safe but forces an unnecessary SlateDB lookup; at repository scale the fixed filter therefore recreates the database-size-dependent read path it was intended to avoid.

Replace it with a scalable Bloom filter:

- allocate immutable-capacity layers as the population grows;
- size each layer for a declared capacity and false-positive target;
- use the near-optimal probe count for that layer rather than fixing four probes;
- query all layers and insert into the newest layer;
- allocate the next layer before the active layer exceeds its design capacity;
- expose layer count, bytes, inserted IDs, estimated false-positive rate, and lookup outcomes;
- enforce a memory ceiling derived from the fresh-import memory profile;
- if the ceiling is reached, degrade explicitly to database lookups without violating correctness.

Target an aggregate false-positive probability no worse than 0.1% through the supported import size. With geometric capacities and successively tighter layer probabilities, covering 400 million IDs allocates approximately 1.31 GiB and remains within the 1.5 GiB fresh-import ceiling. Tests must use deterministic hashes and tiny layers to exercise rollover and saturation cheaply.

## Checkpoints, retries, and shutdown

For an index that fits in one transaction, commit all packs and its checkpoint together. For an index split across transactions, commit the checkpoint only in the final transaction. Root receipt IDs derive from canonical batch content (high 48 bits) with the low 16 bits reserved for deterministic split paths, so partial resume and later full replay avoid ordinal collisions while preserving deterministic child IDs. If the process stops earlier, the next resumable import replays that index; existing pack and blob merge rules make prior batches idempotent.

Only update process-local progress counters after a successful batch commit. Return the first input position associated with a planning or commit failure so reporting remains actionable. An aborted transaction retries the entire batch from fresh reads. A validation, timeout, cancellation, or permanent storage error aborts the batch and prevents its filter updates.

The existing successful-import marker remains the sole authorization for memory-WAL handoff. Batching must not make a work-budget stop, partial index, finding, or failed checkpoint appear complete. When activation is requested, validate the complete candidate, write the marker, cleanly stop the temporary daemon so the memory WAL is handed off, reopen the candidate on the inherited persistent local WAL, and only then publish SlateDB authority. A shutdown, handoff, or reopen failure must leave legacy metadata authoritative.

## Observability

Implement the counters locally in this phase and expose them through the Phase 33 monitoring schema when that phase lands:

- packs, blobs, unique keys, encoded bytes, and source indexes per batch;
- preparation queue depth and bytes;
- preparation time, gate wait, planning reads, mutation RPC time, commit time, and total batch latency;
- transaction commits, mutation RPCs, retries, conflicts, and replanned bytes;
- definitely-absent, possibly-present, found, and false-positive-equivalent lookups;
- membership-filter layers, bytes, occupancy, inserts, and estimated false-positive rate;
- SST flush/compaction backpressure observed during import;
- current limiting reason: preparation, transaction item limit, transaction byte limit, publication, WAL, flush, compaction, backend, or checkpoint.

Import progress must distinguish records selected, prepared, committed, skipped as duplicate, and awaiting checkpoint publication. Paths, pack IDs, blob IDs, and raw backend errors remain excluded from metric labels.

## Phased implementation

### Stage 1: Measurement and scalable lookup avoidance

1. Add import batch/transaction timing, lookup outcome, queue, and filter occupancy counters.
2. Replace the fixed 64 MiB membership filter with the bounded scalable filter.
3. Preserve one-pack transactions and establish before/after results with identical fixtures.

This stage isolates the database-growth component and can ship independently.

### Stage 2: Serial multi-pack transactions

1. Add `ImportLegacyPacks` and retain `ImportLegacyPack` as a one-item wrapper.
2. Implement batch coalescing, unique existing-state reads, one aggregate update, one history allocation, and post-commit filter publication.
3. Refactor legacy import into bounded preparation and writer stages.
4. Add atomic final-index checkpoint mutation and adaptive pack/mutation/byte limits.
5. Benchmark pack counts of 1, 4, 8, and 16, then choose the smallest default that captures most of the gain.

This is the completed baseline for Phase 32. The implemented Stage 3 extension
has separate performance acceptance gates below.

### Stage 3: Write-side latency hiding

#### Evidence and measurement gate

The post-implementation clean 20-million-record run took 276.893 seconds, with
271.334 seconds inside publication, 87.520 seconds in planning, 31.621 seconds
in mutation RPCs, and 91.925 seconds in commit. It completed 4,616 transactions
at approximately 72,230 blobs/s. Publication therefore occupies about 98% of
import time. Planning includes VaulticDB reads and record encoding, not reading
the initial legacy indexes. The approximately 60.3 seconds outside the three
named publication sub-timers must be attributed before choosing optimizations;
they include transaction begin, validation, membership hints, and filter updates.

These measurements establish serialized publication as the bottleneck, but do
not establish NFS as its dominant cause. Memory-WAL deferred commit time must
not be labeled NFS or durable-flush time without server-side evidence. Low CPU
utilization likewise does not distinguish RPC waiting, locks, storage waits,
allocation, or memory-access costs. No backend replacement or latency reduction
is a prerequisite for this work.

The short eight-versus-sixteen-pack experiment reduced transactions from 475 to
341 and elapsed time from 24.993 to 24.262 seconds, but processed 2,664 versus
2,491 packs. It is suggestive, not a controlled proof of a 2.9% tuning gain.
Repeat tuning with a frozen selected-input prefix and repeated runs before
changing defaults. Larger batches alone have not demonstrated a large gain.

Add non-overlapping client timers for admission, validation/filter work, begin,
planning, mutation submission, commit acknowledgement, and post-commit work.
Split planner reads by pack/blob, aggregate, history, and debt/placement category,
separating key counts from RPC counts and RPC wait from local encoding work.
Correlate daemon admission, lock/conflict waits, transaction apply, WAL waits,
flush/compaction stalls, and object-store request latency with these timers.
Report overlapping asynchronous durations separately from wall-time totals.

#### Live-run observations: 2026-09-16

The full source contains 10,019 legacy index files, approximately 19.25 GB
(18 GiB). The run used 32 preparers, `local.connections=32`, two publication
lanes, eight packs per transaction, 8 MiB transaction and 256 MiB prepared-data
targets, snapshots disabled, memory WAL with a 500 ms flush interval, a 16 GiB
unflushed limit, and 256 MiB L0 SSTs. The configured candidate path was
`/volume2/NASDA2/rustic/db`; shared read-cache tiers were disabled. Record the
resolved storage configuration, not just environment variables, in future runs.

The binary was built from `ef5af31bd` with the fixes subsequently committed as
`902b94389`; its embedded version still reported the former revision as dirty.
The following is an intermediate sample at 09:02:09 UTC, not a completion result:

| Measurement | Observed result |
|---|---:|
| Completed source indexes | 656 / 10,019 |
| Committed packs / blobs | 34,923 / 28,061,467 |
| Wall time | 8m51s |
| Cumulative throughput | 65.8 packs/s; 52,880 blobs/s |
| Preparation aggregate time | 31.532s |
| Publication aggregate-lane time | 9m18.699s |
| Checkpoint batch time | 9.282s |
| Prepared queue at sample | empty; peak 353 packs / 13,444,704 bytes |
| Combined CPU over a separate five-second sample | 1.64 cores |

Publication and preparation durations accumulate overlapping work; checkpoint
time is not an additional disjoint wall-time bucket. Publication exceeding wall
time is expected with multiple lanes and cannot establish its wall-time share.
An empty queue at a source boundary does not establish preparation starvation.
The early Stage 3 rate cannot be compared directly with the 72,230 blobs/s
Stage 2 budgeted result: source order, selected records, candidate age, and total
work differ. No Stage 3 speedup or completed-import ETA is established yet.

Live profiling found:

- Approximately 65% of sampled CPU cycles belonged to daemon worker/encryption
    threads and 35% to Vaultic. Hot work included SlateDB skip-list traversal,
    memory copies, SST encoding, local reads, and encryption. This is CPU-time
    attribution, not off-CPU wait attribution or proof of a single dominant function.
- Interval NFS samples showed essentially no data reads or writes, apart from
    small log writes, despite continuing progress. The first `nfsiostat` report
    is a cumulative mount average, not current throughput; the earlier inference
    of a 478 KB/s source-read bottleneck was invalid.
- Daemon process I/O and the configured candidate directory size did not agree
    with a simple NFS-only storage interpretation. Resolve actual SST/WAL/cache
    paths, mounts, cached reads, and object-store request counters before assigning
    those bytes or delays to NFS. Short file-open tracing did not resolve this.
- Modest CPU/RSS and a small prepared queue point to limited useful concurrency,
    not a reason by themselves to increase worker counts, memory limits, or caches.

The run exposed two correctness failures before useful measurement was possible:

1. Dependency inversion: with batches requesting `A`, `A+B`, and `B`, bypassing
     the middle waiter could leave a later receipt holding `B` while ordered
     reduction waited for the middle batch. Admission now carries forward all
     blocked dependencies, including transitive waiters. Unrelated work may still
     proceed; later work must never reserve a dependency needed by an earlier waiter.
2. A pack with 10,000 blobs produced 10,002 mutations, exceeding the 8,000-mutation
     planning target. Restoring the indivisible-pack exception avoids rejection
     while retaining transaction atomicity and bounded RPC submission.

Keep both regressions in the performance fixture. A prior daemon SIGHUP was a
launch/lifecycle failure, not evidence of a storage bottleneck. Preserve failed
run diagnostics separately from successful benchmark results.

Raw logs use `/volume2/NASDA2/rustic/log/index-import-stage3-full` with `.log`,
`.console.log`, `.run`, `.time`, and `.exit` suffixes. `legacy-import-live.log`
points to the application log; `.cpu-profile.txt` contains the CPU report.
Check run timestamps and exit status together: an empty or stale timing file
must not be presented as the current run's completed measurement.

The run was intentionally stopped with SIGINT before controlled profiling so it
would not contend with the experiment. Its final logged boundary was 2,025 of
10,019 indexes, 107,076 packs, and 85,864,124 blobs after 30m32s; the exit status
is 130. This remains an incomplete diagnostic run, not an end-to-end result.

#### Implemented measurement and controlled experiment

The importer now reports mutually exclusive coordinator phase totals, a
time-weighted active-lane histogram, ready and unreduced queue depths, retained
prepared bytes, periodic Go heap/GC state independent of progress callbacks,
and detailed receipt-cleanup scan/begin/write/commit totals and counts. Command
lifecycle logs separately include close, successful-marker/handoff/reopen, and
total wall time. These coordinator states classify what the scheduler was doing
when it blocked; they are not daemon-side causal wait attribution.

`--import-defer-cleanup` changes receipt deletion to `CommitDeferred` only when
all three guards hold: destructive fresh reset, memory WAL, and at least two
publication lanes. Ingest and reduction correctness remains receipt-backed;
the existing durable successful-import marker is still the only handoff
authorization. Tests cover bounded deletion, idempotent replay, fresh-import
rejection, absence of premature authorization, and successful persistent-WAL
handoff/reopen after deferred deletion.

`make profile` writes optimized binaries to `bin/profile/<platform>` without
overwriting production binaries. Go is built with the existing `profile` tag so
DWARF and the symbol table are retained; Rust release builds already use
`debug=1` and `strip=false`. Verify both artifacts before observing them:

```
readelf -S bin/profile/linux-amd64/vaultic \
    bin/profile/linux-amd64/vaulticdb | rg 'debug_info|symtab'
perf record -F 499 --call-graph dwarf -o run.perf.data -- <command>
perf report --stdio -i run.perf.data
```

A deterministic real-daemon fixture imported the same 128 packs and 65,536
blobs for every variant, completed the memory-WAL handoff, reopened the local
persistent WAL, and verified the source checkpoint. Each cell below is the mean
of three cold temporary candidates; standard deviation is for total wall time.

| Available CPUs | Lanes / cleanup | Wall time | Blobs/s | Cleanup | Finalize | Wall-time SD |
|---:|---|---:|---:|---:|---:|---:|
| 4 | 1 / ordinary Stage 2 | 1,149.8 ms | 56,999 | n/a | 597.1 ms | 1.6 ms |
| 4 | 2 / ordinary | 1,146.5 ms | 57,161 | 122.5 ms | 636.1 ms | 1.1 ms |
| 4 | 2 / deferred | 673.5 ms | 97,301 | 2.0 ms | 272.7 ms | 0.7 ms |
| 4 | 4 / deferred | 674.6 ms | 97,155 | 1.7 ms | 337.6 ms | 1.7 ms |
| 4 | 8 / deferred | 676.0 ms | 96,952 | 1.9 ms | 318.2 ms | 1.4 ms |
| 32 | 1 / ordinary Stage 2 | 1,172.3 ms | 55,904 | n/a | 547.3 ms | 4.8 ms |
| 32 | 2 / ordinary | 1,175.7 ms | 55,748 | 93.4 ms | 666.2 ms | 11.9 ms |
| 32 | 2 / deferred | 691.8 ms | 94,744 | 2.4 ms | 260.5 ms | 7.0 ms |
| 32 | 4 / deferred | 695.5 ms | 94,250 | 2.4 ms | 372.9 ms | 9.1 ms |
| 32 | 8 / deferred | 696.9 ms | 94,046 | 2.8 ms | 356.6 ms | 2.9 ms |

On this fixture, guarded cleanup deferral improved two-lane end-to-end
throughput by 70.2%. Four and eight lanes did not improve on two, and exposing
32 CPUs was 2.6% slower than restricting the run to four. The host affinity is
CPUs 0-31; no ancestor CPU quota, throttling, `memory.max`, or `memory.high` was
observed. The interrupted production run used about 1.4 GiB RSS in Vaultic and
9.8 GiB in VaulticDB while approximately 273 GiB remained available. Its peak
prepared queue was only 13.4 MiB against a 256 MiB budget. More preparers,
publication lanes, prepared memory, or a larger Go memory limit therefore have
no supporting evidence. Retain two lanes, and consider reducing CPU affinity
for operational isolation rather than increasing it.

The corresponding 499 Hz DWARF profile captured 1,897 samples with zero loss
and named frames from both Go and Rust. Go CPU was distributed across SHA-256
batch/receipt identity, sorting/comparison, memory movement, and the fresh-ID
filter. Rust CPU included SlateDB skip-list/memtable traversal, bytes clone/drop,
memory copy/compare, allocation, and block encoding. No single CPU hotspot
explains wall time. Finalization remained 38-55% of the deferred fixture, so the
next repository-scale run must test whether deferred cleanup merely moves work
to flush/handoff after crossing L0 and compaction thresholds.

#### Throughput investigation and decision plan

The strongest current hypothesis is a serial publication/control path with
insufficient overlap. Three implementation points require separate attribution:

- [Per-index cleanup](../../../internal/index/daemon/schema_store_import_split.go)
    scans receipts and deletes them with ordinary `Commit`, unlike deferred ingest
    and reduction. The daemon's
    [commit path](../../../vaulticdb/src/storage.rs) then awaits durability. This is
    a real barrier before the next source index, but its measured cost is unknown.
    A 500 ms flush interval is not proof that every cleanup waits 500 ms, nor that
    memory-WAL acknowledgement requires an NFS flush.
- The [Stage 3 scheduler](../../../internal/index/legacyimport/import_stage3.go)
    performs reduction synchronously. Existing ingests can continue, but it cannot
    admit replacements while blocked in reduction. Dependency retention through
    ordered reduction can further reduce effective lane occupancy.
- [Index callbacks](../../../internal/repository/index/index_parallel.go) are
    serialized although fetch/decode is parallel. Each callback drains its pack
    pipeline and completes receipt cleanup before the next callback proceeds.
    More index download workers cannot remove this publication boundary.

Bounded periodic telemetry is now implemented. Collect the following in the
next repository-scale run before changing additional defaults:

| Question | Required measurement |
|---|---|
| Where does wall time go? | Mutually exclusive coordinator states: source/selection, waiting for preparation, dependency-blocked admission, ingest-result wait, reduction, cleanup, and finalization; keep overlapping worker timers separate. |
| Is cleanup the barrier? | Per-index receipt validation scan, delete scan, begin, mutation RPC, commit acknowledgement, daemon durability wait, cleanup pages/bytes, and total cleanup wall time. |
| Are lanes actually occupied? | Time-weighted active-ingest histogram for 0..N lanes, ready queue depth/bytes, completed-but-unreduced bytes, oldest pending age, dependency-blocked time, and reducer-busy time. Peak lanes alone is insufficient. |
| Which transaction work costs most? | Separate ingest/reduce/cleanup counts and p50/p95/p99 begin, planning reads, local encoding, mutation submission, commit/apply, and post-commit times, including retries. |
| Is the engine or backend stalling? | Daemon admission/lock waits, WAL enqueue/flush/durable sequence lag, memtable/L0 pressure, flush/compaction bytes, object-store operation latency and concurrency, cache hits/misses, and actual SST/WAL/cache destinations. |
| Are resources restricted? | Interval per-process/per-thread CPU, off-CPU stacks or Go execution trace, RSS and heap/GC data, effective CPU affinity and ancestor cgroup quota/throttle deltas, device I/O and interval NFS statistics. |

Correlate bounded traces by run, source ordinal, logical batch, physical receipt,
and transaction operation; do not expose raw repository identities as metric
labels. Report liveness periodically even when no index or batch completes.
Instrument cleanup and final handoff explicitly: the existing publication and
checkpoint timers do not cover all source-completion/finalization work.

Prioritize experiments according to those measurements:

1. **Cleanup durability and frequency.** If cleanup contributes materially to
     wall time, test deferred cleanup only for the fenced destructive memory-WAL
     rebuild, or bounded cleanup amortized across sources. Preserve ordinary
     durability for non-fresh/persistent operation. Crash-test a checkpoint whose
     receipt deletions are lost, partial deletion, replay, and ambiguous commit;
     retain bounded recovery state, final durable handoff, reopen validation, and
     the prohibition on activating an incomplete candidate. Do not merely remove
     durability waits or postpone all cleanup until the end.
2. **Reducer scheduling.** If ready work waits while the coordinator reduces,
     test a dedicated ordered reducer with bounded handoff so independent ingestion
     can refill lanes. Preserve dependency fairness, one owner for shared metadata,
     ordered checkpoints, cancellation, and capacity for the earliest unresolved
     batch. Do not release overlapping-key dependencies earlier without a proof
     that filter hints and old/new receipt states remain correct.
3. **Lane and batch tuning.** Only when independent ready work exists, compare
     one, two, four, and eight lanes. Tune pack/item/byte targets separately if fixed
     transaction overhead dominates. Stop increasing concurrency when conflicts,
     unreduced bytes, p99 latency, or compaction pressure rise without useful gain.
     Extra preparers or memory are justified only by measured preparation starvation
     or byte-budget blocking, respectively.
4. **Engine/cache/backend changes.** Pursue these only if attributed read,
     allocation/GC, WAL, flush, or compaction costs dominate. CPU hotspot percentages
     alone do not justify replacing the backend or adding a second commit scheduler.

Freeze an ordered manifest of source indexes and pack selections, not only a
record-count budget: parallel decode completion may otherwise change the selected
prefix. Compare identical input and batch semantics, snapshot policy, resolved
WAL/cache/backend settings, encryption, and candidate starting state. Change one
variable at a time and repeat at least three times, reporting spread and warm/cold
cache conditions. Include duplicate-heavy, large-pack, and dependency-chain cases,
plus runs large enough to cross SST flush and compaction thresholds.

Choose an optimization only when it reduces an attributed wall-time component
and improves repeated end-to-end blobs/s without violating correctness, memory,
or tail-latency bounds. Report startup/reset, import, cleanup, successful marker,
shutdown/handoff, reopen, validation, and requested activation separately and in
the total. A hot-path gain that moves work into finalization is not a total
throughput gain. Keep the existing 20% Stage 3 acceptance target and require a
complete uncapped run before claiming repository-scale completion or durability.

#### Ownership and concurrency model

The VaulticDB daemon owns bounded admission, transaction execution, completion
receipts, and any supported commit coalescing. Vaultic's `SchemaStore` and legacy
import writer own deterministic input selection, batch identity, and submission
through a capability-negotiated bulk-import interface. The existing serial API
remains the compatibility path. Inspect daemon and SlateDB scheduling first:
if independent transactions are already concurrent, reuse that capability;
change the engine only where measured serialization requires it.

Introduce a fresh-candidate-only bulk session fenced against other writers,
identified by candidate generation and stable ordered batch IDs. The session
admits several bounded transactions without changing normal `PublishPack` or
resumable non-fresh import semantics. Candidate activation remains forbidden
throughout ingestion and reduction. Merely increasing `legacyImportGate`
capacity is not an implementation of this design.

Use the following logical pipeline:

1. Admit ordered selected batches into a bounded daemon queue.
2. Plan and write independent pack/blob records concurrently, atomically with
     private reduction inputs and a batch completion receipt.
3. Reduce completed batches in selected-input order into canonical aggregate,
     history, debt, and placement state where those keys are shared.
4. Advance index checkpoints only behind the contiguous reduced prefix.
5. Validate the completed candidate before the existing handoff and activation.

Bound outstanding transactions, retained bytes, unreduced bytes, and distance
ahead of the contiguous prefix independently. Start with two publication lanes;
evaluate four and eight only while useful throughput improves without excessive
tail latency, conflicts, or backend pressure. Keep preparation worker count
independent. Backpressure must reserve capacity for the earliest unresolved
batch and the reducer so later work cannot prevent forward progress.

#### Shared keys, ordering, and replay

Pack/blob transactions must not update canonical aggregate counters or the
global history sequence. Write immutable per-batch reduction records under
disjoint session/batch keys in the same transaction as their catalog changes
and receipt. Define a versioned private namespace and validate its encoding;
it must not become part of an activated repository's unresolved state.

Duplicate blob and pack IDs still create cross-batch dependencies. Schedule
overlapping read/write key sets in input order, allowing only independent sets
to execute concurrently; include mutable debt and placement keys in dependency
analysis. Never infer independence from pack IDs alone. Use bounded conflict
retries with fresh reads as a correctness backstop. Acquire dependencies in a
deterministic order and release them on every failure path. Fresh-filter hints
must be taken after dependencies are satisfied and prior committed inserts are
published; concurrent admission must not introduce stale definitely-absent hints.

The ordered reducer allocates history sequences and applies aggregate deltas
exactly once, atomically with its reduction watermark. Derive deltas from the
actual pre/post states of successful catalog transactions, preserving Stage 2
batch sequence semantics and advisory-history rules. Replay must neither append
history twice nor double-count aggregates. A receipt resolves an ambiguous
commit acknowledgement before resubmission. An identical batch ID with different
content is rejected. Preallocating sequence ranges or reconstructing history is
not a substitute unless equivalence with the existing history contract is proven.

Canonical records may be ahead of canonical aggregates only inside the fenced,
inactive candidate. Checkpoints and progress distinguish ingested, reduced, and
checkpointed work. Publish an index checkpoint atomically with the reducer
watermark only after every selected preceding batch and all of that index's
required effects are complete. Out-of-order receipts cannot close a gap.

On terminal error or cancellation, stop admission, cancel or settle in-flight
transactions, resolve ambiguous outcomes, and report the earliest failing input
position. Already admitted later batches may have committed privately, but must
not advance the checkpoint past a failure. Persisted sessions replay receipts
and reduction watermarks idempotently; memory-WAL sessions follow the existing
destructive restart policy and must not claim crash-resumable durability.
Work-budget selection occurs before concurrent admission so no batch can extend
the selected prefix. Any partial run still forbids successful-import marking.

#### Commit scheduling and durability

Allow multiple independent submissions to overlap backend waits where the
daemon and SlateDB support this. Evaluate group commit only if measured WAL or
flush waits are substantial and the existing engine does not already coalesce
them. Do not add a second flush scheduler without evidence. Grouping must retain
per-transaction atomicity, conflict isolation, bounded acknowledgement latency,
and the requested durability level. Deferred memory-WAL acknowledgement is not
durable completion; a flush must never be added per transaction merely to enable
grouping. Retain the existing successful-import marker, clean shutdown, persistent
WAL reopen, validation, and authority-switch requirements.

#### Delivery and acceptance

Implement attribution first, then private receipt/reduction semantics with one
lane, then bounded concurrency, and only then any justified engine scheduling
change. The current CLI selects two lanes for fresh reset imports; that default
is not evidence that the performance gate passed. Retain one-lane Stage 2 as
the comparison/fallback and do not expand Stage 3 beyond fresh imports before
all gates pass:

- Compare canonical metadata and ordered history with Stage 2 using identical
    batch boundaries, including duplicate-heavy and repeated-source fixtures.
- Inject crashes and ambiguous acknowledgements before and after catalog commit,
    receipt persistence, reduction, checkpoint, handoff, and activation; prove
    exactly-once reduction and no activation of an incomplete candidate.
- Exercise cross-batch duplicate keys, stale filter hints, out-of-order finishes,
    reducer failure, transaction expiry, cancellation, and byte-limit exhaustion;
    prove bounded memory, deterministic checkpoints, and no admission deadlock.
- Recompute canonical aggregates independently before completion and verify all
    receipts are reduced, all required checkpoints exist, and private recovery
    state is finalized before writing the successful-import marker.
- Benchmark one, two, four, and eight lanes on the same fixed input and unchanged
    backend/WAL/cache configuration, including a candidate large enough to flush
    and compact. Report repeated-run spread, conflicts, reducer backlog, resource
    bounds, backend request concurrency, and p95/p99 acknowledgement latency.
- Target at least 20% sustained publication-throughput improvement over Stage 2
    on the fixed 20-million-record NFS fixture, with no correctness regression or
    unbounded pressure. This is a proposed acceptance target, not a measured gain;
    keep the extension opt-in if it is unmet and report the remaining limiter.

## Tests

- Compare one-pack and multi-pack imports byte-for-byte for pack, blob, placement, debt, aggregate, history, and checkpoint namespaces after normal, duplicate-heavy, and repeated-index fixtures.
- Import the same pack from multiple source indexes in one batch and across batches; verify source provenance, physical-size handling, blob counts, payload totals, and aggregate deltas.
- Import the same blob from multiple packs and repeated locations; verify canonical ordering, deduplication, and no lost location.
- Inject transaction conflicts before and during commit; verify full replanning, bounded retry, no stale mutations, and no pre-commit membership inserts.
- Inject cancellation and timeout during preparation, gate wait, reads, mutation batches, commit, and final checkpoint.
- Split an index at every batch boundary, terminate after each committed batch, resume, and compare with an uninterrupted import.
- Exercise one oversized pack, exact item/byte boundaries, estimator undercount, multiple mutation RPCs, and server transaction expiry.
- Test scalable-filter rollover, target false-positive rate, memory ceiling, deterministic serialization-free startup, and correctness after forced fallback to database lookup.
- Run race tests over preparers, ordered collection, progress reporting, filter publication, and cancellation.

## Performance validation

Use deterministic legacy-index fixtures with unique-heavy and duplicate-heavy distributions at 20 million, 100 million, and a projected or real 400 million IDs. Report, rather than infer:

- packs and blobs per second by interval and cumulatively;
- transaction commits and mutation RPCs per million blobs;
- lookup count and found rate per million blobs;
- filter false-positive estimate and memory;
- CPU time by `vaultic` and `vaulticdb`;
- peak prepared bytes and transaction bytes;
- SST bytes read/written, compaction bytes, and backend latency;
- p50, p95, and p99 batch planning and commit latency.

Run local-filesystem and NFS-backed candidates when available, with the read cache both disabled and enabled. Storage choice is a reported dimension, not a correctness prerequisite. Compare against the measured per-pack baseline and retain raw logs plus exact binary revisions.

## Exit criterion

The implemented Stage 1-2 baseline is complete when:

- the scalable filter stays within its declared memory and false-positive bounds at the largest supported import;
- one-pack and batched imports produce equivalent authoritative metadata and resume safely after every injected interruption;
- the selected default reduces transaction commits by at least 4x on the 20-million-record fixture;
- the 20-million-record fixture improves pack throughput by at least 2x without increased errors or unbounded memory;
- interval throughput no longer declines because the membership filter saturates;
- local metrics identify preparation, publication, lookup, transaction retries, and checkpoint waiting separately; the configured WAL profile is logged, while live WAL, flush, compaction, and backend attribution remains part of the Phase 33 monitoring schema;
- normal `PublishPack`, non-fresh import, activation, and metadata durability semantics remain unchanged.

The Stage 3 extension is accepted only when its delivery and acceptance
gates pass. Stage 2 remains the fallback; no Stage 3 candidate may activate with
unreduced records or unvalidated aggregate/history state.

## Implementation validation

The Stage 2 implementation was validated on the same filesystem-backed
repository and candidate directories on an NFSv3 mount used for the baseline.
The candidate used the local object-store adapter, memory WAL, and no configured
read-cache tier. The command used 32 preparers, the default eight packs and
8 MiB per logical transaction, a 256 MiB prepared-work limit, snapshots
disabled, and a 20-million-record work budget. Reaching that budget
intentionally returns the incomplete exit status and does not authorize
memory-WAL handoff.

| Measurement | Per-pack baseline | Phase 32 |
| --- | ---: | ---: |
| Elapsed time | 9m28.1s | 4m32.6s |
| Overall pack throughput | 43.9 packs/s | 91.6 packs/s |
| Overall blob throughput | 35,219 blobs/s | 73,422 blobs/s |
| Logical commits | 24,942 | 4,616 |
| Final interval pack throughput | 31.3 packs/s | 79.6 packs/s |

The measured speedup was 2.08x and logical commits fell by 5.4x. The final run
imported 24,942 packs and 19,999,995 blob records with no transaction conflicts,
retries, adaptive splits, or import errors. It issued 5,344 mutation RPCs for
20,062,630 final mutations. The prepared queue peaked at 151 packs and 13.4 MiB.

The scalable filter held 19,950,312 unique IDs in two layers using 50,346,239
bytes. Its estimated aggregate false-positive probability was 0.052%, below the
0.1% target; 6,353 of 68,259 possibly-present lookups missed, and the filter did
not enter database fallback. A zero-allocation sizing test projects the layered
configuration through 400 million IDs within the 1.5 GiB ceiling and false-
positive budget.

A deterministic 64-pack benchmark also exercises transaction sizes 1, 4, 8,
and 16. It reports 64, 16, 8, and 4 commits per operation respectively. Eight
packs remains the default because it clears the commit-reduction target while
keeping transaction lifetime and encoded-size exposure conservative.
