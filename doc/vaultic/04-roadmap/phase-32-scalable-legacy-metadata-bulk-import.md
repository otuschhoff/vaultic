# Phase 32: Scalable legacy metadata bulk import

[← Back to roadmap index](00-overview.md)

[← Phase 31](phase-31-read-only-nfsv3-snapshot-server.md) · [Phase 33 →](phase-33-operational-monitoring-and-metrics-export.md)

[CLI and operations architecture](../02-architecture/04-cli-and-operations.md) · [Operational monitoring](phase-33-operational-monitoring-and-metrics-export.md)

**Status: Stages 1-2 implemented; Stage 3 split-session importer is enabled only for fresh imports (`--force-reset-old-idx`) with deterministic content-derived root receipt IDs, bounded cleanup, and resume-safe receipt replay. Acceptance benchmarking is still pending.**

**Goal:** make a fresh legacy JSON-index rebuild sustain useful throughput as the SlateDB candidate grows, without weakening duplicate-location preservation, aggregate accuracy, history ordering, transaction atomicity, resumability, or metadata durability. Pack preparation remains parallel, publication amortizes fixed transaction work across bounded multi-pack batches, and fresh-import lookup avoidance keeps a stable false-positive rate for repositories containing hundreds of millions of blob IDs.

## Motivation and measured baseline

The current importer starts multiple pack workers, but `SchemaStore.ImportLegacyPack` admits publication through a one-slot gate. Serialization is required because every pack transaction updates shared aggregate and history keys; unrestricted optimistic transactions repeatedly invalidate one another and can exhaust conflict retries. The worker count therefore overlaps pack `Stat` and Go-side record construction, but it cannot create concurrent successful publications.

Each admitted pack currently pays for a transaction begin, pack and duplicate-blob lookups, aggregate reads and rewrites, history marker and sequence reads, one or more mutation RPCs, and a commit. As the candidate gains SSTs, false-positive duplicate lookups and LSM read/compaction work increase the latency of that single publication lane. More workers cannot compensate, so the producer waits while most host CPUs remain idle.

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
8. Mutation count, encoded bytes, transaction lifetime, queued prepared bytes, and retry work are bounded independently.
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

Whichever limit is reached first closes the batch. A single oversized pack is not committed and returns `ErrLegacyImportBatchTooLarge`; adaptive splitting cannot split one input further. The estimate includes protobuf framing headroom, receipt/update overhead, pack records, aggregates, history, debt, placement, and checkpoint mutations. Record actual values and tighten the estimator if an RPC must split unexpectedly often.

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

This is the completed baseline for Phase 32. The proposed Stage 3 extension has
separate implementation and acceptance gates below.

### Stage 3: Proposed write-side latency hiding

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
change. Keep Stage 3 opt-in until all gates pass:

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

The proposed Stage 3 extension is complete only when its delivery and acceptance
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
