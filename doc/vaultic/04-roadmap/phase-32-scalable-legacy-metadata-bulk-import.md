# Phase 32: Scalable legacy metadata bulk import

[← Back to roadmap index](00-overview.md)

[← Phase 31](phase-31-read-only-nfsv3-snapshot-server.md) · [Phase 33 →](phase-33-operational-monitoring-and-metrics-export.md)

[CLI and operations architecture](../02-architecture/04-cli-and-operations.md) · [Operational monitoring](phase-33-operational-monitoring-and-metrics-export.md)

**Status: design specification, not yet implemented.**

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

Out of scope for the first implementation:

- changing SlateDB or its transaction isolation model;
- allowing arbitrary concurrent legacy pack commits;
- weakening metadata encryption, writer fencing, or schema validation;
- requiring local NVMe, a particular NFS implementation, or a read cache for acceptable behavior;
- changing normal `PublishPack` semantics;
- activating an incomplete candidate.

## Required invariants

1. A committed batch is equivalent to applying its input packs one at a time in deterministic order, except that aggregate update sequences and history sequence allocation are amortized over the batch.
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

Pack preparation timeout and database batch timeout are separate. `--pack-timeout` continues to bound source `Stat` and preparation for one pack. Add `--import-batch-timeout` for one database batch, with a default derived from the existing transaction timeout and an explicit upper bound.

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

Read `aggregateKeys()` once. For each unique pack, calculate the delta from its pre-batch record to its final record, apply all deltas in memory, increment each touched aggregate sequence once, and emit one mutation per touched aggregate key. This removes the principal shared-key conflict and repeated read/write amplification.

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

Whichever limit is reached first closes the batch. A single oversized pack is admitted alone and continues to use multiple mutation RPCs inside one transaction. The estimate includes protobuf framing headroom, pack records, aggregates, history, debt, placement, and checkpoint mutations. Record actual values and tighten the estimator if an RPC must split unexpectedly often.

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

Target an aggregate false-positive probability no worse than 0.1% through the supported import size. Approximately 400 million IDs require about 720 MiB at that target, well below the existing high-memory bulk-import allowances. Tests must use deterministic hashes and tiny layers to exercise rollover and saturation cheaply.

## Checkpoints, retries, and shutdown

For an index that fits in one transaction, commit all packs and its checkpoint together. For an index split across transactions, commit the checkpoint only in the final transaction. If the process stops earlier, the next resumable import replays that index; existing pack and blob merge rules make prior batches idempotent.

Only update process-local progress counters after a successful batch commit. Return the first input position associated with a planning or commit failure so reporting remains actionable. An aborted transaction retries the entire batch from fresh reads. A validation, timeout, cancellation, or permanent storage error aborts the batch and prevents its filter updates.

The existing successful-import marker remains the sole authorization for memory-WAL handoff. Batching must not make a work-budget stop, partial index, finding, or failed checkpoint appear complete.

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

This is the required completion point for Phase 32.

### Stage 3: Optional parallel bulk ingestion

Proceed only if Stage 2 still leaves material resources idle and measurements show commit serialization, rather than backend or compaction throughput, dominates.

Design a fresh-import-only path that removes shared mutable keys from pack transactions:

- write pack/blob/debt/placement records in bounded concurrent transactions;
- write history events with preallocated disjoint sequence ranges or reconstruct import history after ingestion;
- accumulate per-batch aggregate deltas under disjoint keys and reduce them once after all packs commit;
- retain conflict retries for duplicate pack/blob keys;
- rebuild and validate canonical aggregates before marking the import complete.

Start with two concurrent transactions and increase only when conflict, tail-latency, and backend-pressure measurements justify it. This stage must not ship by merely increasing `legacyImportGate` capacity.

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

Phase 32 is complete when:

- the scalable filter stays within its declared memory and false-positive bounds at the largest supported import;
- one-pack and batched imports produce equivalent authoritative metadata and resume safely after every injected interruption;
- the selected default reduces transaction commits by at least 4x on the 20-million-record fixture;
- the 20-million-record fixture improves pack throughput by at least 2x without increased errors or unbounded memory;
- interval throughput no longer declines because the membership filter saturates;
- metrics identify preparation, publication, lookup, WAL, flush, compaction, backend, and checkpoint waiting separately;
- normal `PublishPack`, non-fresh import, activation, and metadata durability semantics remain unchanged.

If Stage 2 cannot meet the throughput target because the single writer remains dominant, Stage 3 becomes required and its aggregate/history rebuild must complete and validate before the candidate can be activated.
