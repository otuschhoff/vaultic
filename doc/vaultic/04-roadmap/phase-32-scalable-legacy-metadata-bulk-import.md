# Phase 32: Scalable legacy metadata bulk import

[Back to roadmap index](00-overview.md) |
[Phase 31](phase-31-read-only-nfsv3-snapshot-server.md) |
[Phase 33](phase-33-index-check-scalability-and-performance.md)

[CLI and operations architecture](../02-architecture/04-cli-and-operations.md) |
[Operational monitoring](phase-34-operational-monitoring-and-metrics-export.md)

**Status:** Stages 1-3, P0-P2, P3a0-P3b, and P7a are complete as of 2026-09-17.
Stage 3 is fresh-reset-only and defaults to two ingestion lanes; deferred cleanup
is opt-in. Representative NFS evidence selects P4 as the next isolated experiment
because eligible work waits during synchronous ordered reduction. The baseline
remains unchanged until P4 satisfies its correctness and performance gates; P5
and P6 are not selected. P3c, P3d, and P7b remain partial pending the complete
response matrix, remote main-store comparison, and current-revision uncapped
acceptance, so repository-scale performance acceptance is not claimed.

**Goal:** sustain legacy-index-to-SlateDB throughput as the candidate grows,
without weakening duplicate preservation, metadata ordering, atomicity,
bounded recovery, or final durability. Normal backup publication is unchanged.

## Conclusions

The evidence supports a **latency-sensitive publication pipeline with two
serialization points**, not a proven CPU, RAM, NFS-bandwidth, or daemon-only
bottleneck:

1. **Vaultic admission/reduction:** `importPacksStage3.reduceReady` performs
   ordered reduction synchronously. Existing ingests continue, but the coordinator
   cannot receive their completions or admit replacements during reduction.
   Dependencies and prepared-byte reservations remain held until reduction ends.
2. **VaulticDB/SlateDB apply:** the pinned SlateDB transaction implementation
   submits commits to one sequential batch-writer task. Deferred commit skips
   explicit durability waiting, but still waits for backpressure admission,
   queue service, conflict checking, WAL append, and memtable application.

Both are established code constraints. Their separate contributions to the
critical path are **not yet measured**. More daemon CPU does not prove daemon
saturation; idle ingest lanes do not prove all missing overlap is recoverable.
Measure eligible-ready work during reduction and engine queue/service time before
choosing between client pipelining and engine optimization.

Cleanup deferral has removed the earlier per-index durability barrier from the
hot path. Preparation and prepared-buffer capacity are not current priorities.
The later decline in throughput still needs read/flush/compaction attribution.
Library changes, including a SlateDB fork, are in scope when measurements identify
the limiting function; replacing the backend is not a prerequisite.

## Required Invariants

- Preserve every physical blob location, including repeated packs and duplicate
  blob IDs within and across indexes. Reject contradictory facts as before.
- Match sequential metadata results for identical batch boundaries. Aggregate
  sequence increments and advisory history ordering remain defined per batch;
  physical adaptive child reductions are distinct from logical batches.
- Replan aborted transactions from current state; publish fresh-filter inserts
  only after commit. False positives may add reads; false negatives must not
  suppress required reads. Take hints only after dependency admission.
- Admit overlapping pack/blob/debt/placement keys in input order. Preserve the
  `A, A+B, B` dependency-fairness regression and reserve progress capacity for
  the earliest unresolved batch. Do not merely enlarge the publication gate.
- Catalog mutations and their content-addressed receipt commit atomically.
  Reduction applies aggregates/history and marks the receipt exactly once;
  mismatched receipt content is an error. Resolve ambiguous acknowledgements
  through receipts before resubmission.
- Checkpoints advance only behind the contiguous reduced prefix. Work/error
  budgets select a pack-boundary prefix before admission. Cancellation stops
  admission and settles in-flight work without checkpointing past a failure.
- Bound transaction items, bytes, lifetime, preparation memory, retry work, and
  recovery state. One indivisible pack may exceed planning targets, never RPC
  limits or validation rules. Preserve the oversized-pack regression.
- Fresh memory-WAL work is destructively restartable, not crash-durable
  resumable work. Partial, failed, interrupted, or finding-bearing imports never
  authorize handoff or activation. Preserve writer fencing and encryption.
- Transaction atomicity and ordered visibility remain mandatory for every
  deferred commit. “Deferred” means the caller skips a persistence wait, not
  that a partial batch may become visible. A resume checkpoint is valid only if
  it durably identifies the input/cursor and fences every preceding ingest,
  reduction and revision write; otherwise restart the private generation.
- The durable successful-import marker alone authorizes handoff. Clean close,
  persistent-WAL reopen, required validation, and authority publication must
  succeed before claiming an activated repository. Normal `PublishPack` and
  non-fresh import semantics remain unchanged.

## Implemented Pipeline

### Vaultic: Selection Through Reduction

| Step | Owning code | Behavior and throughput implications |
|---|---|---|
| Fetch/decode/select | [ForAllIndexes](../../../internal/repository/index/index_parallel.go), [Import](../../../internal/index/legacyimport/import.go) | Fetch/decode uses `Connections()+GOMAXPROCS` workers; a mutex serializes callbacks. Each source drains publication and cleanup before the next callback proceeds. Decode-completion order is not a frozen benchmark manifest. |
| Prepare | [preparePackJobs / dispatchPackJobs](../../../internal/index/legacyimport/import.go) | Parallel pack `Stat` and record construction, ordered collection, bounded byte reservations. More preparers cannot remove downstream serialization. |
| Identify/admit | [importPacksStage3](../../../internal/index/legacyimport/import_stage3.go) | `queueBatch` hashes canonical content and computes dependencies on the coordinator; `admitReady` dispatches independent work. Dependencies remain held through reduction. |
| Ingest | [IngestLegacyPacks / planLegacyIngestBatch](../../../internal/index/daemon/schema_store_import_split.go) | Runs **inside Go Vaultic**, despite the package name `daemon`. Canonicalization, hashes, filter hints, transaction begin, receipt/possibly-present-key reads, merge/encode/sort, mutation RPCs, commit, then filter updates. |
| Reduce | [reduceLegacyImportBatchOnce](../../../internal/index/daemon/schema_store_import_split.go) | Sequential `Begin`, receipt `Get`, aggregate `MultiGet`, history marker/sequence reads, local encoding, mutation RPCs, and commit. `reduceReady` waits for all of this before receiving completions/refilling lanes. |
| Cleanup/finalize | [CompleteLegacyImportSession](../../../internal/index/daemon/schema_store_import_split.go), [completeFreshBulkImport](../../../cmd/vaultic/indexcmd/cmd_index.go) | Validate reduced receipts, delete bounded pages, then eventually mark completion and perform required close/handoff/reopen/activation. Per-index cleanup is not final durability. |

Stage 2 retains `SchemaStore.ImportLegacyPacks(imports, finalCheckpoint)` behind
one publication gate; `ImportLegacyPack` is its one-item wrapper. Its planner
coalesces duplicate keys, reads original states once, updates aggregates and
history once per batch, and commits the checkpoint with the final batch.
Stage 3 separates catalog ingestion from ordered shared-metadata reduction using
private receipts. Debt and placement changes remain dependency-protected.

### VaulticDB: RPC Through SST Files

The dependency is already a fork, pinned in [Cargo.toml](../../../vaulticdb/Cargo.toml)
to `otuschhoff/slatedb` revision `fc68f09a25defb128edfd722ec82696492dbb692`.
Engine function names below refer to the
[pinned engine source](https://github.com/otuschhoff/slatedb/tree/fc68f09a25defb128edfd722ec82696492dbb692/slatedb/src).

| Boundary | Code | What may limit progress |
|---|---|---|
| RPC admission/fencing | [commit_inner / begin_inner](../../../vaulticdb/src/service/transactions.rs), [mutation_admission](../../../vaulticdb/src/service/operations.rs) | Admission takes a shared read guard, not an exclusive one-request gate. Authority/fence checks and lifecycle locks still need timing. |
| Transaction construction | [Storage::begin / write_batch](../../../vaulticdb/src/storage.rs) | Serializable-snapshot creation; `begin` holds the transaction-map write lock across `writer.begin().await`. Mutation RPCs populate a buffered batch under its transaction-slot mutex. Lock contention is a candidate, not a measured bottleneck. |
| Commit submission | [Storage::commit](../../../vaulticdb/src/storage.rs), `DbTransaction::commit_with_options` in `db_transaction.rs` | Fence/registry work, batch clone and conflict-key tracking, then engine submission. Fresh deferred calls **skip** `handle.await_durable()`; client commit latency is not synonymous with a 500 ms WAL flush. |
| Engine admission | `DbInner::write_with_options` / `maybe_apply_backpressure` in `db.rs` | Checks estimated active/immutable memtable plus WAL bytes against `max_unflushed_bytes`, may wait for WAL flush or memtable upload, enqueues `BatchWriterMessage`, then awaits a oneshot result. |
| Sequential apply | `WriteBatchEventHandler::handle` / `DbInner::write_batch` in `batch_write.rs` | One writer loop assigns sequence, checks conflicts, extracts entries, validates segments, appends WAL, inserts memtable entries, and updates transaction/visibility state. Flush messages share this handler. More RPC lanes do not parallelize apply. |
| File production | `memtable_flusher`, `DbInner::stream_imm_ssts` / `stream_imm_sst` in `flush.rs`, `compactor` | Frozen tables are encoded/written through the table/object store; compaction reads and rewrites SSTs. Encryption and local/NFS I/O occur here. Apply acknowledgement does not mean the corresponding SST output is finalized. |
| Final durability | [Storage::close](../../../vaulticdb/src/storage.rs) | Reads completion marker, awaits `db.close()`, records eligible local-WAL handoff only after successful close, then releases writer ownership. Include this tail and required reopen in throughput. |

The path is: legacy JSON -> Go canonical records/plans -> transaction RPC
buffers -> sequential engine apply/WAL/memtable -> SST flush/compaction -> final
close/handoff. Caller RPC timers span multiple owners, not pure server execution.

### Controls and Lookup Avoidance

Fresh defaults: 8 packs, 8,000 planned mutations, 8 MiB transaction target,
256 MiB prepared budget, and 2 lanes. The observed memory-WAL run uses a
500 ms flush interval, 16 GiB unflushed limit, and 256 MiB L0 SST target.
`--pack-timeout` and `--import-batch-timeout` bound different work;
`--batch-size` limits each mutation RPC, not the entire transaction.
`--packs-per-transaction` is capped at 256; preparation reserves a batch's
progress floor. Adaptive splits retain deterministic content-derived identities.

`--import-defer-cleanup` requires fresh reset, memory WAL, and at least two
lanes. It changes bounded receipt-deletion commits only; ordinary cleanup and
the final successful-import marker retain durable acknowledgement.

The former fixed 64 MiB/four-probe filter would reach approximately 81% false
positives at 400M IDs. The implemented geometric layered filter targets <=0.1%,
projects about 1.31 GiB at 400M IDs, and has a 1.5 GiB ceiling with explicit
database fallback. It avoids most reads, but probes/inserts still cost CPU.
Growing it beyond measured needs is not a throughput recommendation.

## Measured Evidence

### Baseline and Controlled Experiment

Stage 2's recorded 20M comparison imported 24,942 packs and 19,999,995 blobs
with no conflicts/retries/errors: 9m28.1s -> 4m32.6s, 35,219 -> 73,422 blobs/s,
and 24,942 -> 4,616 logical commits (2.08x time improvement, 5.4x fewer commits).
A separate clean timing run took 276.893s, including 271.334s publication,
87.520s planning, 31.621s mutation RPC, and 91.925s commit. These budgeted
incomplete runs do not prove final handoff or activation.

The real-daemon `BenchmarkImportStage3Daemon` uses one fixed 128-pack/65,536-blob
index, cold temporary candidates, local object store, memory WAL, and verifies
checkpoint persistence after handoff/reopen. Means include import/finalization,
exclude initial daemon setup, and use three repeats per variant. This small
fixture does not reproduce production encryption, duplicates, NFS, or LSM scale.

| CPUs | Lanes / cleanup | Mean elapsed (SD) | Blobs/s | Cleanup | Finalization |
|---:|---|---:|---:|---:|---:|
| 4 | 1 / Stage 2 | 1,149.8 (1.6) ms | 56,999 | n/a | 597.1 ms |
| 4 | 2 / ordinary | 1,146.5 (1.1) ms | 57,161 | 122.5 ms | 636.1 ms |
| 4 | 2 / deferred | 673.5 (0.7) ms | 97,301 | 2.0 ms | 272.7 ms |
| 4 | 4 / deferred | 674.6 (1.7) ms | 97,155 | 1.7 ms | 337.6 ms |
| 4 | 8 / deferred | 676.0 (1.4) ms | 96,952 | 1.9 ms | 318.2 ms |
| 32 | 1 / Stage 2 | 1,172.3 (4.8) ms | 55,904 | n/a | 547.3 ms |
| 32 | 2 / ordinary | 1,175.7 (11.9) ms | 55,748 | 93.4 ms | 666.2 ms |
| 32 | 2 / deferred | 691.8 (7.0) ms | 94,744 | 2.4 ms | 260.5 ms |
| 32 | 4 / deferred | 695.5 (9.1) ms | 94,250 | 2.4 ms | 372.9 ms |
| 32 | 8 / deferred | 696.9 (2.9) ms | 94,046 | 2.8 ms | 356.6 ms |

Deferral improved the four-CPU two-lane fixture by about 70%; extra lanes/CPUs
did not help. Much of the elapsed difference is in finalization, not cleanup
alone. Do not extrapolate this short result to repository-scale acceptance.

### Full-Run Observations: 2026-09-16

Source: 10,019 indexes, approximately 19.25 GB. Both full runs used 32 preparers,
`local.connections=32`, two lanes, snapshots disabled, metadata encryption,
memory WAL, and disabled shared read-cache tiers. Logged derived cache sizing
is not proof a cache was enabled; distinguish policy from active tiers.

| Run/sample | Result | Interpretation |
|---|---|---|
| Ordinary cleanup, intentionally stopped | 2,025 indexes, 107,076 packs, 85,864,124 blobs, 30m32s, ~46.9k blobs/s, exit 130 | Incomplete baseline, not a matched-prefix experiment. |
| Deferred `8bc9cd7cb`, 09:53:17 UTC | 702 indexes, 29.84M blobs, 6m8s, ~81.2k blobs/s | Early sample, not a sustained full-run speedup. |
| Same run, 10:16:44 UTC | 2,763 indexes (27.6%), 148,116 packs, 116.57M blobs, 29m34s, ~65.7k blobs/s | Rate declined as input/candidate changed; cause is not established. |

At 10:16:29, the coordinator's 1,760 seconds split into ingest wait 744.4s
(42.3%), reduce 575.9s (32.7%), schedule 264.3s (15.0%), dependency wait 106.3s
(6.0%), cleanup 42.7s (2.4%), source 17.1s (1.0%), and prepare wait 9.2s (0.5%).
Two/one/zero ingests were active for 54.2%/27.4%/18.4%, averaging 1.36 lanes.
Active-ingest time includes Go transaction preparation and RPC waits, not just
engine service. Queue snapshots do not establish eligible work throughout reduction.
`ingest_wait` labels a coordinator `select` with active ingests; it can wake on
preparation too. It is not a measured interval spent exclusively waiting for
VaulticDB. `schedule` is elapsed coordinator work, not a pure on-CPU timer.

The same sample reports ingest planning 748.0s, ingest mutation RPC 201.1s,
ingest commit 706.9s, and whole reduction attempts 575.5s. These timers overlap
across lanes and must **not** be added to coordinator wall time. `reduction`
includes its own reads/write/commit; existing `commit` and `mutation_rpc` here
cover ingestion, not all transaction categories. Begin, receipt lookup,
canonicalization/hash/filter, and post-commit work lack complete sub-attribution.

Across 27,305 completed ingests/reductions, this is approximately 27.4 ms ingest
planning, 7.4 ms mutation RPC, 25.9 ms ingest commit, and 21.1 ms complete reduction
per batch, at 15.5 batches/s. These are means, not tail latency or independent
wall-time fractions. The reducer is not continuously busy, so its existence
alone does not prove that it sets the throughput ceiling.

Other evidence and corrections:

- Short per-process samples measured Vaultic at 0.47 cores and VaulticDB at
  1.16; separate combined samples ranged from 1.64 to 2.61. More active CPU in
  VaulticDB does not assign it 71% of wall latency. `ps` elapsed/%CPU was anomalous;
  use interval counters. The earlier "mostly VaulticDB" conclusion was too strong.
- The 09:48:41 Go profile contains 15 CPU-seconds over 15 seconds.
  `planLegacyIngestBatch` is 20.5% cumulative; flat costs include filter probes
  5.5%, inserts 4.9%, SHA-256 5.3%, and `memmove` 6.3%, plus sorting, marshaling,
  and allocation. These are **Vaultic**, not Rust planner costs.
- Earlier/synthetic Rust profiles show skip-list/memtable work, copies,
  allocation, SST codecs, and encryption. The full-run DWARF report did not
  finish resolving; retain the raw capture without claiming a completed Rust
  critical-path profile. CPU profiles do not attribute off-CPU waits.
- At ~116M blobs there were zero reported conflicts/retries, filter fallback
  was false, filter storage was 286 MB with estimated false positives ~0.09%,
  and cumulative Go stop-the-world pause was ~448 ms. Heap was ~1.1 GiB and
  prepared bytes peaked at 13.57 MB against 256 MiB. Queue capacity and GC
  pauses are not primary limits; this does not rule out GC CPU/allocation cost.
- Affinity is 32 CPUs with no observed ancestor quota/throttling or memory cap.
  `/proc/meminfo` showed ~293 GiB total while the import's detector reported
  ~755 GiB. Reconcile container/host views before sizing defaults. Spare RAM
  alone does not justify larger buffers or caches.
- **Storage correction:** `/volume2/NASDA2/rustic/db` resolves to
  `/ncl1-1-vs-50/fme_dump/amakura/db`. `du -sh` measured the symlink; `du -shL`
  measured ~35 GiB at 10:17, and daemon descriptors pointed to real compacted
  SSTs there. The earlier 4 KiB observation was not missing SST output.
- The first `nfsiostat` report is cumulative. Quiet short intervals, especially
  on the wrong NASDA2 mount, cannot exclude NFS latency or bursty flush/compaction.
  Zero dirty/writeback pages and process I/O totals likewise do not establish
  storage headroom or physical network bytes. Measure the resolved mount and
  object-store/engine counters over matching intervals.

## Implementation Recommendations

### 1. Attribute the Critical Path

Add bounded histograms/counters by ingest/reduce/cleanup operation, without raw
IDs as metric labels. Correlate sampled traces by logical/physical batch and
transaction; separate local CPU work, RPC elapsed, queue waits, and durability.

| Point | Measurement and decision enabled |
|---|---|
| Vaultic scheduler | Eligible-ready time while reducer is busy, completion-receive delay, unreduced bytes and oldest ordinal age. Distinguishes refill starvation from true dependencies. |
| Go ingest/reducer | Begin, receipt/pack/blob/aggregate/history reads, local encode/hash/filter, mutation RPC, commit, post-commit, retries; p50/p95/p99 plus key/RPC counts. Separates client work from remote latency. |
| VaulticDB service/storage | Shared admission, fencing/coordination reads, transaction-map/slot lock waits, begin, mutation buffering, commit submission/response, explicit durability wait. Tests service/fencing contention. |
| SlateDB apply | Backpressure duration and WAL/active/immutable bytes; enqueue-to-dequeue delay; writer service split into conflict tracking, entry extraction, WAL append, memtable insertion, freeze/flush messages. Distinguishes an idle from a saturated writer. |
| SST output | Flush backlog/age, L0/compaction pressure, SST bytes/read amplification, encode/encrypt/upload time, object-store request latency/concurrency, cache hits/misses and resolved paths. Locates file-production stalls. |
| Finalization | Marker acknowledgement, close/flush, handoff, reopen, validation/activation and total elapsed. Prevents moving costs out of the measured import window. |

Reuse existing SlateDB statistics, supplementing the fork at these boundaries
where necessary. Keep periodic reporting alive when progress stalls. Never infer
durability latency solely from the configured flush interval.

Use the [Phase 34 wait-state contract](phase-34-operational-monitoring-and-metrics-export.md#wait-state-accounting-contract)
for shared names, bounded histograms/active wait ages and outcome semantics.
Observe operation/worker states, not a new process scheduler. Separate lock
acquisition from hold time, client RPC from server queue/service, and inclusive
spans from exclusive phases. Close submit timers before explicit durability
waits; do not assume marking a drop-based timer successful ends it. Concurrent
wait totals are worker-time, not additive job wall-time. Correlate sampled
dependencies without raw IDs as metric labels. Profiles remain necessary for
CPU/runtime scheduling time not explained by explicit waits.

P2/P3 deliver the minimum Phase 34 M0-M2 foundation and first write-heavy
experiments, without waiting for monitor commands or exporters. These additions
remain pending; completed P0/P1 evidence does not certify the new contract.

### 2. Test a Bounded Asynchronous Reducer

Use one ordered reducer worker while the coordinator receives completions and
admits independent ready work. Keep dependency ownership/accounting in the
coordinator; release dependencies/bytes only after successful reduction acknowledgement.
Keep one owner for aggregates/history/checkpoints. Bound queued/active reduction
bytes, receipt count, and distance ahead of the contiguous prefix; reserve
capacity for the earliest unresolved batch. Cancellation joins all workers and
preserves the earliest meaningful failure.

Accept only if eligible work waits during reduction and the experiment improves
end-to-end throughput without growing engine queues unboundedly. This cannot
remove sequential apply cost or safely parallelize shared metadata. Do not
predict a 33% gain from the reduce bucket: ingestion already overlaps some of it.

### 3. Reduce Proven Per-Batch Overhead

- **Vaultic:** `uniqueLegacyImportIDs` is recomputed across hints/planning and
  twice after fresh ingestion (filter updates and metrics). Reuse immutable
  prepared IDs/canonical representations when safe; preserve byte-identical
  hashes, duplicate merges, fresh-read retries, and memory bounds. Measure
  allocation/CPU savings before changing the filter or hashing algorithm.
- **Reducer RPCs:** if round trips dominate, test combined known-key reads or
  bounded contiguous-receipt reduction. Preserve per-physical-batch aggregate
  sequence increments, advisory history order, receipt idempotence, and checkpoint
  atomicity. Fewer transactions are not automatically equivalent semantics.
- **Service locks:** if transaction-map contention is material, narrow
  `Storage::begin`'s write-lock lifetime using reserved admission slots; preserve
  expiry, maximum active transactions, and drain/demotion races. Do not cache or
  remove fencing checks without an equivalent ownership proof.

### 4. Optimize Imported Libraries Where Justified

The existing SlateDB fork is the preferred home for engine changes; retain a
pinned revision and separate instrumentation, optimization, and dependency-bump
commits. Propose upstreamable changes instead of duplicating an engine in VaulticDB.

| Evidence needed | Targeted fork/library experiment | Required safety gate |
|---|---|---|
| Writer queue stays occupied; entry extraction dominates | Move sequence-independent validation/canonicalization outside `DbInner::write_batch`, or reuse prepared entries; keep sequence/time/TTL-dependent work ordered. | Identical validation/errors, transaction conflicts, merge/TTL/segment semantics and atomic visibility; no stale precomputation after retries. |
| Transaction cloning/allocation dominates | Reduce copies in `DbTransaction::commit_with_options` and batch extraction using ownership transfer or immutable sharing. | Snapshot/read-your-write behavior, iterators, rollback/cancellation, and transaction lifetime tests; no aliasing mutations. |
| Memtable insertion dominates the single writer | Benchmark a batch insertion path or a cache-efficient memtable alternative against the current skip-list. | MVCC sequence visibility, concurrent readers, sorted scans, retention, memory bounds, and replay equivalence. Do not parallelize commit visibility casually. |
| WAL append/flush messages monopolize writer service | Measure current coalescing, then test bounded append grouping or separating expensive preparation from ordered append. | Per-transaction atomic WAL records, conflict/order semantics, bounded acknowledgement latency, crash replay, fencing, and durable-handle correctness. Deferred commit is not permission to lose required durability. |
| Flush/compaction backlog or encode/encrypt/upload dominates | Tune existing flusher/compactor concurrency or optimize measured codec/object-store work; parallelize independent SST preparation where supported. | Bounded in-flight bytes, manifest publication ordering, authenticated-encryption integrity, recovery, and no foreground starvation. |

Do not begin with parallel apply, new group commit, a second flush scheduler,
or a backend replacement. First establish which writer substage is occupied
and whether more submission concurrency improves its utilization. An engine
with one ordered commit point can still benefit from parallel preparation;
that is different from making transaction visibility concurrent.
The pinned fork already exposes `l0_flush_parallelism` in its memtable uploader;
measure existing settings and utilization before inventing another uploader pool.

### 5. Keep Resource Defaults Conservative

Retain two lanes and the prepared budget until repository-sized tests show
otherwise. Do not generalize the tiny fixture's four-CPU result into production
affinity changes. Compare 1/2/4/8 lanes and CPU limits independently with fixed
input/encryption/backend; stop at a plateau or rising latency/backlog. Test cache
and RAM after observing misses or byte-budget/backpressure stalls. More unflushed
RAM absorbs bursts but cannot raise steady-state disk or sequential-writer
capacity, and may worsen shutdown/recovery. Never trade fencing, isolation,
encryption, or durability for benchmark speed.

## Phased Execution Plan

This is the next implementation sequence, not a claim that the work is complete.
Use P0-P7 to distinguish it from the already implemented Stages 1-3. All phases
start pending. P0-P3 establish evidence; P4-P6 are conditional experiments, not
a checklist of optimizations that must all ship. P7 validates the accepted result.

### Execution Contract

- Execute one phase, or one explicitly named substep, per coding session. Read
  the current worktree and relevant tests first; preserve unrelated edits.
- Before editing, state the local hypothesis, owning function, smallest change,
  and a check that can disprove it. Run that check immediately after the edit;
  repair the same slice before expanding scope.
- Reuse neighboring tests, telemetry helpers, and benchmarks. Keep normal backup,
  non-fresh import, schema/receipt formats, durability, and defaults unchanged
  unless a phase explicitly requires a reviewed change.
- Do not stop/reset/activate a live repository, overwrite its binaries, or run
  destructive experiments without explicit authorization. Use isolated candidate
  directories, sockets, and artifact prefixes. Never record credentials.
- For dependency work, use an owned checkout of the pinned fork, not Cargo's
  shared checkout cache. Keep instrumentation, optimization, and dependency-pin
  changes independently reviewable; do not commit or publish without authorization.
- A failing correctness check blocks promotion. A performance loss or ambiguous
  result blocks default changes; retain the baseline and record the finding.
  Roll back only the experiment's own changes, never unrelated work.

### P0. Freeze the Reproduction Contract

**Status:** complete for the isolated synthetic baseline as of 2026-09-16.
Repository-scale acceptance remains blocked on an authorized representative
candidate; the active production import is not a benchmark resource.

**Prerequisite:** current committed revisions and existing import/daemon fixtures.
Inspect `BenchmarkImportStage3Daemon`, source callback ordering, and run settings.
Record input identity/order, encryption, storage/WAL/cache settings, resource
limits, candidate reset policy, and completion criteria. The one-index fixture
is sufficient for smoke tests, not repository-scale acceptance.

For comparative large runs, first add the smallest test-only deterministic
source/selection adapter to an existing harness if one is missing. Verify the
same ordered index/pack manifest and selected counts across repeated traversal;
do not use decode-completion order or record budget alone as input identity.
**Exit:** reproducible isolated baseline command, input digest, and safety scope.
If representative input is unavailable, report that blocker rather than infer
large-repository gains from the small fixture.

The benchmark now constructs a test-only, preselected four-index source rather
than relying on map iteration, parallel decode-completion order, or a record
budget. An opt-in importer path decodes in parallel but invokes callbacks in
memorized list order with retained decoded indexes bounded by worker count. A
gated-first-index test forces a later load to complete first and verifies that
it still cannot overtake callback execution. The
canonical digest covers each ordered index identity and its exact encoded bytes,
plus sorted pack and blob identities. A focused test pins the smaller fixture at
`8131bd970bae45469fd851568b0970618e66eea1c2d2c49620702281618493fc`
and verifies repeated construction, list order, selected counts, and two complete
ordered dry-run traversals. The real-daemon fixture pins and enforces this contract:

| Field | Frozen value |
|---|---|
| Vaultic baseline parent | `c7211177ee5d6f9c3223c344cff6f6b9e61358db`; the P0 revision is the commit containing this record |
| SlateDB fork revision | `5faf4b086b043c65afdf193a7e2f87a737a11205` |
| Input | 4 indexes, 128 packs, 65,536 blobs; preselected with work budget disabled |
| Input SHA-256 | `f2b658363adcb6760a1fa9fd411fa60a4a9de0a0d2ff15d6ed961efeb9b9e943` |
| Validated daemon SHA-256 | `531d14a54ca983c10366ebd8759d20390deef437eac93eb93a7b07dc16b2e4c4` |
| Resources | `GOMAXPROCS=4`, `GOMEMLIMIT=8GiB`; no process-affinity claim |
| Storage | isolated temporary local object store; read-cache tiers explicitly cleared and configured cache zero; 16 GiB max unflushed; 256 MiB L0 SST |
| WAL | memory/local-process during import, 500 ms flush setting, then local reopen |
| Encryption | disabled in the validated synthetic daemon handshake; production encrypted runs remain separate evidence |
| Variants | 1 lane ordinary; 2 lanes ordinary; 2, 4, and 8 lanes with deferred cleanup |
| Reset and safety | fresh candidate reset in a new temporary directory for every iteration; no live repository paths or sockets |
| Completion | mark complete, close, WAL handoff, reopen, and verify all four import checkpoints |

Run the focused contract test and one complete smoke traversal with:

```console
go test ./internal/index/legacyimport \
  -run '^TestFrozenStage3BenchmarkFixtureReproducible$' -count=1
GOMAXPROCS=4 GOMEMLIMIT=8GiB \
  VAULTICDB_TEST_BINARY="$PWD/bin/profile/linux-amd64/vaulticdb" \
  go test -v ./internal/index/legacyimport -run '^$' \
  -bench '^BenchmarkImportStage3Daemon$' -benchtime=1x -count=1
```

For comparison evidence, use `-benchtime=3x` and retain the full verbose output;
each sub-benchmark iteration gets a newly reset candidate that is removed before
the next iteration. The benchmark reports import-only and end-to-end completion
rates separately, alongside the scheduler snapshot, cleanup mode, allocations,
and finalization time. Input/daemon digest, Go resource, effective encryption,
WAL, daemon-limit, or selected-count mismatches fail the run; any missing
checkpoint fails after reopen. The P0 smoke passed all five variants against the
daemon above. Its rates are fixture validation, not a repository-scale
performance claim.

### P1. Complete Vaultic Attribution

**Status:** complete as of 2026-09-16.

**Prerequisite:** P0. Touch the importer scheduler/metrics and Go `SchemaStore`
ingest/reduce metrics only. First add phase/operation counters and bounded
histograms; then add tests. Measure the gaps listed in recommendation 1, especially
begin/receipt reads, reducer suboperations, and completion-to-receive latency.
Choose a concurrency-safe way to observe eligible-ready time during reduction
without changing admission behavior or treating occasional queue samples as a
time-weighted measurement.

**Check:** `go test -race ./internal/index/legacyimport ./cmd/vaultic/indexcmd
-run 'Stage3|SchedulerTelemetry|ImportStats' -count=1` and focused daemon metric
tests. Cover timer accounting, snapshot independence, concurrent reporting,
failure/cancellation, and reporter shutdown. **Exit:** documented timer scopes,
unchanged functional results, and measured instrumentation overhead on P0.

P1 uses fixed 64-bucket, power-of-two nanosecond distributions. Reported p50,
p95, and p99 values are conservative bucket upper bounds (`p95<=...`), not exact
samples. SchemaStore histograms lock only while adding or copying 64 counters so
concurrent snapshots are internally consistent. Scheduler distributions share
the existing scheduler telemetry lock. Both snapshots return independent maps;
operation names are fixed in code and cannot create input-dependent cardinality.

| Scope | Counters and timer boundaries |
|---|---|
| Scheduler | Coordinator phase and time-weighted active-lane time; ready and pending-reduction batches; retained and unreduced prepared bytes; oldest unreduced ordinal age. |
| Scheduler completion | Ingest-return to coordinator-receive latency for every outcome. This includes synchronous reduction delay but is not labeled RPC time. |
| Scheduler reduction | Time spent synchronously draining reducible outcomes. Eligible-ready-during-reduce records only the overlap after an ingest completion when a lane could refill with dependency-fair independent ready work; completed dependencies remain retained until reduction. |
| Ingest work | Input preparation/canonicalization, content hashing, fresh-filter hints, plan building after catalog reads (including a possible debt read), and post-commit filter/counter publication. |
| Ingest transaction | Ingest begin, receipt read, pack reads, blob reads, mutation RPC, commit, ingest recovery receipt read, and ingest retry backoff. Attempts and terminal failures are split from reduction. |
| Reducer transaction | Reducer begin, receipt read, aggregate planning (reads, delta computation, and encoding), history planning (marker/sequence reads and local work), final mutation encoding, mutation RPC, commit, reducer recovery read, checkpoint verification read, and reducer retry backoff. |
| Request volume | Primary receipt reads, recovery reads, checkpoint verification reads, attempted catalog and reducer-planning read RPCs/keys, attempted and successful ingest/reducer mutation RPCs, committed ingest mutations/bytes, and committed reducer mutations. Planned keys remain separately available as `PlanningReads`. |

The deterministic blocked-reducer test admits both ingest lanes, waits for
independent ready work, completes the second ingest while the first reduction is
blocked, and verifies completion-to-receive, eligible-ready overlap, reduction
blocking, unreduced bytes, and oldest age. Additional tests cover concurrent
observations/snapshots, map independence, normal Stage 2/Stage 3 equivalence,
idempotency conflict and missing-receipt failure counts, pre-admission validation
and cancellation failures, failed mutation RPC attempts, failed/stopped scheduler
refill exclusion, deterministic log formatting, repeated reporting without
progress, and idempotent reporter stop.

Instrumentation overhead was rechecked after the final P1 changes with the P0
command at `-benchtime=3x`
against parent `236330f4403c0b89fc136f5a65e2fa17a56d1b7c`, using the same fixture
and daemon digests. Ordinary variants changed end-to-end rate by +0.92% (one
lane) and +0.45% (two lanes); allocation counts changed by less than 0.02%.
Deferred variants changed import-only rate by +5.9% to +12.2% and remained
dominated by finalization variance, including one slow P0 two-lane sample. There
was no instrumentation regression. These three-iteration aggregate checks bound
obvious overhead; they are not P3 performance evidence.
The paired raw summary rows, identities, calculations, and source-output hashes
are retained in [Phase 32 P1 instrumentation evidence](phase-32-p1-benchmark-evidence.md).

### P2. Complete VaulticDB and Engine Attribution

**Status:** complete as of 2026-09-16.

**Prerequisite:** P0; align operation names with P1. Split this into P2a service
admission/fencing/transaction timers, P2b fork queue/apply/backpressure timers,
and P2c SST/compaction/finalization counters. Validate each substep before the next.
Reuse engine statistics and expose bounded snapshots through an existing
diagnostic path; avoid a new RPC/protocol unless existing paths are insufficient.

Execute the following substeps independently under the execution contract. Freeze
the shared schema through Phase 34 M0 and implement only the missing M1 primitives
at their existing owners; do not duplicate the Go/Rust accounting libraries.

| Substep | Owning boundary and bounded change | Focused gate and handoff |
|---|---|---|
| P2a: service and storage waits | Existing `vaulticdb/src/attribution.rs`, service admission/fencing and `Storage::begin`, `write_batch`, `commit`. Add active wait ages, contention/hold timing where supported and explicit outcomes; correct overlapping submit/durability scopes before comparing durations. | Controlled lock/admission and durable-handle stalls distinguish acquisition, hold, submit and durability. Immediate acquisition, failed/cancelled/timed-out requests and dropped futures settle counters. Deferred commits record no explicit durable wait. Publish exact timer boundaries and focused Rust/Go daemon test results. |
| P2b: engine queue and apply | Inspect statistics in the pinned fork first. Add only missing enqueue/dequeue, writer-service and backpressure observations, separating WAL/memtable pressure. Keep fork instrumentation and dependency pin changes separately reviewable. | Controlled queue/service/backpressure stalls attribute to the correct bucket; transaction conflicts, ordered visibility and replay are unchanged. Publish fork/build IDs, bounded snapshots and relevant fork test commands/results. |
| P2c: object-store output and finalization | Existing storage object-store/WAL wrappers, flush/compaction statistics and marker/close/handoff/reopen owners. Separate WAL, SST/manifest and coordination attempts, stream transfer, retry delay and background pressure. | Streaming/multipart and flush stalls remain visible until completion; cancellation clears active accounting. Marker acknowledgement, close, reopen and checkpoints retain correctness. Publish role/coverage map and enabled/disabled instrumentation overhead on the frozen fixture. |

**Check:** discover and run the pinned fork's relevant transaction, flush,
backpressure, and fencing tests. Add focused tests proving queue wait is distinct
from service time, failed attempts are counted, deferred durability is not counted
as an explicit durable wait, and instrumentation creates no await-under-lock or
blocking log path. Build optimized symbols and run the Go real-daemon smoke fixture
with `VAULTICDB_TEST_BINARY` pointing to that exact binary.
**Exit:** named client/server/engine boundaries and matching build IDs; no
behavioral changes or unbounded metric cardinality.

P2 extends the existing `WriterStatus` diagnostic response with fixed fields;
it does not add an RPC or derive metric names or labels from requests. Each
timing snapshot reports saturating attempts, completed outcomes, total and
maximum microseconds, eight bounded latency buckets, active work, and oldest
active age. A small synchronous mutex protects only active-start bookkeeping;
no collector lock spans an await. Completed `Err` results are failures, while
dropped guards are cancellations. Service boundaries include admission-lock
wait and hold, writer-fence validation, and separate write-batch, begin, commit,
and rollback request timers.

| Boundary | Exact scope |
|---|---|
| `admission_wait` | Starts before the mutation-admission read-lock await and ends immediately after acquisition. Drain checks and the remaining request are excluded. |
| `fence_check` | Starts before loading storage and reading active writer authority; ends only after authority is confirmed. Stale or unavailable authority is a failed attempt. |
| `*_request` | One fixed timer for each write-batch, begin, commit, and rollback handler, including validation and all subordinate work. |
| `transaction_begin` | SlateDB transaction creation, including expiry pruning, but not service admission or fencing. |
| `engine_submit` | Starts before the VaulticDB storage failpoint and ends when SlateDB returns a write handle. It includes SlateDB backpressure, batch-writer enqueue/queue wait, conflict checking, and in-memory apply; it excludes explicit durability wait. Write-batch and transaction-commit submits share this storage metric and remain distinguishable through their service timers. |
| `durable_wait` | Only `WriteHandle::await_durable`; deferred transaction commits do not create an attempt. |
| `finalization` | The complete storage close path through flush/close, local-WAL handoff eligibility, cache/credential cleanup, and writer-claim release. |

The pinned fork revision `fc68f09a25defb128edfd722ec82696492dbb692`
adds fixed engine lifecycle metrics. Backpressure begins only when pressure is
first observed and ends before enqueue. Queue wait starts after backpressure at
enqueue and ends when the batch writer receives or drains the request. Writer
service starts at receipt and ends when apply returns. Each stage reports bounded
duration buckets, attempts/completed outcomes, active count where meaningful,
and oldest active age where the fork can retain a live guard. Existing counters
cover writes, pressure reasons, memtable/WAL bytes, flushes, L0/SST/sorted-run
state, and compaction output/running work. One recorder snapshot supplies all
engine fields in a status response; counters are cumulative for the `Storage`
lifetime and gauges describe the latest recorder state.

Role-aware object-store metrics separate main metadata, WAL, and coordination
operations. They cover fixed PUT, multipart, GET/body/ranges, HEAD, delete, list,
copy, and rename operations; byte availability is explicit per operation and
stream timers remain active until EOF, error, or cancellation. A shared store
uses path tagging to distinguish WAL. The current object-store interface does
not expose internal retry delay, background pressure, or timeout classification,
so those availability flags remain false rather than reporting inferred values.

The process test holds a mutation after admission and observes that
`admission_wait` has completed while `write_batch_request` has not, then proves
the completed request advances engine submit, durability, and fork write
counters. A second daemon takeover proves a stale owner records failed fence and
request outcomes. Storage tests cover failed begin/finalization/submit,
deferred-durability exclusion, and real recorder-backed writes. Collector and
object-store tests cover explicit failure versus cancellation, active cleanup,
bounded snapshots, streams, multipart, path-tagged WAL, and role isolation. The
matching optimized build, fork tests, full suite results, overhead comparison,
identities, and lint baseline are retained in
[Phase 32 P2 attribution evidence](phase-32-p2-attribution-evidence.md).

### P3. Measure and Select One Experiment

**Prerequisite:** P0-P2. Run identical isolated input at least three times,
including a candidate large enough to flush/compact. Capture unprofiled throughput
separately from short profiles. Report queue occupancy, service/wait distributions,
eligible work during reduction, CPU/RSS, file-output counters, and finalization.

Use the [shared backend profiles and artifact contract](phase-34-operational-monitoring-and-metrics-export.md#backend-scenarios-and-experiment-contract)
for HDD-array NFS, native three-replica RADOS and cloud S3 with an explicitly
assumed 8 ms network RTT unless an operation-specific measurement replaces it.
Map source indexes/pack metadata, repository packs, SST/manifest, WAL,
coordination and caches to physical resources; share capacity when they share
an array, pool or link. Unknown hardware parameters remain unknown.

Execute one substep per session; reuse the existing real-daemon/import harness
and Phase 34 M2 wrappers, never create a workload-specific injection framework.

| Substep | Prerequisite and smallest experiment | Gate and artifact |
|---|---|---|
| P3a0: freeze import durability | Phase 34 M0a-M0c and M2g. Document current fresh memory-WAL restart-from-zero, any persistent-WAL resume mode, checkpoint cursor/input identity and final activation guarantees. Prove whether a durable-through token covers an ordered prefix; do not infer it from a later acknowledgement or `last_durable_sequence`. | Barrier/failpoint crashes before/after apply, checkpoint fence, completion marker, close/handoff and reopen either resume from a fully covered prefix or reject/discard the candidate. Missing SSTs recover from a proven durable WAL; memory-WAL loss never claims resume. No default changes before this gate. |
| P3a: baseline and harness | P0-P2 and Phase 34 M0-M2 minimum contracts/wrappers. Freeze profile placement, ordered input and delay boundaries. Run no-injection enabled/disabled telemetry baselines. | Same input/result/checkpoints and bounded resources; quantify overhead. Profile validation, test-target gating, stream/multipart, conditional-write and cancellation checks pass before latency sweeps. Publish exact commands, binary IDs, seed and settings. |
| P3b: WAL sensitivity pilot | P3a. Use a bounded durable-commit workload with explicit durable acknowledgements in an isolated candidate. Vary only added WAL PUT latency, initially 0/10/50/200 ms; hold flush cadence, batching and concurrency fixed. | Test the hypothesis that delay increases durability wait first, with lock/admission waits downstream only when shared resources remain held. Report submit separately, commit p95/p99, batch size, flush/queue growth and throughput. Unexpected submit growth returns to P2b backpressure attribution, not an assumed WAL explanation. |
| P3c: import role sweeps | P3a and validated P3b instrumentation. Run frozen fresh imports with memory WAL, then a separately labeled supported durable-WAL import configuration. Vary source operations, SST/manifest and coordination latency independently; include pack-storage operations only where the workload issues them. | Never apply WAL-PUT conclusions to memory-WAL deferred commits. Preserve each mode's recovery/acknowledgement contract. Include successful marker, close/handoff/reopen and validation tail, and enough data/time to expose flush/compaction. Record inactive roles as not applicable. |
| P3d: combined scenarios and decision | P3c. Apply all three profiles, cold/warm cache states, independent jitter followed by correlated stalls and shared-capacity contention. Use at least three repeats per comparison. | Unchanged logical metadata, durability and bounded queues; report variance, active wait ages and injected versus observed delays. Rank candidate improvements by end-to-end sensitivity. Separate synthetic from hardware evidence; missing representative infrastructure blocks that acceptance claim, not isolated correctness work. |

**P3a0 status:** complete as of 2026-09-16 for the supported fresh-import
restart-from-zero contract. Fresh memory-WAL checkpoints and the completion
marker are applied-state fences, not process-crash resume points. A successful
`Storage::close` flush, WAL handoff, and completed-shape reopen are all required
before activation. SIGKILL before close discards the candidate on reset and
cannot reopen in completed mode. A failpoint after the close flush but before
handoff likewise leaves the memory binding in place and rejects inherited-WAL
reopen. `last_durable_sequence` is a process-local operation count, not a SlateDB
sequence or durable-through token. SlateDB's ordered-prefix `WriteHandle` token
is internal and, with memory WAL, does not establish process-crash durability.
The contract trace, crash matrix, focused tests, and residual limits are retained
in [Phase 32 P3a0 durability evidence](phase-32-p3a0-durability-evidence.md).

**P3a-P3b status:** validated on isolated resources as of 2026-09-17. The
persistent-WAL 0/10/50/200 ms PUT sweep reaches durability wait at approximately
twice the injected delay because each transaction issues two WAL PUTs. The
matched post-success response sweep changes client latency without changing
server commit duration, and timeout recovery remains idempotent. **P3c status:**
partial. Independent memory-WAL source, main-store and coordination pilots place
the observable fixture sensitivity in mandatory finalization, not sustained
ingest/reducer or engine queue/service work. A supported durable-WAL import and
enough scale to expose flush/compaction were not available. P3d's representative
combined profiles remain blocked on authorized NFS, RADOS and S3 resources.
Commands, measurements, build identity, and limitations are retained in
[Phase 32 P3-P7 evidence](phase-32-p3-p7-evidence.md).

Before revisiting an optimization, complete the remaining P3c response substep
using [Phase 34's dependency-response contract](phase-34-operational-monitoring-and-metrics-export.md#synthetic-dependency-responses)
and M2d/M2e. It extends P3c, not completed P2 evidence. Run it separately
from storage-latency injection so client confirmation sensitivity is not mistaken
for slower SlateDB persistence.

| Substep | Prerequisite and experiment | Gate and handoff |
|---|---|---|
| P3b-response: delayed durable confirmation | P3a, P3b and M2d. Keep real isolated WAL/storage fast and fixed; after successful durable commit, delay only the selected client's commit response by 0/1/8/25/100/250 ms. | Verify durability before withholding with a barrier. Report client commit wait, server submit/durability, eligible idle lanes, end-to-end throughput, queues and memory. Server duration must not include the client delivery delay. Publish comparison with the separate WAL-PUT sweep. |
| P3b-recovery: success followed by timeout | P3b-response and M2e. Let selected responses exceed the caller deadline or be lost after committed success; reuse existing receipt/idempotency recovery with unchanged identities. | No duplicate receipts/reduction/publication or lost committed state; checkpoints and final reopen remain correct. Separate timeout-limited runs from successful throughput evidence and record actual retries/final state. |
| P3c-response: import dependency matrix | P3c and preceding response gates. Delay begin/read/mutation/commit responses one method at a time; compare constant delay with equal-mean jitter, then correlated tails. Label fresh deferred commits as apply acknowledgements, not durable confirmations. | Preserve input order, mode-specific recovery and full finalization. Vary batching/lanes separately; publish ingest/reducer sensitivity and p95/p99 with three repeats. Pack stat/read evidence does not certify backup pack-upload behavior; that uses Phase 34 M2f. |

**Decision:** choose P4 if eligible work is blocked by synchronous reduction;
choose one P5 substep if local planning, reducer RPCs, or service locks dominate;
choose one P6 substep if the engine/output path is demonstrably limiting. Multiple
causes may exist, but change one at a time and rerun P3 after each accepted change.
Representative SSD/HDD NFS samples record 7m49s/9m36s of eligible-ready wait
during reduction and 20m41s/22m11s of reduction blocking at matched 45-minute
boundaries. This satisfies P4's prerequisite and selects it as the next isolated
experiment. It does not establish benefit: retain the existing two-lane,
opt-in-deferred baseline unless P4 passes its checks and improves the matched
end-to-end result. Current evidence does not select P5 or P6.
**Exit:** a cited artifact and falsifiable expected improvement for the selected
experiment. If attribution is inconclusive, return to the missing P1/P2 timer;
do not proceed by increasing lanes, memory, or disabling safety checks.

### P4. Decouple Ordered Reduction

**Prerequisite:** P3 evidence of eligible work waiting during reduction. Start
in `importPacksStage3` and its existing tests. P4a adds a deterministic blocked-
reducer test proving the current refill gap. P4b introduces one bounded reducer
worker and coordinator-owned acknowledgement processing, then reruns that test.
P4c covers cancellation/failure/limits before any performance run.

**Checks:** prove independent ingests can refill while reduction is blocked;
overlapping keys cannot overtake; checkpoints, counters, and dependency/byte
release occur only after the correct reduction acknowledgement. Exercise `A,
A+B, B`, delayed earliest ingest, adaptive child receipts, failed reducer, a full
handoff channel, and cancellation with both worker types active. Run the complete
legacyimport suite under `-race` and real-daemon equivalence/handoff tests.
**Exit:** bounded worker lifecycle and identical ordered metadata, followed by
P3 comparison. Stop if only backlog grows or the engine bottleneck is unchanged.

### P5. Remove One Measured Client/RPC Cost

**Prerequisite:** a specific P3 profile/timer. Select only one: P5a immutable
ID/canonical-data reuse in Go, P5b reducer read/transaction amortization, or P5c
`Storage::begin` lock-scope reduction. Follow recommendation 3's safety constraints.
Do not combine this phase with P4 or change a public format for convenience.

**Checks:** P5a compares exact hashes/mutations and allocation counts, including
duplicates/retries; P5b compares aggregate sequences/history/checkpoints and
ambiguous-reply replay; P5c tests expiry, admission bounds, drain/demotion, and
concurrent begin/cancel. **Exit:** the selected cost falls without correctness or
memory regression; rerun P3 to establish whether total throughput improves.

### P6. Optimize One Fork/Library Boundary

**Prerequisite:** P3 evidence identifying a library function and its queue/service
or output limit. Select one row from recommendation 4. P6a adds a deterministic
engine-level reproducer and baseline benchmark. P6b makes the smallest ownership,
preparation, batching, or concurrency change and runs the row's safety tests.
P6c pins the tested revision in VaulticDB and reruns real-daemon integration.

Never infer that the single-writer design must be removed. Retain ordered conflict
checking, sequence assignment, atomic visibility, and WAL/manifest guarantees.
**Exit:** reproducible fork commit/diff and dependency pin, engine regression
results, and P3 end-to-end evidence. A faster engine microbenchmark with slower
encrypted import or worse finalization is not an accepted optimization.

### P7. Integrate and Establish Acceptance

**Prerequisite:** one or more independently validated experiments, or a documented
decision to keep the baseline. Run affected Go package suites and race checks,
real-daemon crash/replay/handoff tests, and changed Rust/fork suites. Account for
known unrelated failures explicitly; never label a failing broad suite as passed.

Repeat the frozen Stage 2/Stage 3 comparison and the full uncapped import through
the required close/reopen/validation path using authorized isolated resources.
Tune one resource dimension at a time only if P3 warrants it. Promote a default
only after the acceptance criteria below pass; otherwise retain the baseline or
experimental opt-in. **Exit:** updated evidence table, exact commands/revisions,
remaining limitations, and a clear completed/partial/failed run status.

Complete P7a correctness/crash/replay regression before P7b repeated scenario
comparisons, then P7c evidence/default review. P7b consumes P3's versioned artifacts
and all three shared backend profiles; representative runs must distinguish NFS
stable writes, RADOS acknowledgement semantics and S3 RTT from operation latency.
Retain wait distributions, shared-resource placement, cache state, growing-backlog
warnings and finalization in the evidence. A missing backend or scale is an
explicit partial gate, not permission to extrapolate synthetic performance.
P7c hands the result and sensitivity-based recommendation to Phase 34 M7; no
standalone predictive simulator is required for this phase.

**P7 status:** P7a and the frozen-fixture component of P7b are validated as of
2026-09-17; P7b overall remains partial. The two-lane deferred fixture is 41.6%
faster end to end than the Stage 2 equivalent; four and eight lanes are flat. Go
race suites, Rust library and serial binary suites, crash/replay/handoff gates,
and timeout recovery pass. A successful 379,934,385-blob uncapped run is
historical evidence from revision `8bc9cd7cb`, not current-revision acceptance.
Current uncapped and representative backend runs remain blocked on authorized
isolated resources. P7c therefore retains all current defaults and reports
external acceptance as partial. See
[Phase 32 P3-P7 evidence](phase-32-p3-p7-evidence.md).

### Session Handoff

End every execution session with this compact record in the phase's work notes
or change description; update status here only when the phase actually advances:

```text
Phase/substep: Pn / pending|in-progress|blocked|validated|rejected
Baseline and worktree: revisions, relevant pre-existing changes
Hypothesis and scope: limiting function, expected measurable effect
Changes: files/symbols, preserved invariants
Validation: exact commands, results, artifact paths, input/build IDs
Decision: accept/reject/inconclusive; unmet gate or next bounded substep
Live resources: owned processes, sockets/candidates; untouched user resources
```

## Validation and Profiling

Freeze ordered source indexes **and pack selection**, not just a record budget:
parallel decode completion can select different prefixes. Repeat at least three
times; report spread, warm/cold cache, candidate age, encryption, WAL/cache/SST
settings, resources, and exact revisions. Include unique-heavy, duplicate-heavy,
large-pack, and dependency-chain inputs at sizes crossing flush/compaction
thresholds (20M, 100M, projected/real 400M IDs).

Required tests cover Stage 2 equivalence, aggregate/history sequences, duplicate
locations, stale hints, out-of-order completion, dependency inversion, oversized
packs, adaptive splitting, RPC limits, cancellation, expiry, every mid-index
resume boundary, and bounded backpressure. Inject crashes/ambiguous replies around
ingest, receipt, reduction, checkpoint, partial cleanup, marker, handoff, and
activation. Existing cleanup replay and successful handoff tests are not exhaustive
crash proof. Independently recompute aggregates and verify no unresolved recovery
state before claiming completion. Run race tests for coordinator/worker changes
and the fork's transaction/replay/flush/fencing tests for engine changes.

Stage 1-2 acceptance retains >=4x commit reduction, >=2x pack throughput on the
20M fixture, stable filter bounds through 400M IDs, and unchanged ordinary
publication. Stage 3 requires >=20% sustained publication improvement over
Stage 2 on identical repository-scale input, bounded pressure, and no end-to-end
regression. A complete uncapped run through required finalization is still
necessary; Stage 2 remains the fallback.

Build optimized symbols separately from live executables:

```sh
make profile
readelf -S bin/profile/linux-amd64/vaultic bin/profile/linux-amd64/vaulticdb \
  | rg 'debug_info|symtab'
perf record -F 99 --call-graph dwarf -o run.perf.data -- <command>
perf report --stdio -i run.perf.data
```

Go's `profile` tag preserves symbols and enables profiling controls; Rust release
uses `debug=1`, `strip=false`. Verify build IDs and symbolize both processes.
Keep pprof on loopback. Use short captures and offline reporting; do not let
DWARF reporting compete with the import. A completed 499 Hz synthetic capture
had 1,897 samples and zero loss; it is separate from the unresolved full-run
report. CPU profiles need RPC/queue and off-CPU data for wall-time attribution.

Artifacts under `/volume2/NASDA2/rustic/log/`:

- `index-import-stage3-full.*`: interrupted ordinary-cleanup run, exit 130.
- `index-import-stage3-full-deferred.*`: retry started 09:47:07 UTC at
  `8bc9cd7cb`; `.profiles/go-cpu.pb.gz`, `.profiles/go-cpu.txt`, and
  `.profiles/steady.perf.data` hold captured evidence.
- `phase32-scaling-cpu4.txt`, `phase32-scaling-cpu32.txt`, and
  `phase32-scaling-lanes2-deferred.perf.*`: controlled fixture results. The
  separately labeled contended pilot is excluded from the table above.

Check timestamps/exit status before describing a run as completed. An active
run's empty timing file, ETA, or cumulative pack count does not prove final
SlateDB output is durable or authoritative.
