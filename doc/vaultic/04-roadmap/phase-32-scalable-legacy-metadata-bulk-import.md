# Phase 32: Scalable legacy metadata bulk import

[Back to roadmap index](00-overview.md) |
[Phase 31](phase-31-read-only-nfsv3-snapshot-server.md) |
[Phase 33](phase-33-index-check-scalability-and-performance.md)

[CLI and operations architecture](../02-architecture/04-cli-and-operations.md) |
[Operational monitoring](phase-34-operational-monitoring-and-metrics-export.md)

**Status:** Stages 1-3 implemented. Stage 3 is fresh-reset-only and defaults to
two ingestion lanes; deferred cleanup is opt-in. Dependency inversion and the
indivisible-pack regression were fixed in `902b94389`; instrumentation and
guarded cleanup deferral landed in `8bc9cd7cb`. Full-import completion and
repository-scale Stage 3 performance acceptance remain pending.

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
to `otuschhoff/slatedb` revision `5faf4b086b043c65afdf193a7e2f87a737a11205`.
Engine function names below refer to the
[pinned engine source](https://github.com/otuschhoff/slatedb/tree/5faf4b086b043c65afdf193a7e2f87a737a11205/slatedb/src).

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

### P2. Complete VaulticDB and Engine Attribution

**Prerequisite:** P0; align operation names with P1. Split this into P2a service
admission/fencing/transaction timers, P2b fork queue/apply/backpressure timers,
and P2c SST/compaction/finalization counters. Validate each substep before the next.
Reuse engine statistics and expose bounded snapshots through an existing
diagnostic path; avoid a new RPC/protocol unless existing paths are insufficient.

**Check:** discover and run the pinned fork's relevant transaction, flush,
backpressure, and fencing tests. Add focused tests proving queue wait is distinct
from service time, failed attempts are counted, deferred durability is not counted
as an explicit durable wait, and instrumentation creates no await-under-lock or
blocking log path. Build optimized symbols and run the Go real-daemon smoke fixture
with `VAULTICDB_TEST_BINARY` pointing to that exact binary.
**Exit:** named client/server/engine boundaries and matching build IDs; no
behavioral changes or unbounded metric cardinality.

### P3. Measure and Select One Experiment

**Prerequisite:** P0-P2. Run identical isolated input at least three times,
including a candidate large enough to flush/compact. Capture unprofiled throughput
separately from short profiles. Report queue occupancy, service/wait distributions,
eligible work during reduction, CPU/RSS, file-output counters, and finalization.

**Decision:** choose P4 if eligible work is blocked by synchronous reduction;
choose one P5 substep if local planning, reducer RPCs, or service locks dominate;
choose one P6 substep if the engine/output path is demonstrably limiting. Multiple
causes may exist, but change one at a time and rerun P3 after each accepted change.
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
