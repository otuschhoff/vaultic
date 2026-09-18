# Phase 34 M0 contract and durability review

[Phase 34 roadmap](phase-34-operational-monitoring-and-metrics-export.md)

This is the handoff for M0. It freezes experiment schema version 1 and monitor
schema version 2, and records current
guarantees; it does not enable injection, defer a production commit, or claim
that instrumentation and action adapters scheduled for M1/M2 exist.
Vaultic and these contracts support 64-bit systems only; 32-bit builds are not
an M0 compatibility target.

## Executable contracts

`internal/telemetry/monitor_schema.go` is the monitor v2 registry. Version 2 is
intentional because freezing previously open metric identities is incompatible
with the provisional v1 validator. Metric name,
kind, unit, permitted labels and histogram bounds are closed. Unknown metric or
label identities, changed bucket layouts, duplicate identities and excess
cardinality fail validation. Influx series use stable component/deployment tags;
`process_start_id` is a field on the component point, and exporter state resets
counters when it changes. A partially delivered batch invalidates baselines so
the next export marks counters as reset instead of replaying persisted deltas.
Availability is one of `exact`, `estimated`, `stale`, `unavailable`, or
`not_applicable`; zero alone never means unavailable.
Storage snapshots bound object class, placement state, representation,
acknowledgement and reconciliation time. Object count, payload bytes, physical
bytes, and reconciliation time carry separate availability. Cache snapshots
bound family, representation, enabled/controller state, object count and
reconciliation lag. Request counters, traffic bytes, in-flight fills, inventory, reconciliation
age, reconciliation lag, and deletion-pending bytes carry separate availability so an unimplemented
source is exported as `unavailable`, never as an exact zero. Queue names are
closed by the v2 registry (`batch_write`, `legacy_import_ingest`, and
`legacy_import_reduce`); queue capacity has independent availability because
the current SlateDB status does not expose that limit. Cache circuit state is
explicit and bounded. Active-operation gauges inherit component availability,
and overflow is retained as one
saturating counter per bounded operation class.
Influx measurements emitted by this contract use the `vaultic_monitor_v2_`
prefix. The versioned namespace prevents unsigned v2 fields from colliding with
field types written by the provisional schema v1 exporter.

Latency histograms use microseconds. Vaultic request/object boundaries use
`10, 100, 1000, 10000, 100000, 1000000, 10000000, MaxUint64`. Pinned SlateDB
engine queue/service boundaries use `1000, 5000, 10000, 25000, 50000, 100000,
250000, 500000, 1000000, 2500000, 5000000, 10000000, MaxUint64`. An unavailable
histogram has only the `MaxUint64` sentinel and zero count.

`internal/telemetry/experiment_schema.go` is the profile/artifact v1 contract.
Strict decoding rejects unknown JSON fields and trailing values. It bounds every
identity, list, delay, bandwidth, concurrency, deadline, retry count and run
window. It separates service, durability and post-completion acknowledgement
delay; records additive versus synthetic interpretation, observer endpoint,
sequential/random access, occupied resources and the backend acknowledgement
claim. `scenario` identifies the overall topology while `backend` identifies the
targeted dependency resource, so mixed-resource scenarios remain representable.
It records sampled and observed delay separately and preserves unknown facts
as bounded field/reason records. The test-only gate is mandatory. The
fixtures under `internal/telemetry/testdata/phase34` are inputs for M1/M2:

- `nfs-hdd.json` leaves mount/export stable-storage semantics explicit.
- `rados-three-replica.json` is disabled and leaves pool `min_size`, recovery
  state, and therefore effective acknowledgement explicit as unknown.
- `s3-8ms-rtt.json` records 8 ms only as assumed network RTT. It is disabled and
  cannot be interpreted as GET TTFB, service completion or PUT completion.

Profiles are contracts, not an injector. M2 must additionally refuse a target
unless its configured identity is known to be disposable; `test_only: true` is
necessary but is not proof by itself.

## M0a owner, metric and durability matrix

`D` means the current call waits on its durability boundary, `A` means atomic
writer-visible state without a crash guarantee, and `E` means ephemeral or
recomputable state. Metrics listed here are required M1 ownership, not a claim
that every metric is already wired.

The final column names exact boundaries; each named boundary emits the following
frozen family with that row's bounded `operation` and `role` values:
`operation_started`, `operation_completed{outcome}`, `operation_active`, and
`operation_processed_bytes{role}` describe action lifecycle; `wait_attempts`,
`wait_contentions`, `wait_completed{outcome}`, `wait_active`, `wait_oldest_age`,
and `wait_duration{outcome}` carry bounded `operation`, `role`, and `throttle`
identity; `dependency_requests{outcome}`, `dependency_bytes{outcome}`, and
`dependency_latency{outcome}` carry bounded `operation` and `role`. Queue depth,
capacity, oldest age, workers, admission/rejection and backpressure use the
versioned `QueueSnapshot`. Storage, WAL, cache and active-operation state use
their versioned snapshot records. M1 may leave a non-applicable family absent,
but must not invent a new identity without incrementing the monitor schema.

The exact action identity map is: pack upload/catalog/revision/snapshot use
`backup` with `source`, `repository`, and/or `database`; revision allocation uses
`maintenance` with `database`;
staging uses `staging_reconcile` with `repository`, `database`, and
`coordination`; all fresh/resumed import and handoff work uses `legacy_import`
with `source`, `database`, `wal`, and `coordination`; generation and writer
authority use `recovery` with `database`, `wal`, and `coordination`; forget uses
`forget` with `database`; both prune implementations use `prune` with
`repository` and `database`; GDPR uses `gdpr` with those roles; placement,
promotion, and placement rebuild use `placement` with `repository` and
`database`; verification uses `maintenance` with `repository` and `database`;
restore uses `restore` with `repository` and `source`; repository copy uses
`replicate` with `source` and `repository`; SlateDB background compaction uses
`compaction` with `database`; export uses `export`; analytics uses `analytics`;
repository initialization, configuration, and deletion use `maintenance` with
`repository`; other rebuild/check work uses `maintenance` or `check`; cache policy/admission
uses `cache_fill` and eviction/drain/reclamation uses `cache_evict`, both with
`cache` and `coordination`; key/envelope/DEK and broker policy use
`key_management` with `broker`, `coordination`, and `repository`; broker
credential waits use `key_management` with `broker`; read sessions use `check`
with `database`. Each dependency call uses the dependency families, each named
wait uses the wait families, and each top-level action uses the operation
families. This mapping is closed by `monitorValues`; adding an action or role is
a schema change.

| Action and owner | Current boundary and recovery | Required M1 boundary |
|---|---|---|
| Repository initialization: `Repository.Init`, `InitWithConfig`, `InitWithConfigAndKey`, `createMasterKey`, `AddKey`, `SaveConfig` | Key publication precedes config publication across separate backend operations. An interruption can leave a key without config; initialization refuses to overwrite detected key/config/snapshot state, so cleanup or operator recovery is required. | operation=`maintenance`; dependency role=`repository`; existence checks, key/config writes, completion guarantees, partial-initialization recovery |
| Repository configuration: `Repository.UpdateConfig`, `UpdateConfigAtomically`, `vaultic.SaveConfig` | Backend config replacement completes before in-memory activation. Crash-sensitive callers require `HasAtomicReplace`; ordinary updates inherit the selected backend's replacement guarantee and retry behavior. | operation=`maintenance`; dependency role=`repository`; validation, replacement capability, save latency/outcome |
| Repository deletion: `Repository.Delete`, `backend.Backend.Delete` | Backend-wide deletion is destructive and not transactionally atomic across objects. Interruption may leave a partial repository; retry continues backend-specific removal, and no success is published before `Delete` returns. | operation=`maintenance`; dependency role=`repository`; listed/deleted bytes and objects, retry, partial-delete state |
| Restore: `restorer.Restorer.RestoreTo`, `fileRestorer.restoreFiles`, `filesWriter` | Repository reads are non-mutating; destination filesystem creation, overwrite, metadata application, and optional removal are incremental rather than transactional. Interruption leaves a partial destination and rerun revalidates/reapplies selected nodes. | operation=`restore`; dependency roles=`repository`,`source`; pack reads/warmup, file writes, metadata, deletion, bytes and errors |
| Repository copy/replication: `runCopy`, `copyTreeBatched`, `repository.CopyBlobs`, `copySaveSnapshot` | Destination packs/trees complete before snapshot publication. Interruption can leave unreachable destination objects; rerun deduplicates existing content and republishes missing snapshots. | operation=`replicate`; dependency roles=`source`,`repository`; source reads, destination writes, bytes, publication completion and retry |
| SlateDB compaction: pinned engine background compactor and `Storage::close`/flush lifecycle | Engine-created SSTs and manifests follow SlateDB's internal durability protocol; obsolete files are derived/reclaimable. Vaultic does not treat compaction completion as an application durability token. | operation=`compaction`; dependency role=`database`; queued/running compactions, input/output bytes, stalls, completion/failure |
| Pack upload: `repository.savePacker` | Backend completion precedes metadata; an interruption may leave an unreachable pack. | admission, transfer, completion guarantee, retry, bytes |
| Pack/catalog publish: `DaemonEngine.storePack`, `SchemaStore.PublishPack` | D transaction atomically publishes pack, blob, aggregate, placement and lineage records; idempotent retry. | begin/read/write/commit, durability, conflict, uploaded-unpublished age |
| Compatibility publish: `DaemonEngine.Flush`, `MarkPackPublished` | Legacy index flush precedes D lifecycle transition; pending packs are retryable. | index upload/flush, commit durability, pending count/age |
| Normal revision: `PublishRevisionBatch`, `PublishReconciledRevision` | D atomic revision/current-pointer/reverse-reference publication. | planning/read, commit response/durability, conflicts |
| Deferred revision/content manifest: `PublishRevisionBatchDeferred`, `PublishRevisionBatchesDeferred`, `PublishContentManifestDeferred` | A and restricted to rebuild-reset; loss restarts the private import. | applied acknowledgement, admitted bytes, queue age, final fence owner |
| Snapshot: `PublishSnapshotScope` | D atomic snapshot, commit/export checkpoint, revision sequence and related state after root verification. | prerequisite/root, commit, durability, ambiguous response recovery |
| Revision sequence allocation: `AllocateRevisionBlock` | D transaction advances the repository sequence before returning the allocated range; unused values after interruption are safe gaps. | operation=`maintenance`; dependency role=`database`; commit/durability wait |
| Snapshot export recovery: `MarkExportPending`, `MarkExportFailed` | D snapshot/export-checkpoint state transition; failure text is bounded/redacted and never a metric label. | operation=`export`; dependency role=`database`; commit/durability and retry |
| Staging journal: `staging.Store.PublishJob` | Authenticated immutable segments/seal reach configured quorum; no snapshot is visible. | mirror PUT, quorum, bytes, sealed-pending age |
| Staging reconcile: `staging.Reconcile`, `DaemonAuthority.CommitMetadata`, `PublishSnapshot` | D idempotent metadata, external snapshot, snapshot scope and completion record; restart retries sealed work. | verify/preflight, commit/fence, snapshot and completion quorum |
| Fresh import ingest/reduce: `importPacksStage3`, `IngestLegacyPacks`, `ReduceLegacyImportBatch` | A receipts/catalog/checkpoints in reset generation; memory-WAL import is restart-from-zero, not resumable. | ingest/reduce queues, ready age, bytes, RPC/apply acknowledgement |
| Ordinary import resume: `legacyimport.Import` | Existing D checkpoints and receipts are replay identities; no durable-prefix token exists. | checkpoint I/O, replay/conflict, cleanup and durable result |
| Import completion/handoff: `MarkBulkImportComplete`, `completeFreshBulkImport`, `Storage::close` | D marker, successful flush/close, local-WAL handoff, persistent reopen and validation are all required. Failure resets/rejects the candidate. | marker durability, flush/close, handoff, writer release, reopen |
| Generation activation/heal: `activateImportedIndex`, `Storage::activate_generation` and generation transition methods | Immutable decision then conditional active-authority update; expected generation/decision fences transitions. | transition lock, coordination GET/conditional PUT, observation state |
| Forget: `runForgetWithPhaseACallback`, `SchemaStore.ForgetSnapshot` | Exclusive revalidation then D atomic membership/index/sequence/analytics change; packs remain. | policy/lock/revalidation, commit durability/conflict |
| Legacy prune: `PrunePlan.Execute`, `BeginPrunePlan`, `CompletePrunePlan`, `FinalizePrunePlan` | Replacement objects precede D config marker; restart discards/revalidates incomplete plans. Unsafe mode has no durable marker. | repack/upload, lock, marker, revalidation, delete/retry |
| SlateDB GC: `GCPlan.deleteWholePacks`, `MarkPackDeletePending`, `MarkPackDeleted` | D delete-pending state, physical delete, then D catalog cleanup; restart retries missing/pending objects. | retention age, DELETE completion/retry, catalog durability |
| GDPR: `ExecuteGDPRForget` | D atomic redactions, rebuilt references, eviction queues, lifecycle and signed certificate. | scans/planning, transaction size, conflicts, durability, queued delete |
| Verification publication: `RecordVerification` | D atomic verification result and placement/repair state update after object verification; retry revalidates current state. | operation=`maintenance`; dependency roles=`repository`,`database`; verify and commit/durability |
| Placement plan/execution: `PlanPlacement`, `ExecutePlacement`, `persistPlacementAttempt`, `completePlacementRequest` | A request/transitions surround idempotent physical place/evict. Lost derived state is replanned; eviction checks durability policy first. | queue/capacity/age, credentials, transfer/delete, retry and policy |
| Promotion: `PromotePack` | New packs upload and publish D before source becomes D delete-pending; successor records support restart. | repack/transfer, successor durability, delete-pending |
| Placement/index rebuild: `RebuildBackendPackIndex`, `RebuildDerivedTierSummary`, `RebuildPlacementRecords` | A derived and recomputable repair batches. | paged scan, drift, batch/RPC, cancellation |
| Legacy export: `maintenance.Export`, `exportIndexBatch`, `MarkIndexPublished` | Object save/verify precedes D checkpoint/publication; restart can skip checkpointed work. | scan/encode/upload/verify, checkpoint durability, orphan cleanup |
| Analytics rebuild: `analytics.Rebuild`, `saveBuildCheckpoint`, `publishRebuild` | Candidate batches are recomputable; checkpoints and final manifest/watermark publish D. | source scan, capacity, checkpoint age, publication durability/lag |
| Analytics lifecycle/jobs: `Enable`, `Disable`, `Purge`, `Start`, `Resume`, `Cancel`, `Wait` | Policy/final generation and persistent jobs are D; caches/views/cleanup may be A and rebuildable. | queue/state/age, scan/build/cache, cancellation, watermark |
| Other maintenance: `maintenance.Rebuild`, `RebuildSnapshotCommitIndex` | Authoritative aggregate publication is D; derived repair is A; check scratch is E. | bounded scan/scratch, batch commit, progress/cancellation |
| Read-cache policy: `Repository.UpdateReadCachePolicy`, `readCacheManager.updatePolicy` | Signed policy revision is published to coordination storage before local application; stale/equivocating revisions are rejected. | policy read/conditional write, revision, admission state and failures |
| Read-cache admission: `reserveAdmissionWithSnapshot`, `commitAdmission` | Coordination ledger reserves quota before physical publication and advances admitting/publishing/live ownership with generation and commit-order checks. Interrupted entries are reconciled or reclaimed. | capacity wait, reservations, staging/pinned bytes, publication and reconciliation age |
| Read-cache eviction/reclamation: `markDeletionPending`, `readCacheTier.deleteRetiringEntry`, `finalizeDeletion` | Ledger marks deleting before physical data/metadata removal; ledger entry is removed only after absence and reclamation checks. Restart reconciliation resumes cleanup. | eviction/admission rejection, deletion/reclaim-pending bytes, backend delete/stat and failures |
| Read-cache drain/clear: `Repository.DrainReadCache`, `Repository.ClearReadCache`, `readCacheTier.drain` | Marks entries retiring, performs physical removal and finalizes the coordination ledger; partial work is recovered by normal reconciliation. | operation=`cache_evict`; dependency roles=`cache`,`coordination`; active/progress, delete/stat, reconciliation |
| Writer authority: `claim_writer_epoch`, `release_writer_claim`, `Storage::promote`, `demote` | Conditional coordination object owns epoch; every mutation checks it. Transition flush/close/reopen failures remain explicit. | transition lock, epoch reads/CAS, active transactions, flush/close/reopen |
| Generation authority: `publish_generation_authority`, quarantine/activate/verify/rollback/retire | Immutable decision precedes conditional active pointer; restart reloads authority. | coordination service/conditional write, decision and reconciliation state |
| Key envelope/slots: `KeyManager::add_*_slot`, `remove_slot`, `rotate_local_slot`, `publish_envelope` | Serialized immutable generation PUT; memory state changes only after success. | provider/Argon2, envelope PUT, generation/slot counts, failures |
| DEK lifecycle: `rotate_dek`, `rewrite_old_deks`, `audit_objects` | New envelope precedes write-key install; object rewrites are restartable PUTs; retirement follows clean audit and envelope publication. | rewrite/list/read/decrypt/write/audit and provider waits |
| Broker policy/topology: `prepare_*_mutation`, `activate_policy_mutation`, `publishAndActivatePolicyMutation` | Pending state is process memory; sessions are revoked; immutable mirrors precede digest-checked activation. | quorum/human wait, provider wrap, mirror publication, activation/relock |
| Broker fallback credentials: `acquire_storage_credential_lease`, `persist_fallback_state` | Signed fallback state persists before credential release or provider recovery transition. | issuer service/response, decision, persistence, rotation age |
| Broker sessions and leases: `create_session`, `acquire_lease`, `release_lease`, `expire_state`, `lock` | E bounded TTL/connection/epoch state; restart, disconnect, lock or policy change revokes it. | active/capacity/expiry, quorum, renewal/provider, revocations |
| Read session: `SchemaStore.BeginReadSession`, `ReadSession.Validate`, `Close` | Process-local serializable transaction pins generation/decision/sequence; no restart continuation. | begin/generation checks, active age, lookup/scan, validation and cleanup |

No current action may infer durability from another action's success or from a
monitor counter. In particular, applied-only placement/derived-state writes are
recomputable; they are not permission to defer destructive publication.

## M0b pinned-engine decision

The engine is SlateDB revision
`fc68f09a25defb128edfd722ec82696492dbb692` in `vaulticdb/Cargo.toml` and
`Cargo.lock`. `Storage::write_batch` awaits the handle returned by `Db::write`
when `await_durable` is true or an idempotency record is requested.
`Storage::commit` awaits the optional handle returned by transaction `commit`
unless the reset-only `defer_durability` flag is set. The service response then
reports only a boolean `durable`.

Ordered-prefix durability is **rejected for Vaultic use at this revision**. The
pinned SlateDB `WriteHandle::seqnum` exposes its engine sequence, `DbStatus` has
a durable sequence whose contract covers writes through that value, and
`WriteHandle::await_durable` waits for that prefix. Vaultic currently discards
the handle and sequence at its RPC boundary, returns only a boolean, and does
not bind the engine sequence to repository generation and writer epoch. Its
`last_durable_sequence` increments after successful waits and writer flushes;
it is a monitoring counter, not SlateDB's sequence or a transaction identity.
Consequently an engine prefix cannot be persisted, resumed, or safely presented
across generation/reset/failover through the current Vaultic protocol. A later
Vaultic durable commit or final marker is therefore not accepted as a reusable
fence for earlier deferred commits. Normal commits remain per-commit durable.
The current fresh reset import remains destructively restartable, not
crash-resumable.

### WAL acknowledgement assumptions

| WAL target | What current success establishes | What it does not establish |
|---|---|---|
| `memory` | SlateDB handle completed against process memory. | Process/host crash durability; never a resume token. |
| `local` | Object-store write completion; status deliberately says `local-process`. | `fsync`, stable-media or power-loss survival unless independently measured and configured. |
| `inherited` | Uses the effective metadata object store, which may be memory, local/NFS, S3, RADOS, Azure, GCS, or a replicated topology. | `inherited` is not an acknowledgement guarantee. Evidence must resolve every replica's medium, quorum/failure domain and completion semantics, then apply the weakest guarantee. |
| S3 | Successful object PUT/multipart completion through the configured client; status says `shared-remote`. | Universal service latency, cross-region replication, provider internals or prefix fencing. |
| Native RADOS | Successful operation completion through the configured pool/client; status says `shared-remote`. | Replica/min-size policy, failure domain, recovery state or prefix fencing; evidence must record them. |
| Inherited Azure/GCS | Successful provider-client object completion when the metadata topology selects that provider. | Provider replication/internal persistence or prefix fencing; the explicit WAL selector does not currently support these providers. |

The strongest claim in an artifact is the weakest configured acknowledgement.
Unknown backend facts stay `unknown`; a label such as `persistent` or
`shared-remote` is not upgraded into proof.

## M0c future durability token

The minimum protocol extension is deliberately not implemented in M0:

```text
DurabilityToken {
  repository_generation: opaque bytes
  writer_epoch: uint64
  applied_sequence: uint64
}

CommitResponse { durable: bool, applied: DurabilityToken }
WriteBatchResponse { durable: bool, applied: DurabilityToken }
AwaitDurableThrough(token) -> { durable_through: DurabilityToken }
```

The engine adapter must assign the sequence in commit order and prove that a
successful wait recovers every sequence through the requested value. The server
must reject repository-generation or writer-epoch mismatch, unknown/future
sequences, memory-WAL durability requests, and tokens from a closed/reset
generation. Waiters may coalesce internally, but responses retain their requested
token. Cancellation ends only the wait, not an already applied mutation.
Monitoring counters cannot construct a token.

Go `Client.WriteBatch`, `Transaction.Commit` and their interfaces must return the
opaque token without interpreting it. A future resumable import checkpoint must
atomically contain input identity/order, stable cursor or contiguous reduced
prefix, and its highest required token, then wait for that exact token before
advertising resume. Snapshot/publication code must similarly fence every
referenced prerequisite before external success. Until deterministic M2g tests
prove ordered application, coalesced waiters, stale generation rejection, WAL
failure and restart replay, deferred behavior stays unchanged.

## Crash-model review

The current safe paths satisfy the publication rule: pack bytes precede durable
catalog reachability; durable revisions precede durable snapshot publication;
destructive decisions use durable state around physical deletion; authority and
key changes publish their controlling objects before activation. Staging work is
not authoritative until reconciliation completes. Ephemeral read sessions and
scratch are restart-only.

The sole applied-only authoritative bulk path is isolated to a private reset
generation. It is not activated or advertised as resumable from its in-memory
checkpoint. Its completion requires marker, close/flush, WAL handoff, persistent
reopen/validation and generation activation. Because a generation-bound Vaultic
proof is absent, M0 does not certify its final marker as a general coalesced
fence and does not extend that optimization to any normal action.

Explicit remaining prerequisites are M1 owner wiring/overhead evidence, M2
injection and action adapters, M2g token implementation/proofs, and a persistent
fresh-import resume format. None is part of M0 acceptance.