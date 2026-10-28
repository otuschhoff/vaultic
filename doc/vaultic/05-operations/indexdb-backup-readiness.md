# IndexDB Backup Readiness and Execution Design

[Documentation index](../README.md) |
[Metadata resilience](metadata-resilience.md) |
[M6 preparation](../04-roadmap/phase-35-m6-preparation.md)

## Decision Summary

Assessment date: 2026-10-28. Code baseline: `4b174b5c8`, including the
experimental snapshot checkpoint `cd84d564e`. This is a design and execution
plan, not production approval or a report of newly executed workload tests.
Here, **indexdb** means the daemon-backed VaulticDB/SlateDB metadata engine,
not the temporary RAM/KV working-state backend.

Import, metadata checking, repository checking, and backup with live inode
discovery already have implementations. Regular backups do not require a new
backup engine or an inode scanner built from scratch. They require a completed,
validated bootstrap; demonstrated live identity correctness; an affordable first
verified backup; repeated full-scope backup/restore evidence; and an operated,
recoverable metadata service. These conditions have not all been demonstrated
for the real target distribution.

**Priority: reach safe regular full backups, then optimize measured bottlenecks.**
Qualify catalog-first bootstrap: import all pack/blob locations, preserve all
historical snapshot JSON/root references, and defer materializing historical
inode/debt graphs. The first full-crawl backup discovers and verifies current
live inodes through the existing reconciler. This is the preferred design to
qualify, not a workflow already proven safe by the existing import flags.

Do not equate catalog completion with verified historical inode state, treat
imported metadata as verified, or enable experimental optimizations merely
because their unit tests pass. Complete historical inode materialization,
representative microbenchmarks, candidate comparisons and material-speedup gates
are not prerequisites for this backup-readiness route. Correctness, durability,
complete source coverage and the existing runtime/resource limits still apply.

Pebble removal, new working-state defaults, selective crawling, and the atomic
reconciliation publication prototypes are not prerequisites for regular backups.
Keep them outside the initial readiness target unless evidence makes one necessary.
Instrument the actual bootstrap, checks and full backup runs with bounded metrics
so their costs can guide later optimization without a separate profiling campaign
before the first backup. Implement only a measured correctness/resource/runtime
blocker if the unoptimized path cannot complete safely within the agreed limits.

## Current State

The evidence levels below are deliberately different: implemented means code
exists; fixture-verified means named bounded tests exist or were run; target
accepted means the complete real workflow met its frozen contract.

| Area | Implemented behavior | Readiness and remaining gap |
| --- | --- | --- |
| Legacy import | Index decoding, pack/blob locations, duplicate handling, checkpoints/resume, snapshot JSON, recursive trees, partial metadata and deterministic crawl debt | Large real imports completed the pack/blob catalog but historical snapshot traversal repeatedly missed the 12-hour limit. No accepted full target bootstrap follows from those partial results. |
| Import memory | Managed working-state policies; context-charged fresh-import ID filter and explicit release | Latest filter changes have focused native/race validation, not a new successful real import. Accounted bytes are not process RSS. |
| Snapshot experiments | Default-off debt-write overlap and single-slot size-aware tree prefetch, with managed reservations and worker cleanup | Serial/overlap/prefetch/combined byte-equivalence and failure-before-checkpoint tests pass. Representative performance, broader failure coverage, and the staged gate runner remain pending. Prefetch size hints and decode reservations are not a proof of all transient source allocations. |
| Populated-catalog attribution | Opt-in `TestM6PopulatedCatalogAttribution`; fresh full native catalog, all frozen roots, bounded traversal, debt verification after reopen | Implemented and compiled, but the real-source fixture has not been run. It is not a full-import result or a full catalog audit. Its node cap cannot be used for workflow acceptance. |
| `index check` | Coherent daemon read session; metadata consistency checks, legacy comparison, bounded findings, configurable workers/RPCs, memory and scratch limits | Implemented, including scalable spill paths. Revalidate exact coverage and runtime against the successfully imported target catalog. A subset or `--slatedb-only` result must not be presented as complete legacy comparison. |
| Repository `check` | Index/pack metadata, snapshot structure, optional unused-blob and payload reads; exclusive repository view | Separate from `index check`. Metadata consistency does not prove stored payloads restore. Specify actual data-read coverage and exercise restore. |
| Normal authoritative backup | Daemon engine selection, full or on-demand catalog loading, pack/snapshot publication and recovery, legacy projection, reconciliation hooks | Integrated code exists. The blocked M6 workflow did not reach an accepted full target backup, so regular target backups are not yet qualified. |
| Activation | Feature-gated authority transition; import rejects reached limits and recorded errors; rebuild initialization adds metadata validation | Ordinary `--activate` does not automatically execute both checks or prove the intended full traversal scope. Freeze and enforce the external readiness gates; assess additional CLI guards in Phase 1. |
| Inode discovery | Live `lstat` identity and parent inspection, verified reuse gate, final-node observation, inode/directory revision publication, debt retry/resolution | Implemented. Confirm identity stability on every actual source adapter and mount, then prove first-backup and unchanged-backup behavior end to end. |
| Selective/deferred modes | Pathdiff/FSEvents provenance and fallback; staged data with later metadata reconciliation | Not the initial regular-backup acceptance path. Deferred data is explicitly not a committed backup. Selective coverage is not a global complete-crawl claim. |
| Operations | Writer fencing, demotion/promotion, pending publication recovery, staging and healing workflows | Implementations and runbook exist; deployment-specific restart, recovery, upgrade, restore and scheduled-run exercises are still required. |

### Evidence Boundary

The frozen real distribution has 10,170 indexes, 419,853 packs, 380,166,821
blob records, 143 snapshots and 52 backup sources. Historical RAM/KV/auto
attempts completed index import but often only 28 of 143 snapshots before
timeout; another recorded variant reached 30. These are different partial
workloads, not a causal ranking of modes. Zero findings in a timed-out run is
not a clean audit or proof that all attempted writes were durable.

Several isolated reader, encoder, decoder and native-write candidates were
screened without meeting the unchanged material-gain gates. Those private
variants are not automatically included in the active implementation. The
[archived checkpoint](../04-roadmap/m6-checkpoint-2026-10-26/README.md)
records one such reproducible, rejected candidate; it is historical evidence,
not a runnable production plan. Later private evidence needs explicit manifest
verification before being used as a decision input.

The most recent focused validation passed the native legacy-pack idempotency
regression, seven ID-filter tests, and the complete `legacyimport` and `indexcmd`
package suites under the race detector. No full repository-suite pass is claimed.
A broader test selection encountered a Cargo target-path setup failure before
the process test could execute. Earlier evidence also records a daemon drain
status regression. Reproduce and classify both rather than silently excluding
them from a production qualification gate.

## How Inode Discovery Works

1. Import preserves available legacy facts and marks them imported/unknown.
   It cannot reconstruct missing historical filesystem identities by guessing
   from paths. Missing identity or freshness becomes crawl debt.
2. Normal daemon-backed backup creates `reconcile.Reconciler` and attaches it
   to the archiver. Live identity comes from filesystem observations, not the
   historical JSON's authority.
3. Before unchanged-file reuse, `CanReuse` requires a verified current inode,
   all required metadata bits, matching size/times/mode/ownership, compatible
   parent/path semantics, and identical ordered content IDs. Missing or
   malformed metadata forces the ordinary read/rechunk path.
4. Final saved nodes supply actual content IDs or directory subtrees. The
   reconciler publishes immutable revisions, current pointers, manifests,
   references and directory edges; matching debt is resolved transactionally.
   Hardlinks preserve multiple edges rather than inventing a unique parent.
5. Backup joins reconciliation before snapshot publication and publishes snapshot
   scope. Any failure or uncertain publication must remain distinguishable from
   acknowledged backup completion.

Consequently, the first verified post-import backup can be expensive: files may
need reading and rechunking even when their content already exists in packs.
Deduplication can avoid uploading duplicate content; it does not eliminate the
work needed to establish verified live metadata. Reading a historical snapshot
cannot verify the current filesystem, and a current crawl cannot retroactively
verify every historical identity or resolve unrelated historical debt.

### Identity and Coverage Risks to Qualify

- `statIdentity` currently accepts a nonzero device ID only up to `uint32`, plus
  a nonzero inode. Unavailable/out-of-range identities are deferred. Assess the
  actual local, mounted NFS and direct-NFS representations before deciding
  whether mapping or schema evolution is needed; do not truncate IDs to fit.
- Device/inode stability across reboot, remount, client host changes and inode
  reuse needs a documented source-identity contract. Current metadata matching
  is not evidence that these lifetime boundaries are safe on every adapter.
- Test hardlinks, renamed directories, cross-filesystem parents, excluded roots,
  permissions, marker exclusions, concurrent source mutation and interrupted
  observation. Do not infer a single parent or complete coverage from missing data.
- Backup currently does not opt into `AuthoritativeCrawlClaim`. Verified observed
  revisions and a committed snapshot are not a blanket absence/deletion claim
  over all repository history. Any global debt clearing or destructive maintenance
  needs its own complete-scope/fence proof, not an unconditional new claim.
- Bound retained directory graphs, prepared observations, queue contents,
  manifests, hardlink groups and retry state, not just worker/queue counts.

## Target State and Invariants

A regular indexdb-backed backup is an acknowledged, recoverable snapshot whose
data and authoritative metadata agree, with verifiable restore output. A
scheduler success, staged upload, clean metadata subset, or partial import is
not sufficient.

The target lifecycle is:

`small end-to-end proof -> guarded catalog-first bootstrap -> full checks ->
reviewed activation -> observed full-crawl backup -> check/restore/restart ->
regular full-scope backups`.

"Full backup" means a complete restorable snapshot of every selected source,
with exclusions applied. It does not mean uploading all bytes again or forcing
unchanged verified files to be reread on every run. Initially use full crawling,
not selective pathdiff/FSEvents coverage; existing verified reuse and content
deduplication remain allowed. Later runs must still publish complete snapshots.

Each transition must have a successful immutable evidence receipt. Failure
blocks downstream steps; activation and deployment remain separate approvals.
For pilot testing, use only a private writable repository/view and native
namespace. Never activate or write to the original read-only repository.

Required invariants:

- Authority is explicit and fail-closed. An authoritative repository requires
  the feature and the matching daemon; daemon failure cannot silently fall back
  to writable legacy metadata. A rebuild is not an ordinary import retry.
- Data publication, metadata commit, legacy projection and retry/reopen behavior
  preserve their current durability and fencing contracts. Uncertain outcomes
  are reconciled, not blindly replayed with new mutation identities.
- Every imported snapshot remains restorable. The new backup's scope is a
  complete revision graph for the selected sources; unrelated imported history
  can remain explicitly incomplete without being misrepresented as verified.
- Unknown identity stays unknown. Verified reuse needs live evidence. A failed
  or excluded subtree cannot create a complete-crawl or deletion claim.
- Concurrent jobs respect existing repository locks and writer ownership. Checks,
  backup, export, repair and retention need an explicit scheduling policy.
- Reopen/restart, unchanged backup and changed backup retain exact content,
  metadata, hardlinks and historical membership. A restore is verified, not just
  a snapshot count or an exit code.

### Bootstrap Decision

**Preferred: catalog-first, live discovery on backup.** Reuse the existing
catalog import and snapshot-metadata publication machinery. Preserve all 143
historical snapshots and their tree references without constructing a historical
inode revision or crawl-debt record for every old node. Historical facts remain
explicitly unverified; new live observations supply current verified metadata.

The small-fixture proof must establish historical restore, tree reachability,
missing-reference detection, legacy projection, durable handoff/reopen, activation
and correct current pointers. Existing `--snapshot-metadata-only` requires an
existing blob catalog and cannot be combined with activation, reset, rebuild
initialization or traversal budgets. Implement the minimum guarded orchestration
or completion-state changes the proof requires; do not simply remove these checks
or prescribe an untested combination of flags.

Record catalog completion, snapshot-reference completion, historical inode
materialization deferred, validation coverage and live-backup verification
separately. Bind readiness to the repository, namespace/generation, inputs and
observed completion state. Missing required data is failure, not expected
incompleteness. Historical graph-dependent analytics must report unavailable or
partial coverage; they must not invent complete results. Keep destructive
maintenance disabled until its own deferred-history safety case is established.

**Fallback, not the next automatic task:** retain the existing full historical
import route if the preferred route proves unsafe or requires disproportionate
changes. Report the concrete blocker and obtain a route decision before another
expensive historical import or optimization campaign. Do not start by repairing
the performance of work that regular backups may not require.

This is a distinct backup-readiness contract. It does not satisfy or silently
rewrite the original full-history M6 acceptance milestone. Existing source scope,
exclusions, durability and resource/runtime limits remain unchanged. Import's
resume support does not authorize reusing a timed-out/failed database as fresh
acceptance evidence.

## Phased Execution Plan

Execute phases in order, one bounded change/evidence package at a time. Existing
implementations should be assessed and extended, not rewritten speculatively.
The plan has five phases; only Phases 0 through 3 are on the readiness critical
path. Phase 4 is a separate optimization follow-up after regular backups work.
There is no prerequisite five-pair candidate benchmark,
25 percent gain, full RAM/KV/auto matrix or historical inode backfill.

### Phase 0: Prove the Minimal Workflow on Small Fixtures

**Inputs:** current source/daemon builds, this backup-first contract, private
generated repositories with shared trees, missing identities and multiple snapshots.

**Work:** exercise catalog-only import, snapshot-reference import, both checks,
guarded activation, first full backup with inode discovery, second full-scope
backup, restore and daemon restart through the real command paths. Read the
existing guards first and make only changes necessary to test this route safely.
Qualify live device/inode/parent identities on the actual source adapter; preserve
unknown identity rather than truncating or inventing it. Use existing default
reconciliation and publication, with experimental optimizations off.

**Checks:** exact catalog/snapshot references and new revision graph after reopen;
historical and new snapshot restores; first-backup read/rechunk where metadata is
unverified; verified reuse on the next full crawl; hardlinks, rename, delete and
recreate; corrupt/missing trees, content changes and unavailable identity. Test
cancellation, publication failure and no success on uncertain outcomes. Verify
named native/race tests actually run. Reproduce relevant known test failures and
record whether they block this path; do not require unrelated performance work.

**Output/exit:** command-level proof or a specific blocker. Identify the smallest
missing guard, compatibility or durability change needed for Phase 1. No real
workload, production activation or destructive maintenance is authorized by the
fixture proof. If source identities cannot support safe reconciliation, stop and
design the minimum stable mapping or qualified safe fallback.

### Phase 1: Finish Guarded Bootstrap and Full-Run Observation

**Inputs:** Phase 0 proof, frozen real source/profile/exclusions, private target
plan, pinned binaries and existing pilot/observer helpers.

**Work:** implement only the missing bootstrap/readiness and activation guards.
Reuse existing metadata/receipts when sufficient; avoid a new general framework.
Distinguish intentional deferred historical materialization from incomplete
catalog/snapshot import. Add a simple sequential supervised workflow, not an
optimization comparison runner. Reuse existing JSON counters and bounded observer
exports; add only missing measurements needed by the full-run contract below.
Freeze resource limits, runtime/schedule objectives, check coverage, restore
selection and approval boundaries before launching.

**Checks:** exact pack/blob/snapshot/debt/checkpoint contents after reopen;
missing/corrupt locations and trees; bounded findings and scratch exhaustion;
read-session expiry/generation change; authority denial on incomplete candidates;
explicit rejection of unqualified zero/depth-limited or omitted-snapshot states.
Test observer readiness/loss, PID reuse, stale receipts, changed inputs, output
failure, invalid/missing counters and no downstream action after a failed stage.
Verify export capacity can carry all mandatory fields; unavailable optional
metrics must not silently appear as zero or break backup execution.

**Output/exit:** a minimal tested bootstrap/backup supervisor with bounded full-run
metrics, guarded activation and an explicit deferred-history coverage state.
Expected incompleteness may be accepted only by the newly tested contract; do not
silently waive an existing activation gate. Full source and restore safety stay
mandatory. This phase prepares execution, not production deployment.

### Phase 2: Complete the First Observed Full Backup

**Inputs:** Phase 1 acceptance, reviewed private writable target, full source scope,
resource limits, observers and rollback/stop plan.

**Work:** populate one fresh catalog, import all historical snapshot references,
run `index check` and repository `check`, then perform the qualified private-target
activation/initialization and first full-crawl backup with live inode discovery.
Use the ordering proven in Phase 0, not guessed flag combinations. Observe the
entire invocation, including preload, finalization and close. Run post-backup
checks, verified restores and persistent-native restart/reopen validation.

**Checks:** all 10,170 indexes, 419,853 packs, 380,166,821 blob records and 143
historical snapshot references, plus all 52 backup sources and unchanged exclusions,
including no `.snapshot` subdirectories. These are frozen baseline counts, not
fixed counts for new data added by the backup. No node/depth/work caps or selective
crawl may qualify as a full run. Deferred historical inode graphs remain explicit;
required source read/stat/reconciliation failures cannot be hidden by that state.

Existing pilot limits remain combined owned CLI/native RSS 200,000,000,000 bytes
sampled every 250 ms, managed memory 160 GiB, working scratch 8 GiB, import/check
at most 12 hours each, backup at most one week, and NFS free-space floor
57,481,929,687,040 bytes. Use read-only original inputs and private writes only,
no swap/cores, unchanged default GC, explicit owned-process cleanup and sealed
evidence. Runtime extensions, failed-database reuse and scope reduction are not
automatic. Budget the sequential catalog/snapshot bootstrap explicitly; do not
use two commands to evade the import deadline. Stop on any failed or uncertain stage.

**Output/exit:** one acknowledged full backup with complete selected source scope,
post-backup checks, verified restore results and full-run metrics. No improvement
over a baseline is required. If it cannot finish inside the agreed limits, retain
the run and fix only the measured blocking issue before a fresh attempt. Do not
launch a broad optimization matrix or rebuild historical graphs automatically.

### Phase 3: Establish Regular Full Backups

**Inputs:** accepted first backup, stable live identities, persistent daemon and
explicit production deployment approval for the chosen destination.

**Work:** run unchanged and deliberately changed full-scope backups; verify both
restores and reopen/restart recovery. Install the minimum scheduler with mutual
exclusion, protected credentials, persistent storage, bounded retries and failure
alerts. Preserve full-run metrics on every run. Schedule repository/index checks
and restore drills explicitly; do not rebuild or reimport the catalog before
each backup. Qualify only the deployed working-state/source configuration, not
every backend or optimization prototype.

**Checks:** unchanged verified reuse, changed-file detection, hardlink and directory
membership, no loss of historical snapshots, no false complete-crawl claim or
global debt clearing, and exact required references after reopen. Exercise one
interrupted job/restart path, overlap denial and missed/failed-run alerting. Keep
success tied to acknowledged committed snapshots, not scheduler exit alone or
deferred staged data. Verify recovery keys and required artifacts off-host.

**Output/exit:** regular complete snapshots within the reviewed schedule/RPO/RTO,
with tested restore/restart, alerts and a concise operating runbook. Historical
inode backfill, advanced analytics, selective crawling and destructive maintenance
remain explicitly deferred. Regular backups may continue while Phase 4 analysis
is performed; no performance project is needed to declare this narrow goal met.

### Phase 4: Optimize Later from Complete-Run Evidence

**Inputs:** retained bootstrap/check/backup metrics and verified snapshots from
Phases 2 and 3. This phase is outside the regular-backup readiness critical path.

**Work:** rank costs by actual completed full-run elapsed time and work: source
metadata/data reads, chunking, lookups, reconciliation, native publication,
projection, checks, startup and finalization. Use bounded profiling only when
ordinary metrics leave a discriminating question unanswered. Investigate one
measured bottleneck at a time; separately pin and test candidates. Reuse existing
diagnostic fixtures only when they represent that bottleneck and catalog state.

**Checks:** exact snapshot/restore/reopen semantics, race/cancellation/resource
tests, equal-work comparisons with explicit cold/warm conditions and non-overlapping
measurement boundaries. Existing candidate-specific gates remain unchanged when
reusing those experiments: five pairs per shape, median at least 1.25, every pair
at least 1.10, four pairs at least 1.20, plus any separately frozen requirements.
Those speedup gates qualify those candidates, not ordinary backup readiness.

**Output/exit:** reject or accept a separately reviewed optimization without
disrupting the proven backup service. No automatic prefetch, overlap, atomic
publication, GC tuning, working-state default change, Pebble removal or historical
backfill follows from a slow-stage observation. Preserve original evidence and
keep the operational baseline available for rollback.

## Full-Run Metrics Contract

Observation is part of the backup-first workflow, not a requirement to optimize
before running it. Prefer existing JSON summaries, scheduler/check telemetry,
`reconciliation_stats`, `metadata_lookup_stats`, `commit_rpc_stats`,
`pack_publication_stats`, `working_state_stats` and direct-NFS statistics where
available. Audit field availability in Phase 1; source adapters without direct-NFS
counters must report a supported alternative or an explicit unavailable value.

### Coverage and Stage Boundaries

Observe every bootstrap, check, backup, restore verification and restart action
from before process start through result persistence, finalization and owned
process cleanup. Start the observer before the workload and join it before
sealing outcomes. Record monotonic elapsed and UTC timestamps, action/run IDs,
PID/start identity, executable hashes and native namespace/generation. Separate
one-time bootstrap costs from recurring backup costs. Do not call an inner
archiver timer the whole command duration.

| Stage or Scope | Required Measurements |
| --- | --- |
| Bootstrap | Catalog and snapshot-reference counts, completed/failed batches, setup/preload/import/handoff/reopen wall times; historical materialization explicitly deferred |
| Checks | Coverage/options/read-session identity, scanned records/bytes/ranges, findings/warnings/truncation, payload-read coverage, memory/scratch peaks and total wall time |
| Source traversal | Selected/completed files/directories, exclusions, read/stat errors, observed/reconciled/deferred identities, actual source adapter and available metadata-call counts/time |
| File data | Logical selected/processed bytes, actual source-read bytes when measurable, reused/reread files, new versus existing content, uploaded bytes/packs, chunking/compression/encryption time where available |
| Catalog lookups | Handles/calls, cache hits/misses/evictions, lookup failures and summed call time; distinguish full catalog preload from on-demand loading |
| Reconciliation | Scanned/reused/changed/deferred/failed/reconciled counters, queue high-water/wait or blocked time, allocation/publication calls/retries/errors and durations, retained-state peaks where measurable |
| Native publication | Identity-bound before/after request/operation counts, failures, retries/conflicts, queue/service/durable-wait observations, revision publication, legacy projection and pending-export/snapshot state |
| Completion and recovery | Reconciliation drain, snapshot commit, projection/flush, daemon close/reopen and post-check/restore wall times; acknowledged snapshot IDs and final pending/debt counts by scope |
| Whole owned workload | CLI and native CPU deltas, per-process and combined peak RSS, managed charge, scratch/disk high-water/free space, I/O counters and optional host/cgroup pressure/cache observations with units and provenance |

Emit low-cardinality cumulative counters and stage transitions at a bounded
cadence, for example every 5 seconds and on action completion. Retain the
existing independent 250 ms combined-RSS safety sampling. Derive interval rates
from monotonic deltas, including no-progress intervals; retain raw samples for
later reanalysis. Completion summaries must cover the entire action, not only
the last reporting window. New fine-grained timing is not required where an
existing end-to-end counter answers the immediate question; mark unsupported
substage fields explicitly and add instrumentation later if analysis needs it.

### Bounded Collection and Honest Results

- Bound collector memory, queue depth, label cardinality and artifact bytes before
   launch; account them separately from the CLI/native RSS gate and retain disk
   headroom. Reserve space for final receipts. Long backups need rotation/streaming
   retention, not unbounded per-file events or a sample list held in RAM.
- Export counts, durations and stage identities, not paths, credentials, file
   contents or unbounded per-inode labels. Keep artifacts private, versioned and
   hash-bound. Reuse the existing observer rather than adding a second monitoring
   stack or requiring a remote telemetry service before backups can run.
- Define mandatory safety/completion counters versus optional diagnostic fields.
   Missing optional substage data alone does not invalidate a durable backup.
   Mark gaps/drops/unavailable fields in the receipt; never replace them with zero.
   Loss of mandatory safety observation requires a controlled stop. Missing
   mandatory full-run coverage blocks readiness qualification.
- Test exporter capacity with all enabled stages and mandatory fields, not just
   startup output. Backpressure, export errors and dropped optional observations
   need explicit counters; telemetry must not silently terminate or block useful
   backup work indefinitely. Intrusive CPU/native profiles remain off by default.
- Keep process wall time, summed concurrent RPC time, CPU time, native service
   time and physical I/O distinct. Tree load/decode/debt and reconciliation timers
   can overlap. Do not add them as exclusive stages or subtract independent
   medians. Backend plaintext bytes are not physical NFS traffic; managed charge
   and cgroup cache are not process RSS.
- Every rate names its denominator: completed nodes/s, logical processed bytes/s,
   source-read bytes/s or uploaded bytes/s. Record cache state, source changes and
   configured concurrency. First and unchanged backups perform different work;
   their timing ratio is not a causal optimization speedup.

### Retained Run Receipt and Later Analysis

Each create-only receipt binds the approved contract, source inventory/profile,
build/configuration, daemon/storage identity, input generation and stage outcomes
to raw logs, samples and check/restore results. Include stage start/end times,
completed work, errors/warnings, resource peaks, collector health/coverage,
acknowledged snapshot IDs, pending projection/debt state and cleanup outcome.
Use separate fields for `backup_committed`, `verification_passed`,
`observation_complete` and `readiness_accepted`. A committed snapshot remains
committed if a later observer/check fails; do not blindly rerun publication or
invent a failed-data-write result because qualification is incomplete.

After the run, produce a small cost summary from the retained data: one-time
versus recurring wall time, complete-work throughput, long no-progress intervals,
dominant measured waits/CPU, resource peaks and evidence gaps. This analysis must
not rerun or rewrite the workload. Record suspected bottlenecks as hypotheses
with the next discriminating measurement, not asserted causes. Keep observations
even when no optimization is undertaken or a run fails.

## Validation Starting Points

These are execution-plan commands, not claims that they ran during this document
change. Run from the repository root with a supported toolchain, valid temporary
paths and a private pinned native test daemon. Inspect each package's fixtures
before execution: a configured ordinary daemon binary does not replace a required
test-failpoint build, and opt-in real-source environment variables must remain
unset during ordinary fixture checks.

```sh
go test -race ./internal/index/legacyimport ./cmd/vaultic/indexcmd -count=1 -timeout=180s
go test -race ./internal/index/reconcile ./internal/index/maintenance -count=1 -timeout=10m
go test -race ./cmd/vaultic/backupcmd ./internal/checker -count=1 -timeout=10m
VAULTICDB_TEST_BINARY=/private/pinned/vaulticdb go test -race -v \
   ./internal/index/daemon \
   -run '^(TestSchemaStoreImportsLegacyPacksIdempotently|TestIDSeenFilter.*)$' \
   -count=1 -timeout=180s
```

Use the existing backup integration tests in `cmd/vaultic` for command-level
proof, then add narrow named regressions for the chosen readiness boundary.
Do not treat the commands above as complete coverage of backup/check/activation,
process failure recovery or CI. The broader required gates are defined by
[Go CI](../../../.github/workflows/tests.yml) and
[native CI](../../../.github/workflows/vaulticdb.yml). Retain executed test names,
counts, skips, daemon provenance and raw output. Native crash-barrier tests need
the failpoint-enabled binary through their dedicated fixture configuration.

## LLM Execution Contract

For each phase, an implementing agent must:

1. Read the prior phase receipt, current worktree/commit and owning code/tests.
   Revalidate old notes against code; preserve unrelated user changes.
2. State the smallest falsifiable hypothesis and a focused discriminating check.
   Prefer the minimum correctness/guard/observability change that gets a full
   backup running safely. Do not implement both bootstrap routes, backfill
   historical inode graphs, or start an optimization campaign by default.
3. Make one scoped change, run the focused check immediately, then required
   regression/native/race/resource gates. Verify named tests actually executed.
4. Produce a create-only phase report linking source/binary/input hashes, commands,
   test counts/skips, raw receipts, exact verification scope, resources, findings,
   unresolved risks and `pass`, `fail` or `blocked` status. Never relabel missing
   measurements as zero, or a compiled opt-in test as executed evidence.
5. Stop on semantic divergence, failed ownership/authority, unknown publication,
   resource violation, changed frozen inputs or missing observation. Preserve
   failed evidence and clean up only demonstrably owned processes/state.
6. Advance only when the stated exit and authorization gates pass. Performance
   acceptance, production deployment and destructive maintenance require their
   own explicit approvals, not an agent's assumption of task continuation.
   A candidate speedup gate is not a prerequisite for the proven baseline backup;
   optimize before regular operation only for a demonstrated blocking issue.

## Code and Test Anchors

| Concern | Current owning surface |
| --- | --- |
| Import and debt | [import.go](../../../internal/index/legacyimport/import.go), [tree.go](../../../internal/index/legacyimport/tree.go), [import_test.go](../../../internal/index/legacyimport/import_test.go) |
| Experimental paths | [snapshot_pipeline.go](../../../internal/index/legacyimport/snapshot_pipeline.go), [snapshot_prefetch.go](../../../internal/index/legacyimport/snapshot_prefetch.go), [snapshot_pipeline_test.go](../../../internal/index/legacyimport/snapshot_pipeline_test.go), [snapshot_prefetch_test.go](../../../internal/index/legacyimport/snapshot_prefetch_test.go) |
| Populated-catalog diagnostic | [snapshot_attribution_test.go](../../../internal/index/legacyimport/snapshot_attribution_test.go) |
| Import/check CLI and guards | [cmd_index.go](../../../cmd/vaultic/indexcmd/cmd_index.go), [cmd_index_test.go](../../../cmd/vaultic/indexcmd/cmd_index_test.go) |
| Authority resolution | [repository.go](../../../internal/repository/repository.go) |
| Metadata check | [maintenance.go](../../../internal/index/maintenance/maintenance.go), [Phase 33](../04-roadmap/phase-33-index-check-scalability-and-performance.md) |
| Repository check | [cmd_check.go](../../../cmd/vaultic/cmd_check.go), [checker.go](../../../internal/checker/checker.go) |
| Backup integration | [cmd_backup_run.go](../../../cmd/vaultic/backupcmd/cmd_backup_run.go), [cmd_backup_integration_test.go](../../../cmd/vaultic/cmd_backup_integration_test.go) |
| On-demand catalog | [daemon_engine_backup.go](../../../internal/index/daemon_engine_backup.go) |
| Live identity, reuse and publication | [reconcile.go](../../../internal/index/reconcile/reconcile.go), [archiver.go](../../../internal/index/reconcile/archiver.go), [reconcile_test.go](../../../internal/index/reconcile/reconcile_test.go), [Phase 5](../04-roadmap/phase-05-backup-crawl-reconciliation.md) |
| Charged fresh-import filter | [schema_store.go](../../../internal/index/daemon/schema_store.go), [client_transaction_test.go](../../../internal/index/daemon/client_transaction_test.go) |
| Existing pilot infrastructure | [pilot.py](../../../helpers/phase35-m6/pilot.py), [observe.py](../../../helpers/phase35-m6/observe.py), [prepare.cjs](../../../helpers/phase35-m6/prepare.cjs) |

Existing roadmap status labels describe their own historical milestones. This
cross-workflow readiness assessment does not change those implementation records
or declare their target-distribution acceptance complete.