# IndexDB Backup Readiness and Execution Design

[Documentation index](../README.md) |
[Metadata resilience](metadata-resilience.md) |
[M6 preparation](../04-roadmap/phase-35-m6-preparation.md)

## Decision Summary

Assessment date: 2026-10-28. Code baseline: `4b174b5c8`, including the
experimental snapshot checkpoint `cd84d564e`. This is a design and execution
plan with bounded Phase 0 and Phase 1 evidence below, not production approval or a report
of a completed real-workload run.
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

### Phase 1 Evidence and Runbook (2026-10-28)

**Status: fixture-qualified implementation; no real workload or deployment.**
The Phase 0 tier blocker is repaired by an explicit, supervised
`index rebuild-pack-stats` step. Native tier reconciliation compares both the
pack and complete placement evidence transactionally and moves the pack/type/tier
aggregates together. Stale pack or placement evidence aborts. The original
automatic creator/timing of imported placements was not established; this is a
tested explicit compatibility repair, not a claim that import now guesses tiers.
The command-level post-backup regression now requires a clean, complete audit.

[readiness.py](../../../helpers/phase35-m6/readiness.py) is a separate, fixed
sequential supervisor reusing the existing pilot and observer. The old comparison
runner is unchanged. Its command sequence is catalog-only import, snapshot-reference
import, full index check, repository check with full payload reads and unused-blob
checking, repeated identity-bound activation check, guarded activation, complete
profile backup, tier/statistics repair, both post-checks and verified restores of
the frozen historical selections and every newly acknowledged snapshot. Each
invocation reopens durable native state; there is no blind retry or failed-run reuse.

The opt-in catalog-first activation flags bind the command, under its exclusive
repository lock, to the checked generation, revision sequence and complete
historical snapshot count. The renewable read session checks root locations and
absence of crawl debt, and is validated again before authority changes. The
supervisor still requires both preceding checks; these flags alone are not a
replacement for that evidence. Ordinary full-history activation remains unchanged.
Historical inode materialization is explicitly deferred in the receipt. No
destructive maintenance, scheduler installation or production activation is part
of this supervisor, and its receipt does not authorize those operations.

**Evidence:** the private NFS generated-repository run used source device **153**.
All 17 supervisor tests executed and passed, including three actual CLI activation
rejections (changed generation, changed sequence, wrong snapshot count), then
successful activation, backup, repair, reopen and historical/new restores. The
final generated source was
`/volume2/NASDA2/vaultic-test-m6/r1-2def4pe0/source`; its retained fixture and
evidence manifest are under
`/volume2/NASDA2/vaultic-test-m6/phase1-readiness-20261028-final-r2`.
The current-source CLI SHA-256 is
`3679ae56c7aabb28454b1e60f5bf0472d479cf9f37192dbb373c4ff2cb3f42e6`;
the unchanged, current-source failpoint daemon SHA-256 is
`322d23138c3128dc2245b1c8dd0b48627f3cb77a3475c9dfa8e4909daff79ed1`.
No Rust source changed. These hashes pin fixture executables, not production.

The native/race selection retained 31 passing cases, 10 top-level tests and zero
skips/failures: catalog-first integration, both new tier tests and all seven named
read-session tests. Entire `indexcmd` and `maintenance` package race suites passed.
That final package run retained 490 passing cases, with two macOS Secure Enclave
enrollment tests skipped on Linux and no failures.
The activation option table rejects eight unqualified combinations. The unchanged
pilot's seven self-tests and observer's four self-tests passed. The Phase 0
process-test target-path and drain-status caveats below remain recorded; this is
not a full repository or native CI pass.

Supervisor tests cover create-only sealing/receipts, frozen inputs/scope/identity,
export capacity, redirected/shared writable targets, bounded malformed events,
missing/invalid counters, incomplete history and source-job coverage, generation
and sequence changes, PID reuse/cardinality, observer readiness/loss, output
capacity/runtime failure, no downstream action after failure, and preserving
committed snapshot IDs after later observation failure. Existing maintenance
scratch-exhaustion and native read-session renewal/failure regressions also ran.

**Preparation, without execution:** supply an operator-reviewed draft JSON with
`authorization: "private-target-only"`, canonical `root`, `repository_id`, a short
private local `socket`, `password_file`, `restores: [{"snapshot": "<full ID>"}]`,
and `pins` entries containing `path` for `binary`, `daemon`, `profile`,
`source_profile`, `empty_profile`, `password` and `inventory`. The empty profile
must actually be empty. The frozen backup profile must preserve the original
52 sources, negative globs, markers and one-filesystem jobs. Included profiles,
hooks, stdin/files-from, selective/experimental and skip-if-unchanged modes are
not qualified. Keep original inputs kernel read-only before execution.

The candidate must already contain a separately approved private writable copy
of the repository at `root/repo`, not the old pilot's shared-data view. Sealing
does not copy repository data or grant target approval. Writable symlinks and
hardlinked files are rejected. The bounded JSONL inventory has one unique entry
per original config/key/index/snapshot/pack object: `path`, `size`, and `sha256`
for metadata. All 10,170 indexes, 419,853 packs and 143 snapshots must be covered;
the import must report all 380,166,821 blob records. Original and candidate
metadata bytes are compared with the pins; payload integrity is checked by the
full repository check, not inferred from file sizes.

```sh
python3.12 helpers/phase35-m6/readiness.py /private/contract.json --seal /private/draft.json
python3.12 helpers/phase35-m6/readiness.py /private/contract.json
# Only after separate Phase 2 approval of the real writable target:
python3.12 helpers/phase35-m6/readiness.py /private/contract.json --execute
```

The private target must be under the existing authorized evidence root. Sealing
pins helper/binary/profile/inventory/password hashes, directory device/inode,
resource/time limits and explicit restore IDs. The actual contract is revalidated
before every stage. `root/readiness` and its final receipt are create-only; an
existing run directory is not a resume authorization. The two bootstrap commands
share one 12-hour budget. Checks retain their 12-hour bound, backup one week,
RSS 200,000,000,000 bytes, managed memory 160 GiB, scratch 8 GiB and free-space
floor 57,481,929,687,040 bytes. The repair command does not support the managed
working-policy flags; it retains external RSS/runtime/disk supervision instead.

The observer pre-opens collectors before launch, pins executable and PID/start
identity, retains 250-ms RSS safety samples and five-second process diagnostics,
and reuses import/check five-second native telemetry. Collection continues through
native cleanup, including a final scratch measurement. Working and temporary
scratch are summed against the shared 8-GiB ceiling; the regression rejects two
5-GiB directories even though neither individually exceeds the limit.
Global metadata launchers create persistent children: native
graceful shutdown handles **SIGINT**, not SIGTERM. Unsupported-signal cleanup
left a writer claim in the first fixture; the supported bounded graceful drain
fixed that failure. No force takeover or automatic retry was introduced.

Limits are 16 owned process identities, 128 source jobs/historical restore
selections, 2-MiB event lines, 500,000 inventory entries, 512-MiB stdout/stderr per
stream, 3-GiB safety samples per action, 20-GiB telemetry per action and an 8-TiB
whole-run artifact ceiling. These are maximum capacities, not allocated output.
The telemetry allowance covers the maximum accepted event line at five-second
cadence over a 12-hour action. A 32-MiB physical reserve is released for the final
receipt. Collector RSS is separately capped at 1 GiB. Raw artifacts and rejected
stages are hash-bound by the receipt, with UTC/monotonic action timing, resource
peaks, cleanup outcome and distinct boolean `backup_committed`,
`verification_passed`, `observation_complete` and `readiness_accepted` fields;
acknowledged IDs are retained separately. Optional absent counter families and
unsupported source-call/chunking/compression/encryption timings are nullable.
The compact cost summary derives bootstrap/backup wall time, logical throughput
and peak combined RSS only from retained observations; it makes no causal
bottleneck conclusion from the small fixture.
CPU comes from identity-bound `/proc` samples; final exit ticks can be unavailable,
and sampled CPU is not an exact process-exit accounting claim. Native/RPC summed
time, command wall time, logical bytes and physical I/O remain separate.

**Phase 2 boundary:** the small fixture deliberately substitutes generated inputs
and test limits, not the frozen production-scale contract. It does not prove that
the real catalog, full payload check or 52-source backup finishes within its limits.
Real private-target approval, identity continuity across all source mounts,
execution receipts, schedule/RPO/RTO and eventual production deployment remain
separate gates. Performance acceptance and full historical analytics remain deferred.

### Phase 0 Evidence (2026-10-28)

**Exit: complete through the specific-blocker branch, not backup readiness.**
The generated command workflow works, but its post-backup `index check` rejects
two retained imported packs with `derived_tier_mismatch` (`want single`,
`got unknown`). This failed check is pinned as a regression, not waived or
reported as clean. Phase 1 must resolve the placement/derived-tier compatibility
before it can accept readiness. No production code, activation guard, observer,
scheduler or optimization was changed by Phase 0.

The owning test is
[`TestIndexBackupReadinessCatalogFirstBootstrap`](../../../cmd/vaultic/cmd_index_integration_test.go).
It uses the actual command implementations and a private native process, not
mock import/backup engines. Two historical snapshots deliberately share exactly
one legacy tree; one historical file has no device/inode identity, while two
other paths are genuine hardlinks. The fixture proves:

- Catalog-only import and separate snapshot-reference import create six blob
   records, two pack records and two snapshot references, with no historical
   inode/directory revision or crawl-debt materialization.
- Missing catalog roots, canceled import and prohibited metadata-only activation
   return errors without publishing snapshot references. Missing/corrupt tree
   payloads fail uncached repository checks and verified restores.
- Both checks pass before fixture-controlled activation. This ordering is test
   orchestration, not a new general activation/readiness guard.
- The first full-crawl authoritative backup reads 60 source bytes and reconciles
   four entries without verified reuse. The unchanged full crawl reads zero source
   bytes and reuses four entries. Rename plus delete/recreate causes three changed
   entries and reads the 20 replacement bytes. Required reconciliation failures
   and deferred entries are zero on these successful backups.
- Original imported catalog/snapshot bytes remain unchanged. Current records,
   inode/directory revisions, pack/blob records and all snapshot records are
   byte-identical across owned-daemon shutdown and persistent-local-WAL reopen.
   Every new snapshot has a committed normalized scope and an existing root
   directory revision; every materialized inode revision is verified. The
   snapshot join root intentionally has synthetic zero device/inode identity;
   this is distinct from inventing live filesystem identities.
- All five historical/new versions restore after restart with exact file bytes,
   paths, counts and size; hardlinks, representative file mode and mtime survive.
- Canceled backup, rejected snapshot save and unavailable live identities return
   errors without an acknowledged snapshot or an increased snapshot count.
   Unknown live identities remain deferred and do not fabricate current inodes.
   Rejected publication can leave unused data: the unused-data check passes before
   injection, and full data/snapshot integrity still passes afterward with unused
   checking disabled. This is not a clean unused-data result or permission to prune.
- The post-backup metadata audit has complete coverage and rejects exactly the
   two original imported-pack tier mismatches. Newly published packs are excluded
   from those findings. No unrelated finding or warning is accepted by the test.

The same generated workflow passed on local storage and in a new private
directory on `/volume2/NASDA2` (NFSv3). Device/inode/parent identities are observed
through `fs.NewLocal`, fit the schema, and remain usable through repeated full
crawls and daemon restart. This qualifies the tested adapter/mount behavior,
not identity continuity across server replacement, remount or every real source.
Only generated fixtures were written; original source/repository inputs were not
modified. No real full import, workload backup or production daemon operation ran.
The final NFS run uses `VAULTIC_TEST_TMPDIR` and asserts observed source device
153 plus the private repository path. Unprefixed `TEST_TMPDIR` is ignored by
the helper; earlier such attempts ran locally and are not NFS evidence.

Reproduce the command fixture with an explicit native executable:

```sh
VAULTICDB_TEST_BINARY=/private/pinned/vaulticdb \
   go test -race ./cmd/vaultic \
   -run '^TestIndexBackupReadinessCatalogFirstBootstrap$' -count=1 -timeout=240s
```

For NFS qualification, also set `VAULTIC_TEST_TMPDIR` to an approved private
NFS directory, keep socket `TMPDIR` local, and verify the logged source device
and repository path rather than accepting a passing test label alone.

Retained private evidence is
`/volume2/NASDA2/vaultic-test-m6/phase0-readiness-WXRjGgxT/`, including named JSON
test results, native build provenance, executable pins and the evidence manifest.
The current-source native binary was built with `--locked --features
test-failpoints` in isolated RAM-backed storage; SHA-256:
`322d23138c3128dc2245b1c8dd0b48627f3cb77a3475c9dfa8e4909daff79ed1`.
The final selected race/native run executes 22 top-level tests, 78 including
subcases, with no skips. It covers the command workflow, existing metadata-only
guards and memory-to-local WAL handoff, imported-to-verified transitions,
hardlink/move/deletion, cancellation, publication conflicts/failures, uncertain
Begin ownership and pack publication outcomes. The two existing command fixtures
still use the separately pinned repository debug executable
`0503728b75c412a89207c31e7195f2c2c4ae4aa147239128f3c538f848a5e8e2`;
the new workflow and native safety tests use the current-source binary above.
These are focused runs, not a full-suite or full-workload pass.

Known failures were reproduced and classified:

- Default process-test setup follows a stale `target/process-tests` symlink to a
   removed RAM build and fails before tests execute. The explicit current-source
   `VAULTICDB_FAILURE_TEST_BINARY` bypasses it without modifying that path;
   `TestProcessFreshLegacyImportDefersDurabilityUntilCheckpoint` then passes.
- `TestProcessDrainRejectsEveryMutationClassAndRestarts/begin-transaction` still
   expects `Unavailable` but receives `FailedPrecondition` wrapping an uncertain
   Begin reconciliation with underlying `Unavailable: ... draining`. This is an
   unchanged fail-closed status mismatch, not a successful write or a newly
   introduced failure. It remains a production-qualification caveat; the other
   five mutation subcases pass. Do not retry an uncertain operation merely because
   the underlying cause is unavailable.

**Minimum Phase 1 follow-through:** reproduce and fix the imported-pack placement
and tier-summary transition transactionally, including aggregate consistency;
do not guess imported tiers or suppress the checker finding. Then change the
explicit blocker regression into a clean post-backup audit gate. Add the guarded
sequential bootstrap/readiness supervisor and full-run observation already
specified below. It must bind checks and exact catalog/snapshot coverage to the
candidate generation, keep deferred history explicit, reject failed or uncertain
stages, and freeze source identity continuity assumptions. The checker uses two
workers/two RPCs in this fixture because unbounded host-derived defaults can
exceed native scan-stream admission. This is a bounded fixture setting, not a
large-run performance recommendation.

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

**Status:** completed via the specific-blocker exit; see
[Phase 0 evidence](#phase-0-evidence-2026-10-28). Phase 1 subsequently added the
explicit compatibility repair and clean-audit gate described above.

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

**Status:** fixture-qualified implementation; see
[Phase 1 evidence and runbook](#phase-1-evidence-and-runbook-2026-10-28).
This status authorizes neither a real run nor production deployment.

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

**Status (2026-10-31): database/WAL relocated, all 52 NetApp source mounts
enabled/active, catalog/snapshot checkpoints, live NFS mountpoint backup +
unchanged reuse + verified restore, and post-backup 17-domain `index check`
proven; full 52-source `cdot` backup remains gated on ~288.8 TiB / ~1.18B-inode
combined first-backup scale and supervisor contract assumptions (`backup_committed: true`, `verification_passed: true`, `observation_complete: true`, `readiness_accepted: false`).**

1. **Initial Read-Only Preflight (2026-10-28):** Read-only preflight against
   Phase 1 commit `ed595bbc9e8804dc8ac57ceeccdbeece2c609902` reverified all 271
   prior evidence checksums and the four accepted fixture outcomes under
   `/volume2/NASDA2/vaultic-test-m6/phase2-preflight-20261028`.
2. **DB & WAL Co-Location Next to Repository & Systemd Mount Enablement (2026-10-31):**
   - Following operator approval for exclusive writer ownership with `db` and
     `wal` co-located next to `/volume2/NASDA2/rustic/repo`,
     `vaulticdb-rustic.service` was gracefully stopped (`SIGINT`),
     `/ncl1-1-vs-50/fme_dump/amakura/db` was preserved intact, and namespace
     `e96cae6a7d178db9832fbd4f9e635bb8a855f04818f1ef1761825a647877a1e0` was
     copied into real directories `/volume2/NASDA2/rustic/db` (38 GiB) and
     `/volume2/NASDA2/rustic/wal` with `_vaultic/wal-target-v1` updated to
     `vaulticdb-local-wal-after-bulk-import-v1:/volume2/NASDA2/rustic/wal`.
     `/etc/vaultic/vaulticdb-rustic.env` and
     `/etc/systemd/system/vaulticdb-rustic.service` were updated so the local
     `vaulticdb` daemon owns `/volume2/NASDA2/rustic/db` and
     `/volume2/NASDA2/rustic/wal`. The hardlink between
     `/volume2/NASDA2/rustic/repo/config` and the historical phase32 test
     directory was broken (`st_nlink = 1`).
   - All 16 previously unmounted NetApp source exports (`12` under
     `/ncl1-1-vs-20/*`, `3` under `/ncl1-1-vs-70/*`, and
     `/ncl1-1-vs-50/IT_download`, whose missing `.mount` unit was recreated) plus
     `volume2-NASDA2.mount` were `systemctl enable --now`d. All `52 / 52`
     configured backup source mounts are now `enabled` and `active` in systemd
     with distinct mounted NFS `st_dev` IDs (`103`–`164`, all fitting `uint32`).
3. **Production Code Fixes Discovered and Verified on the Real Repository:**
   - **Mountpoint root directory reconciliation (`internal/index/reconcile/reconcile.go`):**
     When backing up a mounted filesystem root (such as `/ncl1-1-vs-20/fme_vs_dest`
     on NFS `st_dev = 152` whose parent `/ncl1-1-vs-20` is on local rootfs
     `st_dev = 1793`), `statWorker` previously rejected the mountpoint root
     directory with `cross-filesystem parent relationship` (`deferred = true`),
     writing a pending `q:` crawl-debt record on every backup of a mountpoint.
     Fixed `statWorker` and `publishDirectories` so a directory whose parent is on
     a different filesystem is recorded as a filesystem root (`ParentInode: 0`
     without `KnownParent`), while still deferring any cross-filesystem child
     edge if both parent and child directories are observed in the same crawl.
   - **Dry-run authoritative backup isolation (`cmd/vaultic/backupcmd/cmd_backup_run.go`, `internal/index/daemon_engine.go`, `internal/repository/repository.go`):**
     `vaultic backup --dry-run` on an authoritative SlateDB repository previously
     attached `reconcile.Reconciler` and called `DaemonEngine.storePack`,
     publishing pack (`p:`), blob (`b:`), and inode/directory (`i:`/`d:`) records
     to SlateDB while `dryrun.Backend` discarded the pack files on disk. Fixed
     `SetDryRun()` and `configureReconciliationHooks()` so `--dry-run` never
     mutates SlateDB.
   - **Post-backup placement scheduler with imported `TierUnknown` packs (`internal/index/maintenance/scheduler.go`):**
     `PlanPlacement` (invoked at the end of every authoritative backup) previously
     queued `rq:` placement requests for all `419,530` imported `TierUnknown`
     packs in a single `WriteMutableBatch` call, failing with
     `write batch has 419530 items, limit is 10000`. Fixed `PlanPlacement` to
     skip `TierUnknown` packs (matching `RebuildPlacementRecords` and
     `checkPlacementRecords`).
4. **Completed Catalog Checkpoints, Live NFS Backup + Unchanged Reuse + Restore,
   and Full 17-Domain `index check`:**
   - `vaultic index import --resume --snapshot-depth=0 --working-memory-mode=ram --working-memory-ram-limit=160GiB`:
     completed in 47m18s (`EXIT=0`), resuming `10,019` legacy index checkpoints and
     recording the remaining `151` exported-index checkpoints (`10,170 / 10,170`
     total, `checkpoint: 971ca81afba7be561ec62dae258ced15d52b8f1cee16e9e1bd364ed8af3e22e4`,
     `warnings: 0`, `errors: 0`).
   - `vaultic index import --resume --snapshot-metadata-only`: completed in 7.98s
     (`EXIT=0`), confirming all `143 / 143` historical snapshot references.
   - `vaultic index rebuild-pack-stats`: scanned all `419,853` packs in 1m10s
     (`EXIT=0`, `aggregates_changed: 0`, `tier_summary_changed: 0`).
   - Pre-backup 17-domain `vaultic index check` in RAM mode (`--check-workers=2 --check-rpc-concurrency=2 --max-findings=100 --fail-on-warning --working-memory-mode=ram --working-memory-ram-limit=160GiB --working-memory-disk-budget=8GiB`):
     passed (`EXIT=0`, `warnings: 0`, `findings: 0`, `380,166,821` locations,
     `143` snapshots, `173` encrypted objects, `scratch_peak_bytes: 0`).
   - Live NFS backup 1 (discovery) on mounted NetApp export `/ncl1-1-vs-20/fme_vs_dest`:
     published snapshot `12d304e05351793d7e6f07ac91f37aea4a4b44529e0b279fd23423fecca242c1`
     (`scanned: 219, changed: 219, reused: 0, reconciled: 219, deferred: 0, failed: 0`).
   - Live NFS backup 2 (unchanged reuse) on `/ncl1-1-vs-20/fme_vs_dest`:
     published snapshot `9582f977f96e14efccf4cb9b60f7944904352152a97f1a894818260e1377ce0d`
     (`scanned: 219, reused: 219, changed: 0, reconciled: 0, deferred: 0, failed: 0, data_added: 0`).
   - Verified restores (`restore --verify`) succeeded (`EXIT=0`, `220 / 220` files,
     `206,650 / 206,650` bytes) for historical snapshot `770fd42a...` and both new
     authoritative snapshots `12d304e0...` and `9582f977...`.
   - Post-backup 17-domain `vaultic index check` passed in 4m17s (`EXIT=0`,
     `warnings: 0`, `findings: 0`, `legacy_indexes: 10171`, `legacy_snapshots: 145`,
     `slatedb_snapshots: 145`, `legacy_locations: 380166898`,
     `slatedb_locations: 380166898`, `pending_crawl_debt: 0`, `pending_exports: 0`).
5. **Remaining Scale & Supervisor Blockers for the Combined 52-Source `cdot` Job:**
   - **52-source combined job scale and parent grouping:** Across all 52 mounted
     NetApp exports, there are **288.80 TiB** of used data and **1,179,840,102**
     used inodes. Whereas the 143 historical Rustic snapshots were created per
     individual source path (57 path groups), `[[backup.snapshots]]` in
     `/volume2/NASDA2/rustic/etc/rustic.toml` combines all 52 sources into a
     single `cdot` snapshot job. Because no historical parent snapshot has all 52
     paths (and historical inodes are unverified in catalog-first mode), a single
     combined 52-source run must read, chunk, and publish verified inode
     revisions for all ~288.8 TiB / ~1.18B inodes.
   - **Full-repository `check --read-data` duration:** `readiness.py` runs
     `check --read-data --check-unused` before and after backup across all
     `420,562` pack files (`133.5 TB` in `repo/data`), requiring ~74 hours per
     pass over NFSv3 vs. the 12-hour `check_seconds` limit.
   - **Supervisor contract (`helpers/phase35-m6/readiness.py`) assumptions:**
     `load_contract()` enforces `AUTH_ROOT` (`/volume2/NASDA2/vaultic-test-m6`),
     `len(repo/data) == 419,853` (actual: `420,562` including `708` unreferenced
     legacy packs), and fresh-import counters rather than resumed checkpoints.
   - All stage outputs, source-mount audit, pre-backup state snapshot
     (`/volume2/NASDA2/rustic/.state-snapshots/pre-phase2-20261031`), and the
     hash-bound evaluation receipt are retained under
     `/volume2/NASDA2/vaultic-test-m6/phase2-readiness-20261031-live`.

**Snapshot modes (2026-10-31):** The default remains one snapshot containing
all sources of each profile job, or all positional CLI sources. Set
`snapshot-per-source = true` in `[backup]` or a `[[backup.snapshots]]` job,
or pass `--snapshot-per-source`, to create one independent snapshot per
resolved source. Each snapshot selects its own parent and commits independently.
Ordinary per-source runs share one append-locked repository and load its catalog
once per job. Archiver, reconciliation, filesystem and progress state remain
per snapshot. Deferred and on-demand metadata sessions remain isolated.

To coalesce selected directories, put them in one job and set
`snapshot-per-source = false` and `coalesce-sources = true` on that job.
Both modes enabled simultaneously are rejected. CLI overrides may use
`--snapshot-per-source=false --coalesce-sources` to override inherited settings.
Distinct named jobs are never merged. Job hooks surround the whole job, not
each source. Partial-source errors are retained while valid sources finish;
fatal errors stop the job without undoing earlier committed snapshots.

Readiness now counts effective snapshots, accepts imported-plus-resumed index
and snapshot completion, and treats resumed pack/blob counters as invocation
deltas. Its private execution authorization and frozen inventory are unchanged.
For the live repository, use the read-only evidence audit against the actual
pre-crawl baseline, not the original import counts:

```sh
python3.12 helpers/phase35-m6/readiness.py executed-profile.toml \
   --audit-live baseline-index-check.json backup.jsonl final-index-check.json
```

The audit validates snapshot cardinality, reconciliation, full metadata checks,
repository identity/generation, and non-regressing catalog counts. It emits
input hashes and `crawl_metadata_validated`, but deliberately does not grant
`readiness_accepted`: full repository data checks, verified restores, persistent
daemon reopen and pathdiff qualification remain separate gates. Keep the exact
executed profile and command/observer evidence alongside these outputs.

**Resume:** complete the guarded 52-source full crawl, reconcile post-run pack
statistics, validate the live evidence, and complete the remaining gates.
The old private-copy supervisor cannot execute against the live repository;
its read-only-source-repository contract must not be bypassed. Full data-check
runtime and production observation budgets require explicit review before
regular scheduled backups. Configure and qualify pathdiff after the crawl.

**One-Off Live Crawl Retry (2026-11-01):**

The initial RAM-only preflight stopped at 21:31 UTC before backup, with no
committed snapshots. A checker spool exhausted its local memory quota and
attempted a spill, which RAM-only mode forbids even when the aggregate allowance
has headroom. The failed receipt is retained under
`/volume2/NASDA2/vaultic-test-m6/phase2-full-crawl-20261031-per-source`.
The retry enables bounded KV scratch for metadata checks without increasing
memory, disk, runtime, or observation limits. Backup remains RAM-only.

The KV retry passed legacy/encryption scanning and reached `catalog_join`, but
stopped on a mandatory sampling gap exceeding one second, again before backup.
Its receipt is retained under
`/volume2/NASDA2/vaultic-test-m6/phase2-full-crawl-20261031-per-source-kv`.
Mandatory RSS, executable, swap, and process-identity sampling now runs independently
of filesystem diagnostics and log writes, with a bounded sample queue. The
one-second sampler guard remains enforced; sampler stalls and queue exhaustion
abort owned work. Filesystem measurements retain their observation timestamps
and must not become stale beyond ten seconds. Cleanup uses the same stage-specific
scratch cap as observation rather than incorrectly reverting to 8 GiB.

The independent-observer retry completed location comparison but stopped when
the checker deleted a temporary run file between directory enumeration and size
measurement. No backup was committed; its receipt remains under
`/volume2/NASDA2/vaultic-test-m6/phase2-full-crawl-20261031-per-source-observed`.
Scratch measurement now uses a single no-follow stat and tolerates concurrent
entry deletion only. Symlink files/directories and non-ENOENT measurement errors
still fail closed; disappearance of the working root remains an error.

The scratch-safe run passed preflight and processed the first source for about
2h45m, then stopped on another mandatory sampling gap. No snapshot was
acknowledged; its receipt and partial reconciliation counters remain under
`/volume2/NASDA2/vaultic-test-m6/phase2-full-crawl-20261031-per-source-scratch-safe`.
The initiating observation failure must not be confused with the subsequent
reconciliation cancellation cascade. Repository state was not rolled back.
Optional `/proc` I/O and scheduling diagnostics are now collected separately
from mandatory RSS/process/executable/swap sampling and filesystem checks.
Their observation age is explicit, and they are retained sparsely. The
one-second mandatory guard is unchanged; future failures retain their timing
phase and duration. Blocked-diagnostic regressions pass, but production success
still requires the complete live workflow. The retry begins with a fresh full
metadata check before any further backup writes.

- The production profile now explicitly sets `snapshot-per-source = true`.
   All 52 source roots were confirmed to be active NFS mounts. Ordinary CLI
   multi-source coalescing and explicit per-job coalescing remain available.
- The separate `helpers/phase35-m6/live_readiness.py` supervisor uses an explicit
   live authorization, current baseline check, binary/helper/configuration hashes,
   source mount inode/device identities, and a pinned existing daemon process.
   It does not import, activate, replace the daemon owner, or schedule backups.
- Failed transient unit: `vaultic-full-crawl-20261101-light-sampler.service`. No timer was created.
   The unchanged database owner remains `vaulticdb-rustic.service`.
- Contract and evidence:
   `/volume2/NASDA2/vaultic-test-m6/phase2-full-crawl-20261101-per-source-light-sampler`.
   Contract SHA-256:
   `c5294b6b12c6aa374f115af16ca3d1f4694fc14fe99daab0bd146f574826c9ce`.
- Limits: 30 days for the crawl, 14 days for each check, 256 GiB combined
   CLI/daemon RSS, 160 GiB managed backup memory, 8 GiB backup scratch,
   96 GiB metadata-check memory/scratch, 1 TiB artifacts, and a
   `57,481,929,687,040`-byte repository-filesystem free-space reserve.
   The supervisor samples every 250 ms, checks identity-bound executable hashes
   and swap, and stops its own work on breached safety limits. The CLI/supervisor
   unit has an additional 240 GiB cgroup memory cap and swap/core dumps disabled.
- Stages: fresh full metadata check, forced full per-source crawl without
   pathdiff/FSEvents, pack-stat reconciliation, post-crawl full metadata check,
   full repository `check --read-data`, then bounded snapshot-derived verified
   restore samples for each source. Empty-directory samples are allowed only
   when a bounded snapshot listing establishes that the directory is empty.
- Supervisor logs are `supervisor.stdout` and `supervisor.stderr`. Stage logs,
   observer samples, and verification records are under `readiness/`. Normal
   completion or an observed failure writes `readiness/receipt.json`; a forcibly
   terminated supervisor may leave only stage logs and the reserved receipt space.
   Previously acknowledged snapshots remain committed and must not be discarded.
- Stop procedure: `systemctl stop vaultic-full-crawl-20261101-light-sampler.service`.
   This does not stop the independently owned database service. Do not edit
   pinned helpers, binaries or profiles while the contract is executing.
- Validation before launch: focused Go snapshot/profile/configuration and
   native authoritative `restore --verify` checks passed; all 28 Python readiness
   tests passed with the exact production CLI/native daemon and no skips.
   Four observer unit tests also passed.
   The broad Go suite remains affected by unrelated root-permission fixtures,
   default test-version mismatch, and daemon process-fixture/build issues.

**Preflight Verified (2026-10-31 22:40 UTC):**

The initial full metadata check completed with exit 0, complete observation,
and no observer errors, findings, or warnings. Repository generation 1 and
sequence 567157 match the retained verification record. All 10,171 legacy indexes,
145 snapshots, and 380,166,898 blob locations were checked. Peak combined RSS was
82,919,006,208 bytes; measured scratch peak was 38,247,645,772 bytes, within caps.
The imported-pack lifecycle count is 419,530; it is not the total catalog-pack
count. The controller entered the forced per-source backup stage at 22:40:35 UTC.
No completed source snapshots were observed at this milestone. Both preflight
`observation.json` and `verified.json` are retained under the scratch-safe run's
`readiness/metadata-check` directory, not the new retry's evidence root.

This is a historical preflight milestone, not phase acceptance. Persistent daemon reopen,
pathdiff qualification, resolution of all crawl findings, and final phase
reevaluation/commit remain outstanding. Scheduling is still disabled.

**Interrupted Publication Recovery (2026-11-01):**

The light-sampler retry completed its full metadata preflight with exit 2,
`metadata indexes differ`, rather than an observation failure. Backup did not
start. It found 6,636 locations absent from legacy indexes, one missing legacy
pack, and one pending export. All 145 snapshots still matched, with zero
aggregate, reverse-edge, snapshot-commit, path-version, and unresolved-reference
mismatches or pending crawl debt. The pending pack
`f3be9c75f04b08f8e9f507ad9adad1f3cd530350d09c1dd7e45946213f7c846a`
exists physically as an 8,661,092-byte file. These are incomplete compatibility
publication findings after interruption, not evidence that any snapshot was
successfully committed by that crawl.

`index export --pending-only` selects only `export-pending` lifecycle records.
It does not rewrite imported or already-published packs, unlike the broader
default export. It rejects `--full` or nonzero `--since`. Preview uses
`--dry-run`; execution uses `--verify` and acknowledges publication only after
the compatibility index is written and read back. A subsequent full metadata
check remains mandatory. Optional `--expected-pending-packs` and
`--expected-pending-blobs` must both be positive and require `--pending-only`.
They reject a changed selection before writing any index, within the export's
exclusive repository lock. The supervisor forwards these counts for preview
and execution rather than relying only on a separate preview. Tests cover lifecycle preservation, preview, resume,
native imported-pack exclusion, and fail-closed supervisor receipts.

The live supervisor offers separately selected recovery rather than adding a
repair implicitly to the crawl stages:

```sh
python3.12 helpers/phase35-m6/live_readiness.py "$CONTRACT" \
   --recover-pending-exports --expected-pending-packs=1 --expected-pending-blobs=6636
```

This first previews the exact reviewed pack/blob counts, then performs only the
pending export, then runs the full post-metadata check. `--preview-pending-exports`
stops after preview. Each mode needs a freshly sealed create-only evidence root.
Observation failures or count mismatches stop the workflow, with reserved
receipt space; recovery does not create a snapshot or accept readiness.

The first recovery preview was blocked by a stale shared read lock belonging to
PID 44017 on this host, timestamped 2026-10-31 16:58:47 UTC. The process was
confirmed absent and the sole encrypted lock record archived under
`/volume2/NASDA2/vaultic-test-m6/phase2-pending-export-recovery-20261101/stale-lock.encrypted`.
Ordinary `unlock` cleared only stale locks; `--remove-all` was not used. The
lock-blocked receipt remains in that root, with no export performed.

The guarded retry uses unit
`vaultic-pending-export-recovery-20261101-unlocked.service` and evidence root
`/volume2/NASDA2/vaultic-test-m6/phase2-pending-export-recovery-20261101-unlocked`.
Its contract SHA-256 is
`39245455b2936ee3fae9e2d6150f8e8d20df78866c0ce01efe0f059cf9272b74`.
The existing daemon owner and all previous safety limits are unchanged. Inspect
its receipt and stage results separately: all three commands completed with
exit 0 and complete observation, but the supervisor subsequently rejected the
snapshot count because it incorrectly used post-backup accounting for recovery.
The original failed receipt is retained, not rewritten as a success.

The preview selected exactly one pack and 6,636 blobs without writing an index.
Execution wrote verified compatibility index
`6755f4411aa21a740f7f177cced0c1de89d87dc484286c1f7ac2e946cf196ecc`,
with export sequence 1. The full post-recovery check validated 10,173 legacy
indexes, 145 matching snapshots, and 380,206,676 matching blob locations, with
zero findings, pending exports, or warnings. Its read session is
`txn-101997-327128`, generation 1, sequence 942157. No recovery snapshot was
created. Recovery now validates the unchanged baseline snapshot count rather
than incorrectly expecting 52 additional snapshots. Real-validator regression
tests accept 145 and reject 197 in this recovery-only workflow.

**Recovered Full Crawl Retry (2026-11-01):**

The next create-only run uses
`vaultic-full-crawl-20261101-recovered.service`, with evidence under
`/volume2/NASDA2/vaultic-test-m6/phase2-full-crawl-20261101-per-source-recovered`.
Its contract SHA-256 is
`0fec9eec2234b71dd554cbcbf00178be0f8695970a88950b114753d114a39d43`.
The baseline is the clean full post-recovery metadata check above. All 30
readiness/native tests pass with the rebuilt CLI, including in-lock expected-count
rejection, unchanged-snapshot recovery accounting, and verified fixture restores.
The pending-export Go tests also pass. The existing daemon remains PID 101997.

This run begins with another full metadata check and enters the forced 52-source
crawl only on success. Full post-crawl checks and per-source verified restores
remain mandatory. Stop procedure:
`systemctl stop vaultic-full-crawl-20261101-recovered.service`. Do not edit the
pinned helpers, binaries, or profiles while it executes. No timer or pathdiff
deployment was created; phase acceptance and the final commit remain gated.

**Stopped Throughput Checkpoint (2026-11-01):**

At the operator's request, `vaultic-full-crawl-20261101-recovered.service` was
stopped before completing the first source. The independently owned
`vaulticdb-rustic.service` remains running as PID 101997. The final retained
status reports 15,384 backup seconds, 948,758 processed files, and
221,881,268,444 logical processed bytes. No snapshot acknowledgment was observed.
Systemd terminated the supervisor; its successful unit stop is not successful
crawl validation. Stage logs and `readiness/receipt.reserve` are retained, but
there is no final supervisor receipt. Partial repository publication was not
rolled back and must be assessed before any future production restart.

The implementation is committed as an explicitly incomplete operational
checkpoint at the operator's request. Full source coverage, post-crawl data
checks and restores, persistent daemon reopen, pathdiff qualification, and final
phase acceptance remain outstanding. Scheduling stays disabled. The current
throughput is unsuitable for a 1.5-billion-inode crawl: recent file-based inode
proxies ranged from about 32/s to a cumulative 65/s. Further work must measure
and address metadata lookup/read amplification without weakening encryption,
durability, consistency, or observation safeguards. Do not automatically restart
production merely because a microbenchmark improves.

**Local Throughput Implementation (2026-11-01):**

Checkpoint commit `7e733c75d` precedes these performance changes. Production
remains stopped; neither the production daemon nor its pinned profile was changed.
CPU samples showed read/decryption/copy amplification, while native publication
attribution also exposed serial groups waiting on separate revision-reservation
and publication flushes. The 100 ms durable flush policy remains unchanged.

Current-pointer lookup is prefetched per independent publication group. Current
inode/reference-count and manifest-edge reads are batched within the publication
transaction, retaining pending-count precedence, shared-content read-your-writes,
conflict retry and durable acknowledgment. Reference reads use at most 256 keys
per RPC and obey the negotiated batch-item limit.

`backup --metadata-publication-group-size=64` opts into wider atomic groups.
The default remains four; zero also selects four. Sizes above four use one
transaction for revision allocation and changed-inode publication. Groups split
at the existing 4,096-content-ID, 8 MiB input and per-inode related-metadata
limits; individually oversized inputs retain scalar publication. Existing
duplicate-identity/path, hardlink and directory ordering boundaries remain.

Local native attribution, 128 inodes and the same 100 ms flush policy:

| Fixture | Regular groups of four | Wide atomic groups | Logical result |
| --- | --- | --- | --- |
| One content ID per inode | 6.451 s | 0.383 s, two commits | Identical snapshot hash; reopen validated |
| 128 independent IDs per inode | 6.444 s | 1.097 s, four commits | Identical snapshot hash; reopen validated |
| 128 shared IDs per inode | 6.452 s | 1.183 s, four commits | Identical snapshot hash; reopen validated |

The 128-ID groups split at 32 members. Before reference-read batching, their
atomic publication took about six seconds despite only four commits: increasing
group size alone was insufficient. An offline encrypted SST test separately
reduced 512 logical lookups from about 895 ms/1,697 backend reads to
79 ms/96 reads using multi-get; this was a debug-build diagnostic, not production
backup throughput.

Actual encrypted per-source backup tests exercise wide groups, unchanged parents
and verified restores. Shared-manifest transactions also pass with a negotiated
16-item RPC limit. Boundary, rollback, fence and cancellation checks remain
required. These disposable fixtures do not reproduce the existing 380-million-
location production database or NFS crawl. Fresh metadata assessment, sealed
binary/configuration pins and representative end-to-end qualification are still
required before a production restart or a new full-scope ETA.

**Private Large-Catalog Qualification (2026-11-01):**

Evidence: `/volume2/NASDA2/vaultic-test-m6/throughput-ab-20261101-indexfix-k3pA7b`.
Both cells started from independent checksum-verified copies of the closed,
encrypted October 1 seed (143 snapshots), using the pinned candidate CLI and
production daemon binary, 100 ms flush, 128 MiB metadata cache and 512 MiB block
cache. The frozen input contains 512 regular files and 168,688,466 logical bytes.
This is a historical catalog and flattened private sample, not a production NFS
crawl or a complete source-directory qualification.

| Publication group | CLI wall time | Files/s | CLI CPU time | Daemon CPU time | Verified restore wall time |
| --- | --- | --- | --- | --- | --- |
| 4 | 257.817 s | 1.986 | 702.305 s | 909.880 s | 220.089 s |
| 64 | 250.047 s | 2.048 | 691.502 s | 908.040 s | 215.502 s |

Both variants passed exact restored-name/content digest checks and encrypted
database reopen. The selected source tree is identical:
`1348614c17bb3c4c82a5c5d2fcfb1587d0e2c19c60d660c8e6c7dfa7ad5a432d`.
The full root tree IDs differ only because creating the second disposable cell
changed an ancestor directory's timestamps; decoded node comparison verified
that no selected-source metadata differed. Reopen checks compare each snapshot
against its own original full tree ID.

Verification helpers initially failed on index loading, scratch creation and
owned-client shutdown. These failures remain retained; guarded verification-only
resumes used the original backup measurements, not rerun backup timings. Restore
logged missing alternate catalog pack locations in the deliberately partial
private repo, then recovered using the copied locations; final verified bytes
and digests matched. This is not evidence that all historical packs were checked.

The roughly 3% wall-time reduction in this one ordered pair does not substantiate
a drastic end-to-end throughput gain. Most backup and restore time occurred
before file progress. The normal path loads the full catalog unless
`--metadata-on-demand` selects bounded point lookups. A separate fresh-clone,
reversed-order (64 then 4) comparison selects that existing option with a 64 MiB
lookup cache; its corrected fixture results are recorded below. Production binaries, profile,
daemon and stopped backup state remain unchanged. Representative metadata
topology, current production partial-state assessment and an approved pilot are
still required before restart, scheduling or a full-scope ETA.

The initial on-demand attempts exposed private-fixture omissions, not accepted
throughput results: managed RAM policy rejects an explicit `--metadata-scratch`,
and authenticated pending-pack recovery requires bytes absent from the partial
repo. The exact pinned CLI now passes a small encrypted backup/restore with
managed RAM and on-demand lookup. A bounded read-only inventory found 18 seed
pending-export packs (8,589,438,290 bytes), all available for private copying.
The corrected comparison copies these before each backup, keeps recovery inside the
timed backup, and shares the 16 GiB pack-copy ceiling with restore materialization.
It does not mark packs published or skip recovery. The earlier pair did not
exercise that historical pending-pack recovery and must not be treated as a
fully matched startup comparison with the corrected fixture.

The corrected on-demand pair passed in
`/volume2/NASDA2/vaultic-test-m6/throughput-ab-20261101-pendingpackfix-N7b2Rk`:

| Group (execution order) | CLI wall time | Files/s | CLI CPU time | Daemon CPU time | Verified restore wall time |
| --- | --- | --- | --- | --- | --- |
| 64 (first) | 38.305 s | 13.366 | 6.707 s | 52.720 s | 218.368 s |
| 4 (second) | 44.731 s | 11.446 | 6.615 s | 58.390 s | 213.834 s |

Both source trees match, exact 512-file/168,688,466-byte restores pass, and both
encrypted database reopen checks pass. Group 64 reduced wall time by 14.4% in
this pair. Its reported backup duration was 9.394 s, compared with 38.305 s total
CLI wall time; CLI RSS at completion was 666,152,960 bytes. The lookup cache
reported 662 hits, 2,410 misses and 583 size RPCs. Restore still used normal
full-catalog loading and logged alternate-location retries before exact-content
verification succeeded. Total copied pack bytes per cell were 16,962,797,076,
within the shared 16 GiB ceiling.

A final fresh group-64 full-catalog control uses the same pending-pack-complete
fixture, durability, daemon caches and frozen input to isolate lookup mode.
It passed in
`/volume2/NASDA2/vaultic-test-m6/throughput-ab-20261101-fullcatalog-control-R6m8Qp`:

| Group 64 lookup mode | CLI wall time | CLI CPU time | Daemon CPU time | CLI RSS at completion |
| --- | --- | --- | --- | --- |
| Full catalog | 243.886 s | 686.520 s | 898.110 s | 24,569,802,752 bytes |
| On demand | 38.305 s | 6.707 s | 52.720 s | 666,152,960 bytes |

Both passed exact restored-content and encrypted reopen checks. On-demand
lookup was approximately 6.4 times faster in total CLI wall time in this
single-run control comparison. This supports full-catalog startup loading as a
substantial cost for the large historical catalog; it does not establish a
6.4-times improvement in steady-state production inode throughput. The earlier
partial-repo pair is not the matched control. Input paths/inodes differ between
the control and on-demand evidence roots, while frozen content, seed and resource
settings match. Order/cache effects and the small flattened workload remain
limitations. Normal full-catalog restore still took about 217 s in both modes.

This private benchmark sequence is finished; production binaries/configuration
remain unchanged and the production crawl remains stopped. Current production
partial-state audit, representative directory/hardlink/NFS qualification and an
approved bounded pilot remain outstanding. There is no full-phase acceptance,
scheduled backup authorization, or billion-inode ETA from these results.

**Main-Backup Critical Path (2026-11-01):**

The retained CPU profile was from the database daemon, not the CLI. CPU
hotspots are supporting evidence only: prioritize the dependency blocking
progress. After the first hour, the stopped production crawl sustained
33-38 files/s across successive half-hour windows. The final file-progress
plateau was only 44 seconds; it does not establish a long finalization stall.

A read-only production daemon timing snapshot found 857,515 Commit requests,
57,121.310 seconds of summed Commit request time, and 52,304.785 seconds of
summed durable waiting (91.6% of Commit request time). Engine submission was
98.947 seconds, transaction-map lock waiting 7.849 seconds, transaction-slot
lock waiting 6.954 seconds, and engine backpressure count zero. These are
cumulative since process start, not an isolated main-backup interval or wall
time percentages. Nested timers and simultaneous requests must not be added.
No backup, production restart, binary/configuration change, or metadata write
was performed to obtain this snapshot.

The controlling dependency is completed-file callback -> bounded input queue
-> stat/preparation workers -> bounded prepared queue -> sequential publication
writer -> DB planning/read RPCs -> Commit -> durable completion. Default
publication groups contain four inodes and join before the next group starts.
This couples scanner throughput to durable publication rounds; CPU resources
elsewhere cannot break that dependency. The main optimization target is more
useful publication work per durable round, preserving acknowledgement and
recovery guarantees, rather than weakening durability or optimizing arbitrary
CPU-hot code.

With the instrumented CLI, `--json` emits `backup_critical_path` every 30 seconds
and at main-phase completion. Sampling starts immediately before archiving,
after index loading; RPC and daemon counter baselines exclude startup. Fields:

- `reconciliation.writer_phase` and `writer_phase_elapsed_ns` show the writer's
   current phase. `writer_wait_prepared_ns`, `writer_batch_ns`,
   `writer_hardlinks_ns`, `writer_directories_ns`, and `writer_root_ns` are
   completed, sequential phase durations. Collection includes batch assembly;
   it is not a pure source-I/O timer. The current phase is reported separately.
- Queue depths, capacity, `observe_blocked`, and `prepare_blocked` expose
   backpressure. `stat_active` distinguishes active stat work from workers
   blocked on publication. Worker time sums are not wall time; blocked-worker
   totals update when waits finish, while active counts expose ongoing stalls.
- `rpc` separates writer and borrowed lookup connections and reports method,
   completed calls, failures, summed elapsed worker time, active calls and oldest
   active age. `Get`/`MultiGet` versus `Commit` makes DB-read versus DB-write
   waiting visible. Diagnostic WriterStatus polling is excluded.
- `db` reports main-interval engine submission, durability, admission and
   transaction-lock timing, with active/oldest waiters. `db_scope` is explicitly
   `daemon_all_clients`: these server counters may include other clients.
   Missing snapshots/baselines and daemon restarts are flagged; lifetime totals
   are never silently substituted for interval totals.

Full queues plus blocked callbacks/preparation and a publishing writer expose
metadata backpressure. Match active CLI RPCs against DB durable/engine/lock
waiters to locate its downstream cause. An empty prepared queue with a collecting
writer instead points upstream; it does not alone distinguish NFS, hashing or
upload. Compare interval progress and exclusive writer phases, not CPU share or
summed parallel RPC durations. Gated read/publication tests verify that holding
either DB dependency propagates to callbacks and releasing it resumes progress.
The new instrumentation is not deployed; production remains unchanged/stopped.

**Real-Source Qualification Gates (2026-11-01):**

The live-source workload is the first 10,000 eligible lexical entries under
`/ncl1-1-vs-50/fme_cae1`, excluding every `.snapshot` subtree before assigning
the cap. The sealed corrected workload has 9,209 regular files, nine symlinks and
2,460,946,936 logical bytes; its content digest is
`2f76a6f4397effad64c9922cc6337a836b3c4ac21b870114942e02b63b996f80`.
This is a bounded partial workload, never a complete source baseline.

Four attempts were rejected before qualification: `cap10k-a` lacked the
subprocess authoritative feature flag; `cap10k-b` allocated almost its entire
cap inside an excluded NetApp snapshot and actually processed only one file;
`cap10k-c` used the forbidden on-demand/dry-run combination in its preflight;
`cap10k-d` incorrectly compared regular-file summary counts with the digest's
regular-file-plus-symlink count. Its processed byte count matched exactly;
source reinspection confirmed that all nine extra entries were symlinks.
Their private evidence is retained under
`/volume2/NASDA2/vaultic-test-m6/critical-path-fme_cae1-20261101-*`.
None establishes sustained source throughput or an accepted A/B result.

The untimed count preflight uses full-catalog lookup with `--dry-run`; the
timed main backup retains on-demand lookup. Native fixture coverage verifies
that this combination leaves the durable sequence unchanged and no active
transactions or write intents. A native CLI regression also verifies summary
classification with a symlink. On each historical clone, preflight and actual
processed counts must equal the separately sealed regular-file count, while
the restored digest/count includes every selected symlink. Acceptance additionally
requires matching selected trees, verified restored content, bounded provenance
and encrypted daemon reopen. Lookup startup is not main-phase timing.

Snapshot-consistent read-only production inventory at durable sequence 857514
found 419,864 pack records, 145 snapshot records, zero crawl-debt records and
one pending compatibility-export pack. The pending pack is 8,813,968 bytes
with 10,329 blobs; its ciphertext content-address hash matches
`bbb317d2bfbc75a504209a09306dce9caae5a70f284eb81f5c3559259862c590`.
All three prefix scans completed; zero Commit RPCs were issued and the durable
sequence was unchanged. Exact IDs/counts are retained in the create-only
`production-partial-audit-2m.json` artifact in `cap10k-d`.
This is an inventory and content-address check, not a full metadata consistency
proof. Recovery and any production pilot remain gated; no full crawl or
schedule has been resumed.

**Accepted Bounded Main-Phase Pair:**

`cap10k-e` completed both fresh encrypted historical-clone cells, with exact
regular-file counts, all-entry restored digests, matching selected source trees
and encrypted daemon reopen. Main-phase telemetry, not CLI wall time or the
archiver summary timer, gives the comparison:

| Main-Phase Measure | Group 4 | Group 64 |
| --- | ---: | ---: |
| Elapsed seconds | 346.068 | 225.651 |
| Regular-inode writer seconds | 259.194 | 139.242 |
| Collect/prepare writer seconds | 45.993 | 45.505 |
| Directory writer seconds | 40.222 | 40.243 |
| Commit requests | 10,210 | 1,675 |
| Summed durable-wait seconds | 752.822 | 111.992 |

Main elapsed time fell 34.8%; total CLI wall time was 373.682 versus 253.301
seconds. Full input/prepared queues, 59 blocked callbacks and 128 blocked
preparation workers coincide with publication phases. Summed callback and
preparation blocking roughly halved. At directory publication, four active
Commit/durable waiters remain. Admission, transaction locks and engine
backpressure do not account for the observed delay. Worker-time sums remain
nested/parallel diagnostics, not additional wall-time percentages.

Both source trees are
`2a303f1d9b720ac52eb83e8ef1ffdb2e078facb5bd7095404fd351d204dabe8b`.
The verifier recovered through alternate locations when disposable copies
lacked some historical alternate packs; exact restored content passed, but
absence of restore retries is not claimed. One ordered pair, a 10,000-entry cap,
historical rather than current production metadata and warmed caches limit
generalization. This is not full-source or sustained-production acceptance.

Only 26 group-64 publications reached 64 members; 685 atomic groups averaged
about 13.5 entries. Queued directory entries unnecessarily flushed pending
regular-inode groups. The narrowly scoped follow-up retains pending groups
across these entries only in opt-in atomic mode; default grouping, conflicts,
failures and hardlink barriers remain unchanged. Its native grouping regression
and full reconciliation race suite pass. A separate digest-pinned candidate
preserves the accepted binary.

The `cap10k-f` follow-up passed the same regular-file count, exact all-entry
restore and encrypted-reopen gates, with the identical selected source tree.
Main time was 184.386 seconds: 18.3% below the original group-64 cell and
46.7% below group 4. Regular-inode writer time fell to 97.731 seconds;
collect/prepare and directory times remained 45.775 and 40.226 seconds.
Atomic groups fell from 685 to 145, with 143 full 64-member groups. Commits
fell to 1,135 and summed durable waiting to 84.558 seconds. This localized
change supports the publication-backpressure diagnosis without changing
durability. The single follow-up retains the ordered-run/cache/historical
catalog caveats; it is not a production throughput guarantee.

Remaining measured waits are regular-inode publication (about 98 seconds),
upstream collection/preparation (about 46 seconds, not pure source I/O) and
directory publication (about 40 seconds, with active durable waiters).
Do not classify summed MultiGet time as exclusive wall time or infer NFS
saturation from an empty prepared queue. Production recovery, full metadata
consistency and a guarded partial pilot are still separate acceptance gates.

**Production Recovery (2026-11-02):**

The observed count-guarded recovery completed in 7h 8m. Preview selected exactly
one pack and 10,329 blobs without writing an index. Recovery wrote and read-back
verified index
`1914045bd37561ca4c85b262c302717aae807ab8bc1e7eac6669d3883078dda1`,
at export sequence 2. The subsequent full metadata check completed with
10,176 legacy indexes, 145 snapshots in both catalogs, 380,264,003 locations
in both representations, zero pending/failed exports, zero crawl debt,
zero mismatches and zero warnings. AES-256-GCM remained enabled. All three
observations completed with exit zero and no observer errors; production
daemon identity and installed binary/profile pins were unchanged.

Retained receipt:
`/volume2/NASDA2/vaultic-test-m6/phase2-pending-critical-path-recovery-20261102/readiness/receipt.json`.
Its `metadata_validated` is true; `readiness_accepted` and `backup_committed`
remain false because this operation was recovery, not a backup or phase exit.

The bounded production pilot uses `helpers/phase35-m6/bounded_pilot.py`, not
the 52-source live-crawl execution mode. It requires the accepted recovery
receipt, unchanged production pins, the separately pinned qualified candidate,
one exact 10,000-entry source manifest and matching Go/Python content digests.
Backup, verified restore and full post-metadata checking reuse the existing
resource observer. Snapshot inspection must attest bounded provenance; exact
restored path inventory and content must match the manifest. No daemon restart,
installed-binary replacement, full-source acceptance or scheduling is implied.

**Pathdiff Qualification Order:**

No pathdiff systemd unit or executable was found in the deployment locations
checked on 2026-10-31. The current full crawl deliberately does not use pathdiff;
do not modify its sealed profile, helpers, or binaries to enable it mid-run.

The pinned dependency receives ONTAP FPolicy events, not filesystem polling.
Deployment needs reviewed external-engine/policy scopes, receiver reachability
from the relevant FPolicy LIFs, authoritative SVM/volume inventory, and approved
SSH access with verified cluster host keys. Its discovery loop can automatically
enable policies; do not start it against production ONTAP merely to inspect
configuration. Review and authorize those storage-side effects first. Use a
private control socket and durable event database, and protect any diagnostic
captures because they can contain source paths and client identifiers.

Retention is disabled by default, which Vaultic's coverage verifier rejects.
Configure a positive horizon covering the oldest source baseline start through
the optimized run, including baseline duration, backup interval, and recovery
margin. Receiver uptime alone is not proof of complete FPolicy event coverage.

After the full crawl, post-checks, restores, and daemon reopen are validated:

1. Deploy the pinned pathdiff service with persistent event retention and verify
   its actual observation state. Build a version-1 topology map with one complete,
   unambiguous mapping for each source: `target`, `remote_path`, `lif`, `svm_id`,
   `svm`, `volume`, and `volume_msid`. Resolve storage identities from authoritative
   inventory; mount names alone are not proof of SVM or volume identity.
2. Start continuous observation before a fresh baseline backup begins. A service
   started after the current crawl cannot attest changes since these snapshots:
   the required window starts at each parent snapshot's backup-start timestamp.
   Run a normal full traversal after observation starts, without `--force` or
   selective reuse, so unchanged files can use ordinary parent-based reuse while
   the new snapshot establishes an observed baseline. Preserve per-source mode.
3. Qualify an unchanged optimized run using `--use-pathdiff`, the verified socket
   and map, and `--pathdiff-require-coverage`. Verify source-specific parents and
   snapshot contents, not just a successful exit or reduced traversal count.
4. In controlled fixture directories, qualify creation, modification, deletion,
   and rename, including renamed directories. Compare optimized results with a
   full traversal and verify restores. Do not introduce test mutations into
   production source trees without approval.
5. Exercise late observation, reconnect/session changes, retention gaps, and
   incorrect topology. Strict coverage must reject these before publication;
   non-strict operation must fall back to a full traversal. Keep scheduling
   disabled until real deployment coverage and these workflows are qualified.

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