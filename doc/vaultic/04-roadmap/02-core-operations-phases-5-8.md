# Core Operations: Phases 5–8

[← Back to roadmap index](00-overview.md)

### Phase 5: Backup crawl reconciliation

**Goal:** let the next backup crawl fill the intentional import gaps.

**Current implementation state (2026-08-27):** **complete.** The reusable
`internal/index/reconcile` pipeline attaches to the archiver at both sides of
its unchanged-file decision: `CanReuse` permits the fast path only for a
complete `FreshnessVerified` inode whose live filesystem identity, parent,
size, mtime, ctime, mode, uid, gid, and ordered content all match, while
`Observe` receives copied post-save nodes containing their final data chunks
or directory subtree. Imported, unknown, malformed, or manifest-incomplete
records therefore force the normal file read and rechunk path.

The scanner defaults to 128 live-stat workers, a bounded 50,000-item input and
result queue, and 5,000-item writer drains. Scanner workers never access the
daemon. A serialized writer publishes deepest-first directory graphs and
inode revisions through the daemon, applies backpressure to archiver workers,
and exposes scanned, reused, changed, deferred, failed, and reconciled
counters. Live identity is derived from `lstat`, including the parent inode;
unavailable and cross-filesystem relationships are not invented and instead
create deterministic pending crawl debt.

Verified inode and directory records preserve all prior immutable revisions.
Directories now carry backward-readable live metadata and freshness in
addition to sorted child maps. Deletes are represented by omission from the
new parent revision, and moves create new parent and path revisions without
rewriting history. Hardlink aliases are grouped deterministically by
filesystem and inode, checked for consistent metadata/content, represented by
one inode revision plus every directory edge, and do not invent a single
parent. Directory parent cycles are rejected before publication.

Each metadata revision is a serializable durable daemon transaction with
bounded mutation batches and conflict retry. It creates or reuses canonical
`cm:` segments, advances `ri:` inode and `rm:` manifest states, updates `rc:`
counters from reads in the same transaction, and resolves matching crawl debt
atomically. Failed work remains pending with retry count, timestamp, and error
class; debt-only success does not allocate a duplicate metadata revision.

Tests cover imported-to-verified reconciliation, verified reuse, metadata and
content changes, deletion, rename/move, hardlinks, parent-cycle rejection,
large-manifest reuse and replacement, missing manifests, unavailable identity,
debt retry/resolution, real archiver final-node delivery, real-daemon durable
publication, and actual 128-worker queue backpressure under the race detector.
`Attach` is the explicit backup integration boundary. Selecting a daemon-backed
authoritative engine, publishing snapshot scope, and exporting legacy JSON
remain Phase 6 responsibilities; daemon attach/start CLI options remain Phase
7. Phase 5 does not weaken the existing fail-closed authority guard.

**Implementation steps:**

1. Start the bounded scanner pool, default 128 workers and a 50,000-item queue.
2. Read current inode pointers and pending crawl debt with `MultiGet`.
3. Compare live NFS `lstat` results, including parent inode, against verified
  revisions only.
4. Re-read and rechunk files when metadata or freshness is unknown/mismatched.
5. Reuse known chunk hashes, write new inode/directory revisions, and preserve
  old revision graphs.
6. Inline small ordered content lists and write/reuse immutable `cm:` manifests
  for large lists.
7. Resolve crawl debt and update `ri:`/`rm:`/`rc:` records in the same daemon
  batch as the metadata revision.

**Tests:** imported partial repository followed by backup, rename/move,
metadata-only change, content change, deletion, hardlink, parent-cycle
rejection, manifest reuse, and 128-worker backpressure tests.

**Exit criterion:** a post-import backup produces a complete verified revision
graph and never skips a file solely because imported data exists.

### Phase 6: Authoritative dual-write and legacy projection

**Goal:** make SlateDB authoritative only after the crawl path is correct while
keeping Restic/Rustic compatibility.

**Current implementation state (2026-08-27):** **complete.** Authority is
explicitly gated and requires a validated repository-scoped manifest; once
selected, daemon unavailability fails closed. Pack catalog entries, blob
locations, duplicate locations, aggregate deltas, reconciled revisions,
reverse references, hardlink references, and synthetic snapshot roots are
published through bounded daemon operations. Snapshot scope and its commit
sequence are committed atomically only after the referenced metadata and
legacy pack indexes are durable.

The archiver remains the compatibility projection boundary: it serializes the
canonical Restic tree, including each ordered file `content` array, before the
same array is encoded by reconciliation as inline IDs or deterministic `cm:`
segments. This avoids reconstructing legacy trees from a lossy metadata view
while guaranteeing that manifest-backed authoritative files have identical
expanded legacy content. Pack lifecycle records expose pending JSON index
exports and startup reconstructs pending packs as flushable legacy indexes.
Snapshot checkpoints durably distinguish pending, complete, and failed backend
writes, retain the exact immutable root key, and recover publishable pending
scopes after restart. Prune and repair fail closed in authoritative mode;
`PlanPrune` also enforces this at the repository API boundary.

**Implementation steps:**

1. Add an explicit feature gate and repository capability for SlateDB authority.
2. Write packs, blob locations, pack catalog, revisions, reverse references,
  and aggregates through bounded daemon batches.
3. Allocate commit/revision sequences transactionally and publish snapshot scope
  only after all referenced metadata is durable.
4. Expand inline content or `cm:` manifests into deterministic legacy tree
  `content` arrays.
5. Export JSON indexes synchronously, persist export checkpoints, and recover
  pending exports after restart.
6. Keep prune/repair legacy-safe until their SlateDB-aware revalidation paths
  are separately implemented.

**Tests:** backup/restore through vaultic, Restic, and Rustic; crash between
SlateDB commit and JSON export; export retry; duplicate chunks; old-snapshot
restore after later revisions; and mixed-client read compatibility.

**Exit criterion:** a SlateDB-authoritative backup restores correctly through
all supported clients and export lag is visible rather than silent.

### Phase 7: Import/export/check CLI workflows

**Goal:** expose operator-controlled migration and verification tools.

**Current implementation state (2026-08-28):** **complete.** The new
`vaultic index` group leaves `vaultic list index` unchanged and provides
`import`, `export`, `check`, and `rebuild-pack-stats`. Import is best-effort,
bounded, dry-runnable, and resumable through durable index and snapshot
checkpoints; authority activation remains explicit and occurs last, only after
a complete import. Snapshot traversal loads the source blob index first.

Export writes deterministic canonical Restic JSON indexes, either for every
live catalog pack or only packs whose lifecycle lacks a completed export
checkpoint. Each durable JSON object receives an atomic provenance checkpoint
containing a monotonic export sequence and its exact pack IDs before those
packs transition to `published`; interruption therefore causes safe duplicate
re-export rather than silent omission. `--since` selects packs after a recorded
sequence, while `--verify` reads each object back through the authenticated
repository loader and decodes it. Pack-oriented export still scans `b:` because
the current schema intentionally has no pack-to-blob secondary index.

Differential check compares deduplicated physical blob locations and pack
presence between raw legacy JSON and SlateDB, validates pack type/blob/payload
catalog totals and all five aggregates, checks reverse references and
materialized counters, checks snapshot roots, reports crawl debt/export/GC
state, and validates export object presence, decoding, and pack provenance.
Unresolved imported references and checkpointed snapshots without reconstructible
root inode identity are warnings rather than divergence. Manifest and schema
compatibility are enforced before the workflow by repository-open validation
and the daemon health handshake. Aggregate repair tolerates missing or malformed
records, reports before/after deltas, uses checked rebuild arithmetic, and
replaces all records in one durable batch only when numerical drift exists.

Mutating workflows take an exclusive repository view and check takes the
existing read-lock path. All support JSON summaries and map partial import or
detected drift to exit status 2. Import exposes explicit legacy source
selection, daemon transaction batch sizing, work/error budgets, dry-run,
resume, bounded snapshot traversal, and structured record/warning/checkpoint
counters. Daemon
flags support attach or start, repository-scoped Unix sockets by default,
explicit TCP address/allowlist/authentication, persistent startup, and local or
S3-compatible storage configuration. End-to-end tests cover legacy-only,
partial, resumed, and authoritative repositories; dry-run and corruption
paths; checkpointed export; aggregate repair; local daemon storage; and the
same workflow against S3-compatible metadata storage when the existing CI
endpoint is configured.

**Implementation steps:**

1. Add the `vaultic index` command group without changing `list index`.
2. Implement resumable best-effort `index import`.
3. Implement full and checkpointed `index export`.
4. Implement differential `index check`, pack aggregate rebuild, and JSON
  summaries with automation-friendly exit codes.
5. Add daemon attach/start options while keeping Unix sockets as the default and
  TCP opt-in only.

**Tests:** all commands on legacy, partial, and SlateDB-authoritative repos;
  dry-run and resume; corruption and warning exit codes; aggregate repair;
  local and S3-compatible backends.

**Exit criterion:** operators can import, inspect, export, repair aggregates,
and compare engines without manual database intervention.

### Phase 8: Prune, GC, and operational hardening

**Goal:** safely use pack catalogs and reverse references for performance without
weakening snapshot reachability guarantees.

**Current implementation state (2026-08-28):** **complete for the CLI-facing
GC workflow; scale benchmarking deferred to dedicated infrastructure.**
`vaultic index gc` (SlateDB-authoritative repositories only; legacy repositories
keep using `prune`) discovers candidates from a single scan of `ri:`/`rm:`
reverse references, narrowing the search before the more expensive step: a
full re-walk of every retained snapshot root (the same trusted mechanism
`prune` already uses, valid for any engine because the archiver always writes
the classic snapshot/tree/content blob graph independent of SlateDB's own
schema) that is the actual, final reachability authority. A blob is only
treated as unreachable when both signals agree, and reachability is always
re-verified immediately before any destructive action, never trusted solely
from the earlier scan. Packs found wholly unreachable are deleted; packs
mixing live and unreachable blobs are repacked via the same `CopyBlobs`
primitive prune uses, then their now-empty-of-purpose original pack is
deleted the same way. Deletion uses the `published -> delete_pending ->
deleted` lifecycle: a durable delete-pending transition precedes the physical
backend removal, and only a successful removal purges the pack/blob catalog
records and decrements aggregates in one transaction; a failed removal leaves
the pack visible as delete-pending and is retried automatically on the next
run. `--discover-only` records candidates cheaply without the snapshot walk
or any mutation; `--min-candidate-age` requires a candidate to stay
continuously unreachable for a configurable duration (tracked via the `gc:`
record's discovery timestamp, preserved across runs) before it is swept,
guarding against races with concurrent or clock-skewed writers on top of the
exclusive repository lock GC already holds during its destructive phase.

Physically deleting a pack necessarily makes any legacy JSON index that
referenced it stale, including indexes inherited from a pre-import legacy
repository that were never covered by an export checkpoint. `vaultic index
gc` automatically re-exports every remaining live pack and then deletes every
legacy index object (and any checkpoint tracking it) that still references a
now-gone pack, so `index check` reaches a clean state without a separate
manual step.

**Implementation steps:**

1. Discover GC candidates from `ri:`, `rm:`, `rc:`, and pack catalog records.
2. Re-walk retained snapshot roots before every destructive deletion.
3. Delete wholly unreachable packs; repack packs containing a mixture of live
  and unreachable blobs.
4. Use `published -> delete_pending -> deleted` transitions and retry failed
  deletions.
5. Add crash/fencing/eventual-consistency and mixed-client tests.
6. Benchmark 10 million, 100 million, and 1.4 billion inode-equivalent loads;
  tune cache, SST blocks, batch sizes, and scanner workers.

**Tests:** unit coverage for candidate/pack classification (whole/mixed/skip)
and the min-candidate-age gate; real-daemon tests for two-phase pack deletion
(including a simulated crash between delete-pending and physical removal,
and its automatic retry), mixed-pack repack, and stale legacy index cleanup;
a full CLI end-to-end test covering forget, import, export, discover-only,
the age gate, the real sweep, convergence on a repeated run, a clean `index
check`, snapshot restore correctness, and `check --check-unused` finding
nothing dangling. Discovering and fixing this phase's real bugs (a
pack-publish path that silently recorded every authoritative pack's payload
size as zero, and untracked legacy indexes inherited from import) required a
real repacked pack and a real pre-import legacy index respectively; neither
was exercised by any earlier phase's tests.

**Exit criterion:** documented capacity and recovery targets, clean differential
checks after failure injection, and a repeatable large-repository acceptance
test.

Scale benchmarking (10 million, 100 million, and 1.4 billion inode-equivalent
loads) requires dedicated infrastructure beyond this environment; the tunable
surface it needs (daemon transaction batch size, scan page size, snapshot
work budgets) is already wired through Phase 7 and Phase 8's CLI options.

