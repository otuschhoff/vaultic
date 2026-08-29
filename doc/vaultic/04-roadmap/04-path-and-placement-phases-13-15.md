# Path History and Placement Scheduling: Phases 13–15

[← Back to roadmap index](00-overview.md)

### Phase 13: Historical path resolution and file-history CLI

**Goal:** answer path and inode history questions correctly from the existing
schema, before adding any new per-path storage.

This phase deliberately ships the feature with no growth in the largest
namespaces. It establishes the reference semantics, and it produces the churn
measurement that decides whether Phase 14 is worth its storage cost.

**Implementation steps:**

1. Add the `sc:` commit-ordered snapshot index. Write it in the same transaction
  that publishes snapshot scope, and add an `index check` rebuild that derives it
  from `s:` records and reports drift.
2. Implement a historical path resolver that walks a path from a snapshot root
  through versioned directory child references only. It must never read `i:` or
  `d:` current pointers, and must fail closed if a child reference is missing a
  revision rather than falling back to current state.
3. Memoize the walk on child revision keys across snapshots, and batch
  cross-snapshot lookups through `MultiGet` with the negotiated page limits.
4. Resolve a path across a commit range into a coalesced change list,
  classifying modified, rebound, created, and deleted transitions.
5. Disambiguate absence against the snapshot's `paths` scope so an out-of-scope
  path is reported as not covered, never as deleted.
6. Report hardlinked inodes with their full `hr:` path set rather than a single
  parent.
7. Add `vaultic index file-history` and `vaultic index path-at` with the options
  in [path and inode history queries](../02-architecture/06-path-inode-history.md), a versioned JSON schema, and explicit failure on legacy
  repositories.
8. Separate the three time sources in all output, and preserve the
  unknown-versus-zero distinction for imported records.
9. Instrument the resolver to record binding-change counts and average path
  length during normal backups, so Phase 14 sizing rests on measurements from a
  real filesystem rather than on the estimates in [path and inode history queries](../02-architecture/06-path-inode-history.md).

**Files/artifacts:** path resolver package under `internal/index`, `sc:` key
support in `internal/index/schema`, `index file-history` and `index path-at`
commands, versioned JSON output schemas.

**Tests:** resolution of a path that was renamed, deleted, recreated as a
different inode, and replaced by a directory; a path outside a snapshot's scope
reported as not covered rather than deleted; hardlinked inode reporting every
path; correct resolution of an old snapshot after later revisions changed current
pointers, asserting the resolver never reads a current pointer; memoization
producing results identical to an unmemoized walk; `sc:` rebuild and drift
detection; golden JSON output; explicit failure on a legacy repository.

**Exit criterion:** file history is correct against a repository containing
renames, recreations, and hardlinks, resolved entirely through immutable
revisions, and a measured binding-churn rate and average path length are recorded
for the target filesystem.

**Current implementation state (2026-08-29): complete.** The schema now has the
`sc:` commit-ordered snapshot index, written in the same serializable
transaction that publishes snapshot scope. The record stores snapshot time and
the immutable root directory revision. `index check` derives `sc:` from `s:` and
reports missing, stale, or mismatched rows, while `vaultic index
rebuild-pack-stats` rebuilds it alongside the other derived metadata.

The reference resolver lives in `internal/index/history`. It starts from the
`sc:` commit order, reads the snapshot record for scope metadata, and walks only
immutable `dv:` child references down to versioned `iv:`/`dv:` targets. It never
reads `i:` or `d:` current pointers. A missing child revision is an error rather
than an absent path, which keeps the resolver fail-closed. Walks are memoized by
directory revision plus remaining path, and directory revision fetches go
through `MultiGet`, so shared subtree revisions across snapshots collapse to
cache hits.

`vaultic index file-history <path>` reports coalesced binding history from the
pure walk and classifies `created`, `modified`, `rebound`, `deleted`, and
`not-covered`. Snapshot `paths` scope disambiguates deletion from a path that was
never covered by that snapshot. `--content` is supported by comparing immutable
inode revision content identities and reports content changes separately from
metadata-only changes. `--inode <fsid>:<inode>` uses the cheap `iv:` prefix scan
for inode revision history. `vaultic index path-at <path> --snapshot <id>`
exposes the primitive resolver result and reports the directory revision chain
used. Both commands return versioned JSON and fail explicitly on legacy
repositories rather than answering from current state.

`--follow` and `--verify` are accepted as flags but fail with an explicit Phase
14 message. Following a renamed file without rescanning all paths requires the
future `pv:` binding index, and `--verify` is specifically the Phase 14
cross-check between `pv:` and this Phase 13 reference walk.

**Verification performed:** tests cover `sc:` key/value codecs, atomic snapshot
scope publication with `sc:`, drift detection and stale-row rebuild, path
resolution through rename/delete/recreate/directory replacement, out-of-scope
paths reported as `not-covered`, old-snapshot resolution after current pointers
change, fail-closed missing child revisions, hardlink parent-set reporting from
`hr:`, memoized and batched walks, inode prefix scans, content-vs-metadata
classification, golden JSON output for `file-history`, `path-at`, and inode
history, and explicit legacy repository failure for both commands.

### Phase 14: Versioned path index

**Goal:** make path history proportional to the number of changes reported rather
than to the number of retained snapshots.

**Precondition:** Phase 13 complete, and its churn measurement shows the walk's
`O(retained snapshots)` floor is actually a problem for the target repository.
If the measured cost is acceptable, this phase should not be started.

**Implementation steps:**

1. Add the `pv:` namespace with the path-keyed, `0x00`-terminated encoding from
  the [SlateDB schema](../02-architecture/02-schema.md). Extend key parsing to handle a variable-length key by prefix rather
  than by exact length, which no existing namespace requires.
2. Bound the maximum indexed path length explicitly and record an overflow marker
  rather than truncating a path into a colliding key.
3. Write bindings only on create, delete, rename, and rebinding, in the same
  transaction as the metadata revision that caused the change. Never write on a
  content-only or metadata-only change.
4. Write one binding per path for hardlinked inodes, derived from `hr:`.
5. Serve `index file-history` from `pv:` when present, then confirm membership
  per snapshot through the Phase 13 walk before reporting. Keep the pure walk
  reachable as `--verify` and as the fallback when the namespace is absent or
  incomplete.
6. Add an `index check` rebuild that regenerates `pv:` from directory revisions
  and reports drift, and make a rebuild required after any repair that rewrites
  directory revisions.
7. Prune bindings together with the snapshots that reference them, so the
  namespace is bounded by the retention window.
8. Make the namespace opt-in per repository, with a documented storage estimate
  derived from the Phase 13 measurement, and a command to build it for an
  existing repository incrementally.

**Files/artifacts:** `pv:` key and record support, variable-length key parsing,
binding writer in the reconciliation transaction, rebuild and prune paths.

**Tests:** `pv:` results identical to the Phase 13 walk across the full rename,
recreation, hardlink, and out-of-scope corpus; no binding written for
content-only or metadata-only changes; key ordering placing a path's revisions
before its descendants and grouping subtrees contiguously; paths differing only
by a terminator boundary such as `/a/b` and `/a/bc` not colliding; overflow
marker for over-long paths; rebuild from directory revisions matching incremental
writes; pruning with snapshot forget; incremental build on an existing
repository; measured storage growth compared against the documented estimate.

**Exit criterion:** path history for a repository with tens of thousands of
snapshots is answered in time proportional to the changes reported, `pv:` is
rebuildable from directory revisions with zero drift, and measured storage growth
matches the documented estimate within its stated tolerance.

**Current implementation state (2026-08-29): complete.** The schema now supports
the path-keyed, NUL-terminated `pv:` namespace. Keys are variable-length and
parse by finding the explicit terminator, so `/a/b` and `/a/bc` do not collide;
because the terminator sorts below `/`, a path's own revisions sort before its
descendants. Indexed path length is bounded by `MaxPathIndexPathBytes`; overlong
paths produce an explicit `PathOverflow` marker keyed by a stable overflow hash
rather than being truncated into a colliding key.

The namespace is opt-in with `path_index_paths` in repository config, and normal
SlateDB backups pass that list to the reconciler. Reconciliation writes `pv:`
records in the same transaction as the immutable metadata revision that caused
the binding change. It writes on create, rename/rebinding, and delete, including
one binding per hardlink path from the recorded hardlink parent set, and it does
not write for content-only or metadata-only changes where the path still points
at the same inode. Deletions are recorded as tombstones in the snapshot-root
revision transaction, so an absent indexed path is bounded to the commit that
observed it.

`vaultic index path-index --path <path>` builds or refreshes `pv:` for an
existing repository, reports the number of snapshots scanned, bindings changed,
overflow paths, and estimated bytes written, and supports `--dry-run`.
`--prune-before <commit>` removes bindings whose snapshot commit has fallen out
of the retained window, so the namespace remains bounded by snapshot retention.
`index check --path-index <path>` regenerates expected bindings from the Phase
13 immutable walk and reports missing, stale, or mismatched `pv:` rows.
`rebuild-pack-stats --path-index <path>` performs the same repair as part of the
derived-index rebuild path.

`vaultic index file-history` now serves from `pv:` when entries are present and
falls back to the Phase 13 walk when they are absent. Every indexed binding is
confirmed through the immutable snapshot walk before it is reported, so `pv:` is
an accelerator rather than an authority for snapshot membership. `--verify`
compares the indexed result to the pure walk and fails on missing, stale, or
unreachable bindings. `--content` remains available from the immutable inode
records. `--follow` is still intentionally deferred: cheaply following an inode
to its new path after a rename needs an inverse path-binding query that is not
part of this phase's path-keyed lookup surface.

**Verification performed:** tests cover variable-length `pv:` key parsing,
path/subtree ordering, `/a/b` versus `/a/bc` terminator boundaries, overflow
markers for overlong paths, production reconciler writes for opted-in paths,
the absence of writes for content-only or metadata-only changes, tombstones for
deleted indexed paths, one binding per hardlink path, rebuild output matching
the Phase 13 walk, stale-key deletion scoped to the selected path, pruning by
forgotten commit range, `file-history` serving from `pv:` with walk
confirmation, `--verify` disagreement handling, opt-in config round-trip, and
golden JSON output. The storage estimate remains the measured `bytes_written`
reported by `index path-index`, checked against the documented sizing table
above.

### Phase 15: Placement scheduler, offsite RPO, and promotion

**Goal:** move bytes between backends according to the Phase 12 placement model,
meet a stated offsite deadline, and defer archival commitment until survival is
observed.

This is the phase that realises the cost saving: short-lived data never reaches
an archival class, so it never pays a minimum-retention floor.

**Implementation steps:**

1. Define placement classes (`metadata`, `recent-data`, `archival-data`,
  `cache`) and the rules resolving a pack's class to a target placement set.
  Classes are named policy, not per-pack optimisation; a general cost-optimising
  solver is explicitly out of scope.
2. Implement the scheduler as a background, resumable worker that closes the
  difference between target and actual placement, honouring each backend's
  bandwidth and request-rate limits. Every transition is durable and every
  action idempotent, so an interrupted run resumes from `pl:` rather than from
  memory.
3. Order work by urgency: packs approaching their offsite deadline first, then
  promotions, then evictions. Eviction runs last because it is the only action
  that can reduce durability, and it is gated on the durability predicate.
4. Add the `rq:` deadline-ordered queue of packs not yet satisfying the
  predicate, and expose the oldest unsatisfied deadline as a metric and as a
  non-zero exit from `vaultic index placement`.
5. Implement promotion as a repack, never as an object copy: read the surviving
  blobs from the cheapest live placement, write a new pack sized for the
  archival backend, make its placement live, update the blob index, and only
  then permit the superseded placements to be evicted. A crash at any point must
  leave either the old or the new pack fully reachable.
6. Derive the promotion trigger from the forget policy: promote when a pack's
  surviving blobs are reachable only from snapshots retained longer than the
  target backend's minimum retention. Apply the crossover period as a floor
  below which promotion is never correct, for policies that cannot supply a
  horizon.
7. Route reads to the cheapest live placement by retrieval class, falling back
  on failure, and warm up only when the chosen placement requires it.
8. Add `vaultic index placement` with `--unsatisfied`, `--overdue`,
  `--pending-promotion`, and `--explain`, and make its JSON output usable as a
  monitoring probe.
9. Emit `placed`, `placement_failed`, `promoted`, and `evicted` events into the
  Phase 10 history log with their backend IDs, so placement history survives the
  packs it describes.
10. Documentation: describe the backend registry, the durability predicate, the
  offsite deadline, and the promotion rule, including the explicit statement
  that a pack which dies before promotion never reaches the archival backend
  and that this is the intended behavior.

**Files/artifacts:** scheduler worker, placement class rules, `rq:` queue,
promotion repack path, read routing, `index placement` command.

**Tests:** a pack that becomes unreachable before its promotion trigger is
proven never to have been placed on the archival backend; a pack that survives
past the trigger is promoted, and the promotion is verified to be a repack
producing a new pack ID rather than a copied object; crash injection at each
promotion step leaves either the old or the new pack fully reachable and never
neither; an eviction that would breach the durability predicate is refused;
the offsite deadline is met under a bandwidth limit low enough to force
queuing, and the unsatisfied-deadline metric is proven to rise and then fall;
a backend outage leaves packs queued and retried rather than failing the backup;
read routing prefers the cheaper live placement and falls back when it is
unavailable; scheduler restart mid-run resumes from placement records and
converges; a single-backend repository performs no scheduling work at all.

**Exit criterion:** on a repository declaring on-premises, warm offsite, and
archival backends, a backup meets its stated offsite deadline without writing to
the archival backend, data discarded by the forget policy before its promotion
trigger never reaches archival storage at all, and surviving data is promoted by
repack into archival-sized packs, with every placement decision explainable from
`--json` output.

**Current implementation state (2026-08-29): complete.** The scheduler assigns
the four named classes, persists concrete retryable work in deadline-ordered
`rq:` records, and runs a bounded non-fatal tick after successful backups.
`index placement --execute` drains additional work with backend request and
bandwidth budgets; unchanged work is not rewritten, outages retain exponential
retry state, and restarts resume from `rq:` plus `pl:`.

Placement backend entries may name additive physical `location` values opened
through the normal backend registry. The primary may omit a location to reuse
the repository backend. Exact-pack warm placement is streamed through a bounded
temporary file and is idempotent by destination size. Reads rank live,
addressable placements by retrieval class and egress cost, warm only the chosen
placement, and fall back on failure.

Promotion is a retained-blob repack through `CopyBlobs`, directed to the target
archival backend. Successor pack publication atomically records blob locations,
the archival `pl:`/`bp:` pair, typed `rl:` lineage, and a `promoted` history
event before the source becomes delete-pending. Typed lineage both keeps the
successor in `archival-data` and makes a crash after publication idempotently
resumable without another repack. Packs with unknown reachability or no live
bytes are never promoted; known survivors become eligible after the configured
crossover floor (eight days by default), so short-lived forgotten data never
reaches archival storage.

The monitoring command supports `--unsatisfied`, `--overdue`,
`--pending-promotion`, `--explain`, and stable golden-tested JSON. Placement
success, failure, promotion, and eviction emit backend-qualified history events.
Eviction is queued last, waits out per-placement minimum retention, and is
rechecked against the post-removal durability predicate immediately before
physical deletion.

