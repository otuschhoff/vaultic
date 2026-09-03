# Phase 14: Versioned path index

[← Back to roadmap index](00-overview.md)

[← Phase 13](phase-13-historical-path-resolution-and-file-history-cli.md) · [Phase 15 →](phase-15-placement-scheduler-offsite-rpo-and-promotion.md)

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
