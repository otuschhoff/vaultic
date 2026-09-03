# Phase 13: Historical path resolution and file-history CLI

[← Back to roadmap index](00-overview.md)

[← Phase 12](phase-12-backend-registry-placement-records-and-per-backend-prune.md) · [Phase 14 →](phase-14-versioned-path-index.md)

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
