# Phase 5: Backup crawl reconciliation

[← Back to roadmap index](00-overview.md)

[← Phase 4](phase-04-best-effort-legacy-import.md) · [Phase 6 →](phase-06-authoritative-dual-write-and-legacy-projection.md)

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
