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

### Publication Attribution

The backup command emits `reconciliation_stats` at cleanup after joining the
reconciler, including on cancellation or failure. JSON output contains the six
original counters plus these cumulative measurements; verbose text reports a
summary and quiet non-JSON mode suppresses it:

- `publication_groups_by_size`: four counters for ordinary inode groups of sizes
  one through four. Empty flushes are excluded; reused and failed items still
  occupy slots. Hardlinks and directories are outside this histogram.
- `publication_group_ns`: summed wall time to complete those sequential groups,
  including reads, allocation, worker joins and failure recording.
- `revision_allocation_calls`, `revision_allocation_failures`,
  `revision_allocation_ns`: completed inode allocation API calls, their failures,
  and summed client-observed duration. Block reservations count once, not once
  per waiting worker; time waiting for the group's allocation mutex is excluded.
  Internal store retries are included in duration but are not separate calls.
- `revisions_reserved`: inode revision numbers successfully reserved, including
  unused group slots. Failed calls contribute zero.
- `inode_revisions_assigned`: numbers handed to changed inodes, including ones
  whose later path planning or publication fails. The difference from reserved
  numbers is unused capacity, not a count of failed publications.
- `inode_publication_calls`, `inode_publication_failures`,
  `inode_publication_ns`: completed publication API calls, their failures, and
  summed client-observed duration, including internal retries and durable waits.

The allocation and publication counters include hardlinks but exclude directory
and synthetic-root work. Reuse and cancellation before a store call do not count
as allocation/publication calls. Concurrent worker durations overlap: do not add
them to group wall time or equate them with daemon-only durable-wait time.
These counters are diagnostic and do not change durability or concurrency.

### Experimental Atomic Allocation

`Options.AtomicInodePublication` is an internal, default-false prototype switch.
No backup CLI flag enables it, and normal backup configuration still uses the
group-scoped reservation path. A supporting store implements
`PublishAllocatedReconciledRevision(ctx, build)`: the counter read/update,
metadata revision, current pointer, references, path bindings and debt resolution
share one serializable transaction and one normal durable commit. Directory and
synthetic-root publication are unchanged. Verified reuse still does no allocation.
Unsupported stores fail closed when the prototype is explicitly selected.

The builder receives the candidate revision and must construct the same logical
operation without external side effects. It may run again with a new revision
after a serialization conflict. All revision-bearing keys and values must be
rebuilt; callers must not publish or retain the losing candidate as committed.
The existing bounded retry policy applies only to `Aborted` conflicts. Other
errors, including uncertain commit outcomes, are returned without a fresh
allocation retry. An error does not prove that nothing committed: a lost durable
acknowledgement may leave the complete publication and advanced counter durable.
The returned revision is nonzero only after an acknowledged durable commit.

In prototype mode, allocation is included in `inode_publication_ns` and no
standalone allocation call is recorded. Reserved/assigned counters advance only
after an acknowledged atomic publication. Failed or uncertain calls therefore
cannot be used to infer an exact durable revision count. Group size and joined
wall-time metrics retain their existing meaning.

R36's small single-stream measurements halved publication-stage runtime, but R37
rejects general rollout: four independent streams made distinct one-ID files
13.65% slower and distinct 129-ID manifest-backed files 4.52x slower, with 3.57x
and 5.23x daemon CPU respectively. Shared-content cases can still improve. The
prototype remains default-off and is retained only for controlled comparisons;
the standalone counter in every atomic publication is a contention bottleneck.
Bounded group-level allocation/publication is the next candidate to investigate,
not an implemented or accepted solution. Complete backup/restore, sustained load,
client CPU and peak-memory acceptance are still outstanding.

The native attribution fixture accepts bounded test-only environment variables:
`VAULTICDB_TEST_PUBLICATION_INODES` (default 32, maximum 1024),
`VAULTICDB_TEST_PUBLICATION_CONTENT_IDS` (default 1, maximum 1024), and
`VAULTICDB_TEST_PUBLICATION_STREAMS` (default 1, maximum 4). The inode count must
be divisible by four times the stream count. Streams own independent reconcilers
and publication maps, each retaining four-worker groups; they share one daemon
and schema store, not separate backup CLI processes. Full content reconstruction,
all content reference counts, manifest references, path bindings, unchanged reuse,
cleanup and reopen are checked outside the measured publication phase. Group
durations summed across streams overlap and are not elapsed time. Daemon CPU is
a before/after process-counter delta; RSS is an end sample, not a peak.

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
