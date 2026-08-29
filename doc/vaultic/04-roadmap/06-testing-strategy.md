# Testing Strategy

[← Back to roadmap index](00-overview.md)

## 16. Testing strategy

### Unit tests

- Big-endian schema round trips and malformed input rejection
- Key namespace and prefix ordering
- Snapshot-versioned inode and directory records remain readable after later
  moves, renames, metadata changes, and deletions
- Inline and manifest-backed content sequences restore byte-for-byte in order
- Large content manifests remain bounded, immutable, and deduplicated
- Current-pointer updates never mutate historical records
- Snapshot manifests resolve the correct root and historical version scope
- Directory child references carry a complete versioned metadata key, and a
  child reference lacking a revision is rejected rather than resolved against
  current pointers
- Historical path resolution walks only immutable revisions, including after
  later changes to current pointers
- Path change classification for rename, delete, recreation as a different
  inode, and replacement of a file by a directory
- Out-of-scope paths reported as not covered rather than deleted
- `pv:` key ordering: a path's revisions precede its descendants, subtrees are
  contiguous, and terminator-boundary paths do not collide
- `sc:` and `pv:` rebuild from authoritative records with zero drift
- Duplicate blob-location preservation
- Pack catalog deduplication across multiple source indexes
- Data/tree/mixed/unknown pack classification and physical/payload size totals
- Tier assignment, per-tier aggregates, and retention-deadline computation
- Placement set transitions, and the durability predicate over them, including
  two backends sharing one failure domain and pending placements not counting
  as live
- An eviction that would drop a pack below the durability predicate is refused
- Per-placement retention producing different deadlines for one pack
- Promotion triggered from the forget policy horizon, not from a timer
- Pack history event ordering, rollup idempotence, and coverage flagging
- Tier policy resolution and cold cost-model boundary cases
- Deletion-queue deadline ordering and expired-prefix sweeps
- Aggregate rebuild and drift detection after import, backup, export, and prune
- Reverse-edge creation, deduplicated inode counts, reachability candidates,
  and counter rebuilds
- Import checkpoint idempotence
- Crawl-debt creation and resolution
- Export determinism
- Differential-check classifications

### Integration tests

- Legacy JSON repository with no SlateDB namespace
- Empty SlateDB repository
- Partial import followed by backup crawl
- Corrupt JSON record mixed with valid records
- Missing tree and missing snapshot metadata
- Crash after SlateDB commit and before JSON export
- Crash during import batch
- Local object store and S3-compatible object store
- A repository declaring on-premises, warm offsite, and archival backends:
  offsite deadline met under a bandwidth limit that forces queuing, data
  discarded before its promotion trigger never reaching the archival backend,
  and surviving data promoted by repack
- Backend outage leaving placements queued and retried rather than failing the
  backup, and scheduler restart converging from placement records
- Read-only `DbReader` operations while a writer is active
- File history across a repository containing renames, recreations, hardlinks,
  and snapshots with differing backup scopes
- `pv:`-served history agreeing with the pure directory walk over that corpus
- Daemon startup races, singleton reuse, stale socket recovery, and protocol
  mismatch rejection
- Unix-socket permissions and TCP-disabled-by-default behavior
- TCP allowlist and authentication rejection/acceptance cases
- Restic/Rustic reading vaultic-exported JSON indexes
- `cwalk` parallel directory traversal producing results identical to standard walking
- `pathdiff` volume ID resolution, target host LIF $\rightarrow$ SVM $\rightarrow$ volume topology matching, and 100% event coverage validation
- Fallback from selective `pathdiff` crawl to full `cwalk` scan when event coverage sequence gaps or buffer overflows are detected

### Scale tests

- 128 scanner workers against a synthetic NFS-like filesystem
- `cwalk` traversal throughput and memory bounds under high worker concurrency against synthetic 1.5B inode file trees
- Selective `pathdiff` crawl duration vs full crawl on large volumes with sparse churn
- 50,000-item result queue saturation
- 5,000 and 10,000 item SlateDB batches
- MultiGet latency and allocation profile
- Memory use while exporting a repository with billions of records
- Content-manifest lookup, segmentation, deduplication, and restore memory at
  small, medium, and very large file sizes
- Reverse-reference index size, hot-key behavior, and reachability-scan cost
- Path-resolution cost as retained snapshot count grows, with and without `pv:`
- `pv:` size growth against measured binding churn and average path length
- Placement scheduler throughput against a constrained backend, and the offsite
  deadline under a backlog large enough to require queuing across runs
- Compaction and reader lag under sustained backup writes

### Safety tests

Run with `-race` where applicable and test:

- concurrent import and read-only check
- concurrent backup and export
- fencing and reopen behavior
- duplicate retry of a committed batch
- stale manifest and schema mismatch
- legacy fallback when SlateDB is absent
- hard failure when SlateDB is present but corrupt

