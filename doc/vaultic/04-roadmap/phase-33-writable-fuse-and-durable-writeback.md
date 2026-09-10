# Phase 33: Writable FUSE and durable writeback

[Back to roadmap index](00-overview.md)

[Previous: Phase 32](phase-32-operational-monitoring-and-metrics-export.md) | [Next: Phase 34](phase-34-writable-nfsv3-exports.md)

**Status: design specification, not yet implemented.**

**Goal:** mount a snapshot, optionally rooted at a path, as a writable workspace. Preserve the immutable base snapshot and record copy-on-write changes in durable writeback storage with inode and namespace state in VaulticDB/SlateDB. Read new data directly from writeback, asynchronously pack it into normal repository storage, and optionally retain hot committed data in the less resilient Phase 30 read cache. Permit new immutable snapshots of the workspace at any time.

## Scope and prerequisites

Extend Phase 31's protocol-neutral snapshot filesystem so Phase 34 can reuse the same mutation, durability, and snapshot implementation. This is overlayfs-like semantics, not a dependency on Linux overlayfs or a modification of an existing snapshot. Read-only mount behavior remains unchanged. Use Phase 22's fenced VaulticDB writer, Phase 28's WAL durability, Phase 30's disposable read-cache manager, and Phase 32's monitoring contract.

One fenced workspace mutation authority serializes updates in this phase. Multiple clients may use that authority; independently writable frontends for the same workspace require the Phase 37 investigation. Broker and backend authorization use existing local controls; remote principal delegation remains Phase 38 work.

## Workspace and filesystem semantics

Persist a workspace ID, repository/base snapshot/root identity, generation, stable inode IDs, directory entries, inode attributes, versioned extent maps, tombstones/whiteouts, operation IDs, and writeback references in a separate versioned metadata namespace. Read resolution selects the current overlay extent, an explicit hole, or immutable base content. A truncate followed by extension must not resurrect hidden base bytes. Rename and hard links preserve inode identity; a path-derived identity is not sufficient for mutable files.

Support create, positioned and append writes, truncate, unlink, mkdir/rmdir, rename, symlink, hard link, chmod/chown, timestamps, and the supported FUSE xattr contract. Define atomic append, cross-directory rename, replacement, open-unlinked lifetimes, sparse files, and concurrent reader visibility. Namespace mutations are atomic VaulticDB transactions with precondition checks and idempotent operation IDs. Reject unsupported special-node operations explicitly and never follow repository symlinks onto the server host filesystem. Permission checks and privileged operations follow an explicit local identity policy.

Use immutable writeback extents and metadata indirection; do not emulate mutable files by repeatedly rewriting entire S3 objects. Bound extent fragmentation, copy-up, read-modify-write amplification, multipart buffers, and compaction scratch. Large files must stream rather than fit in RAM. References from live inodes, open handles, pending snapshots, and packing jobs participate in reclamation.

## Durable writeback, not disposable cache

Support native librados as the preferred writeback target, one or multiple S3-compatible targets, or one local-disk target. Configure an explicit durability predicate and failure model per profile: for example a Ceph pool configured for three replicas across failure domains, successful writes to required independent S3 targets, or local filesystem durable completion. Three replicas alone are not a guarantee: validate Ceph health, `size`/`min_size`, placement and durable acknowledgements. Multiple S3 targets require a documented required-set or K-of-M publication/recovery rule, not an assumption that all replicas are current. Local disk explicitly lacks host/disk-loss tolerance unless provided externally.

Encrypt and authenticate writeback data under a documented purpose-separated repository key scheme; bind workspace, extent identity, and version. Keep authoritative writeback, metadata/WAL, ordinary packs, and disposable read cache in separate namespaces and permission domains. Cache eviction credentials must never delete dirty extents. No fallback to weaker durability when a required target is unavailable.

For the initial implementation, acknowledge filesystem writes only after:

1. New immutable extent data satisfies the configured writeback durability predicate.
2. The inode/extent transaction and its recovery references are durably committed through VaulticDB/WAL.
3. The operation's replay/result record is recoverable under the declared failure model.

Metadata-only operations require equivalent durable transactional publication. Data written before a failed metadata transaction is an orphan, reclaimed only after safe reconciliation. Metadata must never expose undurable data. Backend and metadata errors propagate to callers; partial writes report only the durably accepted prefix. FUSE kernel-buffered writes may not reach the daemon immediately, so document `fsync`/`fdatasync`, flush/release errors, and mount cache mode precisely. Initially disable unsupported kernel writeback behavior rather than claiming it is durable. `fsync` establishes durable workspace state; it does not require immediate conversion into packfiles.

Recover workspace state after process failure using durable references, not directory listings alone. Fail closed on missing acknowledged dirty data: it is data loss/unavailability, not a cache miss eligible for stale-base fallback. Broker expiry stops admissions and packing, preserves durable encrypted state, and never deletes pending writes on shutdown.

## Pack commit and cache demotion

Use a recoverable per-version state machine:

```text
durable dirty extents -> packing -> durable pack placements
    -> atomic metadata publication -> clean/cache-eligible -> writeback reclaim
```

Packing uses the existing chunking, compression, encryption, deduplication, and pack-placement pipeline. Publish replacement extent references only after the required normal-backend durability predicate is satisfied and placement/index records are committed. Concurrent new writes create later versions; committing an old version must not overwrite the newer workspace head. Retry ambiguous uploads and transactions idempotently. Coordinate reference retention with prune/GC so the base snapshot, active workspace, pending snapshots, and newly published packs cannot be collected prematurely.

Dirty data also serves reads, but remains authoritative and non-evictable. Once the relevant version is committed and no durable/open/snapshot reference needs its writeback copy, offer hot data to the Phase 30 cache admission policy. Copy and verify into a dedicated lower-resilience cache pool/target, or use an explicitly validated equivalent migration, before releasing its old copy where retention is desired. Do not lower replication on an entire shared writeback pool or treat Ceph replication as a portable per-object switch. Cold data can be reclaimed without populating cache. Cache admission failure never delays a completed durable commit; ordinary pack storage is now the fallback.

## Scheduling and controls

Proposed operator surface:

```text
vaultic mount --read-write --workspace NAME --writeback-profile PROFILE SNAPSHOT[:PATH] MOUNTPOINT
vaultic workspace status|drain NAME
vaultic workspace snapshot NAME
vaultic workspace policy get|set NAME
```

Expose minimum batching age, target maximum dirty age, quiet-time batching, target pack size, upload concurrency, per-backend bandwidth limits, dirty-byte/object high/low watermarks, total writeback capacity, and packing scratch reserve. Validate online policy changes with versioned compare-and-swap. Prioritize old versions, snapshot barriers, and capacity pressure with hysteresis and fair service for continuously hot files. Throughput limits and failed backends can make a dirty-age target impossible; report deadline violations rather than promising a hard bound or silently overriding the operator's rate limit.

Atomically reserve capacity before accepting writes, including replication, staging, metadata overhead, and conversion scratch as appropriate. Keep enough reclaim/packing headroom to avoid a full-area deadlock. At exhaustion, apply bounded backpressure and return explicit space/quota errors; never evict dirty data. Shrinking below dirty usage enters drain mode. Normal unmount preserves the workspace and pending packing work; deleting a workspace is a separate authenticated destructive operation with snapshot/reference checks.

## Snapshot barriers

`workspace snapshot` establishes a durable metadata generation barrier capturing all mutations acknowledged by the workspace before the barrier. Pin that generation and its extents while subsequent writes continue copy-on-write. Materialize the selected generation into ordinary durable packs and publish a standard immutable repository snapshot only after all referenced data and tree/index state meet the normal durability predicate. Partial failures leave a resumable pending snapshot, never a falsely completed snapshot that depends on disposable cache.

Snapshot creation may be requested at any time but completion can wait for backend recovery or packing. Report the captured generation, progress, and completion/failure independently. A server-side barrier cannot include bytes still buffered in an application or FUSE/NFS client kernel: coordinated application flush/freeze is required for application-consistent snapshots. Ordinary snapshots are filesystem-consistent at the server barrier, not automatically database/application-consistent.

## Implementation steps

1. Define workspace schema, stable mutable inode identities, extent/version transactions, fencing, and recovery/reference rules.
2. Implement durable encrypted writeback adapters for librados, S3 required-target policies, and local disk with a documented durability matrix.
3. Extend the shared filesystem layer and FUSE adapter with mutations, permissions, bounded buffering, and truthful sync/error semantics.
4. Implement resumable packing, atomic placement/reference publication, concurrent-version protection, and safe Phase 30 cache admission/reclamation.
5. Add scheduling, capacity reservations, policy controls, lifecycle handling, and snapshot barriers with ordinary snapshot publication.
6. Extend Phase 32 metrics for dirty bytes/age, writeback read hits, packing throughput, durable-ack latency, snapshot progress, blocked writes, and cache demotion.

## Tests

- Filesystem tests cover concurrent writes/appends, partial writes, sparse truncate/extend, rename replacement, hard links, open-unlinked files, permissions, xattrs, and unchanged immutable base snapshots.
- Crash at every data/metadata/pack/publication/reclaim boundary; recover all acknowledged writes and syncs without stale reads, broken references, or double publication. Exercise writer fencing, credential expiry, orphan cleanup, and prune races.
- Inject Ceph replica/host failures, S3 partial-target success, timeouts, disk-full and metadata/WAL outages. Verify declared durability, bounded resource use, no weaker fallback, and truthful client errors.
- Overwrite a file while its old version packs; concurrently snapshot, read, truncate and rename. Verify snapshot barrier contents and existing snapshot immutability after restart.
- Retain hot committed extents in a lower-resilience cache, destroy that cache, and read byte-identical data from packs. Prove cache eviction cannot delete dirty data and pool-wide replication is never reduced.
- Measure sequential/random write amplification, read-your-writes latency, backlog drain under rate limits, snapshot completion, fairness, and bounded RAM under a working set larger than memory.

## Exit criterion

A FUSE workspace rooted at a snapshot/path supports tested filesystem mutations with durable writeback on each supported storage profile, truthful sync/error behavior, restart recovery, and immutable-base preservation. Changes become ordinary durable pack-backed repository data asynchronously; only committed, unreferenced writeback copies may be discarded or retained as disposable hot cache. Scheduling and capacity remain bounded and observable. New standard snapshots capture a defined workspace generation while writes continue, and no completed snapshot depends on writeback or cache availability.