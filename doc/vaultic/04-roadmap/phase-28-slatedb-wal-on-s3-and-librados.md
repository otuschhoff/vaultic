# Phase 28: VaulticDB/SlateDB WAL on S3 and librados

[Back to roadmap index](00-overview.md)

[Previous: Phase 27](phase-27-native-ceph-librados-object-store-backend.md) | [Next: Phase 29](phase-29-slatedb-read-cache-tiers.md)

**Status: implemented.**

**Goal:** allow VaulticDB/SlateDB write-ahead logs (WAL) to use S3-compatible object storage or native librados, independently of the main metadata store. Local-disk WAL support is assumed to be the existing baseline for this plan; confirm that baseline during implementation without turning this into a separate local-WAL project.

## Prerequisites and boundaries

- Phase 27 provides the native RADOS adapter; existing S3 support supplies the other remote transport.
- Preserve Phase 22 writer fencing, handoff, transaction idempotency, and durable-commit semantics, and Phase 18 metadata encryption.
- WAL is authoritative recovery data until its contents are durably checkpointed. It is not a best-effort cache and must never be subject to cache LRU or capacity eviction.
- Verify the pinned SlateDB version's separate WAL-store API. If it lacks one, deliver the required supported upstream integration or explicit dependency update; do not substitute an unverified log format or mark configuration-only work complete.

## Design

### Store selection and namespace

Add an independently selectable WAL target: existing/default behavior, local disk, S3, or librados. Sealed topology identifies the WAL store, credentials, repository/database identity, prefix or namespace, and effective durability policy. Separate WAL objects from SSTs, manifests, cache entries, and other repositories. Configuration/status must distinguish WAL placement from database placement and replication.

Preserve the current default when no WAL override is supplied. Resolve remote credentials through the existing broker lifecycle; use dedicated WAL permissions rather than granting a reader or cache process write access. WAL contents, including keys and values, must remain encrypted outside process memory under the metadata encryption policy.

### Durability and recovery

A successful durable-commit acknowledgement requires the WAL durability condition to have completed at the configured target. S3 acknowledgement must mean the committed immutable WAL object is stored, not merely queued in a local upload buffer; librados must use its documented durable completion semantics. Publish segments and their ordering metadata using SlateDB's supported recovery protocol.

Replay must reconstruct every acknowledged transaction after process or host loss and tolerate retries without duplicate logical commits. Fence stale writers across both the WAL and metadata targets, including asymmetric outages. Failed writes, broker expiry, a full target, or an unavailable WAL must return an explicit error or existing deferred outcome; never silently fall back to weaker local durability.

Retain WAL until all recovery consumers required by the configured mode can recover from durable checkpoints. Garbage collection must be bounded by recovery/checkpoint state, not age alone. Expose lag, oldest required segment, retained bytes, and cleanup failures.

### Target changes and operations

Provide a documented drain/checkpoint/validate/switch sequence for changing WAL targets. A switch must survive crashes or remain explicitly restart-required; it must not strand acknowledged transactions. Declare whether a local-only WAL permits host-loss recovery and remote-reader catch-up rather than assuming equivalence with a shared WAL.

Expose effective target, encryption state, write/flush latency, uploaded bytes, outstanding flushes, replay progress, and durability failures. Include failure and recovery procedures for independent WAL and metadata store outages.

## Implementation steps

1. Verify local baseline and SlateDB WAL APIs; document segment, replay, checkpoint, and fencing contracts.
2. Add sealed WAL-target configuration and resolve S3/librados stores independently from SST/manifest placement.
3. Wire WAL encryption, credential renewal, durable acknowledgement, bounded buffering, and backpressure.
4. Implement recovery, checkpoint-aware retention, and target-change/rollback procedures.
5. Add operational status and tests for each WAL/database store combination supported by the compatibility matrix.

## Tests

- Verify local baseline plus S3 and librados WAL with local and remote metadata stores, including reopen on a second host where the durability policy permits it.
- Crash before upload, after durable WAL publication, before checkpoint publication, and during target changes; recover all acknowledged transactions exactly once logically.
- Inject truncated/corrupt segments, interrupted requests, duplicate retries, stale writers, clock skew, credential expiry, and independent target outages.
- Prove WAL retention cannot discard needed replay data and cache eviction cannot touch WAL; verify encryption and secret-free diagnostics.

## Exit criterion

VaulticDB independently selects S3 or librados for WAL, preserves existing local behavior, and recovers every durably acknowledged transaction under the documented failure model. Encryption, writer fencing, retention, credential lifecycle, and safe target changes are verified end to end; no best-effort storage path can weaken WAL durability.

## Implemented result

The pinned SlateDB revision provides supported separate-WAL object-store builders for writers and readers. VaulticDB uses those APIs directly. VaulticDB atomically binds a hash of the effective WAL-store identity into the metadata generation and rejects mismatched reopens. SlateDB publishes immutable WAL SSTs before resolving durable write handles, replays them for writers and followers, and garbage-collects them only after manifest checkpoint state no longer requires them.

`VAULTICDB_WAL_STORE` and the matching `--daemon-wal-*` flags select `inherit`, `local`, `s3`, or `rados`. `inherit` is the default and preserves pre-Phase-28 placement. A separate target receives a repository-hash namespace below its configured prefix, while SlateDB adds database and WAL paths below that root. S3 and RADOS use the object-store client's completed put operation as the durable publication boundary; there is no local upload queue or fallback target.

| Metadata store | Inherited WAL | Local WAL | S3 WAL | RADOS WAL |
|---|---:|---:|---:|---:|
| local | supported | supported, process/host-local | supported, shared | supported on RADOS builds, shared |
| S3 | supported | supported, process/host-local | supported, shared | supported on RADOS builds, shared |
| replicated | supported | supported, process/host-local | supported, shared | supported on RADOS builds, shared |

Memory stores remain test-only. Local WAL does not provide host-loss recovery or remote-reader catch-up unless the directory is itself durable shared storage. S3 and RADOS WAL do. All separate WAL objects pass through the metadata encryption keyring. Capsule mode leases a distinct `wal` target at `storage-maintain`; renewal swaps credentials in place and rejects a changed target identity.

Writer ownership remains anchored by the Phase 22 compare-and-swap epoch in the authoritative coordination store. VaulticDB checks that epoch before every mutation, including when WAL and metadata are independently reachable, and SlateDB carries its writer epoch through WAL publication and replay. Losing either target therefore fails or fences the mutation; reaching the WAL alone never grants writer authority.

Changing a WAL target is restart-required. Drain writers, wait for active transactions to reach zero, demote or shut down so SlateDB flushes and checkpoints, retain the old target, update sealed topology, and start against the new target. SlateDB validates the persisted WAL identity and fails closed if the switch could strand replay data. To migrate an existing database, create and verify a new metadata generation under the new WAL target, activate it through the metadata-generation workflow, and retain the old generation and WAL until observation and retirement checks complete. Never copy only currently visible WAL objects or delete them by age.

Capabilities report target kind, inherited/separate state, durability class, encryption state, successful initial replay, uploaded bytes, outstanding flushes, durability failures, last flush latency, retained bytes and segments, the oldest retained segment timestamp, and cleanup failures. The separate-store monitor inventories existing objects before opening SlateDB and then observes the post-encryption object stream, so retention survives restarts and uploaded-byte accounting reflects bytes sent to the provider.