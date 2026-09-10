# Phase 34: Writable NFSv3 workspace exports

[Back to roadmap index](00-overview.md)

[Previous: Phase 33](phase-33-writable-fuse-and-durable-writeback.md) | [Next: Phase 35](phase-35-nfsv3-locking-and-recovery.md)

**Status: design specification, not yet implemented.**

**Goal:** export a snapshot plus optional path as a writable NFSv3 workspace using the same durable overlay, VaulticDB inode state, pack commit, cache demotion, and snapshot barriers as writable FUSE. Do not create an NFS-specific writeback implementation or mutate the source snapshot.

## Scope and prerequisites

Extend Phase 31's NFS adapter over the Phase 33 mutable filesystem API. Retain read-only exports unchanged and make write access explicit. Initially one fenced workspace authority owns mutations; multiple NFS clients are supported, but independently active server instances for the same workspace are not enabled here. NLM is Phase 35; authoritative server-sourced group membership is Phase 36. Until then, writable exports are restricted to explicitly trusted clients and workloads that do not require distributed advisory locks.

## Export and protocol contract

Proposed command:

```text
vaultic serve nfs --read-write --workspace NAME --writeback-profile PROFILE SNAPSHOT[:PATH]
```

Resolve the immutable base once, persist the workspace/export binding, and reject attempts to reopen the workspace with a different repository/base/root. Workspace status, policy, drain, and snapshot commands are the Phase 33 commands. Writable export capacity reflects admissible writeback headroom, not unlimited underlying repository capacity.

Implement `WRITE`, `COMMIT`, `CREATE`, `SETATTR`, `REMOVE`, `RENAME`, `LINK`, `MKDIR`, `RMDIR`, and `SYMLINK` through shared transactions. Define `MKNOD` support explicitly or return the appropriate unsupported/permission error; never instantiate host devices. Implement NFSv3 exclusive-create verifiers and guarded attribute updates, weak cache consistency attributes, mutation timestamps, cross-directory rename and directory cookie invalidation. Writable mode must not reuse Phase 31's immutable-attribute caching assumptions. Read-only exports still return `ROFS` for every mutation.

Use stable workspace inode IDs and generations independent of paths. Authenticate handles with export/workspace identity and reject cross-export handles. Persist the export identity and protected handle-verification material required for restart recovery; this intentionally extends Phase 31's process-lifetime handle scheme for writable exports. Removed and reused inodes cannot validate an old handle. Define mutable-directory cookie/verifier behavior without promising an atomic directory enumeration across concurrent changes.

## Stable writes, replay, and snapshots

Follow NFSv3 `UNSTABLE`, `DATA_SYNC`, `FILE_SYNC`, write verifier, and `COMMIT` semantics. The conservative first implementation may make every accepted write `FILE_SYNC`, even when a client requests weaker stability: reply only after Phase 33's data and inode recovery state are durable. A stable reply means recoverable workspace data, not immediate pack conversion. `COMMIT` must enforce the relevant durability barrier and return truthful errors and verifier state, never inherit the read-only no-op shortcut without validation.

If unstable writes are later enabled, define bounded buffering, retransmission on verifier change, range-commit behavior, and crash recovery before exposing that mode. A restart or failover that loses unstable state must change the write verifier. Do not claim client-side buffered data is included in a server-side workspace snapshot; NFS clients/applications must flush for application-consistent snapshots.

Use the chosen NFS library's duplicate-request handling and augment it with bounded, appropriately retained durable mutation replay records for non-idempotent operations. Specify replay keys, client identity limits under AUTH_SYS, retention, and restart behavior; RPC XID alone is not a globally unique transaction ID. Ambiguous retries cannot duplicate exclusive creates, links, or namespace mutations. Map durability, quota, stale-handle, conflict, and authorization failures to correct protocol errors.

## Security and lifecycle

Keep loopback default, explicit network exposure acknowledgement, source restrictions, and trusted-network/VPN requirements. AUTH_SYS does not authenticate the incoming UID. Apply server-side POSIX operation checks, root squashing by default, and explicit anonymous UID/GID mapping; reject read-only presentation overrides such as `--permissions readable` when they would bypass writable authorization. Phase 36 replaces reliance on client-provided group lists with authoritative server-side membership, not with stronger UID authentication.

Share the workspace authority's permission and inode-version checks, cache identity, quotas, and snapshot consistency with FUSE. Simultaneous FUSE and NFS access is supported only through that same authority with a tested invalidation model; otherwise reject the second attachment rather than allowing divergent views. Credential loss and shutdown stop admission, drain bounded work, preserve durable writeback, and expose pending pack commits for restart.

## Implementation steps

1. Extend export configuration and persistent handles to attach Phase 33 workspaces while retaining immutable exports.
2. Map NFSv3 mutations, stable writes, COMMIT, replay protection, attributes and cookies onto shared filesystem transactions.
3. Add writable export permissions, capacity/error reporting, lifecycle handling, and workspace snapshot controls.
4. Document trusted-client and no-NLM limitations, supported client mount options, durability guarantees, and later Phase 35/36 upgrades.
5. Extend Phase 32 metrics for write/COMMIT latency, stable bytes, replay hits, dirty backlog, and protocol failures.

## Tests

- Run mutation and byte/metadata equivalence fixtures through FUSE and NFS, including concurrent clients, holes, rename, hard links and snapshot barriers.
- Verify each write stability mode, COMMIT ranges, exclusive-create replay, duplicate non-idempotent RPCs, dropped replies, server crashes, handle persistence and stale-inode rejection.
- Inject writeback/WAL outages and capacity pressure; stable acknowledgements must survive the declared failure model and failures must not become false successes.
- Exercise native Linux/macOS/BSD clients where available, mutable attribute visibility and cookies, root squash, read-only regression, and shutdown with pending pack work. Missing privileged mount facilities are explicit skips, not substitutes for protocol tests.

## Exit criterion

A trusted client can mount a selected snapshot/path as a writable NFSv3 workspace with the same data, snapshot, durability, and cache-transition guarantees as FUSE. Stable writes and COMMIT survive tested restart scenarios, mutation retries are safe, handles and attributes have documented mutable semantics, and existing read-only exports remain unchanged. No distributed-lock or multi-server availability guarantee is implied before the later phases.