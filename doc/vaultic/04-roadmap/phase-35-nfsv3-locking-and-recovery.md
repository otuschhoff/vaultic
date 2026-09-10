# Phase 35: NFSv3 locking and recovery

[Back to roadmap index](00-overview.md)

[Previous: Phase 34](phase-34-writable-nfsv3-exports.md) | [Next: Phase 36](phase-36-nfs-server-side-group-authorization.md)

**Status: design specification, not yet implemented.**

**Goal:** support interoperable NFSv3 advisory byte-range locking, including blocking acquisition and recovery after client/server restart. Provide an NLM service and the necessary NSM/statd integration where these are not supplied by the selected server stack.

## Protocol and ownership

Audit Phase 31's maintained ONC RPC/NFS dependency for NLM version 4 (64-bit offsets for NFSv3), Network Status Monitor support, callback handling, and recovery facilities. Reuse a maintained implementation where possible; if extension is necessary, isolate it behind a bounded lock-service interface and validate it against the relevant NLM/NSM specifications and native clients. Do not assume NFSv3 core procedures provide locks.

Implement the negotiated TEST, LOCK, CANCEL, UNLOCK, GRANTED callback, reclaim, and applicable asynchronous message/result procedures. Define support or explicit rejection for SHARE/UNSHARE and unmonitored operations based on the client compatibility matrix. Support shared/exclusive ranges, zero-length-to-EOF, overlapping/split/merged ranges, owner identity and cookies, duplicates, blocking waiters, cancellation races, and deterministic conflict results. Lock identity binds to export/workspace and stable inode generation, not a pathname or PID alone.

Locks are advisory, not mandatory access control: ordinary NFS READ/WRITE does not acquire or enforce application advisory locks implicitly. If FUSE and NFS share a workspace, route supported POSIX locks through one owner-aware lock manager with tested semantics, or reject/document unsupported cross-protocol locking modes. Do not claim all `flock` behavior is identical to POSIX range locks on every client OS.

## Recovery and network service

Persist the bounded recovery information needed to recognize reclaiming clients and owners. On server restart, enter a configured grace period: allow valid reclaim, deny conflicting new lock acquisition, and expire unreclaimed state only under the documented protocol. Integrate NSM client reboot notifications, durable server incarnation state, and callbacks so rebooted clients release stale locks without trusting arbitrary packets to clear another client's locks. Duplicate callbacks and crash-during-reclaim are safe.

A network partition is not proof that a client rebooted. Do not discard a live client's lock just because a health timeout expired. Specify the cleanup and administrative recovery policy and its safety limits. Coordinate server/lock-manager lifetime with the workspace writer epoch; a fenced server cannot grant new locks. Multi-server shared lock state is deferred to Phase 37.

Document fixed/configured NLM and NSM ports, callback addresses, firewall rules, and rpcbind registration requirements for supported native clients. Phase 31's two-port, no-rpcbind deployment assumption may no longer suffice; review collision handling, privilege needs and existing system statd integration rather than silently starting competing daemons. Expose only the reviewed TCP/UDP transports required by the compatibility matrix. Callbacks use admitted client identities/addresses with rate limits, not arbitrary caller-supplied destinations.

## Implementation steps

1. Select the NLM/NSM integration and publish procedure, transport, and client compatibility matrices.
2. Implement bounded lock state, owner/range semantics, wait queues, callbacks, replay and cancellation.
3. Add restart persistence, grace/reclaim, NSM reboot handling, and writer-fence integration.
4. Add lock-service configuration, security controls, and Phase 32 lock/conflict/wait/grace metrics without paths as labels.
5. Document client setup, advisory-lock limits, manual recovery, and cross-protocol behavior.

## Tests

- Native Linux/macOS/BSD clients exercise lock conflicts, blocking wakeups, unlock/cancel races, multiple owners, ranges beyond 4 GiB, EOF ranges, and rename/unlink of locked files.
- Restart clients and server, drop/duplicate callbacks, partition and reconnect networks, interrupt reclaim, and verify no simultaneous conflicting grants or premature timeout-based lock theft.
- Fuzz RPC decoding, owner/cookie lengths, range overflow, and spoofed reboot/callback traffic. Bound memory, waiters, callback retries and persisted recovery state under abusive clients.
- Verify lock-manager fencing and document tested POSIX/FUSE/NFS interoperability; unsupported client facilities are reported explicitly.

## Exit criterion

Supported NFSv3 clients can acquire, test, block on, cancel, and release advisory locks with correct conflict semantics. Client/server recovery, grace and reclaim are interoperable and bounded, and restart or partition does not silently grant conflicting locks. Networking and security requirements are explicit; clustered lock failover is not claimed yet.