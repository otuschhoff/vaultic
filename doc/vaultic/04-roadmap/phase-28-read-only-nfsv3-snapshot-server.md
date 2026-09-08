# Phase 28: Read-only NFSv3 snapshot server

[← Back to roadmap index](00-overview.md)

[← Phase 27](phase-27-remote-principals-and-brokered-vaulticdb-access-tickets.md)

[CLI and operations architecture](../02-architecture/04-cli-and-operations.md) · [Restore documentation](../../050_restore.rst)

**Status: design specification, not yet implemented.**

**Goal:** expose one repository snapshot as a read-only NFSv3 export so clients can browse and copy snapshot contents without installing FUSE, macFUSE, WinFsp, or another kernel filesystem extension on the Vaultic host. The command runs an unprivileged userspace NFS server backed directly by immutable repository trees and data blobs. It complements the existing `vaultic mount` command: FUSE remains the convenient multi-snapshot local namespace, while NFS serves one explicitly selected snapshot through a portable protocol understood by macOS, Linux, BSD, hypervisors, recovery appliances, and many NAS clients.

## Scope and constraints

This phase implements:

- NFS version 3 over TCP;
- the NFSv3 mount protocol over a separate fixed or configured TCP port;
- a single immutable snapshot, optionally rooted at a subfolder, per server process;
- read-only metadata, directory, symlink, and regular-file operations;
- stable opaque file handles for the lifetime of the export;
- loopback-only service by default, with explicit controls for network exposure;
- repository-backed random reads with bounded tree and blob caches;
- client mount instructions printed from the effective configuration.

It does not implement NFSv2, NFSv4, UDP transport, NLM locking, writable exports, snapshot mutation, server-side restore, Kerberos, NFS-over-TLS, or a general replacement for a production NAS. NFSv3 has no secure native identity or transport layer. Remote use therefore requires a trusted private network, VPN, or tunnel and remains opt-in.

## Why a separate server

The current `vaultic mount` path is tied directly to `anacrolix/fuse` node and request types. It provides useful repository behavior that is not inherently FUSE-specific: snapshot selection, tree traversal, stable inode derivation, node metadata, symlink targets, data-blob offset reads, and bounded blob caching. Implementing those behaviors again inside an NFS handler would create two subtly different read paths.

Extract a protocol-neutral `internal/snapshotfs` layer first. Both FUSE and NFS adapt that layer to their respective protocols:

```text
repository + selected snapshot
            |
      internal/snapshotfs
   lookup / getattr / readdir / read / readlink
        |                       |
 internal/fuse adapter    internal/nfs adapter
        |                       |
      kernel FUSE           NFSv3 RPC/TCP
```

The extraction must preserve existing FUSE behavior and tests before the NFS server is added.

## Design

### Command and snapshot selection

Add:

```text
vaultic serve nfs [flags] SNAPSHOT
```

`SNAPSHOT` uses the same single-snapshot selector as `restore`, `ls`, and `dump`, including `latest`, full or abbreviated snapshot ID, and `snapshot:path/to/subfolder`. The selector is resolved once at startup under a repository read lock. The server exports that immutable tree until shutdown; a later `latest` snapshot does not alter an active export.

Core flags:

| Flag | Meaning |
|---|---|
| `--listen ADDRESS` | NFS and mount-service bind address; default `127.0.0.1` |
| `--nfs-port PORT` | NFSv3 TCP port; default an unprivileged documented port |
| `--mount-port PORT` | mount protocol TCP port; default a neighboring unprivileged port |
| `--export-name NAME` | exported mount path, default `/snapshot` |
| `--allow-cidr CIDR` | client source range; repeatable; loopback ranges are implicit for loopback binds |
| `--owner preserved|server|root` | report stored UID/GID, server UID/GID, or root; default `preserved` |
| `--permissions preserved|readable` | preserve stored mode bits or expose every node as owner-readable; default `preserved` |
| `--tree-cache-size SIZE` | bounded decoded-tree cache |
| `--blob-cache-size SIZE` | bounded plaintext data-blob cache, default aligned with FUSE |
| `--max-read-size SIZE` | advertised and enforced NFS read maximum |
| `--idle-timeout DURATION` | optional shutdown after no mounted clients or RPC activity |

The server prints exact macOS, Linux, and BSD mount commands using the effective address, ports, and export name. It never invokes `mount`, edits `/etc/exports`, registers a system daemon, or requires root merely to bind the default ports.

### Protocol implementation

Use a maintained Go NFSv3 ONC RPC/XDR implementation after a dependency and interoperability review; do not hand-roll RPC framing, XDR, mount protocol, or duplicate-request handling. Wrap the library behind `internal/nfs` so protocol dependency types do not leak into `snapshotfs` or command code. Pin the selected module and audit its request-size bounds, context cancellation, concurrency model, file-handle limits, and malformed-packet behavior.

Implement the NFSv3 procedures required for a normal read-only filesystem:

| Procedure | Behavior |
|---|---|
| `NULL` | liveness |
| `GETATTR` | immutable snapshot metadata |
| `LOOKUP` | child lookup with normalized component validation |
| `ACCESS` | read, lookup, and execute according to export policy; never modify or extend |
| `READLINK` | return the stored symlink target without following it server-side |
| `READ` | bounded random file read across repository blobs |
| `READDIR` / `READDIRPLUS` | deterministic directory entries, cookies, and attributes |
| `FSSTAT` | read-only synthetic capacity derived from snapshot and repository facts |
| `FSINFO` | transfer sizes, name limits, and read-only properties |
| `PATHCONF` | stable name and link constraints |
| `COMMIT` | success only as a no-op permitted by protocol compatibility, with no mutation |

Every mutating procedure, including `SETATTR`, `WRITE`, `CREATE`, `MKDIR`, `SYMLINK`, `MKNOD`, `REMOVE`, `RMDIR`, `RENAME`, and `LINK`, returns `NFS3ERR_ROFS`. Unsupported protocol or transport versions fail explicitly. UDP listeners are never opened.

The mount service implements `MNT`, `UMNT`, `UMNTALL`, `DUMP`, and `EXPORT` for the single configured export. Unknown paths are rejected. Mount tracking is observational and bounded; NFSv3 clients may disappear without unmounting, so stale mount records expire and never control authorization.

### Protocol-neutral snapshot filesystem

`internal/snapshotfs` owns:

- immutable snapshot root and optional subfolder resolution;
- path-component validation and special `.` or `/` tree-node handling;
- node type, mode, UID, GID, size, timestamps, and link-count projection;
- lazy tree loading and deterministic bytewise directory ordering;
- regular-file cumulative blob offsets and random range reads;
- symlink target retrieval;
- stable node identity derived from repository ID, snapshot ID, canonical path, and node type;
- bounded concurrent tree and plaintext blob caches;
- context cancellation and repository error classification.

FUSE becomes a thin adapter over this API. Existing xattr behavior remains in the FUSE adapter because NFSv3 has no standard extended-attribute RPC. Device nodes, sockets, and unsupported special nodes are reported as metadata only and cannot trigger host device access. Repository symlinks are returned verbatim; path traversal is always performed by the client and never causes the server to read outside the selected snapshot.

### File handles and directory cookies

NFSv3 file handles are opaque and limited in size. Each handle contains a version, per-process random export ID, compact stable node ID, node generation, and keyed authenticator. The authenticator prevents a client from altering a valid handle to reach an unexported node or another snapshot. Handles from another process, repository, snapshot, or export are rejected with `NFS3ERR_STALE`.

The server maintains a bounded node table from compact IDs to immutable snapshot nodes. Entries may be reconstructed from canonical path metadata after eviction. File-handle keys and export IDs exist only in memory and are zeroized at shutdown; persistence across server restarts is deliberately not promised.

`READDIR` cookies bind to directory node ID, immutable tree ID, and sorted entry index. Cookie verifiers change on process restart and reject cross-directory or forged continuation. Because the snapshot never changes, valid cookies remain stable for the export lifetime.

### Read semantics and caching

Reads use the same cumulative content-blob offset mapping as FUSE. The server validates signed RPC offsets and counts before conversion, caps each response at the advertised maximum, handles empty and sparse files, and never allocates directly from an untrusted client count. Concurrent reads share the repository pack cache and a bounded plaintext blob LRU.

Cache keys include blob or tree IDs, never client-controlled paths. Cache size limits are hard limits with measurable eviction. Plaintext cache buffers are cleared on eviction where practical and always on shutdown. A slow backend produces NFS I/O delay or a bounded protocol error; it must not deadlock all RPC workers. Per-client and global in-flight request limits provide backpressure.

Read errors map consistently: missing or invalid snapshot nodes to `STALE` or `NOENT`, permission denial to `ACCES`, malformed names to `INVAL` or `NAMETOOLONG`, canceled and unavailable repository reads to `IO` or `JUKEBOX` where retry is appropriate, and integrity/authentication failures to `IO` plus a high-severity secret-free event.

### Attributes and filesystem semantics

Stored POSIX mode, UID, GID, timestamps, size, symlink, and hard-link metadata are preserved by default. `--owner server` and `--owner root` change only reported ownership. `--permissions readable` is an explicit recovery convenience that adds read and directory-traverse bits without mutating repository metadata.

Inodes and file IDs are stable within one export. Directory entries are deterministic. Hard-linked nodes share identity when the snapshot metadata proves they are the same archived inode; otherwise Vaultic does not infer links from equal content. Case sensitivity follows archived names, with collisions surfaced rather than silently renamed. Names containing slash or NUL are rejected during traversal.

NFSv3 clients may cache attributes and directory entries. Since an export is immutable, the server advertises long cache-friendly values and never sends change notifications. Snapshot immutability also means close-to-open consistency cannot reveal a different generation halfway through a copy.

### Security boundary

Default binding is loopback. Binding a non-loopback address requires at least one `--allow-cidr` and an explicit `--acknowledge-insecure-nfsv3` flag. Startup explains that AUTH_SYS UID/GID claims are client-controlled, traffic and file contents are unencrypted, source addresses can be spoofed on hostile networks, and NFSv3 provides no equivalent to Phase 27 principal authentication.

Source CIDR filtering is defense in depth, not authentication. Remote deployments should use WireGuard, another authenticated VPN, an SSH TCP tunnel, or host firewall rules. The server does not accept wildcard exports, hostname-based allowlists, privileged-client assumptions, or `no_root_squash`-style authority. Reported UID/GID affects client presentation only; all server operations remain read-only regardless of AUTH_SYS identity.

The command keeps the repository read lock and required broker leases for its lifetime. Broker lock, key-lease expiry, repository integrity failure, or context cancellation stops admission, drains bounded in-flight reads, closes listeners, clears caches and handle keys, and exits without leaving a background service.

### Networking and lifecycle

NFS and mount listeners bind before readiness is announced. Partial startup closes every listener. Signal handling uses the existing command context, with a configurable graceful-drain timeout. Fixed ports make RPCBind unnecessary and keep unprivileged operation possible; printed client commands include both ports.

A machine-readable readiness record may be written only when explicitly requested and contains PID, repository ID, snapshot ID, addresses, ports, export name, and start time, but no credentials, keys, or repository password. Stale records are advisory and removed only after process-identity verification.

Metrics and events cover active mounts, RPCs by procedure and status, bytes read, cache hit and eviction counts, backend latency, denied source addresses, malformed RPCs, stale handles, and shutdown reason. Paths, symlink targets, file contents, repository keys, and client AUTH_SYS auxiliary groups are not logged by default.

## Implementation steps

1. Extract `internal/snapshotfs` from the FUSE implementation with protocol-neutral nodes, lookup, directory iteration, attributes, symlinks, range reads, stable identities, and bounded caches.
2. Refactor `internal/fuse` into an adapter over `snapshotfs` and prove existing mount behavior and tests remain unchanged.
3. Select and pin a maintained Go NFSv3 ONC RPC/XDR library after malformed-input, concurrency, cancellation, and license review; isolate it behind `internal/nfs`.
4. Implement the read-only NFSv3 and mount-protocol procedure sets, explicit `ROFS` mutation responses, fixed-port TCP service, source filtering, request limits, and error mapping.
5. Implement authenticated opaque file handles, bounded node reconstruction, deterministic directory cookies, and restart-stale semantics.
6. Add `vaultic serve nfs SNAPSHOT` with single-snapshot and subfolder selection, owner and permission projection, cache and transfer limits, lifecycle handling, readiness output, and printed client commands.
7. Add observability, secret-free security warnings, broker-lease shutdown behavior, and optional system service examples that preserve loopback defaults.
8. Document macOS, Linux, and BSD client mounting, VPN/tunnel guidance, firewall rules, expected caching, unsupported xattrs, and the choice between FUSE mount, NFS serving, and restore.

## Tests

Protocol unit tests cover every implemented NFSv3 and mount procedure, every mutating operation returning `ROFS`, malformed and oversized XDR, unsupported versions and transports, invalid names, offset overflow, short and cross-blob reads, empty and sparse files, symlinks, hard links, special nodes, deterministic `READDIRPLUS`, cookie verifier rejection, forged and stale file handles, and repository error mapping.

`internal/snapshotfs` contract tests run the same fixtures through FUSE and NFS adapters and compare lookup results, attributes, directory ordering, symlink targets, and file bytes. Existing FUSE integration tests remain passing after extraction.

Platform integration tests start the server on loopback with isolated ports and mount it using the native client on supported macOS, Linux, and BSD CI workers. They read whole and ranged files, traverse nested trees, copy a snapshot tree, verify hashes and metadata, exercise concurrent readers and cache eviction, unmount cleanly, and prove writes, deletes, renames, chmod, and timestamp changes fail read-only. Tests skip with an explicit reason when the host cannot perform NFS mounts; protocol-level tests remain mandatory everywhere.

Security tests verify loopback defaults, non-loopback acknowledgement and CIDR requirements, source denial, no UDP listener, no RPCBind dependency, no credential or path leakage in events, file-handle tamper rejection, bounded mount and node tables, malformed-request resilience, rate limiting, graceful shutdown on broker lock and lease expiry, and cache clearing. Fuzz tests target XDR decoding boundaries, file-handle parsing, directory cookies, and component normalization.

Performance tests compare sequential and random reads with FUSE over the same local repository, verify bounded memory under a working set larger than both caches, and ensure one slow client cannot starve unrelated reads. The phase sets regression thresholds from measured baselines rather than requiring NFS to outperform FUSE.

## Exit criterion

`vaultic serve nfs SNAPSHOT` exposes exactly the selected immutable snapshot, or selected subfolder, through a standards-compliant read-only NFSv3 TCP export without requiring FUSE or root privileges on the Vaultic host. Native macOS, Linux, and BSD clients can mount, browse, and copy the export with byte-for-byte correct contents and stable metadata; every mutation fails with `NFS3ERR_ROFS`. FUSE and NFS share one tested snapshot filesystem implementation, memory and request concurrency remain bounded, forged or stale handles cannot escape the export, shutdown follows repository and broker lease lifetime, and any non-loopback deployment requires explicit acknowledgement plus network restrictions with the protocol's lack of encryption and strong authentication clearly documented.
