# Phase 37: Multi-server writable NFS availability research

[Back to roadmap index](00-overview.md)

[Previous: Phase 36](phase-36-nfs-server-side-group-authorization.md) | [Next: Phase 38](phase-38-remote-principals-and-brokered-vaulticdb-access-tickets.md)

**Status: research and design, not yet implemented.**

**Goal:** determine whether multiple writable NFS servers can safely serve one workspace active-active (preferred), or active-passive if necessary, and specify the requirements for client-transparent failover and useful load balancing. Deliver evidence and an architecture decision, not an untested production HA promise.

## Candidate architectures

Compare at least:

- Active-active NFS frontends using one fenced, authoritative workspace transaction service. SlateDB retains one writer; horizontally scaled NFS processes do not imply multiple SlateDB writers.
- Active-active frontends with partitioned workspace/inode ownership, including cross-directory atomic operations and lock routing. Determine whether the complexity offers measured benefit over a shared authority.
- Active-passive NFS service with replicated/shared writeback and metadata, persistent export identity and handles, and fenced failover of the service endpoint and workspace ownership.

Shared librados/S3 objects alone do not serialize inode mutations, replicate lock/replay state, or invalidate frontend caches. Read-only VaulticDB replicas may lag; a frontend cannot use stale permission, namespace or extent state to decide mutations. Investigate transactional routing, version checks, read-your-writes and invalidation requirements across FUSE/NFS and snapshot/packing operations.

## Required protocol and storage evidence

Study stable export/filesystem IDs, inode generations and file handles across processes and hosts; secure distribution of handle keys; NFS write-verifier continuity; duplicate-request/replay records; exclusive-create retries; mutable directory cookies; and client attribute/data cache behavior. A new random export ID on takeover would cause stale handles, not smooth failover.

Determine where acknowledged dirty data, durable inode updates, open-unlinked references, snapshot barriers, and in-progress pack publication survive host loss. Single local-disk writeback is not a host-failover profile without a separately proven replication/recovery mechanism. Map required S3/librados acknowledgements, metadata/WAL durability, and replica lag to explicit recovery-point guarantees.

NLM/NSM recovery is part of HA, not an afterthought. Investigate shared versus migrated lock ownership, blocking callbacks, server incarnation and reclaim grace, client reboot records, routing to the correct lock service, and prevention of conflicting grants during partitions. Determine which grace periods necessarily delay clients even when data service is available.

Use Phase 22's authoritative writer fencing and conditional coordination as a starting point. Define frontend/workspace/lock/endpoint authority separately and prove a failed or partitioned old server cannot keep acknowledging writes or granting locks. Loss of the required coordination quorum must stop unsafe writes; heartbeat timeout alone cannot elect a safe replacement.

## Client failover and load balancing

Test virtual-IP takeover, L4 connection routing, client affinity and reconnect behavior, and any supported client-native multipath/trunking mechanisms. Do not assume DNS round-robin moves established mounts or that per-RPC round-robin is safe. Distinguish distributing independent exports, distributing client connections to one shared export, and balancing one client's workload across servers. Include mount service, NLM, NSM and callbacks in routing, not just NFS TCP traffic.

Measure interrupted syscalls, retry/retransmission behavior, handle validity, lock reclaim and application-visible errors on supported Linux, macOS and BSD clients with documented mount options. Define measurable RPO/RTO, maximum observed pause, and circumstances requiring remount or manual intervention. Zero downtime and transparent failover are not assumed properties of NFSv3. Compare NFSv4.1+ session/state/migration approaches if they materially improve the result, but keep protocol migration as a separate proposed implementation phase.

## Security and broker availability

All nodes must agree on Phase 36 identity revisions, export policy, root squash and permission-cache invalidation. Protect cluster control traffic and ensure a frontend compromise does not imply permission to change broker policy or issue credentials. Ordinary writable-workspace mutation is not automatically covered by Phase 38's additive-only remote-worker tickets; identify the required service authorization separately.

Model broker lock, key/storage lease expiry and broker-host loss as explicit HA dependencies. Phase 38 provides remote principals, not broker federation or automatic ceremonial unlock. Research may use independently authorized laboratory nodes, but must not bypass quorum custody, clone broker private identities, or assume an expired key can no longer decrypt data already obtained. State when continued availability requires a new ceremony and propose any broker-succession work separately.

## Research steps and deliverables

1. Review NFS/NLM/NSM specifications, selected library support and proven clustered server designs; record references and exact supported versions.
2. Build a disposable two-frontend fault-injection prototype for the strongest active-active candidate and an active-passive baseline, without enabling HA in production commands.
3. Exercise shared writes, rename/link/unlink, advisory locks, permission changes, snapshots, packing and backend degradation during host loss and partitions.
4. Measure performance, coordination overhead, client failover behavior and operational complexity against a single-server baseline.
5. Publish an architecture decision, dependency/state-ownership matrix, failure-mode table, reproducible test results, resource bounds and security analysis. Recommend active-active only if evidence supports it; otherwise explain the active-passive choice or a no-go result.
6. Propose separately scoped implementation phases with configuration, migration/rollback, client compatibility gates and explicit broker/identity prerequisites. This research phase does not itself authorize that implementation.

## Acceptance experiments

Partition the frontends from each other and from the writer/coordination store; kill the active server before and after each acknowledgement boundary; lose a writeback target; restart during lock reclaim; duplicate or drop RPC replies; route retries to a different frontend; and return an old server after takeover. Assert no lost stable writes under the declared failure model, conflicting lock grants, duplicate namespace effects, stale permission bypass, split-brain metadata, or invalid completed snapshots.

Report whether clients retain handles and locks without remount, their observed pauses/errors, the effects of hard/soft mount policy, and how many clients or operations actually benefit from load balancing. Publish failed experiments as constraints, not hidden exceptions.

## Exit criterion

A reviewed, reproducible decision establishes whether active-active writable NFS for the same workspace is feasible, which centralized services or ownership rules it requires, and its measured advantage over active-passive. Client failover and balancing guarantees, durability and locking limits, broker dependencies, and remaining implementation work are explicit. A justified active-passive recommendation or documented no-go result satisfies the research goal; unverified claims of transparent failover do not.