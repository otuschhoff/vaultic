# Phase 29: VaulticDB/SlateDB read-cache tiers

[Back to roadmap index](00-overview.md)

[Previous: Phase 28](phase-28-slatedb-wal-on-s3-and-librados.md) | [Next: Phase 30](phase-30-vaultic-best-effort-read-cache-tier.md)

**Status: complete; implementation and independent audit finished.**

The current implementation caches only tagged foreground compacted-SST reads. Each tier has immutable startup confidentiality: encrypted tiers cache authoritative ciphertext below metadata envelope encryption, while explicitly acknowledged highly trusted decrypted tiers cache plaintext above it. Mixed modes share one coordinator, policy revision, and aggregate budget. WAL, manifests, writer fencing, coordination, policy, and other control traffic bypass both layers. Conditional reads and SlateDB retry reads bypass cache. Retry reads evict the affected cached identity before reading the authority.

SlateDB's pinned revision assigns compacted SSTs fresh ULID identities and publishes them under identity-derived paths. VaulticDB treats those paths as generation-immutable. Cache sidecars retain the authority's ETag/version metadata, response range, attributes, and byte-domain digest. Confidentiality is part of the tier namespace and entry identity, so a mode change cannot reinterpret existing bytes. A retry after SlateDB detects corruption evicts the path/range identity, preventing reuse of a stale generation even if a backend violates the immutable-path contract.

Policy is stored with conditional CAS under a dedicated repository/database-scoped path in the uncached coordination authority, independent of tier order or enabled state. The successful policy-document CAS is the commit point. A lost response is resolved by reading back the exact revision and document; after commit, quota-ledger synchronization is recoverable asynchronous work and is reported as lag/error rather than returned as an ambiguous policy failure. Rollback is a new validated CAS revision restoring the prior values, never an in-place rewrite or a reversal inferred from a failed response. Only constructor bootstrap may create revision 0, and only when both policy and quota-ledger state prove pristine. Heartbeats poll and atomically install a complete validated policy snapshot, reject lower revisions and same-revision canonical-content changes, and never recreate a missing policy. Missing, unavailable, incomplete, or malformed policy fails closed by disabling every tier while authoritative reads continue; the error clears only after a valid policy read and install. Confidentiality/trust remains immutable startup configuration and is not part of online policy mutation.

A separate versioned CAS quota ledger records immutable cache entries, per-manager committed and reserved bytes by tier, aggregate usage, policy revision, and manager/reconciler leases. Before physical writes, each admission is recorded with its cache key, owner, bytes, expiry, policy revision, and monotonic generation in `admitting` state. Commit CAS-transitions that exact generation to `active`; eviction transitions it to `deleting` before touching objects, blocks replacement while deletion is pending, confirms both data and sidecar are absent, and CAS-removes only that generation. Failed marks and deletes remain charged in an unbounded in-memory pending map serviced by one wakeable worker with capped exponential backoff, independent of fill-task permits. The worker periodically rediscovers ledger-owned deleting entries. Managers use random 256-bit identities. Production leases last 60 seconds and renew every 20 seconds; bounded CAS retries handle concurrent creation and updates. Every policy and quota get, body read, put, and CAS has a bounded coordination deadline. A failed ledger read, write, or renewal disables new admission while authoritative reads continue. Heartbeat recovery reacquires a manager lease only through fenced complete inventory; any ledger mutation during inventory aborts publication. Clean shutdown fences new cache work, cancels and awaits cache fills, promotions, the deletion worker, and heartbeat work, then makes bounded deletion progress and reports any remaining pending work before releasing reservations and the lease. Abrupt termination remains conservatively charged until lease expiry and a successful fenced reconciliation.

Startup blocks cache admission while a fenced reconciler streams a complete inventory of enabled and disabled tiers and conservatively charges data, sidecars, and orphans. Partial listings and inventories whose ledger revision changed are never published. Only entries with both data and a valid sidecar become active; unexpired admissions survive reconciliation, while expired or partial generations remain charged as deleting until cleanup is confirmed. Cache entries remain globally keyed by immutable cache identity, so restarts can reuse them without a stable process identity; the ledger accounts each physical entry once while manager grants serialize reservations. Policy CAS remains the sole policy authority: heartbeat polling updates hit-only managers, reservations verify the installed revision, and commit validates the persisted policy revision and exact admitting generation in the ledger CAS. If a local policy check fails after the active commit, that exact generation enters the deletion protocol. Ledger policy revision never decreases. Physical usage is removed from the ledger only after confirmed deletion. Fills and promotions are bounded by bytes and task count; coalesced leaders publish origin results to waiters before detached fill, and completed flight-map entries are removed after bounded work. Responses larger than one configured part bypass admission. Read access time is maintained in local ledger state rather than rewritten into persistent sidecars, so reads cannot recreate metadata after retirement. Pinned retiring entries remain charged and are deleted only after final unpin, with deletion failures visible as reconciliation lag, unverified/stale bytes, and pending reclaim. Shared used/reserved and per-tier counters come from the latest ledger snapshot; process-local counters are exposed separately, and only a fenced complete inventory clears unverified state.

**Goal:** give VaulticDB/SlateDB a disposable read-cache hierarchy backed by local disk, S3-compatible storage, and/or native librados. Operators can bound cache size, configure LRU and ageing, and grow, shrink, or reprioritize cache tiers online without reopening the database.

## Scope and prerequisites

Reuse Phase 27's native adapter, Phase 26 credential management, and SlateDB's supported cache hooks. Phase 28 WAL and the authoritative metadata store remain outside the cache hierarchy. Prefer the pinned SlateDB cache abstractions; if remote persistent caching needs an extension, implement and validate that integration explicitly.

This phase caches metadata database reads, not Vaultic repository pack data. Phase 30 uses the same storage and policy concepts for the latter without mixing identities, quotas, or eviction authority.

## Design

### Correctness and lookup

Define the cached unit using SlateDB's immutable SST/block identities, version/generation, offsets, and encryption context. Do not serve mutable manifests, fencing state, or WAL from a stale cache. Cache hits must pass integrity and identity checks before use; corrupt or mismatched entries are evicted/quarantined and fetched from the authoritative store.

Read through tiers in configured priority order, then the authoritative store. Admission and promotion are best effort and bounded; cache failures, expiry, missing entries, or credential loss must not turn a successful authoritative read into a failure. Apply timeouts and circuit breakers so a slow remote cache cannot stall reads indefinitely. Never acknowledge a write based on cache contents or count cached replicas toward metadata durability/quorum.

Default every tier to ciphertext. Permit plaintext only with explicit per-tier acknowledgement and a highly trusted backend; backend encryption at rest, TLS, or CephX does not change the plaintext trust boundary or promise secure erase. Reject encrypted-mode tiers when repository metadata encryption is off rather than claiming plaintext is encrypted. Namespace by repository, database identity, object generation, cache format, and confidentiality. Rebuild cache bookkeeping after restart without relying on it for recovery or correctness.

### Capacity, LRU, and ageing

Each tier has an enabled state, maximum byte size, eviction policy, idle-age limit, optional absolute age limit, and priority. Provide an aggregate budget when tiers share a resource. Account for entries, in-flight admissions, staging, and cache metadata; define a bounded transient-overhead allowance. Reject new admissions when capacity cannot be reserved rather than allowing unbounded growth.

Use LRU within the configured priority class and expire entries under the ageing policy. Keep active reads safe by reference tracking; capacity pressure may defer reclamation of pinned entries but cannot invalidate an in-flight read. Remote listing and access bookkeeping must be bounded and batched. Report estimated versus verified usage and reconciliation lag for shared remote caches, with coordinated quota enforcement rather than independent clients each assuming the whole budget.

### Online control plane

Provide authenticated status and update operations for maximum size, ageing, enabled state, read priority, and admission/promotion priority. Apply validated, versioned configuration atomically and persist it through the appropriate topology/configuration authority; never expose credentials in the update API.

Growing makes new capacity available immediately. Shrinking stops excess admissions and evicts asynchronously toward the new limit while existing reads finish; report requested limit, effective usage, pending reclaim, pinned bytes, and completion/failure. A zero limit drains/disables the tier. Priority changes affect new lookups and admissions without moving or dropping active reads; migrations and warming remain bounded background work. Concurrent updates use compare-and-swap revisions and have a documented rollback path.

## Implementation steps

1. Define the cache identity/integrity contract and integrate with SlateDB read-cache APIs.
2. Implement local, S3, and librados tiers with bounded asynchronous fill, read coalescing, bypass, and corruption recovery.
3. Add quota accounting, LRU, ageing, admission reservations, and restart reconciliation.
4. Implement online grow/shrink, drain, priority updates, configuration persistence, and progress/status APIs.
5. Expose hit/miss rate, origin reads avoided, latency, used/reserved/pinned bytes, evictions by reason, age distribution, and resize progress.

## Tests

- Compare cold/warm/disabled-cache query results across all three backends and mixed tier orders; exercise readers and writer-side reads.
- Verify LRU selection, idle/absolute ageing, reservation bounds, shared quotas, restart reconciliation, and oversized-object admission rejection.
- Resize up/down, disable, and change priorities under sustained reads; verify no restart, incorrect reads, deadlock, or unbounded over-limit growth, and eventual shrink completion after pins release.
- Delete or corrupt the entire cache, deny credentials, and inject latency/outages; authoritative reads still succeed and stale generations are never served.
- Prove WAL, manifests, writer fencing, and authoritative SSTs cannot be deleted through cache credentials or eviction; verify encryption and no secret leakage.

The automated contract suite includes deterministic two-manager delete/admit and reconciler/mutation races, reservation-before-inventory preservation, post-active-commit policy races, hit-only policy disable propagation, policy deletion/rollback/equivocation/malformed fail-closed behavior, committed-CAS response loss, startup policy outage recovery, hanging-coordination origin latency, lease expiry recovery, delete outage and burst retry, pinned-read retirement without sidecar recreation, delayed-fill waiter latency, bounded flight/task state, shutdown release, and global-versus-local status accounting. A temporary-directory test exercises the real local object-store adapter across restart and confidentiality namespaces. The generic `ObjectStore` fault harness covers S3-compatible semantics without claiming provider-specific behavior. Native RADOS store tests are feature-gated; linking and live-cluster integration require the librados development library and an external Ceph test environment.

## Exit criterion

VaulticDB uses local-disk, S3, and librados read caches with measurable warm-read benefit and unchanged query/durability semantics. Size limits, LRU, ageing, online grow/shrink, and priority changes work under load with observable convergence. Total cache loss is recoverable by normal reads alone and cannot lose committed metadata.

The implementation meets this criterion, including cross-process shared tier and aggregate quota enforcement. The final independent audit found no actionable issues.