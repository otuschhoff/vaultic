# Phase 29: VaulticDB/SlateDB read-cache tiers

[Back to roadmap index](00-overview.md)

[Previous: Phase 28](phase-28-slatedb-wal-on-s3-and-librados.md) | [Next: Phase 30](phase-30-vaultic-best-effort-read-cache-tier.md)

**Status: design specification, not yet implemented.**

**Goal:** give VaulticDB/SlateDB a disposable read-cache hierarchy backed by local disk, S3-compatible storage, and/or native librados. Operators can bound cache size, configure LRU and ageing, and grow, shrink, or reprioritize cache tiers online without reopening the database.

## Scope and prerequisites

Reuse Phase 27's native adapter, Phase 26 credential management, and SlateDB's supported cache hooks. Phase 28 WAL and the authoritative metadata store remain outside the cache hierarchy. Prefer the pinned SlateDB cache abstractions; if remote persistent caching needs an extension, implement and validate that integration explicitly.

This phase caches metadata database reads, not Vaultic repository pack data. Phase 30 uses the same storage and policy concepts for the latter without mixing identities, quotas, or eviction authority.

## Design

### Correctness and lookup

Define the cached unit using SlateDB's immutable SST/block identities, version/generation, offsets, and encryption context. Do not serve mutable manifests, fencing state, or WAL from a stale cache. Cache hits must pass integrity and identity checks before use; corrupt or mismatched entries are evicted/quarantined and fetched from the authoritative store.

Read through tiers in configured priority order, then the authoritative store. Admission and promotion are best effort and bounded; cache failures, expiry, missing entries, or credential loss must not turn a successful authoritative read into a failure. Apply timeouts and circuit breakers so a slow remote cache cannot stall reads indefinitely. Never acknowledge a write based on cache contents or count cached replicas toward metadata durability/quorum.

Persist ciphertext where possible; any representation requiring plaintext must be encrypted at rest under an explicit policy. Namespace by repository, database identity, object generation, and cache format. Rebuild cache bookkeeping after restart without relying on it for recovery or correctness.

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

## Exit criterion

VaulticDB uses local-disk, S3, and librados read caches with measurable warm-read benefit and unchanged query/durability semantics. Size limits, LRU, ageing, online grow/shrink, and priority changes work under load with observable convergence. Total cache loss is recoverable by normal reads alone and cannot lose committed metadata.