# Phase 19: Multi-provider cold storage pool and replicated metadata store

[← Back to roadmap index](00-overview.md)

[← Phase 18](phase-18-slatedb-metadata-encryption-and-unified-key-envelope.md) · [Phase 20 →](phase-20-quorum-based-encryption-unlock.md)

**Goal:** implement multi-backend cold storage pools ($K$-of-$M$ durability quorums across arbitrary active cold providers with read-only legacy backends) and multi-cloud replicated metadata stores for `vaulticdb`.

**Implementation steps:**

1. Implement `ingest` and `read_enabled` flag evaluation in the backend registry (`placement_backends`): mark legacy cold backends (`ingest: false`, `read_enabled: true`) as read-only pools, and route all new pack allocations exclusively to active ingesting backends (`ingest: true`).
2. Implement $K$-of-$M$ multi-provider cold placement scheduling: evaluate the durability predicate (`min_copies`, `min_domains`, `min_offsite`) over the active ingesting backend pool (e.g. 2-of-3 active cold backends), writing parallel placements (`pl:<pack-id>:<backend-id>`) during backup jobs.
3. Implement `ReplicatedObjectStore` wrapper in `vaulticdb` Rust layer for synchronous parallel writes of SlateDB metadata (SSTs, WALs, manifests) across multiple cloud providers (e.g. AWS S3 + Azure Blob / Cloudflare R2) with primary provider read routing, transparent failover, and epoch-based fencing.
4. Implement zero-egress natural drain for legacy backends (`ingest: false`): allow old cold packs to linger on legacy backends until expired by retention policy (`min_retention_until`), purge unreachable packs directly via deletion queue (`dq:`), and route defragmentation repacks into new packs written to the active ingesting pool.
5. Expose CLI backend management commands: `vaultic index backends` status showing `ingest`/`read_enabled` flags per pool, `vaultic index placement` showing $K$-of-$M$ quorum compliance per pack, and `vaultic index placement migrate-pool` options.

**Tests:** multi-provider cold placement test verifying new packs are placed on $K$ of $M$ active backends while legacy backends receive 0 new writes; legacy backend read and warm-up test confirming restore requests route to old backends via `pl:` records; zero-egress natural drain test verifying old packs are deleted from legacy backends when retention expires and repacked blobs write to active ingesting backends; `ReplicatedObjectStore` unit test verifying synchronous multi-cloud write, transient provider outage handling, and primary-to-secondary read failover.

**Exit criterion:** new data packs are durably multi-homed across $K$-of-$M$ active cold providers, legacy cold backends receive zero new writes and naturally drain as retention expires, and `vaulticdb` metadata is synchronously replicated across multi-cloud storage.

**Current implementation state (2026-08-30): complete.** Placement backend registry entries now carry explicit `ingest` and `read_enabled` flags with default-on semantics. New placement planning and worker execution target only ingest-enabled backends, while read routing still uses live placements on read-enabled legacy backends for restore and warm-up. `index backends --no-list` reports configured backend IDs, roles, locations, and ingest/read flags without issuing archival list requests; `index placement migrate-pool --from <legacy> --to <active>` queues pack copies from a read-enabled source to an ingest-enabled target, and existing eviction/GC retention checks handle natural drain after active copies satisfy durability. `vaulticdb` supports `VAULTICDB_OBJECT_STORE=replicated`, synchronously writes/copies/deletes/multipart-completes to all configured local, S3-compatible, and Azure Blob replicas, and reads/lists from the primary with error failover to later replicas. Focused Go placement/config/CLI tests and the full Rust daemon test suite pass.
