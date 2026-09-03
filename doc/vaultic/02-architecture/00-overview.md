# Architecture Overview

[← Back to index](../README.md)

The vaulticdb metadata service and its SlateDB-backed schema replace vaultic's
legacy JSON index at scale (approximately 1.4 billion inodes, 500+ TB) while
keeping every legacy Restic/Rustic repository fully readable and writable. See
[Vision, Scope, and Non-Negotiable Guarantees](../01-strategy/01-vision-and-principles.md)
for why these constraints exist, and [Roadmap](../04-roadmap/00-overview.md) for
the phased plan that implements this design.

## Documents in this section

1. [VaulticDB Service Architecture](01-vaulticdb-service.md) — binding/build
   decision, process architecture, and engine resolution.
2. [SlateDB Schema](02-schema.md) — the binary key/value schema for blobs,
   inodes, directories, packs, placements, references, and snapshots.
3. [Legacy Interop and Crawl Reconciliation](03-legacy-interop-and-crawl.md) —
   JSON import policy, backup crawl reconciliation, dual-write/export, and
   differential checking.
4. [CLI and Operations](04-cli-and-operations.md) — the index command group,
   locking, and failure recovery.
5. [Storage Placement Policy](05-storage-placement.md) — multi-backend
   placement, durability, pack history, and backend introspection.
6. [Path and Inode History](06-path-inode-history.md) — historical path/inode
   resolution queries.
7. [Analytics Engine](07-analytics-engine.md) — high-dimensional creation
   analytics, growth/churn, and per-user/group attribution.
8. [Quorum Key Broker Implementation and State Machines](08-quorum-key-broker.md) —
   Phase 20 component ownership, broker/session/lease/mutation/recovery states,
   transition invariants, failure handling, and operator reconciliation.
