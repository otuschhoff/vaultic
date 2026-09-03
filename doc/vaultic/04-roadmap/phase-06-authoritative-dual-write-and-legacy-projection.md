# Phase 6: Authoritative dual-write and legacy projection

[← Back to roadmap index](00-overview.md)

[← Phase 5](phase-05-backup-crawl-reconciliation.md) · [Phase 7 →](phase-07-import-export-check-cli-workflows.md)

**Goal:** make SlateDB authoritative only after the crawl path is correct while
keeping Restic/Rustic compatibility.

**Current implementation state (2026-08-27):** **complete.** Authority is
explicitly gated and requires a validated repository-scoped manifest; once
selected, daemon unavailability fails closed. Pack catalog entries, blob
locations, duplicate locations, aggregate deltas, reconciled revisions,
reverse references, hardlink references, and synthetic snapshot roots are
published through bounded daemon operations. Snapshot scope and its commit
sequence are committed atomically only after the referenced metadata and
legacy pack indexes are durable.

The archiver remains the compatibility projection boundary: it serializes the
canonical Restic tree, including each ordered file `content` array, before the
same array is encoded by reconciliation as inline IDs or deterministic `cm:`
segments. This avoids reconstructing legacy trees from a lossy metadata view
while guaranteeing that manifest-backed authoritative files have identical
expanded legacy content. Pack lifecycle records expose pending JSON index
exports and startup reconstructs pending packs as flushable legacy indexes.
Snapshot checkpoints durably distinguish pending, complete, and failed backend
writes, retain the exact immutable root key, and recover publishable pending
scopes after restart. Prune and repair fail closed in authoritative mode;
`PlanPrune` also enforces this at the repository API boundary.

**Implementation steps:**

1. Add an explicit feature gate and repository capability for SlateDB authority.
2. Write packs, blob locations, pack catalog, revisions, reverse references,
  and aggregates through bounded daemon batches.
3. Allocate commit/revision sequences transactionally and publish snapshot scope
  only after all referenced metadata is durable.
4. Expand inline content or `cm:` manifests into deterministic legacy tree
  `content` arrays.
5. Export JSON indexes synchronously, persist export checkpoints, and recover
  pending exports after restart.
6. Keep prune/repair legacy-safe until their SlateDB-aware revalidation paths
  are separately implemented.

**Tests:** backup/restore through vaultic, Restic, and Rustic; crash between
SlateDB commit and JSON export; export retry; duplicate chunks; old-snapshot
restore after later revisions; and mixed-client read compatibility.

**Exit criterion:** a SlateDB-authoritative backup restores correctly through
all supported clients and export lag is visible rather than silent.
