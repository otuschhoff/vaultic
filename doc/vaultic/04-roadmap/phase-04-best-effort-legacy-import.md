# Phase 4: Best-effort legacy import

[← Back to roadmap index](00-overview.md)

[← Phase 3](phase-03-versioned-schema-and-daemon-storage-adapter.md) · [Phase 5 →](phase-05-backup-crawl-reconciliation.md)

**Goal:** recover maximum useful information from legacy JSON indexes without
inventing inode or freshness facts.

**Current implementation state (2026-08-27):** **complete.** The reusable
`internal/index/legacyimport` library parses encrypted legacy indexes through
the existing Restic-compatible parser and continues after per-index decoding
errors. It groups entries by pack, canonicalizes duplicate physical locations
without dropping alternate locations, preserves sorted source-index
provenance, classifies data/tree/mixed/unknown packs, and obtains physical pack
sizes through the backend abstraction. Pack records now carry an explicit,
backward-readable `physical_size_known` state so failed `Stat` calls cannot be
confused with a zero-sized object.

Each pack import runs in a serializable daemon transaction that merges blob
locations, pack provenance, catalog metadata, all aggregate classes, and
pack-stat crawl debt atomically. Concurrent imports of different legacy
indexes that reference the same pack retry transaction conflicts and derive
payload/count totals from the union of physical locations. Successful later
`Stat` calls resolve prior unavailable-pack debt. Mutation batches respect both
negotiated item and encoded-message limits.

Optional snapshot traversal is enabled only with a depth or work bound. It
uses the existing snapshot and streaming tree readers, writes revisions only
when legacy `(device_id, inode)` identity and parent identity are available,
keeps omitted zero-valued metadata unknown, marks all imported metadata as
`FreshnessImported`, and records deterministic pending debt for unknown
identity, parent, freshness, missing trees, depth truncation, and malformed
snapshot/tree data. Ordered content is retained inline or in canonical
segments with reverse-manifest and reverse-inode records. The snapshot root
itself remains explicit debt because legacy tree JSON has no root inode/device
identity; no synthetic identity is invented.

Durable per-index and per-snapshot checkpoints make retries resumable and
idempotent. Dry-run executes parsing, classification, bounds, and summaries
without mutation. Work and error limits return a distinct limit result, while
the structured result records imported/resumed indexes and snapshots, packs,
blob locations, traversed/imported nodes, debt, and stage-specific findings.
Tests cover malformed-index continuation, real encrypted legacy repository
import, duplicate and concurrent same-pack references, resume and dry-run,
pack `Stat` failure/recovery, mixed and unknown packs, aggregate consistency,
depth/work bounds, conservative metadata, large content manifests, and
canonical reverse-reference segments under the race detector. SlateDB remains
non-authoritative, and the operator-facing `index import` command remains a
Phase 7 concern.

**Implementation steps:**

1. Parse legacy indexes through the existing Restic-compatible parser.
2. Group valid entries by pack ID and import every valid blob location,
  including duplicates and provenance.
3. Create pack catalog records and aggregate updates atomically with blob data.
4. Traverse snapshot trees only within a bounded `--snapshot-depth` or work
  budget; write inode revisions, directory revisions, content manifests, and
  reverse references for traversed data.
5. Create durable crawl-debt records for missing trees, inode relationships,
  parent inodes, freshness, content sequences, and unavailable pack metadata.
6. Add checkpoints, resume, dry-run, max-error handling, and structured result
  summaries.

**Tests:** duplicate index references, malformed record continuation,
idempotent resume, partial tree traversal, pack `Stat` failures, mixed/unknown
  pack classification, aggregate consistency, and import of a real legacy
repository.

**Exit criterion:** every valid recoverable blob mapping is imported; unresolved
inode/directory/content facts are explicit crawl debt and never appear verified.
