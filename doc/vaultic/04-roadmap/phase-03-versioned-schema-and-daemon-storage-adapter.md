# Phase 3: Versioned schema and daemon storage adapter

[← Back to roadmap index](00-overview.md)

[← Phase 2](phase-02-engine-abstraction-and-legacy-adapter.md) · [Phase 4 →](phase-04-best-effort-legacy-import.md)

**Goal:** implement the durable SlateDB record model and RPC-backed access.

**Current implementation state (2026-08-27):** **complete.** The shared
`vaulticdb.v1` contract now exposes bounded `Get`, `MultiGet`, pageable prefix
`Scan`, atomic `WriteBatch`, and serializable `Begin`/`Commit`/`Rollback` RPCs.
Every request carries the Phase 1 request context and deadline. The daemon
enforces item and encoded-byte limits on requests and responses, rejects new
storage work while draining, maps transaction conflicts to retryable gRPC
`Aborted` errors, and returns durable acknowledgements only after SlateDB's
write handle completes. Response limits include protobuf envelope framing, and
abandoned transactions are reclaimed after a bounded idle interval without
pruning in-flight operations.

The daemon opens one repository-scoped SlateDB handle only after winning the
endpoint singleton lock. Local persistent object storage is the default;
in-memory storage is explicit and test-only, and S3/S3-compatible storage uses
the standard AWS credential/endpoint configuration with required bucket and
repository prefix. Explicit S3 namespace roots are always extended with the
repository identity hash. Database shutdown drains the server and closes
SlateDB.

Vaultic's `internal/index/schema` package implements strict schema-0 binary
keys and values for blob locations (including duplicates), pack catalog and
all aggregate classes, current and immutable inode/directory revisions,
snapshot commit scopes, segmented content manifests, reverse manifest/inode
edges, reference counts, garbage-collection state, crawl debt, and the durable
next-revision counter. Integers are fixed-width big-endian, variable fields are
length-delimited and bounded, unknown enum/boolean/version values and trailing
or truncated bytes are rejected, directory children serialize in deterministic
name order, and content-manifest identity is independent of segmentation.
Inline content is canonical up to 128 IDs; larger sequences use canonically
segmented manifests. Encoded values are bounded below the RPC ceiling, and
key-context validation rejects mismatched record families and cross-filesystem
directory references.

The Go daemon adapter validates advertised limits, preserves missing entries in
ordered multi-get results, pages scans with exclusive cursors, forwards
authentication on every RPC, and provides transaction handles. `SchemaStore`
adds idempotent immutable creates, atomic revision/current-pointer publication,
canonical atomic content-manifest creation, validated atomic mutable batches,
mixed immutable/mutable schema batches, companion reverse-reference updates in
content/revision publication transactions, non-regressing current pointers,
and conflict-retried transactional revision allocation. Failed transaction
cleanup uses a detached bounded context and ambiguous commits remain
rollback-cleanable. Engine resolution
continues to reject an authoritative SlateDB manifest: enabling authority is
intentionally deferred until legacy import and parity phases, as required by
this phase's exit criterion.

Verification covers all key namespaces and record codecs, every truncation and
trailing-data boundary, historical snapshot-root resolution after current-state
mutation, content ordering/segmentation, directory cycles/conflicting parents,
mixed and unknown pack states, aggregate and reverse-count rebuilds, durable
restart behavior, transaction visibility/rollback/commit, concurrent monotonic
revision allocation, immutable revision conflicts, Unix/TCP authentication and
drain behavior, race-enabled client/daemon handles, local storage, and an actual
MinIO S3-compatible durable write/read. Repository/global tests and the complete
affected Go package set also pass with `CGO_ENABLED=0`.

**Implementation steps:**

1. Add fixed-width big-endian encoders/decoders with schema-version and bounds
  validation.
2. Implement `b:`, `p:`, `a:pack:*`, `i:`, `iv:`, `d:`, `dv:`, `s:`, `cm:`,
  `rm:`, `ri:`, `rc:`, `gc:`, and crawl-debt key namespaces.
3. Allocate `meta:next-revision-seq` and commit/revision scope transactionally.
4. Keep current inode/directory pointers separate from immutable revisions.
5. Add inline content IDs and immutable, segmented content manifests.
6. Implement daemon `Db`/`DbReader` lifecycle and vaultic RPC adapter methods:
  `Get`, `MultiGet`, scans, transactions, and bounded `WriteBatch`.
7. Add parent-inode, cycle, conflicting-parent, mixed-pack, and unknown-state
  validation.

**Tests:** schema round trips, malformed input, revision immutability, snapshot
scope resolution, content ordering, manifest segmentation, reverse edges,
aggregate rebuild, local object store, S3-compatible object store, and race
tests for client/daemon handles.

**Exit criterion:** the daemon-backed engine can durably round-trip every schema
record, but remains disabled as the repository authority.
