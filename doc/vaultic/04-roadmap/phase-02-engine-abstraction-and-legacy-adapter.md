# Phase 2: Engine abstraction and legacy adapter

[← Back to roadmap index](00-overview.md)

[← Phase 1](phase-01-protocol-contract-and-daemon-lifecycle.md) · [Phase 3 →](phase-03-versioned-schema-and-daemon-storage-adapter.md)

**Goal:** introduce one vaultic-owned metadata interface while preserving the
existing JSON engine byte-for-byte.

**Current implementation state (2026-08-27):** **complete.** Vaultic owns
separate lifecycle, read, scan, write/load, and export capability interfaces
under `internal/index`. The legacy adapter delegates normal lookup, size lookup,
iteration, pending-blob registration, pack publication, JSON index load/flush,
pack scans, and export to the repository's existing concurrency-safe
`MasterIndex`; prune and repair retain their specialized direct rewrite APIs.
`Repository.ListBlobs` is the read-only diagnostic operation routed through the
scan adapter.

Normal repository open now resolves the engine after decrypting the config and
before applying repository options. Resolution uses only backend `Stat`, `Load`,
and `List` operations against the dedicated `slatedb` namespace. An absent
namespace selects the legacy adapter without starting `vaulticdb`; partial,
unreadable, malformed, mismatched, non-authoritative, oversized, and unsupported
manifests fail closed. A valid authoritative manifest is recognized as SlateDB
mode and currently returns the explicit unavailable-engine error until the
Phase 3 RPC storage adapter is implemented, rather than silently hiding a
daemon outage behind legacy data.

Verification includes backend/layout compatibility, absent/partial/malformed/
unreadable/unsupported/valid manifest states, repository mismatch, a shared live
`MasterIndex`, concurrent lookups under the race detector, complete blob-record
export, existing repository index behavior, a workspace-wide compile, and a
workspace-wide `CGO_ENABLED=0` compile. Legacy repository initialization retains
its prior directory set byte-for-byte.

**Implementation steps:**

1. Add read, write, scan, and export capability interfaces under
  `internal/index`.
2. Wrap the existing `MasterIndex` and JSON index save/load behavior as the
  legacy implementation.
3. Add manifest detection through the backend abstraction.
4. Define explicit absent, valid, corrupt, and unsupported SlateDB states.
5. Route one read-only diagnostic operation through the adapter.

**Tests:** legacy repository behavior, absent-manifest fallback, malformed and
unsupported manifest errors, concurrent legacy lookups, and existing repository
index tests.

**Exit criterion:** legacy repositories behave unchanged and engine resolution
is deterministic without requiring `vaulticdb`.
