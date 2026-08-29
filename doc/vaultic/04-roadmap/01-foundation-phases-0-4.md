# Foundation: Phases 0–4

[← Back to roadmap index](00-overview.md)

## 15. Execution-oriented phased implementation plan

Each phase below is an independently executable change set. An implementation
phase must end with its listed tests passing, a documented artifact or API
handoff, and no unexplained changes to legacy behavior. Do not begin the next
phase when an exit criterion is failing. Keep each phase in a separate commit
or small commit series so it can be reviewed or reverted independently.

### Phase 0: Freeze assumptions and native build

**Goal:** make the Rust/SlateDB dependency reproducible without touching
vaultic runtime behavior.

**Current implementation state (2026-08-27):** **complete.** The `vaulticdb`
crate scaffold, versioned protobuf contract, Unix/TCP transport configuration,
private socket permissions, fail-fast protobuf generator, and musl build script
are present on branch `kvdb`. Generated Go bindings are checked in under
`internal/index/proto/vaulticdb/v1`. The host daemon compiles against the pinned
SlateDB revision; its native self-test and Unix-socket RPC smoke test pass. A
local `cargo zigbuild --target x86_64-unknown-linux-musl --release` also
produced `vaulticdb`, verified as a statically linked, stripped x86-64 Linux ELF
artifact (SHA-256 `7eb16913b78fd69702792e3094a24c5ae331fd6b468b8f5a3306f7cd28d4dd88`).

**Implementation steps:**

1. Pin the SlateDB Rust crate commit and record the official
  `slatedb.io/slatedb-go` binding revision used as the API reference.
2. Add the `vaulticdb` Rust crate, protobuf generation inputs, and a reproducible
  musl Linux build script.
3. Build the native SlateDB dependency and statically link it into `vaulticdb`.
4. Add macOS development linking and Linux musl CI jobs; retain the existing
  no-CGO vaultic build.
5. Add a daemon smoke binary that opens `Db`, opens the read-only equivalent,
  writes a `WriteBatch`, scans with `NextBatch`, and shuts down cleanly.

**Files/artifacts:** `vaulticdb/`, `proto/`, build scripts, CI workflow, pinned
dependency metadata.

**Tests:** native build, binding API smoke test, static-link inspection, and a
legacy `go test ./...` run with no daemon installed.

**Exit criterion:** a reproducible `vaulticdb` binary and a legacy vaultic build
that remains usable when `vaulticdb` is absent.

### Phase 1: Protocol contract and daemon lifecycle

**Goal:** establish a secure, versioned process boundary before metadata logic.

**Current implementation state (2026-08-27):** **complete.** The generated,
versioned protobuf contract includes lifecycle, request-context, error,
batch, scan, and transaction envelopes; the service exposes only
`Health`, `Capabilities`, `Drain`, and `Shutdown`, so it cannot mutate
SlateDB. The Go client can attach to and validate a compatible Unix daemon or
start one on demand. It now supplies explicit TCP transport, CIDR allowlist,
and bearer-token configuration when starting an opt-in TCP daemon, uses bounded
connection attempts, records endpoint-specific ownership, and forcibly reaps
owned children on startup or shutdown deadlines. The Rust daemon has a
process-lifetime advisory endpoint lock, allowing a stale socket to be removed
only after exclusive ownership is established; it also creates PID/capability
metadata, private Unix socket directories, bounded gRPC message handling,
per-connection concurrency limits, CIDR admission, and authenticated TCP
lifecycle requests.

Local verification covers protocol compatibility, Unix and TCP attachment,
Unix RPC smoke/shutdown cleanup, native SlateDB smoke, advisory-lock recovery,
repository-scoped endpoints, stale-socket recovery, Unix permission checks,
compiled-daemon startup, and concurrent Unix/TCP startup races under Go's race
detector. Process tests cover missing and incorrect authentication on every
lifecycle RPC, CIDR rejection, truthful transport and work-limit capabilities,
required request IDs, expired deadlines, oversized messages, cancellation
cleanup, bounded shutdown, and the `Drain` ready-to-draining transition. Pure
Rust validators enforce the advertised batch and scan-page limits before those
envelopes are connected to storage RPCs. CI builds through `cargo zigbuild`,
runs the Linux musl native smoke path, retains the no-CGO vaultic build, and
runs the compiled-daemon Go race suite.

**Requirements to complete Phase 1:**

1. Enforce `RequestContext`: reject expired `deadline_unix_ms` values before
  handler work begins, require or generate request IDs for structured
  diagnostics, and honor gRPC cancellation consistently.
2. Define lifecycle state: make `Drain` transition the service from ready to
  draining, reject new non-lifecycle work, expose the state through `Health`,
  and make `Shutdown` drain and terminate within a bounded deadline. Remove
  PID/capability metadata on bind/startup errors as well as normal shutdown and
  signals.
3. Harden endpoint lifecycle: derive default runtime/socket paths from the
  repository identity; validate socket ownership and permissions before
  attaching; acquire the endpoint lock and verify that no compatible daemon is
  serving before removing a stale socket; and prove concurrent `Ensure` calls
  and crash recovery converge on one daemon.
4. Complete TCP policy: test Go-launched TCP daemons; prove that a listener is
  impossible without both a non-empty CIDR allowlist and bearer token; test
  allowlist rejection and missing/wrong-token rejection on every lifecycle
  RPC; and verify `Capabilities` reports the selected transport accurately.
5. Bound runtime work: test the advertised 16 MiB gRPC message limit; validate
  advertised page and batch limits in the request envelopes before storage RPCs
  exist; and add bounded concurrency/backpressure behavior for both Unix and
  TCP transports.

**Required Phase 1 tests:** use the compiled `vaulticdb` process, rather than
only in-process mocks, to cover two racing `Ensure` clients, compatible reuse,
protocol/schema/repository mismatch rejection, stale socket/metadata/lock
recovery, Unix `0700` directory and `0600` socket permissions, startup timeout
and cancellation cleanup, TCP default/allowlist/authentication behavior,
expired deadlines, oversized requests, and `Drain`/`Shutdown` state
transitions. CI must build through `cargo zigbuild --target
x86_64-unknown-linux-musl --release`, matching the verified artifact path, and
run the resulting Linux musl binary in addition to static-link inspection while
retaining the no-CGO legacy vaultic build and core regression tests.

**Implementation steps:**

1. Define versioned protobuf messages for capabilities, health, requests,
  errors, batches, scans, transactions, drain, and shutdown.
2. Generate the Go client and Rust server from the same `.proto` source.
3. Implement Unix-socket serving with private-directory and `0600` checks.
4. Implement endpoint identity, singleton lock, PID/capability metadata,
  connect-before-start, startup race handling, and stale-socket recovery.
5. Add opt-in TCP serving only behind explicit authentication and a non-empty IP
  allowlist; reject insecure configurations at startup.
6. Add request deadlines, cancellation, request IDs, bounded message sizes,
  streaming/page limits, and server backpressure.

**Files/artifacts:** `internal/index/proto/`, `vaulticdb/src/rpc/`, daemon
launcher/client packages.

**Tests:** two clients racing to start one daemon, compatible daemon reuse,
endpoint mismatch rejection, stale socket recovery, Unix permissions, TCP
disabled-by-default, allowlist rejection, authentication failure, cancellation,
and bounded-message tests.

**Exit criterion:** vaultic can reliably attach to or start one compatible
daemon, and no RPC path can mutate SlateDB yet.

### Phase 2: Engine abstraction and legacy adapter

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

### Phase 3: Versioned schema and daemon storage adapter

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

### Phase 4: Best-effort legacy import

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

