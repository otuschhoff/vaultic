# VaulticDB Service Architecture

[← Back to architecture index](00-overview.md)

## 2. Binding and build decision

Use the official module published by the SlateDB project:

```text
slatedb.io/slatedb-go
```

The generated Go binding, useful for API validation and compatibility tests, is
imported as:

```go
import slatedb "slatedb.io/slatedb-go/uniffi"
```

The binding provides `Db`, `DbReader`, `DbSnapshot`, `WriteBatch`, range and
prefix scans, transactions, reader modes, cache configuration, and
`ObjectStoreResolve`.

The official Rust UniFFI crate declares both library forms:

```toml
[lib]
crate-type = ["cdylib", "staticlib"]
```

The binding's checked-in CGO declaration currently links with
`-lslatedb_uniffi`. Because `vaulticdb` is Rust, production code should link the
native SlateDB crate directly and avoid loading UniFFI into either vaultic or
the daemon. Keep a binding smoke test only where it validates API compatibility.

### Build requirements

- Pin a SlateDB release or commit and record it in the daemon build metadata.
- Build `vaulticdb` statically for Linux with the musl target, linking the
  native SlateDB crate and Rust dependencies into the daemon binary.
- The main vaultic CLI uses protobuf RPC and can remain `CGO_ENABLED=0`.
- Keep a legacy-only vaultic binary path available when no daemon artifact is
  installed.
- Add explicit build metadata containing the SlateDB binding version, Rust
  commit, target triple, and binding checksum.
- Fail the SlateDB build clearly when the native artifact is absent; do not
  replace the engine with an in-memory implementation.
- Add macOS and Linux link tests. Windows support should remain legacy-only
  until a supported static SlateDB artifact exists.

### Suggested build layout

```text
third_party/slatedb/<version>/<target>/libslatedb.a
vaulticdb/                            Rust daemon crate
vaulticdb/proto/                      generated Rust protobuf code
internal/index/proto/                generated Go protobuf client
```

Generated UniFFI files should be updated only by the pinned regeneration
workflow. Hand-written code must not edit generated files.

## 3. Process architecture

The process boundary is intentional. Vaultic must not load the Rust UniFFI
library into every backup, restore, or diagnostic process. This centralizes
SlateDB handles, caches, and background workers in one service.

```text
vaultic CLI processes
        |
        | protobuf RPC over Unix socket by default
        | optional authenticated TCP + IP allowlist
        v
vaulticdb (Rust, singleton per repository/SlateDB endpoint)
        |
        | native SlateDB / UniFFI crate
        v
SlateDB object store and local cache
```

### `vaulticdb` responsibilities

- Own `Db`, `DbReader`, and `DbSnapshot` handles.
- Resolve local, S3, and S3-compatible SlateDB object stores.
- Maintain local NVMe/block-cache configuration.
- Batch, coalesce, and write back requests received from vaultic.
- Apply durability policy and return explicit commit acknowledgements.
- Serve point reads, `MultiGet`, range scans, prefix scans, and batched iterator
  reads.
- Serialize lifecycle operations such as shutdown, drain, fencing, and
  manifest refresh.
- Expose health, capabilities, schema version, and daemon instance identity.

`vaulticdb` must not parse Restic JSON indexes, decide import policy, generate
legacy JSON indexes, crawl NFS, or decide whether a file is safe to reuse.

### Vaultic responsibilities

- Detect the SlateDB manifest and resolve legacy versus daemon-backed mode.
- Own the `IndexEngine` compatibility interface.
- Parse legacy JSON indexes and import as much valid data as possible.
- Generate deterministic legacy JSON exports.
- Maintain import/export checkpoints and crawl-debt records.
- Run the NFS scanner and reconcile inode freshness.
- Decide transaction boundaries for backup, restore, prune, and repair.
- Validate daemon capabilities and reject incompatible protocol/schema versions.

### Singleton lifecycle

Use a per-endpoint runtime directory containing:

```text
vaulticdb.sock       Unix socket
vaulticdb.pid        advisory process metadata
vaulticdb.lock       singleton acquisition lock
vaulticdb.cap        protocol/schema/capability record
```

When vaultic needs SlateDB, it first connects and validates an existing daemon.
If no compatible daemon is available, it acquires the endpoint lock and starts
`vaulticdb`. A losing starter waits for readiness and connects to the winner.
Vaultic may shut down a daemon only when it started that daemon and no active
clients remain, unless persistent service mode was requested.

Socket and lock paths must use a canonical repository/SlateDB endpoint identity,
not only a repository basename. Stale PID files are diagnostic hints, never
proof that a process is alive.

### Protocol and transport

Define a versioned `.proto` contract under `internal/index/proto`, generating
the Go client and Rust server from the same source. RPC groups should include:

- `Health`, `Capabilities`, `Drain`, and `Shutdown`
- `Get`, `MultiGet`, `Scan`, and `ScanPrefix`
- `WriteBatch` with explicit durability acknowledgement
- `Begin`, transaction mutations, `Commit`, and `Rollback`
- bounded/pageable `ExportScan`

Unix sockets are the default transport and must use a private runtime directory
with mode `0700` and a socket with mode `0600`. TCP is disabled by default. An
opt-in TCP listener must bind only to a configured address, require a non-empty
IP allowlist, and use mutual authentication or an equivalent authenticated
channel. Missing authentication or an allowlist is a startup error.

Every RPC needs request IDs, deadlines, cancellation, bounded message sizes,
and backpressure. Large scans and batches must stream or page rather than use
unbounded protobuf messages.

## 4. Engine resolution

Introduce a repository-level engine resolver before loading the legacy
`MasterIndex`.

```text
open repository
    |
    +-- detect /slatedb/manifest/
            |
            +-- absent --> LegacyJSON engine
            |
            +-- present --> validate manifest and schema
                               |
                               +-- valid --> SlateDB authoritative engine
                               |
                               +-- invalid --> hard error with repair guidance
```

### Manifest detection

The resolver must use the backend abstraction rather than local filesystem
calls. Detection should distinguish:

- no SlateDB namespace: legacy mode
- valid SlateDB manifest: SlateDB mode
- partial namespace or unreadable manifest: damaged SlateDB state
- manifest with unsupported schema version: unsupported engine state

A repository configuration field may cache the selected mode, but the manifest
check remains authoritative so copied repositories and external tooling are
handled correctly.

The implemented Phase 2 marker is the backend object `slatedb/manifest`. It is
bounded to 64 KiB and decoded as strict JSON with these required fields:

```json
{
  "format_version": 1,
  "schema_version": "0",
  "repository_id": "<repository config ID>",
  "authoritative": true
}
```

The dedicated backend file type maps to the `slatedb` namespace but is omitted
from repository initialization paths, so creating or opening a legacy
repository does not create an empty directory or otherwise change its layout.
The namespace is created on demand when a manifest is saved. Unknown fields,
trailing JSON, oversized or unreadable payloads, repository-ID mismatches, and
non-authoritative markers are corrupt states. Unsupported format or schema
versions are reported separately and never fall back to the legacy engine.

### Engine interface

Phase 2 implements `internal/index/engine.go` with interfaces owned by vaultic,
not by SlateDB. Lifecycle and capability interfaces are separate so callers can
request only the operations they need:

```go
type Engine interface {
    Mode() Mode
    Close() error
}

type ReadEngine interface {
  Engine
  Lookup(vaultic.BlobHandle) []*pack.PackedBlob
  LookupSize(vaultic.BlobHandle) (uint, bool)
}

type ScanEngine interface {
  Engine
  Values() iter.Seq[*pack.PackedBlob]
  ListPacks(context.Context, vaultic.IDSet) <-chan legacyindex.PackBlobs
}

type WriteEngine interface {
  Engine
  AddPending(vaultic.BlobHandle, uint) bool
  StorePack(context.Context, vaultic.ID, pack.Blobs,
    vaultic.SaverUnpacked[vaultic.FileType]) error
  Load(context.Context, vaultic.ListerLoaderUnpacked, vaultic.Counter,
    func(vaultic.ID, *legacyindex.Index, error) error) error
  Flush(context.Context, vaultic.SaverUnpacked[vaultic.FileType]) error
}

type ExportEngine interface {
  Engine
  ExportLegacy(context.Context, LegacySink) error
}
```

`LegacyIndexEngine` composes these capabilities and delegates to the existing
`MasterIndex`, retaining its locks, data types, JSON encoding, duplicate
handling, and incremental-load behavior. Phase 3 will add daemon RPC-oriented
point-read, multi-get, prefix-scan, batch, and transaction capabilities without
putting SlateDB or CGO into the vaultic process.

Do not replace the existing index package in one step. First route repository
operations through a legacy adapter, then add the daemon client as an alternate
implementation. A daemon outage must produce a clear unavailable-engine error
when a SlateDB manifest is present.

