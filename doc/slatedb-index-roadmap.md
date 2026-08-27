# Vaultic SlateDB Metadata Service (`vaulticd`) Roadmap

## 1. Purpose

Integrate SlateDB through a separate Rust `vaulticd` metadata service. There is
one SlateDB database per vaultic repository. The official Go UniFFI binding is
the reference for the supported API surface, but the daemon should use
SlateDB's native Rust crate directly. Vaultic remains responsible for repository
semantics, legacy Restic JSON compatibility, import/export, crawl policy, and
the CLI. `vaulticd` is responsible only for SlateDB access and performance
mechanics: caching, batching, write-back, scans, and transactions.

The design target is a repository containing approximately 1.4 billion inodes
and 500+ TB of data on NetApp NFS with pack data and metadata replicated to S3.
The existing JSON index path must remain independent of SlateDB, Rust, and CGO
when `vaulticd` is absent.

### Non-negotiable guarantees

- A repository without a SlateDB manifest remains a legacy JSON-index
  repository.
- Existing Restic and Rustic JSON indexes remain readable and writable.
- SlateDB records are never treated as authoritative until their manifest and
  schema version have been validated.
- A failed SlateDB open falls back to legacy mode only when doing so cannot hide
  a detected SlateDB corruption or split-brain condition.
- JSON import is best effort. Recoverable records are imported immediately;
  unresolved inode and directory facts are recorded as crawl debt for the next
  backup crawl.
- Legacy JSON export is deterministic, complete for the blob index, and safe
  for Restic/Rustic readers.
- No operation silently deletes legacy indexes while SlateDB parity is being
  established.
- Vaultic processes communicate with `vaulticd` over protobuf RPC. Unix domain
  sockets are the default transport; TCP is disabled unless explicitly enabled
  and protected by an IP allowlist and authentication policy.
- There is at most one `vaulticd` instance per configured repository/SlateDB
  endpoint.
  Vaultic may start it on demand, then reuse an already-running compatible
  daemon.

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
`-lslatedb_uniffi`. Because `vaulticd` is Rust, production code should link the
native SlateDB crate directly and avoid loading UniFFI into either vaultic or
the daemon. Keep a binding smoke test only where it validates API compatibility.

### Build requirements

- Pin a SlateDB release or commit and record it in the daemon build metadata.
- Build `vaulticd` statically for Linux with the musl target, linking the
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
vaulticd/                            Rust daemon crate
vaulticd/proto/                      generated Rust protobuf code
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
vaulticd (Rust, singleton per repository/SlateDB endpoint)
        |
        | native SlateDB / UniFFI crate
        v
SlateDB object store and local cache
```

### `vaulticd` responsibilities

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

`vaulticd` must not parse Restic JSON indexes, decide import policy, generate
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
vaulticd.sock       Unix socket
vaulticd.pid        advisory process metadata
vaulticd.lock       singleton acquisition lock
vaulticd.cap        protocol/schema/capability record
```

When vaultic needs SlateDB, it first connects and validates an existing daemon.
If no compatible daemon is available, it acquires the endpoint lock and starts
`vaulticd`. A losing starter waits for readiness and connects to the winner.
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
with mode `0600`. TCP is disabled by default. An opt-in TCP listener must bind
only to a configured address, require a non-empty IP allowlist, and use mutual
authentication or an equivalent authenticated channel. Missing authentication
or an allowlist is a startup error.

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

### Engine interface

Create `internal/index/engine.go` with a narrow interface owned by vaultic,
not by SlateDB. The SlateDB implementation is an RPC client to `vaulticd`,
not a direct UniFFI or CGO wrapper in the vaultic process:

```go
type Engine interface {
    Mode() Mode
    Get(ctx context.Context, key []byte) ([]byte, error)
    MultiGet(ctx context.Context, keys [][]byte) ([][]byte, error)
    ScanPrefix(ctx context.Context, prefix []byte, fn func(KeyValue) error) error
    Begin(ctx context.Context, writable bool) (Transaction, error)
    ExportLegacy(ctx context.Context, dst LegacySink) error
    Close() error
}
```

The exact interface should be split into read, write, and export capabilities
so read-only commands cannot acquire a writable SlateDB handle. The legacy
adapter should wrap the existing `MasterIndex` and JSON index operations.

Do not replace the existing index package in one step. First route repository
operations through a legacy adapter, then add the daemon client as an alternate
implementation. A daemon outage must produce a clear unavailable-engine error
when a SlateDB manifest is present.

## 5. SlateDB key and value schema

Use binary keys and values. All integer fields use big-endian encoding. Every
record includes a schema version or is governed by the database schema version
in the manifest.

### Blob index

```text
key:   b:<32-byte blob hash>
value: pack-id (32 bytes)
       offset (8 bytes)
       length (4 bytes)
       uncompressed-size (4 bytes)
       blob-type (1 byte)
       schema-version (1 byte)
```

A blob may have multiple pack locations in legacy JSON indexes. The value
format must therefore support duplicates, for example as a length-prefixed
sequence of fixed-width locations. Do not overwrite an existing location
without preserving duplicate semantics required by Restic/Rustic.

### Versioning and historical state

The inode and directory keys must not represent only one mutable global state.
Paths, parent relationships, metadata, and content references may change
between snapshots, while old snapshots must remain restorable and checkable.
SlateDB's internal MVCC sequence numbers are useful for transaction isolation,
but they are not sufficient as the logical version identifier: compaction and
replication must not change which records belong to a named repository
snapshot.

Use a durable monotonic per-repository revision sequence for changed inode and
directory records. Keep an optional current-state pointer for fast incremental
backup lookups, but retain immutable historical revisions. The public snapshot
ID remains the 32-byte Restic-compatible identity and maps to a snapshot
commit record; it is not repeated in every inode key:

```text
current inode:
key:   i:<4-byte filesystem id>:<8-byte inode>
value: latest revision sequence + metadata-record reference

inode revision:
key:   iv:<4-byte filesystem id>:<8-byte inode>:<8-byte revision-seq>
value: versioned file metadata, including parent inode

current directory:
key:   d:<4-byte filesystem id>:<8-byte directory inode>
value: latest revision sequence + directory-record reference

directory revision:
key:   dv:<4-byte filesystem id>:<8-byte directory inode>:<8-byte revision-seq>
value: versioned directory metadata, including parent inode and child entries
```

Snapshot metadata maps the public ID to the commit/revision scope:

```text
key:   s:<32-byte snapshot ID>
value: commit-seq, root directory revision, metadata
key:   meta:next-revision-seq
value: next uint64 revision sequence
```

The revision sequence is fixed-width big-endian `uint64`. It is allocated
transactionally by `vaulticd` and must never be derived from wall-clock time,
process-local counters, or the number of currently visible snapshots. Keys
must be fixed-width or length-delimited so prefixes cannot be ambiguous.

The current pointer is an optimization, never the historical source of truth.
An incremental crawl may read it to avoid scanning unchanged state, then write
a new immutable revision only when metadata changes. A snapshot record must
reference the root directory revision and exact commit scope used by that
snapshot. Restore and `index check` must resolve through that record rather
than through current pointers.

When a file moves, write a new inode revision with the new `parent_inode`
and path/name relationship; do not mutate the record referenced by the older
snapshot. When a directory changes children, write a new directory revision and
leave the previous child array intact. Deletions are represented by snapshot
membership or tombstone records, not by deleting historical keys.

### Directory structure

```text
key:   dv:<4-byte filesystem id>:<8-byte directory inode>:<8-byte revision-seq>
value: versioned directory record
  parent inode (8 bytes)
  child-entry array
```

Each directory record must contain its `parent_inode`, including the root record
(whose parent is an explicit sentinel such as `0`). Each child entry should
contain a stable name, inode number, node type, and reference to the file
metadata key. Child entries must be sorted by name in the serialized value to
make exports deterministic. Reconciliation must reject cycles and conflicting
parent ownership.

### File metadata

```text
key:   iv:<4-byte filesystem id>:<8-byte inode>:<8-byte revision-seq>
value: versioned record containing
  parent inode (8 bytes)
       mtime (8 bytes)
       ctime (8 bytes)
       size (8 bytes)
       mode, uid, gid (12 bytes)
  content references (inline when small)
  content manifest reference (when large)
  file content hash (derived)
       source path and freshness flags where needed
```

The record must distinguish unknown fields from zero-valued fields. An imported
JSON index must never make an unknown mtime, ctime, inode, or path look freshly
verified.

### Ordered content manifests

The content references in a file revision are an ordered sequence of plaintext
chunk IDs. The sequence is the authoritative representation used to restore a
file:

```text
file bytes = blob[0] || blob[1] || ... || blob[n-1]
```

Keep short sequences inline in the inode revision to avoid an extra lookup. For
large files, spill the ordered sequence into an immutable content manifest:

```text
small file revision:
key:   iv:<fsid>:<inode>:<revision-seq>
value: content_mode = inline
  content_count
  ordered blob IDs (32 bytes each)

large file revision:
key:   iv:<fsid>:<inode>:<revision-seq>
value: content_mode = manifest
  content_count
  content_manifest_id
  file_content_hash

content manifest:
key:   cm:<manifest-id>
value: schema version
  content_count
  ordered blob IDs, optionally in bounded numbered segments
```

The inline threshold is an implementation parameter, not a compatibility
assumption; begin with 128 chunk IDs and tune it using memory and lookup
benchmarks. A manifest ID must be content-addressed or otherwise immutable,
and retries must not create conflicting manifests for the same sequence.

`file_content_hash` is derived from the complete plaintext file or from a
canonical hash of the ordered blob-ID sequence, as specified by the schema
version. It is an optimization for comparison and diagnostics, not a
replacement for the ordered blob IDs. A missing or unknown hash must never be
treated as proof that a live file is unchanged.

When a file changes only in metadata or parent/name relationship, a new inode
revision may reuse the previous content manifest. When file content changes,
write a new ordered sequence and reuse any unchanged blob IDs; only missing
chunk hashes need new blob uploads. Content manifests and their referenced
blob mappings are immutable and must never be modified in place.

### Pack catalog and aggregate statistics

Store one pack record for every known repository pack:

```text
key:   p:<32-byte pack ID>
value: versioned pack metadata
  pack type (data, tree, mixed, unknown)
  physical size
  payload size
  header size
  child blob count
  creation time and time-known flag
  lifecycle state
  source index IDs and provenance
```

`physical_size` is the complete backend object size, including the encrypted
header and format overhead. `payload_size` is the sum of packed blob payloads.
They must remain separate because they answer different operational questions.
`child_blob_count` includes every pack-header entry, including duplicates where
the legacy format contains duplicate locations.

Use explicit lifecycle states such as `imported`, `published`,
`export_pending`, `delete_pending`, `deleted`, `orphaned`, and `unknown`.
Imported packs without trustworthy timestamps must retain
`creation_time_known = false`; an invented timestamp must never drive
`--keep-pack` or destructive retention decisions.

Maintain rebuildable aggregate records for constant-time repository statistics:

```text
key:   a:pack:data
key:   a:pack:tree
key:   a:pack:mixed
key:   a:pack:unknown
key:   a:pack:all
value: pack count, physical size, payload size, header size, blob count,
  last update sequence
```

Aggregate records must be updated atomically with pack catalog records. They
are acceleration structures, not the sole source of truth. `index check` must
be able to rebuild them from `p:` records and report drift, missing records,
and mixed/unknown packs rather than silently excluding them from totals.

### Reverse references and reachability

Keep `b:<blob-hash>` focused on physical pack locations. Do not append a
mutable, unbounded list of logical references to popular blob records; common
chunks would become hot keys and every inode revision update would rewrite the
same blob value.

Use separate, shardable reverse indexes:

```text
blob -> content manifest:
key:   rm:<blob-hash>:<manifest-id>
value: schema version, manifest segment, reference state

blob -> distinct inode:
key:   ri:<blob-hash>:<fsid>:<inode>
value: latest known inode revision, reference state

materialized reference counts:
key:   rc:<blob-hash>
value: total references, distinct inode count, distinct revision count,
       distinct manifest count, reachable snapshot count, update sequence

garbage-collection state:
key:   gc:<blob-hash> or gc:<pack-id>
value: candidate state, observed commit, discovered time, revalidation state
```

`ri:` deliberately omits the revision sequence from its key, so a chunk used
by ten revisions of the same inode counts once for distinct-inode metrics.
`rm:` remains useful for identifying manifest ownership and for avoiding a
full scan of every large file when discovering candidates.

Reverse edges and materialized counters are accelerators, not the final
authority for deletion. A blob with no current-pointer references may still be
reachable from an older retained snapshot. Final reachability is resolved from
retained snapshot roots through immutable directory revisions, inode
revisions, content manifests, and blob hashes.

For pack-level deletion, treat an entire pack as removable only when every
contained blob is unreachable. Otherwise repack live blobs into a replacement
pack and delete the old pack only after index and snapshot revalidation.
Use the lifecycle sequence `published -> delete_pending -> deleted`; failed
backend deletion leaves the record visible for retry.

Reference updates should be atomic with the corresponding inode revision or
content-manifest write. Counts must be rebuildable from `ri:`/`rm:` records,
and `index check` must report stale counters rather than silently trusting
them.

### Snapshot metadata

```text
key:   s:<32-byte snapshot identifier>
value: versioned snapshot metadata
  commit sequence
  root directory revision
```

Store the original snapshot JSON alongside normalized fields or retain a
canonical JSON payload hash. This permits exact export and differential checks
without losing additive vaultic metadata.

The snapshot record must reference the root directory revision and define the
commit sequence used for all historical inode and directory lookups. A current
pointer alone is never sufficient to restore an older snapshot.

### Crawl-debt records

Use a dedicated namespace:

```text
key:   q:<snapshot-id>:<stable-work-id>
value: versioned unresolved-work record
```

A crawl-debt record should include:

- source snapshot ID
- source index ID or pack ID when known
- path or tree ID requiring traversal
- reason (`missing_inode`, `missing_directory`, `unknown_freshness`, etc.)
- retry count and last attempt timestamp
- status (`pending`, `resolved`, `failed`)
- optional error classification

Crawl debt is durable and idempotent. A later backup crawl may resolve it or
mark it permanently unavailable without blocking unrelated imports.

## 6. Legacy JSON import policy

Import must maximize useful information without inventing facts.

### Import stages

1. Enumerate all legacy JSON index files through the existing backend API.
2. Decode each index using the existing Restic-compatible parser.
3. Validate each index record and continue past recoverable bad records while
   collecting structured warnings.
4. Write all valid blob-to-pack locations to the SlateDB blob table.
5. Group records by pack ID, deduplicate pack catalog entries, calculate known
  type/payload/header/blob counts, and obtain physical size through backend
  `Stat` when available.
6. Write pack catalog, reverse references, and aggregate updates atomically
  with blob records.
7. Write pack and index provenance records so exports can preserve source
  ordering and diagnostics.
8. Import snapshot metadata that is available from snapshot files.
9. Traverse snapshot trees only as an optional bounded phase; never require a
   full 1.4-billion-inode traversal to complete import.
10. Create crawl-debt records for every inode, directory, freshness fact, or tree
   relationship that cannot be reconstructed from the JSON index alone.
11. Commit each batch atomically and persist an import checkpoint.

### What can be imported immediately

- Blob ID
- Blob type
- Pack ID
- Blob offset
- Blob length
- Uncompressed length
- JSON index file ID
- Duplicate locations
- Snapshot IDs and metadata when snapshot files are available
- Tree IDs referenced by snapshots
- Import provenance and validation status
- Pack type, payload/header/blob counts, and backend physical size when known
- Ordered file content IDs and content manifests when snapshot trees are
  traversed
- `rm:` manifest edges and deduplicated `ri:` inode edges for imported content
  references

### What must be deferred

Legacy JSON index files do not contain enough information to reconstruct all
filesystem inode records. Defer the following to the next backup crawl or an
explicit repair crawl:

- inode-to-path relationships
- directory child maps
- reliable NFS `ctime` and `mtime` state
- filesystem ID assignment when it is not present in the source metadata
- hardlink relationships not represented in the available tree metadata
- content freshness decisions requiring a live filesystem stat
- damaged or missing tree nodes
- Pack timestamps when the backend cannot provide them
- file-to-blob sequences for trees that were not traversed
- reverse references for content in trees that were not traversed

Imported records must carry `unknown` freshness rather than being considered
safe for incremental-scan skipping.

### Import result

`vaultic index import` should return a structured summary:

```text
indexes_seen
indexes_imported
records_seen
records_imported
records_skipped
snapshots_imported
crawl_debt_created
warnings
errors
checkpoint
```

A nonzero exit status is appropriate for a fatal database or backend failure.
Recoverable malformed records should be reported and counted without discarding
successful records from the same batch.

## 7. Backup crawl reconciliation

The next backup run is the normal completion mechanism for partial import.

### Crawl order

1. Open SlateDB as authoritative and load pending crawl debt.
2. Scan configured backup roots with the NFS-aware scanner.
3. For each path, call `lstat` and compare filesystem ID, inode, parent inode,
   size, mtime, ctime, mode, uid, and gid with the SlateDB file record.
4. Reuse content only when the record is marked verified, all configured
  freshness fields match, and its inline content sequence or content manifest
  is present and valid.
5. Crawl directories whose child map is missing, stale, or marked pending.
6. Write inode and directory records in batches of 5,000 to 10,000.
7. Write or reuse immutable content manifests for large ordered chunk lists.
8. Mark resolved crawl-debt items in the same transaction as the corresponding
   metadata writes.
9. Leave unrelated failed items pending with retry metadata.
10. Write the new snapshot and export legacy JSON index files before completing
   the backup transaction.

Imported-but-unverified metadata must never suppress a file read. The first
post-import backup may do additional work by design; correctness takes
precedence over avoiding that scan.

### Scanner pipeline

- Start with a configurable worker count, defaulting to 128.
- Use a bounded result channel with capacity 50,000.
- Separate filesystem stat workers from SlateDB batch writers.
- Use `sync.Pool` for reusable stat and encoding buffers.
- Bound memory by limiting outstanding path and KV batches.
- Use `MultiGet` for existing inode and blob checks.
- Make each work item idempotent so retries cannot duplicate directory entries.
- Expose counters for scanned, reused, changed, deferred, failed, and
  reconciled records.
- Send metadata mutations to `vaulticd` through bounded `WriteBatch` RPCs;
  scanner workers must never call SlateDB or CGO directly.
- Let `vaulticd` coalesce batches and apply write-back asynchronously only when
  the caller has selected an explicit non-durable mode; normal backup commits
  wait for the daemon durability acknowledgement.

## 8. Dual-write and legacy export

When SlateDB is authoritative, primary metadata writes go to SlateDB. The
legacy JSON index remains a compatibility projection.

### Write ordering

1. Upload data and tree packs.
2. Send blob locations to `vaulticd` as a bounded write batch.
3. Send snapshot/tree/inode changes, including parent inodes, and resolve crawl
  debt through the daemon transaction or write batch.
4. Wait for the daemon's requested durability acknowledgement.
5. Generate the corresponding legacy JSON index projection.
6. Upload the JSON index file and record its export checkpoint.
7. Publish the snapshot only after the required compatibility projection is
   durable.

For each newly published pack, obtain backend `Stat` metadata and commit its
`p:<pack-id>` record, blob locations, aggregate updates, and export state in a
single bounded daemon batch. If `Stat` is unavailable, retain an explicit
`physical_size_known = false` state rather than substituting payload size.

For two-phase prune, transition catalog records from `published` to
`delete_pending`, revalidate that each pack is unreferenced, delete the backend
object, and only then remove its catalog record and decrement aggregates. A
failed deletion remains visible as `delete_pending` for retry.

Use reverse references only for candidate discovery and deduplication
statistics. Before deleting a pack, re-walk retained snapshot roots and prove
that every contained blob is unreachable. A blob with zero current inode
references is not sufficient evidence because an older retained snapshot may
still reach it.

If asynchronous export is introduced later, it must use an explicit pending
export queue and make compatibility lag visible. The default mode should remain
synchronous until recovery and mixed-client behavior are proven.

### Export requirements

- Emit valid Restic JSON index files with the existing format and encoding.
- Include every known blob location, including duplicates.
- Split output according to existing index size and age limits.
- Produce deterministic ordering for reproducible checks.
- Never export inode-only records into the JSON index format.
- Resolve inline content sequences and `cm:` manifests back into the ordered
  `content` blob-ID arrays required by legacy file tree nodes.
- Preserve index provenance and export checkpoints in SlateDB.
- Do not delete old JSON indexes during normal export.

`vaultic index export` should support a full export and a checkpointed export.
A full export scans the blob namespace; a checkpointed export emits only changes
since a recorded export sequence where the legacy format permits it.

Add a maintenance operation such as `vaultic index rebuild-pack-stats` or
`vaultic index check --repair-aggregates`. It must rebuild `a:pack:*` records
from the `p:` catalog, report before/after deltas, and require an explicit
repair option before mutating aggregate records.

## 9. Differential checking

`vaultic index check` compares the authoritative SlateDB view with legacy JSON
indexes without mutating either engine.

Checks should include:

- blob key presence in both engines
- pack ID, offset, length, uncompressed length, and type equality
- duplicate-location equality
- reverse-edge presence and distinct inode/reference counts
- index count and record count
- exported JSON checksum and provenance
- snapshot/tree references associated with imported records
- pending crawl debt count
- pending export count
- pack-catalog count/type/size/blob totals and aggregate consistency
- mixed/unknown pack count
- reachable and unreachable blob/pack candidate counts
- materialized reference-counter consistency with reverse indexes
- schema and manifest version

Report differences by category and support JSON output for automation. A check
must distinguish expected incompleteness (`crawl_debt`) from actual divergence
(`hash_mismatch`, `missing_blob`, `unexpected_blob`, `stale_export`).
It must also classify pack-catalog issues as `missing_pack`, `pack_metadata_mismatch`,
`aggregate_drift`, or `unknown_pack_type`, and reference issues as
`missing_reverse_edge`, `reference_count_drift`, or `unreachable_blob_candidate`.

## 10. CLI command group

Add an `index` command group without changing existing `list index` behavior.
Each command resolves or starts the singleton `vaulticd` only when the
repository has a valid SlateDB manifest. Import, export, and check logic stays
in vaultic; the daemon is used only for storage operations and performance
services.

### `vaultic index import`

Options should include:

- repository and backend options shared with existing commands
- `--from-legacy` or equivalent explicit source selection
- `--batch-size`
- `--max-errors`
- `--resume`
- `--snapshot-depth` for bounded optional tree traversal
- `--dry-run`
- `--json`

The command must be safe to rerun. It should detect already imported index IDs
and skip or verify them rather than duplicating records.

### `vaultic index export`

Options should include:

- `--full`
- `--since`
- `--output-repo` only if a separate destination is supported
- `--verify`
- `--json`

Export must use an exclusive metadata publication boundary when required by the
existing repository lock model.

### `vaultic index check`

Options should include:

- `--legacy-only`
- `--slatedb-only`
- `--include-crawl-debt`
- `--fail-on-warning`
- `--json`

Read-only check mode should use a daemon `DbReader` through the RPC client and
the existing read-lock policy. Vaultic remains the owner of the differential
comparison and exit-code decision.

## 11. Locking and failure recovery

The daemon boundary does not replace vaultic's repository lock policy
automatically. `vaulticd` is a singleton for one endpoint, not a substitute for
cross-tool repository locks.

- Legacy JSON writes continue to use existing vaultic lock behavior.
- SlateDB writers acquire the appropriate repository lock before pack,
  snapshot, and projection publication.
- Read-only commands use `DbReader` with `SkipWalReplay` only when the selected
  reader mode has been proven safe for the desired consistency level.
- Never run a JSON export concurrently with destructive prune until the export
  checkpoint and pack/index revalidation rules are defined.
- A crash between SlateDB commit and JSON export creates a visible pending
  export, not silent data loss.
- A crash during import resumes from the last committed batch checkpoint.
- A crash during crawl reconciliation leaves idempotent pending work.
- A detected manifest epoch change or fencing event aborts the current write
  transaction and requires reopening the engine.
- Daemon startup, attach, and shutdown use the endpoint singleton lock; normal
  RPC handling remains concurrent and does not serialize all vaultic clients
  behind one process-global mutex.
- A client disconnect does not roll back a committed batch. Uncommitted
  requests have bounded deadlines and are explicitly aborted.
- TCP remains disabled unless both configuration and daemon startup policy
  explicitly enable it.

## 12. Execution-oriented phased implementation plan

Each phase below is an independently executable change set. An implementation
phase must end with its listed tests passing, a documented artifact or API
handoff, and no unexplained changes to legacy behavior. Do not begin the next
phase when an exit criterion is failing. Keep each phase in a separate commit
or small commit series so it can be reviewed or reverted independently.

### Phase 0: Freeze assumptions and native build

**Goal:** make the Rust/SlateDB dependency reproducible without touching
vaultic runtime behavior.

**Current implementation state (2026-08-27):** **complete.** The `vaulticd`
crate scaffold, versioned protobuf contract, Unix/TCP transport configuration,
private socket permissions, fail-fast protobuf generator, and musl build script
are present on branch `kvdb`. Generated Go bindings are checked in under
`internal/index/proto/vaulticd/v1`. The host daemon compiles against the pinned
SlateDB revision; its native self-test and Unix-socket RPC smoke test pass. A
local `cargo zigbuild --target x86_64-unknown-linux-musl --release` also
produced `vaulticd`, verified as a statically linked, stripped x86-64 Linux ELF
artifact (SHA-256 `7eb16913b78fd69702792e3094a24c5ae331fd6b468b8f5a3306f7cd28d4dd88`).

**Implementation steps:**

1. Pin the SlateDB Rust crate commit and record the official
  `slatedb.io/slatedb-go` binding revision used as the API reference.
2. Add the `vaulticd` Rust crate, protobuf generation inputs, and a reproducible
  musl Linux build script.
3. Build the native SlateDB dependency and statically link it into `vaulticd`.
4. Add macOS development linking and Linux musl CI jobs; retain the existing
  no-CGO vaultic build.
5. Add a daemon smoke binary that opens `Db`, opens the read-only equivalent,
  writes a `WriteBatch`, scans with `NextBatch`, and shuts down cleanly.

**Files/artifacts:** `vaulticd/`, `proto/`, build scripts, CI workflow, pinned
dependency metadata.

**Tests:** native build, binding API smoke test, static-link inspection, and a
legacy `go test ./...` run with no daemon installed.

**Exit criterion:** a reproducible `vaulticd` binary and a legacy vaultic build
that remains usable when `vaulticd` is absent.

### Phase 1: Protocol contract and daemon lifecycle

**Goal:** establish a secure, versioned process boundary before metadata logic.

**Current implementation state (2026-08-27):** **in progress.** The generated,
versioned protobuf contract includes lifecycle, request-context, error,
batch, scan, and transaction envelopes; the service exposes only
`Health`, `Capabilities`, `Drain`, and `Shutdown`, so it cannot mutate
SlateDB. The Go client can attach to and validate a compatible Unix daemon or
start one on demand. It now supplies explicit TCP transport, CIDR allowlist,
and bearer-token configuration when starting an opt-in TCP daemon. The Rust
daemon has a process-lifetime advisory endpoint lock, allowing a stale socket
to be removed only after exclusive ownership is established; it also creates
PID/capability metadata, private Unix socket directories, bounded gRPC message
handling, CIDR admission, and authenticated TCP lifecycle requests.

Local verification currently covers protocol compatibility, basic Unix client
attachment, Unix RPC smoke/shutdown cleanup, the native SlateDB smoke path,
advisory-lock recovery, repository-scoped endpoints, stale-socket recovery,
Unix permission checks, compiled-daemon startup, and four-client startup races
under Go's race detector. Lifecycle handlers now require a request ID and
reject expired request deadlines. CI builds through `cargo zigbuild` and runs
the resulting musl binary's native smoke path on Linux. Phase 1 is **not
complete**: TCP allowlist/authentication, cancellation, message-limit,
paging/streaming, and backpressure still lack automated integration coverage;
`Drain` remains a no-op because no storage RPC exists yet.

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

**Required Phase 1 tests:** use the compiled `vaulticd` process, rather than
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

**Files/artifacts:** `internal/index/proto/`, `vaulticd/src/rpc/`, daemon
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
is deterministic without requiring `vaulticd`.

### Phase 3: Versioned schema and daemon storage adapter

**Goal:** implement the durable SlateDB record model and RPC-backed access.

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

### Phase 5: Backup crawl reconciliation

**Goal:** let the next backup crawl fill the intentional import gaps.

**Implementation steps:**

1. Start the bounded scanner pool, default 128 workers and a 50,000-item queue.
2. Read current inode pointers and pending crawl debt with `MultiGet`.
3. Compare live NFS `lstat` results, including parent inode, against verified
  revisions only.
4. Re-read and rechunk files when metadata or freshness is unknown/mismatched.
5. Reuse known chunk hashes, write new inode/directory revisions, and preserve
  old revision graphs.
6. Inline small ordered content lists and write/reuse immutable `cm:` manifests
  for large lists.
7. Resolve crawl debt and update `ri:`/`rm:`/`rc:` records in the same daemon
  batch as the metadata revision.

**Tests:** imported partial repository followed by backup, rename/move,
metadata-only change, content change, deletion, hardlink, parent-cycle
rejection, manifest reuse, and 128-worker backpressure tests.

**Exit criterion:** a post-import backup produces a complete verified revision
graph and never skips a file solely because imported data exists.

### Phase 6: Authoritative dual-write and legacy projection

**Goal:** make SlateDB authoritative only after the crawl path is correct while
keeping Restic/Rustic compatibility.

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

### Phase 7: Import/export/check CLI workflows

**Goal:** expose operator-controlled migration and verification tools.

**Implementation steps:**

1. Add the `vaultic index` command group without changing `list index`.
2. Implement resumable best-effort `index import`.
3. Implement full and checkpointed `index export`.
4. Implement differential `index check`, pack aggregate rebuild, and JSON
  summaries with automation-friendly exit codes.
5. Add daemon attach/start options while keeping Unix sockets as the default and
  TCP opt-in only.

**Tests:** all commands on legacy, partial, and SlateDB-authoritative repos;
  dry-run and resume; corruption and warning exit codes; aggregate repair;
  local and S3-compatible backends.

**Exit criterion:** operators can import, inspect, export, repair aggregates,
and compare engines without manual database intervention.

### Phase 8: Prune, GC, and operational hardening

**Goal:** safely use pack catalogs and reverse references for performance without
weakening snapshot reachability guarantees.

**Implementation steps:**

1. Discover GC candidates from `ri:`, `rm:`, `rc:`, and pack catalog records.
2. Re-walk retained snapshot roots before every destructive deletion.
3. Delete wholly unreachable packs; repack packs containing a mixture of live
  and unreachable blobs.
4. Use `published -> delete_pending -> deleted` transitions and retry failed
  deletions.
5. Add crash/fencing/eventual-consistency and mixed-client tests.
6. Benchmark 10 million, 100 million, and 1.4 billion inode-equivalent loads;
  tune cache, SST blocks, batch sizes, and scanner workers.

**Exit criterion:** documented capacity and recovery targets, clean differential
checks after failure injection, and a repeatable large-repository acceptance
test.

## 13. Testing strategy

### Unit tests

- Big-endian schema round trips and malformed input rejection
- Key namespace and prefix ordering
- Snapshot-versioned inode and directory records remain readable after later
  moves, renames, metadata changes, and deletions
- Inline and manifest-backed content sequences restore byte-for-byte in order
- Large content manifests remain bounded, immutable, and deduplicated
- Current-pointer updates never mutate historical records
- Snapshot manifests resolve the correct root and historical version scope
- Duplicate blob-location preservation
- Pack catalog deduplication across multiple source indexes
- Data/tree/mixed/unknown pack classification and physical/payload size totals
- Aggregate rebuild and drift detection after import, backup, export, and prune
- Reverse-edge creation, deduplicated inode counts, reachability candidates,
  and counter rebuilds
- Import checkpoint idempotence
- Crawl-debt creation and resolution
- Export determinism
- Differential-check classifications

### Integration tests

- Legacy JSON repository with no SlateDB namespace
- Empty SlateDB repository
- Partial import followed by backup crawl
- Corrupt JSON record mixed with valid records
- Missing tree and missing snapshot metadata
- Crash after SlateDB commit and before JSON export
- Crash during import batch
- Local object store and S3-compatible object store
- Read-only `DbReader` operations while a writer is active
- Daemon startup races, singleton reuse, stale socket recovery, and protocol
  mismatch rejection
- Unix-socket permissions and TCP-disabled-by-default behavior
- TCP allowlist and authentication rejection/acceptance cases
- Restic/Rustic reading vaultic-exported JSON indexes

### Scale tests

- 128 scanner workers against a synthetic NFS-like filesystem
- 50,000-item result queue saturation
- 5,000 and 10,000 item SlateDB batches
- MultiGet latency and allocation profile
- Memory use while exporting a repository with billions of records
- Content-manifest lookup, segmentation, deduplication, and restore memory at
  small, medium, and very large file sizes
- Reverse-reference index size, hot-key behavior, and reachability-scan cost
- Compaction and reader lag under sustained backup writes

### Safety tests

Run with `-race` where applicable and test:

- concurrent import and read-only check
- concurrent backup and export
- fencing and reopen behavior
- duplicate retry of a committed batch
- stale manifest and schema mismatch
- legacy fallback when SlateDB is absent
- hard failure when SlateDB is present but corrupt

## 14. Operational observability

Expose metrics and structured events for:

- engine mode and manifest version
- SlateDB open/read/write/scan failures
- WAL and SST flush latency
- batch size and batch commit latency
- MultiGet hit/miss rate
- scanner queue depth
- NFS stat latency and errors
- imported, reconciled, deferred, and failed records
- crawl-debt backlog and age
- pending JSON exports
- differential-check discrepancies
- pack counts and physical/payload/header/blob totals by type
- mixed and unknown pack counts
- aggregate drift and pack-catalog repair counts
- distinct inode, inode-revision, manifest, and retained-snapshot reference
  counts per blob class
- logical-to-unique-content deduplication ratio and physical pack amplification
- GC candidate, revalidation, repack, delete-pending, and deletion-failure
  counts
- daemon attach/reuse/start counts and active client count
- RPC latency, queue depth, batch size, write-back delay, and rejected requests
- daemon fencing, restart, and native SlateDB health state
- reader lag and compaction status

Never log inode paths, access tokens, or raw repository keys at normal verbosity.

## 15. Rollout and rollback

1. Ship the legacy adapter and detection code disabled by default.
2. Enable schema and import commands for operators without making SlateDB
   authoritative.
3. Run import plus `index check` and inspect crawl debt.
4. Run one backup crawl in legacy-authoritative mode while populating SlateDB.
5. Compare exported JSON with the original JSON index set.
6. Enable SlateDB authority for a test repository with synchronous JSON export.
7. Expand by repository size and backend type only after recovery tests pass.
8. Keep a rollback command that stops SlateDB writes, preserves the SlateDB
   namespace, and resumes from legacy JSON indexes.
9. Do not remove legacy JSON indexes until a separately approved migration
   policy exists.

## 16. Known constraints

- The official Go binding is CGO-based and generated from UniFFI, but it is
  isolated inside the Rust `vaulticd` build rather than loaded by vaultic.
- The native binding API and generated files must be pinned together.
- Static linking must be verified per target platform and toolchain.
- SlateDB's object-store consistency and fencing behavior must be tested with
  the actual S3/MinIO/NetApp deployment model.
- Legacy JSON indexes cannot by themselves reconstruct all inode and directory
  metadata.
- Partial import is therefore intentional: JSON import recovers all available
  blob-index facts, while the next backup crawl fills metadata blanks.
- The existing vaultic index model and lock behavior must remain the fallback
  until SlateDB authority has passed interop and recovery acceptance tests.
- `vaulticd` is an operational dependency only for SlateDB-authoritative
  repositories. Legacy repositories continue to work without Rust, CGO,
  protobuf services, or a running daemon.
- A daemon must not be shared across repositories or object-store endpoints
  unless its capability identity proves that endpoint and schema match exactly.
