# Vaultic SlateDB Metadata Service (`vaulticdb`) Roadmap

## 1. Purpose

Integrate SlateDB through a separate Rust `vaulticdb` metadata service. There is
one SlateDB database per vaultic repository. The official Go UniFFI binding is
the reference for the supported API surface, but the daemon should use
SlateDB's native Rust crate directly. Vaultic remains responsible for repository
semantics, legacy Restic JSON compatibility, import/export, crawl policy, and
the CLI. `vaulticdb` is responsible only for SlateDB access and performance
mechanics: caching, batching, write-back, scans, and transactions.

The design target is a repository containing approximately 1.4 billion inodes
and 500+ TB of data on NetApp NFS with pack data and metadata replicated to S3.
The existing JSON index path must remain independent of SlateDB, Rust, and CGO
when `vaulticdb` is absent.

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
- Vaultic processes communicate with `vaulticdb` over protobuf RPC. Unix domain
  sockets are the default transport; TCP is disabled unless explicitly enabled
  and protected by an IP allowlist and authentication policy.
- There is at most one `vaulticdb` instance per configured repository/SlateDB
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
transactionally by `vaulticdb` and must never be derived from wall-clock time,
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
(whose parent is an explicit sentinel such as `0`). Each child entry contains a
stable name, inode number, node type, and a **complete versioned metadata key**
for the child: `dv:<fsid>:<inode>:<revision-seq>` when the child is a directory,
`iv:<fsid>:<inode>:<revision-seq>` otherwise. The revision sequence is part of
the stored reference and is never omitted.

This is a correctness requirement, not a convenience. A child entry that carried
only an inode number would force a historical traversal to consult the `d:` or
`i:` current pointer to find the child record, which resolves an old snapshot
against present-day state. Directory revisions must be walkable using only the
keys they contain, so that a snapshot root reaches exactly the revision graph
that snapshot committed. The child reference must therefore be validated against
both the node type and the child inode when a record is encoded and decoded.

Child entries must be sorted by name in the serialized value to make exports
deterministic. Reconciliation must reject cycles and conflicting parent
ownership.

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

`s:` is keyed by public snapshot ID, so it answers "what is in this snapshot"
but not "which snapshot corresponds to this commit sequence". Maintain a second,
commit-ordered index over the same records:

```text
key:   sc:<8-byte commit-seq>:<32-byte snapshot identifier>
value: schema version, snapshot time, root directory revision
```

The key is fixed width and time-ordered by construction, so enumerating
snapshots in commit order, bounding a scan to a commit range, or finding the
first snapshot at or after a given revision are range scans rather than a full
scan of `s:`. The record is derived: it is written in the same transaction that
publishes the snapshot scope, and it must be rebuildable from `s:` records by
`index check`. It is never the authority for snapshot content.

### Path history index

The inode namespaces answer "what happened to this file object". They cannot
answer "what happened at this path", because a path may be unlinked and
recreated as a different inode, and an inode may move between paths. Maintain an
explicit versioned binding from path to inode:

```text
key:   pv:<4-byte filesystem id><path bytes><0x00><8-byte revision-seq>
value: schema version
       binding state (bound, tombstone)
       node type
       inode (8 bytes)
       metadata revision-seq (8 bytes)
```

This is the only variable-length key in the schema. It is length-delimited by an
explicit `0x00` terminator, which is unambiguous because POSIX path components
cannot contain a NUL byte. The terminator must be present even though the
revision suffix is fixed width, because without it `/a/b` and `/a/bc` would
produce overlapping prefixes.

The resulting order is the useful one. All revisions of one path are contiguous,
and because `0x00` sorts below `/`, a path's own revisions sort before any of its
descendants. A prefix scan of `pv:<fsid>` plus a path yields that path's complete
binding history; a prefix scan of a path plus `/` yields every descendant across
all time.

A record is written only when the binding changes: create, delete, rename, or
replacement of a path by a different inode. Content and metadata changes do not
write here, because they do not change which inode the path resolves to; they
are already recorded as `iv:` revisions reachable from the binding. Entries are
deletable together with the snapshots that reference them, so the namespace is
bounded by the retention window rather than growing without limit.

`pv:` is an accelerator, exactly like the reverse-reference and aggregate
namespaces. It is never the authority for snapshot membership: a binding that
exists at a commit sequence does not prove the path was reachable from a given
snapshot root. Membership is resolved through directory revisions, and
`index check` must be able to rebuild `pv:` from them.

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
- Send metadata mutations to `vaulticdb` through bounded `WriteBatch` RPCs;
  scanner workers must never call SlateDB or CGO directly.
- Let `vaulticdb` coalesce batches and apply write-back asynchronously only when
  the caller has selected an explicit non-durable mode; normal backup commits
  wait for the daemon durability acknowledgement.

## 8. Dual-write and legacy export

When SlateDB is authoritative, primary metadata writes go to SlateDB. The
legacy JSON index remains a compatibility projection.

### Write ordering

1. Upload data and tree packs.
2. Send blob locations to `vaulticdb` as a bounded write batch.
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
Each command resolves or starts the singleton `vaulticdb` only when the
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

### Introspection commands

`vaultic index stats`, `vaultic index packs`, `vaultic index history`,
`vaultic index history prune`, and `vaultic index backends` are specified in
section 12.2 and implemented in Phase 11.

`vaultic index file-history` and `vaultic index path-at` are specified in
section 13.2 and implemented in Phases 13 and 14. Note that `index history`
reports pack lifecycle history, not file history; the two are unrelated.

## 11. Locking and failure recovery

The daemon boundary does not replace vaultic's repository lock policy
automatically. `vaulticdb` is a singleton for one endpoint, not a substitute for
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

## 12. Tiered storage policy, pack history, and backend introspection

### 12.1 Objective

Vaultic already supports hot/cold repositories: the hot backend holds metadata
and tree packs, the cold backend holds data packs, and reads of cold data
require a warm-up step. Prune, however, is tier-blind. Every repack and delete
decision is evaluated with one global policy, so the operator must tune for the
strictest tier. Cold storage is the strict tier: repacking pays egress, request,
and early-deletion charges, and objects usually carry a minimum retention period
(commonly 180 days) during which deletion saves nothing. Tuning for cold
therefore forfeits the one place where defragmentation is genuinely cheap, which
is the hot tier.

The objective of this work is to make tier a first-class property of the pack
catalog so that:

1. Hot and cold packs are governed by two independent policies while the blob
   index keeps exactly one authoritative pack location per blob.
2. Cold repack and delete decisions are driven by an explicit cost and
   retention model rather than by a fixed unused-ratio threshold.
3. Pack lifetime facts are recorded durably instead of being inferred from
   backend object metadata, which does not reliably carry a creation time.
4. Operators can query repository composition and growth over time from the
   CLI without scanning the backend or loading a full index.

Non-goals: changing the on-backend repository format, changing the hot/cold
split rule (tree packs hot, data packs cold), or making SlateDB required for
hot/cold repositories. Legacy JSON repositories keep today's tier-blind prune.

### 12.2 Methodology

#### Invariant: one authoritative location per blob

Tier is a property of a pack, not of a blob. A blob resolves to exactly one
authoritative pack, and that pack's tier determines where the bytes live. Two
policies do not imply two copies, and no dual `hot pack id` / `cold pack id`
tracking is introduced. The existing multi-location capability of the blob
record stays reserved for legacy duplicate semantics and for the optional hot
data cache described below, where the extra location is explicitly non
authoritative.

If a hot data cache is later introduced, the asymmetry is what keeps it
tractable:

- the cold location is authoritative and obeys the cold policy;
- a hot data-pack copy is an evictable cache and may be deleted at any time
  because it is never the last copy;
- reads prefer the hot location and fall back to cold plus warm-up;
- `check --check-hot-cold` asserts that every blob has at least one cold
  location.

#### Tier as a pack catalog attribute

Extend the pack record of section 5 with tier and lifetime fields:

```text
key:   p:<32-byte pack ID>
value: versioned pack metadata
  ... existing fields ...
  tier (hot, cold, mirrored, unknown)
  storage class (backend-reported, free-form, optional)
  created-at and creation-time-known flag
  min-retention-until and retention-source (config, backend, unknown)
  used-payload-bytes
  unused-payload-bytes
  delete-after
```

`tier` is derived at publish time from the pack type and the configured
hot/cold routing, and recorded rather than recomputed, so that a repository that
later stops using `--repo-hot` can still explain where a pack came from.
`min_retention_until` is `created_at + configured tier retention`; it is only
trustworthy when `creation_time_known` is true. Imported legacy packs without a
trustworthy timestamp keep `creation_time_known = false` and are treated as
retention-unknown, which means conservative: never eligible for early deletion
savings claims, always eligible for ordinary reachability-based deletion once
the operator opts in.

`used_payload_bytes` and `unused_payload_bytes` are maintained incrementally as
reachability changes, so prune planning reads a pre-computed number instead of
recomputing usage from a full index sweep.

Aggregate records gain a tier dimension alongside the existing type dimension:

```text
key:   a:tier:hot
key:   a:tier:cold
key:   a:tier:mirrored
```

#### Two policies, one planner

`decidePackAction` becomes tier-parameterised rather than duplicated. The
policy set is resolved per pack from its tier:

| | hot / tree | cold / data |
| --- | --- | --- |
| repack trigger | unused ratio above a low threshold, small-pack merging, free defragmentation | only when the cost model below is satisfied; default off |
| delete trigger | wholly unreachable, delete now | wholly unreachable **and** past `min_retention_until` |
| repack budget | `--max-repack` applied to the hot subtotal | separate `--cold-max-repack`, default `0` |
| target pack size | current defaults | substantially larger, to reduce future fragmentation |

The existing tier-blind flags keep their meaning for legacy repositories and for
repositories without a hot part. `--repack-cacheable-only` remains the coarse
equivalent of "hot policy only, never touch cold", and stays supported.

#### Cost model for cold repacking

A cold pack is repacked only when the projected storage saving over a horizon
exceeds the cost of moving it, including the retention charge that is still
owed on the object being replaced:

```text
saving = unused_payload_bytes * price_per_gb_month * horizon_months
cost   = egress(physical_size) + request_cost
       + physical_size * price_per_gb_month * remaining_retention_months
repack if saving > cost
```

Prices, horizon, and retention are operator inputs (`--cold-price-per-gb-month`,
`--cold-egress-per-gb`, `--cold-request-cost`, `--cold-horizon`,
`--cold-min-retention`), defaulting to values that make the predicate false. The
point of the model is not to guess a cloud bill accurately; it is to degrade
gracefully into "do not repack cold" instead of hard-coding a refusal, and to
make the reason auditable in `--json` output.

#### Retention-aware deferred deletion

Two-phase prune already supports deferring deletion by a fixed duration. Tier
awareness generalises the deadline:

```text
delete_after = max(now + keep_delete, min_retention_until)
```

Packs entering `delete_pending` are additionally indexed by deadline so the
sweep is a range scan rather than a catalog scan:

```text
key:   dq:<8-byte delete-after unix seconds>:<32-byte pack ID>
value: tier, physical size, reason, originating run ID
```

Any later `prune`, `index gc`, or dedicated sweep collects the expired key
prefix. Cold packs therefore linger until their retention expires and are then
collected on a routine run, with no operator arithmetic and no early-deletion
charge. This is the single largest practical benefit of having a metadata
service: a time-ordered deletion queue is not expressible in the JSON index.

#### Preventing cold fragmentation instead of repairing it

Because cold defragmentation is the expensive operation, the cheaper lever is
write-time placement. Cold packing should group blobs whose expected lifetime is
similar (same snapshot cohort, same source path class, same retention class) so
that cold packs tend to become wholly unreachable at once, plus a larger cold
target pack size. In steady state this drives cold repacking toward zero and the
cost model rarely fires at all.

#### Pack history: recording every change, including to deleted packs

The pack catalog only describes the present. Growth rates, churn, and
"what did this repository look like six months ago" require history, and the
history must survive the deletion of the pack it describes.

The decision here is deliberate: **yes, record every pack lifecycle transition,
but as an append-only event log in its own key namespace, not by retaining full
pack records forever.** A pack record is a mutable current-state object with
provenance, sizes, and reference accounting; keeping every historical version of
it would grow with reachability churn rather than with real events, and would
entangle statistics retention with index correctness. An event log is bounded by
the number of transitions, is independent of the catalog's own compaction, and
can be pruned on its own schedule without ever affecting restorability.

```text
key:   ph:<8-byte event unix seconds>:<8-byte event seq>:<32-byte pack ID>
value: schema version, event type, tier, pack type,
       physical size, payload size, used/unused deltas,
       predecessor pack IDs (for repack lineage), run ID, reason code
```

Event types: `created`, `imported`, `published`, `tier_changed`,
`usage_changed` (coalesced, not per blob), `repacked_from`, `repacked_into`,
`delete_pending`, `deleted`, `delete_failed`, `orphan_detected`.

The event key is time-ordered, so histograms and growth queries are range scans
with no catalog access, and events for packs that no longer exist remain
readable. `repacked_from`/`repacked_into` preserve lineage, so churn can be
distinguished from genuine growth: a repack that rewrites 100 GiB is not 100 GiB
of new data, and any growth-rate answer that cannot tell those apart is
misleading.

**Retention and downsampling.** Raw events are pruned, but not by simply
dropping them: they are first rolled up into fixed time buckets that are cheap
enough to keep effectively forever.

```text
key:   pb:<bucket-granularity>:<8-byte bucket start>:<tier>:<pack type>
value: packs created, packs deleted, packs repacked,
       bytes added, bytes deleted, bytes repacked,
       end-of-bucket pack count and physical/payload totals,
       coverage flag (complete, partial, reconstructed)
```

Granularities: hourly, daily, monthly. The rollup is a pure function of the raw
events in the bucket, so it is idempotent and rebuildable while the raw events
still exist. Default retention: raw events kept for a bounded window, hourly
buckets for a longer window, daily buckets for years, monthly buckets
indefinitely. A monthly bucket per tier and pack type is a handful of records
per month; the storage cost is negligible against a 500 TB repository.

Two rules keep this honest:

- A bucket is only marked `complete` if the roll-up ran over a fully retained
  raw range. Buckets covering a period before history collection was enabled,
  or reconstructed from an import, are flagged `partial` or `reconstructed`, and
  every CLI and JSON output surfaces that flag. Statistics must not silently
  present an incomplete series as authoritative.
- History is strictly derived and advisory. `index check` may report history
  gaps or drift against the pack catalog, but a missing or corrupt history
  record is never an error that blocks backup, restore, prune, or GC, and
  history is never an input to a destructive decision. Retention decisions read
  `min_retention_until` from the pack record, not from the event log.

History pruning itself is a normal maintenance operation with a dry-run mode,
and it emits its own coverage marker so a later query can tell that the raw
range was intentionally truncated rather than lost.

#### Backend introspection commands

Add read-only commands to the existing `index` group. They resolve the daemon
when the repository is SlateDB-authoritative; for legacy repositories they
either operate in a reduced mode from the JSON index or fail with an explicit
message, never with a partially populated answer presented as complete.

`vaultic index stats` — constant-time repository composition from the aggregate
records.

- `--by tier|type|state|tier,type` grouping
- `--tier hot|cold|mirrored`, `--type data|tree|mixed|unknown`
- `--verify` recompute from `p:` records and report drift
- `--rebuild` rewrite aggregates from `p:` records
- `--json`

Reports pack count, physical size, payload size, header size, blob count, used
and unused payload bytes, unused ratio, and mixed/unknown counts. Unknown-tier
and unknown-type packs are always reported explicitly rather than folded into a
total.

`vaultic index packs` — query the pack catalog.

- filters: `--tier`, `--type`, `--state`, `--created-before`, `--created-after`,
  `--min-size`, `--max-size`, `--unused-ratio-above`, `--retention-expired`,
  `--retention-unknown`, `--delete-pending`
- `--sort size|created|unused|unused-ratio|delete-after`, `--limit`,
  `--count-only`
- `--json`

`vaultic index history` — time-series and histograms over the event log and
rollups.

- `--metric packs|bytes|created|deleted|repacked|net-growth|unused`
- `--bucket hour|day|week|month`
- `--since`, `--until`
- `--by tier|type`
- `--histogram` render a distribution of pack creation or last-change times
- `--forecast` project growth from the retained series, always annotated with
  the coverage flags of the buckets used
- `--json`

Repack churn is reported separately from net growth by default. `--forecast`
refuses to extrapolate from a series whose buckets are `partial` or
`reconstructed` unless `--allow-incomplete` is given.

`vaultic index history prune` — retention for the event log.

- `--keep-raw`, `--keep-hourly`, `--keep-daily`, `--keep-monthly`
- `--dry-run`, `--json`

`vaultic index backends` — per-tier backend view for hot/cold repositories.

- reports, per backend: configured location, object count and bytes by file
  type, storage class where the backend exposes it, configured minimum
  retention, warm-up configuration in effect
- `--compare` cross-check backend listing against the pack catalog and report
  packs present in the catalog but missing on the backend, and objects present
  on the backend but unknown to the catalog
- `--no-list` answer from the catalog only, without paying for a backend
  listing
- `--json`

`--compare` performs a full backend listing and is explicitly opt-in, because on
a cold tier a listing has a real cost.

Implementation phases for this section are Phases 9 to 12 of the plan below.

## 13. Path and inode history queries

### 13.1 Objective

The schema already retains immutable inode and directory revisions, so a
repository physically contains the answer to "how did this file change over
time". None of that is queryable today. An operator asking the obvious support
question — *when did `/home/x/report.odt` change, and which backup captured each
version* — has no command to run, and no index that makes the question cheap.

Three capabilities are missing, and they are not the same problem:

1. **Path resolution.** Nothing maps a path to an inode. Paths are only
   discoverable by walking directory revisions from a snapshot root, one lookup
   per component.
2. **Snapshot attribution.** `s:` maps a snapshot to a commit sequence, but
   there is no reverse index, so a revision cannot be attributed to the snapshot
   that captured it without scanning every snapshot record.
3. **Path identity over time.** Inode history follows the file object through
   renames but loses the path; path history must additionally capture unlink and
   recreation, where the same path becomes a different inode.

The objective is to answer path and inode history questions from the CLI, in
time proportional to the number of changes reported rather than to the number of
snapshots retained or the size of the repository, without weakening the rule
that snapshot membership is resolved through immutable directory revisions.

Non-goals: changing restore, making history a required input to any destructive
decision, exposing history for legacy JSON repositories beyond what the JSON
index already contains, or replacing `diff` for whole-snapshot comparison.

### 13.2 Methodology

#### Three questions, three costs

The commands must not conflate these, because they give different answers
whenever a path is recreated or a file is renamed:

| question | mechanism | cost |
|---|---|---|
| revisions of an inode | prefix scan `iv:<fsid>:<inode>:` | proportional to revisions |
| revisions at a path | resolve path per snapshot, or scan `pv:` | see below |
| which snapshot captured a revision | commit-order lookup via `sc:` | proportional to results |

Only the first is answerable today, and it is already cheap: `revision-seq` is
fixed-width big-endian, so a single bounded prefix scan returns every revision of
an inode in chronological order.

#### Resolution by walking, without new keys

Path history is answerable from the existing schema. For each snapshot, read its
root directory revision from `s:`, then resolve the path one component at a time
through the child entries of each `dv:` record, following the versioned child
reference. Each snapshot yields either absent or a resolved
`(inode, metadata revision-seq)` pair. Coalescing consecutive identical pairs
across snapshots in commit order produces the change list:

| transition | meaning |
|---|---|
| revision-seq changes, inode unchanged | modified in place |
| inode changes | path rebound to a different file object |
| present to absent | deleted, **or** outside this snapshot's backup set |
| absent to present | created, **or** newly included in the backup set |

The last two rows are ambiguous by nature and must be disambiguated against the
snapshot's `paths` scope before being reported; a path that was never in scope
must never be presented as a deletion.

Because a directory revision is immutable, two snapshots that share a directory
revision along the path share the entire remainder of the walk. Memoizing on the
child revision key collapses most of the work: the root revision differs whenever
anything anywhere in the tree changed, but sharing appears within a level or two
below it, so the practical cost is a small number of lookups per snapshot rather
than full path depth. Lookups across snapshots are independent and must be issued
through `MultiGet` rather than serially.

This approach is exactly correct, requires no new namespace, and is the reference
implementation against which any index is validated. Its floor is
**O(retained snapshots)**, because every snapshot root must be consulted before
it can be excluded. That is acceptable for hundreds of snapshots and not for tens
of thousands.

#### Commit-ordered snapshot index

`sc:` (section 5) removes the scan over `s:` and gives the walk its iteration
order. It is small, bounded by snapshot count, and is worth adding regardless of
whether the path index is adopted, since it also serves any other query that
needs snapshots in commit order.

#### Versioned path index

`pv:` (section 5) makes path history a single prefix scan, bounded by the number
of times that path's binding changed. It is the only structure that answers the
recreation case directly, and the only one whose cost is proportional to the
answer rather than to the repository's snapshot count.

It must remain derived and rebuildable. A path query resolves the candidate
revision range from `pv:`, then confirms membership per snapshot through the
directory walk. Skipping that confirmation would report a binding as present in a
snapshot that never contained the path.

#### Path-keyed, not hash-keyed

A hashed path key (`pv:<fsid>:<path-hash>:<revision-seq>`) is the obvious way to
keep keys fixed width and match the rest of the schema. It is the wrong trade
here, for a reason that is easy to miss: hashing destroys sort locality.

With a hash key, adjacent entries are unrelated paths, so keys gain no prefix
compression, path strings in values do not compress against their neighbours, and
the path must be stored in the value anyway because a hash cannot be reversed for
listing. Subtree queries become impossible. Collisions additionally force a wide
hash: at 1.4 billion paths, a 64-bit hash has roughly a 5% birthday collision
probability, so 128 bits would be the minimum.

Storing the path as the key costs variable-length keys but wins on every other
axis, including size once block prefix compression and zstd are accounted for.
Order-of-magnitude estimates for the 1.4 billion path baseline, assuming a
100-byte average path:

| encoding | raw bytes/entry | baseline |
|---|---|---|
| hash key, full path in value | ~149 | ~210 GB |
| hash key, parent inode plus name | ~73 | ~105 GB |
| hash key, no path stored | ~49 | ~70 GB |
| **path-keyed, length-delimited** | ~134 raw, **~40 effective** | **~56 GB** |

Incremental cost is driven by binding churn, not repository size. At daily
backups and the path-keyed encoding: roughly 20 GB/year at 0.1% daily churn,
100 GB/year at 0.5%, 200 GB/year at 1%.

Against an estimated 350 to 400 GB baseline for the rest of the schema at this
scale, the path-keyed encoding adds roughly 15%, where the naive hashed encoding
would add over 50%. These figures are estimates whose dominant inputs are average
path length and real churn rate; both must be measured on a representative
filesystem before the namespace is enabled by default, and the measurement is an
explicit exit criterion below.

#### Hardlinks

A hardlinked inode has several paths, and the schema already records this: `hr:`
holds the full set of parent and name edges for an inode revision, and
reconciliation deliberately does not invent a single parent. Path history and
inode history therefore diverge legitimately for hardlinks, and both are correct.

Every path of a hardlinked inode receives its own `pv:` binding. Inode history
for such an inode must report all known paths at each revision rather than
choosing one, and path history must not present a shared inode's changes as
exclusive to the queried path.

#### Time semantics

Three different times exist and must never be merged into one column. The
revision sequence is monotonic but explicitly not derived from wall-clock time,
so it orders changes without dating them. Backup time comes from the snapshot
record. Filesystem `mtime` and `ctime` come from the source and, for imported
records, may be unknown or unverified.

Output must therefore report backup time as the time a change was *captured*,
report filesystem times separately, and preserve the existing distinction between
unknown and zero-valued fields. An imported record must never be rendered as
though its metadata were verified.

#### Commands

Add to the existing `index` group. `index history` is already the pack history
command from section 12.2, so file history takes a distinct verb.

`vaultic index file-history <path>` — binding and revision history for a path.

- `--fsid` select the filesystem when the repository spans several
- `--since`, `--until` bound the commit range
- `--follow` continue across renames by tracking the inode when a binding ends
- `--inode` query by `<fsid>:<inode>` instead of a path
- `--snapshots` annotate each change with the snapshots that captured it
- `--content` report content-manifest identity changes, distinguishing metadata
  only changes from content changes
- `--verify` resolve every reported revision through the directory walk and
  report any disagreement with `pv:`
- `--json`

`vaultic index path-at <path> --snapshot <id>` — resolve one path within one
snapshot, reporting the resolved inode, revision, and the directory revision
chain used. This is the primitive the walk exposes, and it is the diagnostic used
when `--verify` reports a disagreement.

Both commands are read-only, use the daemon `DbReader` path, and must fail
explicitly on legacy repositories rather than answering from current state.

Implementation phases for this section are Phases 13 and 14 of the plan below.

## 14. Data growth, churn, per-user/group attribution, and GDPR compliance

### 14.1 Objective

Repository growth and churn are not uniform across time, paths, or users. In an enterprise setting with hundreds of users and massive shared storage (e.g., 500+ TB, 1.4B inodes), operators and compliance officers need answers to specific operational and regulatory questions:

1. **Growth & Churn Time Series:** When is data growing or churning (per week, per month, per year) across the overall repository and within major subdirectories or explicitly tracked paths?
2. **User & Group Attribution:** Which users or groups are driving storage growth or generating the highest churn?
3. **GDPR / Compliance Audit:** Which user-produced or user-owned data (files, revisions, blobs, and cold storage packs) currently exists in the vaultic repository?

The objective is to implement time-bucketed rollups, path-prefix rollups, POSIX UID/GID attribution, and user-data indices in `vaulticdb` with negligible processing and storage overhead, making these insights instantly queryable via the CLI without scanning full backup trees or cold storage packs.

### 14.2 Methodology

#### Invariant & Metadata Source

POSIX `stat()` calls during backup crawls already supply file `uid`, `gid`, `size`, and modification times. Inode records (`iv:<fsid>:<inode>:<revision-seq>`) already persist `uid` and `gid`. By aggregating these fields at backup reconciliation time, vaultic maintains user and path attribution incrementally without additional filesystem I/O.

#### Time-Bucket & Path Churn Rollups

To answer growth and churn questions per week, month, year, or tracked path, `vaulticdb` maintains rollup time-series records:

```text
key:   g:time:<granularity>:<timestamp>:<tier>
value: bytes_added, bytes_deleted, net_change, files_added, files_deleted

key:   g:path:<path_prefix>:<granularity>:<timestamp>
value: bytes_added, bytes_deleted, net_change, files_added, files_deleted
```

Granularities: `week`, `month`, `year`.
`path_prefix` is normalized (e.g. `/home`, `/data/projects`, `/var/log` or paths specified via `--track-path`).
Rollups are updated atomically in the same transaction that commits new inode revisions or reconciles deleted snapshots.

#### Per-User and Per-Group Ownership Aggregates

Maintain real-time current storage state and time-series churn per UID and GID:

```text
key:   u:summary:<uid>
value: active_bytes, active_files, unique_blobs_count, unique_blobs_bytes

key:   g:summary:<gid>
value: active_bytes, active_files, unique_blobs_count, unique_blobs_bytes

key:   u:churn:<uid>:<granularity>:<timestamp>
value: bytes_added, bytes_deleted, files_modified, files_deleted
```

- **Active Bytes:** Total uncompressed size of active files currently owned by `uid` across active snapshots.
- **Unique Blobs Bytes:** Attributed storage footprint accounting for deduplication. Deduplicated blobs can be attributed equally across referencing UIDs or attributed to `first_seen_uid`.
- **User Churn:** Incremental delta recorded per backup run / snapshot purge window.

#### User-to-Data Links for GDPR Auditing

To answer GDPR "Right of Access" and "Right to be Forgotten / Erasure" queries (*"Which data produced by User X is in the vault?"*), SlateDB maintains secondary lookup mappings:

```text
key:   u:inodes:<uid>:<fsid>:<inode>
value: latest_revision_seq, path_sample

key:   u:blobs:<uid>:<blob_hash>
value: ref_count, first_seen_timestamp
```

- **GDPR Inspection:** A query for UID `1042` performs a prefix scan on `u:inodes:1042:` and `u:blobs:1042:`. It instantly yields all active paths, inode revisions, blob hashes, and the cold/hot pack IDs where UID 1042's data resides.
- **GDPR Erasure / Policy Handling:** Shows which snapshots hold UID 1042's data and which packs contain those blobs. If a retention lock prevents immediate cold pack deletion, the compliance report explicitly details when the retention window expires (`min_retention_until`).

### 14.3 Processing & Storage Overhead Analysis

#### Storage Overhead

In a 500+ TB repository with 1.4 billion inodes and 100M blobs:

1. **User/Group Summaries (`u:summary`, `g:summary`):**
   - ~64 bytes per user/group record.
   - For 10,000 users across an enterprise: **~640 KB total** (negligible).
2. **User Monthly Churn Time-Series (`u:churn`):**
   - ~48 bytes per user per month.
   - For 10,000 users over 5 years (60 months): **~2.88 MB total**.
3. **User Blob Index (`u:blobs` for exact GDPR lookup):**
   - ~36 bytes per `(uid, blob_hash)` mapping.
   - For 100 million unique blobs distributed across users: **~3.6 GB total**.
   - Against a 500 TB data repository, 3.6 GB is **~0.0007%** (less than 1 thousandth of one percent) of overall storage footprint.

#### Processing Overhead

1. **Backup Phase:**
   - `lstat()` already reads file `uid` and `gid`.
   - In-memory aggregation during the scanner phase adds an $O(1)$ map lookup per file.
   - Updating `u:summary` and `u:churn` in the daemon's `WriteBatch` adds < 1% CPU overhead to backup reconciliation and zero additional disk/network I/O calls.
2. **Prune / Forget Phase:**
   - When snapshots are purged, the daemon computes user deltas by reading deleted revision trees via fast SlateDB range iterators.

### 14.4 CLI Command Surface

`vaultic index growth` — Query growth and churn over time and subdirectories.

- `--granularity week|month|year`
- `--path <prefix>` filter or group by major subdirectories
- `--since`, `--until`
- `--json`

`vaultic index user-stats` — Query user/group storage usage and churn rankings.

- `--top-storage` rank users/groups by total active stored bytes
- `--top-churn` rank users/groups by churn (`bytes_added + bytes_deleted`) over a time window
- `--since`, `--until` (e.g., `--since 2m` for the last 2 months)
- `--group-by user|group`
- `--limit N` (e.g., `--limit 10`)
- `--json`

`vaultic index gdpr audit` — GDPR compliance inspection tool.

- `--uid <uid>` or `--username <name>`
- `--gid <gid>`
- `--detail` include exact file paths, inode revisions, blob hashes, and pack IDs
- `--json`

Outputs a complete audit report listing active paths, referenced blob IDs, target storage packs (hot vs cold), and retention expiry dates (`min_retention_until`).

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

### Phase 5: Backup crawl reconciliation

**Goal:** let the next backup crawl fill the intentional import gaps.

**Current implementation state (2026-08-27):** **complete.** The reusable
`internal/index/reconcile` pipeline attaches to the archiver at both sides of
its unchanged-file decision: `CanReuse` permits the fast path only for a
complete `FreshnessVerified` inode whose live filesystem identity, parent,
size, mtime, ctime, mode, uid, gid, and ordered content all match, while
`Observe` receives copied post-save nodes containing their final data chunks
or directory subtree. Imported, unknown, malformed, or manifest-incomplete
records therefore force the normal file read and rechunk path.

The scanner defaults to 128 live-stat workers, a bounded 50,000-item input and
result queue, and 5,000-item writer drains. Scanner workers never access the
daemon. A serialized writer publishes deepest-first directory graphs and
inode revisions through the daemon, applies backpressure to archiver workers,
and exposes scanned, reused, changed, deferred, failed, and reconciled
counters. Live identity is derived from `lstat`, including the parent inode;
unavailable and cross-filesystem relationships are not invented and instead
create deterministic pending crawl debt.

Verified inode and directory records preserve all prior immutable revisions.
Directories now carry backward-readable live metadata and freshness in
addition to sorted child maps. Deletes are represented by omission from the
new parent revision, and moves create new parent and path revisions without
rewriting history. Hardlink aliases are grouped deterministically by
filesystem and inode, checked for consistent metadata/content, represented by
one inode revision plus every directory edge, and do not invent a single
parent. Directory parent cycles are rejected before publication.

Each metadata revision is a serializable durable daemon transaction with
bounded mutation batches and conflict retry. It creates or reuses canonical
`cm:` segments, advances `ri:` inode and `rm:` manifest states, updates `rc:`
counters from reads in the same transaction, and resolves matching crawl debt
atomically. Failed work remains pending with retry count, timestamp, and error
class; debt-only success does not allocate a duplicate metadata revision.

Tests cover imported-to-verified reconciliation, verified reuse, metadata and
content changes, deletion, rename/move, hardlinks, parent-cycle rejection,
large-manifest reuse and replacement, missing manifests, unavailable identity,
debt retry/resolution, real archiver final-node delivery, real-daemon durable
publication, and actual 128-worker queue backpressure under the race detector.
`Attach` is the explicit backup integration boundary. Selecting a daemon-backed
authoritative engine, publishing snapshot scope, and exporting legacy JSON
remain Phase 6 responsibilities; daemon attach/start CLI options remain Phase
7. Phase 5 does not weaken the existing fail-closed authority guard.

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

### Phase 7: Import/export/check CLI workflows

**Goal:** expose operator-controlled migration and verification tools.

**Current implementation state (2026-08-28):** **complete.** The new
`vaultic index` group leaves `vaultic list index` unchanged and provides
`import`, `export`, `check`, and `rebuild-pack-stats`. Import is best-effort,
bounded, dry-runnable, and resumable through durable index and snapshot
checkpoints; authority activation remains explicit and occurs last, only after
a complete import. Snapshot traversal loads the source blob index first.

Export writes deterministic canonical Restic JSON indexes, either for every
live catalog pack or only packs whose lifecycle lacks a completed export
checkpoint. Each durable JSON object receives an atomic provenance checkpoint
containing a monotonic export sequence and its exact pack IDs before those
packs transition to `published`; interruption therefore causes safe duplicate
re-export rather than silent omission. `--since` selects packs after a recorded
sequence, while `--verify` reads each object back through the authenticated
repository loader and decodes it. Pack-oriented export still scans `b:` because
the current schema intentionally has no pack-to-blob secondary index.

Differential check compares deduplicated physical blob locations and pack
presence between raw legacy JSON and SlateDB, validates pack type/blob/payload
catalog totals and all five aggregates, checks reverse references and
materialized counters, checks snapshot roots, reports crawl debt/export/GC
state, and validates export object presence, decoding, and pack provenance.
Unresolved imported references and checkpointed snapshots without reconstructible
root inode identity are warnings rather than divergence. Manifest and schema
compatibility are enforced before the workflow by repository-open validation
and the daemon health handshake. Aggregate repair tolerates missing or malformed
records, reports before/after deltas, uses checked rebuild arithmetic, and
replaces all records in one durable batch only when numerical drift exists.

Mutating workflows take an exclusive repository view and check takes the
existing read-lock path. All support JSON summaries and map partial import or
detected drift to exit status 2. Import exposes explicit legacy source
selection, daemon transaction batch sizing, work/error budgets, dry-run,
resume, bounded snapshot traversal, and structured record/warning/checkpoint
counters. Daemon
flags support attach or start, repository-scoped Unix sockets by default,
explicit TCP address/allowlist/authentication, persistent startup, and local or
S3-compatible storage configuration. End-to-end tests cover legacy-only,
partial, resumed, and authoritative repositories; dry-run and corruption
paths; checkpointed export; aggregate repair; local daemon storage; and the
same workflow against S3-compatible metadata storage when the existing CI
endpoint is configured.

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

**Current implementation state (2026-08-28):** **complete for the CLI-facing
GC workflow; scale benchmarking deferred to dedicated infrastructure.**
`vaultic index gc` (SlateDB-authoritative repositories only; legacy repositories
keep using `prune`) discovers candidates from a single scan of `ri:`/`rm:`
reverse references, narrowing the search before the more expensive step: a
full re-walk of every retained snapshot root (the same trusted mechanism
`prune` already uses, valid for any engine because the archiver always writes
the classic snapshot/tree/content blob graph independent of SlateDB's own
schema) that is the actual, final reachability authority. A blob is only
treated as unreachable when both signals agree, and reachability is always
re-verified immediately before any destructive action, never trusted solely
from the earlier scan. Packs found wholly unreachable are deleted; packs
mixing live and unreachable blobs are repacked via the same `CopyBlobs`
primitive prune uses, then their now-empty-of-purpose original pack is
deleted the same way. Deletion uses the `published -> delete_pending ->
deleted` lifecycle: a durable delete-pending transition precedes the physical
backend removal, and only a successful removal purges the pack/blob catalog
records and decrements aggregates in one transaction; a failed removal leaves
the pack visible as delete-pending and is retried automatically on the next
run. `--discover-only` records candidates cheaply without the snapshot walk
or any mutation; `--min-candidate-age` requires a candidate to stay
continuously unreachable for a configurable duration (tracked via the `gc:`
record's discovery timestamp, preserved across runs) before it is swept,
guarding against races with concurrent or clock-skewed writers on top of the
exclusive repository lock GC already holds during its destructive phase.

Physically deleting a pack necessarily makes any legacy JSON index that
referenced it stale, including indexes inherited from a pre-import legacy
repository that were never covered by an export checkpoint. `vaultic index
gc` automatically re-exports every remaining live pack and then deletes every
legacy index object (and any checkpoint tracking it) that still references a
now-gone pack, so `index check` reaches a clean state without a separate
manual step.

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

**Tests:** unit coverage for candidate/pack classification (whole/mixed/skip)
and the min-candidate-age gate; real-daemon tests for two-phase pack deletion
(including a simulated crash between delete-pending and physical removal,
and its automatic retry), mixed-pack repack, and stale legacy index cleanup;
a full CLI end-to-end test covering forget, import, export, discover-only,
the age gate, the real sweep, convergence on a repeated run, a clean `index
check`, snapshot restore correctness, and `check --check-unused` finding
nothing dangling. Discovering and fixing this phase's real bugs (a
pack-publish path that silently recorded every authoritative pack's payload
size as zero, and untracked legacy indexes inherited from import) required a
real repacked pack and a real pre-import legacy index respectively; neither
was exercised by any earlier phase's tests.

**Exit criterion:** documented capacity and recovery targets, clean differential
checks after failure injection, and a repeatable large-repository acceptance
test.

Scale benchmarking (10 million, 100 million, and 1.4 billion inode-equivalent
loads) requires dedicated infrastructure beyond this environment; the tunable
surface it needs (daemon transaction batch size, scan page size, snapshot
work budgets) is already wired through Phase 7 and Phase 8's CLI options.

### Phase 9: Pack tier model and lifetime facts

**Goal:** make tier, creation time, retention deadline, and usage accounting
durable properties of the pack catalog, without changing any policy yet.

**Implementation steps:**

1. Extend the versioned pack record with `tier`, `storage_class`, `created_at`
  plus `creation_time_known`, `min_retention_until` plus `retention_source`,
  `used_payload_bytes`, `unused_payload_bytes`, and `delete_after`. Bump the
  record schema version; older records decode with `tier = unknown` and
  `retention_source = unknown`.
2. Record the tier at pack publish time from the pack type and the configured
  hot/cold routing, rather than deriving it on read.
3. Add `a:tier:*` aggregates and update them atomically with pack records.
4. Maintain `used_payload_bytes`/`unused_payload_bytes` incrementally where
  reachability already changes (forget, GC discovery), and make them
  rebuildable from the blob index.
5. Populate `created_at` for packs written by vaultic; leave imported legacy
  packs `creation_time_known = false`. Do not synthesize a timestamp.
6. Teach `index check` to rebuild tier aggregates and report drift,
  unknown-tier packs, and retention-unknown packs.

**Tests:** schema round trip including forward/backward compatibility with
Phase 3 records; tier assignment for data, tree, and mixed packs in a hot/cold
repository and in a single-backend repository; aggregate atomicity and rebuild;
usage accounting matching a full recomputation after forget and GC; import of a
legacy repository leaving every pack retention-unknown.

**Exit criterion:** a hot/cold repository reports correct per-tier totals, and
`index check` rebuilds them from `p:` records with zero drift.

### Phase 10: Pack history event log and rollups

**Goal:** durable, append-only history of pack lifecycle transitions that
survives deletion of the packs it describes.

**Implementation steps:**

1. Add the `ph:` event namespace and write an event in the same transaction as
  every pack catalog transition: create, import, publish, tier change,
  coalesced usage change, repack lineage, delete-pending, delete, delete
  failure, orphan detection.
2. Record `predecessor_pack_ids` on repack events so churn is distinguishable
  from growth.
3. Coalesce `usage_changed` events per pack per run; never emit one per blob.
4. Add the `pb:` rollup namespace with hourly, daily, and monthly
  granularities, computed idempotently from retained raw events, each bucket
  carrying a `complete`/`partial`/`reconstructed` coverage flag.
5. Implement history retention: roll up, then truncate raw events, writing a
  coverage marker for the truncated range.
6. Guarantee history is advisory: a missing, truncated, or corrupt history
  record must never fail or alter backup, restore, prune, or GC. Add a
  fault-injection test that corrupts history and asserts all data paths still
  succeed.
7. Mark buckets covering periods before history collection was enabled, or
  produced by legacy import, as `reconstructed`.

**Tests:** event ordering and key uniqueness under concurrent writers; rollup
idempotence and equality against a direct scan of raw events; coverage flags
after enabling history on an existing repository and after a retention run;
events for deleted packs still readable; repack lineage reconstructable across
several generations; corrupted-history fault injection leaving every data path
green.

**Exit criterion:** a repository that has been backed up, repacked, and pruned
can report its full pack history, including for packs no longer present, with
correct coverage flags.

### Phase 11: Introspection CLI

**Goal:** answer composition and growth questions from the CLI without a
backend listing or a full index load.

**Implementation steps:**

1. Add `vaultic index stats` with grouping, filtering, `--verify`, `--rebuild`,
  and `--json`.
2. Add `vaultic index packs` with catalog filters, sorting, `--count-only`, and
  `--json`.
3. Add `vaultic index history` with metric, bucket, range, grouping,
  `--histogram`, and `--forecast`. Report repack churn separately from net
  growth. Refuse to forecast from incomplete series unless
  `--allow-incomplete` is passed.
4. Add `vaultic index history prune` with per-granularity retention and
  `--dry-run`.
5. Add `vaultic index backends` with per-tier reporting, opt-in `--compare`
  backend listing, and `--no-list`.
6. Define stable JSON output schemas for all of the above and version them; the
  human-readable output may change, the JSON contract may not without a version
  bump.
7. Ensure every output surfaces unknown tier, unknown type, retention-unknown,
  and incomplete-coverage counts explicitly instead of folding them into
  totals.
8. Legacy repositories: run in a documented reduced mode or fail explicitly.
  Never present a partial answer as complete.

**Tests:** golden JSON output tests for each command; filter and sort coverage;
histogram bucketing across timezone and DST boundaries; `--compare` against a
backend with a deliberately missing object and a deliberately extra object;
`--no-list` performing zero backend requests; forecast refusal on incomplete
series; behavior on a legacy repository.

**Exit criterion:** an operator can obtain pack counts, sizes, per-tier
composition, and a creation/change histogram with growth rate for a repository
whose cold tier is never listed.

### Phase 12: Tier-aware prune and cold cost model

**Goal:** two independent policies driven by the Phase 9 facts, with
retention-aware deferred deletion.

**Implementation steps:**

1. Parameterise `decidePackAction` by a resolved per-tier policy instead of one
  global policy. Preserve today's behavior exactly when the repository has no
  hot part or is legacy.
2. Add hot-tier options (aggressive defaults) and cold-tier options
  (`--cold-max-repack`, defaulting to zero) with separate repack budgets.
3. Implement the cold cost model with `--cold-price-per-gb-month`,
  `--cold-egress-per-gb`, `--cold-request-cost`, `--cold-horizon`, and
  `--cold-min-retention`, defaulting so the predicate is false. Emit the
  decision inputs and outcome in `--json`.
4. Compute `delete_after = max(now + keep_delete, min_retention_until)` and add
  the `dq:` deadline-ordered deletion queue; make prune, `index gc`, and a
  dedicated sweep collect the expired prefix by range scan.
5. Treat retention-unknown packs conservatively: eligible for ordinary
  reachability-based deletion, never credited with early-deletion savings in
  the cost model.
6. Add tier-aware write-time placement: a larger cold target pack size and
  lifetime-based grouping of cold blobs.
7. Warm up cold packs before any cold repack read, reusing the existing warm-up
  path, and abort the cold repack rather than block indefinitely when warm-up
  fails.
8. Documentation: extend the cold storage and forget documentation with the
  per-tier policy table, the cost model, and the retention-aware deletion
  deadline.

**Tests:** unit coverage for tier policy resolution and cost-model boundary
cases including zero, unknown, and expired retention; deletion-queue ordering,
crash between enqueue and sweep, and convergence on a repeated run; an
end-to-end hot/cold test proving that a default-configured prune repacks hot
packs and performs no cold reads at all; a test proving a cold pack past its
retention deadline is collected on a routine run without any extra operator
action; an explicit regression test that a non-hot/cold repository's prune
decisions are byte-for-byte unchanged.

**Exit criterion:** on a hot/cold repository, default prune defragments the hot
tier, issues zero cold-tier reads, and defers cold deletions until retention
expiry, with every decision explainable from `--json` output.

### Phase 13: Historical path resolution and file-history CLI

**Goal:** answer path and inode history questions correctly from the existing
schema, before adding any new per-path storage.

This phase deliberately ships the feature with no growth in the largest
namespaces. It establishes the reference semantics, and it produces the churn
measurement that decides whether Phase 14 is worth its storage cost.

**Implementation steps:**

1. Add the `sc:` commit-ordered snapshot index. Write it in the same transaction
  that publishes snapshot scope, and add an `index check` rebuild that derives it
  from `s:` records and reports drift.
2. Implement a historical path resolver that walks a path from a snapshot root
  through versioned directory child references only. It must never read `i:` or
  `d:` current pointers, and must fail closed if a child reference is missing a
  revision rather than falling back to current state.
3. Memoize the walk on child revision keys across snapshots, and batch
  cross-snapshot lookups through `MultiGet` with the negotiated page limits.
4. Resolve a path across a commit range into a coalesced change list,
  classifying modified, rebound, created, and deleted transitions.
5. Disambiguate absence against the snapshot's `paths` scope so an out-of-scope
  path is reported as not covered, never as deleted.
6. Report hardlinked inodes with their full `hr:` path set rather than a single
  parent.
7. Add `vaultic index file-history` and `vaultic index path-at` with the options
  in section 13.2, a versioned JSON schema, and explicit failure on legacy
  repositories.
8. Separate the three time sources in all output, and preserve the
  unknown-versus-zero distinction for imported records.
9. Instrument the resolver to record binding-change counts and average path
  length during normal backups, so Phase 14 sizing rests on measurements from a
  real filesystem rather than on the estimates in section 13.2.

**Files/artifacts:** path resolver package under `internal/index`, `sc:` key
support in `internal/index/schema`, `index file-history` and `index path-at`
commands, versioned JSON output schemas.

**Tests:** resolution of a path that was renamed, deleted, recreated as a
different inode, and replaced by a directory; a path outside a snapshot's scope
reported as not covered rather than deleted; hardlinked inode reporting every
path; correct resolution of an old snapshot after later revisions changed current
pointers, asserting the resolver never reads a current pointer; memoization
producing results identical to an unmemoized walk; `sc:` rebuild and drift
detection; golden JSON output; explicit failure on a legacy repository.

**Exit criterion:** file history is correct against a repository containing
renames, recreations, and hardlinks, resolved entirely through immutable
revisions, and a measured binding-churn rate and average path length are recorded
for the target filesystem.

### Phase 14: Versioned path index

**Goal:** make path history proportional to the number of changes reported rather
than to the number of retained snapshots.

**Precondition:** Phase 13 complete, and its churn measurement shows the walk's
`O(retained snapshots)` floor is actually a problem for the target repository.
If the measured cost is acceptable, this phase should not be started.

**Implementation steps:**

1. Add the `pv:` namespace with the path-keyed, `0x00`-terminated encoding from
  section 5. Extend key parsing to handle a variable-length key by prefix rather
  than by exact length, which no existing namespace requires.
2. Bound the maximum indexed path length explicitly and record an overflow marker
  rather than truncating a path into a colliding key.
3. Write bindings only on create, delete, rename, and rebinding, in the same
  transaction as the metadata revision that caused the change. Never write on a
  content-only or metadata-only change.
4. Write one binding per path for hardlinked inodes, derived from `hr:`.
5. Serve `index file-history` from `pv:` when present, then confirm membership
  per snapshot through the Phase 13 walk before reporting. Keep the pure walk
  reachable as `--verify` and as the fallback when the namespace is absent or
  incomplete.
6. Add an `index check` rebuild that regenerates `pv:` from directory revisions
  and reports drift, and make a rebuild required after any repair that rewrites
  directory revisions.
7. Prune bindings together with the snapshots that reference them, so the
  namespace is bounded by the retention window.
8. Make the namespace opt-in per repository, with a documented storage estimate
  derived from the Phase 13 measurement, and a command to build it for an
  existing repository incrementally.

**Files/artifacts:** `pv:` key and record support, variable-length key parsing,
binding writer in the reconciliation transaction, rebuild and prune paths.

**Tests:** `pv:` results identical to the Phase 13 walk across the full rename,
recreation, hardlink, and out-of-scope corpus; no binding written for
content-only or metadata-only changes; key ordering placing a path's revisions
before its descendants and grouping subtrees contiguously; paths differing only
by a terminator boundary such as `/a/b` and `/a/bc` not colliding; overflow
marker for over-long paths; rebuild from directory revisions matching incremental
writes; pruning with snapshot forget; incremental build on an existing
repository; measured storage growth compared against the documented estimate.

**Exit criterion:** path history for a repository with tens of thousands of
snapshots is answered in time proportional to the changes reported, `pv:` is
rebuildable from directory revisions with zero drift, and measured storage growth
matches the documented estimate within its stated tolerance.

### Phase 15: Growth, churn, per-user/group attribution, and GDPR audit CLI

**Goal:** expose growth time series, major subdirectory churn, top user/group storage ranking, and GDPR compliance inspection tools via the CLI.

**Implementation steps:**

1. Implement `g:time:*` and `g:path:*` rollup updates in the backup reconciliation transaction.
2. Implement `u:summary:*`, `g:summary:*`, `u:churn:*`, `u:inodes:*`, and `u:blobs:*` updates during backup reconciliation and snapshot purge transactions.
3. Implement `vaultic index growth` with time bucket granularities (`week`, `month`, `year`) and path prefix filters.
4. Implement `vaultic index user-stats` with `--top-storage` and `--top-churn` rankings, `--since` time bounds, and `--limit`.
5. Implement `vaultic index gdpr audit --uid <uid>` returning active paths, inode revisions, referenced blob hashes, and storage pack locations with retention expiry dates.
6. Add `index check` validation for user/group summaries and time-series rollups, with automated rebuild capabilities.

**Tests:** growth rollup accuracy across simulated weekly/monthly backup series; user storage ranking accuracy against raw file sizes; top churner query results over 2-month window; GDPR inspection returning exact paths and cold storage pack retention dates for a given UID; aggregate rebuild after user stats drift.

**Exit criterion:** operators can query top churners, top storage consumers, growth by subdirectory, and complete GDPR user-data location reports instantly from the CLI.

## 16. Testing strategy

### Unit tests

- Big-endian schema round trips and malformed input rejection
- Key namespace and prefix ordering
- Snapshot-versioned inode and directory records remain readable after later
  moves, renames, metadata changes, and deletions
- Inline and manifest-backed content sequences restore byte-for-byte in order
- Large content manifests remain bounded, immutable, and deduplicated
- Current-pointer updates never mutate historical records
- Snapshot manifests resolve the correct root and historical version scope
- Directory child references carry a complete versioned metadata key, and a
  child reference lacking a revision is rejected rather than resolved against
  current pointers
- Historical path resolution walks only immutable revisions, including after
  later changes to current pointers
- Path change classification for rename, delete, recreation as a different
  inode, and replacement of a file by a directory
- Out-of-scope paths reported as not covered rather than deleted
- `pv:` key ordering: a path's revisions precede its descendants, subtrees are
  contiguous, and terminator-boundary paths do not collide
- `sc:` and `pv:` rebuild from authoritative records with zero drift
- Duplicate blob-location preservation
- Pack catalog deduplication across multiple source indexes
- Data/tree/mixed/unknown pack classification and physical/payload size totals
- Tier assignment, per-tier aggregates, and retention-deadline computation
- Pack history event ordering, rollup idempotence, and coverage flagging
- Tier policy resolution and cold cost-model boundary cases
- Deletion-queue deadline ordering and expired-prefix sweeps
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
- File history across a repository containing renames, recreations, hardlinks,
  and snapshots with differing backup scopes
- `pv:`-served history agreeing with the pure directory walk over that corpus
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
- Path-resolution cost as retained snapshot count grows, with and without `pv:`
- `pv:` size growth against measured binding churn and average path length
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

## 17. Operational observability

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
- pack counts and totals by tier, plus unknown-tier and retention-unknown counts
- pack history events written, rollup runs, and history retention truncations
- history coverage state per granularity (complete, partial, reconstructed)
- cold cost-model evaluations, accepted and rejected repack decisions
- deletion-queue depth, oldest and nearest deadline, and expired-prefix sweep
  counts
- warm-up invocations, batches, and wait time attributable to prune or GC
- mixed and unknown pack counts
- aggregate drift and pack-catalog repair counts
- distinct inode, inode-revision, manifest, and retained-snapshot reference
  counts per blob class
- path-binding changes written per backup, and average indexed path length
- path-resolution lookups, memoization hit rate, and snapshots consulted per
  file-history query
- `pv:` and `sc:` record counts, rebuild runs, and detected drift
- logical-to-unique-content deduplication ratio and physical pack amplification
- GC candidate, revalidation, repack, delete-pending, and deletion-failure
  counts
- daemon attach/reuse/start counts and active client count
- RPC latency, queue depth, batch size, write-back delay, and rejected requests
- daemon fencing, restart, and native SlateDB health state
- reader lag and compaction status

Never log inode paths, access tokens, or raw repository keys at normal verbosity.

## 18. Rollout and rollback

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

## 19. Known constraints

- The official Go binding is CGO-based and generated from UniFFI, but it is
  isolated inside the Rust `vaulticdb` build rather than loaded by vaultic.
- The native binding API and generated files must be pinned together.
- Static linking must be verified per target platform and toolchain.
- SlateDB's object-store consistency and fencing behavior must be tested with
  the actual S3/MinIO/NetApp deployment model.
- Legacy JSON indexes cannot by themselves reconstruct all inode and directory
  metadata.
- Backend `FileInfo` does not carry a reliable creation or modification time,
  so pack creation time must be recorded by vaultic at publish time. Packs
  inherited from a legacy repository stay retention-unknown permanently, and no
  timestamp may be invented for them.
- Cold cost-model inputs are operator-supplied estimates, not billing data. The
  model exists to make the decision explicit and auditable, not to predict a
  cloud invoice.
- Pack history is advisory and derived. It must never gate or influence a
  destructive decision, and a history gap must never fail a data path.
- Path and inode history are likewise derived and advisory. `sc:` and `pv:` are
  accelerators that must be rebuildable from snapshot and directory records, and
  snapshot membership is always confirmed through immutable directory revisions
  rather than from a binding record alone.
- `pv:` is the only variable-length key in the schema. Its `0x00` terminator is
  safe only because POSIX path components cannot contain a NUL byte, and key
  parsing must match it by prefix rather than by exact length.
- Path history cost without `pv:` has a floor proportional to the number of
  retained snapshots, because every snapshot root must be consulted before it can
  be excluded. Memoization reduces the constant, not the floor.
- Hardlinked inodes legitimately have several paths. Path history and inode
  history diverge for them by design, and neither view may present a shared
  inode's changes as exclusive to one path.
- Partial import is therefore intentional: JSON import recovers all available
  blob-index facts, while the next backup crawl fills metadata blanks.
- The existing vaultic index model and lock behavior must remain the fallback
  until SlateDB authority has passed interop and recovery acceptance tests.
- `vaulticdb` is an operational dependency only for SlateDB-authoritative
  repositories. Legacy repositories continue to work without Rust, CGO,
  protobuf services, or a running daemon.
- A daemon must not be shared across repositories or object-store endpoints
  unless its capability identity proves that endpoint and schema match exactly.
