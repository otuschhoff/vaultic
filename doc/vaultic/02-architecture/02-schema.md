# SlateDB Schema

[← Back to architecture index](00-overview.md)

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

### Pack placement

Where a pack's bytes physically live is a set that changes over the pack's
lifetime, not a single attribute of the pack. A pack may be live on local
storage while still being uploaded offsite and already evicted from a third
backend. Placement is therefore recorded per `(pack, backend)` with its own
lifecycle:

```text
key:   pl:<32-byte pack ID>:<8-byte backend ID hash>
value: schema version
       state (pending, live, evicting, evicted, failed)
       storage class (backend-reported, free-form, optional)
       placed-at and placement-time-known flag
       bytes
       min-retention-until and retention-source (config, backend, unknown)
       delete-after
       last-verified-at
```

Eviction planning, capacity accounting, and reconciliation against a backend
listing all need the opposite direction, so it is a separate range-scannable
namespace rather than a scan of every pack:

```text
key:   bp:<8-byte backend ID hash>:<32-byte pack ID>
value: state, bytes, placed-at
```

Both records are written in the same transaction as the transition that
produced them. `bp:` is derived and must be rebuildable from `pl:` by
`index check`.

Minimum retention, retention source, and the deletion deadline belong here
rather than on the pack record, because they differ per backend: a local copy
usually has no minimum retention while an archival copy has one measured in
months. A single pack-level field cannot express both, and collapsing them
would either invent a retention obligation for local storage or discard a real
one for archival storage. The pack record keeps `created_at`, usage accounting,
and a denormalised summary of the current placement set for fast filtering; the
summary is always rebuildable from `pl:`.

The blob index is unchanged. A blob still resolves to exactly one pack, and the
existing multi-location capability of the blob record remains reserved for
legacy duplicate semantics. Two copies of one pack on two backends are one
logical location with two placements, never two blob locations. [Storage placement policy](05-storage-placement.md)
defines the durability predicate that governs when a placement may be removed.

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

