# Legacy Interop and Crawl Reconciliation

[← Back to architecture index](00-overview.md)

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


### 14.9 Crawl Optimization with `cwalk` and `pathdiff`

#### Architecture & Rationale

Crawling datasets containing 1.5+ billion inodes across high-performance enterprise storage (e.g. NetApp NFS volumes) requires maximizing filesystem traversal throughput while minimizing redundant directory `stat()` calls. To achieve maximum crawl performance, `vaultic` incorporates two complementary optimization mechanisms:

1. **High-Concurrency Traversal via `cwalk` (`github.com/otuschhoff/cwalk`):**
   - Replaces or enhances standard directory walking in the archiver with `cwalk` for parallel, multi-threaded directory traversal.
   - Provides configurable and scalable concurrency (`--cwalk-concurrency N`, queue capacity, work-stealing) to fully saturate network storage IOPS without thrashing host memory.
   - Streams discovered file entries directly to the reconciliation scanner pipeline and `vaulticdb` SlateDB batch writers.

2. **Selective Change-Path Acceleration via `pathdiff` (`github.com/otuschhoff/pathdiff`):**
   - Uses storage system change event feeds (`pathdiff`) to selectively identify subdirectories and target paths that have experienced changes since the timestamp of the last successful backup crawl/snapshot.
   - **Guaranteed Event Coverage Requirement:** `pathdiff` acceleration is enabled **only when 100% event coverage** can be strictly verified since the last snapshot timestamp (i.e. contiguous event log sequence numbers with zero buffer overflows, missing event windows, or service restarts). If 100% coverage cannot be guaranteed, `vaultic` automatically falls back safely to a full `cwalk` directory traversal.
   - **Volume & Topology Semantic Matching:**
     - Maps `vaultic` source paths (e.g., `/mnt/nfs_finance/dept_a`) to `pathdiff`'s `volume + subpath` model.
     - Resolves storage volume IDs to canonical volume names.
     - Resolves `vaultic` source paths to storage volumes via target host LIF (Logical Interface) IP/hostname $\rightarrow$ SVM (Storage Virtual Machine) $\rightarrow$ volume mapping.
   - **`pathdiff` Enhancements & In-Tree Import:**
     - Making necessary enhancements to `pathdiff` to support volume ID-to-name resolution, LIF $\rightarrow$ SVM volume matching, and contiguous event coverage verification is explicitly within scope.
     - `pathdiff` may be imported directly into `vaultic` (e.g., under `internal/pathdiff` or as an embedded module) to eliminate RPC latency, streamline volume/SVM matching, and ensure zero-copy event validation.

