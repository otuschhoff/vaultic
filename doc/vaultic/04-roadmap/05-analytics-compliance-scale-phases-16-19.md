# Analytics, Compliance, and Scale-Out: Phases 16–19

[← Back to roadmap index](00-overview.md)

### Phase 16: Growth, churn, per-user/group attribution, and GDPR audit CLI

**Goal:** expose growth/churn and GDPR views plus exact, high-dimensional file
creation and live-versus-archive residency analytics across ownership, time,
source topology, custom path groups, and file size. Keep common queries fast
without precomputing every possible aggregate tuple.

**Implementation steps:**

1. Define versioned analytics configuration for source-root/LIF topology mapping,
  default SVM/volume extraction, custom `first-subdir`/depth/prefix path groups,
  history retention, query concurrency, and memory/persistent cache budgets.
2. Add dictionary-coded `af:` columnar creation segments, `am:` segment
  metadata, `ai:` compressed dimension bitmaps, and the mutable `ar:` residency
  overlay. Record UID, GID, exact logical size, decimal magnitude bucket,
  trustworthy birth time or first-seen basis, calendar year, ISO week-year/workweek,
  SVM, volume, custom group, and identity generation.
3. Emit creation facts only for new verified file identities. Update source
  state only after complete deletion proofs and update retained-snapshot
  reachability in snapshot publish/forget transactions. Atomically append
  idempotent `ae:` deltas with authoritative changes, consume them in commit
  order, and advance `aw:` only after a whole analytics commit is durable.
  Preserve `unknown` for incomplete evidence and expose metadata-head lag.
4. Implement a vectorized query engine that prunes segments, intersects bitmap
  predicates, applies exact range checks, joins residency, and computes count
  and logical-byte metrics for arbitrary filters/groupings. Add bounded
  concurrency, progress, cancellation, explain plans, snapshot consistency,
  and checkpointed asynchronous query jobs.
5. Add canonical query hashing, bounded in-memory and persistent result caches,
  scan-cost/frequency telemetry, watermark/epoch invalidation, TinyLFU-style
  admission, asynchronous cleanup, and adaptive promotion/demotion of popular
  expensive query shapes into partial `aq:view:` cuboids.
6. Implement `g:time:*`, `g:path:*`, `u:summary:*`, `g:summary:*`, and
  `u:churn:*` as rebuildable common materialized views. Implement `u:inodes:*`
  and `u:blobs:*` for exact GDPR lookup during reconciliation and snapshot
  purge transactions.
7. Implement `vaultic index analytics` with all ownership, creation-time,
  topology, custom-path, size, and residency predicates; arbitrary grouping;
  async query lifecycle; cache controls; `--explain`; and stable JSON.
8. Implement `vaultic index growth`, `index user-stats`, and
  `index gdpr audit --uid` on the same facts/views, including active/archive-only
  paths, blob hashes, pack locations, and retention expiry dates.
9. Add checkpointed `index analytics rebuild`, cache/view inspection and purge,
  classification-rule migration, and `index check` validation for fact/index,
  residency, dictionary, rollup, and adaptive-view consistency.
10. Benchmark 10M and 100M representative facts and extrapolate to 1.4B before
   general enablement. Measure compressed bytes/fact, write amplification,
   reconciliation overhead, bitmap cardinality, cold/cached latency, cache hit
  ratio, outbox catch-up lag, and rebuild duration. Any failed storage,
  overhead, lag, or query-timeout gate blocks general enablement and requires
  redesign plus a complete benchmark rerun.

**Tests:** creation-time fallback and inode-reuse identity tests; calendar-year/ISO
week boundary and zero/unknown/decimal/exact-size tests; trusted birth-time,
clock-skew fallback, concurrent basis selection, and source-generation tests;
SVM/volume and qtree/custom-rule
classification tests; complete-crawl deletion versus inaccessible/unknown tests;
archive-only transitions across backup, forget, and prune; randomized
multidimensional queries checked against a brute-force fact scan; broad async
query cancellation/resume; outbox crash/replay, whole-commit visibility, lag,
and concurrent snapshot-membership tests; cache key canonicalization, watermark invalidation,
admission/eviction, stale-read labeling, and adaptive-view fallback tests;
growth/user/GDPR rollup and rebuild tests; 10M/100M storage and latency
benchmarks using the reference feasibility profile and documented conservative
1.4B extrapolation.

**Exit criterion:** operators can correctly answer arbitrary conjunctions and
groupings over UID, GID, creation year/ISO workweek, SVM, volume, configured
path group, exact/decimal-magnitude file size, and live/archive residency, including
the UID 600 / 2024 / 1-10 MiB / archive-only example. Common and popular queries
use verified materialized or cached results; broad cold queries are bounded,
observable, cancellable, and resumable rather than falsely promised as instant.
At the 1.4B extrapolation, core analytics storage is at most 175 GB (250 GB
temporary planning ceiling), reconciliation overhead meets the measured 5% CPU
and 10% metadata-write targets, analytics catches up within one normal backup
interval, and the representative broad query meets its configured timeout.
Failure of any gate leaves analytics experimental and off by default. Disabling
or rebuilding it never affects backup correctness or the authoritative index.

The numeric gates above are evaluated exactly by the [analytics feasibility](../02-architecture/07-analytics-engine.md) reference
profile: 1,000/1,000 brute-force comparisons match, the named broad query meets
120 seconds at 100M and projects below 30 minutes at 1.4B, popular cache p95 is
at most two seconds, and catch-up meets the smaller-of-24-hours interval rule.
The real-daemon reconciliation profile runs seven alternating identical
baseline/analytics-enabled workloads through `SchemaStore` transactions. Its
paired median authoritative wall-time overhead is 0.0239% (paired p95 0.1555%)
against the 5% CPU/time target, and same-transaction encoded metadata grows from
316,000 to 336,400 bytes, or 6.4557%, against the 10% authoritative-write target.
Post-commit derived writes are measured separately; physical SlateDB compaction
amplification is not claimed by that logical encoded-byte ratio. All Phase 16
feasibility gates pass on the recorded reference environment.
Rebuild now checkpoints the exact authoritative source key and retains at most
`segment_rows` facts, while catch-up publishes bounded parent-linked delta
generations and compacts at eight layers. Production-path results expose peak
buffer and working-set estimates. The final 100M profile was rerun against the
current production segment codec, bitmap planner, and query path. Builder-memory
evidence is reported separately by the production rebuild/catch-up paths because
the direct large profile intentionally avoids duplicating authoritative input.
The reproducible harness, profile settings, and compact run artifacts are under
[the Phase 16 analytics benchmark](../benchmarks/phase16-analytics/README.md).

**Current implementation state (2026-08-30): complete.** Analytics is optional,
disabled until explicitly enabled, purgeable, and fully rebuildable from the
authoritative index. Rebuild streams authoritative source keys into bounded,
zstd-compressed columnar `af:` segments; dictionary records, segment metadata,
per-value bitmap indexes, residency overlays, common rollups, ownership
summaries, churn views, and GDPR inode/blob mappings are all derived state.
Candidate generations remain invisible until a constant-size transaction
publishes their manifest, completion marker, watermark, and active metadata.
Checkpoint validation prevents a corrupt or stale rebuild cursor from skipping
facts.

Authoritative inode, snapshot-membership, and proven source-state changes append
ordered `ae:` deltas in the same serializable transaction as the source change.
Retained-reference deltas carry explicit increment/decrement semantics while
remaining compatible with the original encoded form. Bounded catch-up creates
parent-linked immutable generations without rescanning the inode namespace,
uses tombstones for replaced derived records, compacts at eight layers, and
reclaims outbox records only after durable watermark publication. Complete,
debt-free crawl proofs are the only evidence that can turn absence into
deletion; incomplete or scoped-out evidence remains `unknown`. Snapshot forget
changes logical retained-reference state independently of physical GC.

The query engine pins a complete manifest/watermark, prunes segments, intersects
zstd or raw bitmap indexes, applies exact range predicates, joins residency,
and supports arbitrary filter/grouping subsets. Missing or malformed optional
indexes fall back to exact scans and appear in explain output. Persistent jobs
checkpoint at segment boundaries and support start, resume, wait, cancellation,
and generation-safe cleanup. Generation-scoped result caches and bounded
adaptive `aq:view:` cuboids use canonical predicates, TTL and admission/eviction
budgets, and exact rollup compatibility checks.

`vaultic index analytics`, `growth`, `user-stats`, and `gdpr audit` expose the
facts and materialized views with stable JSON, explicit stale/current behavior,
cache/view/job inspection, and bounded catch-up controls. `vaultic index check`
exactly validates active manifests, parent chains, segments, dictionaries,
indexes, overlays, rollups, ownership/churn/GDPR views, outbox ordering, cached
results, adaptive cuboids, and pinned jobs.

**Verification performed:** deterministic query tests compare 1,000 arbitrary
queries with an independent brute-force oracle; crash tests cover candidate
writes, checkpoints, pointer publication, outbox replay, and job resume; tests
cover identity reuse, complete/incomplete crawl evidence, snapshot publish and
forget, archive-only/expired transitions, malformed-index fallback, cuboid
promotion/eviction, exact materialized-view equivalence, bounded buffers,
generation compaction, and concurrent rebuild/query/catch-up. Focused package
tests and analytics/index race tests pass. Exact 10M and 100M profiles and the
real-daemon reconciliation profile pass every stated Phase 16 numeric gate. The
recorded write gate measures same-transaction encoded authoritative metadata;
physical SlateDB WAL, block-compression, and compaction amplification remains
explicitly unclaimed because it is storage-engine deployment evidence, not a
different hidden interpretation of the passed 10% gate.

### Phase 17: ISO27001 & GDPR compliance, Azure Key Vault, Syslog, and Storage Verification

**Goal:** provide enterprise compliance features including Azure Key Vault Option A passphrase vaulting, multi-target syslog event routing, advanced GDPR "Right to be forgotten" erasure & chunk survival analysis, UID backup exclusion policies, and attribute/sample-based storage verification across hot and cold tiers.

**Implementation steps:**

1. Implement Azure Key Vault Option A (Secret Store) integration using Azure Arc Managed Identity / `DefaultAzureCredential` to fetch repository key passphrases at startup (`SecretGet`) without modifying restic keyfile formats or requiring mid-job WAN connections.
2. Implement multi-target syslog exporter supporting UDP, TCP, TLS (syslog-over-TLS RFC 5424/3164), and local Unix domain sockets, with event routing filters by category (`auth`, `integrity`, `gdpr`, `restore`, `lifecycle`) and severity.
3. Expand `vaultic index gdpr audit` with `--explain-surviving-chunks` to report per-chunk deduplication survival and external non-scoped file references.
4. Implement `vaultic index gdpr execute-forget --uid <uid>` to purge user file references/inodes, re-evaluate blob reference counts, enqueue unreferenced packs into the deletion queue (`dq:`), and issue a cryptographic deletion certificate.
5. Implement `vaultic index gdpr set-policy --exclude-uid <uid>` writing persistent blocklist rules (`u:policy:blocklist:<uid>`) enforced by archiver/reconciliation during backup crawls.
6. Implement `vaultic index verify-storage` / `vaultic verify-packs` supporting attribute filters (tier, backend, age, retention, size, pack type), sampling controls (`--all`, `--sample-count`, `--sample-percent`), verification levels (header, checksum, full unpack), and automatic cold pack warm-up.

**Tests:** Azure Key Vault SecretGet mock integration test verifying single startup fetch and restic keyfile decryption; syslog multi-target exporter test verifying TLS/UDP socket sending and category filter routing; GDPR audit `--explain-surviving-chunks` test confirming accurate identification of exclusive vs shared chunks and external file reference listings; GDPR `execute-forget` end-to-end test verifying inode reference removal, blob reference count decrements, `dq:` enqueuing, and deletion certificate generation; UID blocklist policy test verifying archiver skips files owned by blocked UIDs during subsequent backup crawls; sampled storage verification test verifying uniform random percentage selection, attribute filtering, level 1/2/3 checks, and automated cold pack warm-up invocation.

**Exit criterion:** enterprise operators can manage keys via Azure Key Vault, route structured audit events to SIEM/syslog targets, execute verifiable GDPR erasure with survival analysis and UID exclusion policies, and run sampled integrity checks across hot and cold storage tiers.

### Phase 18: Crawl optimization with `cwalk` and `pathdiff`

**Goal:** accelerate backup crawls across 1.5+ billion inode storage targets using `cwalk` concurrent directory traversal and `pathdiff` selective change-path crawling with guaranteed event coverage and SVM/volume topology mapping.

**Implementation steps:**

1. Integrate `cwalk` (`github.com/otuschhoff/cwalk`) into the archiver and reconciliation scanner pipeline, replacing sequential traversal with parallel, multi-threaded directory walking with configurable concurrency (`--cwalk-concurrency N`) and queue capacity bounds.
2. Import `pathdiff` (`github.com/otuschhoff/pathdiff`) into `vaultic` (e.g. `internal/pathdiff`), making in-scope enhancements to support volume ID-to-name resolution, target host LIF (Logical Interface) $\rightarrow$ SVM (Storage Virtual Machine) $\rightarrow$ volume mapping, and event sequence continuity checks.
3. Implement 100% event-coverage verification: query `pathdiff` for contiguous change events since the last snapshot timestamp of the source path; verify zero sequence gaps, buffer overflows, or unmonitored windows.
4. Implement selective change-path crawl execution: if 100% event coverage is verified, crawl only the modified subtrees identified by `pathdiff`; if event coverage is incomplete or unverified, fall back automatically to a full `cwalk` traversal.
5. Expose CLI crawl options: `--use-cwalk`, `--cwalk-concurrency N`, `--use-pathdiff`, `--pathdiff-endpoint`, `--pathdiff-require-coverage`, and `--pathdiff-svm-map`.

**Tests:** `cwalk` high-concurrency directory traversal correctness test comparing results against standard traversal; `pathdiff` volume ID resolution and target host LIF $\rightarrow$ SVM $\rightarrow$ volume topology matching test; event coverage gap detection test verifying automatic fallback to full `cwalk` scan when event logs are truncated; selective change-path crawl integration benchmark demonstrating subtree skipping when changes are sparse; imported `pathdiff` module unit tests.

**Exit criterion:** backup crawls achieve linear scaling with `cwalk` concurrency, selective `pathdiff` crawls skip unchanged subtrees when 100% event coverage is verified, and any coverage gap or topology mismatch falls back safely to a full `cwalk` crawl.

### Phase 19: Multi-provider cold storage pool and replicated metadata store

**Goal:** implement multi-backend cold storage pools ($K$-of-$M$ durability quorums across arbitrary active cold providers with read-only legacy backends) and multi-cloud replicated metadata stores for `vaulticdb`.

**Implementation steps:**

1. Implement `ingest` and `read_enabled` flag evaluation in the backend registry (`placement_backends`): mark legacy cold backends (`ingest: false`, `read_enabled: true`) as read-only pools, and route all new pack allocations exclusively to active ingesting backends (`ingest: true`).
2. Implement $K$-of-$M$ multi-provider cold placement scheduling: evaluate the durability predicate (`min_copies`, `min_domains`, `min_offsite`) over the active ingesting backend pool (e.g. 2-of-3 active cold backends), writing parallel placements (`pl:<pack-id>:<backend-id>`) during backup jobs.
3. Implement `ReplicatedObjectStore` wrapper in `vaulticdb` Rust layer for synchronous parallel writes of SlateDB metadata (SSTs, WALs, manifests) across multiple cloud providers (e.g. AWS S3 + Azure Blob / Cloudflare R2) with primary provider read routing, transparent failover, and epoch-based fencing.
4. Implement zero-egress natural drain for legacy backends (`ingest: false`): allow old cold packs to linger on legacy backends until expired by retention policy (`min_retention_until`), purge unreachable packs directly via deletion queue (`dq:`), and route defragmentation repacks into new packs written to the active ingesting pool.
5. Expose CLI backend management commands: `vaultic index backends` status showing `ingest`/`read_enabled` flags per pool, `vaultic index placement` showing $K$-of-$M$ quorum compliance per pack, and `vaultic index placement migrate-pool` options.

**Tests:** multi-provider cold placement test verifying new packs are placed on $K$ of $M$ active backends while legacy backends receive 0 new writes; legacy backend read and warm-up test confirming restore requests route to old backends via `pl:` records; zero-egress natural drain test verifying old packs are deleted from legacy backends when retention expires and repacked blobs write to active ingesting backends; `ReplicatedObjectStore` unit test verifying synchronous multi-cloud write, transient provider outage handling, and primary-to-secondary read failover.

**Exit criterion:** new data packs are durably multi-homed across $K$-of-$M$ active cold providers, legacy cold backends receive zero new writes and naturally drain as retention expires, and `vaulticdb` metadata is synchronously replicated across multi-cloud storage.

