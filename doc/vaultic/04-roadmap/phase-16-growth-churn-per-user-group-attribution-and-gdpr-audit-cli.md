# Phase 16: Growth, churn, per-user/group attribution, and GDPR audit CLI

[← Back to roadmap index](00-overview.md)

[← Phase 15](phase-15-placement-scheduler-offsite-rpo-and-promotion.md) · [Phase 17 →](phase-17-iso27001-gdpr-compliance-azure-key-vault-syslog-and-storage-verification.md)

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
