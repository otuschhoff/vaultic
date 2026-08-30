# Phase 16 Analytics Feasibility Benchmark

This directory contains compact results from the reference analytics feasibility
profile. The profile exercises the production `buildSegment`, segment decoder,
bitmap planner, residency overlay join, query cache, and query executor. It
builds segments directly so the 10M and 100M runs do not duplicate the same
facts as authoritative `iv:` records.

The direct 10M/100M profile remains the storage, codec, bitmap, and query
evidence. It is not a rebuild-memory measurement. The separate reconciliation
profile exercises production `SchemaStore` reconciliation and production
`CatchUp`, and now reports maximum buffered deltas plus a conservative peak
working-set estimate. Production rebuild tests likewise assert that the maximum
fact batch is exactly bounded by `segment_rows` and that views remain exact
across batches.

## Reproduce

Run from the repository root:

```sh
commit=$(git describe --always --dirty)
VAULTIC_ANALYTICS_FEASIBILITY=10000000 \
VAULTIC_ANALYTICS_FEASIBILITY_COMMIT="$commit" \
VAULTIC_ANALYTICS_FEASIBILITY_HARDWARE="Apple M1 Max, 64 GiB" \
VAULTIC_ANALYTICS_FEASIBILITY_JSON="$PWD/doc/vaultic/benchmarks/phase16-analytics/reference-10m.json" \
VAULTIC_ANALYTICS_FEASIBILITY_MD="$PWD/doc/vaultic/benchmarks/phase16-analytics/reference-10m.md" \
go test -timeout 30m -count=1 -v ./internal/index/analytics -run '^TestReferenceFeasibilityProfile$'

VAULTIC_ANALYTICS_FEASIBILITY=100000000 \
VAULTIC_ANALYTICS_FEASIBILITY_COMMIT="$commit" \
VAULTIC_ANALYTICS_FEASIBILITY_HARDWARE="Apple M1 Max, 64 GiB" \
VAULTIC_ANALYTICS_FEASIBILITY_JSON="$PWD/doc/vaultic/benchmarks/phase16-analytics/reference-100m.json" \
VAULTIC_ANALYTICS_FEASIBILITY_MD="$PWD/doc/vaultic/benchmarks/phase16-analytics/reference-100m.md" \
go test -timeout 30m -count=1 -v ./internal/index/analytics -run '^TestReferenceFeasibilityProfile$'

VAULTIC_ANALYTICS_RECONCILIATION_FEASIBILITY=200 \
VAULTIC_ANALYTICS_FEASIBILITY_COMMIT="$commit" \
VAULTIC_ANALYTICS_FEASIBILITY_HARDWARE="Apple M1 Max, 64 GiB" \
VAULTIC_ANALYTICS_RECONCILIATION_JSON="$PWD/doc/vaultic/benchmarks/phase16-analytics/reference-reconciliation-200.json" \
VAULTIC_ANALYTICS_RECONCILIATION_MD="$PWD/doc/vaultic/benchmarks/phase16-analytics/reference-reconciliation-200.md" \
go test -timeout 30m -count=1 -v ./internal/index/analytics -run '^TestReferenceReconciliationFeasibilityProfile$'
```

Normal CI validation and the microbenchmark are:

```sh
go test -count=1 ./internal/index/analytics -run '^TestFeasibilityHarnessRegression$'
go test -count=1 ./internal/index/analytics -run '^TestReconciliationFeasibilityRegression$'
go test -run '^$' -bench '^BenchmarkFeasibilityBuildSegment$' -benchtime=1x ./internal/index/analytics
```

## Profile

The generator seed is `160019`; segment size is 262,144 rows. It creates 2,049
UID values including UID 600, 256 GIDs, 64 SVMs, 256 volumes, and 1,024 path
groups. Creation dates span 2020 through 2026. Sizes span zero through 32 MiB;
1% are unknown and 2% are zero. Residency is 90% live, 5% archive-only, and 5%
unknown. Identity continuity is 89% proven, 10% source-generation, and 1%
unknown. The incremental profile contains 0.1% creations and 0.5% residency
changes.

Columns and raw bitmaps are encoded by the production analytics codec using
zstd level 3. The disk-backed harness retains all `af:` payloads and only the
`ai:` payloads needed by the named query, while counting every emitted index
record. This bounds benchmark disk and memory without replacing production
encoding or query behavior.

Storage-engine setting: direct analytics `Store` harness, one file per `af:`
segment, no SlateDB block compression or compaction. Consequently, the report
measures encoded analytics records and cumulative logical writes, not physical SlateDB
LSM amplification. This is conservative for record storage and unresolved for
peak compaction I/O.

The named cold query is UID 600, calendar year 2024, `1 MiB <= size < 10 MiB`,
and archive-only residency. The oracle runs 1,000 deterministic arbitrary
queries against an independent brute-force scan of 100,000 generated facts.

## Accounting

Namespace storage includes key and encoded value bytes for `af`, `am`, `ai`,
`ad`, `ar`, views, outbox, and cache. `ar` is calculated from the actual schema
encoder for every deterministic residency class. Logical write amplification is
every emitted key/value byte, including mutable cache rewrites and incremental
outbox records, divided by logical core bytes; it does not claim SlateDB
compaction amplification.

The combined 1.4B projection uses the larger marginal core-bytes/fact slope from
0-10M and 10M-100M, then reports a +/-20% sensitivity band. Peak space includes
two projected cores for rebuild/compaction plus the 10 GB cache allowance.

The direct-segment profile cannot produce a like-for-like baseline crawl for the
5% reconciliation CPU/time and 10% authoritative metadata-write gates. The
separate reconciliation profile closes that gap through the real
`SchemaStore.PublishReconciledRevision` and vaulticdb transaction path. It runs
identical deterministic first-seen inode workloads with analytics disabled and
enabled, alternates pair order, performs 50 untimed warm-up reconciliations per
repository, and reports median and p95 paired wall-time overhead across seven
samples. Fixture encoding, daemon startup, metadata setup, revision allocation,
validation reads, and catch-up are outside the authoritative timer.

The final 100M artifacts were rerun against the current production segment
encoding, stored namespaces, bitmap construction, and query execution. Builder
working set remains a separate production-path measurement: the direct large
profile intentionally avoids duplicating authoritative input, while bounded-
buffer fields and focused crash/resume tests exercise the streaming rebuild and
incremental catch-up implementations.

The authoritative write target means logical key plus encoded-value bytes
committed by the metadata transaction. The enabled total includes the
transactional `ae:` delta; it excludes post-commit `af:`, `am:`, `ai:`, `ad:`,
`ar:`, `aw:`, manifest, and materialized-view writes. Those derived writes and
their catch-up time are reported separately. This is the exact correctness and
operator-controlled metadata-write boundary in the roadmap. Physical SlateDB
WAL, block compression, and compaction amplification remain explicitly
unmeasured rather than being represented by the logical ratio.
