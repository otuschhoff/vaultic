# Phase 16 Analytics Feasibility Result

Date: `2026-08-30`
Commit: `4e092f9ff-dirty`
Host: Apple M1 Max, 64 GiB, darwin/arm64, 10 logical CPUs
Go: `go1.27.0`; seed: `160019`; segment rows: `262144`

## Profile Results

| Metric | 10M | 100M |
|---|---:|---:|
| Core storage | 686,760,002 B | 6,880,667,485 B |
| Core bytes/fact | 68.676000 | 68.806675 |
| `af` | 60,972,941 B | 625,476,725 B |
| `am` | 4,602 B | 45,076 B |
| `ai` | 15,751,803 B | 155,115,028 B |
| `ad` | 30,656 B | 30,656 B |
| `ar` | 610,000,000 B | 6,100,000,000 B |
| views | 0 B | 0 B |
| outbox (0.6% changes) | 6,120,000 B | 61,200,000 B |
| cache | 1,323 B | 8,412 B |
| Logical write amplification | 1.008914x | 1.008896x |
| Bitmap index values | 145,587 | 1,426,006 |
| Build/rebuild | 79.845 s | 800.064 s |
| Build throughput | 125,242 facts/s | 124,990 facts/s |
| Cold named query | 9.743 s | 98.923 s |
| Cold named result | 16,079 files | 160,712 files |
| Oracle | 1,000/1,000 | 1,000/1,000 |
| Cached query p95 | 0.00003675 s | 0.000101917 s |
| Catch-up changes | 60,000 | 600,000 |
| Catch-up | 0.479 s | 4.790 s |
| Catch-up throughput | 125,295/s | 125,261/s |

The named query is UID 600, calendar year 2024, `1 MiB <= size < 10 MiB`,
and archive-only residency. The independent oracle scans a deterministic
100,000-fact sample for 1,000 arbitrary queries.

## Projection

The 0-10M core slope is `68.676000200 B/fact`; the 10M-100M marginal slope is
`68.821194256 B/fact`. The larger slope projects `96,349,671,958 B` at 1.4B
facts. The required +/-20% sensitivity band is `77,079,737,566 B` to
`115,619,606,349 B`.

Projected peak space is `202,699,343,916 B`: two projected cores for rebuild or
compaction plus the 10 GB cache allowance. Linear projection of the measured
100M cold query is `1,384.917 s` (`23m04.9s`) at 1.4B.

## Gates

| Gate | Result | Evidence |
|---|---|---:|
| Core <=175 GB | PASS | 96.350 GB projected |
| Peak <=250 GB | PASS | 202.699 GB projected |
| Cold broad query <=120 s at 100M | PASS | 98.923 s |
| Projected cold query <30 min at 1.4B | PASS | 23m04.9s |
| Cached p95 <=2 s | PASS | 0.000102 s |
| Oracle exactness | PASS | 1,000/1,000 |
| Catch-up within default 24 h | PASS | 4.790 s for 0.6M changes |
| Reconciliation CPU/time <=5% | PASS (wall time) | 0.0239% paired median; 0.1555% paired p95 |
| Authoritative metadata writes <=10% | PASS | 6.4557% encoded-byte overhead |

## Reconciliation Profile

The real-daemon profile runs seven alternating baseline/enabled pairs of 200
first-seen inode reconciliations after 50 warm-up inodes. Both sides publish the
same eight unique content IDs per inode through
`SchemaStore.PublishReconciledRevision`; analytics-enabled transactions add the
production encoded `ae:` delta. Median authoritative time was `20.478760 s`
baseline and `20.474569 s` enabled. The median of paired overhead ratios was
`0.023901%`; paired p95 was `0.155509%`.

Baseline authoritative output was 3,600 mutations and 316,000 encoded key/value
bytes (`1,580 B/inode`). Enabled output was 3,800 mutations and 336,400 bytes
(`1,682 B/inode`), a `6.455696%` overhead. Post-commit catch-up was excluded from
both authoritative totals and timers; separately, 200 deltas caught up in
`0.408895 s` and retained 26,876 derived bytes.

**Overall Phase 16 feasibility-gate status: PASS.** All stated numeric gates now
pass. Reconciliation uses authoritative wall time because interval CPU for the
separate vaulticdb process is not exposed without including setup noise. The
write gate is logical authoritative metadata, including same-transaction
`ae:` emission. Physical SlateDB WAL/block/compaction amplification remains a
separate unmeasured limitation and is not represented by the 6.4557% ratio.
