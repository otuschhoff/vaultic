# Phase 16 Analytics Feasibility: 10000000 Facts

Date: `2026-08-30T02:11:36Z`
Commit: `4e092f9ff-dirty`
Environment: `Apple M1 Max, 64 GiB`; `go1.27.0 darwin/arm64`, 10 CPUs
Seed: `160019`; segment rows: `262144`; codec: `json-columns-v1;zstd=3`

| Metric | Result |
|---|---:|
| Core bytes/fact | 68.676 |
| Projected core at 1.4B | 96.146 GB |
| Logical write amplification | 1.008914x |
| Build/rebuild | 79.845 s (125242 facts/s) |
| Cold named query | 9.743 s (16079 files) |
| Projected query at 1.4B | 1363.955 s |
| Oracle | 1000/1000 on 100000 facts |
| Cached p95 | 0.000037 s |
| Catch-up | 0.479 s (125295 facts/s) |

## Namespace Bytes

```json
{
  "ad": 30656,
  "af": 60972941,
  "ai": 15751803,
  "am": 4602,
  "ar": 610000000,
  "cache": 1323,
  "other": 0,
  "outbox": 6120000,
  "views": 0
}
```

## Gates

```json
{
  "broad_1.4b_30m": "pass",
  "broad_100m_120s": "pass",
  "cache_p95_2s": "pass",
  "catch_up_24h": "pass",
  "core_175gb": "pass",
  "metadata_write_10pct": "not_measured",
  "oracle_1000": "pass",
  "peak_250gb": "pass",
  "reconciliation_cpu_5pct": "not_measured"
}
```

Authoritative reconciliation CPU and metadata-write baselines are `not_measured` by the direct-segment profile.
