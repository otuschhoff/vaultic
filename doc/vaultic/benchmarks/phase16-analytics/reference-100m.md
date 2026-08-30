# Phase 16 Analytics Feasibility: 100000000 Facts

Date: `2026-08-30T02:28:30Z`
Commit: `4e092f9ff-dirty`
Environment: `Apple M1 Max, 64 GiB`; `go1.27.0 darwin/arm64`, 10 CPUs
Seed: `160019`; segment rows: `262144`; codec: `json-columns-v1;zstd=3`

| Metric | Result |
|---|---:|
| Core bytes/fact | 68.807 |
| Projected core at 1.4B | 96.329 GB |
| Logical write amplification | 1.008896x |
| Build/rebuild | 800.064 s (124990 facts/s) |
| Cold named query | 98.923 s (160712 files) |
| Projected query at 1.4B | 1384.917 s |
| Oracle | 1000/1000 on 100000 facts |
| Cached p95 | 0.000102 s |
| Catch-up | 4.790 s (125261 facts/s) |

## Namespace Bytes

```json
{
  "ad": 30656,
  "af": 625476725,
  "ai": 155115028,
  "am": 45076,
  "ar": 6100000000,
  "cache": 8412,
  "outbox": 61200000,
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
