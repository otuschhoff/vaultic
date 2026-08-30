# Phase 16 Reconciliation Feasibility

Date: `2026-08-30T03:02:20Z`
Commit: `4e092f9ff-dirty`
Environment: `Apple M1 Max, 64 GiB`; `go1.27.0 darwin/arm64`, 10 CPUs
Workload: `200` inodes/sample, `7` samples, `50` warm-up inodes, `8` unique content IDs/inode

| Metric | Baseline | Analytics enabled | Overhead |
|---|---:|---:|---:|
| Median authoritative time | 20.478760 s | 20.474569 s | 0.024% |
| p95 authoritative time | 20.498967 s | 20.486552 s | paired p95 0.156% |
| Authoritative mutations | 3600 | 3800 | 5.556% |
| Authoritative encoded bytes | 316000 | 336400 | 6.456% |
| Encoded bytes/inode | 1580.000 | 1682.000 | 6.456% |

Post-commit catch-up: 200 deltas in 0.408895 s (489 deltas/s), 26876 retained derived bytes.

## Gates

```json
{
  "authoritative_metadata_write_10pct": "pass",
  "reconciliation_cpu_time_5pct": "pass"
}
```

## Methodology

- Each pair publishes identical deterministic first-seen inode revisions through SchemaStore.PublishReconciledRevision and a real vaulticdb transaction.
- Fixture encoding, daemon startup, analytics metadata setup, revision allocation, warm-up, validation reads, and catch-up are outside the authoritative wall-time interval.
- Sample order alternates baseline-first and enabled-first; reported overhead is the median and p95 of paired sample ratios.
- Authoritative bytes are exact key plus encoded-value bytes for every mutation produced by these first-seen reconciliations; enabled accounting includes ae: deltas.
- Post-commit catch-up runs in a separate repository and is reported independently.

## Limitations

- The CPU/time gate is evaluated with authoritative wall time because vaulticdb executes in a separate process; process CPU attribution is not portable through the public client API.
- Encoded-byte accounting is logical authoritative metadata, not physical SlateDB WAL, block-compression, or compaction amplification.
