# Operational Observability

[← Back to roadmap index](00-overview.md)

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
- per-backend placement counts and bytes by state (pending, live, evicting)
- packs below the durability predicate, and the age of the oldest unsatisfied
  offsite deadline
- placement attempts, failures, retries, and bytes transferred per backend
- scheduler queue depth by urgency class, and bandwidth utilisation per backend
- promotions evaluated, deferred, and performed, with bytes promoted and bytes
  never promoted because the data expired first
- capacity headroom per backend where a ceiling is declared
- pack history events written, rollup runs, and history retention truncations
- history coverage state per granularity (complete, partial, reconstructed)
- history events dropped or unreadable, and the retained raw floor
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
- syslog events dispatched, delivered, and dropped by severity/category and target endpoint
- Azure Key Vault secret fetch requests, latency, and token acquisition events
- GDPR audit queries, chunk survival analysis calculations, and erasure executions
- UID backup blocklist policy matches and excluded file counts during backup crawls
- storage verification requests, candidate matches, sample selection counts, verification level results (header/checksum/unpack), and failed pack detections
- `cwalk` traversal throughput, active worker concurrency, and directory queue saturation
- `pathdiff` query latency, changed paths returned, event coverage validation status (verified/fallback), volume/SVM resolution hits, and subtrees skipped

Never log inode paths, access tokens, or raw repository keys at normal verbosity.

