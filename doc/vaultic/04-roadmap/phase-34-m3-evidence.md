# Phase 34 M3 production-boundary evidence

This artifact records Phase 34 M3 at base revision `dfbe36f3a` plus the M3
worktree on 2026-09-19. M3 installs bounded accounting at production owners; it
does not change scheduling, durability, repository capability interfaces, or
claim representative throughput.

## Coverage map

| Boundary | Production owner and observation |
|---|---|
| Backup source | `runBackupPipeline` owns `backup`; `productionFS` attributes open, stat, directory, metadata and read latency/bytes as `source`. It preserves local-filesystem detection through `UnwrapFS`. |
| Data packs and repository objects | Repository raw reads, pack publication, unpacked writes/removes, placement reads and place/evict transfer completion attribute `repository` latency, outcome and actual bytes. Integrity failures settle as failures after verification. |
| Restore and replication | Restore and copy command owners preserve their operation context. Destination create/write/release work is `restore/source`; repository reads and writes remain `repository`. |
| Placement and maintenance | Placement plan, execute, promotion and derived-index rebuild own `placement`. A capability-safe `Store` decorator attributes database calls without advertising optional placement-event support. Repository initialization, configuration and deletion own `maintenance` unless a caller already owns the context. |
| Cache lifecycle | Existing bounded cache snapshots remain authoritative. Policy updates own `cache_fill/coordination`; drain and clear own `cache_evict/cache`. Unconfigured no-op paths do not manufacture successful work. |
| VFS/FUSE/NFS | Mount and NFS lifetimes own `restore/read`. Snapshotfs inherits that identity for metadata and content reads; FUSE directory refresh and lookup inherit the mount context. Normal `ErrOK` shutdown is success. |
| Key broker and human input | The serialized broker client measures only the response interval after request write as `key_management/broker/none`, exposes `rpc_response`, and classifies canceled I/O from the context. Blocking terminal password input is separately `key_management/coordination/none` with `human_confirmation`; fail-fast confirmation flags are not waits. |
| WAL, object stores and compaction | Inherited VaulticDB attribution remains authoritative for writer/engine latency, role-aware object-store operations, WAL state and compaction state. No pinned-fork change was required. |
| Queues | The only frozen v2 queue identities remain SlateDB `batch_write` and legacy import `legacy_import_ingest`/`legacy_import_reduce`, all inherited from their actual owners. Pack upload is an unbuffered handoff and placement is a bounded scan, not retained queues. |

Production action classes share one `OperationRegistry`, enforcing the component
limit of 128 active records and globally unique IDs. Only touched metric families
are emitted. Snapshot output is capped at 256 metrics; displaced accounting and
wait series increment `cardinality_dropped`, including the two slots reserved for
exporter health.

## Availability and exclusions

A zero value is never used to imply unavailable data. VaulticDB queue capacity,
SST payload/physical size, unsupported WAL facts, and cache traffic/inventory
fields retain their existing explicit `unavailable`, `estimated`, or `stale`
availability. Placement catalog scans expose estimated object count and physical
bytes; payload and physical-backend reconciliation remain unavailable. They are
not labeled as reconciled inventory.

Configured backend identity is available in bounded storage snapshots. The
frozen dependency family contains operation and role, not backend ID, so source
and destination placement calls are distinguished causally by active phase and
storage snapshots rather than an unbounded metric label. Representative backend
throughput, cache effectiveness and infrastructure-specific latency remain M7
work and are not inferred from M2 fixtures.

## Attribution and resource checks

Deterministic checks cover:

- an M2 source service delay appearing in `backup/source` latency while payload
  bytes and result remain identical;
- an M2 repository GET delay appearing in `restore/repository`, with exact bytes
  and no retained active operation;
- blocked and canceled broker responses producing live and settled wait state;
- VFS requests inheriting the long-lived restore owner;
- restore destination bytes, placement database calls, and cache policy/drain
  attribution;
- graceful mount/NFS shutdown classification;
- overlapping blockers, including the same reason in different phases;
- global active-operation overflow and a schema-valid saturated 256-series
  exporter snapshot.

All blocked tests use channels or context cancellation, not sleeps. Guards settle
on success, failure, cancellation and timeout without retaining per-request
history. Repository backend interfaces are instrumented at owning completion
boundaries, so optional capabilities are unchanged.

## Measured overhead

On linux/amd64, Go 1.27.1, Intel Xeon Gold 5217, three repeated `-benchmem` runs:

| Benchmark | Disabled | Enabled |
|---|---:|---:|
| Production dependency event | 8.06-8.18 ns/op, 0 B, 0 allocs | 216.2-217.4 ns/op, 32 B, 1 alloc |
| Production source open plus 7-byte read | 1.05-1.16 us/op, 1280 B, 13 allocs | 2.63-2.64 us/op, 1536 B, 19 allocs |

The source fixture is intentionally tiny and allocation-dominated. Its roughly
1.5 us absolute increment is a hot-path instrumentation bound, not representative
backup throughput. M7 must measure current-revision workloads on representative
NFS, RADOS and cloud infrastructure.

## Validation

Focused acceptance commands include:

```text
go test -race ./internal/telemetry ./internal/index/broker ./internal/index/maintenance
go test -race ./internal/snapshotfs -run '^TestVFSReadInheritsRestoreOperation$'
go test -race ./internal/restorer -run '^TestFilesWriterAttributesRestoreDestinationBytes$'
go test -race ./internal/repository -run '^Test(ReadCachePolicyAndDrain|Phase34M3RepositoryReadAttributesM2ServiceStall)$'
go test ./cmd/vaultic/backupcmd
go test ./cmd/vaultic -run 'TestPhase34M3|TestMonitorExportHealthReservesBoundedMetricCapacity|TestPlacementStorageSnapshotsAreSorted|Test.*(Restore|Copy|Forget|Prune|Mount|ServeNFS|Cache)'
go test ./internal/telemetry -run '^$' -bench 'Benchmark(ProductionDependencyAccounting|ProductionFSRead)$' -benchmem -count=3
```

The full repository race package retains an unrelated reproducible
`TestReadCacheChunkBoundaryAndFinalShortRead` failure where a repeated unaligned
read reaches origin (`requests 3 -> 4`, `bytes 216 -> 220`). M3 does not modify
that cache read path. A repository-wide `go test ./... -count=1` attempt also
retains the root-only `TestArchiverErrorReporting/file-unreadable` panic, the
existing production-discard allowlist findings, and the environment-sensitive
`TestBackupErrors` failure. The latter passes in isolation and fails in the full
command package. Focused M3 suites pass in the same environment. Full-suite test
children can leave PPID-1 `vaulticdb` daemons that ignore `SIGTERM`; the verified
workspace children were terminated with `SIGKILL` after validation.

Two final independent reviews of the implementation and M3 gate reported exactly
`No actionable findings.`
