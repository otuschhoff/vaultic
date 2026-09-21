# Phase 32: VaulticDB critical-path telemetry design

[Phase 32](phase-32-scalable-legacy-metadata-bulk-import.md) |
[Phase 34](phase-34-operational-monitoring-and-metrics-export.md)

**Status (2026-09-21):** V1 and V2 are implemented; V3 is partial. Existing
VaulticDB attribution plus new transaction-lock and process-resource signals can
distinguish the main client, service, queue, durability, LSM and backend waits.
Aligned importer-stage distributions and existing SlateDB GET-key and point-
filter counters are included in monitor snapshots. SlateDB revision
`f549d4af9e8a7e41f46c45f4f31095cc743a703a` also exports MultiGet fanout, needed
blocks/bytes, coalesced reads, and gap projections, with explicit mixed-version
availability. Exclusive writer substages remain unmeasured.

This document owns telemetry coverage and interpretation, not benchmark results.
See the [experiment ledger](phase-32-p3-p7-evidence.md) for accepted/rejected
candidates and P7b recovery, and the roadmap's
[validation policy](phase-32-scalable-legacy-metadata-bulk-import.md#validation-and-profiling)
for run durations and promotion gates.

## Objective

Explain the complete legacy-import wall clock well enough to select one change
that increases useful CPU, network and memory utilization despite backend
latency. Every diagnostic run must answer:

1. Why is work waiting: admission lock, transaction lock, writer queue,
   backpressure, dependency response, retry, acknowledgement, durability,
   compaction, confirmation or shutdown?
2. Where is execution time spent, separating client elapsed time, server
   request time, queue wait, exclusive writer stages and background output?
3. Which ordered chain is the observed critical path, without adding nested or
   concurrent timers as though they were disjoint wall time?
4. Which resource has usable headroom: CPU, network request/byte concurrency,
   memory/memtable/cache/WAL capacity, writer occupancy or compaction capacity?

## Coverage and gaps

| Boundary | Current signal | Required work |
|---|---|---|
| Import scheduler | lanes, ready/reduction queues, active age, bounded waits, and process-local importer stage distributions persisted with the daemon timeline | Add stages only for unresolved boundaries demonstrated by the latest run. |
| Admission/fencing | admission wait/contention/hold and fence-check histograms | Map active age/contentions into explicit wait-state metrics. |
| RPC request | write-batch, begin, commit and rollback request histograms are exported | Preserve inclusive request scope; do not add these to their nested stage times. |
| Transaction internals | transaction begin, map/slot lock wait and engine submit elapsed | Lock hold and submit prework remain part of their enclosing spans. |
| Sequential writer | queue depth, queue wait, service time and backpressure | Export capacity when known; add exclusive validation, conflict, WAL append and memtable-apply stages. |
| WAL/durability | retained/uploaded bytes, flushes, durable wait, WAL object operations, configured flush interval and unflushed-byte limit | Outstanding/oldest ages are required where not already exposed; do not infer durability from the flush interval. |
| LSM output | memtable/L0/SST/sorted-run gauges, flush/compaction counters, GET keys, point-filter outcomes, MultiGet calls/keys/SST visits/blocks/bytes/coalesced reads and gap projections | Preserve `engine_multi_get_metrics_available`; compare early/late and matched-work deltas rather than terminal totals alone. |
| Object store | operation histograms and bytes by database/WAL/coordination role | Add active request count/oldest age to utilization views; record retry delay when the backend exposes it. |
| Cache | occupancy, traffic, admission/eviction/failure, latency, and admission-rejection reasons | Add object identity/reuse distribution only if reason counters cannot select the next experiment. |
| Process resources | importer Go runtime plus VaulticDB CPU time, RSS, thread count and process I/O bytes on Linux | Virtual memory remains unavailable; non-Linux process fields are explicitly unavailable. |
| Host resources | external tools only | Capture CPU, memory, disk and interface counters in the experiment artifact; do not label host totals as VaulticDB-exclusive. |
| Finalization | marker/close/handoff/reopen action phases and server finalization | Preserve final snapshots and distinguish a bounded timeout from successful lifecycle completion. |

Human confirmation is not part of unattended import execution. It remains a
Phase 34 operation state for workflows that actually require it and must not be
synthesized for VaulticDB imports.

## Delivery plan

### V1. Self-contained capture

Implemented. `--monitor-export-jsonl` creates a new private file and emits one
validated combined snapshot per line through the bounded asynchronous exporter.

The append-only JSON Lines `SnapshotExporter` and import CLI path are
mutually exclusive with InfluxDB. Create the output with `0600`, reject symlinks
and non-regular files, encode one validated monitor-schema snapshot per line,
and flush each record to the kernel. Continue to use the bounded asynchronous
replace-oldest queue, expose drops/failures, and make one timeout-bounded final
attempt. A diagnostic run is incomplete performance evidence if any interval is
missing or exporter drops/failures are nonzero.

### V2. Complete existing mapping

Implemented. Request timers, flush/stall/compaction counters, aggregate cache
diagnostics, and reservation/background-budget/background-task admission
rejection reasons are mapped without adding unbounded tier labels.

Already-available request, engine flush/stall/compaction and cache counters are
exposed through monitor schema v2. Preserve cumulative counters and fixed
histograms. Mark unsupported bytes, retry delays, pressure, capacity and process
resource fields unavailable rather than zero.

### V3. Exclusive critical-path stages

Partially implemented. Transaction-map and transaction-slot lock acquisition
are measured directly. Process counters and configured flush, unflushed-byte and
L0 thresholds are exported. The pinned SlateDB fork still reports sequential
batch queue/service and backpressure as aggregate stages; WAL append, conflict
validation and memtable apply are not separately exposed and must not be inferred
as exclusive timings from the broad engine-submit span.

Instrument bounded server stages at their direct owners. Timers must be either
exclusive or documented as inclusive. The minimum ordered commit chain is:

```text
admission wait -> fence/authority -> transaction lookup/lock -> commit prework
-> engine backpressure -> writer queue -> writer validation/conflict
-> WAL append -> memtable apply -> response -> optional durability wait
```

Flush, compaction and object-store work are concurrent background paths. Relate
them to foreground stalls by aligned queue/backpressure and active-age changes;
never add their worker time to foreground wall time.

### V4. Saturation and decision report

For each run under the roadmap's duration policy, compute interval deltas and
rates from local JSONL, including early/late windows and per-blob costs:

- useful blobs/s, mutations/s and engine bytes/s;
- lane, writer and compactor occupancy;
- queue depth, oldest age, wait/service p50/p95/p99 and backpressure ratio;
- object requests/s, bytes/s, active operations and role latency;
- memtable, immutable, L0, WAL and cache occupancy against known limits;
- VaulticDB process CPU cores used, RSS growth, threads and I/O rates;
- exporter coverage and reset/restart detection.

Select P5 only when client planning/read/lock evidence dominates while the
writer and backend have headroom. Select P6 only when writer queue/service,
backpressure or LSM output is the limiting aligned boundary. Tune concurrency
or batch sizes only when queue and resource headroom show that extra in-flight
work can hide latency without unbounded memory or finalization growth.

## Acceptance

- Monitor schema validation, fixed cardinality and no raw IDs or secrets.
- Disabled telemetry preserves existing behavior; enabled overhead is measured.
- Unit tests cover partial writes, close, permissions, invalid snapshots,
  bounded queue loss and process restart/reset.
- Rust tests prove every timer settles on success, failure and cancellation.
- Go/Rust race and concurrency suites pass.
- Matched HDD runs persist complete importer and VaulticDB series with zero
   drops, explicit startup/stale availability, and no unexplained counter resets.
- An explicitly authorized uncapped acceptance run includes marker, close,
   handoff, reopen, verification, activation, and clean shutdown; diagnostic
   timeout runs are never called completion evidence.