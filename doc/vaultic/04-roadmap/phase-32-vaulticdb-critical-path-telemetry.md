# Phase 32: VaulticDB critical-path telemetry design

[Phase 32](phase-32-scalable-legacy-metadata-bulk-import.md) |
[Phase 34](phase-34-operational-monitoring-and-metrics-export.md)

**Status:** V1 and V2 implemented as of 2026-09-20; V3 is partial. Existing
VaulticDB attribution plus new transaction-lock and process-resource signals can
distinguish the main client, service, queue, durability, LSM and backend waits.
A valid 30-minute representative HDD timeline selects P5 read amplification,
not P6 writer concurrency. Its first cache fill-budget experiment was rejected;
reason-specific admission rejection counters now discriminate the next run.

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
| Import scheduler | lanes, ready/reduction queues, active age, bounded waits | Persist locally with the daemon timeline. |
| Admission/fencing | admission wait/contention/hold and fence-check histograms | Map active age/contentions into explicit wait-state metrics. |
| RPC request | write-batch, begin, commit and rollback request histograms exist in VaulticDB | Export all four; begin/commit/rollback are currently dropped by the monitor adapter. |
| Transaction internals | transaction begin, map/slot lock wait and engine submit elapsed | Lock hold and submit prework remain part of their enclosing spans. |
| Sequential writer | queue depth, queue wait, service time and backpressure | Export capacity when known; add exclusive validation, conflict, WAL append and memtable-apply stages. |
| WAL/durability | retained/uploaded bytes, flushes, durable wait, WAL object operations | Add configured limits and outstanding/oldest ages needed for saturation ratios. |
| LSM output | memtable/L0/SST/sorted-run gauges, flush/compaction counters | Export currently dropped flush/stall/compacted-SST counters; add immutable backlog and oldest age. |
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

Add an append-only JSON Lines `SnapshotExporter` and an import CLI path that is
mutually exclusive with InfluxDB. Create the output with `0600`, reject symlinks
and non-regular files, encode one validated monitor-schema snapshot per line,
and flush each record to the kernel. Continue to use the bounded asynchronous
replace-oldest queue, expose drops/failures, and make one timeout-bounded final
attempt. A 30-minute run is invalid performance evidence if any interval is
missing or exporter drops/failures are nonzero.

### V2. Complete existing mapping

Implemented. Request timers, flush/stall/compaction counters, aggregate cache
diagnostics, and reservation/background-budget/background-task admission
rejection reasons are mapped without adding unbounded tier labels.

Expose all already-available request, engine flush/stall/compaction and cache
counters through monitor schema v2. Preserve cumulative counters and fixed
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

For each 30-minute run, compute interval deltas and rates from local JSONL:

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
- A matched 30-minute HDD run persists complete importer and VaulticDB series
  with zero drops before any optimization is selected.
- A later uncapped run includes marker, close, handoff, reopen, verification and
  activation; diagnostic timeout runs are never called completion evidence.