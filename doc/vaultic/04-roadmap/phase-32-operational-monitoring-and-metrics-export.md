# Phase 32: Operational monitoring and bounded metrics export

[← Back to roadmap index](00-overview.md)

[← Phase 31](phase-31-read-only-nfsv3-snapshot-server.md) · [Phase 33 →](phase-33-writable-fuse-and-durable-writeback.md)

[Operational observability](02-observability.md) · [CLI and operations architecture](../02-architecture/04-cli-and-operations.md)

**Status: design specification, not yet implemented.**

**Goal:** make storage use, active work, queues, throughput, latency, WAL pressure, and read-cache effectiveness visible across `vaultic`, `vaulticdb`, and the key broker without retaining an unbounded local time series or adding material hot-path overhead. Operators get cheap point-in-time commands and an optional live terminal dashboard. Long-term graphing and retention belong to an explicitly configured exporter, initially InfluxDB v2, rather than process memory or the repository metadata database.

## Questions this phase must answer

- How many data-pack objects and bytes are stored on each authoritative backend, by pack type and placement state?
- How many SlateDB WAL, SST, manifest, and supporting objects and bytes are stored on each database or WAL backend?
- Which backup, upload, writeback, replication, prune, cache-fill, compaction, and recovery operations are active, and how far have they progressed?
- What work is queued, how old is the oldest item, and which concurrency, bandwidth, capacity, durability, or WAL limit is applying backpressure?
- What are the recent transfer and operation rates, error rates, and response-time distributions?
- Is SlateDB WAL flush or checkpoint progress throttling commits or allowing retained WAL to grow?
- Are the SlateDB and repository data-pack read caches serving useful bytes, or consuming space without avoiding origin reads?
- For every read-only cache target, how much of its limit is used, reserved, pinned, staging, reclaim-pending, and free, and how is occupancy divided among encrypted packs/ranges, compressed derived containers, decoded blobs/extents, whole files, SlateDB SSTs/blocks, and cache metadata?

## Measurement model

Do not treat every metric as a time series inside the serving process. Use four bounded instrument types:

| Type | Examples | Local retention |
|---|---|---|
| Current gauge | cache bytes, queue depth, active workers, retained WAL bytes, oldest backlog age | current value only |
| Monotonic counter | bytes uploaded, origin bytes read, cache hits/misses, operations and failures | process lifetime, with reset/start identity |
| Bounded distribution | backend, VFS, RPC, WAL flush, and commit latency; request and batch size | fixed buckets in rotating recent windows |
| Active-operation record | kind, phase, start time, progress, rate limit, blocking reason | active work only, in a bounded registry |

The TUI and exporters derive rates from counter deltas. They must not require the daemon to retain per-request samples. Histograms use fixed, versioned buckets and a small rotating ring sufficient for recent views such as 1, 5, and 15 minutes; rotation cost is constant and idle buckets are allocated lazily. A cumulative histogram may also be exposed for exporters. Do not store completed-operation history locally beyond aggregate counters and structured lifecycle events.

Instrumentation on request and I/O paths uses atomics or thread-local aggregation and never blocks on rendering, disk, DNS, or an exporter. Collection reads a consistent-enough snapshot with a capture timestamp and per-process start ID; it does not stop active work to create a globally atomic view. Failed or slow collectors are reported as stale sections, not allowed to stall the monitored operation.

Metric names, units, counter-reset semantics, histogram buckets, and labels form a versioned schema. Labels are restricted to bounded enums and configured identities such as component, operation class, backend ID, storage role, cache target, representation, outcome, and throttle reason. Paths, snapshot IDs, pack IDs, object keys, principal IDs, client addresses, error strings, and arbitrary user labels are forbidden metric dimensions. Detailed identifiers belong only in access-controlled active-operation views or rate-limited structured events.

## Accounting sources

### Authoritative data and metadata storage

Repository pack totals come from the Phase 9 pack catalog and Phase 12 placement records, grouped by backend, pack type, and placement state. These are logical catalog aggregates, not repeated backend listings. Show physical bytes where known, payload bytes, object count, last reconciliation time, and whether each value is exact, estimated, or stale. Scheduled reconciliation compares catalog totals with bounded/paginated backend inventory and repairs drift through the owning phase's rules.

SlateDB reports WAL, SST, manifest, checkpoint, temporary, and other object counts and bytes separately for the database and WAL stores. Prefer SlateDB manifest/checkpoint state and object-store adapter counters for the fast view; use asynchronous inventory reconciliation for physical usage. Never scan S3 buckets, librados pools, or the full pack catalog on every status refresh.

### Operations, queues, and throttling

Each component exposes a bounded active-operation registry. Records contain a generated operation ID, operation class, current phase, start and last-progress times, completed and expected units when known, instantaneous blocking reason, and parent operation ID. Human-readable paths, object keys, command arguments, credentials, and key material are excluded. When the registry is full, aggregate overflow by operation class and increment an overflow counter rather than growing memory.

Every real work queue exports depth, capacity, admitted/rejected totals, oldest-item age, active workers, and configured/effective concurrency. Backpressure reports a bounded reason: concurrency, bandwidth, cache capacity, backend retry, credential renewal, writer fencing, WAL flush, WAL retention/checkpoint, compaction, durability, or shutdown. Components without a queue report `not_applicable`, not zero work.

Writeback views separate bytes accepted, buffered, in flight, durably acknowledged, retried, and failed. They report throughput per authoritative backend and time waiting for local scheduling, remote service, retry delay, WAL durability, and metadata commit. SlateDB views include WAL append/flush latency, outstanding flushes, retained segments/bytes, oldest uncheckpointed age, checkpoint and compaction progress, commit latency, throttled duration, and the active throttle reason. This makes a cloud bottleneck distinguishable from SlateDB WAL or compaction pressure.

### Read-cache effectiveness

Report each Phase 29 and Phase 30 cache independently and as a coordinated total:

- requested and effective byte limits;
- logical, allocated, and raw-estimated bytes where the backend exposes them;
- used, reserved, staging, pinned, deletion-pending, reclaim-pending, and available bytes;
- object/entry count and bytes by cache family and representation;
- hits, misses, partial hits, corruptions, bypasses, evictions, and admission rejections by bounded reason;
- requested bytes, cache-served bytes, origin bytes, origin requests avoided, fill/write bytes, and read/write amplification;
- lookup, hit, fill, verification, decode, and origin latency distributions;
- coalesced-request count, in-flight fills, reconciliation age, and quota-controller state.

The live view derives hit ratio, byte hit ratio, origin avoidance, useful bytes per occupied byte, and recent rates. Counters remain meaningful when no TUI is attached. Kernel page cache, NFS client cache, and storage-system internal caches are identified as outside the measured Vaultic cache unless a reliable source explicitly reports them.

## Commands and live interface

Add snapshot commands with stable machine-readable output:

```text
vaultic monitor status [--component COMPONENT] [--backend ID] [--json]
vaultic monitor storage [--backend ID] [--reconcile] [--json]
vaultic monitor operations [--active] [--json]
vaultic monitor caches [--cache ID] [--json]
vaultic monitor watch [--interval DURATION] [--view overview|storage|operations|wal|caches|latency]
vaulticdb monitor status|storage|operations|wal|caches [--json]
vaultic-key-broker monitor status|operations [--json]
```

`vaultic monitor` is the preferred aggregate client. It reads the local Vaultic process where applicable and authenticated status endpoints from VaulticDB, the shared cache coordinator, and the key broker. Missing, unauthorized, incompatible, or stale components remain visible as unavailable sections. Component-native commands remain useful for diagnosis and bootstrap without a working aggregate client.

Snapshot commands exit after one collection and are suitable for scripts. `monitor watch` requires a terminal, maintains only the samples needed for its displayed windows, and never changes daemon retention. It provides keyboard-selectable overview, storage, active operations, WAL/writeback, cache, and latency views; sortable tables; pause/reset-window controls; explicit sample age; counter-reset markers; and narrow-terminal fallback. Rendering is rate-limited and decoupled from collection. Non-interactive use requires `--json` or a snapshot command rather than emitting terminal control sequences.

The default overview emphasizes actionable saturation: slowest backend, writeback rate and backlog, WAL/checkpoint pressure, cache occupancy and byte-hit ratio, origin traffic, active failures/retries, and stale collectors. Expensive `--reconcile` inventory is an explicit asynchronous operation with progress and cancellation; it is never triggered by `watch` refreshes.

## Collection and transport

Define one shared telemetry schema and small instrumentation library per implementation language, not a second metrics vocabulary in each command. Each long-running component exposes an authenticated local status API over its existing protected control channel. Short-lived `vaultic` commands can expose an ephemeral in-process collector to the aggregate monitor or emit a final snapshot. Remote status follows the component's existing authentication and least-privilege rules; Phase 38 later extends this access to enrolled remote principals.

Collection has configurable timeouts, maximum response size, maximum active-operation records, histogram count, and refresh frequency. Metrics collection and export have separate bounded queues. On overflow, coalesce gauges, preserve counters through the next successful snapshot where possible, drop distribution intervals or events according to documented policy, and expose dropped-export counters. Telemetry failure never blocks backup, restore, database durability, cache reads, or broker lease handling.

## InfluxDB v2 and future exporters

Keep the collector independent from any monitoring vendor. Define an exporter interface over schema-versioned snapshots, then provide an opt-in InfluxDB v2 exporter using its HTTP write endpoint. Configuration includes URL, organization, bucket, token file or protected environment source, export interval, batch limit, timeout, TLS trust, and bounded retry/backoff. Tokens are never accepted as command-line flags, returned by status, or written to logs.

Export gauges, counter deltas with reset markers, and histogram buckets or agreed quantiles using low-cardinality tags. A deployment ID and process start ID distinguish restarts without creating one series per operation. Active-operation details and arbitrary IDs are not exported as metric tags; export counts and oldest ages by class, with lifecycle details going to the existing structured event path. The exporter keeps only one bounded retry batch or spool budget and drops oldest telemetry when exhausted. It must never write monitoring history into VaulticDB, the repository, a WAL, or a read-cache tier.

Prometheus/OpenTelemetry or other exporters may be added against the same snapshot contract later. The InfluxDB implementation must not leak its naming, retry, or authentication model into instrumentation call sites.

## Security and operational constraints

Monitoring is read-only and follows existing local socket ownership and remote authentication boundaries. Summary views may expose backend aliases, capacity, performance, and operation classes, which are operationally sensitive even without secrets. Redact credentials, key material, object keys, paths, repository object IDs, user-controlled error text, and broker principal details. JSON output carries a schema version and explicit units.

Default telemetry memory is a documented fixed budget per component and scales only with configured backends, cache targets, operation classes, and histogram definitions, all with hard caps. Cardinality-overflow, active-registry-overflow, stale-snapshot, dropped-event, and dropped-export counters make loss visible. Disabling live windows or export leaves correctness and point-in-time accounting intact.

## Implementation steps

1. Inventory existing Phase 9, 12, 22, 28, 29, 30, and 31 counters and status APIs; define the versioned names, units, labels, availability states, and reset semantics.
2. Implement bounded gauges, monotonic counters, rotating fixed-bucket distributions, and the active-operation registry in Go and Rust with memory/cardinality tests.
3. Instrument authoritative backend placement, data-pack transfer, VaulticDB object-store and WAL paths, work queues, cache accounting, VFS/FUSE operations, and NFS RPCs without per-request allocation where practical.
4. Add authenticated component snapshot endpoints and component-native `monitor` commands with stable JSON.
5. Implement the aggregate `vaultic monitor` snapshots and terminal dashboard, including counter-delta rates, rolling latency views, stale/reset handling, and explicit asynchronous storage reconciliation.
6. Add the exporter interface and opt-in InfluxDB v2 writer with bounded batching, retry, secret handling, health metrics, and documented dashboard field/tag examples.
7. Document metric interpretation and diagnostic playbooks for slow cloud writeback, WAL throttling/checkpoint lag, ineffective SlateDB cache, ineffective pack/blob cache, and cache capacity pressure.

## Tests

- Unit tests prove counter concurrency, gauge replacement, histogram rotation and bucket boundaries, operation-registry overflow, label allowlists, reset detection, and fixed memory bounds.
- Golden JSON tests pin schema version, names, units, unavailable/stale states, and redaction. Compatibility tests allow an older client to ignore newer fields and reject an unsupported major schema safely.
- Integration tests inject cloud latency, retryable errors, bandwidth limits, WAL flush stalls, compaction pressure, full queues, cache misses, cache corruption, quota shrink, and slow NFS/FUSE operations; each condition must appear under the correct backend, queue, cache, and throttle reason.
- Accounting tests compare pack placement, SlateDB object, and cache totals with controlled local, S3, and librados inventories and verify exact/estimated/stale markers plus asynchronous reconciliation.
- TUI tests use deterministic snapshots to verify rate and percentile calculations, counter resets, stale components, narrow terminals, resize, pause, and clean shutdown without increasing daemon retention.
- Load tests demonstrate bounded telemetry CPU, allocation rate, memory, response size, and exporter queues at maximum supported backend/cache counts and request concurrency. A blocked or unavailable InfluxDB endpoint cannot affect data-path latency or durability.
- Security tests verify status authorization and prove paths, object IDs, credentials, keys, tokens, user error strings, and unbounded labels do not appear in metrics, JSON, terminal output, logs, or InfluxDB tags.

## Exit criterion

Operators can use snapshot commands or a live terminal dashboard to identify where repository packs and SlateDB objects reside, what work is active or queued, which limit is applying backpressure, how quickly each backend is moving data, whether WAL/checkpoint behavior is throttling commits, and whether each SlateDB or repository read cache is saving origin traffic relative to its occupied space. Point-in-time state and lifetime aggregates remain cheap when nobody is watching; recent latency and throughput windows use fixed memory; detailed completed-operation timelines are not retained locally. An optional InfluxDB v2 exporter can persist low-cardinality time series without putting secrets, arbitrary identifiers, monitoring history, or unbounded queues into Vaultic, VaulticDB, the key broker, or repository storage.