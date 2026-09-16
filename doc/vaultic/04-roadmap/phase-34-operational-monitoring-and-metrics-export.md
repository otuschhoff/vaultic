# Phase 34: Operational monitoring and bounded metrics export

[← Back to roadmap index](00-overview.md)

[← Phase 33](phase-33-index-check-scalability-and-performance.md) · [Phase 35 →](phase-35-writable-fuse-and-durable-writeback.md)

[Operational observability](02-observability.md) · [CLI and operations architecture](../02-architecture/04-cli-and-operations.md)

**Status: design specification, not yet implemented.**

**Goal:** make storage use, active work, queues, throughput, latency, WAL pressure, and read-cache effectiveness visible across `vaultic`, `vaulticdb`, and the key broker without retaining an unbounded local time series or adding material hot-path overhead. Operators get cheap point-in-time commands and an optional live terminal dashboard. Long-term graphing and retention belong to an explicitly configured exporter, initially InfluxDB v2, rather than process memory or the repository metadata database.

## Shared foundation and ownership

Phase 34 owns the shared wait-accounting, backend-profile and experiment-artifact
contracts. Deliver their foundation incrementally through
[Phase 32](phase-32-scalable-legacy-metadata-bulk-import.md) attribution and import
experiments, and [Phase 33](phase-33-index-check-scalability-and-performance.md)
Stage A and checker experiments. Reuse existing helpers and harnesses, not a
different metric vocabulary or scheduler per workload. Neither workload waits
for monitor commands, the dashboard or exporters. Existing instrumentation is
partial; the additions below remain pending and do not retroactively certify
completed benchmark stages.

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

## Wait-state accounting contract

Observe states per operation or worker, not one global process state. This
instruments existing Go/Tokio scheduling rather than replacing it. Separate
admission, prerequisite wait, execution and completion, with bounded reasons for
dependencies, locks, concurrency/byte budgets, source I/O, backend I/O, RPC
responses, retry backoff, credential/lease renewal, fencing, human confirmation,
durability acknowledgement, flush and compaction pressure. Human confirmation
is distinct from a machine acknowledgement.

Each applicable boundary records attempts, observed contention count, completed
wait count, cumulative duration, bounded histogram, current waiters and oldest
active wait. Completed-duration histograms alone hide permanently stalled work.
Distinguish immediate acquisition from contention using the owning primitive's
supported semantics, not an elapsed-time threshold; mark unavailable information
explicitly. Measure lock acquisition and hold time separately. Outcomes include
success, failure, cancellation and timeout. Rust guards and Go deferred cleanup
must settle accounting on dropped futures and early returns without changing
fairness, ownership or cancellation behavior.

Use monotonic durations and document inclusive/exclusive timer boundaries. End
engine-submit timing before explicit durability timing: marking a drop-based
timer successful does not end its lifetime. Ten workers waiting together for
one second accumulate ten worker-seconds, not ten seconds of job elapsed time.
Nested RPC, durability and object-store durations cannot be summed as independent
wall-time phases. Cross-process correlation must not subtract unrelated
monotonic clocks or assume synchronized clocks.

Keep aggregates available without tracing. Optional bounded, sampled parent/child
spans and dependency links explain fan-out, coalesced flushes and shared work.
Use generated correlation IDs, not raw object identifiers or metric labels;
report sampling/drop counts and export off the hot path. Active states do not
prove on-CPU execution, and client RPC time is not pure server service time.
Use Go profiles/traces and Tokio diagnostics for complementary CPU/runtime waits.

## Backend scenarios and experiment contract

Start with controlled injection around the real pipeline in isolated disposable
repositories. A later discrete-event simulator is optional, calibrated against
real runs and validated on held-out scenarios. Aggregate counters alone cannot
predict batching, cache behavior, sequential apply, queueing or compaction.

### Shared profiles

| Profile | Required distinctions |
|---|---|
| On-prem HDD array via NFS | Metadata/open/stat latency, sequential versus random reads, client/server cache state, write acknowledgement versus stable-storage commit, shared array/link bandwidth, concurrency and bursty flush/compaction queueing. Record resolved mount and relevant NFS mount/export settings. |
| On-prem native RADOS with three replicas | Operation-specific acknowledged latency, object size, replica/min-size and acknowledgement semantics, shared network/OSD capacity, normal versus recovery/backfill conditions and contention tails. Three replicas do not imply three times client latency. |
| Cloud S3 with an 8 ms baseline | Until measured otherwise, label 8 ms as assumed network RTT, not universal operation completion time. Distinguish RTT, GET time-to-first-byte and small-object PUT completion. Model GET/range GET, HEAD, PUT/multipart completion, LIST and conditional operations separately, including bandwidth, jitter, tails and throttling. |

These are hypotheses, not measured hardware specifications; retain unknown
parameters explicitly. Every scenario maps source data, repository packs,
SlateDB SST/manifest, WAL, coordination, caches and scratch to configured
resources. Shared arrays, pools and links share capacity limits. Record the
measurement boundary and endpoint perspective of latency; never count an RTT
twice or silently replace service latency with it.

### Injection and safety

Reuse Rust `ObjectStore`, Go backend, and Go source filesystem/file wrappers;
include checker scratch I/O. Target WAL, SST/manifest and coordination separately
while preserving shared capacity. Place origin injection below caches. Cover
stream consumption, listing and multipart completion, not merely returning a
reader/future. Separate queue admission, I/O attempts, transfer and retry backoff.
Preserve conditional writes, fencing and actual durability acknowledgements.

Versioned profiles specify operation/role, added baseline delay, seeded jitter,
tail stalls, correlated slow periods, bandwidth/concurrency and optional retryable
errors. Declare additive delay over a real backend versus synthetic service
latency. Record sampled and observed delay. Use stable operation identities for
seed assignment where available; a seed alone cannot make concurrent execution
deterministic. Async paths use async timers; respect deadlines/cancellation and
disclose blocking-filesystem cancellation limitations.

Injection is explicitly gated, test/benchmark-only and disabled by default.
Require isolated test targets; refuse live repository targets. Never shape
production networking, drop production caches or weaken durability for a result.
Bound delays, queues and run duration. API-level injection cannot reproduce all
filesystem, OSD or provider internals; validate consequential recommendations
on representative infrastructure.

### Synthetic dependency responses

Test both sides independently: run the real Vaultic action against controlled
vaulticdb RPC and pack-backend responses, and run the real vaulticdb service and
SlateDB against controlled WAL, SST/manifest and coordination I/O. Prefer wrappers
around real isolated dependencies. Small fakes are scheduling/correctness oracles,
not throughput models unless their batching, state and contention are validated.

Profiles distinguish three injection modes, never one ambiguous response delay:

| Mode | Injection point and interpretation |
|---|---|
| Service delay | Delay work at a named dependency execution boundary. Declare which admission slots, locks or capacity remain occupied; this may create server-side queueing. A sleep before admission models a different boundary. |
| Durability delay | Delay the actual persistence prerequisite before durable completion, for example a WAL write through the object store. The real engine still decides when durability is satisfied. Do not merely delay observation of an already completed durable handle and call it slower persistence. |
| Acknowledgement delay | After the real operation completes, withhold its result from the caller. For a durable commit, verify durability before withholding; for deferred commits, label it an apply acknowledgement only. This tests caller pipeline sensitivity without claiming slower persistence. |

For RPC acknowledgement tests, prefer a test-only client delivery wrapper after
the actual response, without retaining server locks/admission slots. A server
egress hook is a separate resource-holding model and must document guard lifetime.
Neither API wrapper alone models network transport flow control. For pack storage,
delay the backend's existing completion boundary and retain its precise guarantee:
successful filesystem write/close is not automatically crash durability. Record
whether the boundary is write, sync/stable-storage acknowledgement, or object PUT
completion. Never add a sync, alter commit options or manufacture success solely
to make backends appear equivalent.

Select methods/operation classes and caller-scoped versus shared dependency delay
explicitly. Do not delay unrelated status, fencing or lease-renewal traffic unless
the profile selects it. Include streamed response consumption and bounded buffering;
never accumulate complete scan results merely to delay a page. Distinguish response
availability, withheld delivery and caller observation in telemetry, using local
monotonic durations and causal links rather than cross-host clock subtraction.

Delayed delivery must honor the caller's deadline/cancellation even after the
underlying operation has succeeded. A timeout does not undo a committed mutation.
Barrier-driven tests must prove success before withholding the response, then
exercise timeout/lost-response recovery with the same idempotency/receipt identity.
Assert no duplicate publication, lost durable state or false success. Preserve
normal errors and retry policy; separate timeout-after-success from failure-before-
execution and cancellation while work is still in flight.

### Action-specific workload adapters

Use the same injection engine with one bounded fixture/adapter per real action:

| Adapter owner | Dependencies to vary | Completion/correctness oracle |
|---|---|---|
| Backup: existing archiver/repository and daemon integration tests | Source stat/read, pack-write completion, metadata commit acknowledgement | Snapshot publication plus content restore/verification; no snapshot references unavailable packs after timeout/retry. |
| Import: Phase 32 P3 | Source-index reads, pack stat, transaction responses, finalization | Identical metadata/checkpoints and required marker/close/handoff/reopen; no assumption that import uploads packs. |
| Check: Phase 33 A/H | Scan pages, batched lookup responses, source reads, scratch I/O | Exact verdict/counter parity and coherent read-session identity. |
| Restore: existing restorer/repository tests | Metadata lookups, pack-read responses/streams, destination writes | Restored content and metadata according to existing restore semantics, with bounded in-flight data. |
| Forget/prune: existing maintenance and command integration tests | Metadata commit responses, listing and deletion completion | Preserved retained snapshots/data and existing deletion/retry safety, only on disposable repositories. |

Backup/restore/forget/prune adapters belong to Phase 34 M2f, not import/check
acceptance. Ship and validate each independently; missing adapters remain explicit
coverage gaps. All are pending design additions, not available benchmark commands.

### Evidence and acceptance

Use one versioned artifact schema: profile/version, seed, input identity/order,
binary/engine revisions, placement, hardware/runtime limits, cache/encryption/WAL
settings, effective batching/concurrency, delay semantics, observation window,
completion criterion and result digest. Store redacted artifacts outside the
measured repository/database.

Response-test artifacts additionally identify the action, method/operation, delay
mode and placement, backend acknowledgement guarantee, occupied resources,
deadline/retry policy, batch/concurrency settings and whether server completion
preceded caller timeout. Start a caller-response sweep at 0/1/8/25/100/250 ms
of added delay, independently of the profile's network RTT. Compare constant
latency and jitter distributions with the same mean, then long/correlated tails.
Vary concurrency and batch size in separate comparisons. Classify deadline-limited
runs separately from successful throughput measurements; report completions,
timeouts/retries and unfinished work rather than hiding failures in an average.
Publish action-by-dependency sensitivity tables (throughput change versus added
delay), not a single aggregate Vaultic score. A sequential confirmation-per-batch
loop has a latency-only ceiling of batch size divided by confirmation latency;
use that as a sanity check, not a forecast for concurrent/shared-resource work.

Establish no-injection and instrumentation-disabled baselines. Vary one parameter
before combining latency, jitter and contention; compare at least three repeats
and report variance and instrumentation overhead. Include cold/warm caches and
runs long enough to expose flush/compaction, identifying warm-up and finalization.
Growing backlog is not steady state. Report elapsed completion, logical throughput,
p50/p95/p99 with histogram bucket bounds, wait worker-time, active wait ages,
queues, CPU/memory, request counts/bytes, cache behavior and injected versus
observed latency. Rank opportunities by measured end-to-end sensitivity, such
as halving one role's latency, not the largest summed wait counter. Correctness,
durability, bounded resources, cancellation cleanup and attribution to the
injected boundary are mandatory; missed performance targets remain findings.

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

Define one shared telemetry schema and small instrumentation library per implementation language, not a second metrics vocabulary in each command. Each long-running component exposes an authenticated local status API over its existing protected control channel. Short-lived `vaultic` commands can expose an ephemeral in-process collector to the aggregate monitor or emit a final snapshot. Remote status follows the component's existing authentication and least-privilege rules; Phase 40 later extends this access to enrolled remote principals.

Collection has configurable timeouts, maximum response size, maximum active-operation records, histogram count, and refresh frequency. Metrics collection and export have separate bounded queues. On overflow, coalesce gauges, preserve counters through the next successful snapshot where possible, drop distribution intervals or events according to documented policy, and expose dropped-export counters. Telemetry failure never blocks backup, restore, database durability, cache reads, or broker lease handling.

## InfluxDB v2 and future exporters

Keep the collector independent from any monitoring vendor. Define an exporter interface over schema-versioned snapshots, then provide an opt-in InfluxDB v2 exporter using its HTTP write endpoint. Configuration includes URL, organization, bucket, token file or protected environment source, export interval, batch limit, timeout, TLS trust, and bounded retry/backoff. Tokens are never accepted as command-line flags, returned by status, or written to logs.

Export gauges, counter deltas with reset markers, and histogram buckets or agreed quantiles using low-cardinality tags. A deployment ID and process start ID distinguish restarts without creating one series per operation. Active-operation details and arbitrary IDs are not exported as metric tags; export counts and oldest ages by class, with lifecycle details going to the existing structured event path. The exporter keeps only one bounded retry batch or spool budget and drops oldest telemetry when exhausted. It must never write monitoring history into VaulticDB, the repository, a WAL, or a read-cache tier.

Prometheus/OpenTelemetry or other exporters may be added against the same snapshot contract later. The InfluxDB implementation must not leak its naming, retry, or authentication model into instrumentation call sites.

## Security and operational constraints

Monitoring is read-only and follows existing local socket ownership and remote authentication boundaries. Summary views may expose backend aliases, capacity, performance, and operation classes, which are operationally sensitive even without secrets. Redact credentials, key material, object keys, paths, repository object IDs, user-controlled error text, and broker principal details. JSON output carries a schema version and explicit units.

Default telemetry memory is a documented fixed budget per component and scales only with configured backends, cache targets, operation classes, and histogram definitions, all with hard caps. Cardinality-overflow, active-registry-overflow, stale-snapshot, dropped-event, and dropped-export counters make loss visible. Disabling live windows or export leaves correctness and point-in-time accounting intact.

## LLM-executable implementation steps

All steps start pending. Execute one named step or boundary substep per session;
inspect the current worktree and owning tests first. Before editing, state one
falsifiable hypothesis, smallest change and focused check. Run that check directly
after editing, then required owner tests. Do not combine instrumentation with
algorithm tuning, protocol changes or dependency upgrades. Do not touch live
repositories/services or publish changes without authorization. On a failed gate,
repair only the touched slice or report a blocker; do not advance or change defaults.
Use existing test helpers and select actual test names after inspection, not
invented commands. Each handoff records owners, revisions, exact commands/results,
artifacts, semantics/overhead, gaps and the next prerequisite. No performance
acceptance may be inferred from a small synthetic correctness fixture.

### M0. Freeze the shared contracts

**Prerequisite:** this spec and Phase 32 P0/P1 evidence. Inspect existing Go
import/daemon attribution, `vaulticdb/src/attribution.rs`, storage/service metrics
and Phase 9/12/22/28/29/30/31 status sources. Define versioned metric names, units,
scope/outcome semantics, bounded labels/buckets, reset/unavailable states and
profile/artifact fields. Preserve compatible existing counters or version changes.
**Gate/handoff:** schema fixtures reject unknown/invalid profile parameters and
unbounded labels, preserve explicit unknowns and distinguish S3 RTT from service
latency. Publish the owner-to-metric map and artifacts consumed by M1/M2.

### M1. Implement bounded accounting primitives

**Prerequisite:** M0. Extend existing Go and Rust attribution owners one language
at a time: duration/hold guards, gauges, bounded histograms and active wait states;
add rotating windows and bounded sampled correlation as separate substeps. Use
atomics or bounded local aggregation, not a process-wide hot-path metrics lock.
**Gate/handoff:** boundary/nesting, concurrent snapshots, reset/overflow, stalled
wait, cancellation/timeout and dropped-future tests pass; measure CPU/allocation
and memory bounds with telemetry enabled/disabled. Phase 32 P2 and Phase 33 A
consume the minimum primitives without waiting for later windows/export work.

### M2. Build the isolated scenario harness

**Prerequisite:** M0 and minimum M1 primitives. Reuse existing test/benchmark
harnesses; implement profile validation/gating first, Rust object-store injection
second, Go backend/source-file injection third, then scratch and shared-capacity
modeling. Complete and test each boundary before the next. Keep delays separate
from production failure policy; do not implement a predictive simulator here.
**Gate/handoff:** disabled mode preserves results; invalid/live targets fail;
seeded assignment, streamed reads/listing, multipart completion, cache hits,
conditional writes, deadlines and cleanup pass focused tests. A controlled stall
appears at its selected role; correlated stalls share the configured capacity.
Publish reusable fixtures and profile artifacts for Phase 32 P3 and Phase 33 H.

Execute the following named substeps individually; each handoff supplies exact
owner/test names discovered in the worktree, commands/results and bounded artifacts.
Existing wrapper coverage may satisfy a gate only with recorded evidence.

| Substep | Prerequisite and bounded implementation | Gate/handoff |
|---|---|---|
| M2a: profile and target validation | M0/minimum M1. Extend the existing harness with mode, method/role, placement, resource-holding semantics and test-target gates. | Invalid/ambiguous modes fail; disabled behavior is unchanged. Publish profile fixtures and explicit unknown guarantees. |
| M2b: vaulticdb dependency service | M2a. Wrap existing Rust object-store boundaries one role at a time, including real WAL persistence and stream/multipart completion. | Controlled stalls distinguish persistence from submit and shared queueing; unchanged conditional writes, durable handles, cancellation and replay. Hand off real-engine fixtures to P3b. |
| M2c: Vaultic data dependencies | M2a. Extend Go backend/source-file wrappers, then scratch and shared-capacity behavior as separate changes. | Streaming, cache hits, operation completion guarantees and bounded cleanup pass; no injected origin delay on cache hits. Publish owner-scoped fixtures. |
| M2d: Vaultic RPC response delivery | M2a and existing daemon-client tests. Add a test-only method-selective post-response delivery wrapper, then separate server-service hooks only where required. | Barrier tests prove underlying completion before delayed delivery; client wait rises without artificial server hold time. Preserve RPC results/errors, stream bounds, deadlines and cancellation. Publish mode-specific attribution evidence for Phase 32/33. |
| M2e: ambiguous successful responses | M2d and existing real-daemon receipt/idempotency tests. Withhold a successful commit response until deadline, then test lost delivery and normal recovery independently. | Same-identity retry/recovery neither republishes nor loses committed state; no success claimed for incomplete work. Compare failure-before-execution and delayed apply-only acknowledgements. Publish exact final-state and recovery assertions. |
| M2f: action adapters and response sweeps | Relevant M2b-M2e gates. Reuse Phase 32/33 adapters; add backup, restore and forget/prune individually at the owners above. Freeze each fixture/oracle before its no-delay baseline and one-boundary sweep. | At least three repeats, identical successful result/coverage and bounded memory/queues; publish action-by-dependency sensitivity, variance and timeout classification. Missing adapters/infrastructure remain gaps. Never combine an adapter with pipeline optimization. |

### M3. Wire one production boundary at a time

**Prerequisite:** M1; reuse Phase 32 P2 and Phase 33 A coverage without duplication.
Extend authoritative placement/data-pack transfer, source I/O, WAL/object stores,
queues, caches, broker/confirmation waits, VFS/FUSE and NFS operations in separate
owner-scoped substeps. Reuse engine statistics; use a pinned fork change only
for proven missing internal queue/service observations. No scheduling changes.
**Gate/handoff:** focused owner tests and isolated M2 injection attribute each
stall correctly, preserve outcomes and resource bounds, and disclose unavailable
measurements. Publish the coverage map and measured overhead before proceeding.

### M4. Expose authenticated snapshots

**Prerequisite:** M0/M1 and each exposed M3 boundary. Implement component status
endpoints and native snapshot commands one component at a time over existing
protected channels, with bounded responses, timeouts and redaction.
**Gate/handoff:** golden JSON, authorization, compatibility, stale/unavailable,
reset and collector-timeout tests pass without blocking data-path work.

### M5. Build aggregate monitoring and terminal views

**Prerequisite:** M4. Deliver aggregate JSON snapshots first, then counter/window
derivation, then terminal views and explicit asynchronous reconciliation as
independently tested substeps. Do not make inventory scanning a refresh action.
**Gate/handoff:** deterministic snapshots verify rates, bucket-derived percentiles,
missing components, narrow terminals, resize and cancellation; retention remains
bounded regardless of attachment. Publish operator examples with real output.

### M6. Add optional export

**Prerequisite:** M4's snapshot contract. Implement the vendor-neutral exporter
interface, then opt-in InfluxDB v2 batching/authentication and bounded retry.
**Gate/handoff:** unavailable/slow exporters, overflow, counter resets, redaction
and secret handling pass; blocked export has no data-path dependency. Record
memory/CPU and drop/staleness behavior under maximum configured cardinality.

### M7. Publish cross-workload evidence and playbooks

**Prerequisite:** M2/M3 and Phase 32 P3/P7 plus Phase 33 H evidence as available.
Run all three shared profiles for import and checking, distinguish synthetic
injection from representative hardware, and publish sensitivity/overhead results.
Include M2f's independently validated action/response matrices as they become
available. Identify untested backup/restore/maintenance actions explicitly; import
and check results cannot certify pack-upload or destination-write sensitivity.
**Gate/handoff:** artifacts satisfy the experiment contract and workload correctness
gates; missing infrastructure is an explicit acceptance gap, not a passed gate.
Document lock-versus-I/O causality, WAL/compaction/cache diagnosis and uncertainty.
A predictive simulator requires a separate scoped design, calibration and
held-out validation; it is not required to complete instrumentation or monitoring.

## Tests

- Wait-accounting tests cover exclusive submit/durability scopes, concurrent worker-time, immediate/contended acquisition, hold time, stalled waits and cancellation/timeout/drop cleanup without changing scheduling.
- Scenario tests cover profile/gating validation, seeded assignment, streams and multipart completion, cache bypass of origin delay, shared capacity/correlated stalls and unchanged conditional-write/durability semantics. Reuse Phase 32/33 workload oracles and the shared artifact contract for all three profiles.
- Response tests distinguish service, true durability and post-completion delivery delay, method/caller targeting and resource hold time. Prove timeout-after-success with barriers, same-identity idempotent recovery, bounded delayed streams and preserved backend completion guarantees. Each action adapter retains its own correctness oracle and sensitivity evidence.
- Unit tests prove counter concurrency, gauge replacement, histogram rotation and bucket boundaries, operation-registry overflow, label allowlists, reset detection, and fixed memory bounds.
- Golden JSON tests pin schema version, names, units, unavailable/stale states, and redaction. Compatibility tests allow an older client to ignore newer fields and reject an unsupported major schema safely.
- Integration tests inject cloud latency, retryable errors, bandwidth limits, WAL flush stalls, compaction pressure, full queues, cache misses, cache corruption, quota shrink, and slow NFS/FUSE operations; each condition must appear under the correct backend, queue, cache, and throttle reason.
- Accounting tests compare pack placement, SlateDB object, and cache totals with controlled local, S3, and librados inventories and verify exact/estimated/stale markers plus asynchronous reconciliation.
- TUI tests use deterministic snapshots to verify rate and percentile calculations, counter resets, stale components, narrow terminals, resize, pause, and clean shutdown without increasing daemon retention.
- Load tests demonstrate bounded telemetry CPU, allocation rate, memory, response size, and exporter queues at maximum supported backend/cache counts and request concurrency. A blocked or unavailable InfluxDB endpoint cannot affect data-path latency or durability.
- Security tests verify status authorization and prove paths, object IDs, credentials, keys, tokens, user error strings, and unbounded labels do not appear in metrics, JSON, terminal output, logs, or InfluxDB tags.

## Exit criterion

Operators can use snapshot commands or a live terminal dashboard to identify where repository packs and SlateDB objects reside, what work is active or queued, which limit is applying backpressure, how quickly each backend is moving data, whether WAL/checkpoint behavior is throttling commits, and whether each SlateDB or repository read cache is saving origin traffic relative to its occupied space. Point-in-time state and lifetime aggregates remain cheap when nobody is watching; recent latency and throughput windows use fixed memory; detailed completed-operation timelines are not retained locally. An optional InfluxDB v2 exporter can persist low-cardinality time series without putting secrets, arbitrary identifiers, monitoring history, or unbounded queues into Vaultic, VaulticDB, the key broker, or repository storage.