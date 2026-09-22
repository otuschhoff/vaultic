# Phase 33: index check scalability & performance

[← Back to roadmap index](00-overview.md)

[← Phase 32](phase-32-scalable-legacy-metadata-bulk-import.md) · [Phase 34 →](phase-34-operational-monitoring-and-metrics-export.md)

**Status: bounded implementation complete; production NFS validation reaches
the SlateDB location scan; representative scale acceptance pending.**

The bounded checker implementation is available on the development branch. It
includes pinned serializable read sessions, immutable legacy inventory fencing,
memory-first canonical sorting with encrypted disk overflow, bounded
pack/reference/snapshot/path and analytics reductions, deterministic finding
selection, global worker and RPC limits, and stage progress. Synthetic profiling
has already driven spool/sort, audit-I/O, scan-page, key-range parallelism, and
scratch-I/O optimizations. Production NFS runs removed the encryption-audit
blocker and showed that the current paged SlateDB location scan still exceeds
the ten-minute feedback window. The 50/500 GB NFS, native RADOS, and S3
acceptance matrix remains open.

**Goal:** make the complete `vaultic index check` practical at ten times the
current metadata scale, with bounded working memory, exact results, observable
progress, and useful parallel execution across available CPU cores. The scale
reference is a roughly 50 GB SlateDB growing to roughly 500 GB, not a claim
that physical database bytes alone predict runtime or memory requirements.

## Current implementation and constraints

The controlling implementation is
[CheckWithOptions](../../../internal/index/maintenance/maintenance.go), called by
[the index CLI](../../../cmd/vaultic/indexcmd/cmd_index.go).
The current pipeline inventories and scans legacy locations, audits metadata
encryption, scans SlateDB locations and packs, then checks catalogs, aggregates,
placements, verification state, operational state, export provenance,
references, snapshots, path versions, and analytics. These top-level stages run
sequentially; eligible work inside legacy decoding and independent validators is
parallel.

Current scale-dependent costs and blockers:

- Canonical location, pack-contribution, reference, snapshot, placement,
  path-version, and analytics sets now use byte-budgeted memory runs with
  encrypted external overflow. The pre-Phase-33 formatted-string maps and
  unbounded per-blob sets are no longer the production implementation.
- Memory-first spooling, direct tuple comparison, and fixed-capacity runs removed
  the dominant synthetic encrypted-I/O, comparison, and slice-growth costs.
  Remaining synthetic CPU/allocation ownership is concentrated in legacy JSON
  decode, packed-blob conversion, runtime scanning/GC, and shared legacy-spool
  insertion.
- Metadata encryption remains a monolithic server operation, but the checker now
  applies a dedicated one-hour default audit deadline without replacing a caller
  deadline. Initial classification reads only the bounded 38-byte envelope
  header before one authenticated full read. Production audits complete in
  roughly 48 seconds to 2 minutes 24 seconds. A paged/resumable audit remains the
  durable observability and cancellation design.
- Production `--check-memory=auto` can admit nearly all reclaimable memory. The
  first partial production run admitted 260.3 GiB and peaked at 93.7 GiB RSS
  without spilling. This is bounded by configuration but requires an explicit
  production budget and a measured memory/spill curve.
- Snapshot validation still issues root and checkpoint point reads. Related
  checkers need production measurement for repeated scans, per-record RPCs, and
  response volume after the encryption blocker is removed.
- Maintenance requests pages of 10,000 entries and the daemon now advertises the
  same item limit; the 16 MiB message limit remains the effective byte boundary.
  Blob locations are split into 256 exhaustive first-ID-byte prefixes and scan
  concurrently through the pinned read transaction. Each partition owns bounded
  spools, avoiding shared insertion serialization, and transfers its sorted runs
  to the downstream exact reducers without copying tuples.
- The production location scan still rebuilds a SlateDB iterator for every unary
  response. At roughly 380 million blob records this causes hundreds of GiB of
  logical reads and millions of NFS metadata operations. Raising concurrency
  from 32 to 64 did not improve completed output; a leased server-side range
  stream or resumable scan cursor is the next required architectural change.
- [Crawl-debt handling](../../../internal/index/maintenance/check_helpers.go)
  scans pending debt even without `--include-crawl-debt`; that flag controls
  detailed findings, not coverage. `--max-findings` caps retained details, not
  work. `--slatedb-only` skips legacy comparison and export-provenance checks;
  it is not a substitute for migration validation.

The observed deployment uses separate NFS-backed source and database mounts;
the database path is a symlink. The first warm production run averaged 86.94%
host CPU idle and 0.11% I/O wait during the legacy stage, so NFS latency was not
the dominant limiter in that run. This does not characterize cold cache or later
SlateDB stages. Continue to measure CLI and daemon CPU, memory, network, and
storage separately.

## Correctness and coverage contract

Preserve all existing full-check domains, result counters, warning distinctions,
and exit behavior. Default remains full differential validation. Keep legacy-only
and SlateDB-only explicitly identified as reduced coverage. Sampling, count-only
checks, digests, or Bloom filters cannot certify equality. An accelerator may
reject unequal inputs early, but exact comparison is still required for success.

Pin the current semantics with fixtures before changing algorithms: location
equality includes blob ID, pack ID, type, offset, stored length, and uncompressed
length. Duplicate source-index entries are deduplicated for equality; existing
pack-accounting multiplicity must be documented and preserved separately. Keep
empty/catalog-only packs, unresolved import checkpoints, unresolved references,
optional unbuilt tier aggregates, advisory history, and export states distinct
from corruption. Checked integer arithmetic is required for every reduction.
Any intentional correction to existing semantics needs a separate reviewed test
and documented decision, not an incidental performance change.

The checker must never certify a mixture of generations as clean:

- Establish one check-session identity: repository ID, metadata generation,
  schema, committed read sequence, coverage/options digest, and immutable legacy
  index/snapshot inventory identity. Legacy inventory must have a bounded
  representation, not an in-memory list proportional to repository size.
- First determine what stable snapshot facilities the pinned SlateDB version
  and existing RPCs actually provide. A latest-following reader is not a pinned
  read view. Separate paginated calls must not silently observe different commits.
- Preferred design: a leased read-only session pins the same generation and
  sequence for every scan and `MultiGet`, plus a coherent legacy export boundary.
  Prevent required SST/WAL objects from being reclaimed while the lease is valid;
  bound lease duration and retained storage, and report their costs.
- Initial conservative fallback: an explicit quiescent maintenance window with
  enforced writer exclusion for all relevant mutation routes. A shared repository
  lock alone does not prove that daemon metadata or compatibility exports are
  frozen. If consistency cannot be established, fail incomplete, not clean.
- Fencing, generation changes, lost sessions, or an invalid legacy inventory
  invalidate the result. Stop with a nonzero operational/incomplete outcome and
  a clear diagnostic; do not restart a cursor against newer data implicitly.

No repair, authority activation, metadata reset, forced compaction, or pack
mutation belongs in this command. Existing backend locks and any explicitly
required read-session lifecycle bookkeeping remain governed by their owning
abstractions; do not promise literally zero physical writes from a running DB.

Phase 34's writeback optimization does not authorize deferred authoritative
metadata from the checker. Check output and encrypted scratch are disposable:
they may be buffered and lost, causing a restart, unless a later authenticated
resume design durably binds them to the same input inventory, generation, read
sequence, schema and options. In contrast, the read-session lease/fence and any
retention state preventing SST/WAL reclamation are correctness state and follow
their owning durability contract. A faster check must not weaken that protection
or certify a view whose lease expired while a delayed response was in flight.

## Bounded execution architecture

### Exact external comparison and reductions

Replace string-key location maps with versioned canonical binary tuples using
fixed-width IDs and numeric fields. Legacy JSON order is not compatible with
SlateDB key order, and a blob can contain many locations: do not zip the raw
streams and assume equality. Decode into byte-budgeted runs, sort and deduplicate
them, then perform a bounded fan-in external merge with the normalized SlateDB
stream. Use external sorting for both sides where ordering has not been proven.

Fuse count, pack-type and payload reductions with tuple production when semantics
permit. Replace per-location type slices with a fixed type summary. Sort pack
contributions by pack ID and merge-join the pack catalog; do not leave a growing
per-pack hash map behind. Compute aggregate groups with bounded configured
cardinality and spill if schema dimensions can grow beyond the budget.

Merge ordered `ri:`, `rm:` and `rc:` records by blob ID after proving key ordering
and uniqueness. Otherwise externally sort canonical edge tuples and deduplicate
exactly. Even a single blob with extreme fan-out must not build an unbounded
in-memory set. Apply the same bounded join/reduction approach to snapshot
membership, snapshot-commit indexes, path-version checks, placements, backend
reverse indexes, export provenance, history, and analytics. Maintain a coverage
matrix mapping each current checker to its replacement and peak-memory bound.

Sorted runs remain in memory while they fit their assigned checker-memory share.
When the next record would exceed that share, the spool switches to encrypted
disk runs with bounded open files and merge fan-in. Disk spill remains an
explicitly bounded part of the design: check space before admission and estimate
spill amplification from measured tuple volumes and merge passes, not compressed
SlateDB size. Exceeding the disk budget must fail clearly without changing
repository data or returning a clean result.

Location tuples use direct field comparison equivalent to their canonical wire
order. Fixed-capacity memory runs are admitted using the Go tuple's actual
in-memory size, sorted independently, and merged without geometric slice growth.
This keeps retained tuple capacity within its assigned share while allowing a
fitting spool to avoid encrypted scratch entirely.

Temporary metadata can expose IDs, paths and relationships. Create owner-only
directories and files, encrypt spill runs with an ephemeral per-run key by
default using established authenticated-encryption primitives, authenticate run
headers/chunks, and never log contents or secrets. Local encrypted SSD scratch
is preferred. Cleanup is restricted to a verified session-owned directory on
success, failure and cancellation; never recursively remove a user-supplied
directory. Abrupt termination leaves identifiable encrypted orphan runs with a
documented cleanup procedure. Secure deletion on SSD/NFS is not guaranteed.
Encrypted location runs use 64 MiB bounded sort chunks with 1 MiB buffered I/O.
This reduced one ten-minute production run from about 13,200 run-file cycles to
778; it does not change the encrypted format or scratch admission accounting.

### Parallelism and backpressure

Represent the checker as a dependency graph, not one goroutine per prefix.
Inventory/decode producers feed bounded tuple queues; sort workers produce runs;
merge/reduction consumers feed independent validators. Share immutable spools
where useful to avoid rescanning the same source. A coordinator owns result
publication and cancellation; validators return local counters and bounded
finding candidates rather than racing on a shared `CheckResult`.

Parallel work candidates include legacy index read/decrypt/decode, tuple
normalization and sorting, disjoint SlateDB ranges, metadata encryption audits
where the auditor supports safe partitioning, and independent reference,
placement, snapshot, path and analytics validators after their inputs are ready.
Each is eligible only after consistency, memory ownership, and measured cost
are established. Dependent merges remain ordered; bounded parallel merge groups
may run when input partitions are independent.

Use one global CPU-worker budget derived from the Go runtime's effective CPU
availability, plus separate global in-flight RPC and byte budgets. Account for
VaulticDB and SlateDB background CPU on the same host; multiplying per-stage
worker pools by the core count oversubscribes it. Auto mode must report its
effective limits and respect container quotas. Test 1, 2, 4, 8 and available-core
settings; more workers must not be assumed faster on NFS or a saturated daemon.

Shard scans by disjoint half-open key ranges with explicit prefix boundaries,
an exclusive continuation cursor, stable read-session identity, and exhaustive
coverage. Negotiate range-scan support if current prefix APIs cannot express it.
Do not fake partitions by repeatedly scanning the whole prefix and filtering.
Skew-aware splitting uses bounded work queues and preserves exactly-once range
coverage. Batch point lookups through `MultiGet` within effective item/message
limits; deduplicate only within bounded batches. Avoid per-record RPCs where a
merge join or batch can provide equivalent validation.

Every queue is bounded in bytes as well as records. Reserve memory before
decoding or enqueueing. A single oversized JSON index, blob location list,
directory revision, or RPC response needs an explicit streaming/chunked path
or a precise nonzero resource-limit error. An item cap alone is not a memory
bound. Define fair admission so large records cannot starve forever. Cancellation
unblocks producers and consumers, cancels RPCs, closes iterators, releases read
leases and file descriptors, and waits for workers to stop before cleanup.

### Memory and cache budgets

Account for decoded records, sort buffers, queued pages, merge buffers, lookup
batches, finding samples, inventory, encryption-audit buffers and analytics.
Track CLI and daemon RSS independently and together. A Go heap setting is not a
hard total RSS limit; include native allocations and avoid swapping. Daemon read
cache has its own explicit cap with host headroom for the CLI and kernel cache.

Use the existing read-cache abstraction, not a checker-specific cache. Benchmark
cold and warm reads, cache admission/bypass, decryption CPU, NFS requests, and
scan cache pollution. Increasing RAM or relocating authoritative storage is not
required for correctness. Relocation is a separate operator procedure involving
a consistent copy and clean lifecycle; never copy a live database casually.

## Proposed operator contract

The following controls are available:

```text
vaultic index check [existing coverage options]
  --check-workers N                 # 0: quota-aware automatic CPU budget
  --check-rpc-concurrency N         # bounded across all check stages
  --check-memory BYTES|auto         # buffer budget; auto uses reclaimable memory
  --check-temp-dir PATH             # parent of an owned encrypted scratch dir
  --check-temp-max-bytes BYTES       # hard scratch admission limit
  --check-progress-interval DURATION
```

`--check-memory=auto` is the CLI default. On Linux it starts with the smaller of
host `MemAvailable` and finite cgroup headroom at any hierarchy level, counting
inactive file cache as reclaimable. When capacity exceeds twice the greater of
10% or 512 MiB, auto reserves that margin; smaller detected capacities are split
equally instead. Detection failure falls back to 64 MiB. An explicit byte value
selects a fixed checker-buffer admission budget. Neither mode is a hard process
RSS limit because runtime, decoded RPC values, and native allocations remain
outside these buffers. The effective byte value is included in progress, the
result resource report, and the options digest.

Freeze exact names, defaults and resource-limit exit mapping in Stage A after
checking repository CLI conventions. Preserve exit 0 for successful coverage,
existing fatal/workflow statuses and exit 2 for detected differences or warnings
when requested; new incomplete cases must never exit 0. Keep `--max-findings 0`
meaning all details, but spool/stream those findings instead of retaining them.
For finite limits, deterministically select findings by a canonical total order;
all counters still reflect every finding regardless of worker completion order.

Progress reports inventory, encryption audit, scans, sorts, joins, validation
and finalization separately: records/bytes read, total when known, effective
workers/pages, RPC counts/latency, queue wait, CPU time, memory high-water, scratch
bytes, merge passes, cache hits and origin bytes. Unknown totals have no invented
percentage/ETA. Send rate-limited progress to stderr so the final JSON result
remains valid. Report coverage and consistency identity with the final summary.
Reuse bounded telemetry primitives; the next monitoring phase may consume this
data but is not a prerequisite for implementing the checker.

Use the [Phase 34 wait-state contract](phase-34-operational-monitoring-and-metrics-export.md#wait-state-accounting-contract)
and its M0-M2 foundation, delivered incrementally with Phase 32, rather than
checker-specific accounting or injection libraries. Monitor commands/exporters
remain independent. Track per-operation waits for scan pages and batched lookups,
source reads, lock acquisition/hold, RPC admission/service, memory/byte budgets,
scratch reads/writes and merge prerequisites. Measure active wait ages as well
as completed distributions; preserve cancellation/timeout outcomes. Worker-time
and nested RPC/backend spans are not additive stage wall-time. CPU/runtime
profiles complement, rather than replace, explicit prerequisite measurements.

Durable resume is deferred. Baseline cancellation starts a fresh session on the
next invocation; cursors alone cannot safely resume a multi-source check. A later
resume design must authenticate encrypted scratch/checkpoints, validate identical
input inventory, generation, read sequence, schema and options, and retain the
necessary read view. No cached previous success can substitute for this run.

Interrupted runs can leave directories named `vaultic-check-*` below the
configured `--check-temp-dir`. Their runs are authenticated and encrypted with
an ephemeral key that is not persisted, so they are not resumable. After
confirming that no `vaultic index check` process owns the directory, remove that
specific directory. The checker only recursively removes a directory whose
private ownership marker matches the current session; it never recursively
removes the configured parent.

## Implementation evidence

Implemented ownership and bounds:

- `daemon.ReadSession` routes scans and point reads through one serializable
  transaction and validates repository, generation, decision, and committed
  sequence before a successful result is returned.
- Location equality, pack contribution multiplicity, references, snapshot
  membership, snapshot-commit indexes, placements, path versions, analytics
  dictionaries/materializations/GDPR expectations, and legacy inventory retain
  fitting sorted runs in bounded memory and overflow to encrypted external runs.
  Merge fan-in is capped at 32.
- Scratch admission has a hard byte limit, owner-only permissions, authenticated
  AES-GCM records, unique nonce namespaces, cancellation checks, and ownership-
  checked cleanup. Oversized records and exhausted scratch return errors.
- `--check-workers`, `--check-rpc-concurrency`, `--check-memory`,
  `--check-temp-dir`, `--check-temp-max-bytes`, and
  `--check-progress-interval` are wired into the options digest and final
  resource report. Independent validators publish private results and merge in
  a fixed order; tests prove parity at 1, 2, 4, and 8 workers.
- Finite finding limits retain the canonical prefix while all mismatch counters
  continue to cover the full input. Explicit `--max-findings=0` retains all
  requested output and therefore remains output-size proportional by contract.

Validated locally:

```text
go test ./internal/index/schema ./internal/index/pathindex ./internal/repository/index \
  ./internal/index/analytics ./internal/index/maintenance ./cmd/vaultic/indexcmd \
  ./internal/index/daemon
go test -race ./internal/index/maintenance ./internal/index/pathindex \
  ./internal/index/analytics
```

Both commands pass. `go test ./...` reaches and passes every Phase 33 owner but
the repository-wide run is currently red because the unrelated
`internal/archiver/TestArchiverErrorReporting/file-unreadable` test panics.

Not available in this workspace: representative 50 GB and 500 GB fixtures,
dedicated HDD-array NFS, native three-replica RADOS, S3 at an independently
controlled 8 ms RTT, and three-repeat cold/warm performance experiments.
Consequently the H1-H4 memory, scale, CPU-scaling, and latency targets remain
pending external execution; no synthetic or small-fixture result is presented
as satisfying those gates.

The [local synthetic benchmark evidence](phase-33-local-benchmark-evidence.md)
records three-repeat 1x/10x worker and memory-budget controls, matched local/NFS
scratch runs, exact result digests, failure checks, and symbolized CPU, block,
mutex, heap, and runtime-trace findings. It identifies encrypted spill I/O,
tuple merge/allocation work, and shared legacy-spool contention as the dominant
local costs. It also records that four-worker throughput improved only 5.8% on
the synthetic 10x fixture, so the CPU-scaling gate remains open.

The [production NFS benchmark evidence](phase-33-production-benchmark-evidence.md)
records the initial full-check attempt and the subsequent bounded optimization
runs against the activated 46 GiB authority. Legacy inventory and scan complete
in about 5m11s for 10,019 index files and 379,934,385 imported blobs. The fixed
encryption-audit deadline and duplicate full-object read are resolved; production
audits now complete and the checker reaches `slatedb_scan`. Reduced-coverage
diagnostics show that 256 concurrent key ranges and independent spools remove
client insertion serialization, while raising concurrency from 32 to 64 does not
increase completed scan output. No run has completed the SlateDB scan, catalog
join, or later validators. The evidence remains partial, not a clean check or an
acceptance pass.

## Consolidated state and next steps

Implemented and retained:

- pinned serializable read sessions and immutable legacy inventory fencing;
- exact bounded reducers and encrypted external spools;
- a dedicated encryption-audit deadline and bounded header classification;
- a 10,000-item scan limit with the 16 MiB response cap retained;
- 256 exhaustive first-ID-byte scan ranges with independent bounded spools;
- 64 MiB sort chunks and 1 MiB buffered encrypted run I/O;
- cancellation cleanup, production writer safety, and reduced/full coverage
  labeling.

The remaining production bottleneck is the unary paged SlateDB location scan.
Every page recreates an iterator inside the pinned transaction. At approximately
380 million blob records this repeatedly traverses table metadata, producing
hundreds of GiB of daemon logical reads and millions of NFS metadata operations.
The best measured setting is 32 workers/RPCs; 64 did not improve completed
output. Host I/O wait remained below 1%, so more workers, larger checker memory,
larger scratch limits, and further item-page growth are not supported as primary
optimizations.

Execute the remaining work in this order:

1. Add a server-owned resumable range scan or server-streaming RPC. Keep one
  SlateDB iterator alive across bounded response chunks and bind it to the
  existing read-session identity, lease, generation and cancellation contract.
  Enforce response-byte and in-flight-byte limits, cursor expiry, exhaustive
  range coverage, and compatibility negotiation.
2. Add scan-local telemetry before production tuning: records, encoded bytes,
  pages/chunks, iterator setup and service time, table/object reads, continuation
  age, and per-range progress. Preserve separate client delivery, RPC admission,
  daemon service and object-store timing.
3. If persistent scans expose range skew, split only slow first-byte ranges into
  bounded second-byte subranges. Do not add more partitions while each partition
  still pays unary iterator reconstruction.
4. Consider server-side projection of canonical location tuples after the stream
  contract is proven. It may reduce protobuf volume, Go allocation and duplicate
  blob-record decoding, but must preserve every exact location field and remain
  versioned/negotiated.
5. Rerun the ten-minute SlateDB-only diagnostic at 32 workers, then run one full
  differential check. Only after `slatedb_scan` completes should catalog join
  and later validators be profiled and optimized.
6. Optimize the approximately 5m11s legacy stage next by giving decode workers
  independent producer spools and profiling JSON/index decode and packed-blob
  conversion. Preserve the final immutable-inventory confirmation.
7. Replace the interim monolithic encryption audit with a paged/resumable audit
  tied to the same stable physical inventory contract. This is still desirable
  for progress and bounded cancellation, but it is no longer the critical path.
8. Complete repeated 32/64/96 GiB memory curves and the NFS/RADOS/S3 acceptance
  matrix only after a full exact check can finish. Full differential success,
  stable consistency identity and exact result digests remain the sign-off gate.

## LLM-executable implementation stages

Execute one stage at a time. Each handoff records touched owners, invariants,
commands and results, benchmark artifacts, remaining limitations and the next
stage's input contract. Keep patches reviewable; do not combine an algorithm
rewrite with protocol changes or unrelated import tuning. Stop on a failed gate.

For each named substep, inspect its current owner and neighboring tests, state
one falsifiable hypothesis and smallest check, edit only that boundary, then run
the focused check immediately and the owner suites before advancing. Reuse existing
fixtures and choose real test names after inspection. Preserve unrelated work;
never use a live repository, service or production cache for fault injection.
Record pending/in-progress/blocked/validated status, exact revisions/commands,
artifact paths, unmet gates and the next prerequisite in each handoff. The new
substeps below start pending; documentation is not implementation evidence.

### A. Freeze semantics and measure the existing pipeline

Add a coverage matrix and oracle fixtures in the existing maintenance tests,
plus bounded stage timing/counters and a deterministic benchmark fixture builder.
Record actual scan limits and all whole-input collections, including subordinate
checkers and daemon encryption audit. Distinguish worker wait from RPC service
and backend wait. Measure unmodified results, RSS, allocation and CPU profiles.

Execute A1-A4 independently, without changing checker algorithms:

| Substep | Prerequisite, owner and bounded change | Focused gate and handoff |
|---|---|---|
| A1: freeze the oracle | Existing maintenance/analytics checkers and tests, daemon schema scans and index CLI. Build deterministic coverage/expected-result fixtures and record effective page limits and whole-input collections. | Clean/corrupt/duplicate/skew/unresolved and reduced-coverage fixtures pin verdicts/counters. Publish input/result digests, coverage map and exact baseline commands. |
| A2: measure read prerequisites | A1 and Phase 34 M0/minimum M1. Instrument existing scan/lookup, source filesystem, RPC and daemon object-store boundaries one owner at a time. Reuse Phase 32's server instrumentation. | Controlled delayed pages/lookups/source reads produce the expected inclusive/exclusive timing, active ages and outcomes without changing results. Publish timer boundaries, request/byte counts and cold/warm baseline artifacts. |
| A3: measure queues and scratch | A2. Instrument existing admission/queues where present; define hooks consumed by C/F for byte reservations, run creation, merge input and scratch transfer. Do not invent a queue to measure it. | Bounded snapshots, hold-versus-wait timing, cancellation and overflow tests pass. Mark future or unavailable boundaries explicitly, not as zero waits. C/F must wire and test their hooks before claiming coverage. Publish memory/overhead evidence with telemetry enabled/disabled. |
| A4: prove isolated injection | A2/A3 and relevant Phase 34 M2 wrappers. Extend existing small-fixture harnesses for source, database and scratch roles; activate scratch tests when C implements it. | No-injection parity plus delayed/stalled stream, deadline and cancellation checks identify the intended role with bounded cleanup. Use immutable/quiescent fixture inputs before B; do not claim coherent concurrent checking yet. Publish reusable scenario commands and unresolved hooks. |

**Gate:** clean, corrupt, duplicate, unresolved and reduced-coverage fixtures pin
all verdicts/counters; instrumentation does not alter results. Publish baseline
artifacts and proposed resource/default contracts before choosing tuning values.

Complete A4-response as a separate pending substep after Phase 34 M2d: reuse the
daemon-client delivery wrapper to delay selected scan-page and batched-lookup
responses after successful service, independently of object-store read delay.
Use barrier-driven immutable/quiescent fixtures before Stage B; assert exact
result parity, unchanged server service timing, bounded delayed pages and prompt
caller timeout/cancellation. Once B exists, a response delayed past read-session
expiry must fail incomplete rather than silently retry against a newer view.
Hand off response-mode fixtures, commands and timer boundaries to H2-response.

### B. Establish coherent check sessions

Implement and test the stable-read contract in the owning Go/Rust RPC and daemon
layers, or deliver the enforced quiescent fallback first. Inventory legacy
sources at the same publication boundary. Include capability negotiation,
lease limits, fencing, expiry and cleanup; retain compatibility or reject older
daemons explicitly rather than silently falling back to inconsistent reads.

**Gate:** concurrent backup/export/forget and daemon mutation tests cannot create
false clean or false corruption results. Compaction, restart, generation switch
and lease-expiry tests prove no mixed-view continuation or leaked retention.

### C. Build bounded tuple and encrypted spill primitives

Add canonical encoders, byte admission, external runs, bounded fan-in merges,
exact deduplication and deterministic findings. Reuse repository crypto and test
utilities; avoid new packages unless ownership warrants them. Define spool
interfaces consumed by the next stage without changing default check behavior.

**Gate:** round-trip/order tests, chunk corruption, ENOSPC, short writes,
oversized records, skew, cancellation and descriptor-leak tests pass at tiny
budgets. Runs larger than RAM merge correctly; scratch is never mistaken for
authoritative metadata and cleanup cannot escape its owned directory.

Complete A3/A4 scratch hooks as a separate C substep: distinguish byte admission,
scratch create/read/write/close, transfer and merge-input wait using shared
primitives. Inject delay into session-owned scratch only. The same result digest
and cleanup bounds must hold under delayed reads/writes, full queues and cancellation.

### D. Replace location and pack materialization

Move legacy/SlateDB location equality and pack/aggregate joins onto the bounded
pipeline. Retain the old algorithm only as a small-fixture test oracle, not a
production fallback that can exhaust memory. Preserve duplicate accounting and
empty-pack semantics. Expose memory/scratch controls for this slice.

**Gate:** randomized and corruption-fixture results match the oracle, including
overflow and multi-location blobs. Increasing record count at fixed budgets
increases spill, not live location/pack maps. Other unbounded check domains must
still be disclosed; this stage alone is not the 10x scalability milestone.

### E. Bound every remaining validation domain

Convert references, snapshots/commit indexes, path versions, placement/reverse
indexes, provenance, verification, history and analytics one domain per small
patch with focused tests before the next. Batch lookups or use bounded joins.
Include metadata object enumeration/decryption and unlimited findings output.

**Gate:** the coverage matrix has no unexplained whole-database collections or
unbounded per-key fan-out. Full and reduced modes pass oracle tests under fixed
budgets, including total warning counters when displayed findings are capped.

### F. Add measured parallel scheduling

Introduce global admission and the dependency graph first, then parallel legacy
decode/sort, batched reads and independent validators in separately measured
increments. Add negotiated range partitioning only when profiling shows it is
needed. Keep worker=1 as the deterministic diagnostic mode, not reduced coverage.

**Gate:** identical counters and canonical findings for 1/2/4/8 workers, page
boundaries, empty ranges and skew. Go race tests and native concurrency tests
pass. Faulted/stalled RPCs, cancellation and backpressure cannot deadlock, leak,
duplicate ranges, skip records or overshoot byte budgets.

Wire the A3 queue hooks as an independent F substep before worker tuning.
Blocked producers/consumers must report dependency versus memory/RPC admission
wait and oldest age correctly, including fair progress for oversized work.
Use barrier-driven tests to verify counters; sleep-based timing alone is not
proof of a dependency. Keep observer overhead out of tuning conclusions.

### G. Integrate CLI, progress and operational documentation

Finalize defaults using measured working sets, publish coverage/consistency and
resource diagnostics, and document encrypted scratch sizing and recovery from
interruption. Validate attach-versus-start behavior and keep JSON parseable.
Document limited versus full checks without presenting reduced coverage as a
speed optimization suitable for migration sign-off.

**Gate:** CLI integration and JSON golden tests pass for normal completion,
findings, warning policy, no space, incompatible daemon, cancellation and stdout
failures. No secrets or unbounded labels in progress. Existing scripts retain
their documented result fields and exit semantics.

### H. Execute scale and performance acceptance

Run the matrix below on dedicated infrastructure; publish reproducible inputs,
commands, binary revisions and raw artifacts. Tune one variable per experiment.
Use at least three repeats for performance comparisons and report variance.
Do not declare production-scale acceptance from scaled-down unit tests.

Use the [shared profiles and evidence schema](phase-34-operational-monitoring-and-metrics-export.md#backend-scenarios-and-experiment-contract)
for HDD-array NFS, native three-replica RADOS and S3 with the explicit assumed
8 ms RTT. Reuse Phase 34 M2 rather than adding another scenario harness. Execute:

| Substep | Prerequisite and experiment | Gate and handoff |
|---|---|---|
| H1: freeze placement and baseline | A-G and oracle/session gates. Map legacy source, repository packs, database/WAL/coordination, caches and encrypted scratch to shared or independent resources. Freeze input/read-view identity, page size and worker/budget settings. | No-injection repeats preserve result digest and coverage. Record cold/warm policy, enabled/disabled instrumentation overhead, actual RPC limits and all resource/delay boundaries. |
| H2: isolate read and scratch sensitivity | H1. Vary only one role/operation: source metadata/read, database range read/list or batched lookup, then scratch I/O. Keep worker/page/cache settings fixed for each latency comparison. | Test whether slow pages leave workers waiting on input versus RPC admission, and whether slow scratch blocks producers through byte budgets. Preserve exact verdicts and stable reads; unexpected stalls return to the missing A/C/F boundary before tuning. |
| H3: evaluate combined backend profiles | H2. Run all three profiles, adding jitter, long/correlated stalls and shared capacity, then vary workers/page sizes and cache state in separate comparisons. Include normal compaction and representative recovery/backfill conditions only in isolated authorized infrastructure. | Report throughput/elapsed, p50/p95/p99, worker-wait time, active ages, queues, origin traffic, scratch amplification, read-session retention and CPU/RSS. Growing backlog is not steady state; more workers are accepted only when end-to-end results improve within budgets. |
| H4: scale and publish decisions | H3, the 1x/10x matrix below and at least three repeats per performance comparison. Validate representative hardware independently of synthetic API-level injection. | All correctness, consistency, memory, scale and failure gates hold. Rank tuning by measured end-to-end sensitivity; publish versioned artifacts, variance, missing infrastructure/scale and accepted/rejected/inconclusive decisions to Phase 34 M7. No simulator-based prediction substitutes for a measured gate. |

H2-response is a pending independently executable extension of H2, requiring
A4-response, Stage B and Phase 34 M2d. Follow the
[shared response-delay contract](phase-34-operational-monitoring-and-metrics-export.md#synthetic-dependency-responses):
keep storage, page size, cache and worker limits fixed, then vary only post-service
scan-page delivery or batched-lookup delivery at 0/1/8/25/100/250 ms added delay.
Compare with the separate object-store service-delay experiment before combining
them. Test constant and equal-mean jitter distributions, then correlated tails;
vary page size and worker concurrency only in subsequent matched comparisons.

**Gate/handoff:** at least three repeats preserve exact verdict/counter digest and
read-session identity; delayed pages remain bounded and cannot reorder/skip ranges.
Report caller input/admission wait separately from daemon service/backend wait,
throughput, p95/p99, queue ages, memory and lease retention. Deadline/lease-expiry
runs must be classified incomplete, not slower successful checks. Publish the
action-by-method sensitivity artifact to H3/H4 and Phase 34 M7. Checker read
results do not certify restore destination writes or backup durability behavior.

## Benchmark matrix and acceptance gates

Build deterministic 1x and 10x fixtures with measured physical sizes near 50 GB
and 500 GB, holding realistic record/value distributions. Report both logical
cardinalities and physical compressed sizes: locations per blob, packs, indexes,
snapshots, edges, directories, path versions, history and analytics. Grow each
domain independently as well as the combined dataset; include highly skewed
fan-out and duplicate-heavy legacy indexes. Inject corruption in every domain
and near range/page boundaries with known expected counts.

Compare the old checker at 1x where feasible, the new checker at worker=1, and
2/4/8/quota-available cores. Keep local SSD as a control; test the three shared
backend profiles with representative NFS, native three-replica RADOS and S3
metadata, local scratch plus separately delayed scratch, cold/warm cache,
encryption enabled, daemon cache limits,
different effective RPC page sizes, and normal background compaction. Keep
durability, coverage and source identity identical across comparisons. Never
drop production caches or reset a production database to obtain a cold run.

Record wall time by stage and end-to-end; user/system CPU for both processes;
CPU/GC profiles; combined and separate peak RSS; swap activity; scratch peak and
total I/O; RPC counts, bytes and latency distributions; backend requests/bytes;
queue occupancy/wait; cache behavior; read-session retention; and result digest.
Report throughput per logical unit rather than dividing DB size by wall time.

Release gates, to be measured rather than asserted:

- **Correctness:** exact verdict/counter parity on all oracle fixtures at every
  concurrency and budget setting; deterministic limited findings; unchanged
  coverage. Any oracle defect is resolved explicitly before accepting parity.
- **Memory:** no algorithmic allocation proportional to total input cardinality.
  At identical worker, CLI and daemon cache budgets, the 10x combined peak RSS
  is at most 1.25 times the new checker's 1x peak, with no sustained swapping.
  Report fixed/native overhead separately and investigate any limit violation.
- **Scale:** the complete 10x run succeeds without raising the RAM budget or
  omitting validators. For matched hardware, distributions and cache conditions,
  target at most 15x wall time versus new 1x; include spill and merge overhead.
- **CPU scaling:** on a CPU-bound fixture large enough to amortize startup,
  target at least 2x end-to-end throughput at four workers versus one, with
  identical coverage and cache state. Saturated NFS may not meet that ratio;
  report the bottleneck and plateau rather than claiming linear core scaling.
- **No regression:** representative new single-worker 1x runs target no more
  than 10% slowdown versus the old checker where it completes within memory.
  A missed target needs evidence and an explicitly reviewed tradeoff, not hidden
  reduced coverage. OOM of the old checker is a limitation, not a speedup number.
- **Failure safety:** insufficient scratch/RAM, malformed input, lost read view,
  interrupted JSON output, daemon failures and cancellation all yield nonzero
  outcomes, bounded cleanup and no repository mutation or false success.

Use focused Go tests in `internal/index/maintenance`, `internal/index/analytics`,
`internal/index/daemon`, and `cmd/vaultic/indexcmd`, race runs for touched Go
concurrency, and targeted Cargo tests for any native session/scan changes.
Select exact test names after each stage adds them; do not invent existing test
commands. Record required build and CI gates with the stage handoff.

## Exit criterion

A full differential check of the 10x dataset completes with fixed memory budgets,
exact results, bounded secure scratch, coherent reads, and documented performance
on the local control and three shared backend profiles, clearly separating
synthetic injection from representative hardware and disclosing missing gates.
Available cores improve eligible CPU-bound work without
unbounded RPCs or nested worker pools. Operators can see progress and resource
pressure, distinguish incomplete work from clean results, and reproduce the
acceptance measurements. The feature remains design-only until these gates pass.