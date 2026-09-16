# Phase 33: index check scalability & performance

[← Back to roadmap index](00-overview.md)

[← Phase 32](phase-32-scalable-legacy-metadata-bulk-import.md) · [Phase 34 →](phase-34-operational-monitoring-and-metrics-export.md)

**Status: design specification, not yet implemented.**

**Goal:** make the complete `vaultic index check` practical at ten times the
current metadata scale, with bounded working memory, exact results, observable
progress, and useful parallel execution across available CPU cores. The scale
reference is a roughly 50 GB SlateDB growing to roughly 500 GB, not a claim
that physical database bytes alone predict runtime or memory requirements.

## Current implementation and constraints

The controlling implementation is
[CheckWithOptions](../../../internal/index/maintenance/maintenance.go), called by
[the index CLI](../../../cmd/vaultic/indexcmd/cmd_index.go).
The current pipeline loads legacy locations, audits metadata encryption, loads
SlateDB locations and packs, then checks catalogs, aggregates, placements,
verification state, operational state, export provenance, references, snapshots,
path versions, and analytics. These top-level stages run sequentially; that
does not mean every backend or legacy loader operation is single-threaded.

Concrete sources of scale-dependent cost:

- `loadLegacyLocations` and `loadSlateDBLocations` retain entire location sets
  as formatted string keys. Both sets coexist during differential comparison.
- `packLocationStats` retains a type entry per location as well as pack count
  and payload maps. `loadPacks` retains the catalog; aggregate checking copies
  it into another slice. Optimizing only the differential comparison is not
  sufficient to bound the whole command.
- `checkReferences` retains distinct inode and manifest sets per blob and a
  separate reference-count map. High fan-out and skew matter as much as DB size.
- Snapshot validation issues root and checkpoint point reads. Related checkers
  must be audited for repeated scans, per-record RPCs, and full-result slices,
  including export provenance, path indexes, history, placement and
  [analytics consistency](../../../internal/index/analytics/consistency_checker.go).
- Maintenance requests pages of 10,000 entries, but
  [SchemaStore.ScanPrefix](../../../internal/index/daemon/schema_store.go)
  clamps requests to the daemon's negotiated `MaxPageItems`. RPC count and
  response bytes must be measured at the effective page size.
- [Crawl-debt handling](../../../internal/index/maintenance/check_helpers.go)
  scans pending debt even without `--include-crawl-debt`; that flag controls
  detailed findings, not coverage. `--max-findings` caps retained details, not
  work. `--slatedb-only` skips legacy comparison and export-provenance checks;
  it is not a substitute for migration validation.

The observed deployment uses an NFS-backed target behind a local-looking DB
symlink. Resolve real storage paths before attributing latency. NFS is a
candidate bottleneck, not a demonstrated explanation for elapsed time. The
earlier informal 2-6 hour estimate for a 50 GB database is not a benchmark or
an acceptance target. Measure CLI and daemon CPU, memory, network and storage
separately; process CPU percentages cannot identify wall-time ownership.

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

Disk spill is part of the design, not an emergency fallback hidden from users.
Use an explicit scratch byte budget, bounded open files and merge fan-in, and
check space before admission. Estimate spill amplification from measured tuple
volumes and merge passes, not the compressed SlateDB size. Exceeding the budget
must fail clearly without changing repository data or returning a clean result.

Temporary metadata can expose IDs, paths and relationships. Create owner-only
directories and files, encrypt spill runs with an ephemeral per-run key by
default using established authenticated-encryption primitives, authenticate run
headers/chunks, and never log contents or secrets. Local encrypted SSD scratch
is preferred. Cleanup is restricted to a verified session-owned directory on
success, failure and cancellation; never recursively remove a user-supplied
directory. Abrupt termination leaves identifiable encrypted orphan runs with a
documented cleanup procedure. Secure deletion on SSD/NFS is not guaranteed.

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

The following are proposed controls, not flags available in current binaries:

```text
vaultic index check [existing coverage options]
  --check-workers N                 # 0: quota-aware automatic CPU budget
  --check-rpc-concurrency N         # bounded across all check stages
  --check-memory BYTES              # CLI working-set admission budget
  --check-temp-dir PATH             # parent of an owned encrypted scratch dir
  --check-temp-max-bytes BYTES       # hard scratch admission limit
  --check-progress-interval DURATION
```

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

Durable resume is deferred. Baseline cancellation starts a fresh session on the
next invocation; cursors alone cannot safely resume a multi-source check. A later
resume design must authenticate encrypted scratch/checkpoints, validate identical
input inventory, generation, read sequence, schema and options, and retain the
necessary read view. No cached previous success can substitute for this run.

## LLM-executable implementation stages

Execute one stage at a time. Each handoff records touched owners, invariants,
commands and results, benchmark artifacts, remaining limitations and the next
stage's input contract. Keep patches reviewable; do not combine an algorithm
rewrite with protocol changes or unrelated import tuning. Stop on a failed gate.

### A. Freeze semantics and measure the existing pipeline

Add a coverage matrix and oracle fixtures in the existing maintenance tests,
plus bounded stage timing/counters and a deterministic benchmark fixture builder.
Record actual scan limits and all whole-input collections, including subordinate
checkers and daemon encryption audit. Distinguish worker wait from RPC service
and backend wait. Measure unmodified results, RSS, allocation and CPU profiles.

**Gate:** clean, corrupt, duplicate, unresolved and reduced-coverage fixtures pin
all verdicts/counters; instrumentation does not alter results. Publish baseline
artifacts and proposed resource/default contracts before choosing tuning values.

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

## Benchmark matrix and acceptance gates

Build deterministic 1x and 10x fixtures with measured physical sizes near 50 GB
and 500 GB, holding realistic record/value distributions. Report both logical
cardinalities and physical compressed sizes: locations per blob, packs, indexes,
snapshots, edges, directories, path versions, history and analytics. Grow each
domain independently as well as the combined dataset; include highly skewed
fan-out and duplicate-heavy legacy indexes. Inject corruption in every domain
and near range/page boundaries with known expected counts.

Compare the old checker at 1x where feasible, the new checker at worker=1, and
2/4/8/quota-available cores. Test local SSD and representative NFS-backed metadata
with local scratch, cold/warm cache, encryption enabled, daemon cache limits,
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
on local and NFS storage. Available cores improve eligible CPU-bound work without
unbounded RPCs or nested worker pools. Operators can see progress and resource
pressure, distinguish incomplete work from clean results, and reproduce the
acceptance measurements. The feature remains design-only until these gates pass.