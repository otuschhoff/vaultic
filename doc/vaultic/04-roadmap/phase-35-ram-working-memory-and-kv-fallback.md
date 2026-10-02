# Phase 35: RAM-only working memory with a pure-Go KV fallback

[Back to roadmap index](00-overview.md)

[Previous: Phase 34](phase-34-operational-monitoring-and-metrics-export.md) | [Next: Phase 36](phase-36-native-smb-backup-source.md)

**Status: M0-M2 complete; M3-M6 remain design specifications, not implemented.**

[M0 inventory, frozen contracts, and validation evidence](phase-35-m0-contract.md)

[M1 pinned bbolt selection, rejected alternative, and evidence](phase-35-m1-kv-selection.md)

[M2 pointer-light RAM implementation and validation evidence](phase-35-m2-ram-backend.md)

## Goal

Provide two interchangeable implementations of Vaultic's disposable Go-side
working state: pure RAM and a disk-backed pure-Go KV engine replacing Pebble.
Support explicit selection and a bounded RAM-first automatic fallback. Benchmark
import, check, and backup on realistic workloads, including slow vdisks, and
publish evidence-based production recommendations rather than assuming fast
local storage or that a large Go heap is inexpensive.

Pebble is already pure Go. Replacement is about suitability for temporary
working state, write amplification, operational cost, and predictable memory,
not removal of a CGO dependency. This phase changes neither SlateDB nor the
Rust VaulticDB metadata engine. Authoritative metadata, fencing, transactions,
snapshot publication, and durable WAL state remain on their existing paths.

## Scope and starting evidence

Inventory these owning implementations and their direct callers first:

| Surface | Current state | Scaling dimension |
|---|---|---|
| `internal/index/written_blob_store.go` and `blob_lookup_cache.go` | Pebble overlay for on-demand backup's newly written blob locations | Locations written during the current run, not the entire preexisting repository |
| `internal/archiver/marker_store.go` and `exclude.go` | Marker decisions spill after 4,096 in-memory entries | Distinct directory decisions per marker specification |
| `internal/crawl/cwalk.go` and `internal/archiver/archiver.go` | Local non-incremental directory manifest backed by Pebble | Directory paths and child-name lists; incremental/NFS streaming is a separate path |
| `internal/index/maintenance/` | Encrypted external-sort/check scratch, not a Pebble database | Selected records, merge fan-in, and exact comparison semantics |
| `internal/index/daemon/` import callers and reducers | Import staging, bounded caches, and temporary files | Imported records, projection/reducer state, and publication batches |

Do not invent a KV store for a streaming workload. Include import and check even
where their working state is currently non-KV: route relevant retained state
through the budget/policy, retain efficient streaming, and explicitly enumerate
any remaining temporary files. A RAM-only success must not hide disk spills in
an external-sort helper, dependency, fallback branch, or legacy projection.
Repository pack uploads and durable daemon writes are not working-memory spills.
Existing pack staging and ordinary repository caches must be reported separately;
this is not a promise that the CLI performs no filesystem I/O of any kind.

The current blob overlay has approximately 148 encoded key/value bytes per
location before engine overhead. This is a sizing input, not a heap estimate or
an assertion that 100 GB is sufficient. Marker/crawl estimates must include
path/name lengths, repeated specifications, and unusually wide directories.

## Required behavior and invariants

1. RAM, KV, and automatic modes produce identical logical results: duplicate
   blob locations, marker decisions, ordered scans, directory names, import
   results, check findings, and snapshot/restore bytes.
2. Working state is run-scoped and disposable. No migration changes repository
   formats, invents cross-run authority, or acknowledges durability early.
3. Explicit RAM mode never creates a working-state database, WAL, SSTable, value
   log, or external-sort spill file. Budget exhaustion returns a typed error
   before an unbounded allocation; it never silently switches to disk.
4. Explicit KV mode uses the selected replacement engine, not Pebble. Bound its
   heap, mapped pages, batches, handles, and temporary storage as well.
5. Automatic mode starts in RAM and falls back only when policy permits it.
   Storage errors, corruption, authentication failures, and uncertain daemon
   outcomes are not reasons to change backend or retry publication.
6. Cancellation, cleanup, and errors remain observable. A scratch failure must
   prevent an incorrectly successful complete snapshot or check verdict.
7. Existing authenticated/encrypted overlay encoding is preserved or replaced
   with a reviewed equivalent. Sensitive scratch paths, values, and keys need
   an explicit confidentiality design; directory mode 0700 alone is not
   encryption. Never log keys, values, source paths, or credentials as metrics.
8. No production restart, cache change, backup trial, or deletion is authorized
   by implementing this design. Production trials require separate approval.

## Operator policy

Proposed shared controls; settle exact names against existing command/profile
conventions in M0 before implementing them:

| Control | Contract |
|---|---|
| `--working-memory-mode=ram|kv|auto` | Explicit backend or RAM-first fallback; report effective mode and all participating stores |
| `--working-memory-ram-limit=BYTES` | Explicit aggregate cap on managed retained state and transient reservations, not per-store allowances or a whole-process RSS guarantee |
| `--working-memory-scratch=DIR` | Required writable private scratch for KV and automatic fallback; never selects durable VaulticDB storage |
| `--working-memory-disk-budget=BYTES` | Aggregate scratch cap, including migration, rebuilds, and compaction if applicable |

Accept documented decimal/binary units and reject zero, negative, overflowed,
or ambiguous values. RAM limits may be much lower or higher than 100 GB; that
number is an illustrative sizing point, not a default, minimum, or capacity
promise. In automatic mode, an omitted limit uses a documented conservative
resource-derived default, not all reported free memory. Explicit modes must
also report their resolved limit before allocating substantial state.

Keep `--metadata-on-demand` semantics independent of this policy. Resolve
`--metadata-scratch` and `--metadata-cache-mib` compatibility explicitly; reject
contradictory settings instead of silently granting two budgets. Apply policy
to participating import/check/backup paths and profile-driven invocations.
Do not change the production default until benchmark gates pass. Deprecating
older controls requires documented precedence and migration guidance.

Automatic selection uses conservative counts/size estimates when available,
remaining budget, and measured recommendation profiles. Missing estimates must
not start an unbounded allocation or a full preliminary repository scan.
Recommendations are advisory: explain memory/GC headroom and scratch tradeoffs
without silently overriding an explicit operator choice. On slow vdisks prefer
RAM only when both capacity and GC gates hold; do not assume KV wins on SSDs.

## Go GC and memory design: mandatory release gate

A configured RAM envelope is not permission to allocate an equally large Go
heap. Determine the effective host/cgroup limit, daemon and other-process
reservations, non-heap memory, and safety headroom first. Distinguish decimal
GB from GiB in configuration, evidence, and recommendations.

Use pointer-light retained layouts: fixed-width IDs and scalar offsets in
chunked arenas, contiguous byte storage, and compact hash/sorted indexes.
Prefer the existing compact index representation where its semantics fit.
Avoid one heap object per entry, pointer-rich maps of structs/strings/slices,
per-entry interface values, and retaining an entire input buffer through a
small slice. Do not assume a RAM map uses the same bytes as serialized KV.
Variable-length names/values should use byte arenas plus offsets/lengths.
Chunk arenas to bound allocation and release units; do not require one enormous
contiguous allocation, unbounded rehash, or copying the complete working set.

Reserve capacity before allocation. Charge arena capacity, index capacity,
growth overlap, encode/decode buffers, queues, migration buffers, and cache
entries against one run budget. Include alignment and allocator overhead with
documented conservative accounting and validate it against measured heap/RSS.
Release reservations exactly once after ownership transfer or cleanup.
Leave explicit slack for ordinary Go runtime/archiver allocation; this accounting
cannot promise a hard RSS cap for the entire process or prevent external OOM.

Measure allocation rate, live/allocated heap, heap objects, scannable heap,
GC CPU, assist CPU where supported, cycle count, pause distributions, runtime
memory, RSS, and mapped-memory residency. Use version-checked `runtime/metrics`,
heap/CPU profiles, and OS/cgroup data. Report missing metrics explicitly.
Heap size alone is insufficient: a large pointer-free arena and a similarly
sized pointer-rich heap impose different tracing costs. Large arenas still
consume memory and affect allocation pacing. bbolt mmap does not appear as Go
heap but resident pages still consume the machine/cgroup budget.

Do not disable GC, apply a global `GOGC`/`GOMEMLIMIT` change silently, or use
unsafe/manual/off-heap allocation as the initial solution. `GOMEMLIMIT` is a
soft Go-runtime limit, not an RSS/cgroup guarantee; page mappings and other
processes need separate headroom. Test default runtime settings first, then
document any approved tuning as a separately controlled benchmark variable.
Include a high-live-heap, concurrent-backup-allocation scenario to detect GC
assists, latency spikes, and repeated GC near a soft memory limit.

### Limit response and mitigations

Use a shared reservation coordinator on every allocation/growth path and a
bounded runtime/OS sampling loop for pressure outside the managed state.
Resolve usable capacity as the minimum of the configured working-state cap
and the conservative resource envelope after unrelated-memory reservations.
Reject an unsafe request at startup, or report a smaller effective allowance
explicitly; never silently enlarge a limit. Operator-provided co-resident
reservations must cover VaulticDB and other workloads that share the envelope.
Changing cgroup limits or external pressure may reduce the allowance during a
run, but may not increase it without explicit operator action.

Define budget-derived warning, growth-admission, migration, and stop thresholds
in M0. Derive migration timing from copy/growth requirements, not a fixed
percentage alone. Report accounted capacity, observed heap/RSS, effective cap,
pressure cause, action, and remaining migration headroom with bounded events.
Avoid oscillation through hysteresis and the one-way RAM-to-KV transition.

| Mitigation | Required behavior and limits |
|---|---|
| Pointer-light arenas and indexes | Reduce traced pointers and per-entry objects; benchmark actual scannable bytes, not only heap size |
| Streaming and early release | Avoid retaining a whole listing/input; release completed batches and obsolete arenas without losing exact duplicate/history semantics |
| Bound growth and copies | Reserve rehash, arena growth, decoder, and migration overlap first; reject a single entry larger than the limit with a typed error |
| Backpressure | Pause new state-producing admissions and cap queues; reduce adjustable concurrency only at safe batch boundaries, preserving publication/durability semantics |
| Evict disposable caches | Drop recomputable cache entries before touching required working state; never evict the sole copy of a new blob location or a required comparison record |
| RAM-to-KV migration | Start before memory is exhausted; use bounded transfer and verified handoff, including disk/mmap residency in pressure accounting |
| Controlled exhaustion | Strict RAM fails the operation cleanly; automatic mode falls back or fails if scratch/headroom is unavailable; forced KV also bounds batches and resident memory |
| Runtime tuning | Evaluate optional explicit soft-limit tuning only with profiles; do not lower a Go soft limit below unavoidable live state and induce continuous GC |

Backpressure cannot free already-retained data; if streaming/eviction cannot
restore headroom, automatic mode must migrate and strict RAM must return
`working_memory_limit_exceeded`. Include configured/effective limits, required
reservation, participating store, and safe operator remediation without keys
or paths. A handled failure must leave normal transaction cleanup and snapshot
publication rules intact. It is not acceptable to OOM, deadlock waiting for
impossible reservations, discard state, or report an incomplete run as success.

The explicit limit is enforceable for managed reservations, not arbitrary Go
runtime or dependency allocations. RSS sampling is reactive and cannot prevent
every sudden allocation/external OOM. A hard whole-process limit requires an
operator-controlled OS/cgroup boundary with additional headroom; do not create
or change that boundary automatically. Test pressure actions under a bounded
isolated cgroup and collect exit/OOM evidence without involving production.

## Replacement KV engine selection

Leading candidate: maintained, pinned `go.etcd.io/bbolt`, using bounded batched
writes and ordered bucket cursors. A page-based B+tree may avoid LSM compaction
for this workload, but single-writer serialization, mmap residency, random page
updates, remap/growth, large values, and fragmentation can make it worse.
Do not adopt it based solely on that expectation.

Compare bbolt with a maintained pure-Go alternative such as Badger v4 during
the dependency spike; retain Pebble only as a benchmark baseline during
transition. Badger's LSM/value-log behavior is a tradeoff, not an automatic
improvement. Verify current maintenance, licensing, supported OS/architectures,
dependency graph, and `CGO_ENABLED=0` builds before selecting and pinning an
exact version. Dependency types must stay behind the working-state adapter.
The winning KV implementation must pass semantics, security, bounded-memory,
and slow-vdisk gates. If neither qualifies, stop for a reviewed selection
decision instead of adding an untested engine or keeping Pebble silently.

Ephemeral stores may use explicitly documented relaxed sync settings; deletion
or failure after a crash is acceptable because they are not reopened as truth.
Disabling fsync does not eliminate database writes. Creation, allocation, and
ENOSPC behavior must remain bounded. Review cursor/transaction lifetimes so
readers cannot indefinitely retain pages or block writer progress.

## Backend contract and fallback

Define only the operations existing consumers require: typed get/put/batches,
duplicate-location lookup, bounded prefix/range iteration where needed, and
close. Specify ownership/copy lifetimes and ordering; never expose a borrowed
database page after its transaction closes. Do not force sort/merge reducers
into a generic transactional KV abstraction. Preserve specialized streaming
and packed representations under the same resource policy.

For automatic migration, stop new mutations at a per-store boundary, drain
in-flight readers/mutations, and copy a stable view using bounded batches.
Publish the new backend only after copy and integrity checks succeed; then
release RAM ownership. Account for simultaneous old RAM and new KV state and
reserve migration headroom before reaching the limit. Store-level migrations
share the aggregate coordinator; no independent full-budget allowances.
Define admission/backpressure for concurrent callers and preserve their context.

On cancellation or copy failure retain the valid old backend or terminate the
run with the original error; never expose a partially copied store. Avoid
RAM/KV oscillation: once migrated, remain KV for that run. Test duplicate and
ordering parity across the transition. Scratch stores get per-run identities,
private ownership checks, authenticated encoding, and safe cleanup; never remove
an arbitrary caller-supplied directory or adopt leftovers from a previous run.

## LLM-executable milestones

Execute milestones in order as independently reviewable changes. Before each
first edit, read the owning function/direct caller/nearest tests, state one
falsifiable hypothesis, and choose a focused check. Run that check immediately
after editing; repair the same slice before advancing. Reuse existing test and
benchmark files. Do not change runtime behavior during the inventory milestone.
Commit only when explicitly requested by the operator.

### M0: Inventory, baseline, and contract

Completed. See the [M0 milestone record](phase-35-m0-contract.md) for classified
ownership, measurement limitations, executable baseline protocol, and exit gates.

- Enumerate every Go-side temporary KV, sort spill, cache, staging file, and
  dependency-owned working file reached by import/check/backup. Classify each as
  participating working state, durable output, or explicitly excluded pack/cache
  I/O. Trace creation, retention, cleanup, and default activation.
- Add bounded counters for retained entries/bytes, reservations, scratch bytes,
  and backend identity using existing telemetry. Capture unchanged Pebble runs
  and heap/GC profiles on isolated fixtures.
- Freeze the shared policy, typed errors, consumer contracts, confidentiality
  codecs, admission rules, and aggregate-budget accounting in a milestone note.
- Tests: fixture counters, no secret/path disclosure, default-behavior parity,
  cancellation, and cleanup. Exit: complete inventory and executable baselines;
  no claim of RAM-only coverage while an unclassified spill remains.

### M1: Prove and select the KV replacement

Completed: bbolt v1.5.0 selected, with recorded workload regressions and no
production switch. See the [M1 decision record](phase-35-m1-kv-selection.md).

- Build narrowly scoped bbolt/alternative adapters and replay captured entry-size
  distributions: write overlay, marker churn, large directory values, and ordered
  iteration. Use identical encodings and durability settings across candidates.
- Exercise concurrent readers/writes, bounded transactions, memory mappings,
  growth, disk-full, permission errors, cancellation, and close failures.
- Tests: shared semantic conformance and supported `CGO_ENABLED=0` build matrix.
  Exit: version-pinned candidate and decision record with raw latency, CPU, GC,
  RSS/mmap, and I/O evidence. A failed selection gate blocks M2.

### M2: Implement the pointer-light RAM backend

Completed: isolated RAM adapter, shared reservation budget, RAM/KV conformance,
and measured layout/GC scaling. See the [M2 record](phase-35-m2-ram-backend.md).
No production command paths switch backend in this milestone.

- Reuse compact indexing where suitable; implement chunked arenas and bounded
  growth/iteration without per-entry heap objects. Add reservation-based budget
  admission and typed exhaustion before allocation.
- Test duplicate locations, binary keys/values, prefix ordering, replacement,
  wide directories, cancellation, concurrent access, and release accounting.
- Tests: RAM/KV conformance and race tests in touched packages, low-budget
  deterministic boundaries, oversized entries, concurrent reservation races,
  and allocation/GC scaling probes.
  Exit: no working-state filesystem access in forced RAM fixtures; measured
  retained capacity and pointer-scanning growth agree with the design.

### M3: Integrate all participating command paths

- Convert overlay, marker, and applicable manifest callers in separate slices;
  retain streaming paths. Apply policy to import reducers and check spools so
  explicit RAM mode cannot silently invoke disk spill.
- Keep import checkpoints, exact sorted/multiset comparison, daemon authority,
  legacy projections, and publication ordering unchanged.
- Tests: matched import results, check findings, backup snapshot contents and
  verified restored bytes for forced RAM and forced KV; trace working-file
  creation. Exit: every M0 surface has a mode contract and parity evidence.

### M4: Add bounded automatic fallback and CLI policy

- Implement admission, pre-reserved migration headroom, stable-view transfer,
  atomic backend handoff, and one-way fallback. Add flags/profile precedence,
  effective-mode reporting, typed exhaustion, and operator guidance.
- Tests: tiny-budget migrations with concurrent readers/writers; injected copy,
  ENOSPC, authentication, cleanup, and cancellation failures; strict RAM never
  falls back; multiple stores never spend the aggregate budget twice. Validate
  explicit limit parsing/precedence, startup headroom, lowered effective limits,
  warning/backpressure/fallback/stop actions, bounded events, and safe cleanup.
- Exit: no missing/duplicate entries, no partial-backend visibility, and no
  fallback-induced publication retry or false success.

### M5: Realistic end-to-end and GC benchmarks

- Run the benchmark matrix below against isolated immutable clones and source
  fixtures. Require semantic gates before evaluating performance.
- Measure low, medium, and large configurable RAM envelopes, including a
  ballpark 100 GB case only when appropriate to available resources. Document
  actual limits/headroom and achieved live state rather than assuming fit.
- Exit: repeatable sealed raw results, independently reproduced analysis, and
  explicit accepted/rejected/incomplete classifications for every case.

### M6: Production recommendations and removal of Pebble

- Publish workload/storage-specific recommendations and measured safe budget
  envelopes, including GC-sensitive cases where KV is preferable despite fit.
  Document explicit overrides and automatic fallback outcomes.
- Remove Pebble imports/dependency only after all consumers and tests are migrated;
  retain historical baseline artifacts without shipping a hidden Pebble path.
- Tests: focused and required repository suites, supported pure-Go builds,
  dependency/import audit, defaults/profile compatibility, and cleanup probes.
- Exit: both implementations selectable, automatic mode validated, recommendations
  reproducible, no production-default change without reviewed evidence.

## Benchmark and acceptance protocol

Freeze inputs, limits, repetitions, and acceptance rules before running. Include
reduced CI fixtures and a separately authorized large-scale matrix:

| Workflow | Required scenarios |
|---|---|
| Import | Real legacy index distribution; duplicates, damaged input under existing best-effort rules, reducer pressure, checkpoint/reopen parity |
| Check | Full exact comparison and supported smaller scopes; ordered/multiset joins, mismatch fixtures, bounded findings, memory and merge pressure |
| Backup | Initial, unchanged incremental, and high-churn runs; many small files, large files, multiple marker rules, wide directories, local and NFS sources |

Compare forced RAM, chosen forced KV, and automatic fallback; retain the current
Pebble binary as a historical baseline. Keep daemon binary/settings, chunking,
worker counts, batching, source/repository hashes, and durability gates identical.
Use actual slow vdisks and HDD/NFS scratch where relevant; SSD and tmpfs results
are supplemental and must not substitute for the target storage. Record cold
versus warm cache conditions and prohibit overlapping builds/backups/benchmarks.
Use balanced repeated ordering (at least three complete runs per mode/scenario),
retaining outliers, and report dispersion rather than one winning sample.

Exercise working sets below, near, and above the configured RAM budget. Forced
RAM exhaustion is an expected bounded failure, not a successful performance run.
Include 1 GB, 10 GB, and larger retained-state scales where resources permit;
the large matrix must exercise GC near the intended production envelope, not
extrapolate only from tiny fixtures. If resources are unavailable, mark the
intended large-scale production envelope pending instead of claiming support.

Capture wall and CPU time, throughput, peak RSS, runtime/heap and scannable bytes,
objects, allocation rate, GC cycles/CPU/assists, pause percentiles/max, operation
latency tails, I/O bytes/operations, peak scratch, mappings, swap, and migration
time/peak coexistence. Reject production recommendations that require swap,
breach the frozen headroom, show unbounded growth, or cause unacceptable GC
assists/tail latency. Freeze workload-specific GC/latency thresholds in M0 from
the baseline and operator SLO; do not relax them after seeing candidate results.
Map payload capacity to measured RSS/GC cost, not merely to serialized bytes.

Semantic gates: import/check logical parity under narrowly enumerated exclusions;
unchanged authoritative metadata and durability contracts; backup CLI success,
one expected new snapshot, preserved prior snapshots, and representative restores
with verification and byte/hash equality. A timeout, partial scan, healthy daemon,
or absence of mismatches in partial output is not successful completion.

Record source patches/revision, pinned binaries/dependencies, configurations,
fixture identities, raw logs/profiles, analysis scripts, checksums, and exact test
names/counts. A test filter that executes zero tests does not pass a milestone.
Publish recommendations as a table of observed working-set ranges, workload,
storage, budget/headroom, GC cost, selected mode, and confidence/limitations.
RAM wins only when both completion and resource/GC gates pass; KV wins only
when measured benefits justify disk cost. Do not promise that 100 GB always fits.
