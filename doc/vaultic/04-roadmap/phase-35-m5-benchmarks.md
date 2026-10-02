# Phase 35 M5: sealed end-to-end and GC benchmark matrix

[Back to roadmap index](00-overview.md)

[Phase 35 specification](phase-35-ram-working-memory-and-kv-fallback.md) |
[M4 policy and fallback evidence](phase-35-m4-fallback.md)

**Status: execution protocol implemented and focused repairs verified;
accepted capture blocked by competing host builds. M5 exit remains pending.**

## Frozen protocol

Run `bash helpers/phase35-m5/run.sh` from the repository. The runner creates
only private disposable source, repository, daemon and scratch fixtures.
It does not restart production services, read production credentials, alter
kernel caches, change global runtime settings or select a production default.

The baseline is the archived M4 parent with policy omitted: its historical
Pebble/default paths remain intact. Both baseline and candidate receive the
same measurement-only overlay: opt-in test fixtures, the independent benchmark
probe and runtime interval reader. Production algorithms and daemon settings
are not changed in the baseline. Retain both compiled binaries and identities.

An immutable seed contains 2,048 small files in a wide directory, a 32 MiB
deterministic incompressible binary file, and two independent exclusion markers.
The two marker files are included; their sibling payloads are excluded, matching
the existing `--exclude-if-present` contract. Restore verification hashes every
actual restored file without skipping marker directories.
All cases copy the same encrypted repository configuration and source seed.
The live source uses a fixed absolute path, so unchanged runs reuse the actual
prior snapshot rather than changing paths and calling a new scan incremental.
Clones preserve modification times and establish an untimed parent against the
actual source inodes before unchanged/high-churn/high-live timing. The unchanged
CLI summary must report all 2,051 included files unmodified with zero new or
changed files; content deduplication alone does not satisfy this condition.
Every backup must preserve prior snapshots, publish exactly one new snapshot,
and restore all included names and bytes with the same canonical hash.

Real root-command cases cover initial, unchanged, high-churn and high-live
backups, native index import with resume/reopen, full exact native checks, and
the supported legacy-only check scope. Index-only timing explicitly excludes
snapshot JSON setup/verification and full snapshot/tree/path traversal. Snapshot
JSON is present for untimed full exact validation; this is not a claim that
index-only import completed full snapshot traversal. The actual incomplete
full-traversal trial is retained separately.

RAM, bbolt KV, automatic mode and the historical Pebble/default baseline each
run at 32 MiB, 128 MiB and 512 MiB configured working-state envelopes. Synthetic
authenticated working-state probes add below-budget, near-budget, above-budget
and 1 GB (decimal) live-payload cases. The near-budget auto case must actually
migrate, not merely specify auto. Above-budget strict RAM must return typed
exhaustion and preserve the exact valid retained prefix. A bounded failure is
not successful workload completion or a performance win.

High-live probes retain actual data, not just request a large limit. The real
backup case holds 1,000,000,000 bytes of ordinary pointer-light Go heap while
running the CLI backup and 3 GiB of bounded concurrent allocation churn. The
synthetic working-state case retains its authenticated store while issuing
lookups and the same churn. These are two different scenarios, not evidence of
a 1 GB production backup working set. Defaults are `GOGC` and `GOMEMLIMIT`;
no GC disabling, forced collection or soft-limit tuning is used.
The store fixture interleaves two measured lookups per allocation chunk,
retaining 1,536 lookup observations across the entire churn. Write-batch and
lookup latency distributions are independent and each must meet the tail gate.

The default schedule has four balanced mode orders and alternating scenario
order. `PHASE35_M5_REPETITIONS` accepts 3 through 12 repetitions. Inputs, cases,
repetitions and thresholds are written and hashed before the first measurement.
All builds finish before timings, and overlapping build/backup/test processes
reject the preflight. No outlier or slower repeat is discarded.
During timings a 100 ms host process monitor also rejects observed competing
build/backup/test executables, allowing only test binaries in this capture's
owned `bin` directory. It records its checks and never stops another process.
This is sampled overlap detection, not proof against a sub-100 ms transient.
The Node executable is pinned by absolute path and hash and passed explicitly
into the transient service; monitor liveness is mandatory around every case.

Timings use the actual ext4 VM vdisk, with warm file caches caused by cloning
and input verification. A unique transient service enforces 12 GiB memory and
zero swap. The runner records actual cgroup limits, events and aggregate peak;
any `max`, OOM or OOM-kill event rejects the evidence. The aggregate cgroup peak
includes file cache and child daemons and is not substituted for CLI RSS.

## Measurement and gates

The independent probe samples every 50 ms and retains bounded scalar peaks,
not an unbounded timeline. It captures heap/runtime/scannable bytes, objects,
allocation and GC intervals, cycle counts, GC/assist CPU, pause histogram
p50/p95/p99/max upper bounds, process CPU/RSS/swap, scratch, owned mappings and
mapped residency, child-daemon RSS, logical and physical I/O, and bounded
operation latencies. Unsupported/reset metrics are explicit. Linux gopsutil
mapping KiB are converted to bytes. Scratch peaks are sampled logical file
sizes; mapping sizes are not resident memory. Runtime total includes Go's
reserved/released address-space accounting and is not a process RSS limit.
Unavailable observations form a bounded sticky union: a later successful or
differently failed sample cannot erase an earlier missing metric. Wall and
runtime intervals include endpoint sampling; proc/runtime reads are sequential,
not atomic. Process CPU counters have the host's native counter granularity.

Fixture setup, input hashing and semantic restore/full-check verification are
outside the aligned interval. CPU profiles cover measurement; heap profiles
are captured before owner cleanup without forcing GC. Decoded profile tops,
whole-process GNU time results and logs are retained separately. Profiling,
proc sampling, encryption and cache effects are not claimed negligible;
identical instrumentation is used for every comparison.

The M0 gates remain frozen: semantic completion first; no swap/OOM or managed
overspend; GC and assist CPU shares at most two percentage points above paired
baseline; pause p99 upper bound at most `max(10 ms, 1.25 * baseline)`; operation
p99 at most 1.10 times baseline. Both original cumulative and aligned interval
pause bounds are checked. CPU shares use process CPU, not summed wall durations.
Fewer than 100 operation observations cannot establish an operation-tail gate.
In particular a single end-to-end CLI invocation is a workflow duration, not
a validated p99 distribution. Its timing remains useful raw evidence but its
performance classification is incomplete. No operator SLO is invented.

Every case is accepted, rejected or incomplete against these rules. Semantic
failure, skipped/wrong tests, missing profiles, malformed observations, changed
inputs/plans, leaked scratch or cgroup violations reject the capture itself.
Successful semantics with an unmet performance gate is retained as a rejected
performance case; unknown gates stay incomplete. Median/min/max and every repeat
are retained. Analysis is rerun separately and must reproduce the exact JSON.
Known gate violations remain rejected even if another gate is unknown. Paired
baseline tails require the same minimum count. Derived store throughput uses
achieved payload/entries over the measured interval, including churn where
present; workflow source-byte rates are logical inspected input, not physical
backup upload throughput.

## Boundaries

The following intended matrix cells remain explicitly incomplete rather than
being represented by another workload or filesystem:

- Full 2,049-file snapshot/path import exceeded its initial 120-second fixture
  bound. Index-only timing does not replace that result.
- No separately approved disposable NFS source or HDD/NFS scratch endpoint.
- No cold-cache claim or global cache manipulation.
- Separately authorized 10 GB and ballpark 100 GB retained-state trials remain
  pending. Neither payload fit nor production GC cost is extrapolated from 1 GB.
- Generated repository timings are not a real production legacy distribution.
  Duplicate/damaged-input, exact joins, findings and pressure regressions run
  as separately named semantic gates, not as substitute performance samples.
- No production operator SLO, production trial, runtime-tuning approval or
  recommendation/default-change approval.

M6 production recommendations and Pebble removal require their own gates.

## Retained evidence

The accepted capture, classifications, exact counts and final source/commit
seal will be recorded after execution and completion review.
No capture is currently accepted. The implementation is committed separately
at the operator's request, without claiming the M5 exit gate is complete. A quiet
host window spanning all four repetitions is required; repetitions are not stitched
across invalidated captures, and thresholds or overlap rules are not relaxed.

## Completion review repairs

The review/test cycle repaired these protocol defects before accepting a matrix:

- Moved measurement into independent `internal/workingbench` to avoid a
  telemetry/data/test import cycle; unsupported non-Linux mappings are explicit.
- Corrected Linux mapping units and separated logical from physical I/O.
- Treated only typed memory exhaustion as a safe bounded failure, preserving
  authority, valid retained entries and cleanup without calling it completion.
- Preserved clone mtimes and established the real inode-matching parent before
  unchanged timing; required actual CLI unmodified counters.
- Included exclusion marker files and hashed all restored files without
  filtering; independently asserted excluded payload absence.
- Retained a bounded union of every missing metric, serialized latency
  finalization and included endpoint sampling in wall/runtime intervals.
- Preserved known GC failures when another gate is unknown, required paired
  tail counts and all 12 core cross-builds, and separated read/write tails.
- Interleaved required-state reads throughout high-live allocation churn and
  measured their own distribution; verified 1,536 reads and actual GC cycles.
- Rejected observed competing work throughout timings, not merely at startup.

Preliminary captures remain retained, not reclassified as accepted:
`/tmp/vaultic-phase35-m5-JpkeYn` (typed-exhaustion handling and invalid clone
contract), `/tmp/vaultic-phase35-m5-jxHecr` (unchanged clone contract),
`/tmp/vaultic-phase35-m5-0LNBUb` (missing-metric/classification review),
`/tmp/vaultic-phase35-m5-bCSrtq` (unmeasured concurrent read tails), and
`/tmp/vaultic-phase35-m5-luAbZT` (competing build rejected by preflight).
`/tmp/vaultic-phase35-m5-iw5XnT` ran all 400 cases but is invalid: the service
could not locate Node, so the required overlap monitor never started. The
runner now pins its absolute executable and checks liveness for each case.
The correctly monitored `/tmp/vaultic-phase35-m5-eAGicI` was invalidated after
5,596 checks by a competing Go process during repetition two. A fresh guarded
retry, `/tmp/vaultic-phase35-m5-H2qCjB`, was invalidated after 907 checks by
another competing Go process during repetition one. Both violation records
and completed-case artifacts remain retained; neither is accepted evidence.
Focused trial logs remain at `/tmp/vaultic-phase35-m5-probe-3VcsLr`;
their isolated passes are not a substitute for the complete repeated matrix.