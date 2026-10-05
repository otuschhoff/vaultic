# Phase 35 M5: sealed end-to-end and GC benchmark matrix

[Back to roadmap index](00-overview.md)

[Phase 35 specification](phase-35-ram-working-memory-and-kv-fallback.md) |
[M4 policy and fallback evidence](phase-35-m4-fallback.md)

**Status: M5 remediation complete: fresh reduced 400-case and extended
416-case NFS captures sealed and independently reproduced. All modes completed
four 100 GB repetitions under the unchanged limits. Production performance
approval remains unestablished; no recommendation or default change.**

## Frozen protocol

Run `bash helpers/phase35-m5/run.sh` from the repository. The runner creates
only private disposable source, repository, daemon and scratch fixtures.
It does not restart production services, read production credentials, alter
kernel caches, change global runtime settings or select a production default.

The baseline is archived M4 `c567dd4561d512162d823ccbfcb3d8578c9690a1`
with policy omitted: its historical
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
index-only import completed full snapshot traversal. The extended `import-full`
scenario times full snapshot/tree/path traversal and verifies directory-child
paths, referenced inode revisions, included file counts and stable same-run
traversal digests through native reopen/resume. Earlier incomplete trials remain
retained and are not substituted for these measurements.

RAM, bbolt KV, automatic mode and the historical Pebble/default baseline each
run at 32 MiB, 128 MiB and 512 MiB configured working-state envelopes. Synthetic
authenticated working-state probes add below-budget, near-budget, above-budget
and 1 GB (decimal) live-payload cases. The near-budget auto case must actually
migrate, not merely specify auto. Above-budget strict RAM must return typed
exhaustion and preserve the exact valid retained prefix. A bounded failure is
not successful workload completion or a performance win.

`PHASE35_M5_PROFILE=extended` adds full import, uses 32 MiB, 512 MiB and 8 GiB
CLI envelopes, and runs actual 10,000,000,000- and 100,000,000,000-byte
working-state targets at 32 GiB and 140 GiB managed limits. Every profile has
four balanced repetitions of all four modes. Extended storage requires an
actual private NFS mount selected by `PHASE35_M5_PARENT`. Large store entries
are 32,768-byte plaintexts with 31-entry batches, preserving the 1 MiB batch
admission contract; small fixtures retain their original shape. Payload targets
are not guaranteed fits. In the invalid first complete-scale attempt, strict
RAM retained 60,004,794,368 payload bytes before typed 140 GiB exhaustion;
that is a capacity observation, not successful 100 GB completion.

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
All builds finish before timings. No outlier or slower repeat is discarded.
Following the operator's October 2 authorization, the preflight and 100 ms
timing monitor reject competing executables within this disposable capture,
allowing only owned fixture binaries in its `bin` directory; unrelated
host activity is recorded as bounded process-count peaks, not an execution
blocker. Compiled source pins still reject workspace changes. The host is not
claimed globally idle, and acceptance thresholds are unchanged. The monitor
never stops another process.
This is sampled overlap detection, not proof against a sub-100 ms transient.
The Node executable is pinned by absolute path and hash and passed explicitly
into the transient service; monitor liveness is mandatory around every case.

Reduced timings use the actual ext4 VM vdisk; extended timings use the private
NFS fixture. Both have warm file caches caused by cloning and input verification.
A unique transient service enforces 12 GiB for reduced or 200 GiB for extended
captures, with zero swap. Extended cases have a 24-hour execution bound; this
does not relax any performance gate. The runner records actual cgroup limits,
events and aggregate peak;
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
Large write distributions use pinned HDR Histogram v1.1.2, three significant
digits and a fixed 0-to-24-hour nanosecond range rather than unbounded samples.
Both p99 bounds are retained: acceptance requires candidate upper bound at most
1.10 times baseline lower bound; rejection requires candidate lower bound above
1.10 times baseline upper bound. Overlapping uncertainty stays incomplete.

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

## Historical Boundaries

The original captures below retained the following limitations. The later
remediation capture resolves candidate 100 GB capacity and transient scratch
inventory gaps; it does not change or reclassify these original observations:

- All 48 extended full-import cases completed verified traversal/reopen/resume;
  generated 2,051-file fixtures are not a real production legacy distribution.
- NFS is authorized and exercised; the physical HDD identity of its remote
  storage is unknown and no HDD-specific result is claimed.
- No cold-cache claim or global cache manipulation.
- All four modes completed four 10 GB repetitions. Only historical Pebble
  completed the 100 GB target; forced RAM, KV and automatic mode exhausted their
  140 GiB managed envelopes while preserving verified prefixes and cleanup.
  The intended production 100 GB candidate envelope is not validated.
- Generated repository timings are not a real production legacy distribution.
  Duplicate/damaged-input, exact joins, findings and pressure regressions run
  as separately named semantic gates, not as substitute performance samples.
- No production operator SLO, production trial, runtime-tuning approval or
  recommendation/default-change approval.

M6 production recommendations and Pebble removal require their own gates.

## Historical Retained Evidence

Reduced capture `/tmp/vaultic-phase35-m5-g2gjVk` passed capture-level gates:
400 semantic cases, four balanced repetitions, independently reproduced
analysis, and performance classifications accepted 0 / rejected 127 /
incomplete 273. Actual cgroup peak was 4,181,102,592 bytes. Its original
7,401-entry `SHA256SUMS` hash is
`18b6d842181c3b6b0c496fa8efa781e4d557cc7401bd212edf3c8ef39889c3cb`.
The owned-scope monitor performed 19,067 checks and recorded unrelated activity;
the capture is not claimed globally idle. Preserve this original seal.

Extended capture
`/volume2/NASDA2/vaultic-phase35-m5-owned-1wESMl/vaultic-phase35-m5-CtAWDK`
passed all 416 exact named semantic cases and four balanced repetitions.
Its frozen analyzer produced accepted 0 / rejected 89 / incomplete 327;
separate analysis reproduced identical JSON. The 7,535-entry original
`SHA256SUMS` hash is
`59e8af900f306cc123145bca9ee81644725db8a9a6e4fdb1af7c9940c75a0c71`.
All source, seed, Node, private daemon and protocol pins passed. The monitor
performed 885,914 checks with no owned-capture violation; unrelated external
peaks were Go 3, compile 34, link 36, assembler 2 and test 9. Actual cgroup peak
was 157,419,986,944 bytes, with zero max/OOM/OOM-kill events and zero observed
swap. Required preflight retained 265 measurement and 429 import/check race
test/subtest passes, no skips/failures, and all 12 supported pure-Go core builds.

All full imports completed, including exact tree/path coverage and native
reopen/resume. Historical full checks completed at every envelope; candidate
full checks completed at 8 GiB and safely exhausted at 32/512 MiB. The reduced
capture also retains actual below/near/above-budget cases and auto migration;
extended large-state migration is not counted as the reduced near-budget cell.

| Target / Mode | Achieved Plaintext Bytes | Completed Repeats | Wall Median / Min / Max (s) | Sampled Peak RSS (bytes) | Performance |
|---|---:|---:|---:|---:|---|
| 10 GB / Pebble | 10,000,007,168 | 4/4 | 52.9 / 51.6 / 58.9 | 223,793,152 | incomplete |
| 10 GB / RAM | 10,000,007,168 | 4/4 | 470.2 / 382.4 / 573.4 | 19,867,975,680 | rejected |
| 10 GB / KV | 10,000,007,168 | 4/4 | 64.1 / 61.0 / 65.3 | 8,620,273,664 | rejected |
| 10 GB / auto | 10,000,007,168 | 4/4 | 202.1 / 195.9 / 209.2 | 9,548,431,360 | rejected |
| 100 GB / Pebble | 100,000,006,144 | 4/4 | 582.9 / 532.1 / 632.3 | 389,603,328 | incomplete |
| 100 GB / RAM | 60,004,794,368 | 0/4 | 12,475.9 / 11,565.0 / 15,318.3 | 106,552,721,408 | rejected: exhaustion |
| 100 GB / KV | 61,859,659,776 | 0/4 | 979.4 / 959.3 / 1,057.2 | 68,589,973,504 | rejected: exhaustion |
| 100 GB / auto | 62,261,919,744 | 0/4 | 2,044.2 / 1,967.4 / 2,219.9 | 69,117,079,552 | rejected: exhaustion |

Durations include encryption, instrumentation and high-live churn where the
workload completed, not untimed semantic verification. Exhausted-prefix times
are not same-workload speedups versus a completed baseline. All completed
high-live cases retained 1,536 interleaved lookups through 3 GiB allocation
churn with actual GC. At 10 GB, GC cycles ranged 16-17 for RAM, 3,326-3,429 for
KV, 1,825-2,201 for auto and 652-696 for Pebble. Few GC cycles alone do not
establish a performance win: all candidate 10 GB repetitions failed frozen
paired performance gates. At 100 GB, only Pebble exercised the complete target
and churn, with 1,773-2,040 GC cycles. Candidate prefix GC is not full-target GC.

Unknown operation p99 from single CLI invocations remains incomplete rather
than being inferred from workflow duration. Sticky `scratch_inventory`
unavailability occurred in 34 extended cases, including store probes; sampled
sizes and raw observations are retained, but this metric gap prevents claiming
fully verified performance acceptance. All known gate violations remain
rejected despite another unknown metric. No threshold, SLO or cache claim was
invented to obtain an accepted result.

The matrix completed in one capture. Its terminal handle was unavailable at
status review and profile-summary finalization was incomplete. Only the frozen
post-measurement steps were resumed: decode all profiles, verify pins, reproduce
analysis and generate/check the first evidence manifest. No case was rerun,
stitched, dropped or edited. The original manifest is preserved; final
patch/tree/commit artifacts receive a separate completion seal.

Extended captures `...owned-VVRlTU/...Ni67MM`,
`...owned-EEnfYX/...RcIZIl` and `...owned-Bwm15x/...54Rtth` under
`/volume2/NASDA2` are invalid and retained.
The former exposed final read-session context cancellation; the latter passed
the repaired check and full-import cells, then exposed oversized verification
pages after typed 100 GB RAM exhaustion. Neither partial repetition is stitched
into a completed matrix. Final extended classifications and source/commit seals
are recorded separately above; thresholds and the authorized overlap policy
are unchanged.
The third capture verified all first-repetition 100 GB outcomes, then exposed
untimed verification mutating the measured RAM budget in repetition two. Live
policy sampling now stops and joins at the measured-work boundary before the
endpoint snapshot; it remains enabled throughout timed work. A mandatory
fake-clock race regression proves sampling is live before shutdown and that
later resource changes cannot alter the captured endpoint. No GC settings or
production policy behavior change. Ordinary working-KV race tests now join the
preflight; opt-in workload, subprocess-helper entry points and the separately
configured M4 cgroup fixture are explicitly excluded from that ordinary gate.

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
- Pinned historical baseline, all compiled source, Node, capture-local protocol
  scripts and a capture-local daemon; mandatory monitor liveness throughout.
- Bound final read-session validation to its original parent context rather
  than the cleanup-canceled finding-budget child; preserved real cancellation.
- Reduced post-measurement verification pages on typed admission pressure,
  preserving exact retained-prefix checks and limits. The tight-envelope race
  probe verified eight-row pages, exhaustion semantics, every retained entry,
  zero overspend and final cleanup.
- Joined the measured policy sampler before untimed validation, preserving
  timed resource adaptation and avoiding verification-induced endpoint changes.

## Completion review

M5 exits on repeatable sealed raw evidence, independently reproduced analysis
and explicit classifications, not on an invented production acceptance result.
The complete reduced and extended captures satisfy that exit. Review verified
all named cases, balanced inputs/order, achieved payload, semantic authority,
typed-failure cleanup, true unchanged counters, marker/restore hashes, native
traversal/resume parity, latency bounds, GC intervals, source identities,
profiles, cgroup limits/events and unchanged frozen thresholds. No unresolved
implementation finding remains in the exercised protocol. Explicit incomplete
metric/workload cells and rejected performance/capacity gates remain limitations,
not waived findings. M6 recommendations, production trials/defaults and Pebble
removal remain unapproved.

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

## Completed Remediation

The October 4 remediation implements all five follow-up recommendations without
changing payloads, configured caps, safety margins, GC settings or acceptance
thresholds. The original manifests and classifications above remain untouched.

| Recommendation | Implementation and Discriminating Verification |
|---|---|
| Remove quadratic RAM bookkeeping | Indexed free-chunk chain and incremental arena charge replace allocation-time hole/accounting scans. An independent capacity oracle checks reclamation, reuse, exact values and final release. Telemetry alone may walk chunks at the endpoint. |
| Improve packing | Budget-aware 1 MiB arenas for large spans retain 64 KiB arenas under tight limits. Planning and allocation use the same choice. Six boundary sizes include the unchanged 32,768-byte plaintext plus nonce/tag case; packing, admission and contents are checked. |
| Remove KV growth cliff | Mapping ceilings double below 1 GiB, then increase by 1 GiB. Memory and disk deltas are admitted before changing the ceiling; denied growth releases reservations and retains state. The synthetic 64-to-65 GiB test requires a 64-bit address space. |
| Audit resource accounting and recover | Full mapping capacity, transient copies and migration overlap stay charged. Linux `cachestat` measures only registered private KV descriptors, including ownership transfer and close. Cgroup credit is `max(RSS, owned cached bytes)`, never their sum or all cgroup cache. Unsupported/failed probes get no additional credit. Immediate pressure lowering and unavailable-resource failure remain; three consecutive healthy observations recover only to the initial safe/configured ceiling. |
| Validate progressively and repeat | Focused 1/10/100 GB probes precede new four-repetition 400/416-case sealed captures, required race suites, all 12 pure-Go platform builds, decoded profiles, source/tool/seed pins and independently reproduced analysis. |

Re-evaluation found two additional local causes. Full `smaps` walks on a
RAM-only 100 GB heap stalled measurement; a cheap owned-mapping preflight now
avoids those walks only when no owned file mapping exists. KV still measures
mapping residency. Concurrent compaction/unlink no longer marks the whole
scratch inventory unavailable for a vanished child; missing roots and other
I/O failures remain unavailable. Mandatory ownership and concurrent-removal
regressions cover these paths.

The first repaired KV 100 GB attempt exposed RSS dropping during bbolt remap
while its owned page cache remained charged to the cgroup. The resource-credit
repair above fixes this without replacing the mapping charge with RSS. The
completed diagnostic's profile then found 41.52% CPU in bbolt's linear array
freelist allocator. Selecting bbolt v1.5.0's built-in hashmap freelist removed
that hotspot; ordinary conformance, reopen, failure, migration and pressure
tests passed again. Repeated progressive KV timing fell from 3,333.7 to
1,592.6 seconds at 100 GB, with the same 139,586,478,144-byte retained charge.
These sequential diagnostics are not paired production speedup evidence.
An isolated 384 MiB cgroup regression still lowered effective admission from
179,056,640 to 110,329,856 bytes after unrelated allocation pressure.

The private diagnostic root is
`/volume2/NASDA2/vaultic-phase35-remediation-probes-20261004`.
Earlier timeouts, failed RSS-cliff probes and the RAM test whose post-test
wrapper could not locate Node remain diagnostics, not sealed capture evidence.

### Fresh Sealed Captures

Reduced `/tmp/vaultic-phase35-m5-oAOym9`: 400 semantic cases, four balanced
repetitions and four actual near-budget migrations. Frozen classifications are
accepted 12 / rejected 114 / incomplete 274. The original 7,407-entry manifest
hash is `2633890c0bf768e3bb72dea97b7f2ccc1288239e896ec4f3efa23de895e5e0d2`.
Actual cgroup peak was 3,618,988,032 bytes; 14,312 owned-monitor checks passed.

Extended
`/volume2/NASDA2/vaultic-phase35-remediation-probes-20261004/vaultic-phase35-m5-vz3z1B`:
416 semantic cases, four balanced repetitions and all 48 full imports verified.
Frozen classifications are accepted 28 / rejected 74 / incomplete 314.
The original 7,535-entry manifest hash is
`23d6417335b627e5dc8902d080763058c27f1ec206036b114975b1c6bae6e4e5`.
Required preflight retained 278 measurement and 429 import/check race
test/subtest passes, no skips/failures, and all 12 pure-Go core builds.
Actual cgroup peak was 168,025,776,128 bytes; 377,704 owned-monitor checks
passed. Both captures had zero cgroup max/OOM/OOM-kill events and observed
swap, verified pins, decoded profiles and identical independent analysis.
Unrelated host activity was authorized; sampled external peaks were empty,
not proof that the host was globally idle.

| Target / Mode | Achieved Plaintext Bytes | Completed Repeats | Wall Median / Min / Max (s) | Sampled Peak RSS (bytes) | Frozen Performance |
|---|---:|---:|---:|---:|---|
| 10 GB / Pebble | 10,000,007,168 | 4/4 | 50.8 / 50.4 / 52.5 | 219,615,232 | accepted |
| 10 GB / RAM | 10,000,007,168 | 4/4 | 9.1 / 9.0 / 9.1 | 13,427,187,712 | accepted |
| 10 GB / KV | 10,000,007,168 | 4/4 | 47.8 / 46.8 / 49.2 | 10,770,669,568 | rejected |
| 10 GB / auto | 10,000,007,168 | 4/4 | 110.4 / 106.0 / 113.4 | 10,831,523,840 | accepted |
| 100 GB / Pebble | 100,000,006,144 | 4/4 | 508.2 / 504.5 / 523.9 | 371,036,160 | accepted |
| 100 GB / RAM | 100,000,006,144 | 4/4 | 78.6 / 78.5 / 79.3 | 104,500,310,016 | accepted |
| 100 GB / KV | 100,000,006,144 | 4/4 | 1,601.3 / 1,570.6 / 1,616.7 | 109,919,449,088 | accepted |
| 100 GB / auto | 100,000,006,144 | 4/4 | 2,085.3 / 2,069.9 / 2,100.9 | 111,494,991,872 | accepted |

Every large case completed exact ciphertext/sequence/digest validation,
1,536 reads interleaved through 3 GiB allocation churn, actual GC and cleanup,
with no missing peak metrics. At 100 GB RAM retained 103,226,015,744 arena
bytes, including 100,109,869,432 live span bytes and 3,116,146,312 unused
bytes. Its retained managed charge was 129,199,218,112 bytes. KV/auto charged
139,586,478,144 bytes for a 111,669,149,696-byte mapping ceiling. All remain
below the unchanged 150,323,855,360-byte managed cap. These are distinct
capacity, managed-charge and sampled-residency measurements, not interchangeable
whole-process guarantees. Live span bytes include retained reusable overwrite
capacity. RAM GC cycles were 12-13 at 10 GB and 15-16 at 100 GB; KV 100 GB
cycles were 8,694-8,847, and auto 5,512-5,829.

### Final Re-evaluation

The five recommendations and exercised safety/semantic gates are complete.
No unresolved implementation finding remains in this remediation scope.
Both fresh matrices completed in one capture each; no cases were dropped,
stitched or reclassified to obtain approval. Historical exhausted prefixes are
not compared as completed-workload speedups. Source and commit completion
artifacts receive a separate seal, preserving the original capture manifests.

Frozen acceptance is not a total-throughput gate: KV and auto 100 GB are
slower than Pebble despite passing the GC/assist/pause/read/write-tail gates.
The 10 GB KV gate failures remain rejected; single-invocation CLI p99 and other
unknown gates remain incomplete. Generated/warm-cache/NFS-media/production-SLO
limitations persist. Linux cache census availability is not promised on older
kernels or other platforms; conservative RSS-only admission may exhaust sooner.
There is no general guarantee that arbitrary 100 GB distributions fit.
M6, production trials, runtime tuning, defaults and Pebble removal remain
unapproved.