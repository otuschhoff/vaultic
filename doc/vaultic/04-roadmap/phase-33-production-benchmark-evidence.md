# Phase 33 Production Benchmark Evidence

## Offline roadmap gates after writer recovery (2026-09-30)

No production trial, deployment, configuration change, or production snapshot
was performed. The recovered writer stayed at PID 717448, epoch 92; its
installed daemon SHA256 is
`1348a1af915cec718cb0f8f461eff9d6a35df5370f13b170ce59b39c832e547a`.
Build targets, Go caches, and test sockets were on RAM-backed `/run`, not the
slow VM vDisk. Disposable publication and allocated-publication crash databases
were outside the live database, on the same NFS metadata mount.

Raw output is retained under
`/volume2/NASDA2/rustic/db.test/offline-durability-20260930-qB8sIO`.
The isolated debug crash-test daemon used `test-failpoints`, SHA256
`95644ce1413ffbaf23ce6c5dbafb36a83d6815ba705d217198c2f7740e4fe22b`.
The installed release daemon does not activate these test barriers and cannot
substitute for that binary. Publication measurements used the unchanged release
daemon, not the debug build.

### Scheduling and per-group attribution

Two repetitions per mode/interval used 64 inodes, each with 64 shared content
IDs, in one stream with four records per group. Timers excluded startup, reuse,
snapshot construction, reopen validation, and shutdown.

| Flush interval | Ordinary grouped seconds | Atomic-group seconds | Successful Commits, grouped / atomic | WAL PUT attempts, grouped / atomic |
| --- | --- | --- | --- | --- |
| 100 ms | 4.834-4.839 | 3.023-3.223 | 80 / 16 | 96 / 32 |
| 25 ms | 2.826-2.920 | 2.119-2.226 | 80 / 16 | 160 / 32 |
| 10 ms | 2.718-2.736 | 2.046-2.159 | 80 / 16 | 160 / 32 |

All these runs had zero failed, canceled, or timed-out Commits and idle writer
cleanup. Grouped daemon CPU was 1.37-1.48 s, 1.45-1.50 s, and 1.57-1.62 s,
respectively; atomic-group CPU was 1.01-1.09 s, 1.12-1.19 s, and 1.16-1.28 s.
End-of-measurement RSS was about 22-26 MiB, not a peak-memory measurement.
Shorter cadence increased ordinary grouped WAL activity; 10 ms gave relatively
little further wall-time benefit over 25 ms.

The existing attribution fixture now accepts
`VAULTICDB_TEST_PUBLICATION_PROGRESS=1` for single-stream per-group traces.
It records existing-record lookup, revision reservation, planning, Commit,
worker-join wait, group wall time, queued fixture inodes, groups remaining, and
changed/reused counts. Planning/Commit histograms reuse bounded, thread-safe
duration buckets. Commit timing includes separate revision-reservation Commits.
Allocation timing includes its Commit and worker-join wait overlaps worker
execution; these counters must not be added as disjoint wall-time phases.

In `stages-nfs-25ms.txt`, mean grouped group wall time was 180.63 ms, allocation
24.99 ms, existing-record lookup 3.75 ms, summed planning 111.49 ms, and summed
Commit 98.18 ms. Atomic groups averaged 129.02 ms wall time, 1.35 ms existing
lookup, 95.92 ms planning, and 19.38 ms Commit. Both backlogs drained from 64
inodes to zero. Reuse groups recorded no allocation, planning, or Commit work.
This is a fixed synthetic backlog, not evidence about production queue growth
or encrypted reads over the complete imported metadata keyspace.

### Correctness and contention

`VAULTICDB_TEST_DURABILITY_FLUSH_MS` now controls existing test fixtures without
changing their defaults. Crash-before-apply, applied-withheld-response,
before/after durable fence, final-publication restart, accepted-Commit
cancellation ordering, canceled-before-dispatch rollback, and lost-response
recovery passed at 100, 25, and 10 ms. Allocated single/group publication also
passed intervening-fence retries, cancellation, and buffered/durable/acknowledged
crash/reopen checks at every interval. Focused race checks passed shared-content
read-your-writes, bounded-input/partial-publication rejection, and mixed reuse
and failure cases.

The attribution fixture now builds and reopens an immutable snapshot root.
Canonical logical root and inode metadata bytes agree across scalar, ordinary
grouped, individually atomic, and atomic-group modes for shared and nonshared
content. Revision-key identities are excluded from this logical comparison.
This is metadata-tree equivalence, not a production snapshot/restore acceptance.

The four-stream, 256-inode shared-content check at 25 ms rejected broad atomic
group adoption: ordinary grouping took 8.864 s with 320 successful and 6 failed
Commits; atomic grouping took 10.560 s with 64 successful and 139 failed Commits.
Retries recovered, both writers settled idle, and logical snapshot hashes
matched, but atomic-group daemon CPU rose from 6.17 s to 17.64 s. A smaller
nonshared-content fixture also favored ordinary grouping. Fewer durable
transactions alone are not a throughput win under allocation contention.

### Decision

Keep ordinary grouped publication and the production 100 ms interval unchanged.
25 ms is an offline scheduling candidate that passed the tested correctness
gates, not a remedy for R62's blob-admission deadline. These small sequential
fixtures do not establish production read-tail behavior or a safe trial cap.
A separately approved matched trial still needs pinned identities, exact
client/daemon Commit parity, measured live backlog/rate, exit 0, exactly one new
snapshot, and representative byte-for-byte restores. No new trial is authorized
by this offline work, and neither a longer cap nor these metadata fixtures
substitutes for those acceptance gates.

## Offline blob-size read-path discrimination (2026-09-28)

The NFS error-class attribution below was committed locally as
`527f97a6f018e9189592a1abbecfe7c2a0ce1a15`; no push or deployment followed.
The subsequent lookup investigation is a separate offline test; it did not
change the production daemon or its settings.

The R45 candidate emitted 118,553 single-handle size RPCs (18.959 ms mean),
209,914 engine key reads and 544,979 object-store GETs. The archived daemon
startup journal explicitly reports `slatedb_multiget_configured: false`, and
its engine MultiGet counter stayed at zero despite the client using the
`ReadSession.MultiGet` gRPC path. `Storage::multi_get` falls back to individual
transaction gets when this switch is off. Aggregate engine reads and object
GETs also include other work; their ratios are not exclusively attributable to
blob-size lookups or proof that enabling the switch reduces total cost.

An isolated memory-store process test now calls `LookupBlobSizesContext` for
two present and one missing blob in a pinned read session, once each with the
default-off and explicitly-on daemon configuration. Both modes return identical
sizes and record three engine key reads; only the enabled mode records one
engine MultiGet call. The missing blob is published after the session opens, so
its continued absence also checks snapshot isolation in both modes. The test
passes three race-detector repetitions and uses temporary daemon sockets and
stores, not the production repository. Existing Rust storage tests separately
cover ordering and response limits in both modes.

An isolated benchmark of repeated warm reads in the same session excludes
daemon startup, record seeding, session creation, status sampling and shutdown
from its timer. Five non-race repetitions of 1,000 calls per case passed exact
engine key-read/MultiGet and no-write gates:

| Handles per call | Mode off median (range) | Mode on median (range) | Engine MultiGets per call |
| --- | --- | --- | --- |
| 1 | 534.723 us (513.645-542.619) | 516.829 us (503.072-525.423) | 0 / 1 |
| 8 | 953.464 us (937.793-960.935) | 694.494 us (683.397-723.402) | 0 / 1 |

Go allocations were 117 per one-handle call and 161 per eight-handle call
in both modes. The eight-handle median is 27.2% lower when enabled, while the
one-handle median is only 3.3% lower in this run. The timer includes client
validation, gRPC, server reads, response transfer and decoding; it does not isolate
server-side read or decode cost. Memory-store warm reads cannot predict
object-store-backed performance or concurrent backup throughput, especially
since R45 mostly made single-handle requests. This switch is not recommended
for production on the basis of this microbenchmark.

Raw output is retained at `/run/vaultic-multiget-offline-URpxMc/benchmark.txt`
(ephemeral across reboot), SHA256
`bc3e423772c1697f2e54924ab542237ca8c60b33c2bf9345763b5d6028f60ac6`.
The test daemon SHA256 is
`2847e1e97defefbf9089723935022704026f9eae7eb436ce35f9973be5fb2cb9`.
Reproduce with `VAULTICDB_TEST_BINARY` set to that isolated test daemon and
`go test ./internal/index/daemon -run '^$' -bench
'^BenchmarkProcessBlobSizeMultiGetModes$' -benchtime=1000x -count=5`.

A second offline run repeats the decoded lookup alongside a raw pinned-session
`MultiGet` with the same keys, omitting Go blob-size decoding and result
reordering. Both still include gRPC and server response construction; each
case again passed exact read/MultiGet and no-write gates over five 1,000-call
repetitions:

| Path, keys | Mode off median (range) | Mode on median (range) |
| --- | --- | --- |
| Decoded, 1 | 522.837 us (510.796-531.676) | 526.869 us (517.424-534.714) |
| Raw, 1 | 520.919 us (514.344-538.344) | 516.786 us (510.066-527.872) |
| Decoded, 8 | 952.338 us (940.954-970.294) | 703.982 us (687.070-747.590) |
| Raw, 8 | 963.422 us (938.499-993.147) | 688.967 us (673.932-694.665) |

The eight-key mode difference persists without Go blob-size decoding; its
median is 26.1% lower for decoded and 28.5% lower for raw reads. One-key
distributions overlap and their median direction differs between runs and
paths. The raw path allocated 103 objects per one-key call and 126 per
eight-key call versus roughly 117 and 161 for the decoded path. Differences
between path medians cannot be subtracted into a reliable decode time: cases
ran sequentially in separate processes, with no paired request or server-only
timer. This does not distinguish engine read time from service and transport
cost, and still says nothing about production object-store reads or throughput.

Second raw output: `/run/vaultic-multiget-raw-JJ1K6g/benchmark.txt`
(ephemeral across reboot), SHA256
`b9ba9d19dad12e4a80a4cbcf381a244c04a32c0f52fedc99d33a816b256f56a0`;
the test daemon SHA256 remained
`2847e1e97defefbf9089723935022704026f9eae7eb436ce35f9973be5fb2cb9`.
Reproduce with `-bench '^BenchmarkProcess(Raw|BlobSize)MultiGetModes$'
-benchtime=1000x -count=5` and the same isolated daemon.

This establishes a concrete offline read-path and latency difference, not a
production latency or throughput benefit. Cross-caller batching was already
rejected in R43; enabling the SlateDB switch is a separate candidate requiring
isolated read/decode profiling, safety checks and fresh approval before
production comparison. The
daemon setting, installed binaries, previous acceptance gates and sealed trial
artifacts were not changed.

## NFS error attribution follow-up (2026-09-28)

The finalization ownership fix, tests, deployment and R45 evidence were
committed locally as `a047938996331daac2be616c7b0b4e19f136909f` with a detailed
message. No push was performed. The attribution work in this section was
subsequently committed as `527f97a6f018e9189592a1abbecfe7c2a0ce1a15` and
remains undeployed.

Inspection of the NFS scheduler confirmed that its existing `errors` counter
combines missing-path replies with permission, stale-handle, transport and other
failures. The pinned client converts NFS3ERR_NOENT into `os.ErrNotExist` and
NFS3ERR_PERM into `os.ErrPermission`, while NFS3ERR_ACCES and stale-handle
statuses remain typed NFS errors. A breakdown based only on typed NFS errors
would therefore miss important categories.

Each operation now has fixed `error_classes` counters for `cancelled`,
`deadline_exceeded`, `not_found`, `permission`, `stale_handle`, `not_directory`,
`nfs_other`, `transport` and `other`. Classification handles wrapped standard
and typed errors, and covers failures before connection admission as well as
returned call failures. The existing aggregate error and cancellation totals,
EOF exclusion, returned errors, retries and scheduling are unchanged. Context
deadline expiry is distinct from transport-level timeout; the latter is not
added to the existing context cancellation/deadline total. No paths, handles or
raw error strings become telemetry labels.

Focused race tests cover nil/EOF, cancellation/deadline, missing paths,
permissions, stale/bad handles, not-directory, server I/O, closed/short/timeout
transport and unknown errors, including wrapped forms. Native wire regressions
verify classified failures remain counted through successful fallback and
failed retry, and the existing source-close JSON test verifies propagation.
The NFS-focused race suite, JSON reporting test and changed-code lint pass.
Class totals reconcile with aggregate errors after settlement; concurrent
snapshots can briefly see partial atomic updates.

This is future attribution, not a performance optimization or a retrospective
classification of R45's 50,475/52,722 LOOKUP errors. Missing paths can arise
from expected marker probes or from real source changes; caller intent still
matters. Close-time reconciliation failures also remain unclassified. No
failure gate was relaxed, no production process or setting was changed, and
no new live workload was run. Sealed historical archives remain untouched.

Next work is to obtain classified outcomes in a separately approved trial and
attribute the remaining per-key metadata read/decode cost before selecting a
performance change. Repeatable throughput, full backup completion and restore
validation remain outstanding.

## Finalization ownership and R45 matched pair (2026-09-28)

Following explicit approval, the accepted-commit/rollback race was fixed and
validated offline, a release daemon was gracefully deployed, and one matched
baseline/candidate pair was run at 600 seconds plus 45 seconds grace each.
No additional live run or full backup/restore validation was performed.

Commit and Rollback now share a per-transaction service lock acquired after
request validation and before asynchronous authority checks. The first owner
retains ownership through storage finalization and writer accounting. The weak
lock registry prunes inactive entries on subsequent requests; unrelated
transactions do not share their finalization locks. Storage validation,
idempotency, status codes and client cancellation behavior are unchanged.
This fixes rollback overtaking an already-owned commit, not every possible
unknown transaction or finalization failure.

The updated deterministic regression failed against the prior daemon because
rollback completed while an accepted commit was blocked. With the fix, both
before-remove and before-apply cases, for read-only and mutating transactions,
retain successful server commits; rollback waits and then reports NotFound.
Reopening confirms committed writes survive. A separate cancelled-before-dispatch
test confirms rollback still removes uncommitted data when Commit never reached
the daemon. Ten race-detector repetitions also cover lost-response recovery and
durability tokens. Rust tests verify independent locks and cancelled-waiter
cleanup. Twelve service tests, neighboring publication/writer-fence/durability
tests, changed-code lint, formatting and exact-release native checks passed.

The deployment preserved the prior daemon and configuration, required an idle
writer, used handled SIGINT rather than SIGTERM, verified clean exit before
atomic replacement, and restarted the unchanged unit. PID 1572244/epoch 75
became PID 1617561/epoch 76, read-write, with identical 143 snapshot IDs and
unchanged 128 MiB metadata cache, 512 MiB block cache and 100 ms flush interval.
The installed release SHA256 is
`6733b8c867f75c86dd73b0f8e85e2d2f641389103eae370c1f8cdd470cbd507e`.
Deployment artifacts are retained at
`/volume2/NASDA2/rustic/db.test/finalization-deployment-20260928-zvPLOm`.

R45 artifacts are at
`/volume2/NASDA2/rustic/db.test/empty-debt-comparison-20260928-r45-i9euDM`.
Both runs used that same daemon, 52 sources, 4 readers, 8 NFS connections,
explicit `--use-cwalk --nfs-direct`, 32 cwalk workers and 64 MiB client cache.
No builds or tests ran concurrently with either measurement. The archived R43
baseline CLI SHA256 is
`b67d19ebe426bbdef1cfb3f407db19af44db8bfcd2b4c9e65b5f13d3c0cbc9c3`;
the candidate CLI built from `5419edd0b` has SHA256
`f8136ec99463aa111f6965b7e04094321e9ab6304c638369abf12279e0a6c714`.
The only production Go change between their source revisions is the empty-debt
guard. The Rust finalization source patch is preserved in both new archives.

| Metric | Baseline | Empty-Debt Candidate |
| --- | --- | --- |
| Runtime / exit | 600.294 s / 124 | 600.305 s / 124 |
| Files at 599 s | 47,236 | 48,443 |
| Logical MiB/s | 145.881 | 147.999 |
| Commit attempts / successes | 45,519 / 45,519 | 0 / 0 |
| Commit failures / cancellations / timeouts | 0 / 0 / 0 | 0 / 0 / 0 |
| Metadata GETs | 646,898 | 544,979 |
| Aggregate GETs per size-lookup handle | 5.587 | 4.597 |
| Mean size-lookup RPC | 19.441 ms | 18.959 ms |
| CLI CPU seconds | 1,283.21 | 1,292.59 |
| Daemon CPU seconds | 4,566.28 | 4,390.88 |
| CLI peak RSS KiB | 1,029,620 | 1,047,060 |
| Daemon sampled peak RSS bytes | 1,175,781,376 | 1,191,784,448 |
| Engine writes | 0 | 0 |

Both individual safety gates passed: counters settled without commit
non-successes or failure diagnostics, no reported source or sampler errors,
zero final transactions/write intents, empty scratch, and unchanged snapshot
membership. The baseline's 45,519 real empty commits provide live coverage of
the new finalization implementation; the candidate removes those calls entirely.
Neither result retroactively reclassifies the rejected R43/R44 failures.

The observed changes are +1.45% logical throughput, +2.56% files, -15.76%
metadata GETs and -3.84% daemon CPU time, but +0.73% CLI CPU time and slightly
higher peak memory. This single baseline-then-candidate pair has uncontrolled
metadata cache warmth, host activity and processed work mix. It establishes
the removed transaction work, not a repeatable speedup or lower full-backup
runtime. Logical bytes are not newly uploaded bytes, and logical metadata body
bytes (1.546/1.570 trillion) are not physical I/O.

Remaining evidence points to per-key metadata lookup cost: the candidate made
118,553 single-handle size RPCs averaging 18.959 ms, and the 360-second stack
shows four MultiGet callers with blob-save workers waiting on lookup results.
Its direct READ mean queue/service times were 0.0083/0.9858 ms, max active four,
with no connection shortfalls; client-cache peak occupancy was 33.9% with no
evictions. More NFS sockets or client-cache capacity is not indicated by this
run. The CLI CPU profile is led by SHA-256 (24.05% flat samples) and chunk split
calculation (10.14% flat); daemon CPU still averages 7.30 cores and needs its own
per-key read/decode attribution before another metadata optimization.

Important unresolved observations: direct LOOKUP errors were 50,475/52,722 and
remain unclassified; close-time reconciliation failures were 1,724/2,924; the
candidate had three READ cancellations at shutdown. These are not promoted to
success or claimed resolved by the commit-specific safety gate. Both runs timed
out, so full completion and restore correctness remain unverified. The installed
daemon includes the uncommitted finalization patch on `5419edd0b`; no new commit
or push was made in this approval step, and the installed profile CLI was not
replaced. Historical sealed artifacts remain untouched.

## R46 reverse-order matched repeat (2026-09-28)

With fresh approval for the bounded matched comparison, R45's exact CLI
binaries, 52 sources, adopted daemon and settings were reused without a build,
restart or configuration change. This time the candidate ran before the
baseline; each run was capped at 600 seconds plus 45 seconds of termination
grace. Separate private artifacts, including copied run/analysis scripts,
per-run status and process samples, CPU profiles, NFS counters and stack
captures, are at
`/volume2/NASDA2/rustic/db.test/empty-debt-repeat-20260928-r46-b4P4gE`.
The copied analyzer's cache-warmth caveat was corrected for this ordering;
the R45 archive was not modified.

| Metric | R46 baseline (second) | R46 candidate (first) |
| --- | --- | --- |
| Runtime / exit | 600.307 s / 124 | 600.299 s / 124 |
| Files at 599 s | 48,154 | 47,974 |
| Logical MiB/s | 146.734 | 146.345 |
| Empty commit attempts / successes | 45,519 / 45,519 | 0 / 0 |
| Commit failures / cancellations / timeouts | 0 / 0 / 0 | 0 / 0 / 0 |
| Metadata GETs | 545,522 | 537,253 |
| Single-handle size RPCs / mean | 117,725 / 18.990 ms | 117,281 / 18.931 ms |
| CLI CPU seconds | 1,318.40 | 1,262.71 |
| Daemon CPU seconds | 4,615.63 | 4,329.33 |
| Engine writes | 0 | 0 |

Both individual safety gates passed: unchanged 143 snapshot IDs, no reported
source or sampler errors, zero final transactions and write intents, no commit
failure diagnostics, empty scratch, and unchanged daemon PID 1617561, epoch 76
and installed binary SHA256
`6733b8c867f75c86dd73b0f8e85e2d2f641389103eae370c1f8cdd470cbd507e`.
The earlier R45 baseline-then-candidate pair showed +1.45% candidate logical
MiB/s and +2.56% files; this reverse-order pair showed -0.27% and -0.37%.
Removing empty commits is repeatable, as are lower candidate daemon CPU
(-3.84% in R45, -6.20% in R46), but a throughput or total-runtime gain is
not established. The candidate's metadata GET advantage also narrowed from
15.76% in R45 to 1.52% in R46. Sequential order, cache warmth, host activity
and work mix still confound either pair; logical bytes are not uploaded bytes.

The R46 candidate's 360-second stack again has size lookups waiting in
`ReadSession.MultiGet`. All size requests remained single-handle and the
client result cache recorded zero evictions. Its CPU profile had 24.12% flat
SHA-256, 11.98% syscall and 9.95% chunk split-point samples; these are CLI
CPU costs, not isolated metadata-server read times. Direct NFS LOOKUP recorded
52,102/52,256 errors and reconciliation close-time failed counts were
2,459/2,636 (candidate/baseline). Those outcomes remain unclassified in
these binaries: the newer NFS error-class attribution was not deployed.
Candidate/baseline READ cancellation counts of 2/4 at shutdown also remain
visible, not treated as successful reads. Neither bounded run completed a
backup or validated restore. Further changes should target read/decode
attribution and error classification offline; full completion and restore
need a separately scoped live run.

### Offline Go lookup discriminator

An in-process fake MultiGet response with a canonical encoded blob record
times the unchanged `ReadSession.LookupBlobSizesContext` validation, key
construction, response checking, blob-size decoding and result mapping without
starting a daemon. Five 100,000-call repetitions measured a median 3.135 us
(3.127-3.152 us) for one key and 6.928 us (6.868-7.030 us) for eight keys;
allocations were 27 and 55 per call respectively. Raw output is retained at
`/run/vaultic-lookup-inprocess-GM6VNc/benchmark.txt` (ephemeral across reboot),
SHA256 `ca30f03d59fa2d266d93660da7b47aabaecf65c1e452434ab12afd597e06c980`.
Run `go test ./internal/index/daemon -run '^$' -bench
'^BenchmarkBlobSizeLookupInProcess$' -benchtime=100000x -count=5` to repeat.

This discriminator excludes protobuf wire encoding/decoding, Unix transport,
daemon request processing and object-store/engine reads; the fake reuses its
encoded value. It cannot be subtracted from R46's approximately 19 ms
single-handle RPC mean to derive server time or predict throughput. Its
microsecond-scale client-side path does make a large *local* blob-size decode
cost unlikely in this fixture; server-side read and transport attribution
remain open.

## R47 one-hour completion attempt stopped at write-phase gate (2026-09-28)

The user authorized a single production completion attempt capped at one hour
with representative-file restore if it finished. R46's exact candidate CLI,
52 sources, adopted PID 1617561/epoch 76 daemon, settings and direct-NFS
options were retained. A private copy of the harness at
`/volume2/NASDA2/rustic/db.test/backup-completion-20260928-r47-5RPMbd`
used a 3,600-second hard cap plus 45 seconds of cleanup grace; its analyzer
self-tested successful-publication and timeout snapshot gates. The earlier
archives, installed binary and unit were not changed.

After the run entered publication work, the inherited zero-commit-failure
gate triggered at 786 seconds, so the CLI was stopped with exit 130 instead
of waiting
for the one-hour cap. It had processed 57,909 files and 134,749,470,374
logical bytes at 785 seconds. Writer status recorded 116 commit attempts:
108 successes and eight failures, all diagnosed as `Aborted` at
`storage_commit` with consumed transactions; no cancellations or timeouts.
There were 409 engine writes and 79 acknowledged inode publications, with one
revision-allocation failure reported. Go publication and allocation code
retries `Aborted` conflicts, but the aggregate counters do not establish that
each failed attempt later succeeded. The analyzer therefore **rejected** the
run; its commit-non-success and early-stop gates were not waived.

An offline-only classifier in the private R47 analyzer now distinguishes
zero failures, up to eight fully observed `storage_commit / Aborted / consumed`
diagnostics, and unclassified outcomes. Its synthetic self-tests reject
missing diagnostics, other statuses and counts beyond the complete diagnostic
window. R45/R46 have zero failures and diagnostics; R47's eight are all
observed `Aborted`, but R47 remains rejected because the classifier cannot
link each failed commit to a successfully retried logical operation. The
copied live harness was not changed or rerun after this analysis.

The process exited after cancellation with no reported source errors, no
active transactions or write intents, an empty metadata scratch directory,
and the same 143 snapshot IDs. The daemon stayed read-write on PID 1617561,
epoch 76, with the unchanged installed binary SHA256
`6733b8c867f75c86dd73b0f8e85e2d2f641389103eae370c1f8cdd470cbd507e`.
No new snapshot exists to restore, so representative-file restore was not
possible. These checks do not prove the already-written metadata is free of
pending export work or validate backup completion. Do not repeat this live
attempt under the old gate or interpret this as a full-backup runtime result;
first distinguish retryable write conflicts from unresolved publication
failures using an offline-validated write-phase safety criterion.

## R48 offline gate and blocked preflight (2026-09-28)

With approval to prepare another bounded completion attempt, an instrumented
debug CLI and a private R48 harness were built under
`/volume2/NASDA2/rustic/db.test/backup-completion-20260928-r48-Jd5lex`.
Native race-checked tests reconcile recovered allocation and publication
commit retries with failed commits in isolated workloads; the backup JSON test
covers nonzero commit-stage retry outcomes. The R48 analyzer's synthetic cases
reject missing diagnostics, unmatched recovered counts, terminal reconciler
failures, cancellations and timeouts. This does not prove that every kind of
production commit failure originates in those two operations.

Read-only preflight confirmed the same PID 1617561, epoch 76, daemon binary,
configuration, idle transaction state, 52 sources and 143 snapshot IDs.
It then **blocked before starting a backup**: R47 had already consumed the
first eight `storage_commit / Aborted` diagnostics in that daemon process.
The daemon emits subsequent occurrences only at powers of two, so a new run
could not require complete diagnostics for failures 9 through 15. The R48
preflight fails closed on an exhausted diagnostic window; no daemon restart,
CLI deployment, backup or restore occurred. The previous R47 rejection stands.
The private R48 artifact contains source patch and binary hashes, analyzer
self-tests and the blocked preflight record. Source telemetry changes remain
uncommitted; a safe way to restore complete failure attribution (or explicit
operational review of a daemon restart) is required before a live retry.

## R49 complete client commit accounting and stopped run (2026-09-28)

The approved second path added a per-client ledger at the Go transaction
`Commit` RPC boundary (normal, idempotent, deferred and fenced calls) and a
final `commit_rpc_stats` backup JSON record after repository cleanup. Native
allocation and shared-content publication tests match client RPC abort totals
against daemon failed-commit deltas in isolated workloads. The private R49
harness at `/volume2/NASDA2/rustic/db.test/backup-completion-20260928-r49-dhPr1J`
requires exact final client/daemon attempts and outcomes, and all client
`Aborted` returns to be explained by successful commit-stage allocation or
publication retries. Unexplained failures, terminal reconciliation failures,
nonzero cancellations/timeouts, and snapshot mismatch still reject the run.
Thirty-six synthetic gate cases and focused native/reconciler/backup races,
full reconciler and backup tests, vet, and preflight passed before execution.
The same daemon PID, epoch, binary, settings and 52 sources were retained;
there was no restart or production binary replacement.

The single capped R49 backup was **stopped and rejected** at about 1,005
seconds when the predeclared 64-failure live budget was exceeded. At 1,000
seconds it had processed 86,085 files and 155,524,358,512 logical bytes.
The settled daemon recorded 4,412 commits: 4,347 successes, 65 failures,
zero cancellations and zero timeouts. All 65 client `Aborted` responses were
attributed to recovered inode-publication commit retries, across 61 logical
calls. The final client ledger recorded 4,346 successes and one cancellation
instead of the daemon's 4,347 successes: shutdown canceled a client-side
Commit response while an active daemon request subsequently succeeded.
The cancellation also produced one terminal revision-allocation call and
26,451 close-time reconciliation failures. This is **not** an accepted
full-run result, nor evidence that cancellation was harmless. No snapshot
was added (143 IDs remain), so no representative restore was possible.
The daemon remained PID 1617561/epoch 76, read-write, with zero active
transactions and write intents and an empty metadata scratch directory.
Do not raise the live conflict budget or repeat the run solely to reach
completion without reviewing the conflict rate, stop semantics and final
attribution gate. R47 and R48 remain rejected/blocked respectively.

## Isolated publication admission candidate (2026-09-28)

The candidate adds `BeginPublication` to reserve unique content IDs in the
daemon before opening a serializable transaction. Permits are held through
storage Commit or Rollback and released on expiry; canceled admission releases
partial acquisitions. The Go inode planner and overflow checks remain unchanged.
An older daemon returns `Unimplemented`, so the client falls back to normal
Begin and its existing retry behavior. Requests with more than 4096 distinct
content IDs also use normal Begin. Changed inodes reserve both new and previously
observed content IDs. If the prior inode changes between that observation and
admission, or if another writer bypasses this RPC, conflicts can still occur;
concurrent revision-block reservations can also conflict. Existing `Aborted`
retries remain required.

On disposable databases using the development daemon, three repetitions of
32-inode, one-content-ID grouped publication had zero failed commits with
distinct content and zero with shared content; the unchanged deployed binary
had 48 recovered failures for the shared case. With four independent streams
and two IDs per inode, publication-specific recovered Commit aborts remained
zero, while revision-allocation conflicts still occurred. Four concurrent
updates retiring the same old content ID also produced zero failed commits and
correct new inode content in two repetitions. The native
reservation lifecycle test and affected Go daemon/reconciler/backup suites
passed. The development daemon was not deployed and no production backup was
performed; the R49 conflicting keys remain unproven.

## R50 bounded backup on publication-admission daemon (2026-09-28)

The committed `37325adb2` daemon was built and deployed as SHA256
`1348a1af915cec718cb0f8f461eff9d6a35df5370f13b170ce59b39c832e547a`.
The prior executable and exact candidate are preserved under
`/volume2/NASDA2/rustic/db.test/vaulticdb-admission-20260928-c0PjDG`.
After a graceful stop, the service reopened read-write at epoch 77 on
PID 1832232; unit and environment hashes were unchanged and all 143 snapshot
IDs remained present. A matching debug CLI and an R49-derived, private
600-second harness with 45 seconds of cleanup grace are under
`/volume2/NASDA2/rustic/db.test/backup-admission-20260928-r50-zyIwnt`.
Its 36 analyzer self-tests and live identity, source and idle-writer preflight
passed. The old R49 evidence and harness were not modified.

The single trial ended at 600.232 seconds with wrapper exit 124 and completed
cleanup. At 599 seconds it had processed 45,705 files and 87,207,269,602
logical source bytes. The daemon recorded zero Commit attempts, failures,
cancellations or timeouts; the CLI's final Commit RPC counters agree. There
were no engine writes or compactions, source error records, sampler errors,
active transactions or write intents; scratch was empty and snapshot IDs
remained unchanged. However, the final reconciler reported 462 failed items,
and the canceled CLI logged a terminal metadata-admission error. The
predeclared safety analyzer therefore **rejected** the trial for terminal
reconciliation failure. This is not a completed-backup, restore, or successful
publication-admission acceptance result; no inode publication reached Commit.
Do not reclassify the close-time failures after the run merely to make it pass.

Source-side direct NFS completed 1,446,069 READ calls with mean service
1.011 ms and mean queue 0.009 ms, delivering 87,212,540,760 bytes; four
READs were canceled at shutdown, with no transport, timeout, stale-handle or
permission errors. The 48,159 LOOKUP not-found replies were classified as
not-found, not transport failures. Metadata lookups instead made 111,507
one-handle size RPCs averaging 20.176 ms, for 2,249.74 seconds of overlapping
RPC time across four slots (3.75 concurrent on average). The 550-second CLI
stack shows four workers waiting in `ReadSession.MultiGet`; client result
cache recorded zero evictions. These signals make the metadata read path a
stronger bottleneck candidate than slow source NFS responses, but overlapping
work prevents a definitive per-file critical-path attribution.

VaulticDB used 4,331.23 CPU seconds (7.22 cores average, including 3,243.15
user and 1,088.08 system seconds), served 617,897 main-store GETs, and
reported 1,481,552,832,719 logical delivered body bytes, 16.99 times the
source READ bytes. This is logical object-store delivery, not physical disk or
network traffic. It indicates substantial metadata read amplification under
the current 128 MiB metadata cache and non-native MultiGet mode; the daemon
CPU was read-side workload, not publication or compaction. The captured CPU
profile is **CLI only** (SHA-256 and chunking lead); no daemon CPU profile
was captured, so precise Rust hot functions and decryption share remain
unproven. An instrumented daemon profile or controlled metadata-read probe is
needed before changing source concurrency, cache budgets or durability policy.

## R51/R52 canceled-work attribution and daemon CPU profile (2026-09-29)

`Reconciler.Metrics` now exposes `failed_canceled` as a subset of the unchanged
`failed` count. Both wrapped Go `context.Canceled` and gRPC `Canceled` are
classified, while other errors stay outside the subset. The final backup JSON
retains the counters after reconciler join. Focused reconciliation/backup tests
passed three race-detector repetitions; `go vet` passed. Private scripts and
artifacts reside in `db.test/backup-read-profile-20260929-r51-BxcFRr` and
`db.test/backup-read-profile-20260929-r52-7wope8`. The diagnostic debug CLI
has SHA256 `ec4e49f837ed2d93f115adbe53de0bd50e1feb4af13109ab7ef2fe2cf169fe82`;
the production daemon remains SHA256 `1348a1af915cec718cb0f8f461eff9d6a35df5370f13b170ce59b39c832e547a`.

R51 stopped at about 151 seconds and is **rejected**: its harness mistook the
intentional SIGINT used to finish a 30-second `perf` capture for a profiler
failure. The profile was intact (12,930 samples, zero lost samples). All 2,655
reported reconciliation failures were explicitly canceled, with zero other
failures or Commit attempts. A new private R52 copy accepted the SIGINT only
when the runner requested it and a nonempty profile was written. The exit
shape was reproduced offline; 41 analyzer self-tests still reject missing,
partial or excessive cancellation classification, Commit mismatches and
terminal publication failures. R51's decision and the prior R50 evidence were
not rewritten.

R52 passed its **bounded 600-second safety gate**, not full-backup acceptance.
It ended with wrapper exit 124, no early stop, source or sampler errors, zero
Commit attempts, zero engine writes or compactions, empty scratch and unchanged
143 snapshot IDs. At 599 seconds it had processed 49,180 files and
94,633,561,722 logical source bytes. All 337 failed reconciliations at close
were classified as canceled; no non-cancellation failure remained. This
explains the close-time failure category in this new run, but cannot
retrospectively classify every R50 failure and does not prove the admission
candidate under production inode-publication load. The daemon stayed
read-write at PID 1832232/epoch 77 with zero final active work.

R52 source READ calls averaged 0.914 ms service and 0.008 ms queue across
1,568,172 calls. Metadata size lookups were still single-handle: 120,431 RPCs
averaged 18.930 ms. The daemon used 4,424.41 CPU seconds (7.36 cores average)
and reported 558,993 main-store GETs and 1,595,424,624,864 logical delivered
body bytes, 16.86 times the direct-NFS source READ bytes. The 30-second
daemon `perf` profile captured 13,027 cycle samples with zero lost samples.
Flat samples include libc `memmove` 25.56%, AES-GCM assembly 15.75%, kernel
`rep_movs_alternative` 12.08%, libc `memset` 8.45%, CRC32 5.52%, and SlateDB
FlatBuffer index creation/verification. Call stacks place copies in
`EncryptedObjectStore::decrypt_chunks_sync`, `Bytes::copy_from_slice`, and
local object-store reads; kernel copies include NFS-backed page-cache reads.
The separate rejected R51 capture shows similar leading symbols. These are
sample shares, not additive per-request costs or a proof of physical I/O.

Host-wide NFS mountstats for the metadata database mount
`/ncl1-1-vs-50/fme_dump` changed by 1,715,384,278,177 normal-read bytes but
only 22,413,312 server-read bytes during R52. These counters include other
users of the mount and cannot measure server-side disk access. Together with
the profile they support repeated client-side processing of cached metadata,
not an NFS source response bottleneck or 1.6 TB of physical network reads.
SlateDB reported 218,440 point GET keys and 986,130 filter negatives, but
native engine MultiGet calls remained zero. Before changing cache budgets or
concurrency, isolate unnecessary SST/object copying and decrypted-range work
with an offline, representative point-read comparison; a longer separately
approved run is needed to reach inode Commit and restore acceptance.

An isolated release-mode probe of the encrypted object-store wrapper used its
default 256 KiB authenticated chunk size, a warmed header cache, an in-memory
backing store and 500 sequential reads per stage in each of two runs. Raw
64-byte and full-chunk ciphertext range fetches took 0.3-0.6 us/read. Direct
256 KiB authenticated decryption took 70-76 us/read (including output allocation
or an input copy); decrypting through the crypto executor took 95-99 us/read.
Complete encrypted point reads took 89-95 us/read for both a 64-byte range and
a 256 KiB range. This supports full-chunk decryption as the dominant local cost
of small requests; a copy of the returned 64 bytes is not the leading cost.
The probe used synthetic data, omitted NFS and SlateDB index/filter work, and
was removed after measurement. These numbers cannot be scaled to production
throughput or used to justify a cache, concurrency or crypto change.

A second offline probe reused the persisted-SST scan fixture with 4,096
one-KiB records, SlateDB's DB cache disabled, and encryption between SlateDB
and an instrumented in-memory object store. Two release-mode runs returned
identical records and identical underlying GET counts. A full-prefix scan made
1,369 GETs with the default transaction scan options, versus 7 with the
retained stream's existing 1 MiB read-ahead. Mid-scan cursors made 1,028 or
1,027 default GETs versus 6 streaming GETs; an end cursor made 3 in either
case. These counts include other object-store reads and are not a per-SST
physical-byte or latency comparison. The encrypted fixture change was removed
after the runs and the original regression test passed. This confirms a large
fetch-count reduction in that local SST-backed workload, but the retained
production stream already enables 1 MiB read-ahead. It does not explain all
of R52's read-side CPU or prove a faster production backup. A next bounded
offline check should target repeated accesses within the same encrypted chunk
on a non-streaming point-read path, with cache and read-ahead held explicit.

That offline follow-up compared 16 distinct 64-byte ranges within one 256 KiB
chunk: individual encrypted requests took 1,347-1,367 us per group, versus
85-87 us when one full-chunk request supplied all 16 slices. An encrypted
SlateDB SST fixture with 4,096 one-KiB records then compared 16 nearby key
lookups against native MultiGet. With the DB cache disabled, individual
lookups made 53-54 underlying GETs and delivered about 11.4 MB of logical
body data versus 3 GETs and 0.63 MB for MultiGet. With normal caching, a
freshly written fixture made no GETs at all; after reopening separately for
each cold case, individual gets made 8-9 GETs and about 1.94 MB versus 3 GETs
and 0.63 MB for native MultiGet. Both cold orders returned identical values.
All probes used an in-memory object store and synthetic data, were removed,
and the original encryption-range and native MultiGet regressions passed.
These results establish a local repeated-read opportunity, not a production
throughput benefit: R52 size RPCs were still single-handle, and prior R43
cross-caller batching lowered RPC count without improving progress and had
unclassified Commit failures. Do not enable native MultiGet, introduce a
shared decrypted-chunk cache or change production budgets on this evidence.
The R52 bounded-timeout analyzer's 41 offline self-tests and focused Go
reconciliation cancellation/publication tests pass. A run that reaches inode
Commit still needs exact client/daemon outcome attribution and a separate
completion-plus-restore gate; neither is established by these offline probes.

Read-only preflight on 2026-09-29 confirmed the deployed daemon PID 1832232,
SHA256 `1348a1af915cec718cb0f8f461eff9d6a35df5370f13b170ce59b39c832e547a`,
active `vaulticdb-rustic.service`, read-write epoch 77, zero active transactions,
write intents and Commit attempts, and the same 143 snapshot IDs as R52. The
isolated Rust publication-reservation lifecycle regression also passed. The
root filesystem remains full; build/temp space must stay on `/run` and new
trial artifacts on the HDD-NFS test mount. There is no demonstrated runtime
read-path change to deploy, and another 600-second run would not by itself
test publication. A longer publication-reaching run is a separate decision:
first specify its duration, cleanup grace, resource and failure budgets,
exact client/daemon Commit accounting and snapshot/restore gates. Do not
interpret the clean idle preflight as completed-backup authorization.

## R53 capped publication trial (2026-09-29)

The approved single production trial used a private R52-derived harness at
`/volume2/NASDA2/rustic/db.test/backup-publication-20260929-r53-cap1200`.
Only the cap changed to 1,200 seconds with 45 seconds of kill grace; the
64-Commit-failure live stop budget, cancellation/timeout stops, exact identity
and writer checks, and source/cleanup gates remained. Its analyzer added a
requirement for both Commit attempts and inode-publication calls, passed 44
synthetic self-tests, and does not accept early completion without a separate
restore. Read-only preflight passed with the same 52 sources, daemon PID
1832232/epoch 77, binary and CLI hashes, settings and 143 snapshots. Neither
the production daemon nor its configuration was changed.

The wrapper ended at 1,200.847 seconds with exit 124 and no early safety stop.
At 1,199 seconds the CLI reported 106,466 files and 182,615,550,928 logical
source bytes. Unlike R52, this run reached 6,128 inode-publication calls and
8,152 daemon Commit attempts; all 8,152 settled as successes, with zero daemon
failures, cancellations and timeouts. The daemon recorded 30,576 engine
writes, 1,227,552 main-store GETs and 3,167,796,256,106 logical delivered
body bytes. The latter is **not** physical disk or network traffic. The CLI
reported zero source errors and 40,704 failed reconciliations, all explicitly
classified as canceled at shutdown. Scratch drained, the writer remained
read-write and idle, and live rechecks found the same 143 snapshot IDs.

**R53 is rejected by its predeclared publication safety gate.** The final
client Commit ledger reports 8,152 attempts but 8,151 successes and one
`Canceled` response while the daemon reports 8,152 successes. The reconciler
also reports one revision-allocation failure. A canceled client response can
precede successful detached daemon Commit finalization, as covered by an
existing isolated ordering test, but these aggregate counters cannot prove
the affected logical publication was recovered. The CLI also exited with a
context-canceled metadata-admission error at the cap. No new snapshot was
published, so no representative-file restore was possible. Do not waive the
client/daemon mismatch, infer full-backup success, or start another live run
under this gate. First establish an offline-tested shutdown/finalization
criterion that distinguishes pre-dispatch cancellation from an accepted
Commit and links any uncertain outcome to its logical publication.

### R53 shutdown follow-up (offline only)

Schema-store revision reservation and reconciled-revision publication now check
caller cancellation before dispatching a prepared Commit, then allow an
already-dispatched Commit up to the existing independent 10-second RPC deadline
to return its actual result. This covers ordinary inode publication as used in
R53, allocated publication, publication groups and revision blocks. It does
not blindly retry an uncertain Commit or extend the daemon's lifetime. A
Commit with no acknowledged response by that deadline remains unresolved and
must still fail the client/daemon parity gate.

The reconciler now reports canceled revision-allocation failures separately as
a subset of total allocation failures; it does not erase or reclassify the
total. Wrapped Go cancellation and gRPC `Canceled` are both counted. The backup
JSON test checks that the subset is present in final telemetry. An isolated
response-delivery barrier test cancels the caller after the daemon accepts a
Commit and before its response is released, then verifies a successful client
result, exact client/daemon Commit counters, no leaked writer state, and the
published revision. It passes three race-detector repetitions for ordinary
publication, allocated publication and revision-block allocation. Existing
crash-recovery tests also pass with withheld responses and a killed daemon;
the independent deadline remains 10 seconds, not an unbounded drain.

Full race suites for `internal/index/daemon`, `internal/index/reconcile` and
`cmd/vaultic/backupcmd` pass, as do targeted `go vet` and `git diff --check`.
The private prospective R54 analyzer at
`/volume2/NASDA2/rustic/db.test/backup-publication-20260929-r54-prospective`
passes 49 synthetic self-tests. Its timeout exception allows only the
canceled-allocation subset while continuing to reject any client/daemon
Commit mismatch. An uninstalled CLI candidate was built in that private
directory as `vaultic-candidate`, SHA256
`d2b06c2320dc427d1da6a08ae7fe744751053be4644907aaf293d61c59c4451b`.
R53 remains rejected; none of these offline checks proves
that the next production run will publish a snapshot or satisfy a restore
gate. No new live backup, daemon restart, installation or production setting
change was made in this follow-up.

The prospective R54 directory now also contains a private `run.cjs` and
`candidate/` with the pinned CLI, unchanged 52-source manifest and source
identity. It retains R53's 1,200-second TERM cap, 45-second kill grace,
64-failure stop budget, sampling, writer checks and strict post-run analyzer.
The runner additionally rejects source-list drift (SHA256
`01b2d4b2ea54ca86e67649dd71cdf95cae5b401211f4f0b484ce431ab99173d6`)
or snapshot-membership drift from the 143-ID R53 baseline (sorted-ID SHA256
`c81d1eb3b233286835c2b0f8ba004d6739d5116deea548bcd0cca0774c16d0e6`).
Read-only `--preflight` passed with eight NFS connections, four readers,
52 accessible source directories, the pinned CLI and daemon binaries, epoch
77, read-write idle writer, unchanged deployment settings and exact snapshot
membership. Scratch and result mounts have free space. The runner has not
been started at preparation time and `candidate/started.json` did not exist.
Repeat preflight
immediately before a separately authorized single capped trial; evaluate its
artifacts with the R54 analyzer, and preserve fail-closed Commit parity and
snapshot/restore gates.

### R54 bounded publication and throughput trial (2026-09-29)

The approved run first failed at CLI flag parsing after about three seconds:
the untagged candidate lacked `--cpu-profile`. Its log recorded no backup
events, no Commit delta and no snapshot changes; the daemon remained idle.
That failed startup and its artifacts remain under the original R54 prospective
directory. The previous R53 CLI had been built with the Go `debug` tag. A
fresh R54 CLI was built from committed source `04c39789d` with the same tag
(SHA256 `55d7d92d9bc27c1da65201de641aa3b2b7f114dcedd2aa0cd4cc60ea17c46ab2`),
DWARF debug information and both profiling flags. The unchanged deployed
daemon (PID 1832232, epoch 77, SHA256
`1348a1af915cec718cb0f8f461eff9d6a35df5370f13b170ce59b39c832e547a`)
also has debug information. The corrected runner and analyzer are under
`/volume2/NASDA2/rustic/db.test/backup-publication-20260929-r54-debug-cap1200`.
The runner pinned the debug CLI and repeated source, snapshot, identity,
writer and port preflight; the analyzer's 49 self-tests passed. No daemon
installation, restart, configuration change or cache reset occurred.

The corrected single trial used the same 52 sources, eight direct NFS
connections, four source readers, 1,200-second TERM cap, 45-second kill grace,
64-Commit-failure live stop budget and safety sampling. It timed out normally
at 1,200.697 seconds (wrapper cleanup 1,203.641 seconds). The strict R54
analyzer **accepts only its bounded publication safety gate**: 4,857 daemon
Commit attempts/completions/successes exactly match 4,857 successful client
responses, with zero Commit failures, cancellations, timeouts, active
transactions and write intents. There was no early stop, source error or
sampler error; 3,676 inode-publication calls and 1,181 revision-allocation
calls had zero terminal failures. All 23,533 final failed reconciliations
were explicitly canceled at shutdown; revision-allocation failures were zero.
Scratch drained, the daemon stayed read-write and idle with the same identity,
and the original 143 snapshot IDs remained unchanged. The CLI exited with
the expected canceled metadata-admission message at the cap. **No new
snapshot or representative restore exists; this is not a completed backup.**

At 1,199 seconds the CLI reported 92,971 files and 164,253,020,843 logical
source bytes, or 77.5 reported files/s and 130.6 MiB/s over elapsed time.
These are progress rates, not unique-inode or newly uploaded byte rates. The
direct NFS adapter recorded 2,747,668 READs and 164,256,483,129 source bytes:
about 59,780 bytes/read, 1.15 ms mean READ service and only 23.9 seconds
aggregate READ queue wait. Service durations overlap across readers; their
sum is not elapsed time. The daemon recorded 1,174,372 main-store GETs,
2,794,361,067,564 logical body bytes (about 2.38 MB/GET and 17.0 times
direct source-read bytes), and 4,840.9 seconds aggregate GET service (4.12
ms/GET). These are delivered logical bytes, **not physical NFS or disk
traffic**. The CLI issued 208,809 metadata size RPCs for 208,809 handles,
averaging 19.94 ms each, with 271,585 positive cache hits, 53,975 negative
hits, 857,731 misses and zero evictions. The lookup cache peaked at 59.7%
of capacity; simply enlarging it is not supported by these counters. Daemon
CPU totaled 7,871.7 seconds (6.55 mean cores) versus CLI CPU 2,327.0
seconds (1.94 mean cores). Both processes had about 1.1-1.3 GB peak RSS;
no memory-limit stop occurred.

Commits began around minute 16: about 4,800 occurred over the last four
minutes, approximately 20/s. All-run average Commit service was 75.2 ms,
with 18,517 engine writes. The CLI CPU profile (2,300.6 sampled CPU seconds)
attributes 23.4% flat to SHA-256, 12.5% cumulative to chunk-boundary
scanning, 11.3% flat to syscall6, 8.0% flat to memory clearing and 28.4%
cumulative to direct-NFS file reads; cumulative and flat shares must not be
added. The daemon's 30-second perf sample was collected early in the **read
phase**: 11K cycle samples, zero lost, with 25.3% flat in memmove, 15.8%
in AES-GCM/CTR, 11.9% in kernel page copying, 8.5% in memset and 5.4%
in CRC. It cannot diagnose publication-phase CPU on its own. The CLI
profile includes both phases; neither profile proves a single-resource
throughput ceiling.

The strongest measured candidate for limiting more MiB/s and files/s is
metadata read amplification and its associated daemon CPU work, not a
Commit failure or lookup-cache eviction. Source NFS service, chunking, SHA
and per-handle metadata size RPC latency are concurrent costs. Four source
readers reached at most four active READs, but low aggregate READ queue wait
does not establish that raising concurrency will improve progress. One R54
observation cannot attribute the difference from R53's 106,466 files and
182.6 GB at its cap to the shutdown fix, caching, source mix or contention.
Next discriminate offline on representative encrypted point reads: measure
repeated access to the same authenticated chunk with fixed cache/read-ahead
and exact result/GET/CPU counters, then test amortizing proven redundant
decrypt/copy work. Do not blindly increase worker counts, enable native
MultiGet, change the daemon or weaken Commit/snapshot/restore gates on this
evidence. A longer completion/restore run needs a separate decision.

Raw logs, both symbolized profiles, writer samples and analysis are retained
in the corrected R54 `candidate/` directory. SHA256 of `analysis.json` is
`de32bb96ef24ded539fe9abfa945052bb0db802d249369e0fdefedf4d3a091ce`,
of `decision.json` is
`ea197cde340bc510f65abba7eb128b7065675d1c0b0eb8c801f802eacac168cb`,
of `cpu/cpu.pprof` is
`27fb094e1ac2becd70579e954a953134fc8a7164f6960668f2b251c1a1e550ab`,
and of `daemon-perf.data` is
`cbe5f47c0014aa1d552b2756c590e1d066cb1afb51df72a26bef77c9d89064f4`.

An offline-only release-mode follow-up retained an ignored, correctness-checked
encrypted point-read probe in `vaulticdb/src/encryption/tests.rs`. It reads
16 distinct 64-byte ranges from one authenticated 256 KiB chunk, for 100
groups in both orders. Independent `get_range` calls took 138-139 ms per
100 groups; one full-chunk request and local slicing took 8 ms, while the
existing `ObjectStore::get_ranges` API took 10 ms and returned the same bytes.
Each individual request fetches/authenticates the entire chunk. From the
requested ciphertext ranges, these modes imply respectively 1,600 and 100
requests and 419,456,000 versus 26,216,000 logical ciphertext bytes; those
are **modeled request bytes**, not measured host traffic. The focused debug
probe, release probe, 16 other encryption tests and the existing SlateDB
MultiGet amplification test pass. Build and test artifacts stayed under
`/run/vaultic-phase33-attribution`.

A second ignored release-mode fixture uses a 4,096-record persisted encrypted
SlateDB SST with the DB cache disabled and a test-only object-store observer.
It reopens the DB before each 16-nearby-key lookup and measures actual returned
ciphertext bytes by object class and response size. Individual GETs returned
53-54 SST responses and 11,369,360-11,369,398 SST bytes; native MultiGet
returned three SST responses and 628,660 SST bytes in both orders. Of the
individual SST responses, 32 were 183,250-byte reads and 21 were
262,160-byte reads (plus one 38-byte header in the 54-read case); MultiGet
used two 183,250-byte reads and one 262,160-byte read. One replay also had
23 unrelated small WAL/other header responses from background work; the SST
figures exclude them. All 16 returned values matched. Observed in-memory
elapsed times were about 6.3-7.7 ms for individual GETs and 0.55-0.66 ms
for MultiGet. These measured SST response bytes corroborate the earlier
removed probe, but do not measure physical I/O, production latency or the
distribution of nearby handles during the live backup.

This discriminates against a second request-scoped coalescer: `get_ranges`
already performs that reuse. R54's 208,809 size RPCs each had exactly one
handle, so the controlled 16-nearby-key workload is not representative of
the live request boundary. Cross-caller batching previously lowered RPCs
without improving progress, and a shared decrypted-chunk cache would require
immutable-object/version and memory-lifetime evidence. No runtime cache,
batching, daemon binary or production setting was changed. Before proposing
a live speedup, establish the real key/range locality and bounded group
formation without introducing latency or extra memory, then check actual
GETs, CPU, exact results, cancellation and publication parity in a matched
workload. The R54 safety verdict remains bounded-only.

### R55 capped completion attempt (2026-09-29)

After explicit approval, a fresh single-use copy of the R54 debug runner and
analyzer was staged at
`/volume2/NASDA2/rustic/db.test/backup-publication-20260929-r55-cap2400`.
It pinned the same 52-source manifest, CLI SHA256
`55d7d92d9bc27c1da65201de641aa3b2b7f114dcedd2aa0cd4cc60ea17c46ab2`
and deployed daemon PID 1832232 / epoch 77 / SHA256
`1348a1af915cec718cb0f8f461eff9d6a35df5370f13b170ce59b39c832e547a`.
Read-only preflight reconfirmed source access, 143 unchanged snapshot IDs,
an idle read-write writer and no competing Vaultic client. The cap was
raised to 2,400 seconds with the same 45-second kill grace, eight NFS
connections, four readers and 64-Commit-failure live stop budget. The
copied analyzer's 49 synthetic safety tests passed. Neither binary nor
production settings were changed.

The single trial reached the 2,400-second TERM cap (wrapper 2,404.022 s;
CLI status 2,400 s). It reported 185,440 files and 195,770,358,916 logical
source bytes; direct NFS reported 3,488,321 READs and 195,770,361,598
bytes. These are progress counters, not unique inodes or uploaded bytes.
CLI CPU totaled 2,992.7 s and daemon CPU 12,486.65 s. The daemon delivered
4,362,080,401,191 logical metadata GET body bytes in 1,871,689 GETs;
these are not physical storage traffic. Peak CLI RSS was 1,393,756 KiB.
Around 1,600 seconds the reported source-byte curve nearly flattened,
while publication continued: Commit attempts grew from 13,900 at 1,617 s
to 29,555 at 2,397 s, with intermittent later source progress. This is
evidence of a long publication tail in this run, not a measured completion
time or proof of a single CPU/I/O bottleneck.

All 29,601 daemon Commit attempts, completions and successes exactly matched
29,601 successful client responses; there were zero Commit failures,
cancellations, timeouts, active transactions or write intents at postflight.
There were no source or sampler errors or early safety stops. The 52,085
final failed reconciliations were explicitly canceled at shutdown; revision
allocation and inode publication reported zero terminal failures. Scratch
drained, the writer remained read-write at the same PID and epoch, and the
original 143 snapshot IDs remained unchanged. The analyzer accepts only
the **40-minute bounded publication safety gate**: process exit 124,
`completed=false`, no summary or new snapshot. The CLI reported the expected
`unable to save snapshot: context canceled` on TERM. **No backup or payload
restore acceptance was achieved.** A further cap increase is not justified
by a completion estimate from these two censored trials; examine the
publication tail and define a bounded completion criterion before another
production attempt. Raw runner, samples, profiles, postflight and
`analysis.json`/`decision.json` remain in the R55 `candidate/` directory.

### R56-R62 publication and pack trials (2026-09-29)

Offline process-backed publication comparisons at 100, 25 and 10 ms WAL
flush intervals showed that shorter durability waits reduced group wall
time. On an NFS-backed shared-content fixture with 64 inodes and 64
content IDs each, grouped publication took 8.07-8.08 s at 100 ms,
5.84-5.86 s at 25 ms and 5.44-5.76 s at 10 ms. Atomic groups took
5.55-6.45, 5.09-5.25 and 5.00-5.05 s respectively. These are isolated
fixtures, not production speedup estimates. Reopen, shared-reference,
Commit-accounting, cancellation and process crash tests passed. Production
trials kept the daemon binary unchanged and temporarily used 25 ms.
R56 needed independent restoration verification after wrapper assertions;
subsequent wrappers recorded restoration of the original 100 ms setting.

| Trial | Change or diagnostic | CLI result | Snapshots |
| --- | --- | --- | --- |
| R56 | 25 ms flush, unchanged CLI | 3,600 s cap; 160,606 matched successful Commits; bounded safety only | 143 |
| R57 | Bounded parallel directory publication with one revision block per group | Exit 1 at 2,139 s; pack publication deadline | 143 |
| R58 | Longer bounded pack deadline and stage errors | Exit 1 at 2,135 s; pack import planning deadline | 143 |
| R59 | Pack lookup progress diagnostics | Exit 1 at 1,858 s; lookup batch 8,000-9,000 of 18,289 blobs timed out | 143 |
| R60 | Temporary daemon-wide native MultiGet | 3,600 s cap, 376,043 reported files; canceled reconciliation failed the acceptance gate | 143 |
| R61 | Native MultiGet off, bounded 10-minute pack budget | 3,600 s cap, 417,140 reported files; canceled reconciliation failed the acceptance gate | 143 |
| R62 | Same candidate as R61, 7,200 s cap | Exit 1 at 2,052 s; metadata lookup deadline while admitting a blob | 143 |

R57's directory change and the pack-stage diagnostics are in the Go diff.
R62's 34 daemon Commits matched successful client responses, but no
inode-publication call was observed, so its analyzer rejected completion
and publication-safety acceptance. It did not reach the longer cap. Its
final restoration record verifies an idle writer at epoch 91, 100 ms
flush and unchanged snapshot membership. The R56-R62 raw artifacts and
decisions are retained under the corresponding
`/volume2/NASDA2/rustic/db.test/backup-publication-20260929-rXX-*`
directories. None of these trials completed a backup or enabled a payload
restore. The next blocker is the R62 blob-admission metadata lookup; it
must be diagnosed without raising the global RPC deadline or treating
an elapsed cap as a successful snapshot.

## Isolated empty-debt overhead measurement (2026-09-28)

`BenchmarkProcessEmptyCrawlDebtResolution` compares the unchanged
`resolveCrawlDebtOnce(ctx, nil)` transaction helper with the new
`ResolveCrawlDebt(ctx, nil)` fast path. The helper represents the former
successful empty-input path; this is not a comparison of entire old/new backup
binaries or of conflict retries. Each case starts a private memory-store daemon
and uses a temporary Unix socket. Startup, shutdown and status sampling are
excluded from timing; no production endpoint is used.

Five non-race repetitions of 10,000 calls per case produced:

| Metric Per Call | Transaction Helper | Empty Fast Path |
| --- | --- | --- |
| Median elapsed time | 1.107940 ms | 6.312 ns |
| Elapsed range | 1.104130-1.128058 ms | 6.279-7.360 ns |
| Begin RPCs | 1 | 0 |
| Commit RPCs | 1 | 0 |
| Median Go allocated bytes | 12,589 | 0 |
| Median Go allocations | 195 | 0 |
| Server commit service range | 127.0-129.1 us | No calls |

Every measured case checks exact Begin/Commit attempt, completion and success
deltas, zero new failures/cancellations/timeouts, no rollback or engine-write
increments, and zero active transactions/write intents. All checks passed.
Allocation figures cover the Go benchmark process, not daemon allocations.
The nanosecond result measures only the local empty-input/context check; it
does not represent metadata lookup or file-processing throughput.

Reproduction command (with the existing RAM-backed TMPDIR/GOCACHE):

```sh
VAULTICDB_TEST_BINARY=/root/proj/vaultic/vaulticdb/target/process-tests/debug/vaulticdb \
  go test ./internal/index/daemon -run '^$' \
  -bench '^BenchmarkProcessEmptyCrawlDebtResolution$' -benchtime=10000x -count=5
```

Raw output is retained at
`/run/vaultic-empty-debt-benchmark-hUPbKb/benchmark.txt` (ephemeral across reboot),
SHA256 `5457f273d55afec54252522b1c7f3c3b3927d718663deb99775ea96a552267f9`.
The debug test-failpoint daemon SHA256 is
`ba323af363398bc54694fd848751a4acbb2c48df6d07be76f8f58a07513bc610`.
The source base is `2429269d253bb624010eb346e4e2ef433e88f71b` plus the
uncommitted fast path, tests and evidence. The host reports Linux/amd64,
Xeon Gold 5217 and benchmark GOMAXPROCS 32. No concurrent builds or tests were
launched during measurement; unrelated host activity was not controlled.

This quantifies an avoidable transaction cost in isolation, not a live backup
speedup. Memory-store/debug-daemon timing is not a production estimate, and
remaining per-key metadata lookup costs were not measured again. No new
production code was added in this measurement step. The generic finalization
race and R43/R44 rejection decisions remain unchanged. No commit, push,
deployment, production restart, tuning change or live trial was performed.

## Empty debt resolution fast path (2026-09-27)

The diagnostics, ordering reproduction and preceding evidence were committed
locally as `2429269d2` with a detailed message; no push was performed. Follow-up
inspection of the actual reuse path found a narrower optimization than changing
transaction cancellation semantics: `Reconciler::reuseExistingRecord` always
calls `SchemaStore::ResolveCrawlDebt`, even when its debt-key slice is empty.
The latter previously opened and committed a transaction for that no-op.

`ResolveCrawlDebt` now returns the context error immediately for an empty key
slice, or nil when the context is live. It issues no Begin, Commit or Rollback
RPC. Non-empty input retains the existing transaction, validation, retry and
cleanup behavior. No public transaction finalization semantics, deadline policy
or benchmark failure classification were changed. The generic commit/rollback
ordering characterized below still exists; this change removes unnecessary
transaction work from debt-free inode reuse rather than hiding its errors.

A regression with no daemon client reproduced the old attempt to call Begin
and now passes for nil and empty input while preserving cancellation. An
isolated-daemon test verifies pending debt resolves, already-resolved records
remain byte-identical, absent debt is not created and cancelled non-empty work
leaves pending debt unchanged. All eight publication/reuse fixtures now assert
that republishing their 32 identical inodes starts no transactions and performs
no additional engine writes. These checks and the full reconciliation suite
pass with the race detector; changed-code lint passes.

This removes one unnecessary Begin/Commit pair per empty resolution call, but
the historical diagnostics did not record each call's debt-key count. No exact
R44 savings, live throughput improvement or resolution of its specific failure
is claimed. The follow-up remains uncommitted and undeployed pending evaluation;
no new live workload, daemon restart or settings change was performed.

## Offline cancellation-ordering reproduction (2026-09-27)

The cancellation/rollback ordering hypothesized after R44 is now reproducible
with an isolated daemon. A barrier immediately before `Storage::commit`
removes its transaction allows the test to cancel the client RPC, observe its
`transactionCommitUncertain` state, finish rollback using the normal cleanup
context, and then release the detached server commit. The resulting diagnostic
is exactly `storage_commit / NotFound / transaction_consumed=false`.

The control ordering uses the existing before-apply barrier, after transaction
removal. Rollback then returns `NotFound`, while the detached commit succeeds.
Both orderings are tested with read-only and mutating transactions:

| Ordering | Rollback Result | Server Commit | Data After Reopen |
| --- | --- | --- | --- |
| Rollback first, read-only | Success | NotFound, not consumed | Absent; zero engine writes |
| Rollback first, mutating | Success | NotFound, not consumed | Staged value absent |
| Commit takes ownership first, read-only | NotFound | Success | Absent; zero engine writes |
| Commit takes ownership first, mutating | NotFound | Success | Committed value retained |

`TestProcessCancelledCommitFinalizationOrdering` passes all four cases across
ten race-detector repetitions. It verifies settled counters, zero active
transactions/write intents, exact failure diagnostics after process exit and
data visibility after reopening the temporary database. Settlement waits for
reconciled outcomes and quiescence, not `completed` alone: timing counters are
separate atomics and a snapshot can briefly observe a partial update.
Neighboring delayed-response recovery, response-delivery barrier and deferred
durability-token tests also pass. Changed-code Go lint and normal daemon
compilation pass. The new Rust barrier's formatting is valid; existing unrelated
storage-file formatting differences were verified against HEAD and preserved.

The normal lazy-build test path also passes; binary preparation occurs before
the per-case timeout. Its first build exhausted the root filesystem and the
LLVM linker stopped with a bus error. The 6.1 GiB generated process-test cache
was preserved under `/run/vaultic-cancel-ordering-build-yFNQBs/process-tests`,
with the original target path retained as a symlink. This cache is ephemeral
and must be recreated after reboot; no production data or settings were moved.

The new barrier is compiled only for Unix builds with `test-failpoints` and
uses the existing process-test capability gate. It is absent from normal
production builds. No transaction semantics, retry policy, production binary,
daemon settings or live workloads were changed; sealed R44 evidence remains
untouched. This is a deterministic characterization, not a correctness fix or
permission to waive the failed benchmark gate.

The experiment proves that the proposed ordering can generate R44's observed
signature even with zero engine writes. It does not establish the identity or
ordering of the specific live transaction, or retroactively classify R43's
four failures. A fix must coordinate finalization while preserving both
uncertain-commit durability and cleanup when a commit never reached the server;
blindly suppressing `NotFound`, retrying mutations or skipping cleanup would
not satisfy that contract.

## Diagnostic baseline, R44 (2026-09-27)

**The approved baseline completed its 600-second window but failed the strict
commit-outcome gate.** The newly deployed instrumentation captured one
`NotFound` result at `storage_commit`, with `transaction_consumed=false`,
600.008 seconds after launch. No additional live run, candidate sweep, daemon
change, commit or push followed. The result is diagnostic evidence, not an
accepted performance baseline or a retroactive explanation for R43.

Artifacts are retained under
`/volume2/NASDA2/rustic/db.test/diagnostic-baseline-20260927-r44-0hvUOg`.
The immutable baseline CLI is SHA256
`b67d19ebe426bbdef1cfb3f407db19af44db8bfcd2b4c9e65b5f13d3c0cbc9c3`, using source
base `94b54dc3e180284e4907b86d19893d2be5cc5a28`; the diagnostic daemon is SHA256
`7c711452bd218a8715225c8f18398d56a8f9b18e6821c30aa072e0b6bf52427b` at PID
1572244 / epoch 75. A fresh harness and nine synthetic acceptance-gate tests
were validated before the single approved run. Previous sealed trial files
were not modified. The harness pinned binary/unit/environment identities and
would stop the backup on a sampled non-success or identity failure. The error
appeared after the final periodic sample, so the post-shutdown gate rejected
it. `decision.json` records `accepted=false` and retains both rejection reasons.

The run used the same 52 sources, explicit `--use-cwalk --nfs-direct`, four
readers, eight NFS connections, 32 cwalk workers and 64 MiB client lookup cache.
The daemon remained at 128 MiB metadata cache, 512 MiB block cache and 100 ms
flush interval. No builds or tests ran concurrently. GNU timeout returned 124
after 600.295 seconds, within the approved 45-second shutdown grace; no forced
kill or early safety stop was required.

| Measurement | R44 |
| --- | ---: |
| Files / logical bytes at 599 seconds | 45,670 / 87,142,967,123 |
| Logical throughput | 138.741 MiB/s |
| Commit attempts / successes / failures | 42,246 / 42,245 / 1 |
| Commit cancellations / timeouts / active | 0 / 0 / 0 |
| Size RPCs / handles | 111,410 / 111,410 |
| Mean size RPC latency | 19.496 ms |
| Metadata GETs / GETs per handle | 623,177 / 5.594 |
| Metadata logical body bytes | 1,489,364,624,141 |
| CLI / daemon mean cores | 2.040 / 7.278 |
| CLI peak RSS | 998,536 KiB |
| Daemon sampled peak RSS | 1,134,202,880 bytes |
| Client cache peak occupancy / evictions | 31.87% / 0 |
| NFS connections opened / idle-retired / shortfalls | 384 / 329 / 0 |
| Peak / final sampled owned reserved sockets | 387 / 58 |
| Mean NFS READ queue / service latency | 0.0086 / 1.161 ms |

The last periodic status at 595.643 seconds showed 41,806 successful commits
and no non-successes. The failure event occurred at the timeout boundary; final
status at 600.325 seconds showed one failure, with no active transactions or
write intents. All 143 snapshot IDs match, scratch is empty and the profile
listener is gone. Reported source and sampler errors are zero, and engine
writes are zero. NFS READ counters include four cancellations; LOOKUP reports
48,066 unclassified errors, and reconciler close-time accounting reports
3,431 failed entries. These are not silently reclassified as successful work.

The diagnostic identifies a storage-commit error before this commit consumed
transaction state, not an explicit gRPC cancellation or deadline result. Local
inspection found that `Storage::remove_transaction` returns transaction
`NotFound` when the map entry is absent. The client can mark a cancelled commit
response uncertain and subsequently issue rollback with a cleanup context,
while the server continues detached commit processing. Rollback winning that
ordering is a plausible hypothesis, but the event contains no transaction ID
or causal ordering evidence; expiry or another removal cannot be excluded.
A deterministic cancellation/rollback reproduction is required before changing
transaction semantics or relaxing any gate. R43's four historical failures
remain unclassified.

The 360-second stack contains four active size MultiGet calls, four repository
blob-save tasks waiting for lookup results and 32 cwalk waiters. Metadata
per-key cost remains a supported investigation target; more NFS connections or
a larger client cache is not justified by this run. The 138.741 MiB/s result
is not a causal regression estimate against the earlier roughly 153 MiB/s
measurements: this run followed a daemon restart and has different cache warmth
and work mix. Logical throughput is not uploaded-byte throughput, and no
completed-backup runtime or payload-restore claim is made.

## Diagnostic daemon deployment (2026-09-27)

The user explicitly approved deployment after the offline attribution and
benchmark-accounting work. A native Linux release build of the diagnostic
daemon was tested against temporary storage, then deployed to the unchanged
`vaulticdb-rustic.service`. No benchmark, fault injection, commit or push was
performed. This deployment supersedes the earlier pending-deployment notes;
it does not change R43's rejected result or classify its historical failures.

Private deployment evidence is retained under
`/volume2/NASDA2/rustic/db.test/diagnostic-deployment-20260927-W03LCF`.
It includes the guarded deployment script, original and candidate binaries,
source patch, configuration fingerprints, writer/snapshot records, startup
journal, validation results and checksum manifest. The source base remains
`94b54dc3e180284e4907b86d19893d2be5cc5a28` with the recorded uncommitted diagnostic
and test changes. The deployed binary SHA256 is
`7c711452bd218a8715225c8f18398d56a8f9b18e6821c30aa072e0b6bf52427b`;
the retained original SHA256 is
`3a825c277b2273627ca9349122e8c77b32f7ac91710a888411c4765fd6531ccd`.

The exact release candidate passed `TestCommitFailureAttribution`,
`TestRealDaemonAdmissionErrorsAreTyped` and
`TestStorageRoundTripTransactionsPaginationAndRestart` with the Go race
detector before deployment. Production preflight required the original PID,
binary and unit identity, quiescent transactions/commits and 143 snapshots.
The installed helper CLI was stale and failed a socket-correctness preflight;
no service change occurred. The guard was corrected to use the immutable R43
baseline CLI, SHA256
`b67d19ebe426bbdef1cfb3f407db19af44db8bfcd2b4c9e65b5f13d3c0cbc9c3`.

The transport handles SIGINT, but the unit's unchanged default stop signal is
SIGTERM. The deployment therefore explicitly signalled SIGINT to the verified
main process and required an inactive unit with successful exit status zero
before atomically replacing the executable and starting the existing unit.
No forced termination, takeover, WAL manipulation or recovery bypass occurred.
The old binary remains available for a separately verified recovery procedure.

Post-deployment verification confirmed PID **1572244**, read-write role and
current/observed epoch **75**, advancing normally from PID 1440079 / epoch 74.
The configured instance ID remains `vaulticdb-dev`; process identity was checked
using the executable hash, new PID and increased process-start timestamp, not
by incorrectly treating that stable instance ID as a restart identifier.
Unit/environment fingerprints, runtime configuration and all effective engine
tuning values are unchanged: 128 MiB metadata cache, 512 MiB block cache and
100 ms flush interval. All 143 snapshot IDs match exactly; active transactions,
write intents and commits are zero. This is a deployment health/membership
check, not a full metadata audit or payload restore.

The old counters recorded 347,392 commit attempts and four failures. The new
process starts with zero commit attempts, failures, cancellations, timeouts
and engine writes. These resets must not be interpreted as resolving R43.
Future measurements need a fresh baseline using the new PID, epoch and binary,
and must check every non-success outcome. Existing sealed trial artifacts and
their identity guards remain unchanged; no old trial was resumed.

## Rejected client-side lookup batching, R43 (2026-09-26)

**Cross-caller batching reduced RPC count but did not improve throughput, and
the candidate failed the zero-commit-failure gate. The experimental code was
removed from the worktree; its patch, tests and binary remain in the artifacts.**
No implementation commit, push, daemon deployment, restart or setting change
was made. The restored cache files and backup documentation match HEAD, and the
restored cache race tests pass. Earlier R41/R42 evidence edits were preserved.

Artifacts are under
`/volume2/NASDA2/rustic/db.test/backup-metadata-batching-20260926-r43-Jtayol`.
The baseline is commit `94b54dc3e180284e4907b86d19893d2be5cc5a28`, CLI SHA256
`b67d19ebe426bbdef1cfb3f407db19af44db8bfcd2b4c9e65b5f13d3c0cbc9c3`.
The candidate CLI SHA256 is
`6f1e0d6db77f6ebc19dffb09e951adf228bfe99466e5b48017f333a1e13ea68b`;
the frozen `source.patch` SHA256 is
`a43e636e03e18e6c6e56bbf582e404df340f7cbcb9ffe5b8275c9f10ea02e683`.
Both sequential trials used four readers, eight NFS connections, 32 cwalk
workers, the same 52 sources, explicit `--use-cwalk --nfs-direct`, 64 MiB client
lookup capacity and 600-second timeout plus 45-second shutdown grace. Source
and cache state were not reset; differences are not isolated causal estimates
or completed-backup runtime measurements.

The candidate registered distinct misses before admission in one shared queue,
limited to 256 handles, in addition to four active size batches. It dispatched
without an artificial delay, deduplicated across callers, preserved independent
cancellation and local pending-write overlays, and joined queued/active work on
close. Deterministic queued batching, ordering, cancellation, full-queue rollover
and overlay tests passed, including ten repeated race runs. Changed-code lint
reported zero issues. Production lookup concurrency is explicitly four in
`cmd/vaultic/backupcmd/cmd_backup_run.go`, not inferred from a sampled peak.

The index and backup-command race suites passed. The repository suite failed
`TestReadCacheChunkBoundaryAndFinalShortRead` (requests 3 to 4, bytes 216 to 220);
a Go overlay restoring committed cache code reproduced the identical failure.
All affected suites passed with only that pre-existing test excluded. An initial
reduced-capability repository run also failed permission tests; those passed
under ordinary root. Neither issue was changed as part of this experiment.

| Measurement | Baseline | Batched Candidate |
| --- | ---: | ---: |
| Backup duration, seconds (exit 124) | 600.279 | 600.304 |
| Files at 599 seconds | 49,672 | 49,166 |
| Logical bytes at 599 seconds | 96,312,717,818 | 94,089,477,106 |
| Logical MiB/s | 153.341 | 149.801 |
| Size RPCs / handles | 122,079 / 122,079 | 38,875 / 120,130 |
| Mean handles per size RPC | 1 | 3.090 |
| Mean size RPC service, ms | 18.726 | 56.686 |
| Mean queued-batch wait, ms | unavailable | 11.200 |
| Metadata GETs | 565,999 | 555,229 |
| Metadata logical body bytes | 1,625,720,401,434 | 1,596,622,147,972 |
| CLI / daemon mean cores | 2.154 / 7.846 | 2.185 / 7.644 |
| CLI peak RSS, KiB | 1,072,780 | 1,071,704 |
| Daemon sampled peak RSS, bytes | 1,304,006,656 | 1,306,574,848 |
| Commit attempts / failures | 48,844 / 0 | 46,674 / 4 |

RPC count fell about 68%, but service cost remained about 18 ms per handle and
metadata GETs remained about 4.6 per handle. The 360-second candidate stack
shows 32 callers waiting for lookup results, four MultiGet fetches, 32 waiting
cwalk workers and two file savers blocked at blob-save admission. The queue
moved waiting work without removing its underlying metadata service cost.
Client cache occupancy remained about 34% with no eviction. More client
batching or a larger client result cache is not supported by these results.
The next performance investigation belongs in daemon MultiGet per-key work and
metadata table/block reuse, with bounded backend instrumentation before any
daemon change. No server-side cause or daemon CPU attribution is proven here.

The candidate's four commit failures first appear in the post-timeout writer
sample at 601.077 seconds relative to the before-status timestamp; earlier
samples had zero. This bounds detection, not exact failure time. The CLI log
reports termination and `context canceled`; sampled commit counters classify
these as failures with zero cancellation/timeout counts. The daemon journal
provided no matching failure detail. Their exact causes remain unclassified:
they were not waived as benign shutdown failures. The driver stopped with
`commit failures`, so there is no successful sweep-completion record. Preserve
the failure record and investigate commit-error attribution before another live
candidate trial; do not bypass the gate.

Both runs left scratch empty, zero engine writes, all 143 snapshot IDs unchanged
and no reported per-file source or sampler errors. All active source RPCs
returned to zero. Each opened 384 NFS sockets and retired 329 idle sockets,
with no shortage/reuse/regrowth. Each recorded four READ cancellations; LOOKUP
errors (53,837/53,507) and reconciliation close-time failures (833/2,497) remain
unclassified. No snapshot completed. Final writer status was read-write at
epoch 74 with zero transactions/intents; PID 1440079 remained active/running
with 128 MiB metadata cache, 512 MiB block cache and 100 ms flush interval.
The profiling listener was gone. Metadata body bytes are logical, not physical
network/disk traffic; profiles and raw samples are retained for offline analysis.

### Offline attribution follow-up (2026-09-26)

Inspection found that `TimingGuard::record_result` collapses every returned
error into `Failure`. A regression using explicit gRPC `Cancelled` and
`DeadlineExceeded` statuses reproduced zero cancellation/timeout counts. The
commit handler now uses a status-aware recorder; other generic timing callers
are unchanged. This proves an attribution gap, not the cause of R43's four
historical errors. Future gates must check failures, cancellations and timeouts
together and must not retroactively waive the failed R43 gate.

Failure-only, fixed-cardinality `commit_failure` stderr diagnostics now record
the commit stage, gRPC status, transaction consumption and occurrence count,
without raw request/error text. Each stage/status pair logs its first eight
occurrences and then powers of two. Consumption is not a durability assertion.
Twelve Rust attribution tests pass, including status classification and bounded
diagnostics. A temporary in-memory daemon integration test verifies an expired
commit as a validation timeout, a missing transaction as a storage-commit
failure, counter settlement, transaction cleanup and request-data redaction.
Neighboring transaction/publication tests also pass under the Go race detector.
Go changed-code lint and Rust formatting pass. Clippy completes with existing
crate warnings; its only warning in the touched files is an unchanged timing
expression verified against HEAD.

This is an uncommitted, offline diagnostic change, not a throughput optimization
or a production deployment. The production daemon remains PID 1440079; no new
live trial, restart, tuning, commit or push occurred. The four R43 failures
remain unclassified, and further live candidate trials remain gated on their
attribution. Deployment of the diagnostic daemon requires separate approval.

### Offline benchmark-accounting follow-up (2026-09-27)

No explicit production deployment approval was available, so work remained
offline. The revision-allocation and daemon-backed publication tests still
inferred successful commits as attempts minus generic failures. They now use
the explicit success counter, require all attempts to have completed and all
completed outcomes to reconcile, and reject active commits, cancellations and
timeouts. Existing allowance for contention failures is preserved; these tests
are not the zero-error production benchmark gate. Publication output now lists
successful, failed, cancelled and timed-out commits separately.

All four revision-allocation modes and all eight publication cases passed with
the Go race detector against the isolated local daemon and temporary storage.
The publication cases used 32 inodes, one content ID and one stream; expected
contention failures remained separately accounted for, with zero cancellations
or timeouts. Changed-code lint and formatting checks passed. This validates
test accounting, not a production throughput gain or an explanation for the
four historical R43 failures. No production deployment, restart, live trial,
settings change, commit or push was performed; sealed trial artifacts remain
unchanged.

## Direct NFS reader comparison, R42 (2026-09-26)

**Four file readers achieved 152.85 MiB/s versus 106.95 MiB/s with two, at a
fixed eight-connection target.** This is 42.91% more logical bytes and 30.17%
more completed files within the observation window, not a measured reduction
in full-backup runtime. The fresh two-reader baseline agrees with R41's
eight-connection result (107.27 MiB/s). These sequential trials did not reset
caches or source state, and the four-reader run reaches a different work mix;
the observed difference is not an isolated causal speedup estimate.

Artifacts are under
`/volume2/NASDA2/rustic/db.test/backup-direct-nfs-readers-20260926-r42-XYpi1y`.
The exact R41 profile binary was reused at commit
`94b54dc3e180284e4907b86d19893d2be5cc5a28`, SHA256
`b67d19ebe426bbdef1cfb3f407db19af44db8bfcd2b4c9e65b5f13d3c0cbc9c3`.
Only `--read-concurrency=2` versus `4` varied. Both runs retained the same 52
sources, `--use-cwalk --nfs-direct --nfs-connections=8`, 32 cwalk workers,
64 MiB client lookup capacity and 600-second timeout with 45-second grace.
Identity checks allowed only the fingerprinted, pre-existing R41 evidence edit.
No code changes, builds, daemon deployment, restart, cache flush or tuning
overlapped the observation. No new commit or push was made.

| Measurement | Two Readers | Four Readers |
| --- | ---: | ---: |
| Backup duration, seconds (exit 124) | 600.245 | 600.261 |
| Files at 599 seconds | 38,026 | 49,500 |
| Logical bytes at 599 seconds | 67,176,867,875 | 96,005,671,267 |
| Logical MiB/s | 106.953 | 152.852 |
| CLI CPU-seconds / mean cores | 912.33 / 1.520 | 1,327.12 / 2.211 |
| Daemon CPU-seconds / mean cores | 3,369.51 / 5.606 | 4,661.93 / 7.756 |
| CLI peak RSS, KiB | 891,640 | 1,050,996 |
| Daemon sampled peak RSS, bytes | 1,152,471,040 | 1,272,504,320 |
| Peak / average active READ RPCs | 2 / 1.662 | 4 / 2.384 |
| Mean READ queue / service, ms | 0.0065 / 0.8899 | 0.0079 / 0.9001 |
| Single-handle size RPCs | 90,011 | 121,639 |
| Mean size RPC latency, ms | 18.040 | 18.759 |
| Average active size RPCs | 2.705 | 3.801 |
| Metadata GETs | 420,346 | 591,900 |
| Metadata logical body bytes | 1,208,798,934,241 | 1,620,569,741,613 |
| Client lookup peak entries / evictions | 90,010 / 0 | 121,635 / 0 |

Average active RPCs are aggregate recorded service seconds divided by backup
wall seconds, not independent instantaneous samples. Both runs opened 384 NFS
connections, retired 329 idle connections and had 387 peak / 58 final sampled
process-owned reserved TCP sockets, including setup connections. There were
zero shortfalls, pool reuses or growth attempts/failures. Extra readers used
existing transport capacity; no extra sockets were needed. Host-wide reserved
TIME_WAIT peaks of 330/386 are not exclusively attributable to these processes.

The four-reader 360-second stack contains 22 workers waiting at
`CachedBlobLookup.LookupSizesContext` admission, four MultiGet fetches and four
callers waiting for results; all 32 cwalk workers are waiting. Every size RPC
still carries one handle. Lookup-cache occupancy reaches only 35% of capacity
without evictions. Four readers improve source overlap but do not remove
metadata backpressure. The next implementation candidate is bounded cross-caller
size batching before admission, with tests for deduplication, cancellation,
shutdown and concurrent pending-blob visibility. Measure handles per RPC,
admission wait, metadata GETs per unit of work and throughput at fixed four
readers before increasing readers or daemon resource budgets again. No global
reader default was changed; four readers is the next benchmark configuration,
not yet a full-backup production recommendation.

CLI profiles contain 899.61/1,315.58 sampled CPU-seconds. SHA256 accounts for
23.54/24.75% flat samples; chunk splitting accounts for 9.40/10.34% flat and
13.02/14.09% cumulative. Cumulative percentages overlap. No daemon CPU profile
was captured, and metadata body bytes are logical delivered bytes rather than
physical network or disk traffic.

Both trials left scratch empty, all 143 snapshot IDs unchanged, zero active
RPCs and no source, sampler or commit failures. Commit requests were
34,453/48,844 with zero engine writes. Each recorded two READ cancellations;
LOOKUP errors (39,285/53,822) and reconciliation close-time failures
(3,574/658) remain unclassified. No snapshot completed. The same PID 1440079,
epoch 74, read-write role, zero transactions/intents, 128 MiB metadata cache,
512 MiB block cache and 100 ms flush interval were verified before and after.
The profiling listener was gone and the daemon remained active/running.

## Direct NFS capacity-recovery sweep, R41 (2026-09-26)

**All three authorized 600-second observations completed their windows. Raising
the connection target from 4 to 8 or 16 did not improve logical throughput.**
The implementation was committed locally with a detailed message as
`94b54dc3e180284e4907b86d19893d2be5cc5a28`; nothing was pushed or deployed.
Affected race suites and changed-code lint passed before the sweep.

Artifacts are under
`/volume2/NASDA2/rustic/db.test/backup-direct-nfs-sweep-20260926-r41-omISYv`.
The profile-tag CLI SHA256 is
`b67d19ebe426bbdef1cfb3f407db19af44db8bfcd2b4c9e65b5f13d3c0cbc9c3`.
The manifest, exact commands, per-trial analysis, CPU profiles, four goroutine
captures per trial, process/writer/socket samples and sweep comparison are
retained. The workload uses the same 52 sources, `--use-cwalk --nfs-direct`,
32 cwalk workers, two file readers and 64 MiB client lookup capacity. Trials
were sequential without cache or source-state reset; small differences are not
causal regressions, and these windows do not measure completed-backup runtime.

| Measurement | 4 Connections | 8 Connections | 16 Connections |
| --- | ---: | ---: | ---: |
| Backup duration, seconds (exit 124) | 600.252 | 600.265 | 600.256 |
| Files at 599 seconds | 38,046 | 38,028 | 38,012 |
| Logical bytes at 599 seconds | 67,985,806,519 | 67,374,711,419 | 66,712,921,430 |
| Logical MiB/s | 108.241 | 107.268 | 106.214 |
| CLI mean cores | 1.538 | 1.509 | 1.510 |
| Daemon mean cores | 5.595 | 5.603 | 5.594 |
| NFS connections opened / retired idle | 192 / 141 | 384 / 329 | 768 / 705 |
| Peak / final sampled process-owned reserved TCP sockets | 195 / 54 | 387 / 58 | 771 / 66 |
| Peak concurrent READ RPCs | 2 | 2 | 2 |
| Mean READ queue / service, ms | 0.0083 / 0.8753 | 0.0065 / 0.8863 | 0.0061 / 0.8959 |
| Mean READDIRPLUS queue / service, ms | 2.7447 / 0.9090 | 0.9599 / 0.6990 | 0.2971 / 0.6757 |
| Single-handle size RPCs | 90,269 | 90,074 | 89,883 |
| Mean size RPC latency, ms | 17.955 | 17.998 | 18.048 |
| Metadata GETs | 421,590 | 420,655 | 420,086 |
| Metadata logical body bytes | 1,212,428,791,343 | 1,209,502,048,977 | 1,208,076,443,790 |

There were zero allocation shortfalls, fallback-pool reuses, growth attempts or
growth failures. Idle retirement is demonstrated; live shortage recovery was
not exercised in these runs. Process-owned sockets are matched by FD inode and
include setup connections, not just NFS data sockets. Namespace-wide reserved
TIME_WAIT peaks were 142/382/763; those include unrelated processes and cannot
be attributed entirely to Vaultic. The user's observation that only about
10-15 connections carry notable traffic is consistent with excess idle capacity,
but the sampler counts sockets rather than per-socket traffic and cannot verify
that exact busy-connection count.

The 16-connection 360-second stack shows 24 workers at `CachedBlobLookup`
admission, four size fetches waiting on MultiGet, another four callers waiting
for results, 32 cwalk workers waiting, and both file readers in NFS reads.
Each size RPC still carries exactly one handle. Client lookup occupancy peaks
at about 26% with zero evictions, so enlarging that cache is not supported.
Metadata delivers about 2.88 MB per GET; repeated metadata work/cache pressure
is a candidate, not proof of physical network traffic or daemon CPU attribution.
No daemon CPU profile was captured.

CLI CPU profiles contain 910.61/893.04/893.44 sampled CPU-seconds. SHA256
accounts for 23.36/23.76/23.44% flat samples; chunk splitting accounts for
9.33/9.29/9.20% flat and 12.92/12.93/12.89% cumulative. These costs are
consistent across targets; cumulative percentages overlap and are not additive.

The cheapest next discriminating experiment is two versus four file readers at
a fixed eight-connection target and unchanged daemon settings. READ service
occupies about 1.65-1.66 aggregate in-flight calls on average, with a hard
observed peak of two and negligible connection queueing. More readers may hide
source latency but could expose more metadata backpressure. The next code
candidate is bounded cross-caller size-lookup batching before RPC admission,
preserving cancellation, deduplication and pending-blob visibility. Measure
handles/RPC, admission wait, GET amplification and end-to-end progress before
acceptance. Further connection increases or cwalk workers are not supported;
daemon cache changes require separate authorization.

All trials left scratch empty and all 143 snapshot IDs unchanged. Each made
34,453 commit requests with zero failures and zero engine writes. There were
zero reported per-file source errors or sampler errors, and all active RPC
counts returned to zero. Each run recorded two READ cancellations at timeout;
LOOKUP errors (39,308/39,292/39,338) and reconciliation close-time failures
(3,594/3,576/3,563) remain unclassified, not evidence of completed-backup success.
The same PID 1440079, epoch 74, read-write role, zero transactions/intents,
128 MiB metadata cache, 512 MiB block cache and 100 ms flush interval were
verified throughout. No snapshot completed; no restart or tuning occurred.

## Direct NFS connection sweep, R40 (2026-09-26)

**The authorized 4/8/16 sweep stopped on a reserved-source-port allocation
failure at 8 connections. Only the 4-connection observation completed its time
window; 16 connections was not attempted.** The implementation was committed
locally as `d9b496ed7bfbee5add4a262b0abeadd1cbedb6fd` with a detailed message.
Affected race suites, changed-code lint and whitespace checks passed again
before measurement. Nothing was pushed or deployed to the daemon.

Artifacts are under
`/volume2/NASDA2/rustic/db.test/backup-direct-nfs-sweep-20260926-r40-profile-oXpapn`.
The manifest records the exact CLI/source hashes and trial settings. The CLI
was built with `-tags profile`, SHA256
`fc2b8ea427fd97af86c18066c5c44c58f90d90b96adc4bd6449ab4b516187383`.
The scripts, per-trial commands, process/writer samples, CPU profile, four
goroutine captures, final counters and analyzer are retained. An earlier plain
build rejected `--cpu-profile` before backup work; that failed startup and its
healthy postcheck remain separately preserved under
`/volume2/NASDA2/rustic/db.test/backup-direct-nfs-sweep-20260926-r40-awgbab`.

The workload retained R39's 52 source roots, `--use-cwalk --nfs-direct`, 32 cwalk
workers, two readers, 64 MiB client lookup capacity and 600-second timeout plus
45-second shutdown grace. PID 1440079 / epoch 74, 128 MiB daemon metadata cache,
512 MiB block cache and 100 ms flush interval were asserted before and after
each attempt. No cache flushing, service restart or daemon setting change was
performed. Trials are sequential without resetting source or repository state.

| Four-Connection Measurement | Result |
| --- | ---: |
| Timeout exit / measured duration | 124 / 600.254 s |
| Whole harness through postchecks | 601.090 s |
| Last progress sample | 599 s |
| Files / logical bytes | 38,068 / 68,451,353,645 |
| Logical progress rate | 108.98 MiB/s |
| CLI CPU / peak RSS | 903.74 CPU-s / 867,688 KiB |
| Daemon CPU / sampled peak RSS | 3,352.97 CPU-s / 1,107,759,104 bytes |
| Metadata GETs / delivered body bytes | 436,494 / 1,217,158,251,345 |
| Single-handle size RPCs / mean latency | 90,515 / 17.934 ms |
| Client lookup cache peak / evictions | 90,511 of 349,525 / 0 |
| Parent-handle hits / retained metadata opens | 89,850 / 50,000 |
| Snapshot IDs before / after | 143 / 143, unchanged |

Direct source counters recorded 255,812 LOOKUPs, 73,650 GETATTRs, 12,134
READDIRPLUS calls, 1,140,464 reads and 68,452,598,829 read bytes. Operation timing
excludes setup RPCs and is not server-only latency:

| Operation | Mean Queue ms | Mean Service ms | Peak Active |
| --- | ---: | ---: | ---: |
| LOOKUP | 0.026 | 0.248 | 4 |
| GETATTR | 0.301 | 0.217 | 4 |
| READDIRPLUS | 2.687 | 0.921 | 4 |
| READ | 0.008 | 0.872 | 2 |

All operation active counts returned to zero. LOOKUP recorded 39,374 errors
without a reported per-file source error; these are unclassified RPC outcomes,
not evidence of 39,374 failed backup files. The two READ errors were counted as
cancellations. There were no reported source errors, sampler failures or commit
failures in the completed trial. Reconciliation ended with 49,981 scanned,
34,453 reused, zero changed and 3,616 failed at close; individual close-time
failures were not classified. There were 34,453 commit requests, zero engine
writes and no inode revision allocation/publication. No snapshot completed.

The CPU profile contains 890.91 sampled CPU-seconds over 600.09 seconds. SHA256
accounts for 24.11% flat samples and chunker split-point work for 9.66% flat /
13.51% cumulative. The 108.98 MiB/s observation versus R39's 82.05 MiB/s is
historical context only: progress, cache warmth and repository/source state
were not matched, so this is not a causal speedup claim.

The subsequent 8-connection attempt exited 1 after 3.129 seconds with
`no available reserved RPC source port` while initializing a source export.
It made no READDIRPLUS or READ calls. The active-RPC admission limit does not
bound socket allocation: each export still opens its own pool, and the client
allocates reserved source ports in 512..1023. Multi-export pools and sockets
left by preceding trials can exhaust this range; their individual contributions
were not separately measured. The driver stopped rather than changing source
port security, waiting/retrying repeatedly, or attempting 16 connections.

Both attempted profile trials left scratch empty and the same 143 snapshot IDs.
Final read-only preflight reconfirmed PID 1440079, epoch 74, read-write status,
zero transactions/intents and unchanged cache/flush settings; the profile
listener was gone. The next prerequisite is bounded shared or lazy per-server
transport allocation with cancellation/fairness tests, followed by a newly
identified comparison. RPC multiplexing and higher defaults are not justified
by this partial sweep. Recovery evidence and WAL quarantine were untouched.

## Direct NFSv3 backup observation, R39 (2026-09-26)

**The direct-NFS backup ran and stopped cleanly at the approved time limit. No
snapshot completed, and this is not a matched speedup comparison.** The trial
used the existing52 mounted source roots with `--use-cwalk --nfs-direct`,32 cwalk
workers,4 direct NFS connections per export,2 file readers and64MiB client lookup
capacity. The recovered production daemon stayed PID1440079/epoch74 with its
original100ms flush interval,128MiB metadata cache and512MiB block cache.
No daemon/service deployment, restart or setting change occurred during the run.

Artifacts are under
`/volume2/NASDA2/rustic/db.test/backup-direct-nfs-20260926-r39-HVh8lZ`:
the exact CLI, source revision/patch, sources/command, bounded `run.cjs`, samples,
CPU profile, goroutine captures, final statistics and reproducible `analyze.cjs`.
The CLI includes only an additional JSON `nfs_source_stats` event over the
direct-NFS implementation at `d2ca03497`; text-only verbose output previously
hid these counters in JSON mode. Close/cancellation regression tests, complete
fs/nfs/backupcmd race suites, scoped lint and build passed before measurement.
No builds or tests overlapped the backup.

| Measurement | Result |
| --- | ---: |
| Timeout wrapper exit / duration |124 /600.266s|
| Whole harness including final health checks |601.064s|
| Last progress sample |599s|
| Files processed / logical bytes |33,874 /51,536,858,278|
| Logical data rate at last sample |82.05MiB/s|
| CLI CPU / peak RSS |787.10 CPU-s /822,904KiB|
| Daemon CPU / sampled peak RSS |3,134.96 CPU-s /1,073,786,880 bytes|
| Metadata GET attempts / delivered body bytes |433,242 /1,036,704,944,269|
| Size lookups / mean latency |76,723 single-handle RPCs /20.097ms|
| Client lookup cache peak / evictions |76,720 of349,525 entries /0|
| Snapshot IDs before / after |143 /143, unchanged|

Direct-source counters were10,489 READDIRPLUS calls,707,931 LOOKUP calls,
155,871 GETATTR calls,871,770 read calls,51,538,825,256 bytes read and119,285
metadata-cache hits. Mean read payload was59,120 bytes per adapter call. These
are adapter-operation counters, not every setup/wire RPC or a total cache-hit
ratio. Kernel source-mount counters separately showed617,689 GETATTRs,7,847 READs
and26 READDIRPLUS calls; they exclude userspace RPCs and include other users of
those mounts, so they cannot establish per-process fallback or source I/O cost.

The primary observed wait is blob-admission metadata lookup. Captures at60,180,
360 and550s show `CachedBlobLookup` waiting on `LookupBlobSizesContext`/MultiGet;
the360s sample includes15 admission waiters and4 fetches. All32 cwalk workers are
idle in each capture. Metadata bodies average2.39MB per GET, consistent with
expensive repeated metadata reads at the128MiB daemon cache budget. The client
lookup cache used only14,730,240 accounted bytes and had no evictions. Increasing
cwalk concurrency or that client cache is therefore not justified by this run.
The earlier R32 comparison used1GiB daemon metadata cache and a different prior
state; its throughput is not a controlled mounted-versus-direct comparison.

The CLI CPU profile has769.45 sampled CPU-seconds over600.09s: SHA256 accounts
for23.71% flat samples; chunker split-point work9.28% flat/12.82% cumulative;
NFS RPC CallContext28.36% cumulative and NFS readAt24.37% cumulative. Cumulative
figures overlap and are not additive. The source client thus has measurable CPU
cost, but average CLI utilization was1.31 cores versus5.22 daemon cores. No daemon
CPU profile was captured; metadata cache pressure is a supported candidate, not a
proven attribution of every daemon cycle.

No per-file source errors or sampler failures were reported. Reconciliation ended
with44,201 scanned,30,765 reused,0 changed/published and3,110 failed paths; close
cancels work before joining, and individual final failures were not classified.
There were30,765 successful commit requests and0 commit failures, with no inode
revision allocation/publication calls. These counts are not completed new-data
publications. The final CLI error was context cancellation at the time limit.
Scratch was empty, all prior snapshot IDs remained, and the same daemon was
read-write with0 transactions/intents afterward. Retained recovery evidence and
WAL quarantine were not touched. Next comparisons must hold cache budget/source
state constant; changing the daemon cache remains a separately approved action.

## R34 writer recovery and full integrity validation (2026-09-26)

**Recovery is now writable and the full index check passed.** The user explicitly
authorized conditional takeover of epoch73 and confirmed that no other host was
writing. A fresh status check verified the unchanged claim and zero active
transactions/intents. The supported promotion used `--force-takeover` with
`--expected-active-epoch=73`, acquired epoch74, and returned read-write. This was
an explicitly authorized conditional ownership transition, not an unchecked claim
edit or a replay-validation bypass.

The post-takeover full checker reported complete coverage,379,934,385 locations
on each side,143 legacy and143 SlateDB snapshots, zero mismatches, zero unresolved
references/snapshots, zero pending crawl debt and zero warnings.419,530 imported
packs remain informational; unknown tier/retention/usage counts are unchanged.
The checker and timeout processes exited, encrypted scratch was empty, and the
daemon remained PID1440079/epoch74/read-write with zero transactions/intents.
The invoking tool was cancelled while its bounded child continued; no duplicate
check was launched. Final JSON and telemetry are available, but the original
wrapper's exit/time record was lost. This is metadata/index validation, not full
payload restore or crash-recovery acceptance.

Progress reached finalization at about352s. The expensive stages were legacy
scan97s, SlateDB scan104s, and partitioned location comparison84s; encryption
audit14s, spool finalization10s, catalog join26s, cleanup7s and final validation10s
account for the remainder. The scan consumed376,766,240 records/35,623,707,642
logical bytes across257 ranges. Peak accounted scratch was39,883,431,208 bytes.
Configured scan/RPC concurrency was32, but `compareLocationPartitions` caps
comparison tasks at4. A5.022s late comparison/cleanup sample used0.448 checker
cores, with the busiest thread in `rpc_wait_bit_killable`; the daemon was nearly
idle then. Whole-check daemon telemetry measured1,055.78 CPU-seconds over352.294s,
about3cores on average. These observations do not support a wholly single-threaded
checker: bounded merge parallelism and mounted-NFS scratch I/O are relevant.
No CPU profile was captured, so the exact CPU hot functions remain unmeasured.

The temporary recovery service drop-in was removed without restarting the daemon;
the original lifecycle settings and installed binary/unit remain unchanged.
Retain the complete evidence and live WAL quarantine. Do not restore the old tail
onto this writer-advanced state. Artifacts include `live-takeover-*.json`,
`live-check-writer.json`, `live-check-writer-monitor.jsonl`, the progress log and
`live-writer-validated.json` in the existing recovery directory. The read-only
blocker described below is historical and has been resolved.

## Authorized R34 live recovery: readable, writer blocked (2026-09-26)

**Read access is restored, but writable recovery and backup retries remain
blocked.** The user authorized recovery after requesting a direct-NFS backup
retry. This supersedes the historical stopped/unrepaired status below, but does
not constitute a full integrity or no-data-loss claim.

Before changing live storage, `rsync -aHnc` found no differences from the
preserved encrypted `evidence/` tree under
`/volume2/NASDA2/rustic/db.test/recovery-r34-20260925-mWTsrl`.
The service was inactive with MainPID0 and no local `vaulticdb` process.
The incident-specific `recover-live.cjs` passed read-only preflight, then moved
exactly6,621 previously inspected stale WALs (495,526,630 bytes) into
`/ncl1-1-vs-50/fme_dump/amakura/db.r34-quarantine-20260926`.
Each file matched both evidence and the prior quarantine manifest by SHA256;
membership, sequence bounds below the flushed watermark, regular-file type,
single link, canonical paths and same filesystem were checked before moving.
Every quarantined file was rehashed afterward. No WAL was deleted, ordering
validation bypassed, or preserved evidence modified.

The original production daemon started normally as PID1440079, with the original
100ms flush interval,128MiB metadata cache and512MiB block cache. Application
reads using the authoritative feature and required metadata encryption returned
all143 snapshot IDs, exactly matching the pre-incident set. The initial scratch
CLI attempt omitted `VAULTIC_FEATURES=slatedb-authoritative=true`; its unavailable
engine error was a runtime configuration error, not another WAL replay failure.

The daemon opened read-only because writer epoch73 remains claimed. Normal
promotion, without `--force-takeover`, was refused and rolled back. Final observed
epoch remains73, current writer epoch0, and active transactions/write intents0.
The full index checker was attempted with the established96GiB memory/scratch,
32-worker settings and600s+45s limit, but exited1 after0.684s because it could not
begin its fenced read session on the read-only daemon. It produced no integrity
coverage; the snapshot-ID check must not be represented as a full index check.

Conditional takeover with `--expected-active-epoch=73` requires explicit approval
to override the earlier no-forced-takeover restriction and confirmation that no
other host is writing. That confirmation was unavailable, so no takeover,
manual claim edit, backup, or new throughput measurement was performed.

The temporary recovery drop-in
`/run/systemd/system/vaulticdb-rustic.service.d/90-recovery.conf` remains active:
automatic restart is disabled and the stop hook uses normal demotion without
`--force`. The installed daemon and original unit hashes remain unchanged.
The service is active/read-only, not fully recovered. Keep both the complete
evidence tree and the live quarantine (including its manifest). Do not overlay
old WALs onto a running or subsequently advanced database; rollback requires
stopping the service and reviewing the complete preserved state.

Artifacts in the recovery directory include `recover-live.cjs`,
`live-quarantine.jsonl`, `live-snapshots.json`, `live-writer-ready.json`,
`live-writer-after-promotion.json` and `live-check-*` outputs. Resume with explicit
writer-ownership authorization, then complete integrity validation before the
bounded direct-NFS backup experiment.

## Bounded atomic publication groups, isolated R38 (2026-09-26)

**Keep the group prototype default-off; reject general rollout.** Combining up to
four changed inode publications and their revision allocation in one transaction
improves the one-ID and shared-manifest fixtures, but distinct manifest-backed
publication is 2.54x slower and consumes 1.95x daemon CPU versus the current default.
This is an isolated publication-stage experiment, not completed-backup throughput.

`PublishAllocatedReconciledRevisionGroup` allocates consecutive revisions, stages
each member's existing reconciliation plan in order, updates the counter, and uses
one normal serializable durable commit. Later plans read earlier staged writes,
including shared canonical manifests and reference counts. Only `Aborted` causes
bounded retry; the pure builder must regenerate every revision-bearing key/value.
Lost durable acknowledgements can leave the entire group committed despite an
error; returned revisions and success counters require acknowledgement.

The API accepts one to four changed members, at most 4096 content IDs per group,
8 MiB of accounted input bytes, and at most 1024 related puts, debt keys, and
hardlink parents per member. Accounted bytes cover revision values, related
mutation keys/values, debt keys and hardlink names, not all keys, planner expansion,
transaction memory or process RSS. These are input limits, not a total-memory bound.
`Options.AtomicPublicationGroups` is default false, has no CLI flag, and rejects
combination with `AtomicInodePublication`. Ordinary grouping/reservation remains
the default. Directory/root and hardlink routing are unchanged.

Preparation completes before publication; preparation failure fails the group.
Verified unchanged members are reused without allocation. Changed members publish
all-or-nothing, and failed/uncertain calls expose no tentative changed entries in
the reconciler's result map. Already verified reused entries remain valid.
`atomic_group_calls`, `atomic_group_failures`, and `atomic_group_ns` separately
attribute the combined API; group mode records no ordinary inode publication or
standalone allocation calls. Reserved/assigned counts advance only after success.

Matched measurements used the unchanged 12f686c29 daemon, explicit 100ms WAL flush,
fresh disposable HDD-NFS databases, four independent reconciler streams sharing
one schema store/daemon, and three sequential unprofiled repetitions per profile.
There were no concurrent builds/tests. Each profile command retained the
600-second cap plus 45-second termination grace and completed normally. Control
streams publish four independent inodes concurrently after one group reservation;
candidate streams plan four members sequentially inside one atomic transaction.
All values below are three-run means; compare modes within each profile.

| Profile / content / mode | Mean seconds | Daemon CPU seconds | RSS end MiB | Commit attempts / failures | WAL PUT attempts |
| --- | ---: | ---: | ---: | ---: | ---: |
| One ID / distinct / grouped | 1.608005 | 0.580000 | 28.68 | 208.00 / 48.00 | 32.00 |
| One ID / distinct / atomic-group | 0.787151 | 0.420000 | 22.93 | 80.00 / 48.00 | 16.00 |
| One ID / shared / grouped | 1.875280 | 1.426667 | 29.48 | 680.00 / 520.00 | 36.00 |
| One ID / shared / atomic-group | 0.785419 | 0.430000 | 22.91 | 80.00 / 48.00 | 16.00 |
| 129 IDs / distinct / grouped | 1.488564 | 3.163333 | 57.63 | 102.00 / 22.00 | 21.33 |
| 129 IDs / distinct / atomic-group | 3.787211 | 6.173333 | 39.35 | 53.33 / 37.33 | 32.00 |
| 129 IDs / shared / grouped | 6.782913 | 16.880000 | 51.99 | 503.33 / 423.33 | 116.67 |
| 129 IDs / shared / atomic-group | 3.726365 | 6.110000 | 33.76 | 54.67 / 38.67 | 32.00 |

The one-ID profile has 128 inodes; the manifest profile has 64 inodes with 129 IDs
each, above the 128-ID inline threshold. Candidate/control runtime ratios are
0.490, 0.419, 2.544 and 0.549 in table order; daemon CPU ratios are 0.724, 0.301,
1.952 and 0.362. Successful commits fell from 160 to 32 and 80 to 16 respectively.
That reduction still does not guarantee lower runtime: distinct-manifest failed
attempts increased from 22.00 to 37.33 while each conflict can replan four members.
Sequential in-group planning also reduces independent planning overlap. Further
attribution is needed to separate these costs; no automatic size threshold or
fallback is justified by two profiles.

All 24 measured cases passed complete ordered content reconstruction, every
inode/manifest reference count, path bindings, no-work unchanged reuse, publication
accounting, zero active transactions/intents, graceful close/reopen, immutable
records/current pointers and next-revision checks. Native tests additionally cover
shared 129-ID read-your-writes (four inode references plus one manifest reference),
late-member rejection/rollback, bounds/exhaustion, forced fence conflict/retry,
cancellation, process crash before commit, withheld durable acknowledgement and
acknowledged recovery for both single and group publication. Mixed reuse reserves
only changed members; preparation and group failures expose no changed results.
Unsupported stores and mutually exclusive modes are rejected.

Three race-enabled repetitions of native publication/allocation regressions passed,
as did full reconciliation and backup-command race suites, new-code lint and editor
diagnostics. The cancellation fixture initially expected only a Go context error;
it now also accepts gRPC `Canceled`. Failed fixture attempts and the earlier tiny
two-stream exploratory run are excluded from the table. No durability check was
relaxed. Production remains stopped, unrepaired and unmodified.

CPU is the daemon user+system counter delta, including background work and status
overhead. RSS is an end sample, not a peak or total client/service memory claim.
WAL PUTs are instrumented attempts, not distinct physical objects. Group/API time
sums overlap across streams. Client CPU, sustained large-repository load, complete
backup/restore and peak-memory acceptance remain outstanding.

Artifacts: `/volume2/NASDA2/rustic/db.test/atomic-group-20260926-r38-PeLPKO` contains
the measured source diff/base, executable, hashes, both profile logs, reproducible
`analyze.cjs`, validated `analysis.json` and final source diff. Test executable SHA256:
`1898577ee0d73fa2d152150b48bf395acbf89ca6d1002f096cac98a97c0c4fb3`.
No deployment, service restart, settings change, live repair, actual backup or push
was performed. Any future actual backup must explicitly use `--use-cwalk` and
retain the existing time limits unless separately approved.

## Atomic publication scalability gate, isolated R37 (2026-09-26)

**Do not promote the R36 prototype to normal backup configuration.** Its extra
counter conflicts outweigh the saved durability round trip for distinct content
under concurrent publication. No runtime code or defaults changed in R37; the
prototype remains opt-in and disabled in the backup command.

The existing fixture now supports bounded inode/content/stream counts and validates
complete manifest-backed content plus every content reference count. Four streams
own independent reconcilers and maps, each retaining four-worker groups, for at
most sixteen in-flight publications. They share one schema store/daemon, so this
models publisher contention, not four independent full backup processes.

Each profile used three sequential unprofiled repetitions with the unchanged
12f686c29 daemon, fresh disposable HDD-NFS databases, explicit per-file path
bindings, and the normal 100ms WAL flush interval. No other builds/tests ran during
measurement. Each profile command had a 600-second cap and 45-second termination
grace; both completed normally. The profiles were 128 inodes with one content ID
each and 64 inodes with 129 IDs each (above the 128-ID inline threshold). Distinct
content uses unique IDs per inode; shared content uses the same list and canonical
manifest. Compare modes within each profile, not absolute times between profiles.

| Profile / content / mode | Mean seconds | Daemon CPU seconds | RSS end MiB | Commit attempts / failures | WAL PUT attempts |
| --- | ---: | ---: | ---: | ---: | ---: |
| One ID / distinct / grouped | 1.593786 | 0.540000 | 29.91 | 208.00 / 48.00 | 32.00 |
| One ID / distinct / atomic | 1.811381 | 1.926667 | 32.91 | 870.33 / 742.33 | 36.00 |
| One ID / shared / grouped | 2.251795 | 1.570000 | 30.96 | 728.33 / 568.33 | 44.67 |
| One ID / shared / atomic | 1.704827 | 1.850000 | 32.12 | 845.33 / 717.33 | 34.00 |
| 129 IDs / distinct / grouped | 1.440545 | 3.126667 | 57.15 | 101.00 / 21.00 | 19.33 |
| 129 IDs / distinct / atomic | 6.512823 | 16.346667 | 50.93 | 471.67 / 407.67 | 108.67 |
| 129 IDs / shared / grouped | 7.385534 | 19.150000 | 52.89 | 539.67 / 459.67 | 120.67 |
| 129 IDs / shared / atomic | 6.478079 | 15.816667 | 52.35 | 449.33 / 385.33 | 107.33 |

All table values are three-run means, including fractional attempt counts. Runtime
ratios (atomic/grouped) were 1.137 and 4.521 for distinct one-ID/manifest-backed
content, with daemon CPU ratios 3.568 and 5.228. Shared-content runtime ratios were
0.757 and 0.877; daemon CPU ratios were 1.178 and 0.826. The shared-content benefit
does not justify accepting the distinct-content regressions. Aggregate successful
commits still fell from 160 to 128 (one-ID profile) and 80 to 64 (manifest profile),
but fewer successful commits did not imply less work or lower runtime.

CPU figures are daemon process user+system deltas spanning publication, including
background daemon work and measurement-call overhead. CPU/memory availability was
verified for all 24 records. RSS is sampled once at phase end: it is not peak RSS,
service/cgroup memory, or evidence of a peak-memory improvement. CLI CPU/RSS and
sustained large-repository behavior remain unmeasured. WAL PUTs are instrumented
attempts, not a count of distinct physical objects. Summed per-stream/group and
worker durations overlap and must not be added to elapsed time.

All measured cases passed publication accounting, full ordered content assembly,
inode/manifest reference counts, path bindings, unchanged reuse with no extra
allocation/publication, zero active transactions/intents, graceful close/reopen,
current pointers, immutable records and the next revision. A two-stream 129-ID
fixture passed under the race detector, followed by the full reconciliation race
suite. New-code lint is clean. The first small manifest fixture used an inline-only
reference expectation; it was corrected to include canonical manifest references
and its failed runs were excluded. The planner and production code were unchanged.

Next candidate: amortize allocation and publication across a bounded group without
making each independent inode replan against the global counter. Group atomicity,
read-your-writes reference accounting, failure ordering and revision/fence safety
must be proven before accepting such a design. No group-atomic implementation,
heuristic threshold, concurrency increase or durability relaxation is included here.

Artifacts: `/volume2/NASDA2/rustic/db.test/atomic-scaling-20260926-r37-mJnUej`
contains the exact source diff, executable, daemon/test hashes, `inline.log`,
`manifest.log`, and validated `analysis.json`. Production remains MainPID0,
inactive/dead and unrepaired with unchanged daemon/unit hashes. No deployment,
service restart, live repair, actual backup or push was performed. Any later actual
backup comparison must explicitly use `--use-cwalk` under the existing time limits.

## Atomic allocation/publication prototype, isolated R36 (2026-09-26)

Following R35's roughly 49% allocation share, an opt-in prototype reserves one
revision inside the same serializable transaction that publishes its metadata.
It uses the existing complete reconciliation planner and normal durable `Commit`,
including reference counts, immutable records, debt and revisioned path bindings.
Counter conflicts retry the whole transaction with a newly built revision. It
does not weaken writer fencing, acknowledgement requirements, or the 100ms WAL
flush interval. There is no process-wide reservation cache or larger group.

`reconcile.Options.AtomicInodePublication` defaults to false and is enabled only
by the explicit native test fixture in this change. No CLI flag/default or daemon
deployment changes. The regular backup path remains the grouped-reservation path.
The API contract and uncertainty/metric semantics are documented in Phase 5.

Three sequential unprofiled repetitions compared grouped reservations with atomic
publication, using the same unchanged 12f686c29 daemon executable, fresh disposable
HDD-NFS databases, four publication workers per group, 32 inodes, and 100ms flush.
Each mode tested distinct or shared content and explicit per-file path bindings.
Both modes then checked unchanged reuse, current pointers, immutable content,
path bindings, reference counts, zero active transactions/intents, normal close,
reopen and the next revision. No build/test jobs overlapped measured repetitions.
R35's earlier fixture had no path bindings; use the matched R36 control here.

| Content / mode | Mean seconds | Commit attempts / failures | Successful commits | Instrumented WAL PUT attempts |
| --- | ---: | ---: | ---: | ---: |
| Distinct / grouped | 1.603379 | 40 / 0 | 40 | 32 |
| Distinct / atomic | 0.801002 | 80 / 48 | 32 | 16 |
| Shared / grouped | 1.602141 | 88 / 48 | 40 | 32 |
| Shared / atomic | 0.793930 | 80 / 48 | 32 | 16 |

Atomic publication reduced elapsed time 50.04% for distinct content (2.002x) and
50.45% for shared content (2.018x). All commit/failure/WAL counts were identical
across repetitions. Successful commits fell 20%; instrumented WAL PUT attempts
halved. However, distinct-content commit attempts doubled because independent
publications now contend on the same counter. This additional retry/planning work
is a scalability risk; no CPU/RSS improvement or sustained-load benefit is claimed.
The speedup is specific to this small publication fixture, not a real backup.

Correctness gates passed:

- Failed builders, malformed requests, revision mismatches, attempted counter
  overwrite and sequence exhaustion publish nothing and preserve the counter.
- A held transaction loses a conflict to an intervening standalone revision fence,
  retries above it, and leaves no losing revision. Cancellation joins cleanly.
- Process-kill tests on fresh HDD-NFS stores cover buffered/uncommitted mutations,
  durable commit with its response withheld, and an acknowledged commit. Ordinary
  reopen reads recover counter, pointer, revision and references together, or none
  for uncommitted work. No forced ownership, reset or live storage was used.
- Native atomic/fence/crash/validation plus existing concurrent allocation,
  immutability and shared-publication tests passed three race repetitions. Full
  reconciliation and backup-command race suites passed. New-code lint is clean.

These are process-crash tests, not power-loss tests or proof of full repository
integrity. Larger content manifests, sustained concurrent backups and complete
backup/restore acceptance still require broader candidate validation. The prototype
is retained for that next step, not enabled by default.

Two invalid fixture assumptions were corrected before the final measurements:
counter exhaustion must use the allocator rather than protected `SchemaStore.Put`,
and path validation must explicitly select the tested file paths. Failed fixture
runs are excluded from the table.

Artifacts: `/volume2/NASDA2/rustic/db.test/atomic-publication-20260926-r36-4EPhVL`
contains the exact source diff, test executable, daemon/test SHA256 hashes, complete
repetition log, and validated means in `analysis.json`. No backup command ran;
future actual comparisons must use `--use-cwalk` and the normal 600s/45s limits.
Production remains stopped and unrepaired. No deployment, unit change, live repair,
restart or push was performed.

## Publication attribution and cleanup, isolated R35 (2026-09-26)

Backup optimization resumed without restarting or repairing production. R31/R32
profiles identified group revision allocation and durable publication waits, but
their aggregate daemon counters did not separate the two client-side stages.
This iteration adds attribution and joins reconciliation during backup cleanup;
it does not change publication concurrency, reservation scope, or durability.

Backup cleanup now cancels and joins reconciliation before closing source or
repository handles. It emits one final `reconciliation_stats` JSON record, or a
verbose text summary, including failed/canceled runs. Counters distinguish group
occupancy, reserved/assigned inode revisions, allocation calls/failures, and inode
publication calls/failures. Summed client-call nanoseconds include RPCs, internal
retries and durable waits; they are not daemon-only durable-wait measurements.
The group timer measures joined wall time. Concurrent call times must not be
added to it. Unchanged items occupy slots without allocating or publishing.
The metric definitions and exclusions are recorded in the Phase 5 document.

Three sequential unprofiled repetitions used fresh disposable HDD-NFS databases
and the unchanged deployed-daemon executable, with an explicit 100ms WAL flush
interval. Each case published 32 distinct inodes, then checked unchanged reuse,
zero active transactions/intents, graceful close/reopen, current pointers,
immutable content records and the next revision. Content IDs were either all
distinct or shared by all inodes. Groups of one are a diagnostic control, not the
previous production implementation. No build/test jobs overlapped measurements.

| Content / group size | Mean seconds | Allocation summed seconds | Publication summed seconds | Commit attempts / failures | Instrumented WAL PUT attempts |
| --- | ---: | ---: | ---: | ---: | ---: |
| Distinct / 1 | 6.458560 | 3.210757 | 3.236309 | 64 / 0 | 128 |
| Distinct / 4 | 1.594969 | 0.780487 | 3.239493 | 40 / 0 | 32 |
| Shared / 1 | 6.452848 | 3.207743 | 3.233986 | 64 / 0 | 128 |
| Shared / 4 | 1.594311 | 0.779105 | 3.243711 | 88 / 48 | 32 |

All attempt/failure/WAL counts were identical across repetitions. Grouped
allocation consumed 48.93% (distinct) and 48.87% (shared) of measured group wall
time. Shared-content conflicts added 48 failed commits but no meaningful elapsed
time in this small workload; failed-attempt reduction alone is therefore not a
runtime improvement metric. The roughly fourfold grouping benefit is attribution
of already-implemented behavior, **not a new speedup from this commit**. A next
candidate should investigate removing a separate allocation durability round trip
while preserving revision/fence ordering, rather than increase concurrency or
repeat the rejected 10ms flush trial. No such semantic change is included here.

The first fixture attempt was rejected by the daemon's private runtime-directory
check before work began. It was excluded; the corrected fixture uses the existing
short private socket-directory convention, without weakening permissions.

Full reconciliation and backup-command race suites passed, including all native
measurement cases. Focused allocation/reuse/failure/cancellation and cleanup/output
tests passed three race repetitions. New-code lint reports zero issues. This is
an isolated publication experiment, not an actual cwalk backup or completed
snapshot validation; no backup command was invoked. Representative production
comparison remains blocked by the stopped, unrepaired R34 database, and any later
backup trial must explicitly use `--use-cwalk` and the normal 600s/45s limits.

Artifacts: `/volume2/NASDA2/rustic/db.test/publication-attribution-20260926-r35-CXfLWB`
contains the measurement executable, its source diff, daemon/test SHA256 hashes,
and complete three-repetition log. No production deployment, restart, unit change,
live repair, or push was performed.

## Automatic WAL diagnosis implementation (2026-09-26)

After the isolated recovery investigation, the user approved implementing
automatic diagnosis and advisory recovery-plan generation, with repair still
operator-approved. The daemon now inspects configured metadata/WAL stores on
the pinned SlateDB's sequence-ordering startup error, emits a versioned
`wal_recovery_plan` JSON event to stderr, completes normal cleanup and exits78.
Other failures retain exit1. Both writer startup and non-fencing reader startup
are covered; a fencing message alone does not trigger this classification.

Read-only inspection has time/file/encoded-byte/row limits, checks the manifest
before/after and inspected WAL object metadata before/after each read, and
reports incomplete results rather than guessing when inspection cannot finish.
The plan always requires operator approval, never authorizes automatic repair,
and does not claim exclusive ownership. It provides preservation, copy-path
confinement, validation and approval steps without moving or deleting objects.
The complete contract and limits are documented in `vaulticdb/README.md`.

Validation passed:108 Rust library tests and204 daemon tests, including encrypted
inspection, limits, stalled-store timeout, non-mutation and error classification.
Native race tests exercise actual WAL corruption on disposable local databases,
verify writer/reader reports and exit78, preserve suspect WALs and the reader's
existing writer claim, and verify runtime artifact cleanup. Ordinary storage
restart, loader panic, teardown failures and TCP shutdown gates also pass.
New Go-code lint reports zero issues; editor diagnostics and diff checks pass.

No production deployment, service restart, live metadata repair or benchmark was
performed. The installed binary/unit hashes are unchanged and production remains
MainPID0/inactive/dead. Suppressing supervisor retries requires configuring
`RestartPreventExitStatus=78` in the managed daemon unit; this is documented but
has not been applied to the installed service. This implementation does not
activate the recovered copy or prove full integrity of the R34 database.

## R34 isolated recovery investigation (2026-09-26)

**Production remains stopped and unavailable. Recovery succeeded on a separate
working copy, not on live metadata.** The user approved preservation and isolated
investigation after the incident report below. That approval does not authorize
live WAL quarantine, writer-claim changes, database replacement or deployment.

The complete stopped metadata tree, including both WAL locations and coordination
files, was copied to `evidence/` under:
`/volume2/NASDA2/rustic/db.test/recovery-r34-20260925-mWTsrl`.
The copy contains19,713 regular files totaling40,962,375,075 bytes. A checksum
dry run (`rsync -aHnc --numeric-ids --itemize-changes`) found no differences;
source path/type/size/mtime inventories before and after copying were identical.
A second independent copy to `working/` passed the same checksum check. No
hard links connect either copy to live metadata or to the other copy. NFS
initially inherited0777 despite the creation umask; explicitly setting the
recovery parent to0700 succeeded. Stored database objects remain encrypted.
After the experiments, a fresh live-to-evidence checksum dry run again found
no differences and the live source inventory still matched the pre-copy
inventory. Production remained MainPID0/inactive/dead.

The incident-specific `r34_inspect.rs` probe uses existing required-encryption,
manifest, WAL-reader and SlateDB APIs from the pinned dependency. It rejects
roots other than these two copies and allows replay/writer flush only on
`working/`; refusal checks passed for live metadata and evidence. Explicit
object stores point inside the selected copy, so the persisted absolute local
WAL handoff path is never followed. It emits counts, sequence bounds and digests,
not raw metadata keys/values or credentials. No ordering check was bypassed.

The preserved manifest454 has writer epoch41, `last_l0_seq=272534`,
`replay_after_wal_id=80292`, and `next_wal_sst_id=80297`. These are SlateDB fields,
not the daemon's separately tracked ownership epoch73. WAL83950 ends at279193,
while WAL83951 starts at245877. All6,621 files in the higher tail were decoded:

| Old-tail WAL range | Files | Preserved modification date |
| --- | --- | --- |
|83951..83957|7|2026-09-16|
|87199..87204|6|2026-09-16|
|88364..94971|6,608|2026-09-16|

Every row in that tail has sequence245877..256897, below the manifest's flushed
watermark272534. WAL71167..83950 is contiguous and dated September25. SlateDB's
tail discovery uses exponential probing/binary search with an explicit
contiguous-ID assumption. The older ranges above gaps violate that assumption.
This supports stale sparse WAL namespace reuse as the mechanism to investigate;
it does not establish how the old files survived or exclude every other writer.
The10ms interval is not established as the root cause and remains rejected.

The reversible experiment changed only the working copy:

1. Normal `DbReader` FollowLatest open with WAL replay enabled reproduced the
  exact83951/245877/279193 ordering failure.
2. The6,621 inspected tail files were moved to `quarantine-wal/`, not deleted.
  Before moving any file, the script checked its membership, sequence bounds,
  source type, destination absence and SHA256 against evidence. Hashes were
  checked again after moving.
3. Normal replay succeeded and returned143 snapshot records.
4. Restoring all quarantined files reproduced the original error. Quarantining
  them again restored successful replay with the same snapshot-record digest.
5. Stronger checks verified exact equality with all143 pre-trial snapshot IDs
  and visibility of WAL83950's last durable value, byte-for-byte at sequence279193.
6. A normal SlateDB writer open, memtable flush and graceful close succeeded
  without application-data writes. A fresh reader then passed the same checks.
  The copied manifest advanced to459/writer epoch42, flushed sequence279193,
  replay cutoff83951 and next WAL83952. The snapshot digest remained
  `6d7b9c7d5a16c2fa917591d61a638d5ea731969f5315fb8057194556788b9d43`.

SlateDB normally filters rows at or below `last_l0_seq`; its cross-file ordering
check rejects this tail before that filter can discard those rows. Quarantining
only the lower-sequence tail therefore leaves the newer replay range available.
This is evidence for a recovery candidate, not a complete no-data-loss proof:
full index invariants, historical value coverage, application writer ownership,
production daemon/RPC reopening, crash recovery and payload restore are not
validated by these checks. No production daemon was started in this investigation.
The persisted absolute WAL binding also remains unchanged in the working copy;
do not launch it with inherited production settings.

The original `evidence/` remains the recovery baseline. The working copy has now
advanced its manifest through writer replay/flush; do not blindly restore old
WALs onto that advanced state. The quarantine script rejects occupied targets.
Any further experiment requiring the original state must use a fresh evidence
copy. Live recovery still requires a separately approved plan, including writer
coordination, path confinement, retention of original state, read/integrity gates
and a rollback strategy. Do not restart the benchmark loop.

Artifacts include preservation/working-copy checksum logs, source inventories,
`boundary-inspection.jsonl`, `tail-inspection.jsonl`, before/quarantined/restored/
repeat replay logs, `replay-validated.jsonl`, `writer-flush.jsonl`,
`replay-after-flush.jsonl`, `quarantine-manifest.json`, reversible
`quarantine.cjs`, and the archived probe source/binary with hashes. The temporary
Rust example was removed from the source tree after validation; no runtime
implementation, dependency, production executable or default was changed.

## R34 rejected: fencing failure and blocked WAL replay (2026-09-25)

**Incident state: the metadata service is stopped, not restored to availability.**
The temporary settings were removed and the original unit validated/reloaded,
but normal restart cannot open the database. Do not restart the optimization
loop or treat this as an accepted performance result. The subsequently approved
preservation and isolated investigation are recorded above; no live repair has
been authorized or performed.
No WAL files were deleted, fencing/replay checks bypassed, writer claim manually
changed, or replacement daemon deployed. Subsequent unit inspection found that
the existing systemd `ExecStop` invokes writer demotion with `--force`; therefore
the systemd lifecycle operations must not be described as entirely non-forced.

The user explicitly approved the ten-minute cwalk trial using10ms WAL flush
and1GiB metadata cache, followed by restoration. R34 reused byte-identical R32
CLI/daemon binaries (8064db714 CLI,12f686c29 daemon),52 roots, cwalk32, two file
readers,64MiB result cache, HDD-NFS storage/scratch and600s cap/45s grace.
Before measurement the writer was idle and effective interval/cache budgets
were asserted. Original daemon/unit/shared-policy/quota hashes and snapshot
filenames were archived. No builds or tests overlapped the run.

The backup exited1 after578.78s, before the cap, with:

```text
metadata lookup failed: admit blob bfa33a9e:
SlateDB operation failed: Closed error: detected newer DB client
```

The final snapshot query also failed with this error, so the standard analysis
harness did not complete. Writer status still reported read-write/epoch73 with
zero active transactions/intents despite the closed database. Status role and
idle counters alone are therefore not sufficient post-run health checks. Final
lookup statistics were emitted and scratch was empty; those cleanup results
do not establish metadata integrity or successful snapshot publication.

The explicitly requested non-forced demotion failed when it tried to flush the closed writer.
The override was removed, original configuration reloaded, and a normal systemd
restart attempted. Its existing `ExecStop` hook attempts forced demotion (not
forced takeover); the journal records failure while flushing the closed writer.
The later stop used the same unit. Startup first reported
`writer_claim_already_active` and opened a non-fencing reader, then failed with:

```text
WAL replay saw out-of-order seqs across WAL files.
[wal_id=83951, min_seq=245877, last_seq=279193]
```

A readiness-event wait for the initial restart timed out. Inspection then found
an automatic restart loop; the service was explicitly stopped at restart
counter43. Final state was MainPID0, inactive/dead. Original100ms flush and
128MiB metadata/512MiB block configuration is restored on disk, not verified
through a working database. All143 repository snapshot filenames match the
pre-trial set, but the post-recovery snapshot API read did not succeed. This is
not a restore/integrity guarantee. Only one local vaulticdb process was present
when the initial failure was inspected; remote writers have not been excluded.

Read-only inspection of the pinned SlateDB source found that WAL `PutMode::Create`
returning `AlreadyExists` can become `SlateDBError::Fenced`. That is one possible
source of the newer-client message, not proof of the trigger in this incident.
Replay requires strictly increasing sequence ranges across WAL files; the
reported overlap violates that invariant. The causal chain between10ms cadence,
writer ownership, WAL allocation and replay remains unresolved. The successful
small R33 graceful-reopen tests did not cover this sustained workload/state.

Diagnostic counters below cover different windows and reached data. They are
not a valid capped throughput comparison or grounds to retain10ms:

| Metric | R32 completed cap,100ms | R34 failed before cap,10ms |
| --- | --- | --- |
| Last file progress |119,023|143,903 at577s|
| Logical bytes at that status |194,242,030,245|194,800,514,796|
| Commit attempts /failures |40,114 /217|65,119 /1,603|
| Durable waits /failures |8,422 /0|25,991 /4|
| Mean accumulated durable wait |90.479ms|14.208ms|
| Instrumented WAL PUT attempt delta |9,482|31,890|
| Instrumented WAL PUT byte delta |7,404,597|23,081,870|
| Metadata-object body bytes |37,064,973,533|41,003,246,466|
| Daemon CPU seconds |501.55|699.37|
| CLI CPU seconds /peak RSS KiB |1,128.81 /1,510,584|1,189.47 /1,537,656|

R34 sampled daemon process RSS peaked at1.599GiB. Systemd's stop record instead
reported `28.6G memory peak` for the service. These are different measurements
and the discrepancy has not been reconciled; the sampled process figure must
not be substituted for the service peak or used to claim bounded total memory.

WAL attempt counts are instrumentation outcomes, not unique object counts or a
classification of physical storage errors. Counter deltas subtract the recorded
pre-run baseline. The CLI emitted283,468 single-handle size RPCs and no cache
evictions/location RPCs. No snapshot completed. R34 is rejected and was not
repeated. Recovery-copy investigation was proposed, but the user was unavailable
to explicitly authorize broader recovery; no live repair or database-copy
operation was undertaken. Preserve the stopped state pending an operator-approved
copy and recovery plan; do not discard offending WALs or silently roll back data.

Incident artifacts, exact binaries, snapshots-before, failed-run metrics,
original/stopped hashes, and full recovery journal:
`/volume2/NASDA2/rustic/db.test/backup-flush10ms-20260925-r34-yJr3Hv`.
`daemon-incident.log` records the restart failure. `snapshots.after.json` and a
successful `writer-restored.json` are unavailable; empty files from failed
commands must not be interpreted as successful evidence. The workspace revision
was cbf236cf9, with CLI source separately recorded as8064db714. No push occurred.

## Durable flush-cadence experiment, isolated R33 (2026-09-25)

R32's effective `engine_flush_interval_ms` was100. Its8,422 durable waits
accumulated762,016,399 microseconds, about90.5ms per wait. This is concurrent
accumulated time, not elapsed backup time. The transaction commit implementation
awaits the storage handle's `await_durable()` before acknowledging ordinary
commits. The existing `VAULTICDB_WAL_FLUSH_INTERVAL` tuning parameter changes
flush cadence without switching those commits to deferred durability.

The existing native allocation comparison now explicitly selects100ms for its
baseline modes and adds a grouped10ms case. It asserts the effective interval,
unique revisions,32 successful durable commits for128 grouped revisions, no
failed commits or active transactions/intents, and the next allocated revision
after closing/reopening the database. Reopen happens outside the timed window.
This is a graceful reopen check, not a forced-crash recovery test.

Three race-enabled repetitions used fresh disposable HDD-NFS databases, private
runtime sockets under `/run`, the exact unchanged12f686c29 daemon, and no
overlapping builds/tests during measurement. Each case reserved32 groups of
four revisions, retaining ordinary serializable durable transactions.

| Metric |100ms interval|10ms interval|
| --- | --- | --- |
| Elapsed seconds, repetition1 |3.226927|0.359631|
| Elapsed seconds, repetition2 |3.223664|0.355356|
| Elapsed seconds, repetition3 |3.235672|0.363345|
| Mean elapsed seconds |3.228754|0.359444|
| Commit attempts /successes /failures per run |32 /32 /0|32 /32 /0|
| Mean durable wait per commit |95.241ms|6.108ms|
| Instrumented WAL PUT attempts per run |64|64|

The isolated workload is about8.98 times faster at10ms. Equal instrumented
WAL PUT attempt counts here do not establish equal object counts or cost under
sustained backup traffic: this fixture submits one allocation transaction at a
time. A shorter interval can reduce batching efficiency and increase WAL object
creation/request rates in the full workload. Do not claim an end-to-end backup
speedup or globally optimal interval from these results.

Existing native transaction/session, atomic/shared-content publication and
revision-allocation race gates pass three repetitions with10ms configured.
The complete native-backed reconciliation suite also passes three repetitions
at10ms. The comparison's effective-setting and graceful-reopen assertions pass
for both measured intervals; new-code lint reports zero issues. No production
code, daemon binary, durability contract or default setting was changed.

The user was unavailable to explicitly approve the proposed temporary10ms WAL
and1GiB metadata-cache production trial. Therefore no production restart,
configuration change or backup was performed in R33. Production was verified
unchanged at PID1281656/epoch72, read-write,100ms flush,128MiB metadata/512MiB
block cache, and zero active transactions/intents. The subsequently approved
R34 cwalk trial failed and recovery is blocked, as recorded above; this isolated
result is not sufficient to recommend10ms. Longer snapshot completion/reopen/
restore acceptance remains separately outstanding.

Artifacts, source patch, binary hashes, measurements and gate logs:
`/volume2/NASDA2/rustic/db.test/flush-cadence-20260925-r33-Hk2tYW`.
The test patch is relative to19e519cc2; the production candidate would use the
same8064db714 CLI and unchanged daemon as R31/R32, with only the approved
temporary environment settings varied. No production deployment occurred.

## Group-reservation cwalk comparison and repeat, R31/R32 (2026-09-25)

The user approved the proposed production comparison and confirmation plan.
R31 measured the tested8064db714 CLI with a temporary1GiB metadata cache;
R32 repeated the identical candidate after the first result improved capped
progress. Both used the unchanged12f686c29 daemon,52 roots, explicit cwalk32,
two file readers, four lookup slots,64MiB CLI result cache, HDD-NFS authoritative
storage and scratch, and600s cap/45s termination grace. No builds or tests
overlapped either measurement. The daemon was restarted for each trial and the
original128MiB metadata/512MiB block budgets restored after each run. No shared
policy/quota or installed binary was changed.

| Metric | R29: scalar allocation | R31: group reservation | R32: same candidate |
| --- | --- | --- | --- |
| Last files status at or below600s |105,769|114,180|119,023|
| Logical bytes at that status |181,252,115,885|192,788,205,625|194,242,030,245|
| Metadata-object body bytes |34,533,228,474|36,103,758,306|37,064,973,533|
| Metadata GETs |230,500|248,457|255,028|
| Single-handle size RPCs |235,954|253,478|259,514|
| Mean size RPC |3.360ms|3.027ms|3.500ms|
| Engine writes |66,264|46,107|71,543|
| Commit attempts |41,152|34,174|40,114|
| Successful commits |32,131|34,046|39,897|
| Failed commit attempts |9,021|128|217|
| Late successful commits per second |25.09|17.59|17.60|
| CLI CPU seconds |1,054.70|1,116.97|1,128.81|
| CLI peak RSS KiB |1,494,376|1,373,096|1,510,584|
| Daemon CPU seconds |499.95|467.43|501.55|
| Daemon sampled peak RSS GiB |1.616|1.567|1.601|
| Wrapper exit /duration |124 /601.787s|124 /601.641s|124 /602.141s|

R31 and R32 processed7.95% and12.53% more files than R29, respectively, and
6.36% and7.17% more logical bytes. Their mean capped progress is10.24% higher
by files and6.77% by logical bytes. Repeat spread relative to the two-run mean
is4.15% for files and0.75% for logical bytes. Failed commit attempts fell98.58%
and97.59%; their shares of attempts fell from21.92% to0.37% and0.54%.
These repeated results support retaining bounded group reservation, but are
not a controlled completed-backup speedup: source warmth, earlier committed
metadata, existing-record reuse and reached-data differences remain confounders.
No representative snapshot completed. The comparison also does not individually
attribute the remaining failures, or prove all earlier failures were allocations.

Late rates use the last writer samples at or below420s and590s. R31 completed
2,994 successful commits and53 failures in170.170s; R32 completed2,995 successes
and88 failures in170.170s. R29 had4,270 successes and2,424 failures in170.176s.
Attempt/completion deltas can differ due to boundary in-flight requests. Fewer
allocation commits are intentional, so raw successful commits per second are
no longer a comparable proxy for published revisions. The much lower failure
rate is consistent with R30's isolated counter-contention result. Engine-write
counts and early no-write commit traffic also vary with reuse and reached state.

Resource improvements are not uniformly reproduced: CLI CPU increased in both
runs, and R31's lower CLI peak memory was not repeated in R32. Daemon CPU was
lower in R31 and approximately unchanged in R32. Metadata bytes increased as
the runs reached more data. Both runs emitted final lookup metrics with zero
CLI cache evictions and no location RPCs; peak accounted cache usage was
48,667,776 and49,826,688 bytes, respectively. There is still no evidence that
enlarging the CLI result cache would help this workload.

R31's550s profile shows one worker in `AllocateRevisionBlock` -> `Commit` and
another waiting for that group's allocation mutex. R32's550s profile shows
four workers in `PublishReconciledRevision` -> `Commit`, while the writer joins
the group and archiver callbacks remain backpressured. Durable group allocation
and revision publication are the next measured waits; do not infer that adding
more workers or relaxing durability is warranted. Future work should quantify
group occupancy and allocation/publication durable-wait costs before changing
batching or concurrency. Completion/reopen/restore acceptance still requires a
separately approved longer run.

Both runs completed cancellation within grace, left empty scratch and zero
active transactions/intents, and had no sampler errors. All143 snapshot IDs
were unchanged before/after each run and through restoration. Final production
is PID1281656/epoch72, read-write,128MiB metadata/512MiB block cache, zero active
work. The temporary override is absent. Original daemon/unit/shared-policy/quota
hashes match the archived baselines; executable SHA256 remains
`3a825c277b2273627ca9349122e8c77b32f7ac91710a888411c4765fd6531ccd`.
No installed CLI/daemon replacement or permanent cache-default change occurred.

Artifacts, exact binaries, command lines, profiles, analyses and restoration:
`/volume2/NASDA2/rustic/db.test/backup-group-revisions-20260925-r31-BU6cQJ` and
`/volume2/NASDA2/rustic/db.test/backup-group-revisions-20260925-r32-AOZK9z`.
Both used clean8064db714 and byte-identical candidate binaries. No runtime code
changed during this validation; the race/lint/native test gates recorded in R30
apply to this exact candidate. On-demand mode remains experimental.

## Group-scoped revision reservation, isolated R30 (2026-09-25)

R29's aggregate commit failures do not distinguish allocation from publication.
Inspection found that each new inode first allocates a revision through the same
repository-wide counter. Four concurrent publications therefore contend on that
counter even when their inode and path identities are independent. The existing
`AllocateRevisionBlock` API already durably reserves a bounded sequence in one
serializable transaction and is used by snapshot import.

A first candidate serialized scalar allocation within each reconciler. An
isolated native comparison rejected it: eliminating failed attempts also removed
overlap of durable waits, making128 allocations approximately four times slower.
That production-code candidate was removed before committing. Reconciliation now
lazily reserves at most one successful block per pending publication group, sized
to that group (at most four). Unchanged groups allocate nothing. Each changed
inode receives one unique reserved revision and retains its own serializable,
durable publication transaction and conflict retries. Unused numbers are discarded
when the group joins; no reservation survives into another group, directories,
hardlinks or snapshot-root/fence allocation. Gaps are permitted by the existing
reservation contract. Stores without the optional block API retain scalar
allocation. No larger process-wide revision cache or relaxed fence was added.

The user was unavailable to explicitly approve another temporary production
cache-setting change. Production was therefore neither restarted nor modified.
R30 here is an isolated native allocation experiment, **not a cwalk backup run**.
No backup was invoked. A matched ten-minute cwalk comparison with R29's temporary
1GiB metadata budget was pending explicit approval at this checkpoint; the
subsequently approved R31/R32 results are recorded above.

Three repetitions per mode used fresh disposable HDD-NFS databases, runtime
sockets under `/run`, and the exact unchanged12f686c29 daemon from R29. Each
mode allocated128 unique contiguous revisions. The concurrent mode used four
scalar allocators; serialized used one; grouped reserved32 consecutive blocks
of four. Tests verified revision uniqueness, successful-commit counts, and zero
active transactions/intents. There were no overlapping builds or other test
jobs during the measured repetitions.

| Allocation mode | Mean seconds | Commit attempts per run | Successful commits | Failed attempts |
| --- | --- | --- | --- | --- |
| Four concurrent scalar callers |3.232456|320|128|192|
| Serialized scalar caller |12.940688|128|128|0|
| Grouped, four revisions per reservation |3.233254|32|32|0|

All three repetitions had identical attempt/success/failure counts within each
mode. Grouping reduced allocation attempts90% and successful allocation commits
75%, with effectively unchanged allocation throughput versus concurrent scalar
callers. These results isolate counter contention and avoided work, not an
end-to-end speedup, CPU reduction, or proof that all9,021 R29 failures came from
allocation. Shared-content publication can still conflict. Representative cwalk
throughput and cancellation were subsequently checked in R31/R32; completed
snapshot/reopen validation remains outstanding for this candidate.

Regressions cover lazy reservation, partial groups, unused-number disposal,
unique revisions, reservation failure/cancellation, and scalar fallback through
the existing overlap tests. A native reconciliation regression publishes four
nodes, reuses them without allocating again, verifies the next scalar revision
follows the reserved block, and checks cleanup; it passes three race repetitions.
The complete native-backed reconciliation suite also passes three repetitions.
Broad archiver/backup/index/repository-index race gates pass, native
transaction/session/publication tests pass three repetitions, new-code lint
reports zero issues, and the profiling CLI builds successfully.

An initial fixture attempt placed sockets on HDD-NFS and failed before any
measurement because inherited permissions violated the daemon's0700 runtime
directory requirement. The test-only `VAULTICDB_TEST_DATA_ROOT` override now
separates database storage from socket/runtime storage. The failed preflight log
is retained and excluded from the table above. Production remained
PID1260903/epoch68, read-write,128MiB metadata/512MiB block cache, zero active
work, with original executable and unit hashes. No daemon/CLI deployment or
shared-cache policy change occurred.

Artifacts and measurement logs:
`/volume2/NASDA2/rustic/db.test/allocation-r30-UVmGty`.
The source patch is relative todfacb5b4f. The profiling candidate is
`/run/vaultic-phase33-attribution/vaultic-group-revisions`.

## Bounded concurrent inode publication, R29 (2026-09-25)

R28's late profile showed serialized durable revision publication holding up
the archiver. Reconciliation now overlaps at most four ordinary inode
publications. Each retains its own serializable transaction, conflict retries
and durable commit. Duplicate inode identities, source paths and normalized
snapshot paths force an ordered flush. Deferred items, preparation errors,
directories and hardlinks also flush the pending group. Workers join before
the writer updates shared maps or records failures, in original input order.
Directory/hardlink handling, publication fences and durability are unchanged.

The user approved a ten-minute comparison with the same temporary1GiB metadata
cache as R28, followed by restoration. R29 retained52 roots, explicit cwalk32,
two file readers, four lookup slots,64MiB CLI result cache, HDD-NFS storage and
the600s cap/45s grace. The daemon remained the exact12f686c29 binary. No builds
or tests overlapped measurement; no persisted read-cache policy/quota changed.

| Metric | R28: serial publication | R29: at most four publications |
| --- | --- | --- |
| Last files status at or below600s |97,558|105,769|
| Logical bytes at that status |169,557,219,997|181,252,115,885|
| Metadata-object body bytes |33,820,343,954|34,533,228,474|
| Metadata GETs |212,642|230,500|
| Single-handle size RPCs |218,506|235,954|
| Mean size RPC |4.018ms|3.360ms|
| Engine writes |14,367|66,264|
| Commit attempts /successes /failures |19,444 /19,444 /0|41,152 /32,131 /9,021|
| Late successful commits per second |9.89|25.09|
| CLI CPU seconds /peak RSS KiB |968.4 /1,481,144|1,054.7 /1,494,376|
| Daemon CPU seconds /sampled peak RSS |361.09 /1.553GiB|499.95 /1.616GiB|
| Wrapper exit /duration |124 /601.791s|124 /601.787s|

Late rates use the last writer samples at or below420s and590s: R28 completed
1,683 successful commits in170.165s; R29 completed4,270 in170.176s. Thus useful
commit throughput rose2.54 times, not the approximately fourfold increase in
attempts. R29 also recorded2,424 failed commits in that late window. Boundary
in-flight requests mean attempt and completion deltas need not sum exactly.
The full run's9,021 failures are21.9% of attempts. Aggregate commit telemetry
does not classify their causes or distinguish revision allocation from
publication; do not label every failure a measured serializable conflict.
Both code paths already retry aborted transactions. No retry-limit error was
reported; the only terminal error was expected snapshot cancellation.

At550s, three publication workers were waiting in
`PublishReconciledRevision` -> `Commit`, with the writer joining its group and
archiver callbacks backpressured in `Observe`. Publication/durability waits
remain relevant. Attribute failed commits and shared-key contention before
raising concurrency or considering revision-allocation amortization. The
four-worker candidate is retained provisionally: files increased8.4% and
logical bytes6.9%, but CPU cost and failed attempts increased. Repeated-run
source warmth, previous writes and reached-data differences remain confounders.
This is not a completed-backup speedup or a count of published revisions.

Cancellation emitted final lookup metrics and left empty scratch, zero active
transactions/intents and no sampler errors. The CLI cache peaked at235,954
entries /45,303,168 accounted bytes, with no evictions or location RPCs. No
snapshot completed; all143 snapshot IDs remained unchanged through restoration.
The runtime override was removed and the original settings verified at
PID1260903/epoch68, read-write,128MiB metadata/512MiB block cache, zero active
work. Executable, unit and shared policy/quota hashes match the original values.
No installed CLI or daemon executable was replaced.

Regression coverage verifies the four-worker bound, overlap, joining on failure
and cancellation, and duplicate identity/path detection. A native shared-content
test concurrently publishes four inline revisions and replays each, requiring
exactly four references/inodes/revisions. It passes five race repetitions.
All reconciliation tests pass three race repetitions; broad archiver, backup,
index and repository-index race gates pass. Native transaction/session,
publication, recovery and encrypted-read gates pass three race repetitions
against the exact measured daemon. New-code lint reports zero issues.

Artifacts, exact binaries, source patch, profiles, analysis and restoration
evidence are in
`/volume2/NASDA2/rustic/db.test/backup-publication-overlap-20260925-r29-QAMqpE`.
The source patch is relative tobdab29e1c. On-demand mode remains experimental;
completed-backup/reopen acceptance and matched repetitions remain outstanding.

## Metadata-cache sizing and linear directory publication, R27/R28 (2026-09-25)

SlateDB's default split cache has512MiB for data blocks and128MiB shared by
filters, indexes and table statistics. The user approved a temporary increase
of only `VAULTICDB_META_CACHE_BYTES` to1073741824. Effective budgets were
asserted before/after measurement. The daemon binary stayed at12f686c29 and
no shared read-cache tier or persisted policy/quota setting changed. The existing
parser regression now also verifies metadata-only tuning leaves other settings
unspecified. All three tuning tests pass.

R27 used the exact earlier profiling CLI,52 roots, explicit cwalk32, two file
readers, four lookup slots,64MiB CLI result cache and600s cap/45s grace. Read
volume fell sharply, but termination exceeded grace: the process was killed at
645.009s with exit137. It emitted no final lookup statistics, retained encrypted
scratch, and left one read transaction with zero write intents. The610s profile
showed `Reconciler.Close` waiting while `publishDirectories` scanned the published
map through `filesystem.Dir`. Each directory rescanned every published node and
the scan did not check cancellation, yielding quadratic work at publication.

Directory publication now indexes source paths by parent once and updates that
index as child directories are published bottom-up. Child records are still read
from the current published map; snapshot names and cross-filesystem handling are
unchanged. Cancellation is checked before validation and during indexing and
directory/child traversal. Parent grouping is linear in nodes/edges, plus the
existing directory sort and serialization costs, with additional O(nodes) index
memory. Tests verify nested child counts, snapshot rather than source basenames,
a linear bound on parent lookups, and cancellation during index construction.

After R27, normal non-forced demotion pruned the expired transaction and allowed
restoration. Writer status alone does not prune idle transactions; normal demotion
does. The daemon's default transaction idle timeout is300s. No active transaction
was forcibly discarded. The user then explicitly approved R28 using the fixed
CLI and the same temporary1GiB metadata budget, followed by restoration again.

| Metric | R24:128MiB metadata | R27:1GiB metadata | R28:1GiB + publication fix |
| --- | --- | --- | --- |
| Last files status at or below600s |48,847|93,390|97,558|
| Logical bytes at that status |93,604,158,848|164,921,783,316|169,557,219,997|
| Metadata-object body bytes |1,614,533,514,696|32,494,687,655|33,820,343,954|
| Daemon sampled peak RSS |1.014GiB|1.552GiB|1.553GiB|
| Mean size RPC |19.879ms|unavailable|4.018ms|
| Wrapper exit /duration |124 /633.018s|137 /645.009s|124 /601.791s|
| Final transactions /intents |0 /0|1 /0|0 /0|
| Scratch cleanup |empty|encrypted spill retained|empty|

R28 issued218,506 single-handle size RPCs, with no CLI cache evictions or location
RPCs. Metadata body bytes per handle fell from about13.51MB in R24 to154.8KB,
about99% lower. Daemon CPU was361.09s versus4614.22s; CLI CPU was968.4s and peak
RSS1,481,144KiB. There were14,367 engine writes and19,444 commits. The daemon
recorded232,408 positive and1,419,453 negative point-filter checks, including
10,992 false positives. Filter checks are not cache hit/miss counters. These
measurements support metadata-cache sizing as the main read-amplification lever,
but do not isolate filters versus index/statistics objects within that cache.

R28 completed cancellation within the existing grace and emitted final metrics,
with no sampler errors. It did not complete a snapshot: all143 snapshot IDs
remained unchanged in both runs. File progress still plateaued late; the550s
profile shows reconciliation waiting in `PublishReconciledRevision` -> `Commit`,
with bounded queue backpressure reaching the archiver. Serialized revision
publication/durability waits are the next bottleneck, not grounds to weaken
atomic publication or durability. The directory fix addresses measured algorithmic
work and cancellation, not every remaining publication wait.

Repeated-run source warmth, earlier writes and reached-data differences remain
confounders. R28 processed roughly twice R24's files and1.81 times its logical
bytes, but this is a capped-window result, not a completed-backup speedup. Both
cache experiments were restored as approved: current production is PID1243174,
epoch66, read-write, zero transactions/intents,128MiB metadata/512MiB block cache.
Executable SHA256 remains `3a825c277b2273627ca9349122e8c77b32f7ac91710a888411c4765fd6531ccd`.
Unit, shared policy/quota hashes and snapshot IDs match the pre-trial baseline.
No daemon binary replacement or permanent cache-default change was made.

Artifacts, exact binaries, source patch, profiles and restoration evidence:
`/volume2/NASDA2/rustic/db.test/backup-meta-cache-20260925-r27-sYjTlq` and
`/volume2/NASDA2/rustic/db.test/backup-linear-publication-20260925-r28-0zW9b4`.
All reconciliation tests pass three race repetitions, broad archiver/backup/index
race gates pass, and native transaction/session/publication/recovery/encrypted-read
tests pass three race repetitions against the exact measured daemon. New-code
lint reports zero issues. The fixed CLI is archived in R28; no installed CLI
or daemon executable was replaced. On-demand mode remains experimental.

## Cache policy diagnosis and gated R26 probe (2026-09-25)

Read-only inspection resolved R25's policy failure. The configured metadata
root is a symlink; its persisted revision-0 cache policy contains the tier
`bulk-memory`, enabled with a64GiB ceiling. R25 requested `r25`. Policy loading
correctly rejects that identity mismatch rather than replacing shared policy.
The existing local-cache restart test now uses a filesystem-backed policy store
instead of InMemory and asserts healthy coordination. It passes on both local
temporary storage and disposable HDD-NFS storage.

The user approved a corrected, temporary trial using `bulk-memory`, its exact
persisted policy values and a512MiB aggregate cap, with32MiB in-flight fills,
8 background tasks and4MiB entries. The daemon binary stayed at12f686c29; the
5951f82ad fallback was not deployed. Preflight required enabled tier/aggregate
state and512MiB effective capacity. A new30s gate required increasing hits and
accounted usage within the cap, terminating the backup if validation failed.
The same52-root cwalk32 workload, CLI, two file readers and64MiB result cache
were retained. No builds or tests overlapped the probe.

**R26 failed the hit gate and stopped after30.906s**, not600s. At30s it had
processed1,002 files /1,204,747,931 logical bytes. The CLI exited130 after the
harness sent SIGTERM; its final assertion marked the probe unsuccessful. It
emitted1,789 single-handle size RPCs and final lookup metrics, with zero CLI
cache evictions/location RPCs. Metadata-object counters increased by10,370 GETs
and24,325,750,892 body bytes. Scratch was empty and writer cleanup completed.
This is not an end-to-end throughput result or a valid full-window comparison.

The warm cache snapshot reported enabled state,512MiB effective capacity and
zero used/reserved bytes, with0 hits,10,421 aggregate misses and16 coalesced
origin reads avoided. Reconciliation lag was5. Effective capacity alone proved
insufficient to establish healthy quota coordination. The persisted quota ledger
has `aggregate_max_bytes: null`, while R26 requested512MiB. `read_quota_ledger`
requires an exact match and rejects the conflicting limit before admission.
The ledger also contains1,177 old `decrypted/bulk-memory` entries charged at
65,361,707 bytes with an expired manager lease. No plaintext cache was enabled
and those entries were not reinterpreted as ciphertext or manually removed.

The approved override was removed and production restored at PID1229351,
epoch62, read-write with zero active transactions/intents. All143 snapshot IDs
and the original executable, unit and persisted-policy hashes match pre-trial
values. No snapshot completed and no binary replacement occurred. Trial artifacts
are in `/volume2/NASDA2/rustic/db.test/backup-bulk-cache-20260925-r26-N4ttaK`;
policy evidence, restart checks, tests and the subsequent candidate are in
`/volume2/NASDA2/rustic/db.test/cache-policy-probe-20260925-s2A8ZE`.

Local follow-up now preserves existing cache hits during unhealthy quota
coordination but returns origin responses directly on misses, avoiding response
buffering, sidecar hashing and unsuccessful fill setup until coordination
recovers. A regression verifies warm hits, cold range contents and counters.
Another reproduces both persisted tier-identity and aggregate-limit mismatches,
including policy preservation and the distinction between enabled policy and
healthy quota coordination. All108 library and196 daemon tests pass serially;
the68 cache tests also pass with HDD-NFS filesystem fixtures. Package formatting
and three native Go race repetitions against the newly built candidate pass.

The new fallback is **not deployed or production-benchmarked**. Both observed
configuration failures remain fail-closed intentionally. Another cache trial
must first reconcile the persisted tier policy and shared quota contract through
an approved operator workflow, or provide a separately enforced process-local
budget. Do not silently replace either document, enable the old plaintext tier,
or raise the memory limit to match old settings. Preflight must include quota
health/reconciliation state and a successful admission/hit probe, not merely
requested environment values or enabled/effective-capacity fields.

## Inactive ciphertext-cache trial and fallback, R25 (2026-09-25)

The existing read-cache manager can sit below metadata encryption and cache
immutable SST ciphertext. After all65 existing cache tests passed, the user
approved a temporary memory tier:512MiB tier/aggregate budget,4MiB maximum
entry size,32MiB in-flight fill budget and8 background tasks. Authoritative
repository/database storage remained HDD-NFS. Only a runtime systemd drop-in
changed; the daemon and CLI binaries were identical to R24. The writer was
demoted before each restart. Trial PID1221564 ran at epoch59.

**The intended cache did not become operational.** Before/after monitor records
both report tier `r25` disabled, effective capacity0 and used bytes0. The
post-run aggregate reports0 hits,217,537 misses and17 coalesced origin reads
avoided. Policy synchronization lag increased from4 to36. Byte-traffic and
fill telemetry are unavailable, not measured zero. Environment verification
confirmed the requested settings but was insufficient: the effective-state
precondition should have rejected this run before backup started. The exact
policy initialization/synchronization failure remains unresolved; do not treat
this as a comparison against a functioning512MiB ciphertext cache.

R25 retained the52 roots, explicit cwalk32, two file readers, four lookup slots,
64MiB CLI result cache and600s cap/45s grace. Artifacts and exact harness are in
`/volume2/NASDA2/rustic/db.test/backup-cipher-cache-20260925-r25-yA3cH8`.
Configuration, restart safety evidence and subsequent candidate validation are in
`/volume2/NASDA2/rustic/db.test/cipher-cache-trial-20260925-cflRKL`.

| Metric | R24 no read-cache wrapper | R25 inactive read-cache wrapper |
| --- | --- | --- |
| Files at600s |48,847|18,733|
| Logical bytes at600s |93,604,158,848|24,383,534,534|
| Size RPCs /mean latency |119,486 /19.879ms|39,059 /61.184ms|
| Metadata-object body bytes |1,614,533,514,696|518,468,438,795|
| Body bytes per size handle |13,512,324|13,273,981|
| CLI peak RSS |1,300,616KiB|1,040,356KiB|
| Daemon sampled peak RSS |1.014GiB|0.833GiB|
| CLI /daemon CPU |695.24s /4614.22s|196.97s /4642.25s|
| Wrapper exit /duration |124 /633.018s|124 /611.538s|

File progress regressed about62%; lower total read volume primarily reflects
less work. Each size RPC still carried one handle; the CLI cache peaked at
39,055 entries (7,498,560 accounted bytes), with zero evictions/location RPCs.
All143 snapshot IDs remained unchanged; no snapshot completed. There were2,908
engine writes and12,836 commits. Cancellation completed within grace, final
lookup metrics were emitted, scratch was empty and no sampler errors occurred.
Repeated-run warmth, prior writes and reached-data differences remain confounders.

The drop-in was removed as approved. Production is restored to its original
configuration at PID1225182, epoch60, read-write with zero transactions/intents.
Daemon SHA256 remains `3a825c277b2273627ca9349122e8c77b32f7ac91710a888411c4765fd6531ccd`;
the unit hash and snapshot set also match their pre-trial values.

Local follow-up found that `CacheManager::get_opts` continued buffering origin
responses, hashing payloads and scheduling empty fill work when every tier in
its confidentiality domain was disabled. It now returns the origin response
directly in that state, after existing eviction handling. Range/error semantics,
authentication and enabled-tier behavior remain unchanged. A focused regression
verifies range contents, missing-object errors and bypass/origin/miss/admission
counters. All108 library and194 daemon tests pass serially, as do three native
Go race repetitions against the newly built release. Package-scoped formatting
passes; workspace-wide formatting encounters unrelated missing examples and
formatting differences in the vendored ctap-hid-fido2 package.

This fallback fix is **not deployed or production-benchmarked**. R25 predates it
and demonstrates no runtime improvement. Before another cache experiment,
resolve the policy failure and require enabled state, nonzero effective capacity
and a successful fill/hit probe. Keep the current production settings unchanged.

## Right-sized decrypted range buffers, R24 (2026-09-25)

Tracing R23's daemon memory growth found a concrete retained-allocation problem:
EncryptedObjectStore returned a small plaintext range using `Bytes::slice` on
the complete decrypted encryption chunk. A downstream cache could retain the
whole allocation while accounting only for the short returned range. The wrapper
now copies partial ranges into right-sized Bytes buffers. Full selected chunks
still use the existing zero-copy path. Authentication, ciphertext range fetching,
encryption format and cache/concurrency settings are unchanged.

The regression writes nonuniform plaintext with64KiB chunks and verifies exact
content, returned ranges and retained allocation capacity for64-byte reads within
a chunk, across a chunk boundary and at the object end. All16 encryption tests
pass, including tamper/truncation, cross-repository authentication, key rotation,
range/object-store semantics and unique-ciphertext allocation reuse. All108 Rust
library and193 daemon tests pass serially; the exact release also passes three
native Go race repetitions covering transactions, sessions, publication fences,
atomic snapshot export, recovery and encrypted blob/overlay reads.

The user explicitly approved deployment and the next capped comparison. The
quiescent epoch57 writer was demoted before executable replacement. The unchanged
service now runs PID1217457 at epoch58 with SHA256
`3a825c277b2273627ca9349122e8c77b32f7ac91710a888411c4765fd6531ccd`.
Rollback executable, candidate, source/test evidence, hashes and snapshot sets
are retained in
`/volume2/NASDA2/rustic/db.test/daemon-range-buffers-20260925-sLQ3Gj`.
The service-unit hash and all143 snapshot IDs were unchanged after restart.

R24 uses the exact R23/R22 profiling CLI,52 roots, cwalk32, two file readers,
four lookup slots,64MiB result cache, exclusions and600s cap/45s grace. Exact
binaries, source delta, profiles, samples and analysis are in
`/volume2/NASDA2/rustic/db.test/backup-range-buffers-20260925-r24-58gVIl`.

| Metric | R23 sliced buffers | R24 right-sized buffers |
| --- | --- | --- |
| Daemon sampled peak RSS |35.994GiB|1.014GiB|
| Daemon post-run RSS |35.646GiB|0.656GiB|
| Files at600s |50,754|48,847|
| Logical bytes at600s |98,973,507,929|93,604,158,848|
| Mean size RPC |18.714ms|19.879ms|
| CLI peak RSS |1,320,520KiB|1,300,616KiB|
| CLI /daemon CPU |709.96s /4043.2s|695.24s /4614.22s|
| Metadata-object GETs |714,816|686,602|
| Metadata-object body bytes |1,697,757,934,919|1,614,533,514,696|
| Aggregate transaction-slot wait |0.005764s|0.007313s|
| Wrapper exit /duration |124 /626.438s|124 /633.018s|

Both daemons started cold after restart, at0.067GiB and0.039GiB RSS respectively.
R24's result cache peaked at119,482 entries (22,940,544 accounted bytes,34.2%
capacity), with zero evictions and zero location RPCs. All119,486 size RPCs still
carried one handle. Aggregate overlapping size-RPC time was2375.311s. Probe hits
were150,239, including32,362 negative hits, with490,214 misses.

All143 snapshot IDs remained unchanged; no backup snapshot completed. There were
15,845 engine writes and14,137 commits. Final writer state was read-write at
epoch58 with zero active transactions/intents. Scratch was empty and no sampler
errors occurred. Cancellation finished within grace, about33s after the cap,
with final metrics emitted.

Decision: retain the small allocation fix. The measured daemon-memory reduction
is about97%, supported by a direct allocation-capacity regression. This is not a
runtime speedup: file progress was about4% lower, mean RPC latency slightly higher
and daemon CPU higher. Repeated-run warmth/prior writes/reached-data differences
remain confounders. Ciphertext bytes per requested handle are essentially
unchanged; the1.61TB of metadata traffic remains the next bottleneck to address.
The fix bounds the allocation retained by a partial returned range, not total
daemon memory for every workload. On-demand stays experimental.

## Concurrent pinned-transaction reads, R23 (2026-09-25)

Following R22's single-handle admission stalls exposed a server-side bottleneck:
Get and MultiGet held the same exclusive transaction-slot mutex across storage
reads. All four client lookup slots therefore serialized on their pinned read
transaction. With native SlateDB MultiGet disabled, grouping client calls alone
would still execute a sequential per-key fallback under that lock.

TransactionSlot now uses a Tokio RwLock. Get, MultiGet and read-session validation
use shared guards; mutations, scans, stream setup, commit, rollback and other
lifecycle paths retain exclusive guards. This leaves transaction consumption
exclusive and preserves existing write/scan behavior while allowing concurrent
point reads on the same pinned view. Client batching, lookup concurrency, cache
budget, native MultiGet selection and daemon configuration are unchanged.

The existing MultiGet regression now holds a read guard while Get and MultiGet
complete, for both native and fallback modes. It verifies write exclusion,
rollback waiting until the guard is released, transaction consumption afterward,
and NotFound for later reads. All193 daemon tests pass serially. The default
parallel run had two cross-test InventoryWal failpoint failures: the rebuild
test received the failure intended for the dedicated-WAL inventory test. This
test-isolation issue was not changed here. Native transaction/read-session,
publication-fence, atomic snapshot-export, pack-recovery and encrypted-overlay
tests passed three race repetitions against the exact newly built release.
Rust formatting and diff whitespace checks pass; the existing unused
ATTRIBUTES_XATTR warning remains in the Rust test build.

The user explicitly approved deploying this candidate and running the comparison.
After verifying zero active transactions/intents, the writer was demoted, stopped,
and replaced. The unchanged service now runs PID1210406 at writer epoch57, SHA256
`e26ca0e3869d38f55be5bf7cd6b68c87abe073fec4f57572f31da51a10bd27be`.
The prior executable, candidate, source delta, test output, hashes and snapshot
sets are retained in
`/volume2/NASDA2/rustic/db.test/daemon-shared-reads-20260925-86oyPo`.
All143 snapshot IDs and the service-unit hash were unchanged after restart.

R23 reused R22's exact profiling CLI (SHA256 `e6259738...`),52 roots, explicit
cwalk32, two file readers,64MiB result cache, four lookup slots, exclusions,
repository cache and600s cap/45s grace. Artifacts, exact binaries, sampler,
profiles and analysis are in
`/volume2/NASDA2/rustic/db.test/backup-shared-reads-20260925-r23-7lGMQM`.

| Metric | R22 exclusive slot | R23 shared point reads |
| --- | --- | --- |
| Files at600s |21,434|50,754|
| Logical bytes at600s |28,226,857,074|98,973,507,929|
| Mean size RPC |52.961ms|18.714ms|
| Size RPCs /handles |45,104 /45,104|125,072 /125,072|
| Aggregate transaction-slot wait |1771.797393s|0.005764s|
| Slot acquisitions |71,636|160,527|
| CLI peak RSS |986,408KiB|1,320,520KiB|
| CLI /daemon CPU |210s /1349.37s|709.96s /4043.2s|
| Daemon sampled peak RSS |13.24GiB|35.99GiB|
| Metadata-object body bytes |649,906,425,032|1,697,757,934,919|
| Wrapper exit /duration |124 /607.572s|124 /626.438s|

R23 made714,816 metadata-object GETs. Its cache peaked at125,068 entries,
24,013,056 accounted bytes (35.8% capacity), with zero evictions and zero location
RPCs. Probe hits were156,273, including33,337 negative hits, with512,521 misses;
these are rechecked probes, not unique requests. Aggregate overlapping size-RPC
time was2340.648s. The daemon started at0.067GiB RSS after restart and ended at
35.646GiB; R22 started at10.984GiB and ended at13.098GiB. The larger retained
server working set is a material tradeoff, not a bounded-system-memory result.

All143 snapshots remained unchanged, with no completed snapshot. There were
15,914 engine writes and12,026 commits. The daemon remained read-write at epoch57
with zero active transactions/intents, scratch was empty and no sampler errors
occurred. Cancellation completed within grace, though cleanup took about26s
after the cap rather than R22's8s; final counters were still emitted.

Decision: retain the shared-read fix, which removes directly measured unintended
serialization without raising configured client concurrency. The observed file
progress is2.37 times R22 and logical bytes3.51 times, but restart/cache warmth,
earlier writes and reached data prevent treating these ratios as isolated or
general end-to-end speedups. No representative backup completed, and full-index
R19 still reached more logical data. On-demand remains experimental.

The next priority is explaining and bounding daemon memory and reducing metadata
read amplification before increasing concurrency further. Result-cache capacity
is still not limiting. Bounded cross-call batches may become useful with a
verified native MultiGet path, but were deliberately not combined with this
lock change; production cache/MultiGet settings were not silently altered.

## Cache budget and measured cold-lookup cost, R22 (2026-09-25)

Backup now exposes `--metadata-cache-mib` with the existing64MiB default,
positive/platform-overflow validation, and no change to the four RPC slots or
lookup semantics. New atomic counters track actual LRU probes (positive/negative
hits and misses), evictions, peak occupancy, provider size/location calls,
requested handles and aggregate provider-call nanoseconds. Counts survive Close.
After repository cleanup joins workers, JSON backup output emits one
`metadata_lookup_stats` record, including cancellation paths. Text mode has a
verbose summary. These are component measurements, not a total RSS cap, and
rechecks mean the probe counts must not be interpreted as unique requests.
Timed provider calls include errors/cancellation but exclude slot waiting.

A two-entry regression verifies exact probe counts, positive and negative hits,
eviction, peak/capacity and RPC/handle accounting after Close. Location success
and failure accounting, canceled-command JSON after engine close, default budget,
zero/negative/overflow rejection and a valid1MiB budget are covered. Cache/CLI
tests passed three race repetitions; full archiver/backup/index/repository-index
race gates, three native encrypted spill-overlay repetitions, and the new-code
lint gate passed. Editor diagnostics are clean.

R22 used the same52 source roots, cwalk32, two file readers, exclusions,600s cap
and45s termination grace as R21, explicitly setting `--metadata-cache-mib=64`.
The daemon PID1184230/epoch56 and binary/configuration were unchanged. Artifacts,
candidate, source patch, profiles, counters and reproducible analysis are in
`/volume2/NASDA2/rustic/db.test/backup-cache-metrics-20260925-r22-KpZPk3`.
The profiling-enabled CLI SHA256 is
`e62597388ccc91f4f13251cc7a68b2df216a84b03dcb6f841947165d1543da84`.

| Cache/RPC observation | R22 |
| --- | --- |
| Capacity |349,525 entries /67,108,800 accounted bytes|
| Peak occupancy |45,100 entries /8,659,200 accounted bytes (12.9%)|
| Evictions |0|
| LRU probe hits /negative hits /misses |53,701 /11,766 /183,208|
| Size RPCs /requested handles |45,104 /45,104|
| Mean handles per size RPC |1|
| Aggregate size RPC time |2388.748s, overlapping across four slots|
| Mean size RPC duration |52.961ms|
| Location RPCs |0|

Final progress at606s was21,434 files and28,226,857,074 logical bytes versus
R21's18,047 files and23,696,605,565 bytes at603s. CLI CPU was210s and peak RSS
986,408KiB, versus179.42s and1,047,500KiB. The capped wrapper ended with exit124
at607.572s. The daemon used1349.37 CPU seconds and236,521 metadata-object GETs
transferred649,906,425,032 bytes. These repeated runs have cache-warmth, prior-write
and reached-data confounders; instrumentation does not establish a speedup.

All143 snapshot IDs were unchanged; no snapshot completed. There were11,774
engine writes and8,957 commits. Post-run role remained read-write at epoch56,
with zero active transactions/intents, empty encrypted scratch and no sampler
errors. Final lookup counters were emitted despite cancellation.

Decision: retain64MiB as the default, not a larger budget. Zero evictions at13%
occupancy rules out capacity pressure in this measured window. Zero location
RPCs rules out a location cache as an explanation or remedy for this workload.
Four almost continuously occupied size slots, each sending one cold handle,
identify first-time admission lookup cost as the immediate target. Investigate
small bounded cross-call batches and daemon read amplification rather than
retaining more already-cached results. Longer/different workloads still need
their own cache-pressure measurements. On-demand remains experimental.

## Approved deployment, absolute paths and on-demand R21 (2026-09-25)

The user approved the production daemon update. The release built from
`ab09d26c7` passed three native race repetitions of publication-fence and atomic
snapshot-export tests before deployment. `vaulticdb-rustic.service` now runs
PID1184230, writer epoch56, binary SHA256
`706abf74f0a543dab129da812a449a36db4b23ea57bd87e80201201e7dd400e2`.
The unit and configuration were unchanged. The143 snapshot IDs matched exactly
before/after restart, with zero active transactions and write intents.
The previous executable and deployment evidence are retained in
`/volume2/NASDA2/rustic/db.test/daemon-update-ab09d26c7-3rtgZr`.
The initial comparison used a lost shell variable; the fresh explicit-path
comparison is `snapshots-verified.txt`, not the empty interrupted output.

The absolute-source blocker was missing synthetic ancestor directories in the
reconciled snapshot graph. Publication now plans missing path containers and
writes them bottom-up before linking the root. Observed directory references
and the source-keyed observation map remain unchanged; ancestor lookup uses
snapshot paths. Missing intermediate observations below an observed directory,
parent-relative escaping paths, and cancellation fail closed. Synthetic
directories use fresh revision-derived identities in the synthetic FSID0
namespace; no live filesystem attributes are invented. This fixes both full
and on-demand loading, not just the experimental mode.

The existing disposable HDD-NFS fixture completed absolute cwalk snapshots
`458d8797` (two9MiB-total files,0.915s reported backup duration), `0462bccf`
(two unchanged files,0.902s), and full-index control `df7fd96c` (0.864s).
Restore matched all source bytes; `check --read-data` passed five snapshots and
seven packs before the latter two snapshots. Fresh encrypted scratch was empty
after close. These runs reused existing data and are correctness checks, not
throughput measurements. The disposable writer was explicitly demoted and
stopped. Reconciler races passed three repetitions, all backup/index race gates
passed, and the changed reconciliation package passed the new-code lint gate.

R20 was rejected before opening the repository because the ordinary CLI build
omitted profiling flags. R21 used `-tags debug` and the same52 roots, cwalk32,
two file readers, exclusions, cache directory, profiles and600s/45s timeout as
R19. Its extra flags were `--metadata-on-demand --metadata-scratch DIR`.
Artifacts, exact candidate, source delta, sampler and analysis are in
`/volume2/NASDA2/rustic/db.test/backup-on-demand-20260925-r21-nze5jw`.
Candidate SHA256:
`868a82c197ef2f55a54a82160f8251a8b8eb9d1a6c105cbbdd3550ddd2c40c9a`.
The subsequent lint-only split separates ancestor planning/publication; the
archived candidate and source preserve the exact measured implementation.

| Metric | R19 full projection | R21 on-demand |
| --- | --- | --- |
| First archive status |216s|1s|
| Final progress |59,824 files /136,114,669,242 logical bytes at607s|18,047 files /23,696,605,565 logical bytes at603s|
| CLI peak RSS |30,367,292KiB|1,047,500KiB|
| CLI CPU |1485.12s|179.42s|
| End-to-end wrapper |611.110s, exit124|604.760s, exit124|

R21 made227,603 metadata-object GETs, consuming545,327,742,389 body bytes
(about507.9GiB), and1174.28 daemon CPU seconds. The550s stack had all four
size-lookup RPC slots waiting in MultiGet, admission callers waiting on those
results/slots, and file workers blocked on blob-save admission. Final daemon RSS
was11,798,224,896 bytes: the CLI reduction is not a total-system memory bound.
There were16,754 engine writes and7,251 commits. The daemon stayed read-write at
epoch56 with zero active transactions/intents; all143 snapshot IDs were unchanged,
scratch was empty, and there were no sampler errors. Cancellation was surfaced
as a metadata-lookup cancellation and completed within the grace period. No
snapshot completed during the cap.

Decision: keep on-demand experimental and full-index loading the default.
Startup and CLI memory improved substantially, but throughput regressed. Earlier
writes, cache warmth and the daemon restart confound isolated speedup claims;
this result does not justify promoting the mode for runtime performance.

The CLI already has a64MiB accounted LRU for pinned blob size/existence results,
including confirmed misses, plus shared in-flight requests and an active/spilled
write overlay. This is not a process RSS limit or a cache of arbitrary database
records; blob locations are not cached. It is separate from SlateDB block/index
caches and the encrypted pack cache. The budget remains hard-coded. Useful next
work is exposing the budget with hit/miss/eviction, occupancy, batch-size and RPC
latency metrics, then distinguishing result-cache churn from first-read cost.
The measured stalls support investigating daemon read amplification and bounded
admission batching; blindly increasing the client budget or adding a location
cache is not yet supported by these measurements.

## Owned on-demand backup sessions and publication fencing (2026-09-25)

Backup now has an explicit, experimental `--metadata-on-demand` mode requiring
`--metadata-scratch`. Full-index loading remains the default. The mode owns a
renewable pinned ReadSession, a64MiB accounted size cache with four lookup slots,
and encrypted temporary spill for exported writes. It scans `p:` pack records,
not the full `b:` blob catalog. Pending exports are recovered one authenticated
header at a time, checked against pinned blob records, saved as compatibility
indexes and acknowledged only after success. Unknown sizing aborts startup;
an incomplete local projection is never treated as known zero usage.

Activation preserves the existing engine instance and callbacks; repeated
activation is rejected. Ordinary cache pruning is bypassed because it derives
its keep-set from a complete in-memory index. Context-aware reads/admission use
the pinned provider plus active/spilled writes. Known lookup/spill/session
failures block flush and snapshot publication. Close joins lookup workers,
removes spill state, rolls back the pinned session and closes the client.
Repository format2 and authoritative metadata are required; deferred ingest and
dry-run are rejected. Maintenance commands are not switched to this projection.

Client-side validation alone was insufficient: generation decisions are outside
the snapshot transaction. The protocol therefore adds an optional PublicationFence
to Commit and advertises a `publication_fence` capability. Fenced commits require
a separate live read transaction, matching generation and decision, healthy
metadata, and synchronous durability. The daemon holds its generation/writer
transition mutex from validation through commit, excluding concurrent local
generation changes. Normal unfenced commits retain their existing path. The
client validates the session after opening the publication transaction and before
commit, connects session/cache cancellation to the request, and rejects old
daemons before enabling the mode. A lost commit response still has the normal
uncertain-outcome semantics; cancellation cannot undo an already committed write.

Three race-enabled native repetitions cover server rejection of stale generation,
stale decision, closed read transaction and missing capability, with rollback and
zero remaining transactions/intents. Late client validation failure leaves both
the snapshot absent and export checkpoint pending. Successful publication can
reference a root written after the session was pinned. Encrypted-pack integration
recovers515 locations from two pack records with no full projection, reads existing
and post-pin spilled blobs, blocks reads/flush/publication after session loss,
and verifies scratch/session cleanup. Incomplete sizing and repeated activation
are also rejected in repeated race tests.

The candidate CLI and daemon were built locally. Disposable HDD-NFS command
artifacts are under
`/volume2/NASDA2/rustic/db.test/backup-point-smoke-20260925-7NK6j7`;
the daemon socket used a separate private `/run` directory. This fixture used
random test data and no repository password or metadata encryption; the temporary
write overlay remains encrypted. No production credentials or data were used.
All backup commands explicitly used cwalk with four workers and a10-minute cap.

The first nested absolute-source backup failed with "snapshot has no reconciled
root". Preserving engine identity did not resolve it; the full-index control
failed identically. The shared reconciler can return no synthetic root when no
top-level child is represented. This pre-existing absolute-source case was not
changed in this stage and remains a production-validation blocker.

Using the source directory as `.` completed four on-demand snapshots:

| Check | Observation |
| --- | --- |
| Relative-source backup | One8MiB file; snapshot `732ba67d` published |
| Unchanged-parent backup | One unchanged file, zero new blobs; snapshot `1576cd63` |
| New-data backup | One unchanged8MiB file plus one new1MiB file; two data blobs and one tree blob; snapshot `30fd24fc` |
| Standard restore | Restored8MiB file matches source byte-for-byte |
| Standard `check --read-data` | Three snapshots, six packs, no errors |
| Cleanup-fixed backup | Two unchanged files, zero new blobs; snapshot `41b6c76f`; fresh scratch parent empty after exit |

The command smoke tests also exposed a resource-ownership bug: the CLI repository
helper returns an unlock closure, not a repository-close closure. Backup teardown
now closes the repository after unlocking, which releases the owned engine/session
and encrypted spill. An ordering/idempotency regression passes three race runs.
The final command used a new `clean-scratch-XzT8er` parent and left it empty; no
fixture daemon remained on its private runtime path. Earlier pre-fix scratch
directories remain in the disposable artifact tree, not in production.

The first successful relative run reused packs uploaded by the preceding failed
attempts; it is not a clean initial-upload performance baseline. These are workflow
and integrity checks, not a matched runtime comparison or a production speedup
claim. Full archiver/backupcmd/engine/repository-index race suites and targeted
native transaction/session/catalog/publication suites pass. Rust server compilation
passes; the transaction-module unit-test filter matched zero tests, so server
behavior was verified through the native Go tests instead.
Editor diagnostics show only two pre-existing `fmt.Appendf` suggestions in
untouched transaction-test lines; the edited production paths have no diagnostics.

The running production daemon was not restarted, replaced or deployed. A matched
production cwalk comparison requires explicit approval to deploy the new fence
capability, complete sizing metadata, and resolution of the absolute-source
reconciliation blocker. No claim of bounded total backup memory is made: pack
inventory traversal, large individual rows/headers, active uploads, exporters and
scratch disk usage remain relevant scaling costs.

## Incremental, batched pack-export acknowledgments (2026-09-25)

Daemon-backed engines now acknowledge newly written packs after each successful
compatibility-index export instead of retaining every pending pack until final
flush. The master-index saved callback runs after export and optional encrypted
spill, before spill eviction. Failed callbacks retain the index and propagate an
error. Pack IDs come from the index's pack list, not another full blob walk.
Default non-daemon index retention remains unchanged.

Pending IDs are registered before insertion into the compatibility index, since
insertion can synchronously trigger export. Successfully acknowledged IDs are
removed immediately. New-write bookkeeping therefore tracks active/unexported
indexes and concurrent exporters, rather than the entire backup's writes.
Recovered pending packs remain a separate case: catalog loading can split one
pack across four projections, so these IDs are acknowledged only after every
recovery index has exported successfully. The recovered pending set still scales
with unfinished work from previous runs; this is not a whole-backup memory cap.

Flush is exclusive against pack publication/insertion, preventing acknowledgment
of a pack that has not yet reached an exported index. Pack publication and export
errors are sticky within the engine. A subsequent flush cannot accidentally
acknowledge failed finalized indexes that the legacy exporter does not retry.
Recovery requires a fresh engine/catalog load. This is not the owned read-session
and final snapshot-publication fencing required for on-demand backup activation.

`SchemaStore.MarkPacksPublished` accepts at most256 IDs, deduplicates them, reads
pack records with negotiated MultiGet limits, and commits lifecycle changes and
publication-history events atomically. Existing published records are no-ops;
missing/invalid records roll back the entire batch. Mutation batches retain the
negotiated item/byte limits, and existing conflict retries remain in place. Each
engine acknowledgment group is at most256 packs; a larger export can complete in
several independently durable groups. The single-pack API delegates to this path.

Three race-enabled native-fixture repetitions measured the same work reduction:

| Acknowledgment of16 packs | Durable-wait attempts |
| --- | ---: |
| Individual calls | 16 |
| One batch, negotiated two-item RPC limit | 1 |
| Duplicate/replayed batch, empty/canceled/oversized requests, missing-pack failure | 0 additional |

Every pack has exactly one publication event, and transactions/write intents end
at zero. This is a93.75% reduction in acknowledgment durable-wait attempts for this
fixture, not measured storage latency or end-to-end backup throughput. An attempt
to run these fixtures with HDD-NFS `TMPDIR` failed before daemon readiness because
the runtime directory did not satisfy its required0700 mode. Normal test-runtime
directories were used for the successful correctness/counter runs; no storage
performance conclusion is drawn from them.

Native tests also cover16 automatic index rotations acknowledged before final
flush, cancellation after compatibility export leaving a pack pending, and a
four-fragment recovered pack whose second export fails. Retrying the failed engine
remains blocked; a fresh load and successful export completes recovery. Saved-index
callback ordering/failure tests pass three race repetitions. Full archiver,
backupcmd, engine and repository-index race suites pass with child-only permission
capability drops; targeted authoritative catalog/recovery, pinned-session and
publication-history tests pass as well.

Production backup still uses the full catalog and does not select the spill-backed
point-read adapter. No production backup, push, daemon restart or deployment was
performed for this stage; no RPC/schema change is required. Remaining activation
work includes owned read-session/publication fencing, conservative unknown-sizing
handling, and scan-free startup selection before a matched10-minute cwalk run.

## Encrypted spill for exported write indexes (2026-09-25)

`NewSpillingBlobLookup` creates an opt-in write index and temporary encrypted
overlay. Successfully exported indexes are handed to the overlay before being
removed from the in-memory master index. Failed compatibility export or failed
handoff retains the entries and returns an error. Existing constructors keep
their original retention behavior. This spill-backed index is intended for the
context-aware lookup adapter, not synchronous maintenance callers that expect
the master index to contain every location.

The active index retains the existing50,000-blob rotation threshold and bounded
pack-sized overshoot. Exported entries use Pebble with an8MiB block cache,
4MiB memtables, a two-memtable write-stop threshold and write batches capped at
approximately1MiB. These are component budgets, not an exact heap/RSS cap: active
uploads, index export encoding, concurrent exporters, Pebble overhead, OS cache
and individual returned location lists also consume memory. Disk usage grows
with new writes. The daemon engine's pending-pack map still requires bounded
bookkeeping before claiming bounded memory for the whole backup.

Scratch values use AES-GCM with random nonces and key-bound authentication;
handle and location lookup tokens use HMAC-SHA256. Encryption/token keys are
random per instance and are not stored on disk. The constructor requires an
explicit scratch parent, creates a private temporary directory and removes it
on close. The overlay uses non-sync writes because it is disposable process
state: authoritative pack publication and compatibility export precede eviction.
It is not a crash-recovery database and is not reopened after process restart.
Known disk/authentication failures are sticky and cannot be hidden by cache hits.

Lookups check active writes, spilled writes and then pinned-snapshot results.
Positive spill sizes reuse the existing bounded LRU; negative snapshot hits
still check local writes. Admission rechecks local visibility under its lock.
Tests cover32 export cycles retaining zero exported in-memory locations while
all512 spilled locations remain readable, automatic export/spill of50,001
entries,16 concurrent readers/admitters racing eviction, failed export/handoff,
encrypted-value tampering and cleanup. A real-daemon test confirms that a write
published after snapshot pinning remains visible after export and eviction,
cannot be admitted again, and stays absent from the pinned snapshot itself.

The concurrent handoff test passes ten race-enabled repetitions; spill/cache and
native visibility tests pass repeated race runs. Full archiver, backupcmd, engine
and repository-index race suites pass, as do targeted native recovery, point-read,
catalog-loading and metadata-error tests. Editor diagnostics are clean. The first
handoff test needed the existing unrestricted test saver rather than the public
append-only repository interface; correcting that fixture passes the same check.

`BenchmarkWrittenBlobLookup` compares repeated256-handle size batches before
and after spill. Scratch was under the HDD-backed NFS path
`/volume2/NASDA2/rustic/db.test`, with no overlapping tests. The working set is
small and warm; this does not measure cold HDD latency or a full backup.

| Path | Initial ns/op (three runs) | Final ns/op (three runs) | Initial/Final Allocations |
| --- | --- | --- | --- |
| Active index | 22933 / 22700 / 22800 | 22808 / 23046 / 23433 | 1 / 1 |
| Spilled entries | 744554 / 748621 / 741027 | 24563 / 24416 / 24194 | 3073 / 1 |

The initial spill path performed a Pebble iterator/decryption for every handle
on every request, allocating about202.5KB per batch. Bounded positive-cache reuse
reduces warmed spill batches from about744.7 to24.4 microseconds and to about4.1KB
per batch. Final active-index batches average23.1 microseconds and4,096 bytes.
The spill benchmark includes its initial cache fill amortized over repetitions;
cold reads and working sets larger than the LRU remain more expensive.

Production does not yet select the spill-backed adapter and still loads the
full catalog. Next work is owned session/publication fencing, pending-pack
bookkeeping and activation with conservative handling of unavailable sizing.
There was no production backup run, daemon restart, deployment or wire change;
cwalk settings are unchanged.

## Pack-scoped sizing and pending-export recovery (2026-09-25)

`ReadSession.ScanPackInventory` streams the pinned `p:` catalog in bounded pages
and supplies pending exports to a callback instead of retaining all pack records.
For active, classified pure packs with known physical sizes and header sizes
consistent with the existing pack format, it accumulates exact growth-sizing
totals. Mixed packs remain excluded, matching existing sizing. Unknown sizes or
types, uncertain lifecycle states and inconsistent header sizes make the totals
unavailable, not guessed. Overflow, malformed records, callback failures and
cancellation return errors without partial totals. Pending callbacks can already
have run when a later error occurs; callers must not treat them as an atomic
publication operation.

The existing production catalog loader now uses those totals when complete,
skipping its additional global pack-summary map and merge. It still loads all
blob locations for the compatibility projection and retains builder-local state.
Incomplete pack metadata keeps the established blob-derived sizing fallback.
This does not claim that imported repositories have complete physical sizes or
that production startup is now independent of total catalog size.

`ReadSession.ReadPendingPack` reads one authenticated pack header through the new
repository `LoadPackHeader` method, then validates its locations against the pinned
authoritative catalog in batches of at most256, further limited by the negotiated
daemon batch cap. It checks pending lifecycle, count, exact location fields,
payload sum and pack classification, and validates the read session before
returning. Missing packs/blobs, mismatched headers, source errors and cancellation
return no usable recovery entries. There is no full `b:` scan. Recovery retains
one pack header plus one response batch, not an entire catalog; a large individual
pack/header or blob record can still be large. The caller remains responsible for
writing compatibility indexes and marking exports complete only after success.

A real-daemon regression writes513 distinct data blobs and two duplicate tree
entries, deliberately stopping before compatibility export. The new inventory
reads two pack records; header recovery verifies all515 locations using bounded
point batches. Totals are exactly50,200 data bytes and228 tree bytes, matching the
original loaded index. The normal loader's complete-metadata path returns the same
totals. After normal export flush, a fresh session reports no pending exports and
rejects recovery of an already published pack. Tests also reject altered header
offsets, header-read errors, absent packs and cancellation. Existing24,004-location
catalog tests still pass through the incomplete-metadata fallback.

These are correctness and work-scope observations on an in-memory native-daemon
fixture, not storage-throughput or whole-backup runtime measurements. No production
backup run was performed for this stage. Initial test failures were fixture-only:
the additional native inventory scan changed cumulative stream counters, and the
synthetic stream needed initialized KV/generation validation RPCs. Corrected
fixtures pass the same focused race tests without weakening session validation.

Repeated focused race tests cover both sizing paths and recovery. Full archiver,
backupcmd, engine and repository-index race suites pass with child-only permission
capability drops. Editor diagnostics and patch whitespace checks are clean. The
inventory retains the loader's existing10,000-record page setting, capped by the
daemon's negotiated limit.

Activation of scan-free backup still requires bounded write retention and owned
session/publication fencing, plus conservative handling of unavailable sizing.
Pending recovery is available and tested but not yet selected by the default
loader. No daemon restart, deployment or wire-format change occurred; cwalk
settings remain unchanged.

## Authoritative blob-location point reads (2026-09-25)

Pinned read sessions now expose single-blob location reads through the existing
Get RPC. The provider checks caller and session cancellation before and after
the RPC and decoding, cancels blocked Get requests when the session fails, and
preserves the session failure cause. Missing keys and wrong blob types return
no locations; provider and decoding errors remain errors. All matching stored
copies are returned in record order, including compressed and uncompressed
locations. Size and location reads share checks for ciphertext overhead,
conflicting plaintext sizes and offsets beyond the existing32-bit pack index
range. Schema decoding continues to reject malformed records and invalid types.

`CachedBlobLookup` now implements the complete context-read capability, including
single-size forwarding. Location requests share the size-batch RPC semaphore and
shutdown tracking, but location arrays are deliberately not retained in the
fixed-entry size LRU. Local published locations take precedence, with another
local check after RPC completion. A caller cancellation leaves the session usable;
a provider error poisons the lookup object so a later local/cache hit cannot hide
the metadata failure. Close cancels and joins active location requests as well as
size batches. Results still scale with the number of copies of the requested blob
within the RPC message limit; this is not a constant-memory location response.

Native integration tests load encrypted data and tree blobs through the repository
with an empty local index and no catalog load, including two locations per type
and the same content ID used for both types. Closing the session makes subsequent
loads fail with the metadata-error classification. Additional native checks verify
that locations published after pinning remain absent from the session but visible
through the write overlay after compatibility export flush. Decoder, blocked-Get
session failure, adapter cancellation/shutdown, native visibility and encrypted
load tests pass ten race-enabled repetitions.

Full archiver, backupcmd, engine and repository-index race suites pass with
child-only permission-capability drops. Targeted repository admission/error
propagation/catalog-loading tests and all read-session lifecycle tests also pass.
Editor diagnostics and patch whitespace checks are clean.

The focused `BenchmarkDecodeBlobLocations` compares initial per-location heap
objects with an exactly sized contiguous location array plus pointer slice. Three
repetitions before and after, in nanoseconds per decode:

| Locations | Before (ns) | After (ns) | Allocations Before/After | Bytes Before/After |
| --- | --- | --- | --- | --- |
| 1 | 222.8 / 220.3 / 223.6 | 201.9 / 205.4 / 205.7 | 4 / 4 | 200 / 200 |
| 16 | 2136 / 2164 / 2168 | 1873 / 1871 / 1531 | 20 / 4 | 2656 / 2592 |
| 256 | 30797 / 28860 / 32391 | 19096 / 21169 / 23544 | 264 / 4 | 43360 / 41248 |

At256 locations, mean decoder time falls from30.7 to21.3 microseconds (about31%),
with98.5% fewer allocations. These are in-memory decoder measurements, not RPC
latency or whole-backup throughput. Ordinary one-location records still use four
allocations. The test does not establish production replica-count distribution.

Production backup selection is unchanged and still loads the full catalog. Point
reads are now usable through the existing repository error-aware boundary in tests,
but activation still requires bounded write retention, scan-free pending-export
recovery and exact startup sizing, plus session validation/publication fencing.
No production backup run, daemon restart, deployment or wire change occurred in
this stage; cwalk settings remain unchanged.

## Bounded pinned-session lookup cache and write visibility (2026-09-25)

`CachedBlobLookup` provides a bounded size-result cache over the authoritative
pinned-session batch provider. It uses the existing LRU dependency, with a fixed
192-byte accounting charge per entry to turn the supplied byte budget into an
entry cap. This is conservative accounting for fixed-size keys/results, not an
exact Go heap or RSS guarantee. Both positive and negative results are cached;
errors and malformed reply counts poison the cache rather than becoming misses.
Session health is checked on hits as well as misses, including write-index hits.

RPC concurrency is independently bounded. Each active batch contains at most256
distinct handles; overlapping requests share pending results. Cache hits bypass
RPC slots, and callers waiting for slots can cancel. Canceling one caller does
not cancel shared work needed by another caller: RPCs remain owned by the cache's
session context until completion, session failure or cache close. Close rejects
new work, cancels outstanding RPCs, joins workers and purges retained results.
Closing the pinned session remains the owner's responsibility after cache close.

The existing write index takes precedence over cached snapshot results, with a
second check after RPC completion. Admission remains atomic through the existing
pending-blob index. New writes stay visible after publication, compatibility
export flush and cache eviction even though the pinned snapshot cannot see them.
This reuses the existing index; it does NOT bound that index's retention of newly
written blobs. A bounded or spilled write overlay is still needed before claiming
bounded memory for an entire backup.

Focused tests pass ten race-enabled repetitions, including real-daemon snapshot
isolation, publication/flush/eviction,16-way single-blob admission, an in-flight
miss racing a write, independent cancellation, failure handling and shutdown.
With32 simultaneous256-handle callers and two RPC slots, the blocked-provider
test observes exactly two RPCs and512 pending handles; queued requests cannot
start after close. Full archiver, backupcmd, engine and repository-index race
suites pass with child-only permission-capability drops. Editor diagnostics are
clean.

`BenchmarkCachedBlobLookup` uses256 handles and a synthetic zero-latency provider,
not production storage. Its cold case purges the LRU before each operation, so
timing includes purge and refill. Initial cold runs measured
273,852/272,967/275,127 ns, about107.6KB and783 allocations per batch. Replacing
per-handle completion channels with shared per-RPC completion state measured
169,482/168,136/167,952 ns, about69.4KB and273 allocations: approximately38.5% less
time and65.1% fewer allocations for this synthetic cold path. Both versions issue
one provider batch per operation. Warm initial runs were
23,093/23,363/23,169 ns; final runs were23,536/23,792/24,017 ns, with4,096 bytes,
one allocation and zero provider batches per operation. No warm-path speedup is
claimed, and these figures are not whole-backup or RPC-latency measurements.

The cache is implemented and tested but not yet selected by the production
engine. Backup still loads the full catalog. Remaining activation work includes
blob-location point reads, bounded write retention, export recovery and exact
startup sizing without a full blob scan, plus session validation and publication
fencing. There was no new production backup run, daemon restart, deployment or
wire-protocol change in this stage. Existing cwalk backup settings are unchanged.

## Bounded blob-size batches and authoritative point reads (2026-09-25)

Unchanged-file content checks now use batches of at most256 blob handles through
an optional engine/repository capability. Append transactions forward the batch
contract. The daemon's loaded-index implementation checks pending blobs first
and acquires each index lock once per batch, preserving first-match precedence,
duplicate ordering and missing results. Callers without the capability retain
context-aware individual lookup fallback. Invalid types, oversized requests,
provider errors and malformed reply counts fail closed; missing blobs stop
further reuse batches. The archiver retains only one256-handle request at a time.

The focused `BenchmarkMasterIndexLookupSizes` compares the same256 IDs across
four projections. Three repetitions measured individual lookup batches at
46,784/45,470/47,264 ns and batched lookups at30,712/30,593/31,374 ns. Mean lookup
time falls from46.5 to30.9 microseconds, about34%, with one4,096-byte allocation
per batch instead of zero allocations. This is a microbenchmark of content
lookup, not a measured whole-backup runtime improvement.

A pinned ReadSession now also provides the batch contract using the existing
VaulticDB MultiGet RPC. It coalesces duplicate blob IDs, splits requests at the
negotiated item limit, decodes each unique record once, and restores requested
order and blob types. Corrupt records, ciphertext shorter than encryption
overhead, and conflicting plaintext sizes are errors rather than misses.
Session failure cancels in-flight RPCs even with an independent caller context;
closed/failed sessions and canceled callers cannot return successful results.

Real-daemon tests verify that batches see the session's original snapshot and
not later publications, including a negotiated limit of two keys, duplicate IDs,
wrong blob types, absent keys and session closure. Additional tests cover pending
zero-sized blobs, compressed/uncompressed size parity, malformed replies,
513-ID partitioning into256/256/1, cancellation after a provider response, and
session failure during a blocked RPC. Full archiver, backupcmd, engine and index
race suites pass with child-only permission-capability drops; targeted repository
and repeated read-session/decoder race tests also pass. The blocked-RPC fixture
initially lacked KV subclient initialization; correcting the fixture passes the
same checks without a production change.

Backup still loads the full catalog and uses the loaded-index batch provider.
The authoritative session batch provider is implemented and tested, but is not
yet selected by the backup engine. Next work remains a bounded cache/new-write
overlay and a startup mode that avoids full enumeration while preserving export
recovery, sizing, session validation and publication fencing. No new production
benchmark, daemon restart, deployment or wire-protocol change is claimed here.

## Error-aware backup lookup boundary (2026-09-25)

After committing the R19 sizing improvement as `db519782c`, the next local
change adds optional context-aware engine lookup, size-lookup and deduplication
admission capabilities. Repository blob loads preserve provider failures instead
of converting them to absence or falling back to the compatibility projection.
Failed admission prevents encryption/upload even when duplicate storage is
requested. Existing synchronous engines retain their prior fallback behavior.
The daemon adapter implements the new contracts over its existing loaded index;
no additional RPCs or on-demand mode are enabled by this checkpoint.

Append transactions forward error-aware size lookups into the archiver. Parent
tree loads and unchanged-file content checks propagate metadata-service errors,
and the archiver does not allow ordinary source-file error callbacks to suppress
them. Parent metadata failures are returned before uploader startup. Ordinary
missing/unreadable parent handling remains compatible. Snapshot tests explicitly
verify that parent-load and content-lookup failures prevent publication even
when an error callback attempts to ignore them.

Regression checks cover provider errors, append-wrapper forwarding, cancellation,
forced-duplicate admission failure, snapshot publication gates, and concurrent
admission of one blob by16 callers. Full archiver, backupcmd and engine race suites
pass, as do targeted repository load/save, authoritative-catalog and engine
resolution tests. Root initially bypassed the unreadable-file fixture permissions;
running the full suites with child-process-only
`setpriv --bounding-set=-dac_override,-dac_read_search` makes these permission
fixtures effective and passes without skips. No production permissions changed.

This is a correctness/API prerequisite, not a runtime benchmark or a claim that
catalog memory is now bounded. The next step remains bounded batched point reads,
a byte-bounded cache and in-flight/new-write overlay, session/fencing validation,
and a backup startup path that no longer enumerates the full catalog. The RPC
Get/MultiGet layer already returns errors; no wire-protocol change or daemon
deployment was needed for this checkpoint.

## Catalog-size summaries remove the second blob walk, r19 (2026-09-25)

The authoritative backup loader reads the full `b:` catalog from VaulticDB and
builds an in-memory compatibility index; it does not obtain this projection by
scanning legacy pack files. Both startup work and index memory remain linear in
the full blob catalog. R18 then walked that projection again for pack sizing.

Catalog builders now accumulate payload and entry-header bytes using their
existing pack ordinals. Their summaries are merged across workers, adding each
pack's fixed header once and excluding mixed packs. Validated fresh loads expose
constant-time size totals to uploader startup through an optional engine
capability. Canceled/failed loads do not publish totals; pack publication
invalidates them, and legacy or unavailable totals retain the exact scan fallback.
Temporary summaries scale with packs, not an additional copy of all blobs.

R19 used the unchanged 52 sources, explicit cwalk32, two file workers and
HDD-backed NFS scratch. First archival status moved from316 seconds in R18 to216
seconds, and the CPU profile contains no samples in `currentBlobSizes`, compared
with118.39 CPU-seconds in R18. The last status at607 seconds reported59,824 files
and136,114,669,242 logical bytes. Exit124 at611.110 seconds was clean, without a
forced kill. Peak RSS was30,367,292KiB versus33,604,216KiB in R18; full-run CLI CPU
was1,485.12 seconds, with more time spent doing actual archival work.

All143 snapshot IDs remained unchanged. There were10,982 engine writes and4,685
commits, no remaining transactions/write intents, no sampler errors, and no
daemon restart or epoch change. No new snapshot completed. Earlier writes,
filesystem cache warmth and different reached data affect this sequential
comparison: the approximately doubled processed bytes are not an isolated 2x
throughput improvement. Removing the serial sizing phase is directly supported
by the code-path regression and profile evidence.

Focused race tests cover builder accounting, four-way catalog loading of pure
and mixed packs, pending export recovery, canceled-load behavior, post-write
invalidation, exact scan fallback, and cached sizing that performs no iteration.
Artifact directory:
`/volume2/NASDA2/rustic/db.test/backup-catalog-summaries-2026-09-25-r19/`.

The next scalability change is an on-demand backup lookup path, not merely more
catalog-loading workers. The existing Get/MultiGet RPC clients already return
errors and distinguish missing keys; the synchronous Go ReadEngine Lookup and
LookupSize methods, and WriteEngine AddPending, cannot propagate those failures.
Add context-aware, error-returning lookup/admission capabilities first, then use
bounded MultiGet batches, a byte-bounded cache and an in-flight/new-write overlay.
Retain a consistent read session and validate generation/fencing through
publication; never interpret timeout, corruption or session loss as a missing
blob. Avoid retaining arbitrarily many chunk buffers while waiting for batches.
Keep full enumeration for commands that need it, and separately replace backup
startup sizing/export recovery dependencies on that enumeration. On-demand
lookups are not implemented or enabled by R19.

## Incremental cwalk and cancellable pack sizing, r17/r18 (2026-09-25)

The backup command now consumes cwalk directory listings on demand, with bounded
child-directory lookahead, instead of waiting for a complete source manifest.
Demand and speculative reads share the worker limit, retain filesystem stats,
and are joined during cancellation and before snapshot publication. Marker
decisions spill from bounded per-rule maps to a shared temporary Pebble store;
spilled decisions remain sticky, and storage failures prevent publication.
The eager manifest API remains available for existing explicit callers.

R17 exposed a separate startup barrier before the incremental reader was used:
uploader sizing scanned the entire blob catalog four times and used an
uncancellable background context. The CLI stayed near one core after catalog
loading, while the daemon used roughly 0.003 cores. TERM at ten minutes did not
stop sizing, and the 45-second grace expired with exit137. No source archival,
engine writes, commits, or new snapshots occurred in this attempt.

Sizing now computes both blob-type totals in one cancellable pass, preserving
pack-header overhead and exclusion of mixed-type packs. Configurations with no
pack growth skip the scan. Uploader workers are initialized only after sizing
succeeds, avoiding partially started uploaders on cancellation.

R18 used the same 52 saved sources, explicit `--use-cwalk`, 32 cwalk workers,
two file workers, NFSv3 sources and HDD-backed NFS scratch. Its first archival
status arrived at316 seconds. At602 seconds it reported38,017 files and
66,904,728,427 logical bytes processed, approximately223 MiB/s between the first
and last status records. These are processed bytes, not newly uploaded bytes.
It exited124 cleanly at605.384 seconds without a forced kill. CLI CPU was
1,254.07 seconds and peak RSS33,604,216KiB. All143 snapshot IDs remained unchanged;
8,059 engine writes and2,469 commits reflect actual archival/reconciliation work.
No active transactions or write intents remained, the daemon stayed at epoch55,
and no sampler errors occurred. No new snapshot completed.

The CPU profile contains1,228.68 sampled CPU-seconds: blob catalog loading
651.15s/53.00%, remaining serial sizing118.39s/9.64%, SHA-256
197.96s/16.11%, and chunking106.10s/8.64%. Focusing on archival call paths and
excluding sizing gives386.81 CPU-seconds: hashing51.18%, chunking27.43%, and
filesystem syscalls15.25%. At550 seconds traversal waited for two file workers,
one chunking and one reading; all32 directory-stream workers were idle.

On the main active source mount, NFS READ round-trip time averaged0.985ms and
GETATTR0.276ms, with no retransmissions or major timeouts. Mount counters are
shared, not backup-exclusive. Four saved sources had no matching NFS mount in
the captured namespace: `/ncl1-1-vs-50/IT_download` and
`/ncl1-1-vs-70/vf_dmz_ftp2`, `vf_dmz_svn`, `vf_dmz_www`. Their source scope was
not changed and no RPC attribution is invented for them. Metadata commits
averaged94.36ms, including92.35ms waiting for durability. The228.01 seconds of
aggregate durability wait overlap archival work and must not be added to runtime.

Focused race checks pass for crawl and backupcmd, archiver cwalk/marker behavior,
and pack sizing/cancellation. Coverage includes demand-read shutdown, selection
parity, sticky spill decisions, storage failure, and snapshot-publication gating.
The known root-only `TestScannerCWalkSuppressedErrorFallsBack` fixture is excluded.
This is partial-run evidence, not completed-backup or durability acceptance.
Remaining candidates are pack summaries reused during catalog loading, a matched
two-versus-four file-worker experiment, and amortized reconciliation revision
allocation/publication without relaxing durability. Source saturation is not
established by these measurements.

Artifacts, including source patches, binary hashes, CPU profiles where available,
stacks, NFS counters and repository safety records:
`/volume2/NASDA2/rustic/db.test/backup-cwalk-incremental-2026-09-25-r17/` and
`/volume2/NASDA2/rustic/db.test/backup-cwalk-incremental-2026-09-25-r18/`.
No daemon restart, deployment or push was performed.

## Directory-stat reuse not retained, r16 (2026-09-25)

After committing marker prefetch as `1390dea6f`, a second candidate reused
cwalk's directory `os.FileInfo` via the existing `ExtendedStat` converter. An
explicit filesystem capability allowed plain local filesystems and transparent
telemetry wrappers to opt in; mapped and custom filesystems kept normal Lstat.
Tests verified full extended-metadata equality, wrapper fallback and telemetry
request accounting, plus existing cwalk selection/cancellation race checks.
Actual archiving still read fresh metadata through the normal path.

The unchanged cwalk32/four-root/52-source/HDD-NFS r16 reaches2,843,351 directories
and14,116,305 entries at345 seconds of manifest time, completing3 roots. Against
the preceding marker-prefetch r15, that is only0.7% more directories and0.8%
fewer entries, smaller than candidate repetition variation. The added capability
and cross-module code are not justified by this result and are removed. Marker
prefetch remains unchanged. The exact experimental patch/binary remain in the
artifact directory for reproducibility.

R16 exits124 cleanly after603.518 seconds. Its389.366-second manifest phase
reads3,195,068 directories and lists15,994,320 entries, completing3/52 roots;
no manifest or backup completes. CLI CPU is1,786.04 seconds, peak RSS31,967,128KiB.
Daemon CPU is730.61 seconds over604.264 seconds; metadata records41,998 GETs,
132.228141 aggregate service seconds and42,912,913,929 logical body bytes.
All143 snapshot IDs are unchanged, engine writes/commits are zero, no active
transactions/intents remain, scratch is empty, and there are no sampler errors
or forced kills. Focused cwalk/marker race checks pass after removing the
experiment, excluding the known root-only scanner permission fixture.

Artifacts:
`/volume2/NASDA2/rustic/db.test/backup-cwalk-stat-reuse-2026-09-25-r16/`.
Remaining ideas are not exhausted: avoiding unnecessary cwalk file metadata
reads and replacing the whole-manifest startup barrier need separate correctness
work. This run does not establish completed-backup runtime or justify a longer
runtime authorization.

## Concurrent marker prefetch, r14/r15 (2026-09-25)

R13 identified marker-file `Lstat` under the archiver selection lock. The marker
cache also held its own mutex during filesystem access. Independent cache misses
now execute outside the map lock, with `singleflight` coalescing checks for the
same directory and a per-cache warning lock. The backup command shares its marker
predicates with an optional cwalk prefetch callback, invoked before serialized
metadata selection. Normal selection still decides inclusion; arbitrary name,
mandatory and metadata callbacks are not made concurrently callable. Marker
presence/signature semantics, cache lifetime and marker-file retention remain
unchanged. Prefetch can inspect directories later rejected by another rule.

Tests prove independent reads overlap, same-directory concurrent requests issue
one read, cwalk marker I/O overlaps while selection callbacks remain serialized,
and sequential/cwalk snapshots have matching exclusions including retained
markers. Command tests verify shared caches for custom and signed cache markers.
Repeated focused archiver race tests and the full backupcmd race suite pass;
the known root-only scanner permission fixture remains excluded. No dependency
version change is needed: singleflight uses the existing x/sync module.

Both ten-minute runs use explicit cwalk32/four lanes, the unchanged52 roots and
HDD-NFS scratch. R15 reuses R14's exact binary and source attribution. At the same
345-second manifest boundary:

| Run | Directories read | Entries listed | Roots completed |
| --- | ---: | ---: | ---: |
| r11 baseline | 2,309,997 | 13,191,901 | 3 |
| r13 baseline | 2,259,311 | 12,427,735 | 3 |
| r14 prefetch | 2,655,776 | 15,215,548 | 3 |
| r15 prefetch | 2,823,315 | 14,224,809 | 3 |

Candidate means are19.9% higher for directories and14.9% higher for entries.
These are discovery-throughput comparisons, not completed-backup runtime ratios:
roots execute in nondeterministic order and have different contents. Both550s
stacks have no selection-mutex wait stacks; R14 shows concurrent marker reads
through prefetch alongside directory reads and cwalk child stats.

R14 exits124 cleanly at604.470s, with348.142s of manifest work,2,678,322
directories and15,359,527 entries. R15 exits124 at603.477s, with378.160s of
manifest work,3,111,905 directories and15,787,140 entries. Both complete3/52
roots and report `finished:true,complete:false`. Catalog loading is slower than
r11, leaving less traversal time inside the cap; phase-aligned comparison avoids
crediting this variation to discovery. Neither run completes a manifest or backup.

| Metric | r14 | r15 |
| --- | ---: | ---: |
| CLI CPU seconds | 1,686.47 | 1,755.47 |
| CLI peak RSS KiB | 31,680,196 | 32,223,972 |
| Daemon observation seconds | 605.190 | 604.207 |
| Daemon CPU seconds | 735.58 | 733.24 |
| Metadata GETs | 42,069 | 42,024 |
| Aggregate GET-service seconds | 303.573299 | 182.406810 |
| Logical GET body bytes | 43,105,259,057 | 42,974,079,657 |

All143 snapshot IDs stay unchanged; engine writes/commits and final active
transactions/write intents are zero. Scratch is empty, no forced kills or
sampler errors occur, and the original daemon remains PID431717/epoch55. No
deployment or push occurs. The change is retained for repeated bounded discovery
improvement, not claimed as end-to-end acceptance. Remaining work is dominated
by source directory reads/child stats, with the eager full-manifest barrier still
preventing archiving. Further investigation should address that barrier or
duplicate metadata operations without weakening source metadata semantics.

Artifacts: `/volume2/NASDA2/rustic/db.test/backup-cwalk-markers-2026-09-25-r14/`
and `/volume2/NASDA2/rustic/db.test/backup-cwalk-markers-2026-09-25-r15/`.

## Authorized 60-minute cwalk validation, r13 (2026-09-25)

The user explicitly approved one 60-minute validation of the retained four-root
implementation before further architectural changes. R13 tests clean revision
`937c9ec79bc53a2601d1b135b545ad2965256594`, using upstream cwalk v1.0.1,
the unchanged 52 roots, explicit `--use-cwalk --cwalk-concurrency=32`, and
HDD-backed NFS manifest scratch. Only the harness timeout and stack-capture
schedule change. No builds or tests overlap the measurement.

The run does not complete a manifest or backup. It exits124 after 3,604.868
seconds, with graceful cleanup about five seconds after TERM and no forced kill.
Manifest preparation lasts 3,403.678 seconds, reads 19,644,226 directories and
lists 126,290,996 entries, completing only 5/52 roots. Final progress is
`finished:true,complete:false`; stdout contains only `cwalk_status` records,
not archival status or a snapshot summary. Average discovery rates are about
5,771 directories/s and 37,104 entries/s. Root-count milestones first appear
at manifest seconds5 (2 roots),345 (3),1080 (4), and1800 (5). Unequal root sizes
make root counts unsuitable for extrapolating a completion time.

CLI CPU totals 8,748.17 seconds and peak RSS is 33,931,504KiB (about32.36GiB).
The daemon uses 757.69 CPU seconds over a 3,605.614-second observation window;
metadata records 51,733 GETs, 68.564315 aggregate GET-service seconds and
42,928,699,783 logical body bytes. The catalog phase finishes near200 seconds,
so the remaining time is manifest work rather than catalog loading. All143
snapshot IDs remain unchanged, metadata engine writes/commits remain zero,
and active transactions/write intents are zero afterward. The original daemon
remains healthy at PID431717/epoch55. Scratch is empty, the backup process has
exited, and no sampler errors occurred. No daemon restart, deployment or push
was performed.

The CPU profile contains 8,634.73 sampled CPU seconds, of which 5,001.06 seconds
(57.92%) are in `internal/runtime/syscall/linux.Syscall6`. At3550 seconds,
25 cwalk workers wait on the selection mutex (14 before name selection and11
before metadata selection). The lock holder is inside `RejectIfPresent` /
`isDirExcludedByFile`, performing a marker-file `Lstat`; other workers are in
directory reads or cwalk child stats. This is evidence of serialized filesystem
selection work, not proof that all elapsed time is mutex waiting. The manifest
writer remains active and discovery counters continue increasing until TERM.

The hour validates sustained traversal and clean cancellation but still cannot
establish completed-backup runtime or durability of a new snapshot. The next
focused optimization target is directory-marker checks under the shared
selection lock, preserving callback safety and exclusion semantics. An
incremental manifest consumer remains a broader alternative to the eager
whole-manifest barrier. Routine experiments retain the ten-minute cap; this
authorization does not extend future runs automatically.

Artifacts:
`/volume2/NASDA2/rustic/db.test/backup-cwalk-roots4-60m-2026-09-25-r13/`.
The directory contains the exact command and roots, binary fingerprints, clean
source revision/patch, before/after snapshot and writer records, five-second
samples, CPU profile, ten stack captures, and analyzer output.

## Eight-root overlap rejected, r12 (2026-09-25)

The next candidate doubled root lanes to eight while retaining32 total cwalk
workers. Repeated crawl and focused archiver race checks passed. The unchanged
52-root/HDD-NFS run exits124 cleanly at603.551 seconds, after403.427 seconds of
manifest work:2,405,394 directories,15,518,694 entries and2/52 completed roots.
Relative to four-lane r11, directory counts fall8.9%, entry counts rise4.4%,
and completed roots fall from3 to2. Root mixes differ, so these are not equal-work
runtime ratios. With no compelling improvement, the eight-lane source changes
are removed and four lanes retained.

CLI CPU is1,656.74 seconds and peak RSS30,290,100KiB. Daemon CPU is736.09 seconds
over604.286 seconds, with42,064 GETs,65.658390 aggregate service seconds and
43,087,562,419 logical body bytes. All143 snapshots remain identical and metadata
writes/commits remain zero; no sampler errors or forced kill occurred.
Artifacts: `/volume2/NASDA2/rustic/db.test/backup-cwalk-roots8-2026-09-25-r12/`.

Manifest construction still prevents any new backup from completing within the
ten-minute experiment cap. Further ideas remain, especially an incremental
directory-manifest consumer and reducing serialized directory-marker reads.
They require separate lifecycle/selection correctness work. A longer explicitly
authorized run is needed to validate completion and end-to-end runtime before
treating the discovery gains as production backup acceleration.

## Bounded four-root manifest overlap, r11 (2026-09-25)

Manifest construction now admits up to four roots concurrently, dynamically
taking the next root when a lane finishes. The total cwalk worker budget is
divided across lanes, including any remainder; the bounded writer queue stays
shared. The first root failure cancels active walkers and all lane goroutines
are joined before writer shutdown. Tests gate callbacks to prove overlap,
check every completed root's contents, retain queued-root cancellation coverage,
and check original failure propagation/temporary-state cleanup. Repeated crawl
race tests and focused archiver/backupcmd race tests pass.

Explicit cwalk32/52-root/HDD-NFS r11 exits124 cleanly at603.534 seconds.
The404.884-second manifest phase reads2,639,668 directories and lists14,863,532
entries, completing3/52 roots versus r10's2/52. Compared with r10's403.480-second
phase, raw directory work is2.50x and entry work2.97x. These are discovery counts
across differently scheduled roots, not an equal-work or completed-backup speedup.
CLI CPU rises from1,028.31 to1,637.60 seconds; peak RSS rises from29,833,424 to
31,071,540KiB. Daemon CPU is737.22 seconds over604.279 seconds, with42,045 GETs,
66.448638 aggregate service seconds and43,059,069,630 logical body bytes.
All143 snapshot IDs remain identical and metadata writes/commits remain zero.
There are no sampler errors or forced kills; the full manifest still blocks
archiving when the ten-minute cap expires.

Artifacts: `/volume2/NASDA2/rustic/db.test/backup-cwalk-roots4-2026-09-25-r11/`.
Root overlap improves bounded discovery throughput provisionally. The next
bounded comparison will test eight root lanes under the same32-worker budget;
completion-level acceptance still requires a longer authorized run or removal
of the eager whole-manifest startup barrier.

## Manifest progress observability, r10 (2026-09-25)

An optional serialized manifest observer now reports roots completed, successful
directory reads, entries listed and phase duration at start, every five seconds,
and after traversal/writer shutdown. The reporter is joined before returning;
the final `finished` flag does not imply `complete`. Backup JSON emits distinct
`cwalk_status` records, never counting these entries as archived files. Tests
cover exact successful counts and canceled/incomplete termination; crawl,
backupcmd and focused archiver cwalk race tests pass (excluding the documented
root-only permission fixture).

The unchanged explicit cwalk32/52-root/HDD-NFS r10 attempt exits124 cleanly after
603.03 seconds. Manifest preparation runs403.480 seconds, reading1,055,833
directories and listing4,998,943 entries while completing only2/52 roots.
Final status is `finished:true,complete:false`. The catalog phase ends near
200 seconds. CLI CPU is1,028.31 seconds and peak RSS29,833,424KiB; daemon CPU
is740.60 seconds over603.808 seconds. Metadata records42,012 GETs,
65.494816 aggregate service seconds and42,973,515,907 logical body bytes.
All143 snapshot IDs are unchanged; metadata writes/commits remain zero.
No sampler errors or forced kill occurred, and no backup completed.

Artifacts: `/volume2/NASDA2/rustic/db.test/backup-cwalk-progress-2026-09-25-r10/`.
The baseline now exposes completed discovery work. The next candidate is bounded
root overlap under the existing total cwalk worker budget; comparisons must
acknowledge that different roots have different entry/directory ratios and that
only a completed manifest or backup proves end-to-end improvement.

## Directory-only manifest selection, r9 (2026-09-25)

The cwalk manifest stores directory entry names before its ignore callback and
the archiver applies file selection again when saving those names. File and
symlink selection during manifest construction therefore cannot prune traversal
and needlessly repeats metadata reads and marker-file checks. The prepass now
evaluates selection only for directories. Directory pruning, mandatory selection
during archiving, all file names, and actual snapshot filtering remain intact.
Tests assert that prepass callbacks see only directories, metadata reads still
overlap, and sequential/cwalk snapshots contain identical allowed files under
name and mandatory exclusions. Focused cwalk race checks pass.

R9 uses the unchanged explicit cwalk32/52-root/HDD-NFS configuration. Catalog
reads plateau near 205 seconds, but manifest preparation remains incomplete at
the cap. Exit124 occurs at603.021 seconds without forced kill; CLI CPU is
1,005.26 seconds and peak RSS29,740,536KiB. The603.775-second daemon window
records732.64 CPU seconds,42,038 GETs,65.352917 aggregate GET-service seconds
and43,037,972,402 logical body bytes. All143 snapshot IDs remain unchanged;
engine writes/commits and sampler errors are zero. The late stack is now in
directory reads and directory-marker Lstat checks. CPU is lower than r8's
1,072.40 seconds, but completed manifest work is not counted, so this is not
an equal-work throughput claim. Evidence is retained under
`db.test/backup-cwalk-dirselect-2026-09-25-r9`.

The next measurement requirement is manifest progress (directories, entries,
completed roots and phase duration), followed by matched comparisons. Remaining
design candidates include bounded overlap across independent roots and an
incremental manifest pipeline. The latter must preserve callback synchronization,
error/fallback semantics, cancellation cleanup and snapshot publication ordering;
it is not equivalent to simply running the existing prepass in a goroutine.

## Recursive component filter reduction, r7/r8 (2026-09-25)

The r6 profile exposed recursive wildcard expansion in exclusions. Patterns
`**/component` and `**/component/**` now reduce to the existing component-search
matcher, retaining original pattern text, negation, malformed-component errors,
and the old minimum path length for the trailing recursive form. The first
literal-only candidate caught a one-component relative-path edge case in tests;
the final candidate preserves it and supports glob components as well.
An explicit wildcard-expansion oracle covers literals, wildcards, classes,
escapes, malformed patterns, roots and short paths. The full filter race suite
and affected crawl/backup tests pass.

R7 (literals only) still spent 190.45 sampled CPU seconds in filter.match,
versus r6's 251.52; the remaining recursive Trash wildcard was active in stacks.
R8 includes glob components. Both runs use the same cwalk32/52-root/HDD-NFS
scope and ten-minute cap, reach manifest preparation after catalog reads plateau
around 205 seconds, and cancel cleanly without publishing a snapshot.

| Metric | r7 | r8 |
| --- | ---: | ---: |
| Wall seconds | 603.423 | 603.015 |
| CLI CPU seconds | 1,220.20 | 1,072.40 |
| Peak CLI RSS, KiB | 30,445,032 | 29,752,232 |
| Daemon CPU seconds | 734.47 | 733.53 |
| Main GETs | 42,044 | 42,064 |
| Logical body bytes | 43,074,542,380 | 43,107,706,747 |

These are bounded runs, not equal completed-work measurements: manifest record
progress is not yet exposed, so lower CPU is not a proven throughput ratio.
All 143 snapshot IDs remain unchanged; metadata writes/commits and sampler errors
are zero. R8's late stack moves to metadata exclusion checks, including marker
file Lstat inside serialized selection. Evidence is retained under
`db.test/backup-cwalk-filter-2026-09-25-r7` and
`db.test/backup-cwalk-component-2026-09-25-r8`.

## Cwalk metadata read concurrency, r6 (2026-09-25)

Manifest selection previously held one mutex across name selection, a second
filesystem Lstat, and metadata selection. The lock now protects only selection
callbacks; cwalk's bounded workers can overlap the filesystem reads. A gated
regression proves two reads overlap while all selection callbacks remain
serialized. Cwalk parity and cancellation race checks pass against upstream
v1.0.1, with the known root-permission fallback test excluded.

R6 retains 52 roots, 32 cwalk workers, HDD-NFS scratch and the ten-minute cap.
It reaches the manifest phase after catalog reads plateau around 205 seconds,
then exits on TERM at 603.151 seconds without a forced kill. All 143 snapshot
IDs remain unchanged; engine writes/commits are zero. CLI CPU is 1,164.74 seconds
and peak RSS 33,063,352 KiB. The 603.956-second daemon window records 743.11 CPU
seconds, 41,981 GETs, 66.690693 aggregate GET-service seconds and
42,909,993,282 logical body bytes. No sampler errors occurred.

Catalog startup varies substantially from r5, before the changed code executes,
so this does not establish a production runtime win. The manifest still does
not complete. Its next measured CPU cost is exclusion matching: filter.match
accounts cumulatively for 251.52 of 1,159.53 sampled CPU seconds; later stacks
also show selection-lock contention. Optimize matching semantics-preservingly
before considering callback concurrency. Evidence is retained under
`db.test/backup-cwalk-metadata-2026-09-25-r6`.

## Cwalk cancellation and archiver handoff, r4/r5 (2026-09-24)

After catalog parallelism reached source traversal, cancellation required fixes
at two boundaries. Cwalk's worker loops and startup waits did not observe Stop;
that patch and its deterministic regression are now upstream in cwalk v1.0.1
(a5cd220), adopted by Vaultic in aa4bf5c7b. The Vaultic wrapper checks cancellation
before each root and after walker registration, stops callbacks on cancellation,
and joins its cancellation monitor. The archiver also checks cancellation before
snapshot preparation and after manifest preparation, rather than initializing
upload work after a canceled manifest has been discarded.

R4 still required the kill grace despite the cwalk worker fixes. R5 adds the
archiver handoff checks and exits at 602.951 seconds with timeout status 124,
about three seconds after TERM, instead of requiring SIGKILL at 645 seconds.
Its final error is context cancellation, and no 610-second post-TERM stack is
present because the process had already exited. Both runs explicitly use cwalk
with 32 workers, the original 52 roots, and HDD-NFS manifest scratch; r3 had
inherited the default temporary directory and is not a controlled scratch
performance comparison. In-flight filesystem calls remain non-interruptible.

R5 records 766.44 user and 93.46 system CPU seconds, peak RSS 32,555,540 KiB,
and no completed backup. Artifacts, source patch and the then-local cwalk source
are retained under `db.test/backup-cwalk-cancel-2026-09-24-r4` and `-r5`.
Cancellation/temporary-state cleanup, traversal parity, and the regression that
forbids uploader startup after cancellation pass under the race detector against
upstream v1.0.1. The known root-only permission fallback test remains excluded.
This is a shutdown improvement, not a completed-backup throughput result.

## Four catalog workers reach cwalk, r3 (2026-09-24)

The third candidate uses four private compact builders, each scanning disjoint
blob-key partitions under the same pinned read session. Worker failure cancels
and joins the group; all projections remain private until completion and final
validation. Per-chunk counters report processed records, locations and completed
partitions on interrupted startup. Native multi-page lookup/cancellation tests
and affected-package race tests passed, including concurrent counter updates.

Production r3 used the same explicit cwalk, 52-root scope, ten-minute cap and
unchanged primary. Catalog reads plateaued near 205s; the 180s stack is still
in catalog loading, while both 360s and 550s stacks prove the command reached
`Archiver.prepareCWalkManifest -> BuildDirectoryManifest -> cwalk.Walker.Run`.
Thus startup now reaches source traversal within the feedback window, unlike
both single-consumer candidates and the original hour-long unary attempt.
This is not a completed-backup runtime ratio: no snapshot was published.

The newly reached cwalk manifest pass failed to respond to TERM within the
45-second grace. Timeout killed the process group at 645.007s (exit 137).
GNU time then reported wrapper-only CPU/RSS and the CPU profile was not cleanly
finalized; those totals must not be treated as backup resources. The saved
analyzer instead reports the 640.621s sampled CLI window: 1,044.78 CPU seconds
and 30,146,520 KiB sampled peak RSS. Daemon status spans 645.738s with 733.20
CPU seconds, 42,226 GETs, 96.971324 aggregate service seconds and
43,176,477,462 logical body bytes. All 143 snapshot IDs remain unchanged,
engine writes/commits are zero, and the original primary remains healthy at
epoch 55 with no active transactions/intents or remaining backup process.

The cwalk stacks show directory reads in the external walker and the caller
waiting in `Walker.Run`; authoritative reconciliation workers are still idle.
The next required resilience work is cancellation and bounded manifest
preparation while retaining cwalk. Artifacts and corrected sampled-resource
analysis are under `db.test/backup-parallel4-2026-09-24-r3`.

## Direct compact backup projection, r2 (2026-09-24)

The second candidate removes the full `map[PackID][]Blob` staging copy. A
single-owner catalog builder writes directly into the existing compact index,
reuses one ordinal per pack, validates types and 32-bit offset/length bounds,
and releases its pack map when finished. Normal and pending-export projections
remain separate and private until the entire read session validates and closes;
cancellation still installs neither. The native multi-page parity/cancellation
test passed, as did full compact-index, index-engine and backup-package race
tests. Builder tests cover ordinal reuse, duplicate locations, invalid-input
non-mutation and finish semantics.

A 65,536-location synthetic benchmark, three repetitions, measured the old
map-then-copy path at 14.48-14.82ms and 17.47MB/2,082 allocations, versus direct
construction at 9.40-9.56ms and 9.07MB/673 allocations. This is a local allocation
and construction benchmark, not a backup runtime measurement.

Production r2 used the same ten-minute cwalk-enabled 52-root scope, unchanged
primary and settings, from `22d7eab89` plus its recorded patch. It timed out at
602.401s in catalog loading, with unchanged 143 snapshot IDs and zero writes or
commits. CLI CPU was 570.81s and peak RSS 26,634,612 KiB, versus r1's 573.64s
and 38,989,088 KiB. R2 consumed 36,147,888,989 logical body bytes versus r1's
38,922,261,970: memory is lower, but less catalog data was reached, so there is
no demonstrated production runtime win or exact equal-scope memory percentage.
The 603.174s daemon window recorded 562.19 CPU seconds, 36,060 GETs and
80.576054 aggregate GET-service seconds. No sampler errors or leaked sessions
occurred; the original primary stayed healthy at epoch 55.

The new CPU profile shows compact `indexMap.add` at 30.79% cumulative and its
preallocation at 11.89%, with map lookup and protobuf/schema decoding also
material. One scan/build consumer remains CPU-active at about one core.
The next bounded candidate is independent parallel scan/build workers sharing
the same pinned snapshot but not their mutable projections. Artifacts are in
`db.test/backup-compact-2026-09-24-r2`.

## Backup catalog streaming, r1 (2026-09-24)

The first backup optimization replaces unary pending-pack/blob catalog pages
with the existing renewed read-session streaming API. Both catalogs use one
pinned snapshot, final generation validation precedes projection installation,
and bounded cancellation-independent rollback closes the session before backup
writes. Streaming validation and legacy-daemon fallback remain in the shared
API. A native 12,002-location test covers mixed types, shared blob locations,
pending-export projection, cancellation without partial installation, retry,
and zero remaining transactions/intents. Focused index, read-session, repository
and backup/cwalk race tests passed; final native regression passed in 2.961s.

A fresh profile CLI from `4ab4f5a93` plus the recorded patch ran the unchanged
52-root scope with explicit `--use-cwalk --cwalk-concurrency=32`. The diagnostic
retained the ten-minute cap, 45-second grace, HDD-NFS primary and unchanged
daemon; no builds/tests overlapped. It timed out after 603.101s, still loading
the catalog. All four captured stacks remained in that phase. Both inventories
retain the same 143 snapshot IDs, with zero engine writes/commits, and the
primary remained PID 431717, epoch 55, healthy with no transactions/intents.

CLI CPU was 497.90 + 75.74s, peak RSS 38,989,088 KiB, and swaps zero.
The 603.918s daemon window recorded 605.14 CPU seconds, 38,706 main-store GETs,
101.002503 aggregate GET-service seconds and 38,922,261,970 logical body bytes.
The prior ten-minute unary attempt recorded 504,245 GETs and 531.009527 service
seconds, but neither completed and record progress was unavailable: these
figures demonstrate changed request behavior, not an end-to-end speedup ratio.
The streaming run is now approximately one CLI core busy; its CPU profile
attributes 64.44% cumulatively to the catalog consumer, including map growth,
copying and decoding. The intermediate per-pack blob map reached about 37 GiB
RSS before it could be copied into the compact lookup projection.

The next bounded candidate is direct construction of a private compact
projection, avoiding the full duplicate intermediate representation while
retaining all-or-nothing installation. Parallel scan consumption and explicit
record progress remain separate opportunities. Artifacts, source patch,
profiles, samples, analyzer, identities and checksums are under
`db.test/backup-stream-2026-09-24-r1`.

## Authorized 60-minute backup attempt (2026-09-24)

After committing the socket-routing fix as `d350e4983`, a fresh profile CLI
from that clean revision ran the same 52-root `cdot` backup against the existing
primary. The user explicitly extended this backup's limit to 60 minutes;
the 45-second TERM-to-KILL grace remained unchanged. No builds/tests overlapped
measurement, and no daemon binary, service, repository policy, or source scope
changed. Before/after binary checksums matched.

The command reached the 60-minute cap and exited 124 after 3,601.117 seconds.
It still had not reached file processing. All seven goroutine captures at
60, 600, 1,200, 1,800, 2,400, 3,000 and 3,500 seconds show
`loadBackupParent -> LoadIndex -> DaemonEngine.loadBlobCatalog -> ScanPrefix`;
the final cancellation error identifies the same loader. Backup progress JSON
remained empty. Both inventories contain the identical 143 snapshot IDs, with
zero additional engine writes or commit requests. No new backup snapshot or
normalized backup metadata was published.

| Measurement | Value |
| --- | ---: |
| CLI user / system CPU, seconds | 181.00 / 31.22 |
| CLI average CPU cores | 0.059 |
| CLI peak RSS, KiB | 12,574,956 |
| Daemon status window, seconds | 3,601.877 |
| Daemon CPU, seconds / average cores | 2,656.97 / 0.738 |
| Sampled daemon peak RSS, KiB | 694,228 |
| Main-store GET requests | 3,307,923 |
| Main-store GET aggregate service, seconds | 3,122.508038 |
| Main-store GET-body bytes | 113,780,822,474 |
| Engine backpressure / L0-stall event deltas | 0 / 0 |
| Process / writer samples | 720 / 720 |
| Sampler errors / swaps | 0 / 0 |

The CPU profile contains 206.30 sampled CPU seconds across 3,599.98 seconds;
83.16% of sampled CPU is cumulatively below `loadBlobCatalog`, including map
operations, memory copying and protobuf decoding. These are CPU shares, not
wall-time shares. Low CLI utilization and repeated RPC-wait stack captures
identify serialized catalog fetching as the immediate startup bottleneck;
materializing the full catalog is also a growing memory cost (about 12 GiB peak).

Approximate ten-minute windows, computed from actual status timestamps:

| Window, minutes | GET/s | Logical body MiB/s | Daemon cores | CLI RSS at boundary, GiB |
| --- | ---: | ---: | ---: | ---: |
| 0-10 | 1,172 | 56.80 | 0.909 | 3.33 |
| 10-20 | 1,156 | 38.18 | 0.877 | 5.91 |
| 20-30 | 1,140 | 34.83 | 0.863 | 8.69 |
| 30-40 | 898 | 18.60 | 0.730 | 9.93 |
| 40-50 | 623 | 19.94 | 0.566 | 11.35 |
| 50-60 | 523 | 12.48 | 0.484 | 11.31 |

The falling logical read rate is observed, but completed catalog record counts
are unavailable: GET/s is not records/s or percent complete. No reliable ETA
can be inferred. Logical bodies do not establish physical disk/NFS traffic or
saturation; CPU and GET service durations overlap. This is one extended run,
not a matched comparison with the earlier ten-minute attempt.

Source inspection confirms each unary page constructs a new iterator and
collects one page. Backup uses `SchemaStore.ScanPrefix` without a transaction;
it does not use the existing transaction-scoped streaming scan with a retained
iterator and explicit 1 MiB read-ahead. The first candidate is to reuse that
bounded streaming path, preserving catalog validation, cancellation and lookup
projection correctness, and expose catalog progress. The exact costs of
iterator recreation, read-ahead and storage latency were not separately profiled
in the daemon. On-demand lookup is a separate, larger memory optimization;
simply removing `LoadIndex` remains unsafe while lookups require its projection.
Raising file-read concurrency cannot fix this pre-archiving phase.

The original primary remained PID 431717, read-write at epoch 55, with no active
transactions/intents or surviving backup process. No new-snapshot durability,
source traversal or payload throughput was measured. The command, committed
revision, hashes, profiles, interval summaries, five-second samples, inventories,
health snapshots, assertion-based analyzer and checksums are retained under
`db.test/authoritative-backup-60m-2026-09-24`.

## Authoritative backup startup (2026-09-24)

A real backup attempt used the saved `cdot` job's 52 readable source roots,
host `ncl1-1-ps`, paths grouping, one-filesystem traversal, and existing
exclusions. It used the primary HDD-NFS repository and daemon, with authoritative
metadata enabled and no deferred commit or metadata bypass. A profile CLI was
built from `63454280b` plus the recorded explicit-socket routing patch. No
build/test workload overlapped measurement; the daemon was not changed.

Preflight exposed that ordinary password unlock ignored the global
`--metadata-daemon-socket` option. A temporary socket alias was rejected by
endpoint permission checks and removed before backup. The CLI routing fix
honors the explicit socket when attaching the metadata engine, retaining all
endpoint validation. The encrypted native-daemon regression passed under the
race detector, including password unlock before storing a key-in-DB master key.

The backup reached the ten-minute TERM cap and exited 124 after 600.30 seconds.
It did not reach file processing or publish a snapshot. The before/after
inventories contain the same 143 snapshot IDs; daemon engine writes and commit
requests both increased by zero. The primary remained PID 431717, read-write
at epoch 55, with no active transactions/intents or remaining backup process.

| Measurement | Value |
| --- | ---: |
| CLI user / system CPU, seconds | 29.86 / 7.77 |
| CLI peak RSS, KiB | 2,321,456 |
| Daemon status window, seconds | 601.082 |
| Daemon CPU, seconds | 457.52 |
| Main-store GET requests | 504,245 |
| Main-store GET aggregate service, seconds | 531.009527 |
| Main-store GET-body bytes | 27,943,016,751 |

A non-disruptive goroutine capture at 60 seconds places startup in
`loadBackupParent -> LoadIndex -> DaemonEngine.loadBlobCatalog -> ScanPrefix`.
The cancellation error confirms that phase was still active at timeout.
The loader serially fetches 10,000-record `b:` pages and materializes every
location by pack before archiving. The CPU profile attributes 77.51% of its
36.42 sampled CPU seconds cumulatively to this loader; total CLI utilization
was only about 6% of one core. Daemon utilization averaged about 0.76 cores.

This identifies full-catalog preload as the immediate backup startup bottleneck,
not source traversal or file-read concurrency. Logical GET-body bytes do not
prove physical HDD/NFS saturation, and CPU/service durations overlap. No
archiving throughput or new-snapshot durability performance was measured.
The next candidate is bounded catalog streaming or context-aware on-demand
lookup; simply omitting `LoadIndex` is unsafe while lookup uses its projection.

The command, source patch, binary hashes, before/after inventories and status,
five-second samples, goroutine capture, CPU profile, reproducible analyzer,
resource timings and checksums are retained under
`db.test/authoritative-backup-2026-09-24`.

## Full index check after snapshot import (r54, 2026-09-24)

A fresh profile CLI from committed `3725db870` reran the full differential
index check against the primary after the 143 historical snapshot records were
imported. It used the existing HDD-NFS repository and scratch path, 32 workers
and RPC slots, 96 GiB memory/scratch limits, and a ten-minute TERM cap plus
45-second kill grace. No build/test workload overlapped the measurement.

The check completed in 305.21 seconds with exit status 0 and full coverage
marked complete. It detected 143 legacy snapshots and 143 SlateDB snapshots,
with zero snapshot mismatches, unresolved snapshots or snapshot-commit
mismatches. Historical records passed schema validation and their root IDs
resolved to catalog records containing tree locations. This verifies snapshot
membership, stored metadata and root catalog references, not recursive tree
traversal, pack payload integrity or a production restore.

Compared with pre-import r53, only two numeric result counters changed:

| Counter | r53 | r54 |
| --- | ---: | ---: |
| SlateDB snapshots | 0 | 143 |
| Snapshot mismatches | 143 | 0 |

Both sides still contain 379,934,385 blob locations. All location, pack,
aggregate and reference mismatch counters remain zero. The existing 419,530
warnings remain, along with 419,530 pending exports and unknown tier,
retention and usage-accounting pack counts. Exit status 0 is not a
warning-free verdict; this run did not enable `--fail-on-warning`.

CLI user/system CPU was 2,883.36/249.16 seconds, peak RSS was 82,149,368 KiB,
and peak scratch use was 39,886,207,390 bytes, with zero swaps. Wall time is
close to r53's 304.50 seconds, but these are not matched performance repeats.
The primary remained PID 431717, read-write at epoch 55, with no active
transactions/intents and zero additional engine writes. Scratch was empty
after completion; CLI and daemon executable identities were verified.

The command, build identity, full JSON verdict, progress/telemetry, resource
samples, before/after health, reproducible analyzer and checksums are retained
under `db.test/phase33-production-2026-09-24-stream32-historical-snapshots-full-r54`.

## Snapshot metadata-only import (2026-09-24)

A fresh profile CLI built from `3f0544b9f` imported all 143 historical snapshot
records into the existing primary using `--snapshot-metadata-only --resume`.
The repository and database remained on HDD-backed NFS. The run retained the
ten-minute TERM cap plus 45-second grace and stopped on the first source error.
No build/test workload overlapped measurement, and no daemon installation,
restart, reset, activation, or authority change occurred.

| Measurement | Publication | Dry-run resume verification |
| --- | ---: | ---: |
| Exit status | 0 | 0 |
| Wall time, seconds | 15.34 | 2.34 |
| CLI user / system CPU, seconds | 1.98 / 0.26 | 0.53 / 0.11 |
| Peak CLI RSS, KiB | 209,728 | 176,308 |
| Snapshots seen | 143 | 143 |
| Snapshots imported / resumed | 143 / 0 | 0 / 143 |
| Daemon engine write-operation delta | 143 | 0 |

Both commands reported zero errors, warnings, trees/nodes visited, imported
packs/blobs, and crawl debt. The resume check revalidated matching stored JSON
and root locations without additional writes. These are preserved historical
snapshot records, not verified inode identities or full tree/data verification;
no traversal checkpoints were created by this mode. Full traversal still uses
the old catalog preload. The earlier 600-second timeout and this run therefore
have different scopes and do not establish a like-for-like full-import speedup.

The publication daemon-status window was 16.307 seconds, including setup and
final collection, with 5.65 daemon CPU seconds. There were 143 commit requests
with 11.115136 aggregate service seconds, including 143 durable waits totaling
10.694801 seconds. Admission lock hold totaled 11.929311 seconds but admission
wait was only 0.000079 seconds. These nested timings overlap; they must not be
added or interpreted as independent wall-clock phases. They point to per-record
durability, not CPU or admission contention, as the next measured optimization
target. Bounded transactional snapshot batching merits evaluation while retaining
root validation, immutability, durable publication and retry semantics.

The same window recorded 786 main-store GETs, 2.632275 aggregate GET service
seconds, and 1,794,691,842 returned body bytes, plus 286 WAL PUTs totaling
194,667 bytes. These are logical object-store counters, not physical NFS I/O.
The resume check has different work and warmer caches and is not a matched
performance control. No full differential checker or payload restore was run
as part of this measurement.

The primary remained PID 431717, read-write at epoch 55, with zero active
transactions/intents after publication and resume. Binary hashes were unchanged
through the run. Commands, committed candidate, timings, one-second telemetry,
before/after writer status, reproducible `analyze.cjs`/`analysis.json`, and
checksums are retained under `db.test/snapshot-metadata-only-2026-09-24`.

## Historical snapshot import startup observation (2026-09-24)

After historical snapshot support was committed as `c23b12067`, the user
authorized a resumable import into the existing primary database and requested
performance observation. A fresh committed CLI used the existing socket and
HDD-NFS repository, with unlimited snapshot depth, resume enabled, a one-source
error stop and a ten-minute TERM cap plus 45-second kill grace. No reset,
activation, service installation or restart was requested or performed.

Cancelling the terminal tool did not stop its child import. Observation attached
to the surviving PID 917005 rather than launching a second importer. The import
then reached its original cap: the recorded lifecycle is 600.038 seconds and
the error is `load source indexes for snapshot import: load authoritative blob
catalog: rpc error: code = Canceled desc = context canceled`. Snapshot traversal
was never reached. All import result counts are zero, including snapshots and
nodes; daemon engine write operations did not increase. The 143 production
snapshots therefore remain unimported.

The five-second sampler captured 52 live-process observations over the final
255.251 seconds. These are not whole-run resource totals:

| Measurement | Import CLI | VaulticDB |
| --- | ---: | ---: |
| Sample-window CPU seconds | 15.87 | 191.91 |
| Mean CPU cores used | 0.062 | 0.752 |
| First / last RSS, KiB | 1,520,712 / 2,386,812 | 598,252 / 601,596 |
| Sampled peak RSS, KiB | 2,386,812 | 636,044 |
| Logical `rchar` delta, bytes | 753,504,915 | 67,003,384,804 |

The wider 632.583-second daemon-status window includes setup and post-run
observation. It records 459.76 daemon CPU seconds, 506,756 main-store GETs,
532.447 aggregate GET service seconds and 28,018,033,665 returned body bytes.
GET counts and aggregate service are not equivalent to physical NFS reads or
exclusive wall-clock disk wait. Kernel thread samples mostly show futex waits,
with some daemon NFS waits; they are not Go goroutine or Rust async stack
profiles. No CPU stack profile was collected, so exact crypto/iterator/I/O
shares remain unmeasured. GNU time output and the shell timeout exit status
were lost with the cancelled wrapper; lifecycle/error output establishes the
cap, not an invented exit-code observation.

The controlling path is `runIndexImport` calling `repo.LoadIndex` before
`legacyimport.Import`. `DaemonEngine.loadBlobCatalog` serially scans all `b:`
records through 10,000-record unary `ScanPrefix` pages and accumulates all blob
locations by pack. It does not use the checker's parallel streaming scan.
This is the first demonstrated bottleneck, before snapshot publication or
durability can be measured. The next optimization should remove unnecessary
full-catalog materialization for snapshot tree loading, or replace this unary
startup scan with bounded streaming. Increasing pack import workers cannot
parallelize this earlier code path. No optimization was applied in this run.

There are 41 monitor snapshots, with the initial daemon collection unavailable.
The saved analyzer accounts for that missing collection and checks zero imported
snapshots/nodes and healthy writer state. The primary remains read-write at
epoch 55, with zero active transactions/intents and no import process left.
Logs, committed executable, source/binary identities, process/NFS samples and
reproducible analysis are retained under
`db.test/historical-snapshot-import-2026-09-24`.

## Full-check catalog streaming (r53, 2026-09-23)

The matched block-scratch evidence was committed as `212cfba1c`, without
pushing. This working-tree candidate enables full-check pack catalog streaming
only when the checker store exposes its RPC limiter with more than one slot.
A stream holds one slot while its callback may issue a missing-pack `Get`;
single-slot and unknown stores therefore retain pagination. SlateDB-only
streaming is unchanged. No RPC, memory or scratch limit is raised, and no
missing-pack verification is skipped or moved outside admission accounting.

Tests exercise actual one-slot/two-slot admission, exact catalog results and
aggregates, missing contributions before and after scanned packs, omitted
existing-pack detection, cancellation and released permits. Existing pagination,
malformed-record and checker tests pass. Full maintenance race tests (9.890s),
affected CLI `Test(Check|Index)` race tests (20.727s), formatting, editor
diagnostics and isolated profile build passed before the diagnostic. The first
new test compile required a missing standard-library import; its rerun passed.

R53 used the same HDD-NFS repository and scratch, 32 loader workers/RPCs,
at most four comparison tasks, 96 GiB memory/scratch budgets and ten-minute
TERM cap with 45-second kill grace. No build/test workload overlapped it.
There was no daemon installation, restart or cache reset.

| Measurement | Prior block r50/r51 | r53 streaming |
| --- | ---: | ---: |
| Wall seconds including cleanup | 324.75 / 321.67 | 304.50 |
| CLI CPU seconds | 3,202.00 / 3,140.33 | 3,117.63 |
| Legacy scan | 75s / 72s | 75s |
| Encryption audit | 14s / 14s | 14s |
| SlateDB scan | 70s / 70s | 70s |
| SlateDB finalization | 9s / 9s | 10s |
| Catalog join | 41s / 43s | 23s |
| Partitioned merge plus comparison | 84s / 82s | 80s |
| Peak RSS, KiB | 81,942,052 / 82,213,332 | 80,545,028 |
| Peak scratch bytes | 39,874,827,674 / 39,902,974,248 | 39,871,140,044 |
| Recorded scratch read/write bytes | 104,546,699,652 / 104,544,992,400 | 104,546,041,540 |
| Completed merge groups | 96 / 95 | 94 |

Against the prior two-run mean, catalog time decreased 45.2%, elapsed time
5.8% and CLI CPU 1.7%. Scratch traffic is effectively unchanged, with zero
swaps. This is one candidate observation against earlier controls, not a
fresh matched sequence or repeatable speedup acceptance. Cache state and
run variability remain confounders; no cold-cache or native RADOS claim is made.

Exact logical results match both controls excluding resource telemetry,
consistency session ID and separately recorded encrypted objects (161 in all
three). Both sides contain 379,934,385 locations, with zero location, pack,
aggregate or reference mismatches. Full scope completed but is not clean:
143 missing SlateDB snapshots and 419,530 warnings/pending exports remain.
Checker exit 2 is the same metadata-difference verdict; harness exit 1 is its
known final accepted-exit gate. No metadata repair was performed.

Per-partition blob counts/chunks/ranges match across all 256 partitions.
Aggregate stream telemetry now also includes the 419,530 pack records in
42 chunks: 376,766,240 records, 37,811 chunks and 257 completed ranges total.
Reported stream bytes are 35,623,707,627. The added catalog range must not be
interpreted as expanded blob coverage or compared blindly with blob-only
aggregate telemetry from the controls.

The saved analyzer checks exact logical parity, unchanged limits, blob scope,
expected catalog telemetry, exit codes, cleanup, zero swaps and daemon health.
Artifacts, executable, tested patch, validation logs and comparison are under
`db.test/phase33-full-catalog-stream-2026-09-23`; raw output is under
`db.test/phase33-production-2026-09-23-stream32-full-catalog-stream-full-r53`.
The measured CLI SHA256 is
`f33983e390ca2a0fd82c4f34ebd0a3df6f71d38e321cc58d187dba51c2191bf1`.
Raw and supplemental manifests verified, scratch is empty, and the adopted
daemon remains PID 431717, epoch 55, read-write with zero transactions/intents
and unchanged executable. Source, tests and this evidence remain uncommitted.

## Block scratch matched repeats (r49-r52, 2026-09-23)

The block-scratch implementation, tests and initial r48 evidence were committed
as `e008f4c01` with a detailed message, without pushing. A fresh
control/block/block/control sequence reused the exact saved partitioned
per-record and block-scratch executables. No rebuild, daemon restart, cache reset
or installation occurred. Paired executable hashes match; current workspace
revisions in run metadata are not the source identity of the saved binaries.

All four runs used HDD-NFS data and scratch, 32 loader workers/RPCs, at most four
comparison tasks, unchanged 96 GiB checker memory/scratch limits and separate
ten-minute TERM caps with 45-second kill grace. Runs were sequential with no
overlapping builds/tests or detected backup workloads. The wrapper stopped on
unexpected results, cleanup failures or daemon changes; all four completed its
known-exit-2, logical parity, scan scope, limits and operational health gates.

| Measurement | r49 control | r50 block | r51 block | r52 control |
| --- | ---: | ---: | ---: | ---: |
| Wall seconds including cleanup | 375.80 | 324.75 | 321.67 | 370.15 |
| CLI CPU seconds | 3,518.38 | 3,202.00 | 3,140.33 | 3,507.70 |
| Legacy scan | 81s | 75s | 72s | 79s |
| Encryption audit | 14s | 14s | 14s | 14s |
| SlateDB scan | 74s | 70s | 70s | 73s |
| SlateDB finalization | 13s | 9s | 9s | 14s |
| Catalog join | 46s | 41s | 43s | 42s |
| Partitioned merge plus comparison | 115s | 84s | 82s | 116s |
| Peak RSS, KiB | 90,979,164 | 81,942,052 | 82,213,332 | 89,455,116 |
| Peak scratch bytes | 48,720,798,086 | 39,874,827,674 | 39,902,974,248 | 48,714,978,284 |
| Recorded scratch read/write bytes | 127,723,817,300 | 104,546,699,652 | 104,544,992,400 | 127,723,722,760 |
| Completed merge groups | 96 | 96 | 95 | 95 |

Both block runs finished faster than both controls and used fewer CLI CPU
seconds. Mean elapsed time decreased from 372.975 to 323.210 seconds (13.3%),
CLI CPU from 3,513.040 to 3,171.165 seconds (9.7%), and merge/comparison from
115.5 to 83 seconds (28.1%). Mean peak RSS decreased 9.0%, peak scratch 18.1%
and recorded scratch traffic 18.1%. All runs had zero swaps. The scratch traffic
counter combines reads and writes; it is not a physical disk-byte measurement.

Complete logical results match across all four runs after excluding resource
telemetry, consistency session ID and the separately recorded live
encrypted-object count, which was 161 throughout. This includes retained
findings, inventory/options digests and all 379,934,385 locations on each side.
Location, pack, aggregate and reference mismatches remain zero. Every run exits
2 for the same 143 missing SlateDB snapshots and retains 419,530 warnings and
pending exports. Harness exit 1 is its known final accepted-exit gate, not a
crash. No metadata repair was performed and clean full-check acceptance remains
blocked by the snapshot discrepancies.

Configured limits and scan scope match exactly: 10,019 legacy indexes,
376,346,710 SlateDB records, 37,769 chunks and 256 ranges. Scan byte totals vary
slightly (35,556,163,085 / 35,556,163,083 / 35,556,163,092 / 35,556,163,084).
Two observations per variant support the measured NFS elapsed/CPU/I/O improvement
but provide no confidence bounds or cold-cache, other-workload or native RADOS
acceptance. No deployment decision is implied.

All four raw manifests verified, and the saved analysis reproduces exactly,
including paired binary identities, logical parity, scope, limits and health.
Final live checks confirmed empty scratch and unchanged PID 431717, epoch 55,
read-write with zero transactions/intents and the adopted executable unchanged.
Runner, analyzer, comparison JSON, copied binaries and logs are under
`db.test/phase33-block-repeats-2026-09-23`. Raw runs are
`db.test/phase33-production-2026-09-23-stream32-block-control-full-r49`,
`db.test/phase33-production-2026-09-23-stream32-block-candidate-full-r50`,
`db.test/phase33-production-2026-09-23-stream32-block-candidate-full-r51` and
`db.test/phase33-production-2026-09-23-stream32-block-control-full-r52`.

## Block-authenticated scratch (r48, 2026-09-23)

The repeat evidence was committed as `aa677ab5b`, without pushing. The next
working-tree candidate batches up to 512 location tuples into one authenticated
scratch frame instead of one frame per tuple. Temporary run magic changes from
`VLTCHK01` to `VLTCHK02`; repository and daemon formats are unchanged. Scratch
runs are session-owned and are not resumed across binaries. Each frame retains
AES-GCM authentication with the run prefix and sequential block nonce, with
strict positive, aligned and bounded plaintext lengths.

Both direct spill and streaming merge writers construct and encrypt blocks in
the existing 1 MiB buffered output allocation. Readers authenticate a complete
block in their existing input buffer before exposing any of its tuples. No
additional per-reader block allocation or tuple-budget increase is introduced.
Readers also check framing against the trusted in-memory run size, rejecting
whole-block removal and trailing frames as well as partial truncation. This
does not add a separately authenticated EOF footer. Empty runs contain only
the header. Direct reservations account for tuple bytes plus per-block framing;
merge reservations remain bounded by input payload sizes, with unused capacity
released on close and all reserved bytes released on abort.

The first focused check exposed the intermediate mismatch between block-sized
direct writes and old per-tuple merge framing. Batching merge output resolved
the reservation failures, and the merge-headroom fixture now uses actual input
sizes. Block tests cover empty, partial, full, boundary-crossing and multi-buffer
runs for both writer paths, exact tuples and file sizes, surplus reservations,
abort cleanup and writes after close. Reader tests cover refill boundaries,
length/alignment tampering, ciphertext/tag corruption, replay, whole-block
removal and representative partial-frame truncations. Authentication failures
are rejected before returning the affected block's first tuple. Concurrent merge
admission, cancellation, corruption and cleanup tests also pass.

Full maintenance race tests (9.301s), affected CLI `Test(Check|Index)` race tests
(20.445s), formatting, editor diagnostics and isolated profile build passed.
No builds/tests overlapped the HDD-NFS diagnostic. R48 retained 32 loader
workers/RPCs, at most four comparison tasks, 96 GiB checker memory/scratch limits,
the ten-minute TERM cap with 45-second kill grace and the unchanged adopted
daemon without restart or installation.

| Measurement | Prior partitioned r45/r46 | r48 block scratch |
| --- | ---: | ---: |
| Wall seconds including cleanup | 372.62 / 405.09 | 325.26 |
| CLI CPU seconds | 3,505.92 / 3,514.54 | 3,120.18 |
| Merge plus comparison | 113s / 116s | 83s |
| Peak RSS, KiB | 91,338,172 / 87,492,952 | 81,184,508 |
| Peak scratch bytes | 48,700,116,290 / 48,722,914,994 | 39,885,561,628 |
| Recorded scratch read/write bytes | 127,723,817,060 / 127,723,816,892 | 104,543,545,952 |
| Completed merge groups | 96 / 96 | 91 |

R48's rounded stages were legacy scan 73s, audit 14s, SlateDB scan 69s,
finalization 11s, catalog join 45s, partitioned merge/comparison 83s, location
cleanup 4s and parallel validation 16s. Finalization began at 315 seconds.
Relative to the prior two-run mean, observed elapsed time is 16.4% lower, CLI CPU
11.1% lower, scratch traffic 18.1% lower and peak scratch 18.1% lower. These are
one candidate run against prior runs, not matched repeats; the figures are
promising evidence, not a stable speedup or CPU-saving guarantee. The number of
merge groups also changed with runtime spill distribution.

Complete logical results match r45 after excluding only resources, session ID
and the live encrypted-object count (161 in both runs). Configured limits and
scan records/chunks/ranges match exactly: 10,019 legacy indexes, 376,346,710
SlateDB records, 37,769 chunks and 256 ranges. R48 records 35,556,163,090 scan
bytes and zero swaps. All 379,934,385 locations on each side match, with zero
location, pack, aggregate or reference mismatches. Exit remains 2 for the same
143 missing SlateDB snapshots and 419,530 warnings/pending exports; the harness
then returns 1 at its known accepted-exit gate. No snapshot repair was attempted.

The measured source matches the saved candidate patch. Raw checksums, empty
scratch and final live health checks passed. PID 431717 remained read-write at
epoch 55 with zero transactions/intents and the adopted executable unchanged.
The candidate remains uncommitted and is not installed. Matched repeats, clean
snapshot acceptance and native RADOS acceptance remain open. Artifacts are under
`db.test/phase33-block-scratch-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-block-scratch-full-r48`.

## Partitioned comparison repeats (r44-r47, 2026-09-23)

The partitioned implementation and r45 evidence were committed as `f7cb4252a`
with a detailed message, without pushing. A candidate repeat (r46) followed by a
control repeat (r47) extended r44/r45 into a control/candidate/candidate/control
sequence. Each variant reused its exact saved executable; no rebuild, daemon
restart or installation occurred. Binary identity was verified within each pair.
The run metadata records the current workspace revision, while the executable
hashes and original candidate patches identify the actual tested sources.

Every run used HDD-NFS repository and scratch, 32 loader workers/RPCs, unchanged
96 GiB checker memory/scratch limits and a ten-minute TERM cap with 45-second
kill grace. The candidate uses at most four comparison tasks. Runs were strictly
sequential, without overlapping builds/tests or detected backup workloads. The
repeat wrapper verifies the known exit-2 result, complete logical parity, scan
scope, limits, scratch cleanup, daemon identity and health before continuing.
It does not treat the harness's final exit-1 gate as a successful checker verdict.

| Measurement | r44 control | r45 candidate | r46 candidate | r47 control |
| --- | ---: | ---: | ---: | ---: |
| Wall seconds including cleanup | 578.22 | 372.62 | 405.09 | 594.24 |
| Merge preparation plus comparison | 330s | 113s | 116s | 333s |
| Encryption audit | 14s | 14s | 14s | 14s |
| SlateDB scan | 72s | 72s | 75s | 73s |
| Catalog join | 42s | 47s | 56s | 50s |
| Parallel validation | 20s | 18s | 35s | 22s |
| CLI CPU seconds | 3,513.95 | 3,505.92 | 3,514.54 | 3,523.04 |
| Peak RSS, KiB | 92,798,392 | 91,338,172 | 87,492,952 | 90,924,088 |
| Peak scratch bytes | 55,711,066,520 | 48,700,116,290 | 48,722,914,994 | 55,683,485,230 |
| Recorded scratch read/write bytes | 122,802,430,740 | 127,723,817,060 | 127,723,816,892 | 122,802,430,740 |
| Completed merge groups | 7 | 96 | 96 | 7 |

Both candidates finished faster than both controls. Mean elapsed time decreased
from 586.23 to 388.855 seconds (33.7%), and mean merge/comparison time from 331.5
to 114.5 seconds (65.5%). Mean peak scratch decreased 12.5%, while recorded
scratch traffic increased 4.0%. Mean CLI CPU changed only from 3,518.495 to
3,510.23 seconds (0.23% lower); this is a parallelism benefit, not evidence of
material CPU savings. Mean peak RSS was 2.7% lower and all runs had zero swaps.
The slower candidate repeat spent more time in catalog and parallel validation;
its merge/comparison stage remained close to the first candidate.

All four completed checks exit 2 with matching logical results, including all
retained findings, inventory/options digests and 379,934,385 locations on each
side. Location, pack, aggregate and reference mismatch counters remain zero.
The same 143 missing SlateDB snapshots and 419,530 warnings/pending exports
remain; no metadata repair was attempted. Logical comparison excludes resource
telemetry, session ID and the live encrypted-object count. The latter was
recorded separately and was 161 in every run. Scan records/chunks/ranges and
configured limits match exactly; scan byte totals vary slightly. R46/r47 record
35,556,163,098 and 35,556,163,090 bytes respectively.

The reverse-order repeat supports the elapsed-time improvement under this NFS
configuration. There are only two observations per variant, with no confidence
bounds, cache reset or daemon restart; this does not establish cold-cache
performance, other workloads or native RADOS acceptance. Snapshot discrepancies
still prevent clean full-check acceptance. No deployment decision is implied.

All four raw manifests verified, and the reproducible analysis checks paired CLI
hashes, logical equality, scope, limits, cleanup and daemon health. Final live
checks confirmed empty scratch and the unchanged adopted executable at PID
431717, epoch 55, read-write with zero transactions/intents. The repeat runner,
analysis, copied executables, logs and comparison JSON are under
`db.test/phase33-partition-repeats-2026-09-23`. New raw runs are under
`db.test/phase33-production-2026-09-23-stream32-partition-repeat-full-r46` and
`db.test/phase33-production-2026-09-23-stream32-buffered-repeat-full-r47`.

## Bounded partitioned comparison (r45, 2026-09-23)

After buffered-reader commit `2d3950da5`, the next working-tree candidate routes
legacy locations into 16 ordered blob-ID partitions. Worker and partition tuple
allocations divide the existing legacy spool budget. Each partition uses chunks
of at most one eighth of its allocation so overflow retains other sorted memory
runs. Parent adoption accounts for child capacity and transfers run ownership.
Full checks enable partitioning only when each requested worker/partition can
receive at least 1 MiB; smaller budgets retain the existing serial path. Legacy
worker count is also clamped by per-partition tuple capacity.

Comparison aligns each legacy partition with 16 of SlateDB's existing 256 prefix
partitions. Up to four tasks run concurrently, further limited by the existing
memory allocation and merge-reader buffer sizing. Necessary within-partition
merges execute inside these tasks; there is no global legacy merge. Tasks retain
separate results and combine counters and bounded findings deterministically.
Errors cancel sibling tasks, parent contexts are restored, and cleanup retains
the original spool ownership. Exact tuple comparison, deduplication, pack
contributions, scratch encryption and configured resource limits are unchanged.
The `location_compare_partitioned` stage includes both merge preparation and
comparison; partial counters are combined when the tasks finish, not live.

Extended loader tests compare partitioned output with a serial global reference
across budgets, worker counts and optional pack contributions. New concurrent
comparison tests cover duplicates, asymmetric records, tuple differences, existing
findings, unlimited and bounded findings, cancellation, corrupted encrypted runs,
context restoration and cleanup. Focused tests, full maintenance race tests
(9.712s), affected CLI `Test(Check|Index)` race tests (21.669s), formatting,
editor diagnostics and the isolated profile build passed.

R45 used HDD-NFS data and scratch, the same adopted daemon without restart,
32 loader workers/RPCs, at most four comparison tasks, unchanged 96 GiB checker
memory/scratch limits and the ten-minute TERM cap with 45-second kill grace.
No builds or tests overlapped measurement. Rounded progress boundaries:

| Measurement | r44 buffered reader | r45 partitioned comparison |
| --- | ---: | ---: |
| Legacy scan | 73s | 79s |
| Encryption audit | 14s | 14s |
| SlateDB scan | 72s | 72s |
| SlateDB finalization | 15s | 14s |
| Catalog join | 42s | 47s |
| Location merge preparation plus comparison | 330s | 113s |
| Location cleanup starts | 546s | 339s |
| Finalization starts | 572s | 364s |
| Wall time including cleanup | 578.22s | 372.62s |
| CLI CPU seconds | 3,513.95 | 3,505.92 |
| Peak RSS, KiB | 92,798,392 | 91,338,172 |
| Peak scratch bytes | 55,711,066,520 | 48,700,116,290 |
| Recorded scratch read/write bytes | 122,802,430,740 | 127,723,817,060 |
| Completed merge groups | 7 | 96 |

The complete logical results match exactly after excluding only resources and
the consistency session ID. This includes all retained findings, encryption
counts, legacy inventory digest and options digest. Both sides contain exactly
379,934,385 locations with zero location, pack, aggregate or reference mismatches.
Both completed checks exit 2 for the same 143 missing SlateDB snapshots; 419,530
warnings and pending exports remain. This is not clean full-check acceptance.
The unchanged harness exits 1 at its final accepted-exit gate after saving and
validating the run artifacts; no metadata repair was attempted.

Both scans cover 10,019 legacy indexes and 376,346,710 SlateDB records in 37,769
chunks across 256 ranges; r45 records 35,556,163,085 scan bytes and zero swaps.
Observed elapsed time is 35.6% lower and peak scratch 12.6% lower, while scratch
traffic is 4.0% higher and CLI CPU is essentially unchanged (0.23% lower).
The larger number of smaller merge groups is not a reduction in aggregate work.
These are single sequential completed runs, not matched repeated trials; no
confidence bounds or stable speedup claim follow from this pair. The result
supports retaining the candidate for repeat testing, not deployment acceptance.

The measured source matches the saved candidate patch. Raw checksums, empty
scratch and final health checks passed. PID 431717 remained read-write at epoch
55 with zero transactions/intents and the adopted executable unchanged. The
candidate is uncommitted and not installed. Matched repeats, snapshot discrepancy
resolution and native RADOS acceptance remain outstanding. Artifacts are under
`db.test/phase33-parallel-compare-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-parallel-compare-full-r45`.

## Buffered scratch reader (r44, 2026-09-23)

The legacy-summary implementation and r43 evidence were committed as
`366a9532f` with a detailed message, without pushing. The next working-tree
candidate removes two per-record copies from encrypted scratch reads. It peeks
at the length and complete record in the existing buffered reader, authenticates
and decrypts in place, decodes the tuple, then discards the consumed bytes.
The scratch format, AES-GCM authentication, nonce sequence, length validation
and partial-record error semantics remain unchanged. No resource limit changed.

Focused tests passed, including a 12,000-record fixture crossing buffer refills
with 110/111/219-byte and production 1 MiB buffers, all 109 partial-record lengths,
repeated EOF, invalid length, corrupted authentication tag and replay rejection.
Full maintenance race tests and affected CLI `Test(Check|Index)` race tests,
formatting, editor diagnostics and isolated profile build passed. No builds or
tests overlapped the measurement.

R44 used the same adopted daemon without restart, HDD-NFS repository and scratch,
32 workers/RPCs, 96 GiB checker memory/scratch limits and ten-minute TERM cap with
45-second kill grace. Rounded progress boundaries:

| Measurement | r43 legacy summaries | r44 buffered reader |
| --- | ---: | ---: |
| Legacy scan | 74s | 73s |
| Encryption audit | 35s | 14s |
| SlateDB scan | 73s | 72s |
| SlateDB finalization | 17s | 15s |
| Catalog join | 43s | 42s |
| Legacy merge preparation | 56s | 53s |
| SlateDB merge preparation | <1s | <1s |
| Comparison starts | 298s | 269s |
| Comparison duration | 302s, interrupted | 277s, completed |
| Legacy locations | 378,503,112, partial | 379,934,385 |
| SlateDB locations | 378,503,111, partial | 379,934,385 |
| Completed merge groups | 7 | 7 |
| Recorded scratch read/write bytes | 122,633,354,410 | 122,802,430,740 |
| Peak scratch bytes | 55,679,304,826 | 55,711,066,520 |
| CLI CPU seconds | 3,541.78 | 3,513.95 |
| Peak RSS, KiB | 92,782,444 | 92,798,392 |
| Wall time including cleanup | 611.78s | 578.22s |
| CLI exit | 130, wrapper timeout 124 | 2, metadata indexes differ |

R44 completed location comparison at 546 seconds, location cleanup at 552 seconds,
and parallel validation at 572 seconds before finalization. All 379,934,385
locations matched exactly. Missing/invalid packs, aggregate mismatches, reverse
edge mismatches and unresolved references were zero. The final result reported
143 legacy snapshots, zero SlateDB snapshots and 143 snapshot mismatches; its
100 retained findings were all `missing_snapshot`, wanting `slatedb`. The
snapshot comparator reports this category when neither a matching snapshot nor
an import checkpoint is present. This run does not establish when that state
arose, and no metadata repair was attempted. The result also retains 419,530
pending exports and warnings, including unknown-tier, retention and usage counts.

The check finished before the cap but did not pass. The harness subsequently
returned 1 because its accepted exits are only 0 and 124; the checker exit was
2, not an execution crash. Raw artifacts, post-run health and cleanup had already
been saved and verified before that harness exit gate. This is a completed
differential result, not clean full-check acceptance.

The run scanned the same 10,019 legacy indexes and 376,346,710 SlateDB records in
37,769 chunks across 256 ranges, recording 35,556,163,091 scan bytes and zero swaps.
Comparison completed in less time than r43's interrupted comparison, but these
are single sequential runs with different work boundaries. Audit alone was
21 seconds shorter. No precise completed-check speedup or total CPU-saving claim
is justified without matched repeats. Native RADOS acceptance remains open.

The measured Go patch matches the working source. Raw checksums, empty scratch
and final health checks passed. The daemon remained PID 431717, epoch 55,
read-write with zero transactions/intents and the adopted executable unchanged.
The reader candidate remains uncommitted and is not installed. Artifacts are
under `db.test/phase33-buffered-reader-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-buffered-reader-full-r44`.

## Bounded legacy pack summaries (r43, 2026-09-23)

The ordered-partition implementation and r41/r42 evidence were committed as
`8e23554fe` with a detailed message, without pushing. The next working-tree
candidate enables the existing pack-summary representation for legacy catalog
contributions. Each legacy worker reserves half of its pack-spool allocation
for a bounded aggregation map when the budget permits, flushing partial
summaries into the other half. Tiny budgets retain direct summary insertion.
Finalization flushes and releases the maps before adopting worker spools.
Exact location tuples and pack-presence markers remain unchanged; duplicate
contributions retain their counts, payload totals and type information. Scratch
encryption and total configured memory/scratch budgets are unchanged.

New regression tests compare raw and summarized pack streams with one-tuple,
eight-tuple and 1 MiB budgets and requested worker counts 1/4/32. They verify
duplicate multiplicity, payload totals, pack presence, exact location counts,
bounded retained tuple capacity and cleanup. Existing loader, catalog and
pack-contribution tests passed, as did the full maintenance race suite and
affected CLI `Test(Check|Index)` race tests. Formatting, editor diagnostics and
the isolated profile build passed. The previously documented unrelated full
CLI failures were not rerun or modified.

R43 used the same adopted daemon without restart, HDD-NFS repository and scratch,
32 workers/RPCs, 96 GiB checker memory/scratch limits and ten-minute TERM cap with
45-second kill grace. Builds and tests completed before measurement. Progress
boundaries are rounded:

| Measurement | r42 ordered partitions | r43 legacy summaries |
| --- | ---: | ---: |
| Legacy scan | 101s | 74s |
| Encryption audit | 42s | 35s |
| SlateDB scan | 74s | 73s |
| SlateDB finalization | 20s | 17s |
| Catalog join | 179s | 43s |
| Legacy merge preparation | 68s | 56s |
| SlateDB merge preparation | <1s | <1s |
| Comparison starts | 484s | 298s |
| Comparison window before cap | 116s | 302s |
| Partial legacy locations | 140,963,015 | 378,503,112 |
| Partial SlateDB locations | 140,963,014 | 378,503,111 |
| Completed merge groups | 14 | 7 |
| Recorded scratch read/write bytes | 152,533,184,512 | 122,633,354,410 |
| Peak scratch bytes | 70,198,433,856 | 55,679,304,826 |
| CLI CPU seconds | 3,906.20 | 3,541.78 |
| Peak RSS, KiB | 122,980,788 | 92,782,444 |
| Wall time including cleanup | 616.18s | 611.78s |

Both runs scanned 10,019 legacy indexes and 376,346,710 SlateDB records in
37,769 chunks across 256 ranges. R43 recorded 35,556,163,089 scan bytes. It exited
124 with CLI cancellation 130 and zero swaps. The one-location partial-count
difference can occur when cancellation interrupts iterator advancement; zero
partial mismatch counters are not a completed correctness verdict.

Catalog joining took 136 seconds less in this run, with seven fewer merge groups,
19.6% less recorded cumulative scratch traffic and 20.7% lower peak scratch,
despite substantially more comparison work. Peak RSS was 24.6% lower. The memory
limit is a tuple/aggregation budget, not an RSS cap. This supports the bounded
summary approach, but sequential single capped runs are not matched repeated
full-check measurements. Audit time also varied, and partial work boundaries
differ. The CPU figures do not establish total completed-check CPU savings.

Raw checksums and empty scratch cleanup passed. The daemon remained PID 431717,
epoch 55, read-write with zero transactions/intents and its adopted executable
unchanged. The candidate remains uncommitted and is not installed. A completed
differential verdict and matched repeats remain open; native RADOS acceptance
is also outstanding. The remaining measured tail is legacy merge preparation
and exact comparison, which should guide the next bounded optimization.

Artifacts are under `db.test/phase33-legacy-summaries-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-legacy-summaries-full-r43`.

## Preserve ordered SlateDB partitions (r42, 2026-09-23)

The r41 attribution identified 95 seconds preparing the global SlateDB location
iterator. The next candidate retains the 256 disjoint blob-ID partition spools
and concatenates their sorted iterators in prefix order, opening one partition
at a time. Full-mode scans now validate blob key kind and partition membership
before accepting a record. Memory capacity remains charged to the parent spool;
partition ownership, cancellation and cleanup follow the existing parent
context. Legacy spools, pack contributions, exact comparison and the configured
resource limits are unchanged. Any necessary within-partition merges execute
lazily during comparison rather than being hidden as eliminated work.

Tests compare spilled partitions with a global reference spool, including
duplicates, empty and widely separated prefixes, exact counts, cancellation,
resource cleanup and rejection of out-of-partition keys. The fixture verifies
that no global merge is performed. Focused race tests and the full maintenance
race suite passed. The full CLI suite failed in backup permission expectations,
Phase 34 scan-response result equality, monitor snapshot golden output and the
default version string. Each failure category was reproduced on clean HEAD
`326450df5`; the Phase 34 reproduction used the same existing test daemon and
the `0ms/repeat-1` case. These failures were not changed by this experiment.

R42 used the same adopted daemon without restart, HDD-NFS repository and scratch,
32 workers/RPCs, 96 GiB memory/scratch limits and ten-minute cap as r41. Builds
and tests finished before measurement. Rounded progress boundaries:

| Measurement | r41 attribution control | r42 ordered partitions |
| --- | ---: | ---: |
| Legacy scan | 98s | 101s |
| Encryption audit | 56s | 42s |
| SlateDB scan | 75s | 74s |
| SlateDB finalization | 16s | 20s |
| Catalog join | 183s | 179s |
| Legacy merge preparation | 69s | 68s |
| SlateDB merge preparation | 95s | <1s |
| Comparison starts | 592s | 484s |
| Comparison window before cap | 8s | 116s |
| Partial legacy locations | 7,158,424 | 140,963,015 |
| Partial SlateDB locations | 7,158,424 | 140,963,014 |
| Completed merge groups | 30 | 14 |
| Recorded scratch read/write bytes | 200,006,112,552 | 152,533,184,512 |
| Peak scratch bytes | 70,240,992,196 | 70,198,433,856 |
| CLI CPU seconds | 3,908.50 | 3,906.20 |
| Peak RSS, KiB | 119,816,020 | 122,980,788 |
| Wall time including cleanup | 613.26s | 616.18s |

Both runs scanned 10,019 legacy indexes and 376,346,710 SlateDB records in
37,769 chunks across 256 ranges. R42 recorded 35,556,163,087 scan bytes, versus
35,556,163,101 in r41. Both exited 124 with CLI cancellation 130 and zero swaps.
The one-location partial-count difference in r42 can occur when cancellation
interrupts advancement of the two iterators; it is not a mismatch verdict.

R42 performed 16 fewer merge groups and recorded 23.7% less cumulative scratch
traffic despite reaching substantially more comparison work. This supports
removing the cross-partition merge, not merely relabeling it. It does not
establish a completed-check speedup: neither run finished, their audit times
differ, they were sequential single runs, and their work boundaries differ.
Zero partial mismatch counters and full selected coverage are not correctness
acceptance. Scratch peak remains almost unchanged because earlier catalog and
legacy work still controls it. CPU consumption was effectively unchanged over
the capped run; no total CPU-saving claim is made.

Raw checksums and empty scratch cleanup passed; the daemon remained PID 431717,
epoch 55, read-write with zero transactions/intents and its adopted executable
unchanged. The candidate remains uncommitted and is not installed. Further work
should target catalog reduction and the legacy global merge, or partition both
sides for bounded parallel exact comparison. A complete differential verdict
and matched repeats remain required before runtime acceptance.

Artifacts are under `db.test/phase33-ordered-partitions-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-ordered-partitions-full-r42`.

## Full-check tail attribution (r41, 2026-09-23)

After release commit `326450df5`, a diagnostic-only CLI split the broad
`catalog_join` progress label into catalog work, legacy iterator preparation,
SlateDB iterator preparation, and exact location comparison. Additional cleanup
and aggregate-validation labels distinguish later work. A spilled-input test
checks the stage sequence and unchanged comparison output. Focused tests and
affected maintenance/index-command race tests passed.

The full HDD-NFS diagnostic used the adopted daemon without restart, 32 workers
and RPCs, unchanged 96 GiB checker memory and scratch limits, and a ten-minute
cap. No builds or tests overlapped. Stage boundaries, rounded by progress output:

| Stage | Start | Duration before next stage/cap |
| --- | ---: | ---: |
| Legacy scan | 0s | 98s |
| Encryption audit | 98s | 56s |
| SlateDB scan | 154s | 75s |
| SlateDB finalization | 229s | 16s |
| Catalog join | 245s | 183s |
| Legacy location merge preparation | 428s | 69s |
| SlateDB location merge preparation | 497s | 95s |
| Exact location comparison | 592s | 8s, interrupted |

All 10,019 legacy indexes and 376,346,710 SlateDB records were scanned, with
37,769 chunks and 256 completed ranges. Only 7,158,424 locations on each side
were compared before cancellation. Zero partial mismatch counters do not imply
a clean verdict. Exit was 124 (CLI cancellation 130), with 613.26 seconds wall
time including cleanup, 3,908.50 CLI CPU seconds, peak RSS 119,816,020 KiB,
zero swaps, 30 merge groups, and peak scratch 70,240,992,196 bytes.

The result does not support attributing the whole tail to the serial comparison:
catalog work and iterator preparation consumed almost all of the post-scan
window. The longer audit also prevents treating r40/r41 as matched runtime
controls. The next narrow candidate preserves SlateDB's disjoint blob-ID
partitions through ordered iteration instead of merging them into a global
spool. Necessary within-partition merges become lazy comparison work; total
runtime, merge activity and exact results remain the evaluation criteria.

Raw artifact checksums and empty scratch cleanup passed. The daemon remained
PID 431717, epoch 55, read-write with zero transactions/intents. No production
binary was installed. Artifacts are under
`db.test/phase33-location-attribution-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-location-attribution-full-r41`.

## Retained-memory spill experiment, 2026-09-23

The parallel legacy consumer and r39 evidence were committed as `35724eb1b`,
without pushing. The next working-tree change preserves sorted memory runs when
a spool reaches its tuple-memory budget. Instead of writing every retained run
to disk, automatic overflow writes one run and reuses its backing array as the
disk-mode buffer. Other runs remain in memory within the same capacity accounting
and are consumed by the existing mixed memory/disk iterator. Explicit full-spill
behavior is unchanged.

Focused tests verify exact set and multiset output, retained-run capacity, and
a scratch limit that cannot accommodate spilling all runs. Existing full-spill
retry coverage now invokes that explicit helper. The maintenance and index-command
suites passed with the race detector, and formatting/editor checks passed.

Full HDD-NFS run `stream32-retained-spill-full-r40` kept 32 workers/RPCs, 96 GiB
checker memory, 96 GiB scratch, and the ten-minute cap. No builds/tests or known
backup workloads overlapped it. The adopted daemon was unchanged and was not
restarted. The isolated Go 1.27.1 profile CLI SHA-256 was
`9c55f5f699762b22c5b996f46fc6bf6828ed38f9b853e6bab59e80d52dd3d43d`.

All 10,019 legacy indexes loaded by about 97 seconds, with 29,497,229,840 bytes
of scratch, compared with 181 seconds and 83,631,727,424 bytes in r39. Auditing
finished at about 111 seconds. All 256 blob ranges completed by 185 seconds:
376,346,710 records, 35,556,163,076 bytes, and 37,769 chunks. Finalization reached
`catalog_join` at 203 seconds. Scratch peak was 70,418,506,212 bytes (about
65.6 GiB), and 30 merge passes were recorded. This run progressed beyond the
previous scratch-exhaustion boundary without increasing either configured limit.

The ten-minute cap interrupted work still labeled `catalog_join`; this label
also covers subsequent location comparison. Partial location counters had reached
87,054,373 legacy and 87,054,372 SlateDB locations, so the stage label alone must
not be used to attribute the entire wait to catalog reduction. The wrapper exited
124 and the CLI reported cancellation code 130. Total time including cleanup was
611.78 seconds, CLI CPU was 4,000.78 seconds, peak RSS was 122,151,980 KiB (about
116.5 GiB), and no swaps occurred. The checker tuple budget is not an RSS cap.

There is no final differential verdict or end-to-end acceptance. In particular,
the partial result's `coverage.complete: true` describes the selected full-check
coverage, not successful execution after cancellation. Neither incomplete location
counts nor mismatch counters establish correctness. These sequential single runs
are not matched repeated controls, and their different completion boundaries do
not establish total CPU savings. The next diagnostic should distinguish catalog
reduction, location merge, and exact comparison before choosing another change.

Scratch cleanup and raw artifact checksums passed. PID 431717 remained read-write
at epoch 55 with zero transactions and write intents, and its executable still
matched the adopted binary. No installed binary, backend, or resource limit was
changed. The retained-memory change remains uncommitted and native RADOS acceptance
remains open.

Artifacts are under
`/volume2/NASDA2/rustic/db.test/phase33-retained-spill-2026-09-23/`
and the raw run directory
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-23-stream32-retained-spill-full-r40/`.

## Parallel legacy consumer experiment, 2026-09-23

The diagnostic attribution changes were committed as `41207ec50` with a detailed
implementation and evidence message, without pushing. The subsequent working-tree
experiment removes the serialized `ForAllIndexesWorkers` callback from the full
check's legacy loader. Up to 32 workers own private location and pack-multiset
spools, with the existing total spool budgets divided among workers. Workers are
also limited by the number of tuples those budgets can hold. Final buffers are
finished concurrently, then adopted using the parent context rather than the
completed worker group's canceled context. Shared legacy-index APIs are unchanged.

Local validation passed the maintenance and index-command package suites, plus
focused race tests. Serial-reference tests cover worker requests 0/1/4/32,
one-tuple, eight-tuple, and memory-only budgets, optional pack contributions,
duplicate locations, and preserved pack multiplicity. Failure tests cover malformed
input, pre-cancellation, scratch exhaustion, and private spill cleanup. These are
correctness fixtures, not representative-storage performance measurements.

One full HDD-NFS diagnostic, `stream32-legacy-parallel-full-r39`, used 32 workers
and RPCs, 96 GiB checker memory, 96 GiB scratch, and the ten-minute cap. No builds
or tests overlapped the measurement and no known backup workload was observed.
The adopted daemon was not restarted or replaced. The separate CLI was built
with Go 1.27.1 and `selfupdate,disable_grpc_modules,profile`, with SHA-256
`fff9713f3172f6b47fdfc6b0d532245c3659399a3a298a2b8838fcfe24295a75`.

The legacy scan loaded all 10,019 indexes and reached encryption auditing at
about 181 seconds, with 83,631,727,424 bytes of scratch. Auditing completed at
about 196 seconds. The SlateDB scan then exhausted the unchanged scratch cap:
it needed another 76,895,512 bytes with 103,009,415,936 of 103,079,215,104 bytes
already used. The CLI exited 1 after 266.29 seconds including cleanup, before
any merge pass or final differential verdict. Partial scan counters were
250,485,342 records, 25,127 chunks, and 155 completed ranges. Zero final location
or mismatch counters in this failed result are not evidence of equivalence.

CLI CPU was 3,438.25 seconds and peak RSS was 86,211,860 KiB, with no swaps.
Host interval samples averaged 32.77% idle and 10.93% I/O wait. The earlier r34
serial run had not finished legacy scanning at the ten-minute cap, but it is not
a matched control. This establishes progress beyond the previous bottleneck,
not an end-to-end speedup ratio, CPU reduction, or full-check acceptance.

Scratch was empty after failure. The same daemon PID 431717 remained read-write
at epoch 55 with zero transactions and write intents. No installed CLI, daemon,
backend, or resource limit was changed. The next gate needs an explicitly agreed
scratch budget or bounded spill reduction, followed by a completed full check.
Native RADOS validation remains blocked by the previously recorded prerequisites.

Raw artifacts and their verified manifest are under
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-23-stream32-legacy-parallel-full-r39/`.
The source patch, candidate binary, build and test logs, and harness are under
`/volume2/NASDA2/rustic/db.test/phase33-legacy-parallel-2026-09-23/`.

## Local streaming follow-up, 2026-09-22

The working-tree follow-up implements a negotiated `ScanStream` RPC. After
explicit operator approval it was deployed to `vaulticdb-rustic.service` using
the configured clean-demotion stop hook. Preflight found zero active write
intents and transactions; restart advanced writer epoch 37 to 38 and restored
the read-write role. The earlier measurements below still describe the unary
implementation; they are not evidence of a streaming speedup.

Matched profile binaries were built with `make profile BIN_DIR=bin/phase33-stream`.
The deployed CLI SHA-256 is
`0a73f3c753843eb67ea7d966ba33a756ee14ecda80371c14c3f0122ebcdf8267`;
the daemon SHA-256 is
`211d77ca61951ec0d709faa9354630308dd44045b1b69b5bfd0a2d9627a19fbe`.
Previous binaries are retained under `bin/phase33-stream/rollback/`.
The ten-minute diagnostic artifacts are under
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-22-stream32-r1/`,
including source revision/diff and binary hashes. It uses 32 workers/RPCs,
96 GiB checker memory, 96 GiB scratch on the same NFS parent, reduced SlateDB-only
coverage, encryption auditing and crawl-debt details. No production caches were
dropped. The daemon restart resets its process-local caches, so this is not an
identical warm-cache control against the earlier runs.

The new path retains one pinned-transaction iterator per range, including the
lookahead record at item and byte boundaries. The daemon admits at most 32
streams, each with one queued response and bounded chunk construction. Responses
remain below 16 MiB including telemetry, with one retained lookahead record.
These are application-buffer bounds, not a combined RSS guarantee: SlateDB
iterator/cache allocations and transport buffers remain additional costs.
Receiver cancellation drops the iterator; delivery waits are bounded by the
transaction idle timeout and a range lasts at most one hour or the shorter
caller deadline. There is no reconnect/resume against a new transaction.

The Go client negotiates the capability, checks ordering, prefix coverage,
response limits and explicit completion, and cancels on consumer failure.
Older daemons retain the pinned unary path. Checker wrappers hold one RPC permit
through the whole range and exclude tuple consumption from database wait spans.
Session identity is validated after each completed range and before success.
Expired ordinary transaction reads now fail instead of refreshing an already
expired lease. The initial streaming patch required enough idle time for
non-database stages; the subsequent lease follow-up below adds renewal.

Progress and result resources expose records, protobuf bytes, chunks,
started/completed ranges, iterator setup/service nanoseconds, receive wait and
consumer time. Final results retain at most 256 first-byte partition summaries.
Receive wait includes server work and delivery; it is not pure network latency.
Existing backend metrics remain separate: per-scan table/object read attribution
and active continuation age are not implemented by this follow-up.

Local validation:

```text
cargo test --manifest-path vaulticdb/Cargo.toml --all-targets
cargo fmt --manifest-path vaulticdb/Cargo.toml --check
cargo build --manifest-path vaulticdb/Cargo.toml --bin vaulticdb
go test ./internal/index/daemon -count=1
go test ./internal/index/maintenance ./cmd/vaultic/indexcmd
go test -race ./internal/index/daemon ./internal/index/maintenance ./cmd/vaultic/indexcmd \
  -run 'TestReadSession|TestScanStream|TestScanRange|TestLoadSlateDBLocations|TestCheck|TestIndexCheck' -count=1
```

The native all-targets run passes 307 tests. Owner suites and focused race checks
pass. Fixtures cover persistent item/byte boundaries, snapshot parity, one
iterator setup per range, stream capacity, expiry/rollback, cancellation cleanup,
malformed/truncated responses, legacy fallback, and wrapper admission cleanup.
The CI Clippy command remains blocked by unchanged `double_parens`,
`obfuscated_if_else`, `collapsible_if`, `unnecessary_lazy_evaluations`, and RADOS
`dead_code` warnings. No unrelated lint fixes are included.

### Streaming diagnostic outcome

The run was interrupted before its configured ten-minute cap. It exited 130
with `context canceled`, not timeout status 124, after 8m37.56s including cleanup.
The captured artifacts alone did not establish the source of cancellation. A
subsequent forced-spill regression reproduced self-cancellation during spool
adoption after successful `errgroup.Wait()`; see the follow-up below. The last
periodic progress was at 8m25s; no completed-check verdict was produced.

Encryption audit completed at about 1m04s. By the 8m20s sample, all 256 ranges
had completed, delivering 376,346,710 blob records in 37,769 chunks and
35,556,199,869 protobuf bytes. These are blob-key records, not the imported
location/multiplicity count of 379,934,385; their different cardinalities alone
do not imply missing data. The checker still reported `slatedb_scan` after range
completion: spool adoption/finalization precedes `catalog_join`, which was not
reached. The entire scan stage therefore remains incomplete.

| Metric | Interrupted stream32 r1 |
|---|---:|
| CLI user / system CPU | 1,674.24 s / 185.91 s |
| CLI peak RSS | 99,071,456 KiB (94.5 GiB) |
| Major faults / swap | 0 / 0 |
| Encrypted scratch peak | 59,103,515,330 B (55.0 GiB) |
| Daemon user + system CPU | 6,536.77 s |
| Daemon logical / physical reads | 2,625,923,374,777 B / 9,428,381,696 B |
| Main object-store GET attempts | 9,699,631 |
| Main object-store GET-body bytes | 43,026,979,884 B |
| Host NFSv3 operations / getattr | 11,931,784 / 9,933,334 |
| Host NFSv3 reads / writes | 146,716 / 860,738 |
| Iterator setup / service | 12.234 s / 12,507.482 worker-s |
| Client receive / consume | 11,539.735 s / 1,993.562 s |

Worker/service/receive times overlap and must not be added into wall time.
Backend body bytes and process logical reads measure different boundaries.
NFS counters include unrelated host activity. The old buffered32 run accumulated
2,490,400,225,853 logical read bytes and 7,870,426 host `getattr` calls over its
longer timeout run, so this experiment does not establish reduced read
amplification or a matched end-to-end speedup. Stream delivery completion is
new progress, but it is not complete scan-stage or full-check acceptance.

After cancellation, the service remained read-write at epoch 38 with zero active
transactions/write intents; the diagnostic process exited and the configured
scratch parent had no remaining session directories. The artifact manifest
verified successfully. Rollback binaries remain available; the streaming
candidate remains deployed.

### Finalization and lease follow-up

Commit `e06a8ffd6` records the streaming implementation and r1 results. The next
patch reproduces the finalization failure using three records in one partition
with a two-record memory buffer: the remaining spilled record must be flushed
during adoption. `errgroup.Wait()` cancels its context even when every worker
succeeds. Partition spools retained that context, so encrypted `writeRun` failed
with `context canceled`. The fixture fails before the fix and passes afterward.

After workers join successfully, adoption now uses the parent check context.
An explicit cancellation at the adoption boundary still fails, proving the fix
does not detach from caller cancellation. Progress reports `slatedb_finalize`
separately from range delivery and `catalog_join`.

Read sessions now renew by validating the pinned sequence and generation on a
bounded background request. New daemons advertise transaction idle milliseconds
in `BeginResponse`; the interval is one-third of that timeout, capped at 30s,
with a request timeout at most 10s and no longer than the interval. Older daemons
use a 1s interval and a 10s request timeout; unknown custom lease settings are not
guaranteed to survive, and renewal failure fails incomplete. No expired lease
is revived and no replacement transaction is opened. The CLI runs the checker
with the failure-aware session context; close cancels and joins the renewal
worker before rollback. Virtual-time tests cover idle work, bounded stalled
renewals, sticky errors, and cancellation cleanup. Real daemon tests cover timeout
advertisement and session close.

Renewal runs at most one control request at a time outside the checker's scan
RPC semaphore, so the configured 32-RPC scan budget is not a strict total RPC
cap: up to one additional renewal request can be active. Reserving this control
capacity prevents long-lived range streams from starving lease maintenance.

The affected Go owner suites, full owner race suites and 62 native storage tests
pass. A later parallel native all-targets run failed two existing tests because
`clean_incomplete_memory_wal_rebuild_does_not_handoff` consumed the global
`InventoryWal` failpoint intended for
`dedicated_wal_inventory_failure_precedes_cache_startup`. A focused serial rerun
of the daemon target passed all 190 tests, including both failures:

```text
cargo test --manifest-path vaulticdb/Cargo.toml --bin vaulticdb -- --test-threads=1
```

The broader serial retry passed all 102 library tests before being deliberately
stopped during unrelated broker tests; it is not a completed all-targets pass.
No unrelated test changes are made.
The native timeout field was validated locally but does not require another
production restart: diagnostic r2 uses the new profile CLI against the existing
epoch-38 streaming daemon via the legacy timeout-advertisement fallback.

The repeated ten-minute diagnostic is captured under
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-22-stream32-finalize-r2/`.
It preserves 32 workers/RPCs, 96 GiB memory/scratch and the same scratch parent,
with no cache drops, daemon replacement or restart. The CLI SHA-256 is
`8c86b3b768cfd8f30e234c829334b7410cda82f32e52ae6a8733d9cdc8be22b7`;
the daemon hash is unchanged from r1. Local regression workloads overlapped this
run, so timing is diagnostic rather than an isolated performance comparison.

R2 completed the audit at 1m35s and entered `slatedb_finalize` at 9m03s after
delivering the same 376,346,710 records in 37,769 chunks across all 256 ranges.
It remained in finalization until the actual ten-minute timeout (wrapper exit
124), then returned the expected canceled outcome and cleaned up. Wall time
including cleanup was 10m19.47s. This confirms the premature self-cancellation
is fixed, not that the complete scan stage or check finishes within ten minutes.

CLI user/system CPU was 1,662.47/210.22 seconds; peak RSS was 102,249,352 KiB
(97.5 GiB), with no swap. Scratch grew from 59,055,812,608 bytes at finalization
entry to 66,767,726,000 bytes (62.2 GiB) at cancellation, with zero merge passes.
Daemon CPU totaled 6,515.28 seconds. Iterator setup was 11.704 seconds;
service/receive/consume worker times were 12,787.753/11,960.122/1,997.716 seconds.
These overlapping times must not be summed into wall time.

The artifact manifest verified. The writer stayed read-write at epoch 38 with
zero active transactions/intents, no diagnostic process remained, and scratch
cleanup left no session directories. Only the diagnostic CLI changed; production
service binaries remain the r1 deployment.

### Bounded parallel finalization

The follow-up to `d4d5d223e` flushes pending partition buffers in a separate
`errgroup` capped by `--check-workers`, after all scan workers join and before
serial ownership transfer. Each worker finishes one partition's location and
pack buffers sequentially, so at most one run writer per worker is active.
Existing partition buffers, shared scratch reservations and run-name allocation
are reused; no additional tuple buffers or per-partition memory allowances are
introduced. Each active writer retains the existing 1 MiB output buffer and
encryption allocations, outside the tuple-memory budget.

Workers use the finalization group's cancellation context. All workers join on
success or failure before cleanup; successful ownership transfer restores the
parent context and checks caller cancellation. The `slatedb_finalize` stage
remains separate, allowing measurement of this tail independently of delivery.

Focused virtual-time regressions cover one and four simultaneous finalizers,
exact sorted location/pack results, memory bounds, cancellation during active
writes, scratch exhaustion, joined workers, released reservations and scratch
directory cleanup. The existing spill and scan regressions pass, as do:

```text
go test -race ./internal/index/maintenance ./cmd/vaultic/indexcmd -count=1
make vaultic BIN_DIR=bin/phase33-parallel-finalize VAULTIC_BUILD_TAGS=profile
```

Diagnostic r3 uses that separate CLI against the unchanged epoch-38 daemon,
with the same 32-worker/RPC, 96 GiB memory/scratch and ten-minute limits.
Tests and builds completed before measurement; no local test/build workloads
overlap this run, and there is no cache drop, daemon replacement or restart.
Artifacts are under
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-22-stream32-parallel-finalize-r3/`.
The CLI SHA-256 is
`711d95b7c08bc7192c6725596f61b6d175c70740fcefb632b49efbb1edb7e248`;
the daemon hash is unchanged from r1. Starting host load averages were
2.00/3.64/8.91. Cache state still differs from earlier attempts; a single bounded
repeat is not a matched end-to-end speedup measurement.

R3 entered `slatedb_scan` at the 1m55s sample and remained there until the
ten-minute timeout (wrapper exit 124). It delivered 375,902,743 records in
37,720 chunks, totaling 35,514,250,080 protobuf bytes; 250 of 256 ranges completed.
Neither `slatedb_finalize` nor `catalog_join` was reached, so production behavior
and performance of the new finalizer pool remain unverified. The slower scan
cannot be attributed to a finalization path that did not execute.

| Metric | Capped parallel-finalize r3 |
|---|---:|
| Wall time including cleanup | 10m19.20s |
| CLI user / system CPU | 1,440.90 s / 174.49 s |
| CLI peak RSS | 101,242,676 KiB (96.6 GiB) |
| Swap | 0 |
| Encrypted scratch peak | 59,055,812,608 B (55.0 GiB) |
| Merge passes | 0 |
| Daemon CPU | 6,433.03 s |
| Iterator setup / service | 13.301 s / 14,120.982 worker-s |
| Client receive / consume | 13,409.859 s / 1,752.187 s |

The artifact manifest verified. Scratch cleanup left no session directories,
only the daemon process remained, and the writer stayed read-write at epoch 38
with zero active transactions and write intents. Production service binaries
were not replaced. Local exact-result, concurrency and race tests pass; this
production run is incomplete evidence, not finalization or full-check acceptance.

Next: obtain stage-scoped finalization timing without spending most of each
feedback window rescanning the database, and investigate scan/audit variability.
Then assess finalization completion, profile catalog joins and later validators,
and run full differential coverage. The ten-minute diagnostic limit remains in
force; full differential success and the representative acceptance matrix remain
open.

### Attribution Gate

The optimization objective is minimum complete-check runtime and maximum useful
throughput, not minimum CPU or RAM consumption. Additional resource use is
acceptable when it improves that objective within explicit safety limits.
Further tuning requires an attributable bottleneck hypothesis, a distinguishing
measurement and a matched correctness/throughput comparison.

R1-r3 captured aggregate resource use but did not capture production CPU stacks
or persist checker dependency-wait snapshots. Those runs do not establish which
functions consumed CPU or distinguish storage latency from scheduling and
backpressure inside iterator service. Low host I/O-wait is not evidence that
application storage waits are absent. Service, receive and consume timers are
overlapping elapsed worker times, not CPU-time partitions.

The attribution follow-up adds opt-in `index check --monitor-export-jsonl` using
the existing bounded asynchronous exporter: five-second checker and daemon/cache
snapshots, a four-snapshot queue, two-second collection/drain timeouts and
explicit dropped/failure counters. Initial spill runs expose separate sort,
encode/write, buffered flush and `fsync` elapsed histograms. Measurements occur
at run boundaries, not per record. Encode/write includes encryption, allocation,
buffered writes and any full-buffer kernel writes; CPU profiles distinguish
those costs. These stage timers currently cover initial spill runs, not the
later merge writer. `slatedb_finalize` maps to the finalization telemetry phase.

The r4 attribution diagnostic captures a Go CPU profile, a three-second scan
execution trace, five-second goroutine and Linux thread/wait-channel samples,
and timestamped daemon `perf` CPU call stacks with context-switch records.
The unchanged deployed daemon has symbols and frame pointers. Profiling is
local, artifacts are protected, no service restart is required, and the check
retains the ten-minute cap. Profiling overhead means this is an attribution
run, not a clean performance comparison. Tests/builds finish before measurement.

Collection uses the profile-build CLI's existing `--cpu-profile DIR` and
`--listen-profile 127.0.0.1:PORT`, plus the checker's `--monitor-export-jsonl FILE`.
The trace comes from `/debug/pprof/trace?seconds=3`, goroutine stacks from
`/debug/pprof/goroutine?debug=1`, and daemon sampling uses
`perf record -e cpu-clock -F 49 --call-graph fp --switch-events --timestamp
--clockid mono -p PID`. All collectors stop before artifact checksums are made.
Context-switch capture can produce multi-GiB artifacts; retain it for attribution
runs, not ordinary throughput comparisons. The monitor performs serial status
requests outside scan admission, adding at most one concurrent control request
beside the existing single renewal request. Export drops/failures and incomplete
profiles must be reported, not silently treated as zero waits.

For each experiment, report stage wall time and records/s first, then process
CPU-seconds, peak RSS, scratch and backend bytes/record. Attribute CPU using flat
and cumulative stack profiles; attribute waits using their owner, downstream
resource, contention count and elapsed distribution. Do not add nested profile
percentages or overlapping worker spans, and do not count intentionally idle
maintenance threads as bottlenecks. Use matched unprofiled repeats to accept a
performance change after an attribution run identifies the controlling work.

The raw r4 artifacts are under
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-22-stream32-attribution-r4/`.
The measured CLI SHA-256 is
`ee8b0cf16d03f36317afb753fa2ddd69f105d2ef83b9083c3d3e6f300e150cf6`.
The audit ended at 52s; timeout 124 stopped `slatedb_scan` with 376,304,659 records,
37,762 chunks and 252/256 ranges complete. Finalization was not reached. Check
wall time including cleanup was 10m18.67s, CLI CPU was 1,951.47s user plus
191.90s system, peak RSS was 103,139,192 KiB (98.4 GiB), scratch was 55.0 GiB,
and swap was zero. Daemon status deltas include the subsequent perf drain:
6,848.29 CPU-seconds over 702.067s, not just checker wall time.

The raw manifest verified; the nested CLI CPU profile and derived reports have
supplemental checksums under `analysis/`. The writer stayed read-write at epoch
38 with zero transactions/intents; only the daemon remained and scratch was
empty. The service binaries are unchanged.

#### Measured Wait Owners

- 2,008 of 2,533 sampled scan-worker observations (79.3%) were in gRPC receive;
  369 were in spill writes, 34 in `fsync`, 34 in sorting, and 88 elsewhere.
  These are sampled worker states, not exact wall-time shares.
- The three-second trace at 20:23:34-37 UTC shows 77.02 worker-seconds blocked in
  scan receive, about 5.02ms of scan-worker scheduling delay, and 2.62s in the
  transport reader's network poll. This supports waiting for responses rather
  than client CPU scheduling as the controlling delay in that window.
- RPC admission had zero contentions and 442us total across 257 admissions.
  Recorded daemon transaction-map/slot waits totaled 386us/0.528s respectively.
  These metrics do not measure every iterator-internal lock.
- Database GET request service totaled 13,946.089 worker-seconds over 9,699,251
  calls. It includes local backend work and async scheduling, not pure NFS wait.
  The separately recorded body-stream time was 24.750s; these boundaries must
  not be interpreted as a complete I/O-time partition.
- Across 1,024 initial spill runs, sorting totaled 166.536 worker-seconds,
  encode/write 1,597.465s, buffered flush 0.212s and `fsync` 232.419s. These
  overlapping worker spans are not additive to overall runtime.

#### Measured CPU Owners

The Go CPU profile has 2,124.61 sampled CPU-seconds. `ActionGuard.Processed`
accounts for 53.57% cumulatively beneath per-record scratch accounting;
atomic compare-and-swap alone is 25.93% flat. This is a demonstrated telemetry
contention cost, not encryption or useful record validation. `writeRun` is
70.25% cumulative and includes that accounting cost; do not add those shares.
Allocation (`mallocgc`) is 9.46% cumulative and AES-GCM encryption 3.25% flat.

The daemon profile contains 331,192 CPU samples and reports zero lost samples.
It reports 75 out-of-order context-switch events, so no exact off-CPU duration
is inferred. Flat CPU includes 17.48% buffer zeroing (17.44% traced to
`object_store::local::read_range`), 14.55% AES-GCM (callers resolve to
`EncryptedObjectStore::decrypt_chunks_sync`), 6.90% kernel copying, and 4.13%
user-space copying. These establish allocation/copy/decryption costs on the
read path; they do not prove that increasing workers or cache alone will help.

#### Post-Run Corrections

Scratch accounting now sits below the existing 1 MiB buffer, so shared counters
are updated per underlying write rather than twice per tuple. The encrypted
format and successful byte totals are unchanged; canceled bytes now count only
bytes accepted by the underlying writer, not unflushed buffer contents. Exact
byte/short-write, cancellation, corruption, spill and cleanup tests pass.

The accounting-only benchmark, 32-way concurrency and three one-second repeats,
measured 794,980/810,203/817,690 ns per fixed batch at the old accounting boundary
versus 6,525/6,450/6,453 ns buffered. This excludes encryption and disk I/O and is
not an end-to-end speedup. A follow-up local CPU profile is dominated by buffer
writes/copies, with shared telemetry updates absent from the leading costs.
Production runtime improvement still needs a matched capped repeat.

R4 preserved 120 valid monitor snapshots; one additional snapshot was rejected
because daemon histogram count and buckets were sampled inconsistently under
concurrent updates. The adapter now marks only that histogram unavailable rather
than dropping the entire snapshot or fabricating coherent values. Exporter
queue drops/failures were zero; the rejected collection is a separate gap.

Future Go profiles carry `check_stage` labels inherited by stage workers and
restored after the check. These labels, histogram handling and batched accounting
are post-r4 changes, not part of its measured binary. Final race suites pass:

```text
go test -race ./internal/index/maintenance ./internal/telemetry ./cmd/vaultic/indexcmd -count=1
```

Next hypotheses: confirm the accounting correction in a matched production
repeat, then investigate unnecessary local read-range initialization/copying and
repeated encrypted-range work. Use available CPU/RAM when the measured working
set or concurrency bottleneck justifies it; do not trade away verification or
authenticated encryption for throughput.

Remaining attribution boundaries must stay explicit: Linux thread sleep is not
equivalent to a parked Rust async task; CPU samples cannot quantify async wait
duration; the existing server iterator timer excludes output-channel reservation
wait. A daemon-side async wait breakdown is still needed if the captured CPU,
cache and client-wait evidence cannot discriminate the next hypothesis.

### Accounting Comparison Follow-Up

The available storage choices are native RADOS and HDD-backed NFS, not local
disk. The current checker scratch writer requires filesystem operations; RADOS
is not a drop-in value for `--check-temp-dir`. Prioritize avoiding unnecessary
spill and repeated reads using RAM before considering a new scratch backend.
Do not assume historical SSD test targets remain available.

Commit `80ab5b4e6` preserves bounded parallel finalization, attribution and the
post-r4 accounting correction. Its separate profile-tag CLI has SHA-256
`f5d89abe485b9bbf0fbc9935399cb0a7afa487a67f2399d520664eb6146eddde`.
The retained pre-fix r4 CLI is the comparison control. Both runs keep the daemon,
96 GiB checker budget, 96 GiB NFS scratch limit, 32 workers/RPCs, encryption and
SlateDB-only coverage unchanged. Each is capped at ten minutes with TERM and a
45-second kill grace. Five-second JSONL, process and host samples remain enabled;
CPU profiling, execution traces and perf context-switch capture are disabled.
No cache drops, daemon restart, builds or tests overlap the runs. Sequential
cache warming and backend variability remain confounders; a single pair cannot
establish a repeatable end-to-end speedup. The candidate harness mirrors existing
progress lines through `tee`; the control redirects them only to its log. Both
retain exact harness copies. The workspace revision in the control artifacts is
the harness workspace, not the retained pre-fix binary's source revision; use its
binary hash and the r4 source patch for measured-code provenance.

#### R5/R6 Results

Artifacts are under the same `db.test` root, with names
`phase33-production-2026-09-22-stream32-accounting-control-r5` and
`phase33-production-2026-09-22-stream32-accounting-batched-r6`.
Both manifests verified. Each hit timeout 124, returned incomplete SlateDB-only
coverage, cleaned scratch, and left the unchanged writer healthy at epoch 38
with zero transactions/intents. Neither run is full-check acceptance.

| Measurement | Pre-fix r5 | Batched r6 |
| --- | ---: | ---: |
| Encryption audit | 51s | 51s |
| Location scan (stage duration) | 547s | 454s |
| Location records / completed ranges | 376,346,710 / 256 | 376,346,710 / 256 |
| Finalization start (elapsed) | 9m58s | 8m25s |
| Catalog join start (elapsed) | Not reached | 8m57s |
| CLI user + system CPU seconds | 1,709.48 | 1,362.34 |
| Wall time including cleanup | 10m21.45s | 10m23.49s |
| Peak RSS (KiB) | 101,765,404 | 115,474,960 |
| Scratch peak (bytes) | 60,589,131,042 | 85,342,828,262 |
| Merge passes | 0 | 4 |
| Daemon logical read bytes | 2,625,980,781,200 | 2,625,996,773,108 |
| Daemon CPU seconds | 6,405.55 | 6,451.37 |
| Monitor snapshots | 121 | 121 |
| Swaps | 0 | 0 |

The observed scan stage is 17.0% shorter (20.5% greater records/s), and total CLI
CPU is 20.3% lower despite the candidate reaching more work. The candidate
finishes bounded parallel finalization in 32s. Greater RSS/scratch and four
merges are associated with reaching later stages, not equal-work resource
regressions. Complete-check runtime remains unknown. Both binaries already
contain parallel finalization; the pair compares the accounting correction plus
the post-r4 histogram handling and profile-label changes, not an isolated
single-instruction A/B. Retain the result as provisional pending repetitions.

GET counts remain approximately 9.7 million and daemon logical reads remain
2.63 TB. Accounting reduces client cost without fixing read amplification.
The next backend candidate therefore targets fetch granularity rather than
increasing RPC admission or moving scratch to an unavailable local disk.

#### Next Read-Path Hypothesis

The retained streaming iterator calls `scan_prefix_transaction`, which uses
SlateDB's default `ScanOptions`: `read_ahead_bytes=1` (one block per fetch),
`max_fetch_tasks=1`, and `cache_blocks=false`. The encrypted store rounds each
requested plaintext range to complete authenticated chunks (256 KiB by default),
decrypts them, then returns the requested slice. Adjacent small-block requests
can therefore repeatedly fetch and decrypt overlapping chunks. The local-file
object-store implementation also zero-initializes the expanded read buffer.

R4 recorded 9,699,251 database GETs and 43,073,470,791 plaintext GET-body bytes;
daemon `/proc` logical read bytes increased by 2,625,792,550,135. The roughly
61-fold aggregate ratio supports this hypothesis but is not an exact per-read
amplification measurement: the counters have different boundaries, the process
window includes perf drain, and logical reads are not physical NFS traffic.

The next narrow candidate implements 1 MiB read-ahead for retained streaming iterators
only, retaining one fetch task per SST and the 32-stream limit. SlateDB bounds
fetch block ranges to the iterator range; this does not remove authentication,
change snapshots, or require new plaintext disk caching. Memory scales with
active SST iterators, not just stream count, so measure daemon RSS rather than
claiming a fixed 32 MiB total. Verify exact ordered keys/values, exclusive cursor
and prefix boundaries on persisted SSTs, fewer backend GETs, and existing stream
expiry/cancellation/cleanup behavior before any production deployment.

Local implementation and validation completed before the approved deployment
recorded below. Unary scans retain default options. The persisted-SST regression
uses disabled block caching and checks full keys/values, neighboring prefixes,
exact and between-key exclusive cursors, empty results and a pinned snapshot
excluding later writes. Streaming performs over eight times fewer GETs than
default one-block fetching in this fixture. This is a backend-call regression,
not a production runtime or encrypted-byte reduction claim. Existing stream
snapshot/expiry/admission/cleanup tests pass, as do all 63 storage tests:

```text
cargo test --manifest-path vaulticdb/Cargo.toml --bin vaulticdb storage::tests:: -- --test-threads=1
```

Additional RAM remains a separate experiment: the checker currently divides its
budget into four spool shares even for SlateDB-only coverage. Size it from tuple
footprints and peak later-stage memory rather than assuming the entire configured
budget is available to each ordering.

#### R7 Read-Ahead Deployment And Result

With explicit deployment/restart approval, the static frame-pointer release build
passed and only the daemon executable was replaced. The previous daemon remains
at `bin/phase33-read-ahead/rollback/vaulticdb`. The deployed SHA-256 is
`2963b4456fe2cd7f1738f19635745060f5f41df307c496a10e54a917f20a4305`;
PID 100168 reopened read-write at epoch 39. Configuration, service CLI and storage
were unchanged. Deployment provenance is under
`db.test/phase33-read-ahead-deployment-2026-09-22`; diagnostic artifacts are under
`db.test/phase33-production-2026-09-22-stream32-read-ahead-r7`.

The same r6 accounting CLI and resource limits produced:

| Measurement | r6 default reads | r7 1 MiB read-ahead |
| --- | ---: | ---: |
| Encryption audit | 51s | 49s |
| Location scan duration | 454s | 117s |
| Location records / completed ranges | 376,346,710 / 256 | 376,346,710 / 256 |
| Finalization duration | 32s | 30s |
| Catalog join start (elapsed) | 8m57s | 3m16s |
| Database GET calls (monitor delta) | 9,699,968 | 41,784 |
| Plaintext GET-body bytes | 43,094,546,875 | 42,809,360,880 |
| Daemon logical read bytes | 2,625,996,773,108 | 93,741,772,175 |
| Daemon CPU seconds | 6,451.37 | 1,580.19 |
| CLI CPU seconds | 1,362.34 | 1,842.18 |
| CLI peak RSS (KiB) | 115,474,960 | 115,470,136 |
| Scratch peak (bytes) | 85,342,828,262 | 85,343,106,436 |
| Merge count | 4 | 21 |

Scan duration fell 74.2% (3.88 times throughput), while logical read bytes fell
96.4%. The aggregate logical/plaintext read ratio is about 2.19 rather than 61;
it is still not an exact scan-only or physical NFS amplification measure. Daemon
high-water RSS since restart was 1,315,328 KiB, ending at 528,488 KiB. The restart
reset daemon caches, and r7 performs more later-stage work, so this is strong
mechanism evidence but not a repeated matched complete-check speedup result.

R7 hit timeout 124 in `catalog_join`, with wall time 10m06.76s including cleanup,
zero swaps and 121 monitor snapshots. Both manifests verified; scratch is empty,
only the daemon remains, and the writer is healthy with zero transactions/intents.
Coverage is still incomplete SlateDB-only, not a full differential clean verdict.

The user's observation of sustained approximately 100% CLI CPU is consistent
with the newly exposed serial merge path: `checkPackCatalog` constructs a spool
iterator, whose `seal()` synchronously merges each fan-in group in sequence.
Each `mergeRuns()` executes record decryption, heap selection and encryption in
one goroutine. R7 records 21 merge calls during its longer catalog-stage window;
there is no r7 CPU profile to assign exact function percentages. The progress
stage includes preparatory merging, not just the final catalog comparison.

Next target: bounded parallel independent merge groups, initially 2-4 workers,
with explicit output-space admission, joined cancellation/cleanup and unchanged
ordered results. Each 32-input group already needs about 32 MiB of input buffers
plus its output buffer, and retains inputs until output publication. With about
79.5 GiB scratch occupied under a 96 GiB limit, launching all groups concurrently
can exhaust scratch. Measure group CPU/waits and HDD-NFS throughput before
increasing concurrency. The eventual final ordered merge/pack aggregation is
also serial and may need a separately validated partitioned design later.

### Bounded Parallel Merge Diagnostic (r8)

The checker now admits up to four independent scratch merge groups per wave,
bounded by the spool's configured buffer budget and shared scratch headroom.
Admission reserves the worst-case output size before starting a group; successful
publication releases unused reservation and removes inputs only after workers
join. Failed waves discard outputs while retaining input ownership for cleanup.
This conservative reservation can reject a merge whose deduplicated output would
fit but whose maximum output would not. The buffer bound is not a total RSS bound.
Other spool callers retain serial merging by default.

The new `check_scratch_merge_dependency_*` family records group outcomes and
elapsed service time, including iterator setup and output flush/sync. It does not
measure CPU time or separate admission, filesystem and crypto waits. Reservation
accounting replaces per-record reservation locking in merge output writers.

Deterministic virtual-time tests cover four simultaneous outputs, scratch- and
buffer-limited single-worker admission, deduplication and multiset ordering,
cancellation, corrupt input, joined workers and exact reservation cleanup.
The maintenance, telemetry and index-command race suites pass. An initial test
run inherited deployment `umask 077`, causing permission-fixture failures; all
three suites passed with `umask 022`. Editor diagnostics and `git diff --check`
also pass.

R8 used the unchanged r7 daemon, the same NFS paths, 96 GiB memory/scratch limits,
32 scan workers/RPCs and ten-minute cap. No build or test overlapped measurement.
The separate CLI at `bin/phase33-parallel-merge/linux-amd64/vaultic` has SHA-256
`8a04b4dde3ec3fd4331086f311932c7321d65b907d0eab4ca9ddcd45666e3fec`;
service binaries were not replaced. Artifacts are under
`db.test/phase33-production-2026-09-22-stream32-parallel-merge-r8`.

| Measurement | r7 serial groups | r8 bounded parallel groups |
| --- | ---: | ---: |
| Encryption audit | 49s | 48s |
| Location scan | 117s | 115s |
| Finalization | 30s | 28s |
| Catalog stage starts | 3m16s | 3m11s |
| Merge calls started by cap | 21 | 24 |
| CLI CPU seconds | 1,842.18 | 2,183.94 |
| CLI peak RSS, KiB | 115,470,136 | 115,318,008 |
| Scratch accounting peak, bytes | 85,343,106,436 | 90,567,039,078 |

All 24 r8 merge groups succeeded by the 6m05s monitor snapshot, with 627.269
aggregate group-service seconds and a maximum group duration of 38.378s. This
demonstrates overlapping production group service, not an exact CPU-core count.
Scratch peak now includes up-front output reservations, so it is not directly
comparable to r7's incrementally charged bytes or physical NFS allocation.
All 376,346,710 records and 256 ranges completed. The remaining catalog work
continued until timeout 124; total wall time including cleanup was 10m05.29s.
There were zero swaps and 121 monitor snapshots. The artifact manifest verified,
scratch was empty, and the unchanged writer remained healthy at epoch 39 with
zero active transactions/intents.

This establishes completion of merge preparation within the cap, not a measured
complete-check speedup. Both runs remain incomplete SlateDB-only diagnostics,
and sequential cache effects are not controlled. The remaining serial ordered
iterator and per-pack aggregation are the next profiling targets; r8 has no CPU
profile to assign their exact costs or distinguish them from catalog scan waits.

### Catalog Attribution and Iterator Follow-Up (r9-r10)

The read-ahead and bounded-merge work above was committed as `8b65896d7` after
fresh affected Go race suites passed. R9 retained the r8 CLI and daemon, adding
loopback-only pprof, five-second CLI thread/goroutine samples, a 30-second CPU
profile after 24 successful merges, and a subsequent three-second Go trace.
Artifacts are under `db.test/phase33-production-2026-09-22-stream32-catalog-profile-r9`;
derived reports have a separate `analysis/SHA256SUMS`, preserving the raw manifest.

The CPU window at 22:11:08 UTC captured 29.11 CPU seconds in 30 seconds:

| CPU path | Share of sampled CPU |
| --- | ---: |
| Pack contribution iterator, cumulative | 77.74% |
| Ordered location iterator, cumulative | 76.57% |
| Encrypted run reader, cumulative | 45.76% |
| Heap pop plus push, cumulative | 27.13% |
| AES-GCM Open, cumulative | 25.21% |
| Allocator, cumulative | 11.61% |

These overlapping percentages must not be added. The trace attributed 602.52 ms
to scratch read syscalls under the catalog iterator, 0.329 ms to network-read
blocking and 13.141 ms to scheduler delay across goroutines. This short window
supports CPU work as the primary target with material filesystem time; it is not
a complete accounting of every async/network wait. R9 timed out in catalog work,
used 2,141.01 CLI CPU seconds and 115,371,828 KiB peak RSS, and took 10m08.46s
including cleanup. The sampler reported no errors; writer health, scratch cleanup
and both artifact manifests verified. Profiling overhead precludes treating r9
as a matched lightweight timing control.

The next candidate changes only the shared scratch iterator hot path:

- Replace per-tuple heap pop/push with root replacement and one sift-down; retain
  normal pop when the contributing reader reaches EOF.
- Give each run reader private reusable length, nonce and ciphertext buffers;
  authenticate/decrypt in place, then decode into a tuple whose fields are copied
  by value. Record format, nonce sequence, authentication and length checks remain
  unchanged. No per-record allocation is needed on the successful read path.

Three benchmark repetitions measured 32-run in-memory merge time falling from
7.425-7.636 ms to 6.177-6.239 ms per 32,768 tuples, about 17%. Reading 4,096
encrypted records fell from 1.862-1.871 ms to 1.092-1.143 ms; allocations fell
from 16,395 to 11, with bytes allocated falling from about 1.968 MB to 1.050 MB
(including the reader's 1 MiB I/O buffer). These isolate mechanisms, not complete
checker speedup. Existing ordering, deduplication, multiset, corruption,
truncation, cancellation and parallel cleanup tests pass, along with full affected
maintenance/telemetry/index-command race suites and editor diagnostics.

R10 uses this candidate with lightweight monitoring, no profiling, unchanged
daemon and the same NFS paths, 96 GiB limits, 32 workers and ten-minute cap.
No builds or tests overlapped. Candidate CLI
`bin/phase33-iterator/linux-amd64/vaultic` SHA-256 is
`c6d98d7bd6fa6b0de9e09724458b34fecc0fffc28383b07f5587e118f46effb6`;
artifacts are under `db.test/phase33-production-2026-09-22-stream32-iterator-r10`.

| Measurement | r8 baseline | r10 iterator candidate |
| --- | ---: | ---: |
| Audit / scan / finalization | 48s / 115s / 28s | 52s / 119s / 33s |
| Catalog stage starts | 3m11s | 3m24s |
| First 24 merges successful by snapshot | 6m05s | 6m05s |
| Total merge calls at cancellation | 24 | 36 |
| Successful / canceled merge groups | 24 / 0 | 32 / 4 |
| CLI CPU seconds | 2,183.94 | 2,023.14 |
| Peak RSS, KiB | 115,318,008 | 115,413,100 |
| Scratch reservation peak, bytes | 90,567,039,078 | 90,567,039,078 |

R10 completed `checkPackCatalog` and entered the subsequent location-count spool
merge by the 9m25s snapshot, when successful groups first exceeded 24. The
`catalog_join` progress label covers both operations, so it does not expose this
boundary directly. The control remained in the catalog comparison at its cap.
R10 used 7.4% less CLI CPU despite reaching later work. Its 32 successful groups
accumulated 684.207 service seconds; four groups were canceled by the cap.
All 376,346,710 records and 256 scan ranges completed, but the later location
count did not finish and its result field remains zero, not a valid count.

Exit was 124; wall time including cleanup was 10m08.96s, with zero swaps and 121
monitor snapshots. Manifest and empty scratch checks passed; the writer remained
healthy at epoch 39 with zero active transactions/intents. Service binaries were
not changed. These results support the iterator optimization but do not establish
complete-check runtime, repeated performance acceptance or a full differential
clean verdict. The next target is the subsequent location-count pass and its
remaining serial merge, with clearer stage attribution before further parallelism.

### Avoid the Count-Only Location Spool (r11-r12)

The iterator follow-up above was committed as `cfc301939`, including the r9
attribution and r10 production evidence. Inspection of the next bottleneck found
that SlateDB-only checking builds a complete blob-ordered spool solely to count
distinct tuples after catalog comparison. Full differential checking still needs
that spool to compare legacy and authoritative locations.

The new candidate counts distinct locations within each blob during the existing
partition scan, only in SlateDB-only mode. It validates blob key kind, partition
prefix and strictly increasing IDs across callback/page boundaries; therefore
the same blob cannot contribute twice. Every location field participates in
per-blob deduplication. Single-location records need no deduplication map, and
multi-location maps live only for the current decoded record. Partition-local
counts avoid shared per-record atomics and are combined with overflow checks
only after successful scan, finalization and adoption. Errors do not publish a
partial count. The unused blob-ordered spool stays empty, eliminating its sort,
spill, encryption, merge and readback work. Pack-contribution tuples retain their
original multiset, including duplicates; full differential and legacy-only
counting retain their existing spool paths.

Tests compare the new count against the old spilled/deduplicated spool across
partitions, duplicates and differences in every location field, and require exact
pack-multiset equality. Separate cases cover empty/singleton scans, duplicate and
reversed IDs across callbacks, wrong prefixes/kinds, malformed values and
cancellation without partial publication. Existing concurrent scan/finalization
tests and all maintenance, telemetry and index-command race suites pass. Editor
diagnostics and diff checks are clean.

Both diagnostics used the unchanged epoch-39 daemon, the same HDD-NFS paths,
96 GiB memory/scratch limits, 32 scan workers/RPCs and ten-minute cap. There was
no profiling or overlapping build/test work. The separate CLI
`bin/phase33-count-only/linux-amd64/vaultic` has SHA-256
`7dd0d82ded80972b0437f87e679639a1e3f0108a8be93eddd20e89527d52b3ff`.
Artifacts are under `db.test/phase33-production-2026-09-22-stream32-count-only-r11`
and `db.test/phase33-production-2026-09-22-stream32-count-only-r12`.

| Measurement | r10 prior iterator | r11 count-only | r12 unchanged repeat |
| --- | ---: | ---: | ---: |
| Encryption audit | 52s | 219s | 52s |
| Location scan | 119s | 91s | 94s |
| Finalization | 33s | 15s | 16s |
| Catalog starts | 3m24s | 5m25s | 2m42s |
| Parallel validation starts | Not reached | Not reached | 7m42s |
| Exit / total wall including cleanup | 124 / 10m08.96s | 124 / 10m01.88s | 0 / 8m12.27s |
| CLI CPU seconds | 2,023.14 | 1,474.18 | 1,499.19 |
| Peak RSS, KiB | 115,413,100 | 57,757,532 | 57,886,232 |
| Scratch reservation peak, bytes | 90,567,039,078 | 48,774,247,512 | 48,774,247,512 |
| Merge groups started | 36 | 24 | 24 |

R11's unusually slow unchanged encryption audit consumed the saved time; it
timed out during catalog comparison and did not publish its computed location
count. R12 completed the reduced-coverage command successfully, reporting
379,934,385 distinct locations from 376,346,710 blob records across all 256
ranges. All 24 merges succeeded by the 4m55s snapshot, totaling 445.132 group
service seconds. Parallel validation took about 29s, reaching finalization at
8m11s. Relative to r10, r12's scan was 21% shorter and finalization 52% shorter;
CPU was 26% lower while completing later work. Peak RSS fell about 50%, and
scratch reservation peak fell 46%. These resource reductions are consequences
of eliminating unnecessary work, not resource-minimization goals.

Both manifests verified, scratch was empty and the writer remained healthy with
zero active transactions/intents. Neither run swapped. R11 exported 121 monitor
snapshots; r12 exported 100. Service binaries were not replaced.

R12 is the first completed SlateDB-only run in this sequence, not a full
differential clean verdict: `coverage.complete` remains false and legacy indexes,
legacy snapshots and export provenance are skipped. The result also reports
419,530 warnings, with pending-export, unknown-tier, retention-unknown and
usage-unaccounted counters each at 419,530; exit zero does not mean warning-free.
Sequential cache effects and r11's audit variability remain confounders, and no
complete baseline runtime exists for a total-runtime speedup percentage.

The remaining dominant interval is catalog preparation/comparison: about 2m13s
to complete the 24 merge groups, then approximately 2m47s for the ordered catalog
comparison and subsequent cleanup/aggregate checks. Parallel validation is much
smaller. Further optimization should target this measured interval while keeping
the full differential-check path covered; it should not infer performance from
raising scan concurrency or from the count-only shortcut alone.

### Compact Pack Contributions Before Merging (r13-r14)

The count-only optimization was committed as `c6deb2e05` with the r11/r12 evidence.
The next candidate removes redundant catalog work: the catalog consumes per-pack
count, payload sum, type flags and presence, but previously merged every blob
contribution individually before computing those fields.

An explicit pack-summary mode now compacts the authoritative pack-contribution
spool's sorted buffers and merge outputs by pack ID. Private temporary tuples
encode partial count, payload, type flags and presence using the existing
authenticated scratch format; they are not repository records. Duplicates add to
counts and payload rather than being discarded. Count and payload addition are
overflow-checked at every combining boundary, and type/presence flags are ORed.
The final iterator combines partial summaries using the same arithmetic. Ordinary
location spools and legacy contribution spools remain unchanged; adoption rejects
mismatched modes. The authoritative pack-summary path applies to both full and
SlateDB-only checking, independently of the count-only optimization.

Tests compare exact summaries with the original multiset path through memory-only
and repeated disk merge passes, including duplicate contributions, mixed types,
presence-only packs, reduced output size and reservation cleanup. Count and payload
overflow are rejected. A failed-write/retry test checks that already compacted
buffers cannot double-count on retry. All affected maintenance, telemetry and
index-command race suites pass, as do focused summary tests and editor diagnostics.

R13 and r14 use the same candidate binary, unchanged daemon, HDD-NFS paths,
96 GiB limits and 32 scan workers/RPCs, with lightweight telemetry and a ten-minute
cap. No tests or builds overlapped either run. Candidate
`bin/phase33-pack-summary/linux-amd64/vaultic` SHA-256 is
`e8eb0b690206f738c89d1d1b1d6ea9d1f9750294a6b97bceeb08ec5fca65eb9d`.
Artifacts are under `db.test/phase33-production-2026-09-22-stream32-pack-summary-r13`
and `db.test/phase33-production-2026-09-22-stream32-pack-summary-r14`.

| Measurement | r12 multiset baseline | r13 summaries | r14 unchanged repeat |
| --- | ---: | ---: | ---: |
| Encryption audit | 52s | 50s | 51s |
| Location scan | 94s | 85s | 84s |
| Finalization | 16s | 6s | 8s |
| Catalog interval | 300s | 61s | 61s |
| Parallel validation interval | 29s | 31s | 31s |
| Total wall including cleanup | 8m12.27s | 3m53.40s | 4m01.33s |
| CLI CPU seconds | 1,499.19 | 965.02 | 940.65 |
| Peak RSS, KiB | 57,886,232 | 55,545,860 | 53,129,728 |
| Scratch reservation peak, bytes | 48,774,247,512 | 15,657,805,264 | 15,657,805,264 |
| Successful merge groups | 24 | 24 | 24 |
| Aggregate merge service seconds | 445.132 | 78.871 | 71.569 |

Both candidates exited zero. Mean total runtime was 3m57.365s, 51.8% shorter
than r12; the catalog interval fell 79.7%. Mean CLI CPU fell 36.4%, and scratch
reservation peak fell 67.9%. R14 spent roughly six seconds after the finalization
progress transition, explaining much of its total-wall difference from r13.
Both scanned 376,346,710 records over 256 ranges and reported 379,934,385 distinct
locations. JSON results matched r12 exactly after excluding only `resources`
and `consistency` (resource telemetry and session consistency metadata).

Manifests verified, scratch cleanup succeeded, and the unchanged writer remained
healthy at epoch 39 with no active transactions/intents. There was no swap;
r13/r14 exported 48/49 monitor snapshots. No service binaries were replaced.

This is repeated completed-run evidence for the current SlateDB-only NFS workload,
not full differential or cross-backend acceptance. Sequential cache effects remain
uncontrolled. All 419,530 warnings and the related counters remain unchanged;
coverage is still explicitly incomplete. The scan is now the largest individual
stage at 84-85s, followed by catalog work at 61s and encryption audit at 50-51s.
Further optimization should reattribute these remaining intervals rather than
assuming the earlier per-record merge bottleneck still dominates.

### Bounded Scan-Time Pack Aggregation (r15-r16)

After commit `cbdacda92`, the next candidate aggregates pack contributions in
partition-local maps before appending partial summaries to the encrypted spool.
Half the pack-spool budget remains available for retained partition buffers;
the other half is divided among configured scan workers for active maps. Entry
admission uses a conservative 256-byte allowance, not an exact Go heap/RSS bound.
Small budgets retain the existing tuple path. At the entry limit, the map flushes
exact partial summaries to the existing spill-capable spool; no global per-record
lock is added. Count/payload overflow and cancellation are checked, and failed
flushes are sticky so retry cannot replay partially emitted contributions.

R15 removed scratch I/O but left many retained memory runs for the final ordered
heap. R16 adds a bounded reduction of those memory runs by pack ID before final
iteration. It uses only estimated unused spool headroom, leaves original runs
untouched on cancellation/error or when the map limit is reached, and replaces
them with one sorted summary run only after successful reduction. Disk runs
continue through the existing encrypted merge path. Both changes affect only
authoritative pack summaries, not full-check location comparison semantics.

Tests compare map limits 1, 3 and 128 with the original summary spool through
multiple flush/spill passes, including duplicate/mixed-type contributions. Tests
cover repeated empty flushes, count/payload overflow, cancellation, sticky scratch
failure and reservation cleanup. Memory reduction tests cover exact results,
successful compaction, no headroom, entry-limit fallback and cancellation without
mutating the retained runs. All affected maintenance, telemetry and index-command
race suites pass, along with editor diagnostics and diff checks.

Both diagnostics used the unchanged epoch-39 daemon, HDD-NFS paths, 96 GiB limits,
32 workers/RPCs, lightweight telemetry and ten-minute cap, without overlapping
builds/tests. R15 CLI `bin/phase33-scan-aggregate/linux-amd64/vaultic` SHA-256 is
`9d244f7420ed2e2b9e777720875becfec254e4fcbe3ed04d0f27364bd19fe2a4`.
R16 CLI `bin/phase33-scan-aggregate-reduce/linux-amd64/vaultic` SHA-256 is
`afd0df5f93deb1dd9681ab197d601cb0805605ac8d871ef724175f034dd7495c`.
Artifacts are under `db.test/phase33-production-2026-09-22-stream32-scan-aggregate-r15`
and `db.test/phase33-production-2026-09-22-stream32-scan-aggregate-reduce-r16`.

| Measurement | r13/r14 baseline | r15 aggregation | r16 plus memory reduction |
| --- | ---: | ---: | ---: |
| Audit | 50-51s | 51s | 51s |
| Scan | 84-85s | 83s | 82s |
| Finalization | 6-8s | 2s | 2s |
| Catalog interval | 61s | 73s | 67s |
| Parallel validation | 31s | 34s | 34s |
| Total wall including cleanup | 3m57.365s mean | 4m04.96s | 3m57.55s |
| CLI CPU seconds | 952.835 mean | 828.89 | 813.10 |
| Peak RSS, KiB | 53,129,728-55,545,860 | 14,428,988 | 14,431,300 |
| Scratch reservation peak, bytes | 15,657,805,264 | 0 | 0 |
| Disk merge groups | 24 | 0 | 0 |

Both exited zero and matched r14's logical JSON results exactly after excluding
only `resources` and `consistency`. Both scanned all 376,346,710 blob records in
256 ranges, counted 379,934,385 distinct locations, and retained 419,530 warnings.
Manifests verified, scratch was empty, no swap occurred, and writer health checks
showed zero active transactions/intents. No service binaries were replaced.

R16 reduces CPU about 14.7% versus the prior mean and peak RSS about 73%, while
eliminating scratch I/O for this workload. It does not establish lower runtime:
total wall is essentially unchanged and the catalog interval remains longer.
This is a scalability candidate, not a throughput win. It has one production
measurement in its final form; cache effects and host variability remain
uncontrolled. Full differential and cross-backend acceptance remain pending.
Before claiming further runtime gains, profile the changed catalog path and scan
waits; reduced allocation/I/O alone does not demonstrate a shorter critical path.

### Reattribute and Stream the Catalog (r17-r19)

Bounded aggregation was committed as `caecdd695` after fresh affected race suites
passed. R17 profiled the unchanged r16 CLI with stage-triggered 30-second CPU
windows and subsequent three-second traces for scan and catalog. The sampler no
longer waits for disk merges, which this workload no longer performs. Artifacts
are under `db.test/phase33-production-2026-09-22-stream32-current-profile-r17`;
CPU/wait reports have a separately verified `analysis/SHA256SUMS`.

The scan window captured 263.83 CPU seconds in 30 seconds. Pack-buffer insertion
accounted for 24.48% cumulative CPU, protobuf eager unmarshalling 24.69%, blob
record decoding 15.27% and key parsing 8.41%; these overlapping shares are not
additive. Its trace attributed 53.03 goroutine-seconds of synchronization delay
to `ReadSession.ScanRange` in the three-second window, consistent with substantial
receive-side waiting but not sufficient to identify the daemon's internal wait
owner. No new daemon CPU profile was collected.

The catalog CPU window captured 17.06 CPU seconds, 96.07% cumulatively in
`compactPackMemory`; map lookup accounted for 68.17%. The subsequent trace sampled
a different part of the stage: 1.50 seconds of blocking beneath unary `ScanPage`
calls, with only about 1.77 ms total syscall time. CPU compaction and paginated
remote reads are distinct costs, not percentages of the same wall-time window.
R17 completed in 3m40.36s with unchanged logical results, no sampler errors,
verified manifests/cleanup and healthy writer state. Profiling and sequential
cache variation make it attribution evidence rather than a performance control.

The next candidate switches the catalog's `p:` scan to the existing retained
streaming range API when no legacy contribution iterator is present. It reuses
the same record visitor, transaction, cancellation and validation paths and
retains unary fallback for stores without range support. When a legacy iterator
exists, pagination is retained: missing-pack callbacks can issue nested `Get`
requests, which could deadlock under a single RPC permit held by a range scan.
This avoids widening the change to full differential catalog reads.

Regression tests verify exact pagination/range results and aggregates, range
dispatch, malformed-record rejection, cancellation and unchanged legacy
pagination. All maintenance, telemetry and index-command race suites pass, as
do editor diagnostics and diff checks.

R18/r19 used the same candidate binary, unchanged daemon and HDD-NFS paths,
96 GiB limits, 32 workers/RPCs and lightweight monitoring under the ten-minute
cap, without overlapping builds/tests. Candidate
`bin/phase33-catalog-stream/linux-amd64/vaultic` SHA-256 is
`3113876a877b9dfc7e8dbc998bbcf30213f3706ea6817c9675b54817d3a7b108`.
Artifacts are under `db.test/phase33-production-2026-09-22-stream32-catalog-stream-r18`
and `db.test/phase33-production-2026-09-22-stream32-catalog-stream-r19`.

| Measurement | r16 unary catalog | r18 streaming | r19 unchanged repeat |
| --- | ---: | ---: | ---: |
| Encryption audit | 51s | 50s | 50s |
| Blob scan | 82s | 82s | 83s |
| Finalization | 2s | 2s | 2s |
| Catalog interval | 67s | 22s | 26s |
| Parallel validation | 34s | 31s | 32s |
| Total wall including cleanup | 3m57.55s | 3m09.56s | 3m13.56s |
| CLI CPU seconds | 813.10 | 807.40 | 803.87 |
| Peak RSS, KiB | 14,431,300 | 14,606,612 | 14,137,216 |

Both candidates exited zero, with zero scratch bytes, disk merges or swap.
Mean runtime was 3m11.56s, 19.4% shorter than r16, with catalog time averaging
24s instead of 67s. Logical JSON results matched r16 exactly after excluding
`resources` and `consistency`: 379,934,385 distinct locations and all 419,530
warnings remain unchanged. Shared range telemetry now includes the catalog:
376,766,240 records equals 376,346,710 blob records plus 419,530 pack records;
257 completed ranges means 256 blob partitions plus one catalog range. These
totals must not be compared as if they were blob-only scan measurements.

Both manifests verified, scratch was empty, and writer epoch 39 remained healthy
with zero active transactions/intents. No service binaries were replaced. This
is repeated completed SlateDB-only evidence, not full differential or cross-backend
acceptance; sequential cache effects remain uncontrolled. The scan at 82-83s
and serial encryption audit at 50s are now the largest remaining stages. Further
scan work needs daemon-side attribution, while bounded parallel authentication
remains a separate candidate that must preserve complete object verification.

### Bounded Encryption Audit (r20-r23)

Commit `4de3c0076` records the catalog-streaming implementation and r17-r19
evidence. The next candidate overlaps up to four encryption-audit objects
using bounded futures, without spawning an unbounded task per listed object.
Byte-weighted admission allows 2 GiB of listed ciphertext sizes per audit,
rounded up to 1 MiB units; an oversized object consumes the entire allowance
and runs alone. The allowance accommodates four nominal 256 MiB SSTs including
encryption overhead, unlike a 512 MiB allowance that could serialize them.

Every admitted object retains the existing header classification, readable-key
snapshot, old-key accounting and complete payload authentication. Internal
`_vaultic/` objects remain excluded. Payload authentication errors from `get()`
still fail the audit; classification and body-stream error handling are unchanged.
Completion order changes, so the first reported error need not follow list order.
Rotation retirement continues to require a successful clean audit.

This is an admission bound, not a strict RSS ceiling: backend and crypto buffers
add overhead, listed sizes can become stale, oversized objects exceed the byte
allowance, and concurrent audit calls have independent budgets. Dropping the
audit or encountering an error drops pending I/O futures and their permits;
already-submitted crypto work can finish on the existing executor afterward.

Local validation: all 51 library encryption tests passed, including rotation
retirement across restart. After increasing the allowance from 512 MiB to 2 GiB,
all three focused audit tests passed again. Deterministic gated reads demonstrate
four-way overlap, byte-limited two-way admission, oversized-object exclusivity,
cancellation and read-error cleanup. Mixed encrypted/plaintext/malformed/old-key
fixtures match serial counts, and corruption in the final payload byte fails
both serial and parallel audits. Editor diagnostics and whitespace checks pass.

With explicit approval, the three focused audit tests passed again and
`RUSTFLAGS="-C force-frame-pointers=yes" make vaulticdb BIN_DIR=bin/phase33-audit-parallel`
built the static release candidate. After a fresh serial-audit control (r20),
only the service daemon was replaced through the configured clean shutdown/start
path. PID 100168 / epoch 39 became PID 165437 / epoch 40, read-write with zero
active transactions and write intents. The service CLI was not replaced.

The deployed daemon SHA-256 is
`efb779ff8e9eda3cb267a7cf56ef9b1726e6fc07c6423e91392afacd430e6cfb`.
Rollback is preserved at `bin/phase33-audit-parallel/rollback/vaulticdb`, SHA-256
`2963b4456fe2cd7f1738f19635745060f5f41df307c496a10e54a917f20a4305`.
Deployment source patch, revision, hashes and before/after health are under
`db.test/phase33-audit-parallel-deployment-2026-09-22`; its manifest verifies.

All runs used the unchanged r18/r19 catalog-stream CLI, 96 GiB memory/scratch,
32 scan workers/RPCs, HDD-NFS paths, five-second lightweight telemetry and the
ten-minute TERM cap with 45-second kill grace. No tests/builds overlapped runs,
and no caches were dropped. Artifacts under `db.test/phase33-production-2026-09-22-`
have suffixes `stream32-audit-control-r20`, `stream32-audit-parallel-r21`,
`stream32-audit-parallel-r22` and `stream32-audit-parallel-r23`.

R21 failed closed after 15.93s: a listed compaction metadata object,
`db/compactions/00000000000000000377.compactions`, disappeared before its read.
Main-store puts advanced from 10 to 12 and deletes from 1 to 2 during the attempt;
no SST compactions were recorded. This is consistent with concurrent metadata
maintenance after startup, but does not establish the exact cause or whether
parallel auditing increases race exposure. No missing-object errors were suppressed
and no automatic retry was added. The writer stayed healthy and scratch was empty.
R21 is a failed startup-adjacent attempt, excluded from successful timing averages,
and remains an operational reliability caveat rather than being discarded.

R22 and r23 completed on the same daemon without another restart:

| Metric | r20 serial control | r22 parallel | r23 parallel |
| --- | ---: | ---: | ---: |
| Exit | 0 | 0 | 0 |
| Wall time | 3m18.70s | 2m45.84s | 2m41.76s |
| Encryption audit interval | 51s | 15s | 15s |
| Blob scan interval | 82s | 84s | 83s |
| Finalize interval | 2s | 2s | 1s |
| Catalog interval | 28s | 26s | 22s |
| Parallel validation interval | 33s | 36s | 38s |
| CLI CPU seconds | 821.00 | 838.55 | 819.48 |
| CLI peak RSS, KiB | 14,988,656 | 14,978,548 | 15,140,192 |
| Daemon CPU seconds | 1,594.03 | 1,585.58 | 1,566.83 |
| Daemon logical read bytes | 102,465,262,687 | 102,465,601,438 | 102,405,566,354 |
| Main-store GET attempts | 73,667 | 73,562 | 73,526 |
| Main-store body bytes | 42,828,492,916 | 42,828,762,623 | 42,771,271,632 |
| Daemon post-run lifetime VmHWM, KiB | 1,386,180 | 1,319,980 | 1,534,288 |

Successful candidate mean wall time is 2m43.80s, 17.6% shorter than the fresh
control. The audit interval fell 70.6%, with roughly unchanged total daemon CPU
and logical reads: this supports overlapping authentication work rather than
skipping it. Stage intervals are rounded progress boundaries; daemon CPU uses
before/after process counters, logical reads are not physical NFS traffic, and
VmHWM is a process-lifetime high-water mark, not a per-stage peak.

R22/r23 logical results match each other exactly after excluding `resources`
and `consistency`. Compared with r20, the sole remaining differing field is
`encrypted_objects`: 161 before restart, 164 afterward. Do not describe that as
exact cross-restart JSON equality. Both retain 379,934,385 distinct locations,
419,530 warnings, and reduced SlateDB-only coverage (`complete=false`). Final
range telemetry remains 376,766,240 records / 257 ranges.

All four raw manifests verified and all scratch directories were empty afterward.
Completed-run telemetry contains 40/33/33 valid snapshots; host and CLI swap stayed
zero. Final writer health is read-write at epoch 40 with zero active transactions
and intents; the candidate remains deployed. Source and evidence remain uncommitted.
These are repeated completed NFS measurements, not full differential or RADOS
acceptance. Restart/cache differences and r21's listing/read race remain unresolved.

### Listing Race Follow-up (2026-09-23, Local Only)

Commit `fb1d589ea` records the bounded parallel audit and r20-r23 evidence.
After the reported host stall, the host was responsive with low load and about
289 GiB available RAM; the same daemon PID 165437 remained read-write at epoch
40 with zero transactions/intents. The operator subsequently identified the host
backup as the cause of the slowdown. No production diagnostic or service restart
was performed during this follow-up; avoid overlapping future timing runs with
the host backup.

A deterministic gated-store regression reproduces deletion between listing and
reading with both one and four audit workers. Both strict audits fail with
`ObjectStore::Error::NotFound`, confirming that this failure mode is not unique
to parallel auditing; its relative production frequency remains unmeasured.

The local read-only encryption-check entry point now permits one fresh full audit
after a typed `NotFound` error. It does not skip the missing object, retain partial
counts, or suppress a second failure. The replacement listing is fully checked,
and backend errors other than `NotFound` and authentication failures still fail
without this retry. Key retirement and capsule migration retain the strict audit
entry point. This is bounded recovery from a non-snapshot listing, not a guarantee
that the object set stays fixed while checking; sustained churn can still fail.
It can roughly double audit work on the retry path, and already-submitted crypto
work from the first attempt may finish after its futures are dropped.

All five focused audit tests pass: serial/parallel disappearance, fresh replacement
listing and counters, second-disappearance failure, nonretryable I/O failure,
payload corruption, admission and cancellation. The daemon binary target passes
`cargo check`; editor diagnostics and whitespace checks are clean. The new retry
has not been deployed or benchmarked, and its source/evidence remain uncommitted.

### Current Scan Attribution (r24, 2026-09-23)

Commit `b4855dd1d` records the check-only retry and the operator's confirmation
that the intervening slowdown was the host backup. R24 did not deploy that retry:
it retained daemon SHA-256 `efb779ff8e9eda3cb267a7cf56ef9b1726e6fc07c6423e91392afacd430e6cfb`,
PID 165437 / epoch 40, and the r18/r19 catalog-stream CLI. Host load was low and
process-name checks found no backup/build/test jobs before the run; these checks
are not proof that no external backup activity occurred.

The ten-minute capped HDD-NFS diagnostic used unchanged 96 GiB limits and 32
workers/RPCs, with no overlapping builds/tests or service changes. The sampler
captured CLI CPU for 30 seconds and daemon CPU at 49 Hz with frame-pointer stacks
during the scan, followed by a three-second Go trace. No scheduler-switch stream
was collected. Artifacts are under
`db.test/phase33-production-2026-09-23-stream32-scan-profile-r24`, with independently
verified raw and `analysis/` manifests.

R24 exited zero in 2m26.87s: audit 15s, scan 81s, finalize 2s, catalog 26s,
parallel validation 20s. CLI CPU was 801.21s and peak RSS 14,524,248 KiB;
daemon before/after CPU was 1,577.60s. These are diagnostic timings, not a matched
speedup result, particularly given the shorter parallel-validation interval.
Thirty telemetry snapshots parsed successfully. Scratch and swap stayed zero,
and final writer health remained read-write with zero transactions/intents.

CPU windows started at 07:30:33 UTC; profile collection/drain ended at 07:31:04.895,
and the subsequent trace ended at 07:31:07.933. The daemon process counters added
576.16 CPU-seconds across approximately 31.6 seconds; the Go CPU profile recorded
275.66 sampled CPU-seconds over 30 seconds. These are different accounting methods
and windows, not directly additive utilization measurements.

Daemon capture contains 25,080 samples, 7.19 MB, and zero reported lost samples.
The sampler logged `perf capture failed` after its deliberate SIGINT stop because
it treated every nonzero wait status as failure. The exact exit status was not
retained, but the capture decodes successfully with a normal perf completion
summary. Raw artifacts remain unchanged; the sampler now records the exit status
and treats requested-interrupt status 130 separately. Some Rust symbols remain
mangled with LLVM suffixes despite the system demangler; names below come from
the identifiable symbol components.

The dominant daemon CPU path is SlateDB iteration, not encryption:

- `DbIterator::next` accounts for 81.98% cumulative sampled CPU.
- `memcpyFast` is 15.73% flat, with stacks through segment/merge iterators.
- One boxed `RowEntryIterator::next` specialization is 10.55% flat; another
  boxed-next entry is 2.78%. These are distinct symbols, not summed cumulative costs.
- `Bytes` shared clone/drop are 3.60% / 2.66% flat.
- jemalloc malloc/deallocation entries are 3.33% / 3.28% flat.
- Merge advance is 3.06% flat; heap pop/push are 2.22% / 1.59% flat.
- The listed AES-GCM assembly hotspot is 1.11% flat, unlike the old amplified
  read/decrypt path. This does not count all crypto cost.

CLI scan CPU remains distributed across pack aggregation (25.19% cumulative),
protobuf eager unmarshalling (24.01%), blob decoding (13.99%) and key parsing
(8.64%); cumulative costs overlap. In the later three-second trace,
48.93 aggregate goroutine-seconds are under gRPC receive and 48.96 under
`ReadSession::ScanRange`. That identifies the CLI's wait location, not the daemon's
async wait cause. Rust iterator/channel/crypto-queue waits are still not separately
measured; CPU stacks alone cannot supply that attribution.

Compared with r23, all logical result fields match after excluding resources,
consistency and the explicitly reported encrypted-object count (164 -> 161).
Distinct locations remain 379,934,385 and warnings 419,530. Coverage remains
SlateDB-only and incomplete; this is not full differential or RADOS acceptance.

The next discriminating experiment should benchmark the pinned SlateDB iterator's
per-row boxed-future, row-copy and heap-advance costs with exact ordered-output
equivalence before changing the dependency. The current evidence does not justify
another read-ahead change or blindly increasing scan workers. No further source
optimization or daemon deployment was made during r24 attribution.

### Local Merge Experiment (2026-09-23)

The operator authorized changes in the SlateDB fork. The supplied
`/root/proj/slatedb` path did not exist in this environment. The clean checkout at
`/root/proj/slatedb-phase32-p2` matched the pinned `f549d4a` revision in
`otuschhoff/slatedb`, so the experiment used that checkout.

The merge iterator combines ordered input streams with a heap, a structure that
keeps the next row at its root. Its old advance path pushed an updated input into
the heap, then immediately popped the next input. The candidate compares the
updated input with the root instead. If the root comes first or compares equal,
the candidate swaps the inputs and lets `peek_mut` repair the heap once.
Otherwise, it keeps the updated input without a heap change. Exhausted inputs
still select the next input with `pop`. No heap guard spans an await.

The patch changes only `slatedb/src/merge_iterator.rs` in the fork. It keeps the
existing key/sequence comparator, duplicate barriers, byte accounting, public
interfaces, and seek path. It does not address boxed-future allocations or change
payload storage. Those costs remain separate targets.

An ignored release-mode test measures one, six, and 32 input streams. Each case
uses 120,000 rows with 34-byte keys and 56-byte values. The fixture uses RAM,
not local disk as a replacement for NFS or RADOS. Fixture creation and iterator
initialization occur before timing. The timed loop consumes and drops every row
through the existing asynchronous interface. One warm-up round precedes ten
measured rounds per case. The probe lives beside the private iterator tests to
avoid adding a public benchmark interface.

The initial unpinned 32-stream interleaved result regressed from 505.30 to
683.98 ns/row. Later unpinned runs did not reproduce that result. Both results
remain in the raw logs. To reduce migration noise, six further runs used CPU 0
in baseline/candidate/candidate/baseline/baseline/candidate order.
The following values are means of three per-run medians, not confidence bounds:

| Input streams | Key layout | Baseline ns/row | Candidate ns/row | Time reduction |
| ---: | --- | ---: | ---: | ---: |
| 1 | Interleaved | 228.97 | 197.43 | 13.8% |
| 1 | Disjoint | 235.08 | 202.20 | 14.0% |
| 6 | Interleaved | 288.44 | 260.49 | 9.7% |
| 6 | Disjoint | 276.75 | 207.29 | 25.1% |
| 32 | Interleaved | 539.90 | 503.66 | 6.7% |
| 32 | Disjoint | 306.53 | 201.15 | 34.4% |

The fixture does not model the full SlateDB iterator stack, encryption, storage
latency, or concurrent scans. Native release builds use a different allocator
and link target from the production daemon. These results support a candidate
for further measurement, not an end-to-end performance claim.

A sorted-reference test compares every returned row across one, six, and 32
streams in both directions. It covers values, tombstones, merge operands,
duplicate suppression enabled/disabled, empty child streams, and exact processed
byte totals. Existing seek and duplicate tests also pass. The broader iterator
suite passes 164 tests, with the timing probe ignored. All 41 snapshot tests pass,
including snapshot reads across compaction.

All 63 Vaultic storage tests pass against the local fork through command-only
Cargo `[patch]` overrides. They include persisted streaming scans, cursor bounds,
snapshot expiry, encryption, and cleanup. An earlier compile used Cargo's legacy
path override and emitted a dependency-resolution warning. The test run used the
recommended patch mechanism instead. Its script preserves and restores the exact
original generated lockfile. Vaultic's committed dependency pin is unchanged.

Artifacts reside at `db.test/phase33-merge-iterator-2026-09-23`. They include saved
baseline/candidate executables, timing logs, source patches, revision, compiler
version, test logs, and the integration script. A SHA-256 manifest covers these
files. Rustfmt and whitespace checks pass. SlateDB commit
`67abacef0044f5e285c1f59af6c936de4c051954` records the tested patch on
`vaultic-multiget-rebase`. Vaultic's dependency pin remains unchanged.
No production service replacement or production diagnostic occurred during this
local experiment.

The next gate is a matched end-to-end scan experiment with an approved candidate
daemon. Keep the existing worker count and read-ahead configuration for that test.
Do not infer a total runtime gain from these isolated merge-loop timings.

### Matched Merge Runs (r25-r28, 2026-09-23)

With operator approval, four HDD-NFS checks compared the committed merge change
in control/candidate/candidate/control order. Each run started after a daemon
restart and a read-write, idle-writer health gate. No caches were dropped. The
same catalog-stream CLI used 32 workers/RPCs, 96 GiB limits, and unchanged
read-ahead. Each check had a ten-minute TERM cap and 45-second kill grace.
No builds or tests overlapped the checks. Process-name checks found no backup
jobs, but cannot exclude external backup activity.

Both static musl daemons used Vaultic `7b46cd41d`, the production frame-pointer
build flags, and the same compiler and allocator. Both therefore included the
check-only audit retry, unlike the previously deployed daemon. Control used
SlateDB `f549d4a`. Candidate used the clean local checkout of committed
`67abacef0044f5e285c1f59af6c936de4c051954` through Cargo patch overrides.
Generated lockfiles differ only in the three SlateDB package sources.
Tracked dependency pins and lockfiles remain unchanged.

The workspace filesystem was full. Source snapshots, build caches, and temporary
executables used RAM. Build artifacts and all diagnostic output were saved on
NFS. Repository data and check scratch stayed on HDD-NFS. A temporary systemd
override selected each daemon without overwriting the installed binary.

| Run | Variant | Total | Audit | Scan | Catalog | Daemon CPU-seconds |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| r25 | Control | 2m34.93s | 30s | 82s | 19s | 1708.01 |
| r26 | Candidate | 2m23.99s | 15s | 80s | 24s | 1553.67 |
| r27 | Candidate | 2m41.61s | 31s | 80s | 27s | 1624.07 |
| r28 | Control | 2m40.29s | 31s | 83s | 20s | 1721.91 |

Stage durations come from rounded progress timestamps. Mean total time fell from
157.61 to 152.80 seconds, or 3.05%. Mean scan time fell from 82.5 to 80 seconds,
or 3.03%. Mean per-run scan throughput rose from 4.567 to 4.710 million records/s.
Mean daemon CPU fell from 1714.96 to 1588.87 CPU-seconds, or 7.35%. Mean CLI CPU
rose from 804.405 to 808.675 CPU-seconds, or 0.53%.

The two runs per variant do not establish confidence bounds. Candidate r27 was
slower overall than either control. Audit and catalog variation exceed the scan
wall-time gain. Whole-check CPU also includes the variable audit work. These
results support a modest scan improvement, not a stable 3% total-runtime promise.
The available counters do not establish the cause of the audit variation.

Accumulated iterator service time fell from a mean 1920.70 to 1786.77 seconds,
or 6.97%. Client receive time fell from 2039.58 to 1968.91 seconds, or 3.47%.
Consumer time fell from 513.67 to 504.19 seconds. These values sum concurrent
range durations. They are not wall time or independent CPU measurements.

All four checks exited zero. All logical result fields match after excluding
resources, consistency session IDs, and encrypted-object counts. The analysis
also compares the remaining consistency fields and engine configuration exactly.
Each run scanned 376,766,240 records in 37,811 chunks across 257 ranges and
reported 379,934,385 locations and 419,530 warnings. Scanned byte totals vary by
five bytes across runs. Encrypted-object counts are 164/169/169/169. These are
live-maintenance observations, not a byte-identical immutable fixture.
Coverage remains SlateDB-only and incomplete, not full differential or RADOS
acceptance. Scratch and daemon swap stayed zero.

Each scan captured daemon CPU at 49 Hz and CLI CPU for 30 seconds, followed by
a three-second Go trace. All daemon captures report zero lost samples and
expected interrupt status 130. Control heap pop/push symbols total 3.59%/3.77%
flat sampled CPU. Candidate `PeekMut` heap repair is 0.86%/1.02%. The main merge
advance symbol rises from 2.97%/2.87% to 4.49%/4.55%, so some work moves there.
`memcpyFast` falls from 15.82%/15.66% to 14.32%/14.63%. These percentages describe
sampled symbols, not isolated operation timings or allocation counts.

SlateDB iteration still dominates daemon CPU. The largest boxed-next symbol is
10.29-11.02% flat across the runs. CLI traces show 49.46-52.23 aggregate
goroutine-seconds in gRPC receive during each three-second window. This identifies
the client wait location, not the daemon's asynchronous wait cause. Iterator
allocation and row copying remain useful next targets. No scheduler-switch or
separate Rust future-wait trace was collected.

Artifacts are under `db.test/phase33-merge-e2e-2026-09-23` and the four
`db.test/phase33-production-2026-09-23-stream32-merge-{variant}-r{25..28}`
directories. They preserve build/run/analysis scripts, source revisions,
lockfiles, executables, raw profiles, decoded reports, and structured comparisons.
Each run's manifest verifies. The harness records the actual `/proc/PID/exe`
hash, rather than the installed binary path, to identify temporary overrides.
Control SHA-256 is
`4160b8ce4ecea8926fb29eacff010e18552e17e7493dcd15f349e98f23487edd`.
Candidate SHA-256 is
`40458409358812e17c51561a22657237e382c389e90cf191fbfa801b95d6adf1`.

The runner removed its override and restored the original daemon after r28.
Binary comparison confirms SHA-256
`efb779ff8e9eda3cb267a7cf56ef9b1726e6fc07c6423e91392afacd430e6cfb`.
Final PID is 268961, epoch 45, read-write with zero transactions and write intents.
No permanent deployment or dependency update was made.

### Local Boxed-Next Experiment (2026-09-23)

The next local experiment targets the boxed iterator adapters in SlateDB
`slatedb/src/iter.rs`. Their async `next` methods create an outer boxed future,
an allocated object that represents pending asynchronous work. The inner iterator
already returns a boxed future. The candidate returns that inner future directly
for both `Box<dyn RowEntryIterator>` and `Box<dyn TrackedRowEntryIterator>`.
It retains the existing async-trait lifetime and Send contract. Initialization,
seek, comparison rules, row storage, and public interfaces stay unchanged.

Two regression tests compare the returned future's address with the inner
allocation through nested boxes. Both use borrowed state and a future that first
returns Pending. They cover polling, cancellation while pending, future drop,
error propagation, and tracked-byte forwarding. The returned future is the same
allocation, not another wrapper. This does not measure all iterator allocations
or eliminate boxing inside the concrete iterators.

Both release test binaries were built at the same source path, with baseline
`67abacef0044f5e285c1f59af6c936de4c051954` and the candidate patch applied afterward.
The existing RAM-only merge probe is unchanged: 120,000 rows, 34-byte keys,
56-byte values, one warm-up round, and ten measured rounds. Builds finished
before timing. CPU 0 ran baseline/candidate/candidate/baseline/baseline/candidate.
Values below are means of three per-run medians:

| Input streams | Key layout | Baseline ns/row | Candidate ns/row | Time reduction |
| ---: | --- | ---: | ---: | ---: |
| 1 | Interleaved | 203.15 | 178.98 | 11.90% |
| 1 | Disjoint | 206.05 | 178.83 | 13.21% |
| 6 | Interleaved | 263.26 | 232.37 | 11.73% |
| 6 | Disjoint | 208.28 | 181.54 | 12.84% |
| 32 | Interleaved | 533.12 | 492.49 | 7.62% |
| 32 | Disjoint | 200.01 | 173.17 | 13.42% |

Raw logs retain timing outliers, including baseline round maxima of 747.54 and
772.50 ns/row. The means above are not confidence bounds. These gains are relative
to the committed heap optimization, not the original push/pop implementation.
The probe does not model production I/O, concurrent scans, or the full iterator
stack. No new production runtime gain is established by this experiment.

Validation passes 13 focused merge tests, two direct-forwarding tests, 164 broader
iterator tests, 41 snapshot tests, and 169 compaction-filtered tests. These suites
overlap and must not be added into a unique test count. The first compaction run
had six failures because the persistent terminal retained a removed temporary
directory in `TMPDIR`. All 169 tests passed after setting the current RAM path
explicitly. Both logs are retained. No source change was needed for that failure.

All 63 Vaultic storage tests pass against the candidate through Cargo patch
overrides in a RAM source snapshot. This leaves the workspace manifest and
lockfile untouched. Rustfmt, editor diagnostics, and whitespace checks pass.
No tests or builds overlapped a production diagnostic.

Artifacts are under `db.test/phase33-boxed-next-2026-09-23`. They include build and
integration scripts, baseline revision, candidate patch, compiler version, both
test executables, raw timings, and validation logs. SlateDB commit
`43a562e3637d3d9ce12bec82a5b9e3deb47843fa` records the tested patch on
`vaultic-multiget-rebase`. Production stays on the restored original daemon. A matched
end-to-end comparison is still needed before a deployment decision for this patch.

### Matched Boxed-Next Runs (r29-r32, 2026-09-23)

With operator approval, four HDD-NFS checks compared heap-only SlateDB `67abace`
against direct future forwarding in `43a562e`. Both static musl daemons used
Vaultic `d9e4b9fd1`, the same source paths, frame-pointer flags, compiler, and
allocator. Both used identical Cargo patch overrides and generated lockfiles.
The build refreshed the changed source timestamp after archive extraction, and
the logs confirm that Cargo rebuilt SlateDB for the candidate. Binary hashes
differ. Tracked manifests and lockfiles remain unchanged.

The experiment retained the catalog-stream CLI, 32 workers/RPCs, 96 GiB limits,
and existing read-ahead. Control/candidate/candidate/control runs each started
after a daemon restart and an idle-writer health gate. Each check had a ten-minute
TERM cap with 45-second kill grace. Builds completed before measurement. No caches
were dropped. Process-name gates found no backup or build jobs, but cannot exclude
external backup activity. Temporary build output used RAM. Repository data and
check scratch stayed on HDD-NFS, with evidence saved on NFS.

| Run | Variant | Total | Audit | Scan | Catalog | Daemon CPU-seconds |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| r29 | Control | 2m38.41s | 29s | 82s | 22s | 1623.44 |
| r30 | Candidate | 2m11.20s | 16s | 67s | 22s | 1111.36 |
| r31 | Candidate | 2m25.42s | 31s | 67s | 22s | 1172.35 |
| r32 | Control | 2m30.99s | 16s | 78s | 31s | 1497.74 |

Stage durations come from rounded progress timestamps. Mean scan time fell from
80 to 67 seconds, or 16.25%. Mean per-run scan throughput rose from 4.713 to
5.623 million records/s, or 19.33%. Mean total time fell from 154.70 to 138.31
seconds, or 10.59%. Both candidate runs finished faster than either control.
Audit and catalog times still vary, and two runs per variant do not establish
confidence bounds. These are measured results for this workload, not guarantees
for other repositories or storage backends.

Mean whole-check daemon CPU fell from 1560.590 to 1141.855 CPU-seconds, or 26.83%.
Mean CLI CPU rose from 830.995 to 853.960 CPU-seconds, or 2.76%. Iterator service
time fell from 1746.35 to 1230.49 accumulated seconds, or 29.54%. Client receive
time fell from 1928.50 to 1552.21 accumulated seconds, or 19.51%. Consumer time
fell from 534.74 to 524.28 seconds. Accumulated range times overlap and are not
independent CPU measurements. Whole-check CPU includes the variable audit work.

All four checks and the runner exited zero. Logical result fields match after
excluding resources, consistency session IDs, and encrypted-object counts. The
remaining consistency fields and recorded engine configuration match exactly.
Each run scanned 376,766,240 records in 37,811 chunks across 257 ranges and
reported 379,934,385 locations and 419,530 warnings. Byte totals vary by 25 bytes.
Encrypted-object counts are 164/169/172/174. This is a live repository, not an
immutable byte-identical fixture. Coverage remains incomplete and SlateDB-only,
not full differential or RADOS acceptance. Scratch and daemon swap stayed zero.

Each scan captured daemon CPU at 49 Hz and CLI CPU for 30 seconds, followed by
a three-second Go trace. Daemon sample counts are 24,558/20,374/19,986/24,339,
with zero reported lost samples. All captures report expected interrupt status
130, and sampler error logs are empty. Daemon CPU counters across the roughly
31.6-second capture windows are 561.03/464.56/461.63/560.41 CPU-seconds.

The largest control boxed-next symbol accounts for 11.12%/10.56% flat sampled
CPU. Candidate boxed-next entries peak at 2.15% in both runs. These are different
remaining symbols, not the same wrapper with a lower cost. `DbIterator::next`
falls from 81.47%/80.86% cumulative samples to 72.58%/73.08%. `memcpyFast` rises
from 14.35%/14.89% to 16.61%/16.54% of samples. The percentage rise does not
establish an absolute increase in copying, because the candidate consumes less
CPU and processes more rows during the fixed window. No allocation counts were
collected in production.

CLI CPU profiles contain 295.99/353.29/356.04/292.26 sampled CPU-seconds in their
30-second windows. Pack aggregation remains about 25% cumulative sampled CPU.
The later three-second traces contain 47.13-51.51 aggregate goroutine-seconds
under gRPC receive. These short windows do not represent total run wait time.
They identify the client wait location, not the daemon's asynchronous wait cause.
Separate Rust future-wait and scheduler-switch attribution remains unavailable.
Row copying and the remaining iterator allocations are further investigation
targets, not changes included in this experiment.

Artifacts reside under `db.test/phase33-boxed-next-e2e-2026-09-23` and the four
`db.test/phase33-production-2026-09-23-stream32-boxed-next-{variant}-r{29..32}`
directories. They include build/run/analysis scripts, revisions, lockfiles,
executables, raw profiles, decoded reports, and structured comparisons. All four
raw run manifests verify. Control SHA-256 is
`cb710d2a779f2dc67a1ecda9ed4f07e9e220495b0c6df8a303877346bf228cb7`.
Candidate SHA-256 is
`6b00ab81536468897b2e38fa458ed86a39c201464d036608e19afc72e2fc1014`.

The runner removed its temporary override and restored the original daemon.
Binary comparison confirms SHA-256
`efb779ff8e9eda3cb267a7cf56ef9b1726e6fc07c6423e91392afacd430e6cfb`.
Final PID is 385190, epoch 50, read-write with zero transactions and write intents.
No permanent deployment, dependency update, commit, or push occurred during this
experiment. The result supports adopting the candidate for this NFS workflow,
subject to an explicit dependency and deployment decision.

### NFS Adoption (r33, 2026-09-23)

The user approved adoption after the matched comparison. The SlateDB fork now
publishes `43a562e3637d3d9ce12bec82a5b9e3deb47843fa` on
`vaultic-multiget-rebase`. Both direct Vaultic dependencies pin that revision.
The lockfile changes only the three SlateDB Git source identities.
Cargo metadata resolves all three packages from the published revision with
`--locked` and no local overrides. All 63 Vaultic storage tests pass against
that locked Git dependency.

The durable service executable now contains the exact candidate from r30/r31:
`6b00ab81536468897b2e38fa458ed86a39c201464d036608e19afc72e2fc1014`.
That binary was built from Vaultic `d9e4b9fd1` with a local Cargo patch to
SlateDB `43a562e`, not rebuilt from the new Git pin. The published-pin tests
provide a separate dependency-resolution gate. Installation used an adjacent
staged file, a graceful service stop, and an atomic rename. The service
configuration and CLI remain unchanged, with no runtime override.

The capped post-deployment check exited zero in 2m23.58s. Approximate audit,
scan, and catalog durations were 31s, 67s, and 22s. The daemon used 1180.46 CPU
seconds and the CLI used 837.38 CPU seconds. This single run had no CPU or trace
profiling and is a deployment check, not another matched performance comparison.
It used the same 32 workers/RPCs, 96 GiB limits, and HDD-backed NFS storage.
No builds, tests, or detected backup processes overlapped the check.

Logical results match r31 under the same dynamic-field exclusions used above.
The remaining consistency fields and engine configuration match. Scan totals
remain 376,766,240 records, 37,811 chunks, and 257 completed ranges. Locations
remain 379,934,385 and warnings remain 419,530. Encrypted objects total 164.
Scratch use and daemon swap are zero. Coverage remains incomplete and
SlateDB-only. This adoption does not close full differential or RADOS acceptance.

Final PID is 391849, epoch 51, read-write with zero transactions and write intents.
The original daemon remains available as `rollback-vaulticdb` under
`db.test/phase33-boxed-next-adoption-2026-09-23`, with SHA-256
`efb779ff8e9eda3cb267a7cf56ef9b1726e6fc07c6423e91392afacd430e6cfb`.
That directory contains the deployment script, rollback gates, health records,
locked-resolution metadata, and test logs. Raw r33 results reside under
`db.test/phase33-production-2026-09-23-stream32-boxed-next-adopted-r33`.

### Full Differential Attempt (r34, 2026-09-23)

The first full differential attempt after adoption reached the ten-minute cap
in `legacy_scan`. It did not reach the encryption audit or SlateDB scan.
The timeout wrapper returned 124. The CLI reported cancellation and completed
cleanup within the 45-second grace period. Total elapsed time was 10m11.50s.
No final comparison summary exists, so this attempt provides no clean verdict.

The command omitted `--slatedb-only` and retained 32 workers/RPCs, 96 GiB memory
and scratch limits, and HDD-backed NFS storage. Scratch reached 64,438,495,400
bytes at the cap. CLI peak RSS was 81,082,900 KiB and CPU time was 1303.56s.
The 122 interval samples averaged 90.88% host idle and 0.44% I/O wait.
These host counters do not isolate legacy parsing, serialization, or NFS latency.
No build, test, or detected backup workload overlapped this measurement.

The final monitor snapshot gives a more specific explanation. Scratch sorting
took 261.16s across 841 completed operations. The 839 completed spill writes
spent 225.40s in encoding/write, 0.078s in final buffered flush, and 59.63s in
file sync. One additional encoding/write operation was cancelled. No merge
completed. These are elapsed stage totals, not CPU or pure device wait time.

`ForAllIndexesWorkers` loads and decodes concurrently but holds one mutex while
calling the legacy consumer. `loadLegacyLocations` sorts and spills both spools
inside that serialized callback. Each spool receives one quarter of the memory
budget, or 24 GiB here. These facts support a bounded parallel legacy-consumer
experiment with private spools. They do not support adding more decoder workers
or treating all scratch time as NFS latency. No legacy behavior changed here.

The daemon remained at PID 391849, epoch 51, read-write with zero transactions
and write intents after cancellation. Scratch cleanup left no files. The raw
manifest verifies under `db.test/phase33-production-2026-09-23-stream32-adopted-full-r34`.
Legacy scan and spill attribution is now the next full-check acceptance blocker.
Do not extend the diagnostic cap or infer success from the reduced checks.

Native RADOS validation remains blocked on this host. The installed daemon is
a generic static build, and the host has no discoverable librados, Ceph CLI,
Docker, or RADOS-enabled binary. The existing integration harness provisions
a disposable container cluster, not the authorized production storage.
No credential contents were exposed and no production backend was changed.

### Scan Wait Diagnostics (r35-r38, 2026-09-23)

An opt-in `VAULTICDB_SCAN_TIMING=1` probe now records one JSON event per scan
stream. It measures elapsed and polling wall time for setup, chunk collection,
transaction validation, and response-channel reservation. Cancellation settles
the active measurement. The event contains no keys, values, or transaction IDs.
The probe is disabled by default and does not change the wire protocol.

The locked published dependency built as a static musl release with the existing
frame-pointer flags. All 63 storage tests passed with timing disabled. All three
persistent-scan tests passed with timing enabled, and all ten attribution tests
passed. Enabled test logs contained 40 valid timing records across success,
error, and cancellation. The unused RADOS test constant warning is unrelated.

Temporary service overrides selected the same instrumented binary for r35-r37.
Each check retained 32 workers/RPCs, 96 GiB limits, and HDD-backed NFS storage.
No builds, tests, or detected backup processes overlapped the measurements.

| Run | Timing | Total | Approximate scan | Daemon CPU seconds | Timing records |
|---|---|---:|---:|---:|---:|
| r35 | Off | 2m24.80s | 66s | 1175.01 | 0 |
| r36 | On | 2m11.31s | 67s | 1101.33 | 257 |
| r37 | On, raw scheduler attempt | 2m11.50s | 67s | 1101.75 | 257 |

The audit took 29s in r35 and 15s in r36/r37. The lower total with timing enabled
is not evidence of a speedup. One off/on pair cannot establish precise probe
overhead. Logical results, scan counts, and configuration match r33 under the
same dynamic-field exclusions. Every enabled production timing record reports
success. The scan still returns 376,766,240 records in 37,811 chunks and 257 ranges.

Across the 257 r36 streams, setup elapsed time totals 20.05s, with 0.17s inside
polls. Chunk collection totals 1212.50s, with 829.34s inside polls and 383.16s
between polls. Delivery reservation totals 733.75s, almost entirely between
polls. Transaction validation totals 93.53s, with 0.15s inside polls.
These concurrent durations overlap and are not command wall time. Polling wall
time includes preemption and synchronous blocking, not just CPU execution.
SlateDB explicitly consumes Tokio's cooperative budget during iteration, so
the 2,954,179 pending collection polls include fairness yields, not only I/O.
Delivery time identifies downstream backpressure but not its client-side cause.

The raw scheduler attempt in r37 produced metadata but no switch/wakeup samples.
Its local-thread filters are not validated in this PID namespace. Those artifacts
remain available, but they provide no scheduler-delay evidence.
The runner then restored the adopted daemon and removed its override.

R38 used the adopted binary without a restart or source change. A separate
30-second process-scoped perf capture recorded context switches with namespace-safe
attachment. The check exited zero in 2m05.99s and matched r33 logical results,
scan counts, and check configuration. This is not a matched runtime comparison.
The trace contains 3,511,260 records over a 29.46s event window, with no lost-record
markers or decoder errors. Complete intervals across 66 observed threads total
439.67s on CPU and 1474.33s off CPU. Preempted switch-outs account for 97.23s of
the off-CPU total. Initial and trailing boundaries are excluded.
These aggregates include idle workers and background tasks. Without wakeup
events, off-CPU time cannot be split into sleeping time and runnable delay.
They are neither asynchronous-task wait time nor critical-path wall time.

The adopted daemon remains active at PID 431717, epoch 55, read-write with zero
transactions and write intents. Its executable matches the saved adopted binary,
and no runtime override remains. The instrumentation is not permanently deployed.
Artifacts, source patch, test/build logs, diagnostic journals, and analyzers reside
under `db.test/phase33-scan-timing-2026-09-23`. Raw checks use
`db.test/phase33-production-2026-09-23-stream32-scan-timing-{off-r35,on-r36,scheduler-r37}`
and `db.test/phase33-production-2026-09-23-stream32-adopted-context-r38`.
The next performance experiment must address the measured serialized legacy
consumer before another full differential acceptance attempt.

## Prior Production Runs

This record captures bounded full and reduced-coverage check attempts against
the activated production takeover on 2026-09-22. It is representative evidence
for the current NFS deployment, but it is not a successful Phase 33 acceptance
run: the first run stopped at metadata encryption and later runs reached but did
not complete the SlateDB location scan.

The immutable raw artifacts are outside the repository at
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-22/`; its
`SHA256SUMS` manifest verifies all 14 captured files. The normalized
partial-result SHA-256, with the read-session ID removed, is
`a4ebb3fc50c2472d04afab32afb294ddf0ce0412b737d734a9f08d69912b0b03`.

## Environment and command

- Source repository: `/volume2/NASDA2/rustic/repo`, NFSv3 with 128 KiB reads.
- Authoritative database: `/volume2/NASDA2/rustic/db`, resolving to a separate
  NFSv3 mount with 64 KiB reads.
- Repository identity:
  `c4d68689c785d02a28d6eec485c62132823dc9873ff7f627c2bac80258251528`.
- Database service: `vaulticdb-rustic.service`, read-write writer epoch 35.
- Host: 32 logical CPUs, 300 GiB RAM, no swap.
- Source revision: `451d1f98f11ba2c75f3b57ade0858d5f73a573f2`.
- Retained profile binaries identify themselves as
  `v0.2.11-42-g324b22043-dirty`; results therefore apply to those exact binaries,
  not a reproducible clean build of `HEAD`.
- Checker settings: full differential coverage, 32 workers, 32 RPCs,
  `--check-memory=auto`, 8 GiB scratch limit, encrypted metadata required,
  crawl-debt details enabled, and a ten-minute outer timeout.

The source inventory contained 10,019 index files totaling 19,245,153,330 bytes.
The activated aggregate catalog contains 419,530 packs and 379,934,385 blobs.
No caches were dropped and the production service was not restarted.

## Result

The command exited 1 after 5m30.38s, before the outer timeout:

```text
inventory       0s
legacy_scan     0s .. 5m11s
encryption_audit 5m11s .. 5m20s
failure         CheckEncryption RPC DeadlineExceeded
```

The partial result retained full requested coverage metadata, read session
`txn-4152527-2`, generation 1, and legacy inventory digest
`9983d7d4c41bbb87c2335bf6a40d6ef90a627f2f61c16499e12dd974709f4703`.
It had reached 10,019 legacy indexes with no scratch and no merge passes. It did
not reach `slatedb_scan`, `catalog_join`, `parallel_validation`, or successful
finalization, so zero mismatch counters in this partial JSON are not a clean
verdict.

The same fixed failure had occurred in the preceding activation check. This is
repeatable, not a ten-minute timeout artifact.

## Resource and throughput observations

`/usr/bin/time -v` measured the CLI:

| Metric | Observation |
|---|---:|
| User CPU | 1,139.38 s |
| System CPU | 91.69 s |
| Average CPU | 372% |
| Peak RSS | 98,281,444 KiB (93.7 GiB) |
| Major faults / swap | 0 / 0 |
| Scratch peak | 0 B |

The legacy scan completed in about 311 seconds: 32.2 indexes/s and 59.0 MiB/s of
encoded legacy index input. Dividing the known imported blob cardinality by the
stage duration gives about 1.22 million blob records/s as a scale indicator; it
is not a direct progress counter from the checker.

Host samples averaged 11.42% user CPU, 1.52% system CPU, 86.94% idle, and 0.11%
I/O wait. This warm run therefore did not saturate the host or exhibit an NFS
wait bottleneck during the legacy stage. The CLI nevertheless averaged only
3.72 logical CPUs despite 32 workers. Its 93.7 GiB peak RSS and the fall in free
memory show that auto memory admission is intentionally aggressive on this
large host; no swap or scratch occurred, but an explicit lower production budget
must be tested before accepting that default operationally.

The daemon averaged 2.91% CPU in the host sampler, peaked briefly at 115.9%, and
reached about 517 MiB sampled RSS. Across the run it added about 1.33 GiB of
physical reads and 6.11 GiB of logical reads. System-wide NFSv3 counters rose by
25,841 operations, dominated by 21,734 reads and 1,950 writes. These counters
include unrelated host traffic and cannot attribute requests to one mount or
process; they are supporting context, not proof of service time.

## Bottlenecks and decisions

### Initial P0, resolved: encryption audit could not complete

`CheckEncryption` uses the generic ten-second unary RPC deadline. The server's
`audit_objects` implementation lists every metadata object, downloads each full
object, checks its header, and authenticates encrypted contents before returning
one aggregate response. A 46 GiB NFS-backed store cannot reliably complete that
whole-store operation in ten seconds. The client cancels the request and the
full checker can never progress beyond this stage.

Replace this with a bounded paged/resumable audit tied to the pinned read session,
or introduce an explicit long-operation deadline only as an interim unblocker.
The durable solution must expose object/byte progress, cancellation, continuation
identity, and invalid/plaintext/old-DEK counts without weakening exact coverage.
Do not merely raise the generic deadline for every RPC.

### P1: legacy scan is CPU/allocation limited and memory-heavy

The legacy stage occupies at least 97% of the observed pre-failure wall time.
Host I/O wait is negligible and CPUs remain mostly idle, while the CLI averages
3.72 cores and reaches 93.7 GiB RSS. Existing synthetic profiles already locate
the remaining work in JSON/index decode, packed-blob conversion, tuple storage,
runtime scanning/GC, and serialized insertion into the shared legacy spool.
Production evidence agrees with the low parallel utilization but does not yet
provide symbolized production attribution.

Next compare 4/8/16/32 workers with an explicit memory budget and identical warm
input, at least three repetitions after P0 is fixed. Before adding workers,
partition legacy tuple output per decode worker and merge those sorted runs;
this removes the documented shared-spool mutex boundary. Then profile conversion
and allocation ownership. Accept a change only on end-to-end full-check runtime,
not legacy-stage CPU alone.

### P1: auto memory needs an operational cap

Auto admitted 279,539,385,754 bytes and the partial check retained 93.7 GiB RSS.
That avoided encrypted scratch, but leaves little isolation from colocated work
and makes a later full-check peak unknown. Benchmark explicit 32/64/96 GiB
budgets with local encrypted scratch. Measure elapsed time, peak RSS, spill,
merge passes, and daemon cache effects; choose a production default from that
curve rather than using nearly all reclaimable host memory.

### P2: production observability is below the documented contract

Progress exposes only stage, elapsed time, workers/RPCs, and scratch peak. It
omits records, bytes, RPC count/latency, queue/admission wait, CPU, and memory
high-water. `vaultic monitor` could not attach to the daemon using the available
global metadata-unlock socket option and reported `vaulticdb` unavailable without
a diagnostic. Host sampling was therefore necessary and could not separate the
two NFS mounts.

Add the normal index daemon attachment flags to monitor commands, preserve the
unavailability reason, and publish checker counters from the long-lived process
that owns them. Add stage-local records/bytes and wait-state snapshots before the
next tuning campaign. This is required to distinguish source decode, RPC
admission, daemon service, object-store read, response delivery, and scratch
backpressure.

## Superseded initial experiment order

This initial experiment order has been superseded by the follow-up evidence
below. The audit now completes, range sharding and independent spools are
implemented, and 64 workers do not outperform 32. The next experiment requires
a server-owned resumable range scan or stream that retains iterator state across
bounded response chunks. After focused protocol/session tests, rerun the same
ten-minute SlateDB-only diagnostic at 32 workers. Proceed to a full differential
check only when `slatedb_scan` completes; then profile catalog join and later
validators. Defer broader memory and backend matrices until that exact path can
finish.

No Phase 33 representative-scale acceptance gate is closed by these attempts.

## Follow-up production evidence

The encryption audit received a dedicated one-hour default deadline that
preserves shorter caller deadlines, and its initial object classification was
reduced from a duplicate full-object read to a bounded 38-byte header read. The
optimized full check completed the legacy scan in about 5m11s and the audit in
about 2m24s, then remained in `slatedb_scan` until the ten-minute cap. Its daemon
performed 40.1 GiB of physical reads and 631,052 NFS reads while host I/O wait
averaged 0.59%. The audit blocker is removed; zero partial-result mismatch counts
still are not a clean verdict.

Reduced `--slatedb-only` diagnostics then isolated the database path. These runs
skip legacy indexes, legacy snapshots, and export provenance and therefore are
performance diagnostics, not migration sign-off.

| Variant | Audit reached scan | Result at cap | Main observation |
|---|---:|---|---|
| 1,000-item pages, 32 workers, 64 GiB | 1m03s | scan incomplete | 419 GiB daemon logical reads in ten minutes |
| 10,000-item pages, 32 workers, 64 GiB | 48s | scan incomplete | lower iterator/RPC overhead, still serial client ingestion |
| partitioned scan, shared spools, 32 workers | 50s | 8 GiB scratch exhausted at 8m04s | daemon parallelism unlocked; shared spool serialized output |
| sharded spools, 32 workers, 96 GiB | 51s | scan incomplete; 61.6 GiB scratch | about eight times the prior scratch-output progress |
| sharded/buffered, 32 workers, 96 GiB | 2m16s | scan incomplete; 44.3 GiB scratch | run files fell from about 13,200 to 778 |
| sharded/buffered, 64 workers, 96 GiB | 1m07s | scan incomplete; 44.3 GiB scratch | no gain over 32 workers |

The 32-worker sharded run used 5.97 CLI CPUs on average and the daemon used about
9.7 CPUs. The host still averaged 47% idle and 0.67% I/O wait, but NFS counters
rose by 8.64 million operations, including 8.02 million `getattr` calls. With
64 workers, daemon CPU and completed scratch output did not improve. Current
throughput is therefore limited by SlateDB iterator/table metadata traversal and
the unary page model, not by raw storage bandwidth or lack of checker workers.

The accepted local improvements are:

- a 10,000-item daemon scan-page limit while retaining the 16 MiB response cap;
- 256 disjoint first-ID-byte blob prefixes scanned concurrently within the
   configured worker/RPC limits and the same pinned read session;
- independent bounded location spools per partition, adopted into the exact
   downstream reducers without tuple copies;
- 64 MiB bounded sort chunks and 1 MiB buffered encrypted scratch I/O.

The next optimization must be a server-owned resumable range scan or stream that
keeps iterator state across response boundaries, remains tied to the pinned read
session, has bounded flow control, and exposes records/bytes progress. More
workers, larger pages beyond the message cap, or further scratch tuning are not
supported by the measurements. A full differential clean check remains required
before migration validation can be signed off.
