# Phase 33 Production Benchmark Evidence

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
