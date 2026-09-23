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
