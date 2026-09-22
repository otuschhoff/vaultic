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
expired lease. Operators must allow enough idle time for non-database stages;
this patch does not add a read-session heartbeat.

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
The captured artifacts do not establish the source of cancellation. The last
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

Next handoff: instrument/profile spool adoption and finalization separately from
range delivery, and address read-session renewal during long non-database stages
before attempting a full differential check. The historical legacy stage alone
exceeds the default 300-second idle lease; the stricter expiry check must not be
weakened to bypass that limit. Repeat the bounded diagnostic to obtain an
uninterrupted control, then measure the full path and later validators. Full
differential success and the representative acceptance matrix remain pending.

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
