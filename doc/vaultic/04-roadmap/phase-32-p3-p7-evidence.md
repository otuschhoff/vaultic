# Phase 32 P3-P7 Experiment and Acceptance Evidence

This record reports the currently executable Phase 32 work as of 2026-09-17.
P3a, P3b, P7a, matched NFS/RGW/native-RADOS sampling, native RADOS lifecycle
tests, and live RADOS-WAL latency are validated. P3c remains partial: its memory-WAL
role pilots pass, but the complete response matrix is not yet run.
Representative evidence identifies sustained eligible work waiting during
ordered reduction and therefore selects P4 for a separately reviewed
experiment. P5 and P6 remain unselected. A current full import remains
incomplete, so this is not final repository-scale acceptance.

## Frozen Inputs and Build

The real-daemon fixture contains four ordered indexes, 128 preselected packs,
and 65,536 blobs. Import runs used `GOMAXPROCS=4`, `GOMEMLIMIT=8GiB`, and three
measured iterations; latency sweeps used 100 operations per point. Fresh import
used memory WAL unless explicitly labeled persistent local WAL. All runs used
the optimized Linux amd64 daemon with SHA-256
`6e83382b752e372c6434496973fc48d1a635f77fa759118fdfe8113d6b4648a5`.
The daemon uses SlateDB fork revision
`fc68f09a25defb128edfd722ec82696492dbb692` and Rust `1.98.1`.

The representative NFS runs used the profile binaries built from
`2b30207fc77a2711fe83a09bfc4cdc208961ee2e`: Vaultic SHA-256
`ffb4383328ca5dd97958aa068f0465cdd421674593092506c2fba5d6fb3d51e1` and
VaulticDB SHA-256
`dad411c615b4f530f82fffbfde19f3299c0b963eadadcb8fde10ebb209810d83`.
They ran on 32 logical Intel Xeon Gold 5217 CPUs with 292 GiB RAM. The SSD path
resolved to `/ncl1-1-vs-50/fme_dump/amakura/db-test` on
`ncl1-1-vs-55.eu.socionext.com:/fme_dump`, NFSv3/TCP with 64 KiB reads/writes.
The HDD path remained on `172.21.33.209:/volume2/NASDA2`, NFSv3/TCP with 128 KiB
reads/writes. Results are comparisons of those deployed paths, not media-only
microbenchmarks.

The native RADOS comparison used the working tree based on
`668e08a939f9c346a19889a8cbb1ee46479c7f65`, including direct main-store RADOS
support and atomic object attributes required by SlateDB retry verification.
Vaultic SHA-256 was
`47f3eb608c207ce5e1bddc3ae9107ca45a3cbf4aca7319381a39c4d4c267dd64`; the
RADOS-enabled VaulticDB SHA-256 was
`3a430bd05057c4df275608c46669395de562b0e60218d76847876e49eb45b8ad`.
Main data used pool `db-sst`; WAL used `db-wal`. Both used namespace
`vaultic-perf` with fresh, independent `phase32/668e08a93-*-45m-v2` prefixes.

The versioned object-delay profile is accepted only by a build with
`test-failpoints`, requires `target=isolated` and the process-test capability,
and validates its role, operation, and bounded delay before startup. Delay is
inside the selected object-store PUT future, below SlateDB. The client response
delay is a separate typed test option and starts only after a successful commit
RPC has returned, so it cannot be mistaken for server persistence time.

## P3 Results

Attribution enabled and disabled runs had identical input, result, checkpoints,
close, handoff, reopen, and validation. Enabled time was 693.320 ms/op versus
690.433 ms/op disabled, a 0.42% difference. This is bounded smoke-fixture
overhead, not repository-scale performance evidence.

The persistent local-WAL pilot used one mutation and one explicit durable commit
per operation with a 1 ms flush interval. Each transaction issued exactly two
WAL PUTs and one durability wait:

| Added WAL PUT delay | Commit p95 | Commit p99 |
|---:|---:|---:|
| 0 ms | 2.539 ms | 2.788 ms |
| 10 ms | 25.43 ms | 25.55 ms |
| 50 ms | 105.7 ms | 105.8 ms |
| 200 ms | 406.5 ms | 406.6 ms |

The approximately `2 * delay` slope reaches `durable_wait`; queue and writer
service remain small at 3.23-11.02 us/op and 7.07-26.22 us/op respectively. The
matched post-success response sweep leaves storage fast:

| Added response delay | Client commit p95 | Client commit p99 |
|---:|---:|---:|
| 0 ms | 2.614 ms | 2.711 ms |
| 1 ms | 3.776 ms | 4.022 ms |
| 8 ms | 11.31 ms | 11.43 ms |
| 25 ms | 28.58 ms | 28.82 ms |
| 100 ms | 103.6 ms | 103.6 ms |
| 250 ms | 254.1 ms | 254.3 ms |

Across that sweep, server commit stayed at 1.486-2.140 ms and durability wait at
1.315-1.815 ms. A separate deadline test withholds a successful response beyond
the caller timeout and proves idempotent recovery with no duplicate mutation or
lost committed state. The exact benchmark lines are retained in
[Phase 32 P3 latency raw output](phase-32-p3-latency-raw.txt).

Isolated fresh-import role pilots varied one dependency at a time:

| Profile | End-to-end | Import throughput | Finalize |
|---|---:|---:|---:|
| No injection | 693.320 ms/op | 156,892 blobs/s | 0.2756 s |
| Source load +10 ms/index | 693.156 ms/op | 148,502 blobs/s | 0.2518 s |
| Main PUT +10 ms | 847.687 ms/op | 153,931 blobs/s | 0.4219 s |
| Coordination PUT +10 ms | 747.073 ms/op | 156,029 blobs/s | 0.3270 s |

Source delay is hidden at this scale. Main and coordination PUT delays affect
the mandatory finalization tail while import-only throughput remains broadly
flat. The fixture does not show sustained eligible-work blockage that justifies
P4, a dominant Go/RPC cost for P5, or a specific SlateDB function for P6.
Increasing lanes also has no benefit. The isolated pilot alone therefore did
not select P4-P6; two lanes, deferred cleanup opt-in, and all durability checks
were retained pending representative evidence.

Representative storage sampling did reopen that decision. Matched runs used
the same source, fresh candidate settings, 32 pack workers, two publication
lanes, deferred cleanup, and a 45-minute interrupt boundary:

| Main metadata path | Boundary | Indexes | Packs | Blobs | Blobs/s | Peak prepared queue | Max RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| SSD NFS export | 45m02s | 2,369 | 126,229 | 100,525,966 | 37,200 | 13,565,280 B | 38.9 GB |
| HDD NFS export | 44m59s | 2,241 | 119,381 | 94,882,587 | 35,151 | 13,565,280 B | 24.3 GB |
| Native RADOS (`db-sst`; WAL `db-wal`) | 44m42s | 1,114 | 58,226 | 47,651,281 | 17,766 | 13,565,280 B | 20,017,120 KiB |
| RGW/S3 | 44m55s | 404 | 20,481 | 17,158,835 | 6,367 | 13,442,816 B | 4,555,272 KiB |

The selected checkpoints drained the prepared queue to zero. Before their
planned boundaries, no run reported a retry, conflict, reduction failure, or
filter fallback. Timeout cancellation caused one expected interrupted
ingest/recovery read for HDD and one reduction failure/recovery read for RADOS
while draining; exit 124 and final `context canceled` records are comparison
boundaries, not data findings. The SSD row is an in-process checkpoint from its
longer run; the HDD importer stopped at 44m59.315s within a 45m02.46s wrapper.
The RADOS wrapper ended at 45m03.08s and its final scheduler state was
`finished`, with no pending reduction or retained bytes.

Native RADOS delivered 47.8% of SSD-NFS and 50.5% of HDD-NFS blob throughput,
and 2.79 times RGW throughput. The NFS and RGW rows used revision `2b30207fc`;
RADOS required the current working tree above, so this is a matched workload
and boundary comparison rather than a single-revision backend isolation.

The scheduler evidence is decisive for P4:

| Main metadata path | Eligible-ready during reduction | Reduction blocking |
|---|---:|---:|
| SSD NFS export | 10,198 observations / 7m49.316s | 11,861 / 20m41.020s |
| HDD NFS export | 9,907 observations / 9m35.565s | 11,059 / 22m11.127s |
| Native RADOS | 6,273 observations / 12m49.730s | 4,644 / 27m11.354s |
| RGW/S3 | 2,325 observations / 15m28.918s | 1,481 / 28m42.631s |

This satisfies P4's prerequisite of eligible work waiting during synchronous
ordered reduction on representative storage. It does not prove that decoupling
will improve end-to-end throughput, so P4 remains an experiment with the
roadmap's stop condition rather than an accepted optimization. The evidence
does not isolate a P5 client/RPC target or P6 SlateDB function.

Native RADOS access was verified against the supplied three-monitor cluster,
sealed FSID, and isolated `vaultic-perf` namespace. The Go backend's live
create/CAS/range/read/reopen/delete test passed against both `db-sst` (0.44s)
and `db-wal` (0.47s), including cleanup. VaulticDB was then built in release
mode with its `rados` feature against an isolated Ceph 20.2.4 runtime; binary
SHA-256 was
`357b68761590edf492302aadb8ca31356eae7f3619c22d85af8cd0e54e2a211a`.
Its native smoke test passed. A 100-commit `db-wal` benchmark produced exactly
two WAL PUTs and one durability wait per commit:

| Commit p95 | Commit p99 | WAL PUT/op | Durable wait/op | Queue/op | Service/op |
|---:|---:|---:|---:|---:|---:|
| 4.858 ms | 18.83 ms | 2.271 ms | 3.833 ms | 3.72 us | 8.25 us |

The live benchmark is gated as `BenchmarkProcessDurableCommitRADOSWAL`, requires
an explicit RADOS-enabled binary, endpoint/pool/namespace/prefix/client
variables, and a protected `VAULTICDB_BENCH_RADOS_KEY_FILE`; ordinary test runs
skip it.

RGW compatibility was verified separately before the main-store comparison.
VaulticDB's live S3 durability/reopen test passed against both supplied RGW
endpoints, `http://172.21.33.25:7480` in 4.09s and
`http://172.21.33.24:7480` in 3.77s, using isolated prefixes in
`vaultic-phase32-perf`. Each test covered durable write, close/reopen/replay,
repository isolation, and cleanup. Credentials were loaded from a protected
local file and are not retained in commands, logs, or this record. These
preflights establish compatibility only; the 45-minute RGW main-store sample is
reported separately below.

The matched RGW main-store run used endpoint `172.21.33.25:7480`, bucket
`vaultic-phase32-perf`, and isolated prefix
`phase32/2b30207fc/import-45m`; its planned interrupt boundary is included in
the consolidated comparison above.

The timeout wrapper ended at 45m00.82s with exit 124. Before cancellation there
were no ingest failures, retries, conflicts, or filter fallback. Cancellation
interrupted one reduction and triggered one recovery read; the final queue was
drained to zero. Scheduler totals were 2,325 eligible-ready observations over
15m28.918s and 1,481 reduction-blocking observations over 28m42.631s. Commit
p50/p95/p99 were all bounded by 268.435 ms; mutation RPC p95/p99 were bounded by
536.871 ms.

The otherwise idle Ceph cluster's total-operation dashboard showed sustained
roughly 250-330 read operations/s with low bandwidth, plus infrequent read and
write bursts up to approximately 84/77 MB/s. Those are cluster-internal Ceph
operations, not one-for-one RGW requests: three-copy replication and RGW
bucket-index/metadata work amplify writes. Simultaneous high-bandwidth read and
write bursts are consistent with SST compaction. VaulticDB leaves SlateDB's
10-second manifest polling default unchanged, which cannot explain hundreds of
operations per second by itself. The evidence therefore points to remote
transaction/LSM read amplification and compaction, not a tight polling loop;
that attribution remains observational and was not independently isolated.

The principal benchmark commands were:

```console
GOMAXPROCS=4 VAULTICDB_FAILURE_TEST_BINARY="$PWD/vaulticdb/target/release/vaulticdb" \
  go test -v ./internal/index/daemon -run '^$' \
  -bench '^BenchmarkProcessDurableCommitWALLatency$' -benchtime=100x -count=1

GOMAXPROCS=4 VAULTICDB_FAILURE_TEST_BINARY="$PWD/vaulticdb/target/release/vaulticdb" \
  go test -v ./internal/index/daemon -run '^$' \
  -bench '^BenchmarkProcessDurableCommitResponseLatency$' -benchtime=100x -count=1

GOMAXPROCS=4 GOMEMLIMIT=8GiB \
  VAULTICDB_TEST_BINARY="$PWD/vaulticdb/target/release/vaulticdb" \
  VAULTICDB_TEST_EXPECTED_SHA256=6e83382b752e372c6434496973fc48d1a635f77fa759118fdfe8113d6b4648a5 \
  go test -v ./internal/index/legacyimport -run '^$' \
  -bench '^BenchmarkImportStage3Daemon$' -benchtime=3x -count=1
```

The source pilot adds `VAULTICDB_BENCH_SOURCE_LOAD_DELAY=10ms`; object pilots
add `VAULTICDB_BENCH_OBJECT_DELAY_PROFILE` with the documented version-one JSON
and respectively `role=main` or `role=coordination`, `operation=put`, and
`delay_ms=10`.

## P7 Local Validation

The current-revision frozen comparison completed all five variants:

| Variant | Time | End-to-end throughput |
|---|---:|---:|
| Stage 2 equivalent: lanes=1, deferred=false | 1,179.861 ms/op | 55,546 blobs/s |
| lanes=2, deferred=false | 2,667.902 ms/op | 24,565 blobs/s |
| lanes=2, deferred=true | 688.510 ms/op | 95,185 blobs/s |
| lanes=4, deferred=true | 690.035 ms/op | 94,975 blobs/s |
| lanes=8, deferred=true | 688.808 ms/op | 95,144 blobs/s |

The accepted two-lane deferred candidate improves end-to-end time by 41.6%
against the Stage 2 equivalent on this fixture and exceeds the 20% local gate.
Four and eight lanes are flat and are not promoted. This does not replace the
roadmap's required repository-scale comparison.

Correctness and regression gates passed:

```console
go test -race ./internal/index/legacyimport ./internal/index/daemon -count=1
rustup run stable cargo test --manifest-path vaulticdb/Cargo.toml --lib
rustup run stable cargo test --manifest-path vaulticdb/Cargo.toml \
  --bin vaulticdb -- --test-threads=1
```

The Go race suites passed both packages. Rust passed 102 library and 166 binary
tests. A default-parallel binary invocation had 164 passes and two failures in
existing process-global `CloseCache` failpoint tests; the established serial
invocation passed all 166, including the new torn-close and object-role gates.
The focused SIGKILL, post-flush/pre-handoff, role-placement, delayed durable
response, and timeout recovery tests also pass.

## Scale and External Limits

A historical uncapped run at revision `8bc9cd7cb` successfully imported 10,019
indexes, 419,530 packs, and 379,934,385 blobs through close, handoff, and reopen
in 1:54:32. It proves that revision's completion path only; it does not certify
the current changes. A current SSD run reached 6,049 imported indexes, 327,065
packs, and 254,818,044 blobs in 2h52m39s before an operator cancellation; it is
not a completion result. The matched 45-minute checkpoints above are the
accepted comparison boundary.

Representative NFS, RADOS, and RGW/S3 are no longer external blockers. Full
current-revision import, close/handoff/reopen, and post-import validation remain
P7b work. Keep the two-lane baseline while P4 is tested; do not ship decoupling,
increase lanes, or select P5/P6 from these measurements alone.