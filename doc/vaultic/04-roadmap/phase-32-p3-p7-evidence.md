# Phase 32 P3-P7 Experiment and Acceptance Evidence

This record closes the currently executable Phase 32 work as of 2026-09-17.
P3a, P3b, and P7a are validated on isolated resources. P3c remains partial: its
memory-WAL role pilots pass, but the response matrix, durable-WAL import, and
flush/compaction-scale gates are incomplete. P4-P6 are not selected because the
measurements do not identify a qualifying implementation target, and the
existing two-lane baseline remains unchanged. P3d's representative combined
profiles and P7b's current uncapped import remain blocked pending authorized
NFS, RADOS, and S3 resources. No result below is presented as representative
hardware or repository-scale acceptance.

## Frozen Inputs and Build

The real-daemon fixture contains four ordered indexes, 128 preselected packs,
and 65,536 blobs. Import runs used `GOMAXPROCS=4`, `GOMEMLIMIT=8GiB`, and three
measured iterations; latency sweeps used 100 operations per point. Fresh import
used memory WAL unless explicitly labeled persistent local WAL. All runs used
the optimized Linux amd64 daemon with SHA-256
`6e83382b752e372c6434496973fc48d1a635f77fa759118fdfe8113d6b4648a5`.
The daemon uses SlateDB fork revision
`fc68f09a25defb128edfd722ec82696492dbb692` and Rust `1.98.1`.

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
Increasing lanes also has no benefit. P4-P6 are therefore not selected on the
available evidence; two lanes, deferred cleanup opt-in, and all durability
checks are retained. Representative P3d evidence may reopen that decision.

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
the current changes. A current uncapped run was not launched because the
available mounted data was not authorized for destructive isolated-candidate
testing.

No representative HDD-array NFS, native three-replica RADOS, or cloud S3
environment was available. Consequently P3d and the representative/current-
scale portion of P7b remain blocked, and Phase 32's external acceptance status
is partial. The code/default decision is complete: retain the baseline and hand
the measured WAL, response, main-store, and coordination sensitivities to Phase
34 rather than shipping an unsubstantiated optimization.