# Phase 32 P2 Attribution Evidence

This record closes P2 against the frozen P0 fixture. The final Linux amd64
VaulticDB binary was built with Rust `1.98.1`, release optimization, debug
symbols, and the `test-failpoints` feature. Its SHA-256 is
`3699f624c6d06e2b2f15a4f7277278d5e47a36422120cc7c2cf5a2cb328ffc7a`.
Cargo pins `slatedb`, `slatedb-common`, and `slatedb-txn-obj` version `0.16.0`
to the published fork revision
`fc68f09a25defb128edfd722ec82696492dbb692`; the binary's embedded dependency
report attests the direct `slatedb` and `slatedb-common` identities. That
revision contains parent `a970051`, which introduced batch-writer queue and
service attribution.

## Attribution Contract

The existing `WriterStatus` RPC carries one additive, fixed-cardinality
`AttributionSnapshot`; legacy responses with no snapshot remain valid. Timing
rows use saturating counters and eight fixed microsecond buckets. Attempts start
with guard creation, active and oldest-active state remains visible while work is
stalled, completed `Err` results settle as failures, and dropped futures settle
as cancellations through RAII. The collector uses a synchronous mutex only for
active-start bookkeeping and never holds it across an await.

Service/storage scopes are admission wait and hold, fence validation,
write-batch/begin/commit/rollback requests, SlateDB transaction begin, aggregate
engine submit, explicit durable-handle wait, and full storage finalization.
Deferred commits do not create durable-wait attempts. `engine_submit` contains
the three separately reported fork stages but excludes explicit durability:

| Engine stage | Start | End |
|---|---|---|
| Backpressure | First observed WAL/memtable pressure | Pressure clears or the request fails/cancels, before enqueue |
| Batch queue | Enqueue after backpressure | Writer receipt, failed send, or dispatcher drain |
| Batch service | Writer receipt | Conflict check and in-memory/WAL apply return |

The fork also exposes fixed counters and gauges for write batches/operations,
backpressure and L0-stall reasons, memtable and WAL bytes, immutable flushes,
L0/SST/sorted-run state, and compaction bytes/SSTs/running work.

Object-store attribution is split by role:

| Role | Coverage |
|---|---|
| Main | SlateDB metadata/SST/manifest operations on the primary store |
| WAL | A distinct WAL store, or WAL-prefixed paths tagged inside a shared primary store |
| Coordination | Writer claims, local-WAL handoff, and other control-store operations |

Each role has fixed PUT, multipart-init/part/complete/abort, GET/body/ranges,
HEAD, delete, list variants, copy, and rename rows. Stream and multipart guards
remain active through completion, error, or cancellation. Transferred-byte
availability is explicit per operation. The current object-store API does not
expose its internal retry delay, background pressure, or timeout classification;
their availability flags are false rather than inferred.

## Validation

The published fork worktree was clean at `fc68f09a` and its full workspace suite
passed. Focused fork tests covered successful, failed, cancelled, and timed-out
backpressure; queue receipt and dispatcher drain; writer-service success,
failure, and cancellation; active/oldest cleanup; and unchanged transaction,
flush, replay, and write behavior.

The complete VaulticDB suite passed: 102 library tests, 11 broker tests, two
custodian tests, 164 daemon tests, and one version test. Focused tests covered
collector concurrency and drop cleanup, failed transaction begin and
finalization, failed submit and durability, deferred-durability exclusion,
engine snapshot conversion, and role-aware object streams/multipart operations.

```console
cargo test --manifest-path vaulticdb/Cargo.toml
cargo test --manifest-path vaulticdb/Cargo.toml --features test-failpoints \
  -- --test-threads=1
cargo fmt --manifest-path vaulticdb/Cargo.toml --check
rustfmt +stable --edition 2021 --check vaulticdb/src/service/operations.rs \
  vaulticdb/src/storage/object_store.rs vaulticdb/src/storage/role_tests.rs
go test ./internal/index/daemon ./internal/index/legacyimport \
  ./cmd/vaultic/indexcmd -count=1
```

The final optimized binary also passed the real-daemon process test. A held
mutation showed admission completed while the request remained active; release
advanced submit, durability, queue/service, and engine counters. A forced
takeover then made the stale daemon record both a failed fence check and a failed
write-batch request.

```console
VAULTICDB_TEST_BINARY="$PWD/bin/profile/linux-amd64/vaulticdb" \
VAULTICDB_TEST_EXPECTED_SHA256=3699f624c6d06e2b2f15a4f7277278d5e47a36422120cc7c2cf5a2cb328ffc7a \
go test ./internal/index/daemon \
  -run '^TestProcessWriterStatusAttributesServiceAndEngineBoundaries$' -count=1
```

Strict Clippy remains blocked by pre-existing repository-wide warnings outside
the P2 files, including test `unwrap_used` and production `too_many_lines`,
`type_complexity`, and `large_enum_variant` findings. No P2-local warning was
reported before those baseline failures.

## Enabled/Disabled Overhead

The final benchmark binary was explicitly built with `test-failpoints` and
supports a process-test-only disable gate. Disabling requires both
`VAULTICDB_TEST_ATTRIBUTION_DISABLED=true` and
`VAULTICDB_TEST_CAPABILITY=vaulticdb-process-tests-v1`. The Go launcher does not
forward either variable from its ambient environment. Ordinary production builds
omit `test-failpoints` and compile the disable path to false; separate unit tests
cover both configurations. Enabled and disabled runs used identical P0 input,
`GOMAXPROCS=4`, `GOMEMLIMIT=8GiB`, `-benchtime=3x`, and all five variants. Every
fresh candidate completed mark, close, local-WAL handoff, reopen, and checkpoint
verification.

```text
variant                     enabled ns/op  disabled ns/op  delta      bytes delta  alloc delta
lanes=1 deferred=false       1171029572     1166256300     +0.409%    +0.316%      -0.0020%
lanes=2 deferred=false       2674875303     2669350826     +0.207%    +0.107%      +0.0013%
lanes=2 deferred=true         685591729      689934026     -0.629%    -0.036%      +0.0035%
lanes=4 deferred=true         705374165      695720020     +1.388%    -0.265%      +0.0006%
lanes=8 deferred=true         685950014      688517312     -0.373%    -0.024%      -0.0013%
```

Raw enabled and disabled output SHA-256 values are respectively
`a391439304a28a3bae6f8bbd519a38d664438c6228384d2e5a1f8d114879f3a8` and
`6f1995e0afe10aa03bd31c9477442d42e6ce7ae80bcfe42ada1d0b76ce95653b`.
Runtime stayed between `-0.629%` and `+1.388%`, bytes between `-0.265%` and
`+0.316%`, and allocations within `0.0035%`. This bounds obvious P2 overhead on
the smoke fixture; it is not P3 repository-scale performance evidence.