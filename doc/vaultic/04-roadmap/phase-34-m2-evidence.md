# Phase 34 M2 isolated-scenario evidence

This artifact records the Phase 34 M2 implementation and bounded checks taken on
2026-09-18 at base revision `78ada2031` plus the M2 worktree. The fixtures prove
scenario sensitivity, result equivalence, recovery, and durability ordering.
They are small correctness fixtures and are not representative throughput or
capacity evidence.

## Scope and safety

M2 adds a schema-v1 scenario controller with deterministic seeded assignment,
bounded delay, jitter, correlated tails, bandwidth, concurrency, deadlines,
retry faults, and explicit resource-hold semantics. Active profiles require a
matching target identity plus explicit disposable and confirmed flags. Disabled
profiles return the original dependency and do not require target confirmation.
Object names and source paths are represented only by stable hashes in event
identities.

Go adapters cover backend save/load/range/stat/list/remove/conditional write,
stream consumption, source filesystem operations, checker scratch, and
method-selective post-success RPC delivery. Capacity is shared by resource ID
only when the profile names a held resource; caller acknowledgement delay with
`holds: ["none"]` does not manufacture server queueing. Cancellation and errors
settle all active accounting.

The Rust object-store adapter is compiled into activation only with the
`test-failpoints` feature and requires the process-test capability. It accepts
the shared schema-v1 envelope, requires an exact repository target, and projects
the supported deterministic delay and bounded-concurrency fields onto one
selected role and method. Unsupported jitter, tail, bandwidth, retry, or
deadline fields fail closed. Production builds ignore the environment variable.
The adapter covers PUT and conditional PUT, multipart lifecycle operations,
GET response/body/ranges, HEAD, DELETE, LIST variants, COPY, and RENAME while
preserving native object-store options and conditional semantics.

## Action matrices

The response-delay sweep is `0/1/8/25/100/250 ms`, with three repeats per point.
Every profile uses post-completion acknowledgement delivery and
`holds: ["none"]`. The 108 cases cover:

| Action | Selected boundary | Correctness oracle |
|---|---|---|
| Backup | source read | Snapshot count plus byte-for-byte final restore; source changes each iteration to prevent incremental bypass. |
| Restore | repository range GET | Byte-for-byte restored tree. |
| Forget | repository delete | Exactly one retained snapshot and readable retained data. |
| Prune | repository delete | Exactly one retained snapshot plus successful repository check. |
| Legacy import | successful commit response | Imported pack/index counts, completion marker, persistent-WAL close/handoff/reopen, and catalog count. |
| Check | successful scan response | Exact no-delay/injected `CheckResult` parity after excluding only run-local session ID, options digest, and available-memory budget. Repository generation, sequence, inventory digest, coverage, findings, and verdict counters remain equal. |

Each run constructs and validates an `ExperimentArtifact` outside the measured
repository. Artifacts include the profile, seed, input/result digests, revisions,
runtime limits, effective concurrency, delay observations, completion criterion,
and an explicit unknown stating that the fixture is not throughput evidence.
The tests assert selected boundaries were exercised and active operations return
to zero.

## Ambiguous response recovery

The client interceptor invokes the selected hook only after a successful RPC.
Barrier tests prove committed state is readable while response delivery remains
blocked, normal server failures never enter the hook, and acknowledgement delay
does not hold service capacity. A durable commit withheld past its caller
deadline is recovered with the same idempotency identity; the durable sequence
does not advance again and committed state is neither duplicated nor lost.

## Durability fences and crash matrix

The protocol now returns a durability token containing repository generation,
writer epoch, and the real SlateDB applied sequence. `AwaitDurableThrough` uses
the latest cloneable engine write handle as a constant-space prefix fence. It
rejects zero/future sequences, stale generations, stale writer epochs, memory
WALs, unavailable handles, and WAL failures. Generation and epoch are checked
both before and after the engine wait. Generation activation takes exclusive
mutation admission, and each token uses generation and epoch captured under the
write's shared admission before apply. Normal commits retain their prior durable
acknowledgement behavior. Applied-only `CommitDeferred` remains available to the
private fresh-import memory-WAL workflow, while the explicit token-returning API
fails closed when a durable token cannot be issued.

Deterministic engine tests cover ordered sequences, concurrent coalesced waiters,
prefix coverage, future rejection, and WAL failure. Real-process tests cover
persistent-WAL close/reopen replay and stale-token rejection after restart. A
barrier-driven hard-kill matrix covers before apply, after applied acknowledgement
but before delivery, before fence, after fence, after fence but before response,
and after final publication. No blocked operation returns success before its
boundary. Every prefix whose fence completed survives restart; pre-apply state is
absent; an unfenced caller receives no resumable token. A separate memory-WAL
fresh-import kill test proves that a completion marker without clean close and
WAL handoff cannot be reopened and is removed by the required reset.

## Validation commands

Focused checks used while implementing M2 include:

```text
go test -race ./internal/telemetry -run '^TestScenario' -count=1
go test ./internal/index/maintenance -run '^TestLocationSpoolScenarioPreservesDiskResultsAndCleanup$' -count=1
go test ./internal/index/daemon -run '^TestProcess(DeferredCommitDurabilityTokens|DeferredCommitRefusesMemoryWAL|DurabilityCrashMatrix|DelayedCommitResponseRecoversCommittedSuccess|ResponseDeliveryBarrierRunsAfterCommitCompletion)$' -count=1
go test ./cmd/vaultic -run '^TestPhase34M2(BackupSourceReadSweep|RestoreRangeReadSweep|ForgetDeleteSweep|PruneDeleteSweep)$' -count=1
go test ./cmd/vaultic -run '^TestPhase34M2(ImportCommitResponseSweep|CheckScanResponseSweep)$' -count=1
cargo test --manifest-path vaulticdb/Cargo.toml object_delay_profile_ -- --nocapture
cargo test --manifest-path vaulticdb/Cargo.toml durability_fence_ -- --nocapture
```

The final acceptance run also formats Go and Rust, regenerates protobuf bindings,
runs touched Go packages with the race detector, runs the complete Rust test
suite, and verifies a clean generated-code and whitespace diff.

Final results were 183 Rust unit tests plus the version-flags integration test,
all touched non-command Go packages under `-race`, and all 108 M2 command matrix
cases under `-race`. The full unrelated `cmd/vaultic` package still has its
existing environment-sensitive `TestBackupErrors` failure (`Assumed failure,
but no error occurred`); the focused M2 command suite passes in the same
environment and does not modify that test or its injection path.

## Deferred work

M2 does not authorize production telemetry wiring, UI/export changes, or
throughput conclusions. Broad production boundary instrumentation remains M3.
Representative current-revision infrastructure runs and throughput comparisons
remain M7 inputs. Rust deliberately fails closed for shared-profile dimensions
that its object-store adapter does not implement; such profiles must use the Go
scenario engine or wait for a separately reviewed Rust extension.
