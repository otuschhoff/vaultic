# Phase 39: VaulticDB failure recovery and error contracts

Status: proposed

## 1. Purpose

Make VaulticDB failures truthful, recoverable, and machine-classifiable across the
Rust daemon and Go client boundary. This phase addresses operations that can
change durable state or ownership before returning an error, then adds
adversarial tests at each such boundary.

This specification is written as an execution plan. An implementation agent
must complete the steps in order, run the focused validation after every step,
and stop to repair a failed step before proceeding.

## 2. Problems to solve

1. Writer promotion and demotion can leave `Storage::database` unavailable while
   lifecycle and writer-role status report a usable role. Promotion can also
   retain a writer claim after failing to open the writer.
2. A failed transaction commit can consume the storage transaction without
   decrementing writer-role active transaction accounting.
3. Generation rollback can commit authority and then fail while refreshing the
   writer fence, causing a successful mutation to be reported as an ordinary
   failure.
4. Capsule migration can publish local or mirrored artifacts before recording
   enough state to resume the operation.
5. Key-management requests do not consistently enforce request context and
   drain admission.
6. Production errors frequently bypass `VaulticDbError`, so stable gRPC details
   are absent even though the taxonomy exists.
7. Transient provider and storage failures can be reported as permanent
   generation or key-management rejections.
8. The Go `Ensure` path treats every failed connection to an existing daemon as
   daemon absence and may start another process after a permanent error.
9. Transport cleanup and native RADOS multipart cleanup suppress actionable
   failures.

## 3. Scope

Primary implementation surfaces:

- `vaulticdb/src/storage.rs`
- `vaulticdb/src/storage/tests.rs`
- `vaulticdb/src/service/operations.rs`
- `vaulticdb/src/service/writer_role.rs`
- `vaulticdb/src/service/transactions.rs`
- `vaulticdb/src/service/generation.rs`
- `vaulticdb/src/service/encryption.rs`
- `vaulticdb/src/service/tests.rs`
- `vaulticdb/src/encryption/recovery_capsule/`
- `vaulticdb/src/error.rs`
- `vaulticdb/src/error/tests.rs`
- `vaulticdb/src/storage/operations.rs`
- `vaulticdb/src/storage/rados/store.rs`
- `vaulticdb/src/transport.rs`
- `internal/index/daemon/client.go`
- `internal/index/daemon/errors.go`
- corresponding Rust and Go test files
- operational documentation when observable behavior changes

Out of scope:

- changing the SlateDB version;
- changing the active-writer or generation record formats unless a step below
  explicitly requires an additive record;
- changing successful RPC responses;
- adding automatic retries for non-idempotent calls;
- changing CLI flags;
- unrelated refactoring or formatting.

## 4. Required invariants

The implementation is complete only when all of these invariants hold.

### 4.1 Writer ownership

- A daemon reports `read_write` only while it owns the current durable writer
  epoch and has an open writer database.
- A daemon reports `read_only` only while it has an open reader database and
  does not own an active writer claim.
- `Database::Unavailable` is never paired with a ready lifecycle state.
- Once a transition reaches an uncertain outcome, the daemon becomes `fenced`
  or `failed`; it must not infer the previous role.
- A failed promotion either releases the claim it acquired or remains fenced
  with enough status detail for an operator to recover it safely.
- Claim release failure after a successful conversion to a reader is not
  reported as an ordinary demotion failure that restores `read_write`.

### 4.2 Transactions

- Storage transaction ownership and writer-role active transaction accounting
  change exactly once together.
- Once commit consumes a transaction, the active transaction count decreases
  even if serialization, SlateDB commit, or durability waiting fails.
- Retrying an uncertain commit requires the existing idempotency contract; the
  service must not silently recreate the consumed transaction.

### 4.3 Durable mutations

- An RPC must distinguish "not committed" from "committed, follow-up work
  failed" whenever the distinction can be known.
- A committed generation rollback followed by fence-refresh failure fences the
  daemon and returns a machine-readable uncertain/committed outcome.
- Capsule migration can resume using the exact same serialized capsule after a
  process crash or publication failure.
- Immutable artifact publication is idempotent by digest and content.

### 4.4 Request admission and errors

- Every mutating RPC validates repository identity, request context, lifecycle
  admission, writer role, and writer fence through one documented path.
- Drain prevents new mutations, including key-management and capsule migration
  mutations, while allowing explicitly documented read-only status calls.
- Every expected domain failure crosses gRPC with `ErrorDetail`.
- `ErrorDetail.retryable` describes whether retrying the same request without
  operator correction can reasonably succeed.
- Go preserves gRPC status and exposes a stable `errors.Is` classification.
- Startup never treats authentication, repository identity, protocol, or unsafe
  endpoint errors as evidence that no daemon exists.

### 4.5 Cleanup

- Runtime metadata and Unix sockets are cleaned up on every transport exit path,
  including loader task panic and storage close failure.
- Explicit multipart `complete` and `abort` calls report cleanup failures.
- Destructor cleanup remains best-effort and non-blocking.

## 5. Execution rules for an implementation agent

For every step below:

1. Read only the named implementation function, its direct caller, and the
   closest tests before editing.
2. State a local hypothesis and identify the focused test that can falsify it.
3. Make the smallest change needed for that step. Do not combine later steps.
4. Run the focused test immediately after the first edit.
5. If it fails, repair the same slice and rerun the same test before reading or
   editing another subsystem.
6. Run Rust formatting and warning-denying Clippy for Rust steps; run focused Go
   tests for Go steps.
7. Record any intentional wire or durable-format change in this document and in
   the relevant operational documentation.
8. Do not regenerate protobuf code unless the protocol step explicitly changes
   `daemon.proto`.
9. Do not commit generated files whose source schema did not change.
10. Preserve unrelated worktree changes.

Use one commit per numbered implementation step. Each commit must build and pass
its focused tests independently.

## 6. Step 0: Establish failure-injection seams

Goal: make fallible boundaries deterministic in tests without changing runtime
behavior.

### Changes

1. Inventory the operations that occur after ownership or durable state can
   change:
   - writer flush;
   - reader/writer close;
   - reader/writer open;
   - writer claim and release;
   - transaction commit and durability wait;
   - generation authority publication;
   - fence refresh;
   - local and mirrored capsule publication;
   - migration state publication;
   - runtime cleanup;
   - RADOS staged-object deletion.
2. Reuse an existing trait, injected object store, or test driver wherever one
   already controls a boundary.
3. Add a narrow test-only failpoint abstraction only where no existing seam can
   trigger the failure. Keep it private to the owning module and compile it only
   for tests when practical.
4. Failpoints must identify a single boundary and support fail-once behavior.
   Do not introduce sleeps or timing-dependent tests.
5. Add a smoke test proving one selected failpoint fires once and normal
   behavior resumes afterward.

### Focused validation

```sh
cargo test --manifest-path vaulticdb/Cargo.toml --lib failpoint
```

### Exit criteria

- Every later fault-injection test can fail one named boundary deterministically.
- Production behavior and public APIs are unchanged.

## 7. Step 1: Make writer transitions truthful

Goal: prevent role, lifecycle, database, and durable claim from diverging.

### Design

Introduce an internal transition result that reports the final facts instead of
returning only `Result<()>`. The exact Rust names may follow local conventions,
but the result must carry:

- whether an open reader, open writer, or unavailable database remains;
- whether this daemon may still own a durable writer claim;
- the observed or acquired epoch when known;
- whether the transition completed, rolled back, or has an uncertain outcome;
- the original error with context.

Do not restore lifecycle state based solely on the requested transition. Derive
the resulting lifecycle from these facts.

### Demotion sequence

1. Reject demotion while storage transactions exist.
2. Enter role `demoting` and lifecycle `demoting` under the existing transition
   serialization guard.
3. Flush and close the writer.
4. Open the reader.
5. Release the writer claim.
6. Report `read_only` only after reader open and claim release both succeed.
7. On failure before writer close, retain `read_write` only if the writer is
   demonstrably still open and the current epoch is still owned.
8. On failure after writer close or where ownership is uncertain, report
   `fenced` or `failed`, set storage unavailable when necessary, and include the
   failed boundary in lifecycle detail.
9. If the reader opened but claim release failed, keep the reader but report
   `fenced`; never restore `read_write`.

### Promotion sequence

1. Enter role `promoting` and lifecycle `promoting` under the transition guard.
2. Acquire the next writer claim using the existing conditional update.
3. Close the reader and open the writer.
4. Publish `read_write` only after writer open and epoch installation succeed.
5. If a failure occurs after claim acquisition, attempt exact release of only
   the newly acquired epoch when doing so cannot release another writer's claim.
6. If release succeeds and the reader can be reopened, return to `read_only`.
7. Otherwise transition to `fenced` or `failed`, retaining the acquired epoch in
   status detail. Never report `read_only` with an outstanding claim.

### Tests

Add table-driven tests for a failure at each numbered fallible boundary. Every
case must assert:

- database variant;
- lifecycle phase and readiness;
- writer-role state;
- durable active claim and epoch;
- subsequent read/write RPC behavior;
- whether restart can recover.

Include successful promotion and demotion regression cases.

### Focused validation

```sh
cargo test --manifest-path vaulticdb/Cargo.toml --lib storage::tests::writer_
cargo test --manifest-path vaulticdb/Cargo.toml --lib service::tests::writer_
```

### Exit criteria

- No transition error path blindly restores the requested previous lifecycle.
- `Database::Unavailable` is always non-ready.
- Tests cover every operation after `Database::Unavailable` is installed and
  every operation after a new writer epoch is claimed.

## 8. Step 2: Couple transaction ownership and accounting

Goal: decrement active transaction accounting exactly when storage consumes a
transaction.

### Design

Use an ownership guard or a structured commit outcome. The service must not
infer transaction consumption from `result.is_ok()`.

Preferred shape:

1. The service opens a transaction accounting guard when `begin` succeeds.
2. Storage transaction removal transfers ownership to commit or rollback.
3. A guard closes writer-role accounting exactly once when storage consumes the
   transaction, independent of the final operation result.
4. Errors before removal leave both the transaction and count active.
5. Errors after removal consume both.

Avoid decrementing by transaction ID from two independent code paths.

### Tests

Cover:

- unknown transaction ID;
- idempotency conflict before removal;
- idempotency record serialization failure if triggerable;
- SlateDB commit failure;
- durability-wait failure;
- rollback success and rollback of an unknown transaction;
- duplicate commit/rollback;
- demotion after every case.

For each case, assert both `Storage::active_transactions()` and writer status
`active_transactions`.

### Focused validation

```sh
cargo test --manifest-path vaulticdb/Cargo.toml --lib transaction
```

### Exit criteria

- Storage and writer-role transaction counts cannot diverge.
- A consumed failed transaction does not block demotion.

## 9. Step 3: Reconcile committed generation rollbacks

Goal: represent a rollback that committed before fence refresh failed.

### Design

1. Make `rollback_generation` return a result that identifies whether authority
   was durably changed.
2. After durable rollback, attempt writer fence refresh.
3. If refresh succeeds, return the normal success response.
4. If refresh fails, immediately fence the in-memory writer role and lifecycle.
5. Return a structured error indicating that the authority mutation committed
   but writer fencing is incomplete. Add an `ErrorDetail` code only if no
   existing code can express this without ambiguity.
6. Make a retry with the same rollback evidence converge on the already
   committed authority and retry fence refresh rather than reject it as a new
   decision.
7. On startup, reconcile generation authority with writer epoch before becoming
   ready as `read_write`.

Any protocol addition must be additive. Regenerate Rust and Go bindings with the
repository-pinned protobuf toolchain and verify that only expected generated
files change.

### Tests

- failure immediately before authority publication: not committed;
- failure immediately after publication: committed, daemon fenced;
- retry after the committed failure: converges and refreshes the fence;
- restart after the committed failure: cannot become writable on the old epoch;
- Go classification of the resulting structured detail.

### Focused validation

```sh
cargo test --manifest-path vaulticdb/Cargo.toml --lib generation
go test ./internal/index/daemon -run 'Generation|RPCError'
```

### Exit criteria

- No committed rollback is returned as an indistinguishable ordinary failure.
- A stale writer cannot continue after rollback publication.

## 10. Step 4: Make capsule migration resumable

Goal: recover deterministically after failure between capsule creation,
publication, and migration-state recording.

### Durable state

Add a migration intention record before external publication. It must contain or
reference:

- schema version;
- repository ID and target generation;
- exact serialized capsule bytes or an immutable object containing them;
- capsule SHA-256;
- requested local destination identity;
- local publication completion;
- mirror publication completion and path;
- finalization state.

The record update mechanism must follow existing conditional/idempotent storage
patterns. Never regenerate capsule bytes while an unfinished intention exists
for the same logical migration.

### Execution sequence

1. Validate and create the capsule once.
2. Serialize it canonically and compute its digest.
3. Persist the intention containing the exact bytes before publishing files.
4. Publish locally by digest; identical existing content is success and
   different content is a conflict.
5. Mark local publication complete.
6. Publish the mirror with the same idempotency rule.
7. Mark mirror publication complete.
8. Return the exact bytes and paths recorded by the intention.
9. Finalization verifies the supplied digest and both publication completions,
   then atomically marks the migration finalized.
10. A retry resumes the first incomplete step.

Document retention or cleanup of completed intentions. Do not delete an
unfinished intention automatically.

### Tests

Fail before and after every numbered durable/publication boundary. For each
case, restart the service and retry. Assert that:

- capsule bytes and digest never change;
- no conflicting immutable artifact is produced;
- completed work is not repeated destructively;
- finalization succeeds exactly once;
- a mismatched request or artifact is rejected with a structured conflict.

### Focused validation

```sh
cargo test --manifest-path vaulticdb/Cargo.toml --lib capsule_migration
```

### Exit criteria

- Every partial publication state is resumable after process restart.
- Migration state is sufficient to explain what remains incomplete.

## 11. Step 5: Unify mutation admission

Goal: apply repository, context, lifecycle, writer-role, and fence checks
consistently to every mutating RPC.

### Changes

1. Inventory all service handlers and classify them as health/status, read-only,
   mutation, or lifecycle control.
2. Introduce or extend one mutation-admission helper that performs, in order:
   - repository and authentication validation;
   - `RequestContext` validation;
   - lifecycle drain/stop rejection;
   - storage availability check;
   - generation mutation interlock when applicable;
   - writer fence and role admission.
3. Keep Unix-socket-only key-management policy as an additional check, not a
   replacement for common admission.
4. Convert key slot, key rotation, recovery capsule, and capsule migration
   mutations to this path.
5. Keep explicitly read-only key and migration status handlers available during
   drain only if existing shutdown semantics permit reads.

### Tests

Use a table listing every mutating RPC. For every row test:

- missing context;
- expired context;
- repository mismatch;
- draining lifecycle;
- fenced writer;
- successful admission.

### Focused validation

```sh
cargo test --manifest-path vaulticdb/Cargo.toml --lib service::tests::request_
cargo test --manifest-path vaulticdb/Cargo.toml --lib service::tests::drain_
```

### Exit criteria

- No mutating handler assembles a partial subset of the common checks.
- Drain admits no new key-management or capsule mutation.

## 12. Step 6: Complete the structured error contract

Goal: ensure expected production failures use stable details and accurate retry
classification.

### Error matrix

Create a table in `vaulticdb/src/error.rs` tests and operational documentation
covering at least:

| Condition | gRPC code | Detail code | Retryable |
|---|---|---|---|
| writer fenced | `Aborted` | `writer_fenced` | false |
| writer transitioning | `Unavailable` | `writer_transitioning` | true |
| storage temporarily unavailable | `Unavailable` | `storage_unavailable` | true |
| provider timeout/unavailable | `Unavailable` | provider-specific or `storage_unavailable` | true |
| generation decision conflict | `FailedPrecondition` | `generation_changed` | false |
| idempotency conflict | `Aborted` | `idempotency_conflict` | false |
| invalid request field | `InvalidArgument` | `invalid_request` | false |
| encryption integrity failure | `DataLoss` | `encryption_integrity` | false |
| authentication failure | `Unauthenticated` | stable authentication code | false |
| authorization failure | `PermissionDenied` | stable authorization code | false |
| committed but reconciliation pending | explicit choice from Step 3 | stable code | true |

### Changes

1. Replace bare `Status` construction for expected domain failures with
   `VaulticDbError` conversion.
2. Replace `storage_error` with typed mapping that distinguishes availability,
   conflict, invalid input, integrity, and unexpected internal failures.
3. Preserve source chains for server logs without exposing secrets to clients.
4. Split generation and key-management wrappers so provider availability is not
   collapsed into permanent business rejection.
5. Extend Go sentinels and `daemonErrorKind` only for new stable detail codes.
6. Ensure Go `RPCError` continues to preserve the original gRPC status and
   supports `errors.Is`.
7. Add one Rust daemon/Go client integration test per detail family. Synthetic
   decoding tests alone are insufficient.

### Focused validation

```sh
cargo test --manifest-path vaulticdb/Cargo.toml --lib error
go test ./internal/index/daemon -run 'RPCError|ErrorDetail|Classification'
```

### Exit criteria

- Expected production failures do not bypass `ErrorDetail`.
- Retryability is based on cause, not broad subsystem name.
- Tests observe details emitted by a real Rust daemon.

## 13. Step 7: Classify existing-daemon connection failures

Goal: start a daemon only when the endpoint is absent or transiently
unreachable.

### Changes

1. Change `connectExistingDaemon` to return a three-way result:
   - connected client;
   - endpoint absent/transiently unavailable, startup may proceed;
   - permanent rejection, return immediately.
2. Treat authentication, repository mismatch, incompatible protocol/version,
   malformed runtime metadata, unsafe socket permissions, and terminal daemon
   `failed` lifecycle as permanent for this `Ensure` invocation.
3. Treat missing socket, refused connection during bounded startup, and
   retryable loading/transition states according to existing retry policy.
4. Preserve the original classified error. Do not replace it with a later
   singleton or timeout error.
5. Do not remove an existing endpoint on a permanent rejection.

### Tests

Add table-driven `Ensure` tests with a fake or test daemon for:

- no endpoint;
- stale endpoint;
- loading daemon that becomes ready;
- daemon in terminal `failed` state;
- wrong authentication token;
- repository mismatch;
- incompatible protocol;
- unsafe endpoint metadata;
- transient refusal followed by readiness.

Assert whether process launch was attempted and which error is returned.

### Focused validation

```sh
go test ./internal/index/daemon -run 'Ensure|ExistingDaemon'
```

### Exit criteria

- Permanent existing-daemon errors never trigger process launch.
- The first actionable error reaches the caller unchanged through wrapping.

## 14. Step 8: Guarantee transport cleanup

Goal: clean runtime artifacts on every daemon transport exit path.

### Changes

1. Introduce a small runtime-artifact guard owned by each transport branch.
2. Register the Unix socket, metadata path, and singleton lock immediately after
   successful creation/acquisition.
3. Move cleanup into the guard so early `?`, loader join error, lifecycle
   transition error, server error, and storage close error all use one path.
4. Preserve the primary operation error. Join or log cleanup errors according to
   existing cleanup policy without replacing the primary cause.
5. Keep lock release ordered after the service stops accepting requests and
   before stale artifacts can be reused.

### Tests

- storage loader returns an error;
- storage loader task panics;
- gRPC server returns an error;
- stopping lifecycle transition fails;
- storage close fails;
- normal Unix and TCP shutdown.

Assert socket/metadata removal and lock reacquisition where applicable.

### Focused validation

```sh
cargo test --manifest-path vaulticdb/Cargo.toml --lib transport
```

### Exit criteria

- No transport branch has cleanup only after a fallible `?` exit.
- Runtime artifacts are absent after every tested exit.

## 15. Step 9: Report explicit RADOS multipart cleanup failures

Goal: make explicit multipart completion and abort report leaked staging data.

### Changes

1. Change `cleanup_staged` to return a result containing all deletion and worker
   join failures.
2. On `abort`, return cleanup failure and mark the multipart finished so callers
   cannot continue using a partially aborted upload.
3. On `complete`, preserve successful final object publication but return a
   cleanup error that clearly states the final object may already exist.
4. Do not retry final object publication solely because staging cleanup failed.
5. Keep `Drop` cleanup best-effort; optionally log failures if the module has an
   existing safe logging facility.
6. Keep stale staging sweep behavior best-effort only if startup availability is
   intentionally preferred; document and test that choice.

### Tests

- one staged deletion fails during abort;
- multiple deletions fail and are all represented;
- worker task fails;
- final publication succeeds and cleanup fails;
- retry does not corrupt or republish different final content;
- destructor remains non-panicking.

### Focused validation

```sh
cargo test --manifest-path vaulticdb/Cargo.toml --lib storage::rados::store::tests
```

### Exit criteria

- Explicit callers can observe staging cleanup failure.
- Errors distinguish pre-publication failure from post-publication cleanup
  failure.

## 16. Step 10: Adversarial integration matrix and documentation

Goal: verify cross-subsystem behavior and make operator-visible outcomes clear.

### Integration matrix

Add process-level tests for these scenarios:

1. Promotion claims an epoch and writer open fails.
2. Demotion closes the writer and reader open fails.
3. Demotion opens the reader and claim release fails.
4. Commit is accepted but durability wait fails.
5. Generation rollback commits and fence refresh fails.
6. Capsule migration crashes after each publication boundary.
7. Drain races each class of mutation.
8. Storage provider becomes unavailable and recovers.
9. Daemon loader panics after binding its socket.
10. Existing daemon rejects authentication during `Ensure`.

Each test must assert:

- RPC code and decoded `ErrorDetail`;
- lifecycle state, detail, readiness, and timestamp behavior;
- writer role and epochs;
- durable claim or migration record;
- behavior after retry;
- behavior after process restart;
- absence of leaked runtime or staging artifacts where applicable.

### Documentation

Update `doc/078_operational_resilience.rst` with:

- transition failure states and operator actions;
- meaning of committed-but-reconciliation-pending errors;
- retryability contract;
- capsule migration resume behavior;
- startup behavior for permanent existing-daemon rejection;
- RADOS cleanup warning semantics.

Document only behavior implemented and verified in previous steps.

### Focused validation

```sh
cargo test --manifest-path vaulticdb/Cargo.toml --tests
go test ./internal/index/daemon ./cmd/vaultic/indexcmd
```

### Exit criteria

- Every irreversible boundary has a deterministic failure test.
- Operator documentation matches emitted lifecycle and error details.

## 17. Final validation

Run these commands only after Steps 0 through 10 pass independently:

```sh
cargo fmt --manifest-path vaulticdb/Cargo.toml -- --check
cargo test --manifest-path vaulticdb/Cargo.toml --all-targets
cargo clippy --manifest-path vaulticdb/Cargo.toml --all-targets -- \
  -D warnings \
  -A clippy::too_many_lines \
  -A clippy::cognitive_complexity \
  -A clippy::unwrap_used \
  -A clippy::expect_used \
  -A clippy::too_many_arguments \
  -A clippy::type_complexity \
  -A clippy::chunks_exact_to_as_chunks \
  -A clippy::large_enum_variant \
  -A clippy::result_large_err
go test ./internal/index/daemon ./cmd/vaultic/indexcmd
git diff --check
```

If `daemon.proto` changed, additionally run the pinned protobuf regeneration and
verify the generated diff is limited to:

- `vaulticdb/proto/` source schema changes;
- `internal/index/proto/` Go bindings;
- Rust build output references expected by the existing build process.

Do not accept tests that rely on sleeps, wall-clock races, or error-message
substring matching.

## 18. Completion checklist

- [ ] Failure-injection seams are deterministic and test-only where practical.
- [ ] Promotion and demotion preserve truthful database, role, lifecycle, and
      claim state on every failure path.
- [ ] Transaction ownership and active accounting cannot diverge.
- [ ] Committed generation rollback is distinguishable and recoverable.
- [ ] Capsule migration resumes from exact persisted bytes.
- [ ] Every mutation uses common request and lifecycle admission.
- [ ] Production errors emit stable structured details with accurate
      retryability.
- [ ] Go `Ensure` does not launch after permanent existing-daemon rejection.
- [ ] Transport artifacts are cleaned on every exit path.
- [ ] Explicit RADOS cleanup failures reach callers.
- [ ] Adversarial restart tests cover every irreversible boundary.
- [ ] Operational documentation is updated.
- [ ] Full Rust, Go, formatting, lint, and patch-hygiene validation passes.

## 19. Stop conditions

Stop implementation and revise this design before proceeding if any of these
conditions occur:

- exact writer-claim release cannot prove it is releasing only this daemon's
  acquired epoch;
- SlateDB cannot expose whether a failed commit may already be durable;
- capsule bytes cannot be persisted before external publication without a
  durable-format change larger than the intention record described here;
- lifecycle cannot represent an uncertain transition without reporting ready;
- a new error detail would require a non-additive protocol change;
- failure injection would require production timing changes or sleeps;
- a step changes successful RPC semantics outside this phase's scope.

When stopped, record the violated assumption, the observed evidence, and a
minimal revised design before editing further.