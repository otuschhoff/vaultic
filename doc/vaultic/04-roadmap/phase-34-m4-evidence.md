# Phase 34 M4 authenticated-snapshot evidence

This artifact records Phase 34 M4 at base revision `3fab26729` plus the M4
worktree on 2026-09-19. M4 completes bounded component snapshot handoff over
existing protected protocols. It does not certify M5 terminal interaction or
M6 exporter behavior.

## Handoff boundaries

| Component | Protected channel and bounded handoff |
|---|---|
| `vaultic` | The aggregate owner retains exact local process and operation accounting even when repository open fails. Missing remote observations remain explicit `unavailable` components. |
| VaulticDB | Existing bearer-authenticated gRPC `WriterStatus` and `CacheStatus` calls have explicit 1 MiB receive limits. The server accepts at most 63 cache tiers, leaving one bounded aggregate record, and status aggregates shared-ledger accounting once before acquiring local capacity state. |
| Key broker | The existing owner-only Unix socket serves a dedicated negotiated `monitor_status` operation. Its fixed response contains protocol, process start, random per-process instance ID, capture time, lock state and nonnegative session/lease counts only. Incremental framing rejects responses above 1 MiB and closes the connection. |

Components generate and serve statistics only. Polling, validation, aggregation,
rendering and export remain owned by `vaultic`; no component-native monitor CLI,
poller or exporter was added.

## Compatibility, identity and availability

The shared JSON schema accepts unknown additive fields and rejects unsupported
major schema versions. VaulticDB keeps its existing protocol/schema checks; the
broker requires `vaultic-key-broker.v1` while ignoring additive response fields.
Negative broker counters, missing process identity, capture time before process
start and incompatible protocol values are rejected.

Restart identity is the tuple of process start time and an instance identifier.
VaulticDB restart coverage verifies the tuple changes across real process
restarts. The broker creates a random instance ID once per process and includes
it in every monitoring response, avoiding millisecond-start collisions.

Remote captures predating the current collection are marked `stale`. A blocked,
failed or absent component becomes an explicit unavailable/stale component while
other collectors and local accounting still complete. Counter reset detection
therefore remains keyed to stable process identity rather than treating a
restart as ordinary counter growth.

## Bounds, isolation and redaction

Broker responses are bounded while reading, before an unbounded line can be
allocated. VaulticDB monitoring responses have per-call receive caps independent
of the larger data transport allowance. Cache configuration enforces source-side
cardinality and the Go projection retains deterministic truncation as
defense-in-depth, disclosing dropped records and estimated availability.

Broker monitoring uses `try_lock` and fails immediately when key-management state
is busy. It does not increment production broker-operation telemetry. Cache
status performs one shared-ledger pass outside the capacity lock; a concurrent
test repeatedly renders all 63 tiers while an actual cache read completes.
Independent aggregate collectors use the caller deadline and buffered one-shot
handoffs, so one blocked component cannot suppress another or retain a sender.

The deterministic aggregate golden contains exact, stale and unavailable
observations. It proves that broker repository/capsule/policy details and
VaulticDB transition/cache-policy errors are absent from exported JSON.

## Authorization and validation

Focused acceptance commands include:

```text
go test -race ./internal/index/broker ./internal/index/daemon ./internal/telemetry ./cmd/vaultic
cargo test --manifest-path vaulticdb/Cargo.toml --bin vaultic-key-broker
cargo test --manifest-path vaulticdb/Cargo.toml storage::cache::tests
```

The tests cover valid, missing and wrong bearer tokens for both VaulticDB status
methods over real TCP; owner-only broker socket permissions; broker protocol
negotiation; fixed redacted response shape; fail-fast lock contention; 1 MiB
response rejection on both transports; additive compatibility and major-version
rejection; stale/unavailable fallback; local-accounting preservation; process
restart identity; exact cache cardinality; aggregate golden JSON; and collector
timeout isolation.

Repository-wide validation and any unrelated baseline failures are recorded in
the completing commit message. The focused Go race suites passed. The complete
Rust suite passed (102 library, 12 broker, 2 custodian, 185 server and 1 version
test). Formatting and diff checks passed.

The repository-wide Go run retained unrelated baseline failures documented by
M3: the root-only archiver unreadable-file panic, the environment-sensitive
command-package failure, and the production-discard allowlist check. The M4
broker close discard found by that check was repaired; the remaining 11 entries
are outside this change. Two final independent findings-only reviews reported
exactly `No actionable findings.`