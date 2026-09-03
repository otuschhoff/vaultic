# Phase 1: Protocol contract and daemon lifecycle

[← Back to roadmap index](00-overview.md)

[← Phase 0](phase-00-freeze-assumptions-and-native-build.md) · [Phase 2 →](phase-02-engine-abstraction-and-legacy-adapter.md)

**Goal:** establish a secure, versioned process boundary before metadata logic.

**Current implementation state (2026-08-27):** **complete.** The generated,
versioned protobuf contract includes lifecycle, request-context, error,
batch, scan, and transaction envelopes; the service exposes only
`Health`, `Capabilities`, `Drain`, and `Shutdown`, so it cannot mutate
SlateDB. The Go client can attach to and validate a compatible Unix daemon or
start one on demand. It now supplies explicit TCP transport, CIDR allowlist,
and bearer-token configuration when starting an opt-in TCP daemon, uses bounded
connection attempts, records endpoint-specific ownership, and forcibly reaps
owned children on startup or shutdown deadlines. The Rust daemon has a
process-lifetime advisory endpoint lock, allowing a stale socket to be removed
only after exclusive ownership is established; it also creates PID/capability
metadata, private Unix socket directories, bounded gRPC message handling,
per-connection concurrency limits, CIDR admission, and authenticated TCP
lifecycle requests.

Local verification covers protocol compatibility, Unix and TCP attachment,
Unix RPC smoke/shutdown cleanup, native SlateDB smoke, advisory-lock recovery,
repository-scoped endpoints, stale-socket recovery, Unix permission checks,
compiled-daemon startup, and concurrent Unix/TCP startup races under Go's race
detector. Process tests cover missing and incorrect authentication on every
lifecycle RPC, CIDR rejection, truthful transport and work-limit capabilities,
required request IDs, expired deadlines, oversized messages, cancellation
cleanup, bounded shutdown, and the `Drain` ready-to-draining transition. Pure
Rust validators enforce the advertised batch and scan-page limits before those
envelopes are connected to storage RPCs. CI builds through `cargo zigbuild`,
runs the Linux musl native smoke path, retains the no-CGO vaultic build, and
runs the compiled-daemon Go race suite.

**Requirements to complete Phase 1:**

1. Enforce `RequestContext`: reject expired `deadline_unix_ms` values before
  handler work begins, require or generate request IDs for structured
  diagnostics, and honor gRPC cancellation consistently.
2. Define lifecycle state: make `Drain` transition the service from ready to
  draining, reject new non-lifecycle work, expose the state through `Health`,
  and make `Shutdown` drain and terminate within a bounded deadline. Remove
  PID/capability metadata on bind/startup errors as well as normal shutdown and
  signals.
3. Harden endpoint lifecycle: derive default runtime/socket paths from the
  repository identity; validate socket ownership and permissions before
  attaching; acquire the endpoint lock and verify that no compatible daemon is
  serving before removing a stale socket; and prove concurrent `Ensure` calls
  and crash recovery converge on one daemon.
4. Complete TCP policy: test Go-launched TCP daemons; prove that a listener is
  impossible without both a non-empty CIDR allowlist and bearer token; test
  allowlist rejection and missing/wrong-token rejection on every lifecycle
  RPC; and verify `Capabilities` reports the selected transport accurately.
5. Bound runtime work: test the advertised 16 MiB gRPC message limit; validate
  advertised page and batch limits in the request envelopes before storage RPCs
  exist; and add bounded concurrency/backpressure behavior for both Unix and
  TCP transports.

**Required Phase 1 tests:** use the compiled `vaulticdb` process, rather than
only in-process mocks, to cover two racing `Ensure` clients, compatible reuse,
protocol/schema/repository mismatch rejection, stale socket/metadata/lock
recovery, Unix `0700` directory and `0600` socket permissions, startup timeout
and cancellation cleanup, TCP default/allowlist/authentication behavior,
expired deadlines, oversized requests, and `Drain`/`Shutdown` state
transitions. CI must build through `cargo zigbuild --target
x86_64-unknown-linux-musl --release`, matching the verified artifact path, and
run the resulting Linux musl binary in addition to static-link inspection while
retaining the no-CGO legacy vaultic build and core regression tests.

**Implementation steps:**

1. Define versioned protobuf messages for capabilities, health, requests,
  errors, batches, scans, transactions, drain, and shutdown.
2. Generate the Go client and Rust server from the same `.proto` source.
3. Implement Unix-socket serving with private-directory and `0600` checks.
4. Implement endpoint identity, singleton lock, PID/capability metadata,
  connect-before-start, startup race handling, and stale-socket recovery.
5. Add opt-in TCP serving only behind explicit authentication and a non-empty IP
  allowlist; reject insecure configurations at startup.
6. Add request deadlines, cancellation, request IDs, bounded message sizes,
  streaming/page limits, and server backpressure.

**Files/artifacts:** `internal/index/proto/`, `vaulticdb/src/rpc/`, daemon
launcher/client packages.

**Tests:** two clients racing to start one daemon, compatible daemon reuse,
endpoint mismatch rejection, stale socket recovery, Unix permissions, TCP
disabled-by-default, allowlist rejection, authentication failure, cancellation,
and bounded-message tests.

**Exit criterion:** vaultic can reliably attach to or start one compatible
daemon, and no RPC path can mutate SlateDB yet.
