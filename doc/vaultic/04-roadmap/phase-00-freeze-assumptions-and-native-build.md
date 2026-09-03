# Phase 0: Freeze assumptions and native build

[← Back to roadmap index](00-overview.md)

[Phase 1 →](phase-01-protocol-contract-and-daemon-lifecycle.md)

**Goal:** make the Rust/SlateDB dependency reproducible without touching
vaultic runtime behavior.

**Current implementation state (2026-08-27):** **complete.** The `vaulticdb`
crate scaffold, versioned protobuf contract, Unix/TCP transport configuration,
private socket permissions, fail-fast protobuf generator, and musl build script
are present on branch `kvdb`. Generated Go bindings are checked in under
`internal/index/proto/vaulticdb/v1`. The host daemon compiles against the pinned
SlateDB revision; its native self-test and Unix-socket RPC smoke test pass. A
local `cargo zigbuild --target x86_64-unknown-linux-musl --release` also
produced `vaulticdb`, verified as a statically linked, stripped x86-64 Linux ELF
artifact (SHA-256 `7eb16913b78fd69702792e3094a24c5ae331fd6b468b8f5a3306f7cd28d4dd88`).

**Implementation steps:**

1. Pin the SlateDB Rust crate commit and record the official
  `slatedb.io/slatedb-go` binding revision used as the API reference.
2. Add the `vaulticdb` Rust crate, protobuf generation inputs, and a reproducible
  musl Linux build script.
3. Build the native SlateDB dependency and statically link it into `vaulticdb`.
4. Add macOS development linking and Linux musl CI jobs; retain the existing
  no-CGO vaultic build.
5. Add a daemon smoke binary that opens `Db`, opens the read-only equivalent,
  writes a `WriteBatch`, scans with `NextBatch`, and shuts down cleanly.

**Files/artifacts:** `vaulticdb/`, `proto/`, build scripts, CI workflow, pinned
dependency metadata.

**Tests:** native build, binding API smoke test, static-link inspection, and a
legacy `go test ./...` run with no daemon installed.

**Exit criterion:** a reproducible `vaulticdb` binary and a legacy vaultic build
that remains usable when `vaulticdb` is absent.
