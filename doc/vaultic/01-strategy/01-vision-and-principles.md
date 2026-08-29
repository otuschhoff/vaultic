# Vision, Scope, and Non-Negotiable Guarantees

[← Back to index](../README.md)

This document sets the strategic orientation for vaultic: what problem it solves,
what must never break while getting there, and the guiding principles that every
tactical phase (see [Roadmap](../04-roadmap/00-overview.md)) must honor.

## Part A — The vaulticdb metadata service

## 1. Purpose

Integrate SlateDB through a separate Rust `vaulticdb` metadata service. There is
one SlateDB database per vaultic repository. The official Go UniFFI binding is
the reference for the supported API surface, but the daemon should use
SlateDB's native Rust crate directly. Vaultic remains responsible for repository
semantics, legacy Restic JSON compatibility, import/export, crawl policy, and
the CLI. `vaulticdb` is responsible only for SlateDB access and performance
mechanics: caching, batching, write-back, scans, and transactions.

The design target is a repository containing approximately 1.4 billion inodes
and 500+ TB of data on NetApp NFS with pack data and metadata replicated to S3.
The existing JSON index path must remain independent of SlateDB, Rust, and CGO
when `vaulticdb` is absent.

### Non-negotiable guarantees

- A repository without a SlateDB manifest remains a legacy JSON-index
  repository.
- Existing Restic and Rustic JSON indexes remain readable and writable.
- SlateDB records are never treated as authoritative until their manifest and
  schema version have been validated.
- A failed SlateDB open falls back to legacy mode only when doing so cannot hide
  a detected SlateDB corruption or split-brain condition.
- JSON import is best effort. Recoverable records are imported immediately;
  unresolved inode and directory facts are recorded as crawl debt for the next
  backup crawl.
- Legacy JSON export is deterministic, complete for the blob index, and safe
  for Restic/Rustic readers.
- No operation silently deletes legacy indexes while SlateDB parity is being
  established.
- Vaultic processes communicate with `vaulticdb` over protobuf RPC. Unix domain
  sockets are the default transport; TCP is disabled unless explicitly enabled
  and protected by an IP allowlist and authentication policy.
- There is at most one `vaulticdb` instance per configured repository/SlateDB
  endpoint.
  Vaultic may start it on demand, then reuse an already-running compatible
  daemon.


## Part B — Rustic feature parity

# Vaultic ↔ Rustic Feature-Parity Refactoring Roadmap

| | |
|---|---|
| Status | Draft — basis for implementation |
| Audience | vaultic maintainers and contributors |
| Source comparison | rustic 0.11.3 vs restic 0.19.0 (https://rustic.cli.rs/docs/comparison-restic.html), rustic user docs |
| Last updated | 2026-08-26 |

---

## 1. Purpose

Vaultic (formerly restic) aims to reach feature parity with [rustic](https://github.com/rustic-rs/rustic)
while keeping its own strengths (maturity, test coverage, mount/FUSE, `recover`,
`repair packs`, `stats`, sparse restore, `--overwrite` modes, TLS client certs,
`--cacert`, `--key-hint`, `--files-from*`, `cache` command).

This document is the **single source of truth** for:

1. **What** is missing — a complete gap inventory (see [Current State & Gap Analysis](../05-rustic-parity/01-current-state-and-gaps.md)).
2. **How** to implement each feature — architecture workstreams with concrete
   code touchpoints in this repository (see [Foundation Workstreams](../05-rustic-parity/02-workstreams.md)) and per-command work items
   (see [Command Work Items](../05-rustic-parity/03-command-work-items.md)).
3. **When** — a phased roadmap with dependencies and exit criteria (see [Phased Roadmap](../05-rustic-parity/04-phased-roadmap.md)).

Every implementation PR must reference a work item in this document and follow
the conventions in [Strategy & Appendices](../05-rustic-parity/05-strategy-and-appendices.md).

## 2. Scope and non-goals

**In scope**

- All features listed in the rustic↔restic comparison where restic 0.19.0
  (≙ current vaultic) is marked ❌ or (✅) partial.
- Rustic's architectural capabilities: in-repo config, hot/cold repositories,
  lock-free operations, config profiles, hooks, telemetry.
- Keeping **repository-format compatibility** with both restic and rustic:
  a repo written by vaultic must remain readable by restic and rustic, and
  vice versa.

**Non-goals**

- Copying rustic CLI spelling verbatim where vaultic already has an equivalent
  (e.g. rustic's `--glob` ≙ vaultic's `--exclude`). We add rustic-style aliases
  only where it aids migration; native vaultic flags stay canonical.
- Re-implementing rustic internals in Rust; all work is Go.
- Removing vaultic-only features to match rustic.

## 3. Guiding principles

1. **Format compatibility first.** New on-disk data uses additive JSON fields
   (`omitempty`), which Go decoders in restic silently ignore. Nothing may be
   written that makes restic or rustic reject a repository.
2. **Feature-flag everything risky.** All behavior-changing work lands behind
   flags in [internal/feature/features.go](../../../internal/feature/features.go)
   (`Alpha` → `Beta` → `Stable`), mirroring how `S3Restore` was introduced.
3. **Library-first.** New logic goes into `internal/*` packages with the CLI in
   [cmd/vaultic](../../../cmd/vaultic) as a thin layer, so vaultic can later expose a
   supported Go API comparable to `rustic_core`.
4. **Config wins over flags.** Following rustic, durable settings (compression,
   pack sizes, warm-up) move into the in-repo config so users stop repeating
   flags on every call.
5. **Small, reviewable steps.** Each work item in [Command Work Items](../05-rustic-parity/03-command-work-items.md) is sized to be one
   PR (or a short PR series) with integration tests.

