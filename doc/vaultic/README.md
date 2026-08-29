# Vaultic Design & Planning Documentation

This directory is the single entry point for vaultic's strategic orientation,
system architecture, compliance posture, and tactical execution plan. It
replaces the previous flat, monolithic design documents with a structure that
lets a reader start at "why" and drill down to "how" and "when".

## How to read this

| Level | Question answered | Where |
|---|---|---|
| Strategic | Why does this exist, what must never break, how do we compare to alternatives? | [01-strategy/](01-strategy/) |
| Architectural | How is the system designed to meet those constraints? | [02-architecture/](02-architecture/) |
| Regulatory | How does the design support privacy, security, resilience, controls, and I&T governance frameworks? | [03-compliance/](03-compliance/) |
| Tactical | What is the phased plan to build it, in what order, with what exit criteria? | [04-roadmap/](04-roadmap/) |
| Parity-specific | What is the plan to reach rustic feature parity? | [05-rustic-parity/](05-rustic-parity/) |

## 1. Strategy — [01-strategy/](01-strategy/)

- [Vision, Scope, and Non-Negotiable Guarantees](01-strategy/01-vision-and-principles.md) —
  the purpose of the vaulticdb metadata service and of rustic feature parity,
  the guarantees that constrain every design decision, and the guiding
  principles for implementation.
- [Competitive Analysis: Vaultic vs. Rubrik](01-strategy/02-competitive-analysis.md) —
  TCO, architecture, scale, DR, and compliance comparison against a
  proprietary enterprise backup platform, used to validate the strategic bet.

## 2. Architecture — [02-architecture/](02-architecture/)

Start at the [architecture overview](02-architecture/00-overview.md). It
covers the vaulticdb process architecture and engine resolution, the SlateDB
binary schema, legacy JSON interop and crawl reconciliation, the CLI and
locking model, storage placement policy, path/inode history queries, and the
creation-analytics engine.

## 3. Compliance — [03-compliance/](03-compliance/)

Start at the [compliance overview](03-compliance/00-overview.md). It covers
GDPR, ISO/IEC 27001:2022, NIS2, NIST CSF 2.0, CIS Controls v8.1, and COBIT
2019 assessments, plus the shared security/verification architecture (Azure
Key Vault, multi-target syslog, per-chunk GDPR erasure analysis, and sampled
storage verification).

## 4. Roadmap — [04-roadmap/](04-roadmap/)

Start at the [roadmap overview](04-roadmap/00-overview.md). The vaulticdb
implementation is planned as twenty phases (0–19), grouped into five
milestones from native build and protocol design through to analytics,
compliance, and multi-provider scale-out, plus dedicated testing,
observability, rollout/rollback, and known-constraints documents.

## 5. Rustic parity — [05-rustic-parity/](05-rustic-parity/)

Start at the [rustic-parity overview](05-rustic-parity/00-overview.md). This
is a separate, independently phased roadmap to close the feature gap with
[rustic](https://github.com/rustic-rs/rustic) while preserving repository
format compatibility with both restic and rustic.

## Conventions

- Every document links back to its section index and, where useful, sideways
  to related sections, so the set can be read starting from any entry point.
- Code touchpoints are linked with paths relative to the repository root
  (e.g. `../../internal/...`, `../../cmd/vaultic/...`).
- Status markers (✅ done, ⚠️ partial, ⏳ deferred) reflect the state recorded
  at each document's last update, not necessarily the current `main` branch;
  check `CHANGELOG.md` and `git log` for the authoritative current state.
