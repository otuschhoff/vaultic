# Phase 7: Import/export/check CLI workflows

[← Back to roadmap index](00-overview.md)

[← Phase 6](phase-06-authoritative-dual-write-and-legacy-projection.md) · [Phase 8 →](phase-08-prune-gc-and-operational-hardening.md)

**Goal:** expose operator-controlled migration and verification tools.

**Current implementation state (2026-08-28):** **complete.** The new
`vaultic index` group leaves `vaultic list index` unchanged and provides
`import`, `export`, `check`, and `rebuild-pack-stats`. Import is best-effort,
bounded, dry-runnable, and resumable through durable index and snapshot
checkpoints; authority activation remains explicit and occurs last, only after
a complete import. Snapshot traversal loads the source blob index first.

Export writes deterministic canonical Restic JSON indexes, either for every
live catalog pack or only packs whose lifecycle lacks a completed export
checkpoint. Each durable JSON object receives an atomic provenance checkpoint
containing a monotonic export sequence and its exact pack IDs before those
packs transition to `published`; interruption therefore causes safe duplicate
re-export rather than silent omission. `--since` selects packs after a recorded
sequence, while `--verify` reads each object back through the authenticated
repository loader and decodes it. Pack-oriented export still scans `b:` because
the current schema intentionally has no pack-to-blob secondary index.

Differential check compares deduplicated physical blob locations and pack
presence between raw legacy JSON and SlateDB, validates pack type/blob/payload
catalog totals and all five aggregates, checks reverse references and
materialized counters, checks snapshot roots, reports crawl debt/export/GC
state, and validates export object presence, decoding, and pack provenance.
Unresolved imported references and checkpointed snapshots without reconstructible
root inode identity are warnings rather than divergence. Manifest and schema
compatibility are enforced before the workflow by repository-open validation
and the daemon health handshake. Aggregate repair tolerates missing or malformed
records, reports before/after deltas, uses checked rebuild arithmetic, and
replaces all records in one durable batch only when numerical drift exists.

Mutating workflows take an exclusive repository view and check takes the
existing read-lock path. All support JSON summaries and map partial import or
detected drift to exit status 2. Import exposes explicit legacy source
selection, daemon transaction batch sizing, work/error budgets, dry-run,
resume, bounded snapshot traversal, and structured record/warning/checkpoint
counters. Daemon
flags support attach or start, repository-scoped Unix sockets by default,
explicit TCP address/allowlist/authentication, persistent startup, and local or
S3-compatible storage configuration. End-to-end tests cover legacy-only,
partial, resumed, and authoritative repositories; dry-run and corruption
paths; checkpointed export; aggregate repair; local daemon storage; and the
same workflow against S3-compatible metadata storage when the existing CI
endpoint is configured.

**Implementation steps:**

1. Add the `vaultic index` command group without changing `list index`.
2. Implement resumable best-effort `index import`.
3. Implement full and checkpointed `index export`.
4. Implement differential `index check`, pack aggregate rebuild, and JSON
  summaries with automation-friendly exit codes.
5. Add daemon attach/start options while keeping Unix sockets as the default and
  TCP opt-in only.

**Tests:** all commands on legacy, partial, and SlateDB-authoritative repos;
  dry-run and resume; corruption and warning exit codes; aggregate repair;
  local and S3-compatible backends.

**Exit criterion:** operators can import, inspect, export, repair aggregates,
and compare engines without manual database intervention.
