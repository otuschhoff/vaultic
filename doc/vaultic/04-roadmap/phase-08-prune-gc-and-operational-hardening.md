# Phase 8: Prune, GC, and operational hardening

[← Back to roadmap index](00-overview.md)

[← Phase 7](phase-07-import-export-check-cli-workflows.md) · [Phase 9 →](phase-09-pack-tier-model-and-lifetime-facts.md)

**Goal:** safely use pack catalogs and reverse references for performance without
weakening snapshot reachability guarantees.

**Current implementation state (2026-08-28):** **complete for the CLI-facing
GC workflow; scale benchmarking deferred to dedicated infrastructure.**
`vaultic index gc` (SlateDB-authoritative repositories only; legacy repositories
keep using `prune`) discovers candidates from a single scan of `ri:`/`rm:`
reverse references, narrowing the search before the more expensive step: a
full re-walk of every retained snapshot root (the same trusted mechanism
`prune` already uses, valid for any engine because the archiver always writes
the classic snapshot/tree/content blob graph independent of SlateDB's own
schema) that is the actual, final reachability authority. A blob is only
treated as unreachable when both signals agree, and reachability is always
re-verified immediately before any destructive action, never trusted solely
from the earlier scan. Packs found wholly unreachable are deleted; packs
mixing live and unreachable blobs are repacked via the same `CopyBlobs`
primitive prune uses, then their now-empty-of-purpose original pack is
deleted the same way. Deletion uses the `published -> delete_pending ->
deleted` lifecycle: a durable delete-pending transition precedes the physical
backend removal, and only a successful removal purges the pack/blob catalog
records and decrements aggregates in one transaction; a failed removal leaves
the pack visible as delete-pending and is retried automatically on the next
run. `--discover-only` records candidates cheaply without the snapshot walk
or any mutation; `--min-candidate-age` requires a candidate to stay
continuously unreachable for a configurable duration (tracked via the `gc:`
record's discovery timestamp, preserved across runs) before it is swept,
guarding against races with concurrent or clock-skewed writers on top of the
exclusive repository lock GC already holds during its destructive phase.

Physically deleting a pack necessarily makes any legacy JSON index that
referenced it stale, including indexes inherited from a pre-import legacy
repository that were never covered by an export checkpoint. `vaultic index
gc` automatically re-exports every remaining live pack and then deletes every
legacy index object (and any checkpoint tracking it) that still references a
now-gone pack, so `index check` reaches a clean state without a separate
manual step.

**Implementation steps:**

1. Discover GC candidates from `ri:`, `rm:`, `rc:`, and pack catalog records.
2. Re-walk retained snapshot roots before every destructive deletion.
3. Delete wholly unreachable packs; repack packs containing a mixture of live
  and unreachable blobs.
4. Use `published -> delete_pending -> deleted` transitions and retry failed
  deletions.
5. Add crash/fencing/eventual-consistency and mixed-client tests.
6. Benchmark 10 million, 100 million, and 1.4 billion inode-equivalent loads;
  tune cache, SST blocks, batch sizes, and scanner workers.

**Tests:** unit coverage for candidate/pack classification (whole/mixed/skip)
and the min-candidate-age gate; real-daemon tests for two-phase pack deletion
(including a simulated crash between delete-pending and physical removal,
and its automatic retry), mixed-pack repack, and stale legacy index cleanup;
a full CLI end-to-end test covering forget, import, export, discover-only,
the age gate, the real sweep, convergence on a repeated run, a clean `index
check`, snapshot restore correctness, and `check --check-unused` finding
nothing dangling. Discovering and fixing this phase's real bugs (a
pack-publish path that silently recorded every authoritative pack's payload
size as zero, and untracked legacy indexes inherited from import) required a
real repacked pack and a real pre-import legacy index respectively; neither
was exercised by any earlier phase's tests.

**Exit criterion:** documented capacity and recovery targets, clean differential
checks after failure injection, and a repeatable large-repository acceptance
test.

Scale benchmarking (10 million, 100 million, and 1.4 billion inode-equivalent
loads) requires dedicated infrastructure beyond this environment; the tunable
surface it needs (daemon transaction batch size, scan page size, snapshot
work budgets) is already wired through Phase 7 and Phase 8's CLI options.
