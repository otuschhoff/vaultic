# Phase 15: Placement scheduler, offsite RPO, and promotion

[← Back to roadmap index](00-overview.md)

[← Phase 14](phase-14-versioned-path-index.md) · [Phase 16 →](phase-16-growth-churn-per-user-group-attribution-and-gdpr-audit-cli.md)

**Goal:** move bytes between backends according to the Phase 12 placement model,
meet a stated offsite deadline, and defer archival commitment until survival is
observed.

This is the phase that realises the cost saving: short-lived data never reaches
an archival class, so it never pays a minimum-retention floor.

**Implementation steps:**

1. Define placement classes (`metadata`, `recent-data`, `archival-data`,
  `cache`) and the rules resolving a pack's class to a target placement set.
  Classes are named policy, not per-pack optimisation; a general cost-optimising
  solver is explicitly out of scope.
2. Implement the scheduler as a background, resumable worker that closes the
  difference between target and actual placement, honouring each backend's
  bandwidth and request-rate limits. Every transition is durable and every
  action idempotent, so an interrupted run resumes from `pl:` rather than from
  memory.
3. Order work by urgency: packs approaching their offsite deadline first, then
  promotions, then evictions. Eviction runs last because it is the only action
  that can reduce durability, and it is gated on the durability predicate.
4. Add the `rq:` deadline-ordered queue of packs not yet satisfying the
  predicate, and expose the oldest unsatisfied deadline as a metric and as a
  non-zero exit from `vaultic index placement`.
5. Implement promotion as a repack, never as an object copy: read the surviving
  blobs from the cheapest live placement, write a new pack sized for the
  archival backend, make its placement live, update the blob index, and only
  then permit the superseded placements to be evicted. A crash at any point must
  leave either the old or the new pack fully reachable.
6. Derive the promotion trigger from the forget policy: promote when a pack's
  surviving blobs are reachable only from snapshots retained longer than the
  target backend's minimum retention. Apply the crossover period as a floor
  below which promotion is never correct, for policies that cannot supply a
  horizon.
7. Route reads to the cheapest live placement by retrieval class, falling back
  on failure, and warm up only when the chosen placement requires it.
8. Add `vaultic index placement` with `--unsatisfied`, `--overdue`,
  `--pending-promotion`, and `--explain`, and make its JSON output usable as a
  monitoring probe.
9. Emit `placed`, `placement_failed`, `promoted`, and `evicted` events into the
  Phase 10 history log with their backend IDs, so placement history survives the
  packs it describes.
10. Documentation: describe the backend registry, the durability predicate, the
  offsite deadline, and the promotion rule, including the explicit statement
  that a pack which dies before promotion never reaches the archival backend
  and that this is the intended behavior.

**Files/artifacts:** scheduler worker, placement class rules, `rq:` queue,
promotion repack path, read routing, `index placement` command.

**Tests:** a pack that becomes unreachable before its promotion trigger is
proven never to have been placed on the archival backend; a pack that survives
past the trigger is promoted, and the promotion is verified to be a repack
producing a new pack ID rather than a copied object; crash injection at each
promotion step leaves either the old or the new pack fully reachable and never
neither; an eviction that would breach the durability predicate is refused;
the offsite deadline is met under a bandwidth limit low enough to force
queuing, and the unsatisfied-deadline metric is proven to rise and then fall;
a backend outage leaves packs queued and retried rather than failing the backup;
read routing prefers the cheaper live placement and falls back when it is
unavailable; scheduler restart mid-run resumes from placement records and
converges; a single-backend repository performs no scheduling work at all.

**Exit criterion:** on a repository declaring on-premises, warm offsite, and
archival backends, a backup meets its stated offsite deadline without writing to
the archival backend, data discarded by the forget policy before its promotion
trigger never reaches archival storage at all, and surviving data is promoted by
repack into archival-sized packs, with every placement decision explainable from
`--json` output.

**Current implementation state (2026-08-29): complete.** The scheduler assigns
the four named classes, persists concrete retryable work in deadline-ordered
`rq:` records, and runs a bounded non-fatal tick after successful backups.
`index placement --execute` drains additional work with backend request and
bandwidth budgets; unchanged work is not rewritten, outages retain exponential
retry state, and restarts resume from `rq:` plus `pl:`.

Placement backend entries may name additive physical `location` values opened
through the normal backend registry. The primary may omit a location to reuse
the repository backend. Exact-pack warm placement is streamed through a bounded
temporary file and is idempotent by destination size. Reads rank live,
addressable placements by retrieval class and egress cost, warm only the chosen
placement, and fall back on failure.

Promotion is a retained-blob repack through `CopyBlobs`, directed to the target
archival backend. Successor pack publication atomically records blob locations,
the archival `pl:`/`bp:` pair, typed `rl:` lineage, and a `promoted` history
event before the source becomes delete-pending. Typed lineage both keeps the
successor in `archival-data` and makes a crash after publication idempotently
resumable without another repack. Packs with unknown reachability or no live
bytes are never promoted; known survivors become eligible after the configured
crossover floor (eight days by default), so short-lived forgotten data never
reaches archival storage.

The monitoring command supports `--unsatisfied`, `--overdue`,
`--pending-promotion`, `--explain`, and stable golden-tested JSON. Placement
success, failure, promotion, and eviction emit backend-qualified history events.
Eviction is queued last, waits out per-placement minimum retention, and is
rechecked against the post-removal durability predicate immediately before
physical deletion.
