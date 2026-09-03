# Phase 12: Backend registry, placement records, and per-backend prune

[← Back to roadmap index](00-overview.md)

[← Phase 11](phase-11-introspection-cli.md) · [Phase 13 →](phase-13-historical-path-resolution-and-file-history-cli.md)

**Goal:** replace the fixed hot/cold split with the durable placement model, and
make prune and delete decisions per backend, without yet adding the active
scheduler.

This phase is the schema and policy half of [storage placement policy](../02-architecture/05-storage-placement.md); Phase 16 adds the
machinery that moves bytes between backends.

**Implementation steps:**

1. Add the backend registry to repository configuration: identifier, role,
  offsite flag, failure domain, capacity, prices, minimum retention, retrieval
  class, bandwidth ceiling, and per-object overhead. A repository declaring one
  backend must resolve to today's behavior exactly.
2. Add the `pl:` and `bp:` namespaces of the [SlateDB schema](../02-architecture/02-schema.md), written in the same
  transaction as the transition that produced them, with `bp:` rebuildable from
  `pl:` by `index check`.
3. Migrate Phase 9's recorded tier into initial placement sets: `mirrored`
  becomes a placement on each of the hot and cold backends, `cold` a placement
  on the cold backend, `single` a placement on the sole backend, and `unknown`
  an unplaced pack that a reconciliation pass resolves against a backend
  listing. No placement may be invented for a pack whose tier was unknown.
4. Move `min_retention_until`, `retention_source`, and `delete_after` from the
  pack record to `pl:`, and turn `tier` into a derived summary that is
  rebuildable from the placement set. Keep decoding the Phase 9 fields so
  existing records migrate rather than failing.
5. Implement the durability predicate over the placement set, counting distinct
  failure domains rather than backends, and gate every eviction on the state
  that would result from it.
6. Parameterise `decidePackAction` by the properties of the backend holding the
  placement under consideration, instead of by a hard-coded tier.
7. Generalise the cost model to any placement whose backend declares egress or
  minimum-retention charges, sourcing inputs from the registry with per-run
  overrides, defaulting so the predicate is false.
8. Compute `delete_after` per placement and extend the `dq:` queue key with the
  backend, so one pack's local placement can be freed immediately while its
  archival placement waits out its retention.
9. Treat retention-unknown placements conservatively: eligible for ordinary
  reachability-based deletion, never credited with early-deletion savings.
10. Warm up an archival placement before any repack read of it, reusing the
  existing warm-up path, and abort rather than block indefinitely when warm-up
  fails.
11. Teach `index check` to validate placement records against the durability
  predicate, report packs below it, and rebuild `bp:` and the derived tier
  summary.

**Files/artifacts:** backend registry configuration and validation, `pl:`/`bp:`
schema support, placement-aware prune policy, migration from the Phase 9 tier
field.

**Tests:** migration from every Phase 9 tier value into the expected placement
set, including that an unknown tier produces no invented placement; durability
predicate boundary cases including two backends sharing a failure domain,
pending placements not counting as live, and an eviction refused because it
would drop the pack below the predicate; per-placement retention producing
different deadlines for the same pack; `dq:` ordering with the backend in the
key; cost-model boundary cases including zero, unknown, and expired retention;
`bp:` and tier-summary rebuild with zero drift; an explicit regression test that
a single-backend repository's prune decisions are byte-for-byte unchanged.

**Exit criterion:** a repository with several declared backends records every
pack's placement set durably, refuses any eviction that would breach the
durability predicate, and makes prune and delete decisions from the properties
of the backend actually holding each copy.

**Current implementation state (2026-08-29): complete for the Phase 12 schema,
policy, and GC/prune decision layer.** Repository config now has an additive
backend registry (`placement_backends`) and durability policy
(`placement_policy`). Repositories with no declaration resolve to the existing
single-backend or hot/cold behavior, and a single declared backend resolves to
the same primary-backend posture.

The SlateDB schema now has `pl:` placement records, `bp:` reverse backend-pack
records, and `dq:` per-placement delete queue keys. New SlateDB publishes write
`pl:` and `bp:` in the same transaction as the pack catalog transition. New
placement-aware publishes leave pack-level retention fields unknown and record
minimum retention on the placement instead. Existing Phase 9 tier summaries can
be migrated with `vaultic index rebuild-pack-stats`: known tiers seed initial
placements (`mirrored` -> primary+archival, `cold` -> archival, `single` ->
primary), while `unknown` remains unplaced and must be reconciled from a real
backend listing rather than invented.

`index check` resolves the real placement model, validates `pl:` against `bp:`,
reports stale or missing reverse records, detects tier-summary drift from the
placement set, and reports packs below the durability predicate. The predicate
counts live placements only, counts distinct failure domains rather than
backend count, and treats pending placements as not yet protective.

The rebuild path now repairs the derived state in the right order: migrate
missing placement records, rebuild the tier summary from `pl:`, rebuild pack
and tier aggregates from the repaired pack records, then rebuild `bp:` from
`pl:` including deletion of stale reverse records. The `index stats` and
`index packs` JSON contract is version 2 and reports the placement/backend
dimension; `--backend`, `--class`, `--not-offsite`, `--promotion-due`, and the
`offsite-deadline` sort key are active.

SlateDB GC now marks placement records `evicting`, writes per-backend `dq:`
records with `delete_after = max(now, min_retention_until)`, and skips physical
pack deletion until every evicting placement's deadline has passed. Repacked
source packs are warmed before being read, using the existing backend warm-up
path and the caller's context. Mixed-pack classification invokes the
placement-aware cost model for constrained placements; configured archival
prices, egress, request cost, object overhead, and remaining retention can veto
a repack when savings do not exceed movement and remaining-retention cost. With
no costs declared the model degrades conservatively and does not invent savings
for retention-unknown placements.

**Verification performed:** tests cover the `pl:`, `bp:`, and `dq:` key codecs
and value codecs; config registry round-trip and validation; publish-time
placement derivation and atomic `pl:`/`bp:` persistence; delete-pending
transition to `evicting`, `dq:` creation, and final cleanup; migration from
every Phase 9 tier value including the no-invented-placement rule for
`unknown`; durability boundary cases for shared failure domains, pending
placements, and eviction refusal; true `bp:` rebuild including stale-key
deletion; derived tier-summary repair; `index check` placement drift reporting;
GC delete-deadline deferral; production GC classification using the cost model;
and unchanged legacy/single-backend focused workflows. The versioned golden JSON
contracts were refreshed to schema version 2.
