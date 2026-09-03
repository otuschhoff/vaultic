# Phase 9: Pack tier model and lifetime facts

[← Back to roadmap index](00-overview.md)

[← Phase 8](phase-08-prune-gc-and-operational-hardening.md) · [Phase 10 →](phase-10-pack-history-event-log-and-rollups.md)

**Goal:** make tier, creation time, retention deadline, and usage accounting
durable properties of the pack catalog, without changing any policy yet.

**Superseded in part by Phase 12.** [Storage placement policy](../02-architecture/05-storage-placement.md) was revised after this phase
shipped to replace the fixed hot/cold split with a multi-backend placement
model. Three fields implemented here — `min_retention_until`,
`retention_source`, and `delete_after` — are properties of a *placement*, not of
a pack, because a local copy and an archival copy of the same pack have
different retention obligations. Phase 12 moves them to `pl:` records and turns
`tier` into a derived summary of the placement set. Everything else this phase
established stands: recorded-rather-than-derived facts, creation-time handling,
usage accounting, aggregates as rebuildable accelerators, the unknown-versus-zero
discipline, and the treatment of imported packs as permanently unknown. The
recorded tier values migrate directly into initial placement sets, so this
phase's data seeds the new model rather than being discarded.

**Current implementation state (2026-08-29):** **complete.** The pack record
carries `tier`, `storage_class`, `created_at`/`creation_time_known`,
`min_retention_until`/`retention_source`, `used_payload_bytes`/
`unused_payload_bytes`, and `delete_after`. Records written by Phase 3 and by
the pre-`physical_size_known` layout still decode, as tier-unknown and
retention-unknown; both historical layouts are reproduced byte-for-byte in the
codec tests rather than approximated by truncating a current record.

Tier is applied at publish time by a `TierPolicy` derived from the
repository's actual backend layout, so the recorded tier reflects how bytes
were really placed. A tree pack in a hot/cold repository is recorded as
`mirrored`, not `hot`, because `hotcold.Save` writes hot files to the hot
backend and then mirrors them to the cold backend; a hot-only pack never
exists. A repository without a hot/cold split records `single`. Mixed and
unclassified packs stay `unknown`, because vaultic never routes them itself.
Packs vaultic writes always carry a known creation time; imported packs never
do, and no timestamp, tier, or retention deadline is synthesized for them.

The `a:tier:*` aggregates are maintained in the same transaction as the pack
record and the type aggregates. Only tiers that hold packs are materialized.
A batch touching several packs folds every delta into one read-modify-write of
each aggregate, because per-pack updates inside one transaction would not
observe each other's pending writes and all but the last would be lost.

`used_payload_bytes`/`unused_payload_bytes` are refreshed by GC discovery,
which is where reachability is actually computed; `forget` deletes snapshots
without computing reachability, so the accounting follows at the next
discovery pass. The split is derived from the blob index and is rejected
rather than recorded when it disagrees with the catalog payload size, so usage
never contradicts the catalog. `index check` rebuilds both dimensions and
reports drift, unknown-tier, retention-unknown, and usage-unaccounted counts.

Two states are distinguished deliberately. A tier aggregate that exists but
disagrees with the catalog is drift and fails the check. A tier aggregate that
is absent is a pending rebuild, reported as `tier_aggregates_unbuilt` with the
command to run, because a repository written before this phase has none and
must not be reported as corrupt. For the same reason a missing tier aggregate
never blocks pack deletion: an accelerator may not prevent a destructive
operation from completing correctly.

**Deviations from the plan:** the global schema version byte is *not* bumped.
It is shared by every record type and `newDecoder` rejects any other value, so
bumping it would make every Phase 3 record of every type undecodable, which
directly contradicts the same step's requirement that older records still
decode as tier-unknown. Backward compatibility wins; the new fields are
appended and guarded by length, matching how `physical_size_known` was added.

The compatibility is one-way, and deliberately so. A Phase 9 pack record
carries fields an older decoder does not know, and that decoder requires a
record to end exactly where it expects, so it rejects the record as having
trailing data. Downgrading to an older binary against a catalog that Phase 9
has written is therefore not supported. This matches the
[rollback plan](03-rollout-and-rollback.md), which resumes from the legacy
JSON indexes rather than from an older SlateDB build.

Two fields were added beyond the listed set. `usage_known` distinguishes "usage
has never been computed" from "every byte is unreachable", which the listed
fields alone cannot express; without it an unaccounted pack is
indistinguishable from a wholly unused one. `accounted_pack_count` on the
aggregate records how many packs contributed usage, so a consumer can tell
partial accounting from a fully reachable repository. Both follow the existing
`physical_size_known`/`creation_time_known` precedent and the [SlateDB schema](../02-architecture/02-schema.md) rule
that unknown must be distinguishable from zero.

Unknown-tier and retention-unknown packs are reported as counts rather than
per-pack findings. Imported packs are legitimately unknown forever, so
per-pack findings would crowd real findings out of the list on a repository
with millions of packs, and would make a correctly imported repository fail a
check that is otherwise clean.

**Implementation steps:**

1. Extend the versioned pack record with `tier`, `storage_class`, `created_at`
  plus `creation_time_known`, `min_retention_until` plus `retention_source`,
  `used_payload_bytes`, `unused_payload_bytes`, and `delete_after`. Bump the
  record schema version; older records decode with `tier = unknown` and
  `retention_source = unknown`.
2. Record the tier at pack publish time from the pack type and the configured
  hot/cold routing, rather than deriving it on read.
3. Add `a:tier:*` aggregates and update them atomically with pack records.
4. Maintain `used_payload_bytes`/`unused_payload_bytes` incrementally where
  reachability already changes (forget, GC discovery), and make them
  rebuildable from the blob index.
5. Populate `created_at` for packs written by vaultic; leave imported legacy
  packs `creation_time_known = false`. Do not synthesize a timestamp.
6. Teach `index check` to rebuild tier aggregates and report drift,
  unknown-tier packs, and retention-unknown packs.

**Tests:** schema round trip including forward/backward compatibility with
Phase 3 records; tier assignment for data, tree, and mixed packs in a hot/cold
repository and in a single-backend repository; aggregate atomicity and rebuild;
usage accounting matching a full recomputation after forget and GC; import of a
legacy repository leaving every pack retention-unknown.

**Verification performed:** both historical pack layouts decode as
tier-unknown/retention-unknown with no invented facts, and every truncation
except the two legacy record boundaries is still rejected; tier assignment is
pinned for all six pack-type/layout combinations, with configured retention
applying only where cold bytes exist; publish and deletion maintain the tier
dimension against a real daemon, including deletion on a catalog with no tier
aggregates at all; a usage batch spanning several packs is proven to fold every
delta rather than only the last; usage is verified against an independent naive
recomputation from the blob index, and is refused when it disagrees with the
catalog payload size; a real backup, forget, and GC sequence records usage at
the run that computes reachability and is idempotent on the next run; a legacy
import leaves every pack tier-, retention-, creation-time-, and
usage-unknown. Full suites pass under `-race`, and the `CGO_ENABLED=0` build is
retained.

**Exit criterion:** a hot/cold repository reports correct per-tier totals, and
`index check` rebuilds them from `p:` records with zero drift.
