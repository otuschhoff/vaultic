# Pack Lifecycle: Phases 9–12

[← Back to roadmap index](00-overview.md)

### Phase 9: Pack tier model and lifetime facts

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
has written is therefore not supported. This matches the rollback path in
[Phase 18](05-analytics-compliance-scale-phases-16-19.md), which resumes from the legacy JSON indexes rather than from an
older SlateDB build.

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

### Phase 10: Pack history event log and rollups

**Goal:** durable, append-only history of pack lifecycle transitions that
survives deletion of the packs it describes.

**Current implementation state (2026-08-29):** **complete.** The `ph:`
namespace records an event in the same transaction as every pack catalog
transition — created, imported, published, tier changed, coalesced usage
change, repack lineage, delete-pending, deleted — plus the observations that
are not transitions: `repacked_from` on a superseded source, `delete_failed`
when a physical removal fails, and `orphan_detected` when `check` finds a pack
on the backend that no index references.

Events are ordered by time, then by a globally monotonic sequence allocated
inside the caller's transaction. The sequence is what makes keys unique when
two writers record events for the same pack within one second, and what gives
the log a total order independent of clock resolution; a concurrent-writer test
asserts both properties.

Each event is self-contained, carrying the pack's classification and sizes
rather than referring back to `p:`. That is what lets history stay readable for
a pack whose catalog record was removed in the same transaction that recorded
its deletion.

Repack destinations carry `predecessor_pack_ids`, declared through a repack
context set around the copy because the destination pack IDs are chosen inside
it. Lineage is therefore reconstructable across several generations, and a
rewrite is never counted as growth. Usage events are coalesced to one per pack
per update regardless of how many blobs changed reachability.

The `pb:` namespace holds hourly, daily, and monthly rollups computed as a pure
function of the retained raw events, so recomputation is idempotent and equal to
a direct scan. Retention rolls up before truncating and records a raw floor that
only ever advances, so a range whose raw events were intentionally discarded is
reported `partial` rather than silently `complete`. Buckets covering a period
before collection was enabled, and buckets containing legacy imports — which
describe packs that existed before the import ran — are reported
`reconstructed`.

History is advisory throughout. Raw events are append-only: they may be written
only by the transition that produced them, never rewritten through a generic
batch, though retention may prune them. A corrupt or missing record is counted
and skipped by every reader, and `index check` reports the count without
changing its verdict. A fault-injection test corrupts every kind of history
record at once and asserts that export, check, and aggregate rebuild all still
succeed and reach the same verdict.

**Deviations from the plan:** the record carries a backend identifier that stays
zero until Phase 12 supplies one. It is present from the start because the log
is append-only and immutable, so adding the field later would mean either
rewriting history or reading two layouts forever. `tier_changed` was restored to
the event vocabulary that the [storage placement policy](../02-architecture/05-storage-placement.md) revision had dropped: the Phase 9 tier
is a real, observable transition until placement supersedes it.

**Fixed while implementing:** the maintenance test harness silently discarded
the deletes passed to `WriteMutableBatch`, so no test in that package could ever
have caught a deletion bug. It now applies them, which is what surfaced the
retention behaviour these tests assert.

**Implementation steps:**

1. Add the `ph:` event namespace and write an event in the same transaction as
  every pack catalog transition: create, import, publish, tier change,
  coalesced usage change, repack lineage, delete-pending, delete, delete
  failure, orphan detection.
2. Record `predecessor_pack_ids` on repack events so churn is distinguishable
  from growth.
3. Coalesce `usage_changed` events per pack per run; never emit one per blob.
4. Add the `pb:` rollup namespace with hourly, daily, and monthly
  granularities, computed idempotently from retained raw events, each bucket
  carrying a `complete`/`partial`/`reconstructed` coverage flag.
5. Implement history retention: roll up, then truncate raw events, writing a
  coverage marker for the truncated range.
6. Guarantee history is advisory: a missing, truncated, or corrupt history
  record must never fail or alter backup, restore, prune, or GC. Add a
  fault-injection test that corrupts history and asserts all data paths still
  succeed.
7. Mark buckets covering periods before history collection was enabled, or
  produced by legacy import, as `reconstructed`.

**Tests:** event ordering and key uniqueness under concurrent writers; rollup
idempotence and equality against a direct scan of raw events; coverage flags
after enabling history on an existing repository and after a retention run;
events for deleted packs still readable; repack lineage reconstructable across
several generations; corrupted-history fault injection leaving every data path
green.

**Verification performed:** eight concurrent writers produce unique,
monotonically ordered sequences; a rollup over an unchanged raw range writes
nothing and equals an independent fold of the log; coverage is asserted for all
three cases — before collection, before the retained raw floor, and fully
observed — plus the import case; a pack deleted from both the backend and the
catalog still has its four events readable; lineage is followed back through
three generations; corrupting a raw event, a bucket, and both markers at once
leaves export, check, and aggregate rebuild green and the verdict unchanged; a
usage update across six blobs emits exactly one event; retention is proven to
roll up before truncating, to leave the truncated range's totals intact in its
bucket, to never lower the raw floor, and to prune each granularity on its own
schedule. The exit criterion runs end to end against a real repository that was
backed up, imported, repacked, and pruned: its history contains origin,
publication, usage, delete-pending, deletion, and both repack lineage
directions; it still describes packs that no longer exist; and its rollup is
idempotent and correctly reports imported activity as reconstructed rather than
observed. Full suites pass under `-race`, and the `CGO_ENABLED=0` build is
retained.

**Exit criterion:** a repository that has been backed up, repacked, and pruned
can report its full pack history, including for packs no longer present, with
correct coverage flags.

### Phase 11: Introspection CLI

**Goal:** answer composition and growth questions from the CLI without a
backend listing or a full index load.

If Phase 12 has already landed, these commands report the placement dimension;
if it has not, they report the Phase 9 tier dimension and the grouping flags
name tiers rather than backends. The JSON contract is versioned, so the change
from one to the other is a version bump rather than a silent reinterpretation.

**Implementation steps:**

1. Add `vaultic index stats` with grouping, filtering, `--verify`, `--rebuild`,
  and `--json`. Where a pack has several placements, report the logical total
  and the stored total separately so their difference is never mistaken for
  drift.
2. Add `vaultic index packs` with catalog filters, sorting, `--count-only`, and
  `--json`.
3. Add `vaultic index history` with metric, bucket, range, grouping,
  `--histogram`, and `--forecast`. Report repack churn separately from net
  growth. Refuse to forecast from incomplete series unless
  `--allow-incomplete` is passed.
4. Add `vaultic index history prune` with per-granularity retention and
  `--dry-run`.
5. Add `vaultic index backends` with per-backend reporting, opt-in `--compare`
  backend listing, and `--no-list`.
6. Define stable JSON output schemas for all of the above and version them; the
  human-readable output may change, the JSON contract may not without a version
  bump.
7. Ensure every output surfaces unknown placement or tier, unknown type,
  retention-unknown, and incomplete-coverage counts explicitly instead of
  folding them into totals.
8. Legacy repositories: run in a documented reduced mode or fail explicitly.
  Never present a partial answer as complete.

**Tests:** golden JSON output tests for each command; filter and sort coverage;
histogram bucketing across timezone and DST boundaries; `--compare` against a
backend with a deliberately missing object and a deliberately extra object;
`--no-list` performing zero backend requests; forecast refusal on incomplete
series; behavior on a legacy repository.

**Exit criterion:** an operator can obtain pack counts, sizes, per-backend
composition, and a creation/change histogram with growth rate for a repository
whose archival backend is never listed.

**Current implementation state (2026-08-29): complete.** All five commands are
registered under `vaultic index` and backed by a query layer in
`internal/index/maintenance/introspect.go` (`Stats`, `QueryPacks`) and
`internal/index/maintenance/series.go` (`HistorySeries`), with the CLI in
`cmd/vaultic/cmd_index_introspect.go`.

Because Phase 12 has not landed, the reported dimension is the Phase 9 tier and
the grouping flags name tiers. `IntrospectSchemaVersion` is therefore 1;
replacing tiers with backends will raise it to 2 rather than reinterpret a
field in place. The flags that are inherently placement-shaped — `--backend`,
`--class`, `--not-offsite`, `--promotion-due`, and the `offsite-deadline` sort
key — are deliberately absent until there is a placement model for them to
mean something; adding them now would require inventing an answer.

Several decisions in this phase turn on refusing to state more than is known:

- `index stats` answers from the aggregate records in constant time and only
  scans the catalog when a filter, a `state` grouping, or `--verify` requires
  per-pack facts. The result names which path produced it, so a reader never
  has to guess whether a number was measured or summarised.
- No aggregate carries the retention dimension. The constant-time path
  therefore reports `retention_counted: false` rather than
  `retention_unknown_packs: 0`, because "not measured" and "none found" are
  different answers.
- `index packs` reports what a filter *could not decide*. A pack whose creation
  time was never recorded satisfies neither `--created-before` nor
  `--created-after`; without the `undecidable` counts an operator would read
  "matched 0" as "no such packs" when the truth is "cannot tell". The same
  applies to `--retention-expired` on a pack with unknown retention and to
  `--unused-ratio-above` on a pack with unaccounted usage.
- `--retention-expired` requires the retention to have been known. Treating an
  unknown deadline as expired would authorise deleting data a backend lock
  still protects.
- Weekly buckets are derived from the stored daily granularity and truncated to
  the ISO-8601 Monday in UTC. A local-time week would produce 23- and 25-hour
  weeks across a DST transition and would change the answer depending on who
  was asking.
- `index history` folds raw events that no rollup bucket covers yet, so asking
  a question never requires first running `index history prune` to mutate the
  repository. Where a stored bucket exists it wins, because retention may have
  discarded the raw events that bucket summarises.
- `--forecast` refuses a series containing `partial` or `reconstructed`
  buckets, and refuses a series with fewer than two points or no variance,
  unless `--allow-incomplete` is given; a permitted forecast still records
  `incomplete_input`.
- `index backends` is the one command that still has a truthful answer without
  a pack catalog, because backends exist regardless of the metadata engine. It
  runs in a reduced mode that omits every catalog-derived field and says so.
  The catalog commands, and `index backends --compare`, fail with
  `maintenance.ErrLegacyRepository` instead.
- `--compare` reports packs missing on the backend separately from objects
  unknown to the catalog, because the first is data loss and the second is only
  waste.

**Verification performed:** golden JSON tests pin the contract of every command
(`internal/index/maintenance/testdata/`, `cmd/vaultic/testdata/`), regenerable
with `UPDATE_GOLDEN=1`. Every catalog filter and every sort key is covered,
including sort determinism under repeated runs. Weekly bucketing is asserted to
be byte-identical across `UTC`, `America/New_York`, `Australia/Lord_Howe`, and
`Asia/Kathmandu` — the last two chosen for their half-hour and 30-minute DST
offsets — and daily bucketing is asserted UTC-aligned under `Pacific/Chatham`.
`--compare` is tested against a listing with a deliberately missing and a
deliberately extra object. `--no-list` is verified twice: at unit level against
a backend that counts every `List` call, and end to end against a real
repository where the assertion is that no pack, index, or snapshot listing is
issued at all. Forecast refusal is tested for both `partial` and
`reconstructed` coverage and for a series too short to fit. Legacy behaviour is
tested per command against a real non-SlateDB repository. The exit criterion is
asserted end to end in `assertIntrospectionAnswersWithoutListing`, which obtains
counts, sizes, composition, per-backend reporting, and a histogram with a growth
rate while a wrapping backend proves nothing was listed.

Re-evaluation of this phase found and fixed three things that the first pass
got wrong. `index history` originally read only the rollup buckets, so a
repository that had never run `index history prune` reported an empty series;
asking a question must not require first mutating the repository, so pending
raw events are now folded on the fly. `index packs` silently discarded packs a
filter could not judge, so `matched 0` was indistinguishable from `cannot
tell`; the `undecidable` counts now say which. `index backends --compare`
compared every catalog pack against the backend, including packs in
`delete-pending`, `deleted`, and `orphaned` states whose backend objects are
gone by design, so any repository that had ever pruned reported data loss and
exited non-zero; those are now counted as `expected_absent`.

### Phase 12: Backend registry, placement records, and per-backend prune

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

