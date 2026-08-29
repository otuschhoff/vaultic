# Storage Placement Policy

[← Back to architecture index](00-overview.md)

## 12. Storage placement policy, pack history, and backend introspection

### 12.1 Objective

Vaultic already supports hot/cold repositories: the hot backend holds metadata
and tree packs, the cold backend holds data packs, and reads of cold data
require a warm-up step. Two problems follow from that model.

First, prune is tier-blind. Every repack and delete decision is evaluated with
one global policy, so the operator must tune for the strictest tier. Cold
storage is the strict tier: repacking pays egress, request, and early-deletion
charges. Tuning for cold forfeits the one place where defragmentation is
genuinely cheap, which is the hot tier.

Second, and more expensive, the cold backend is unconditionally authoritative.
Every pack must therefore be committed to archival storage as soon as it is
written, before anything is known about whether the data will survive. Archival
classes bill a minimum retention period — commonly 180 days — so a pack that
becomes unreachable after three days is still charged for six months. For a
backup workload, where a large fraction of each run is volatile data that the
retention policy will discard within days, this is not a rounding error: it is
the dominant cost of the cold tier, and it is paid on data the operator already
deleted.

The root cause is that one decision is being used to answer two questions:
*where must a second durable copy exist, and how soon* (a durability
requirement), and *does this data deserve cheap long-term storage* (an
economic judgement that depends on how long the data actually lives). Those are
independent, and conflating them forces the archival commitment to be made at
the one moment when the information needed to make it does not yet exist.

The objective of this work is to replace the fixed hot/cold split with an
explicit placement model, so that:

1. A repository may use several backends, each declaring its own properties and
   constraints, rather than exactly one hot and one cold part.
2. A pack's location is a *set* of placements that changes over its lifetime,
   governed by an explicit durability predicate rather than by which backend was
   declared authoritative.
3. Offsite durability is expressed as a deadline the operator can state and the
   system can measure, instead of being an implicit side effect of writing to
   the cold backend synchronously.
4. Archival commitment is deferred until survival has been observed, so
   short-lived data never pays an archival minimum-retention floor.
5. Repack, delete, and promote decisions are driven by explicit cost, retention,
   and reachability facts rather than a fixed unused-ratio threshold.
6. Pack lifetime and placement facts are recorded durably instead of being
   inferred from backend object metadata, which does not reliably carry a
   creation time.
7. Operators can query repository composition, placement, and growth from the
   CLI without scanning a backend or loading a full index.

Non-goals: changing the on-backend repository format, requiring more than one
backend, building a general cost-optimising constraint solver, or making SlateDB
required for hot/cold repositories. A repository that declares one backend
behaves exactly as it does today, and legacy JSON repositories keep today's
tier-blind prune.

### 12.2 Methodology

#### Invariant: one authoritative pack per blob, many placements

The invariant worth preserving from the hot/cold model is *one authoritative
pack per blob*, not *one authoritative location*. A blob resolves to exactly one
pack ID; the blob index is unchanged and no dual `hot pack id` / `cold pack id`
tracking is introduced. What becomes a set is the mapping from that pack to the
backends currently holding its bytes.

```text
blob -> exactly one pack            (unchanged, authoritative)
pack -> a set of placements         (new, changes over the pack's lifetime)
```

This keeps the expensive part of the index untouched: the 500 TB-scale blob
namespace gains nothing, while placement is tracked once per pack, of which
there are orders of magnitude fewer.

The existing multi-location capability of the blob record stays reserved for
legacy duplicate semantics. It must not be repurposed to express placement; two
copies of one pack on two backends are one logical location with two
placements, not two blob locations.

#### Backend registry

A repository declares its backends in configuration, not per pack. Each entry
states what the backend *is*, so policy can be written against properties rather
than against hard-coded names:

```text
backend:
  id                      stable identifier, recorded in placements
  role                    metadata, primary, archival, cache
  ingest                  boolean; true permits new pack placement, false makes backend read-only
  read_enabled            boolean; true permits read/restore/warmup operations
  offsite                 boolean
  failure_domain          opaque label; two backends sharing one are not
                          independent copies
  capacity_bytes          optional ceiling for eviction planning
  price_per_gb_month
  price_per_gb_egress
  price_per_1k_requests
  min_retention           minimum billable object lifetime, zero when none
  retrieval_class         instant, minutes, hours
  max_bandwidth           optional scheduler rate limit
  object_overhead_bytes   per-object billing overhead, where the class has one
```

`failure_domain` is the field that makes the offsite guarantee meaningful. Two
buckets in one provider account, or a Synology and a laptop in the same room,
are not independent; the durability predicate below counts copies per distinct
domain, not per backend.

Prices and limits are operator-supplied estimates, exactly as in the cold cost
model. They exist to make placement decisions explicit and auditable, not to
predict an invoice.

#### Placement records

Placement is per `(pack, backend)` and has its own lifecycle, because a pack may
be live on one backend while still being written to another and already evicted
from a third:

```text
key:   pl:<32-byte pack ID>:<8-byte backend ID hash>
value: schema version
       state (pending, live, evicting, evicted, failed)
       storage class (backend-reported, free-form, optional)
       placed_at and placement-time-known flag
       bytes
       min_retention_until and retention_source
       delete_after
       last_verified_at
```

The reverse direction is needed for eviction planning, capacity accounting, and
reconciling a backend listing against the catalog, so it is a separate
range-scannable namespace rather than a scan of every pack:

```text
key:   bp:<8-byte backend ID hash>:<32-byte pack ID>
value: state, bytes, placed_at
```

Both records are written in the same transaction as the placement transition
that produced them, and `index check` must be able to rebuild `bp:` from `pl:`.

**This supersedes part of the Phase 9 pack record.** `min_retention_until`,
`retention_source`, and `delete_after` were specified and implemented as pack
attributes. They are properties of a *placement*: the on-premises copy has no
minimum retention while the archival copy has 180 days, and a single pack-level
field cannot express both. They move to `pl:`. The pack record keeps
`created_at`/`creation_time_known`, usage accounting, and a derived summary of
its current placement set for fast filtering.

`tier` likewise becomes derived rather than stored: it is a projection of the
placement set, retained on the pack record only as a denormalised summary for
`index packs` filters, and always rebuildable from `pl:`.

#### Durability predicate and the offsite deadline

Placement policy is expressed as a predicate over the placement set, not as a
designated authoritative backend:

```text
durable(pack) :=
      count(live placements)                                   >= min_copies
  AND count(distinct failure domains among live placements)     >= min_domains
  AND count(live placements on offsite backends)                >= min_offsite

satisfied_by_deadline(pack) :=
      durable(pack)
   OR age(pack) < offsite_deadline
```

Two rules follow, and they are the ones a bug is most likely to violate:

- **No eviction may reduce the placement set below `durable`.** Every removal
  is evaluated against the state that would result, never against the state
  before it. A pack whose other placements are `pending` is not yet protected by
  them.
- **A pack that fails `satisfied_by_deadline` is an operational alarm, not a
  silent state.** It means the repository has data whose offsite guarantee has
  lapsed.

`offsite_deadline` is the recovery-point objective, stated by the operator
(for example "a second offsite copy within four hours of the backup"). It exists
because placement is asynchronous: bandwidth ceilings and API rate limits make
synchronous replication to every backend impractical, so a backup completes
before every placement is live. That window must be explicit and measured rather
than assumed to be zero.

Packs not yet satisfying the predicate are tracked in a deadline-ordered queue,
reusing the deletion-queue pattern below so the scheduler and any alarm are
range scans rather than catalog scans:

```text
key:   rq:<8-byte offsite deadline unix seconds>:<32-byte pack ID>
value: required placement classes, attempts, last error class
```

#### Deferring archival commitment

An archival class with a minimum retention bills a floor per object regardless
of how long the object is kept. Committing a pack to that class before knowing
whether it survives means paying the floor on data the retention policy will
discard within days.

Order-of-magnitude figures for one common provider, to show the shape of the
trade rather than to predict a bill:

| class | rate | minimum | effective floor |
|---|---|---|---|
| standard | ~$0.023/GB-month | none | ~$0.00077/GB-day |
| deep archive | ~$0.00099/GB-month | 180 days | ~$0.0059/GB, always |

The crossover is where the archival floor equals the standard-class cost of the
same period: roughly **eight days**. A pack that becomes unreachable sooner is
strictly cheaper in a standard class; a pack that lives for years is roughly
twenty times cheaper in the archival class. Neither class is wrong. Committing
before the outcome is known is what is wrong.

The promotion trigger should therefore be derived from the retention policy
rather than from a fixed timer, because the retention policy is what actually
determines a pack's lifetime. A pack lives as long as the longest-lived snapshot
referencing it, so the rule is:

> Promote a pack to the archival class when its surviving blobs are reachable
> only from snapshots that the configured forget policy will retain for longer
> than the archival minimum retention.

For a typical `--keep-daily 7 --keep-weekly 4 --keep-monthly 12` policy, that is
the point at which a pack's last daily-only reference has expired and it is held
by a monthly. This is computable from data vaultic already has: the forget
policy and the reachability result. The eight-day crossover is retained only as
a floor below which promotion is never correct, for repositories whose retention
policy cannot supply a horizon.

Deferral is not a durability compromise, because it is a decision about
*archival class*, not about *how many copies exist*. The offsite deadline is
satisfied independently, by a placement on a cheap non-archival offsite backend.
Conflating the two is the failure this section exists to remove.

#### Promotion is a repack, not a copy

A pack must not be promoted by copying the object from one class to another.
Promotion is a repack, and the moment of promotion is the best opportunity the
system ever gets to write a well-formed archival pack:

- survival is now *observed* rather than predicted, so the repack can include
  only blobs that actually lived, and packs written this way tend to become
  wholly unreachable at once;
- the source bytes are still on fast local storage, so the read is cheap and
  needs no warm-up;
- the target object size can be chosen freely, which matters because archival
  classes bill a fixed per-object overhead. At 8 MiB packs a terabyte is roughly
  130,000 objects; at 512 MiB it is roughly 2,000. The overhead difference is
  several gigabytes of pure billing surface.

This supersedes write-time lifetime grouping as the primary defence against cold
fragmentation. Grouping by predicted lifetime at write time remains a useful
secondary heuristic, but it guesses; promotion-time repacking measures. In
steady state, archival repacking after promotion should approach zero, because
the packs were assembled from data already known to be long-lived.

Promotion must be crash-safe in the same way as any other repack: write the new
archival pack, make its placement live, update the blob index, and only then
allow the superseded placements to be evicted. A crash at any point leaves
either the old or the new pack fully reachable, never neither.

#### Placement classes and the scheduler

Per-pack cost optimisation is a research project and is explicitly out of scope.
Policy is expressed as a small number of named placement classes, and every pack
is assigned exactly one:

| class | typical target | rationale |
|---|---|---|
| `metadata` | every backend, never archival | indexes, trees, and snapshots must stay readable to plan any restore |
| `recent-data` | on-premises primary plus a cheap offsite standard class | satisfies the offsite deadline without an archival minimum |
| `archival-data` | archival offsite, on-premises copy optional | entered only by promotion, once survival is observed |
| `cache` | on-premises only, evictable at any time | never the last copy, so eviction is always safe |

The scheduler resolves each pack's class to a target placement set, then works
to close the difference between target and actual, subject to per-backend
bandwidth and request-rate limits. It is a background, resumable worker: every
transition is durable, every action is idempotent, and an interrupted run
resumes from the placement records rather than from memory.

Ordering is by urgency, not by arrival: packs approaching their offsite deadline
precede promotions, which precede evictions. Eviction runs last because it is
the only action that can reduce durability, and it is gated on the predicate.

The existing tier-blind flags keep their meaning for repositories that declare a
single backend, and `--repack-cacheable-only` remains supported as the coarse
equivalent of "act on on-premises placements only".

#### Prune and repack policy per backend

`decidePackAction` is parameterised by the properties of the backend holding the
placement being considered, rather than by a hard-coded tier:

| | cheap, no minimum retention | expensive, minimum retention |
| --- | --- | --- |
| repack trigger | unused ratio above a low threshold, small-pack merging | only when the cost model below is satisfied; default off |
| delete trigger | wholly unreachable, delete now | wholly unreachable **and** past that placement's `min_retention_until` |
| repack budget | `--max-repack` applied to that backend's subtotal | separate per-backend budget, default `0` |
| target pack size | current defaults | substantially larger |

#### Cost model for repacking a constrained placement

A placement on a backend with egress or minimum-retention charges is repacked
only when the projected storage saving over a horizon exceeds the cost of moving
it, including the retention charge still owed on the object being replaced:

```text
saving = unused_payload_bytes * price_per_gb_month * horizon_months
cost   = egress(physical_size) + request_cost
       + physical_size * price_per_gb_month * remaining_retention_months
repack if saving > cost
```

Prices, horizon, and retention come from the backend registry, overridable per
run (`--cold-price-per-gb-month`, `--cold-egress-per-gb`,
`--cold-request-cost`, `--cold-horizon`, `--cold-min-retention`), defaulting to
values that make the predicate false. The point of the model is not to guess a
cloud bill accurately; it is to degrade gracefully into "do not repack" instead
of hard-coding a refusal, and to make the reason auditable in `--json` output.

The same inputs drive the promote decision, with `remaining_retention_months`
taken from the *target* class rather than the source.

#### Retention-aware deferred deletion

Two-phase prune already supports deferring deletion by a fixed duration.
Placement awareness generalises the deadline, and computes it per placement
rather than per pack:

```text
delete_after(placement) = max(now + keep_delete,
                              placement.min_retention_until)
```

Placements entering `evicting` are additionally indexed by deadline so the sweep
is a range scan rather than a catalog scan:

```text
key:   dq:<8-byte delete-after unix seconds>:<32-byte pack ID>:<8-byte backend ID hash>
value: backend, physical size, reason, originating run ID
```

Any later `prune`, `index gc`, or dedicated sweep collects the expired key
prefix. An archival placement therefore lingers until its retention expires and
is then collected on a routine run, with no operator arithmetic and no
early-deletion charge, while the on-premises placement of the same pack is freed
immediately. This is the single largest practical benefit of having a metadata
service: a time-ordered, per-placement deletion queue is not expressible in the
JSON index.

#### Pack history: recording every change, including to deleted packs

The pack catalog only describes the present. Growth rates, churn, and
"what did this repository look like six months ago" require history, and the
history must survive the deletion of the pack it describes.

The decision here is deliberate: **yes, record every pack lifecycle transition,
but as an append-only event log in its own key namespace, not by retaining full
pack records forever.** A pack record is a mutable current-state object with
provenance, sizes, and reference accounting; keeping every historical version of
it would grow with reachability churn rather than with real events, and would
entangle statistics retention with index correctness. An event log is bounded by
the number of transitions, is independent of the catalog's own compaction, and
can be pruned on its own schedule without ever affecting restorability.

```text
key:   ph:<8-byte event unix seconds>:<8-byte event seq>:<32-byte pack ID>
value: schema version, event type, backend ID, pack type,
       physical size, payload size, used/unused deltas,
       predecessor pack IDs (for repack lineage), run ID, reason code
```

Event types: `created`, `imported`, `published`, `tier_changed`, `placed`,
`placement_failed`, `promoted`, `evicted`, `usage_changed` (coalesced, not per
blob), `repacked_from`, `repacked_into`, `delete_pending`, `deleted`,
`delete_failed`, `orphan_detected`.

Placement transitions carry the backend ID, so the log answers where a pack has
lived over time and not merely that it existed. `promoted` records the class
change with both predecessor and successor pack IDs, because promotion is a
repack and produces a new pack rather than moving an object. `tier_changed`
records the Phase 9 tier becoming known or moving; the placement events
supersede it from Phase 12 onward, but the value is never reused because
historical entries must stay decodable.

The event key is time-ordered, so histograms and growth queries are range scans
with no catalog access, and events for packs that no longer exist remain
readable. `repacked_from`/`repacked_into` preserve lineage, so churn can be
distinguished from genuine growth: a repack that rewrites 100 GiB is not 100 GiB
of new data, and any growth-rate answer that cannot tell those apart is
misleading.

**Retention and downsampling.** Raw events are pruned, but not by simply
dropping them: they are first rolled up into fixed time buckets that are cheap
enough to keep effectively forever.

```text
key:   pb:<bucket-granularity>:<8-byte bucket start>:<backend ID>:<pack type>
value: packs created, packs deleted, packs repacked, packs promoted,
       bytes added, bytes deleted, bytes repacked, bytes promoted,
       end-of-bucket pack count and physical/payload totals,
       coverage flag (complete, partial, reconstructed)
```

Granularities: hourly, daily, monthly. The rollup is a pure function of the raw
events in the bucket, so it is idempotent and rebuildable while the raw events
still exist. Default retention: raw events kept for a bounded window, hourly
buckets for a longer window, daily buckets for years, monthly buckets
indefinitely. A monthly bucket per tier and pack type is a handful of records
per month; the storage cost is negligible against a 500 TB repository.

Two rules keep this honest:

- A bucket is only marked `complete` if the roll-up ran over a fully retained
  raw range. Buckets covering a period before history collection was enabled,
  or reconstructed from an import, are flagged `partial` or `reconstructed`, and
  every CLI and JSON output surfaces that flag. Statistics must not silently
  present an incomplete series as authoritative.
- History is strictly derived and advisory. `index check` may report history
  gaps or drift against the pack catalog, but a missing or corrupt history
  record is never an error that blocks backup, restore, prune, or GC, and
  history is never an input to a destructive decision. Retention decisions read
  `min_retention_until` from the pack record, not from the event log.

History pruning itself is a normal maintenance operation with a dry-run mode,
and it emits its own coverage marker so a later query can tell that the raw
range was intentionally truncated rather than lost.

#### Backend introspection commands

Add read-only commands to the existing `index` group. They resolve the daemon
when the repository is SlateDB-authoritative; for legacy repositories they
either operate in a reduced mode from the JSON index or fail with an explicit
message, never with a partially populated answer presented as complete.

`vaultic index stats` — constant-time repository composition from the aggregate
records.

- `--by backend|type|state|class|backend,type` grouping
- `--backend <id>`, `--class metadata|recent-data|archival-data|cache`,
  `--type data|tree|mixed|unknown`
- `--verify` recompute from `p:`/`pl:` records and report drift
- `--rebuild` rewrite aggregates from `p:`/`pl:` records
- `--json`

Reports pack count, physical size, payload size, header size, blob count, used
and unused payload bytes, unused ratio, and mixed/unknown counts. Unplaced,
unknown-class, and unknown-type packs are always reported explicitly rather than
folded into a total. Because a pack may hold several placements, per-backend
byte totals sum to more than the repository's logical size; the report states
both the logical total and the stored total so the difference is never mistaken
for drift.

`vaultic index packs` — query the pack catalog.

- filters: `--backend`, `--class`, `--type`, `--state`, `--created-before`,
  `--created-after`, `--min-size`, `--max-size`, `--unused-ratio-above`,
  `--retention-expired`, `--retention-unknown`, `--delete-pending`,
  `--not-offsite`, `--promotion-due`
- `--sort size|created|unused|unused-ratio|delete-after|offsite-deadline`,
  `--limit`, `--count-only`
- `--json`

`vaultic index history` — time-series and histograms over the event log and
rollups.

- `--metric packs|bytes|created|deleted|repacked|promoted|net-growth|unused`
- `--bucket hour|day|week|month`
- `--since`, `--until`
- `--by backend|type`
- `--histogram` render a distribution of pack creation or last-change times
- `--forecast` project growth from the retained series, always annotated with
  the coverage flags of the buckets used
- `--json`

Repack churn is reported separately from net growth by default, and promotion is
reported separately from both, because a promoted pack is neither new data nor
ordinary churn. `--forecast` refuses to extrapolate from a series whose buckets
are `partial` or `reconstructed` unless `--allow-incomplete` is given.

`vaultic index history prune` — retention for the event log.

- `--keep-raw`, `--keep-hourly`, `--keep-daily`, `--keep-monthly`
- `--dry-run`, `--json`

`vaultic index placement` — placement state and durability posture.

- default output: per backend, the number of packs and bytes `pending`, `live`,
  and `evicting`; the count of packs not yet meeting the durability predicate;
  and the age of the oldest unsatisfied offsite deadline
- `--unsatisfied` list packs failing `durable`, ordered by deadline
- `--overdue` restrict to packs past `offsite_deadline`
- `--pending-promotion` list packs whose retention horizon now justifies
  archival placement
- `--explain <pack ID>` show that pack's placement set, its class, the target
  set, and which rule produced each difference
- `--json`

This command is the operator-facing form of the offsite guarantee. It must be
usable as a monitoring probe, so the JSON output includes the oldest unsatisfied
deadline as an absolute timestamp and an age, and exits non-zero when any pack is
past its deadline unless `--no-fail` is given.

`vaultic index backends` — per-backend view.

- reports, per backend: configured location and declared properties, object
  count and bytes by file type, storage class where the backend exposes it,
  configured minimum retention, capacity headroom where a ceiling is declared,
  and warm-up configuration in effect
- `--compare` cross-check backend listing against the placement records and
  report packs placed in the catalog but missing on the backend, and objects
  present on the backend but unknown to the catalog
- `--no-list` answer from the catalog only, without paying for a backend
  listing
- `--json`

`--compare` performs a full backend listing and is explicitly opt-in, because on
an archival backend a listing has a real cost.

Implementation phases for this section are Phases 9, 12, and 16 of the plan
below.

