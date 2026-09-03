# Phase 11: Introspection CLI

[← Back to roadmap index](00-overview.md)

[← Phase 10](phase-10-pack-history-event-log-and-rollups.md) · [Phase 12 →](phase-12-backend-registry-placement-records-and-per-backend-prune.md)

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
