package maintenance

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// goldenJSON compares a command result against a stored JSON document. The
// stored document is the command's `--json` contract, so a change to it is a
// visible schema change rather than an incidental refactor.
func goldenJSON(t *testing.T, name string, value any) {
	t.Helper()
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join("testdata", name+".json")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (set UPDATE_GOLDEN=1 to create it): %v", path, err)
	}
	if string(expected) != string(encoded) {
		t.Fatalf("golden %s mismatch:\nwant:\n%s\ngot:\n%s", path, expected, encoded)
	}
}

// introspectFixture builds a catalog with deterministic pack IDs so golden
// output is stable across runs.
type introspectFixture struct {
	store *memoryStore
	packs []vaultic.ID
}

func deterministicID(seed byte) vaultic.ID {
	var id vaultic.ID
	for index := range len(id) {
		id[index] = seed
	}
	return id
}

// The catalog stores creation and retention deadlines in Unix nanoseconds.
const introspectNow = int64(1_700_000_000) * int64(time.Second)

func newIntrospectFixture(t *testing.T) *introspectFixture {
	t.Helper()
	fixture := &introspectFixture{store: &memoryStore{values: make(map[string][]byte)}}
	records := []schema.PackRecord{
		{
			Type: schema.PackData, PhysicalSize: 1000, PhysicalSizeKnown: true, PayloadSize: 900,
			HeaderSize: 100, BlobCount: 3, Lifecycle: schema.PackPublished, Tier: schema.TierHot,
			CreationTime: introspectNow - int64(time.Hour), CreationTimeKnown: true,
			RetentionSource: schema.RetentionConfig, MinRetentionUntil: introspectNow - int64(time.Minute),
			UsedPayloadBytes: 600, UnusedPayloadBytes: 300, UsageKnown: true,
		},
		{
			Type: schema.PackTree, PhysicalSize: 2000, PhysicalSizeKnown: true, PayloadSize: 1800,
			HeaderSize: 200, BlobCount: 5, Lifecycle: schema.PackPublished, Tier: schema.TierCold,
			CreationTime: introspectNow - int64(2*time.Hour), CreationTimeKnown: true,
			RetentionSource: schema.RetentionBackend, MinRetentionUntil: introspectNow + int64(24*time.Hour),
			UsedPayloadBytes: 1800, UsageKnown: true,
		},
		{
			// Deliberately unknown along every dimension: unknown tier, unknown
			// type, unknown retention, unknown creation time, unaccounted usage.
			// Phase 11 requires these to be counted rather than folded away.
			Type: schema.PackUnknown, PayloadSize: 500, BlobCount: 1,
			Lifecycle: schema.PackDeletePending, Tier: schema.TierUnknown,
			RetentionSource: schema.RetentionUnknown,
		},
	}
	for index, record := range records {
		packID := deterministicID(byte(index + 1))
		fixture.store.set(t, schema.PackKey(schema.ID(packID)), record)
		fixture.packs = append(fixture.packs, packID)
	}
	types, err := schema.RebuildPackAggregates(records, 1)
	if err != nil {
		t.Fatal(err)
	}
	for kind, aggregate := range types {
		fixture.store.set(t, schema.PackAggregateKey(kind), aggregate)
	}
	tiers, err := schema.RebuildTierAggregates(records, 1)
	if err != nil {
		t.Fatal(err)
	}
	for tier, aggregate := range tiers {
		if aggregate.PackCount == 0 {
			continue
		}
		fixture.store.set(t, schema.TierAggregateKey(tier), aggregate)
	}
	return fixture
}

func (fixture *introspectFixture) stats(t *testing.T, options StatsOptions) StatsResult {
	t.Helper()
	result, err := Stats(context.Background(), fixture.store, options)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func (fixture *introspectFixture) query(t *testing.T, filter PackFilter) PacksResult {
	t.Helper()
	if filter.Now.IsZero() {
		filter.Now = time.Unix(0, introspectNow).UTC()
	}
	result, err := QueryPacks(context.Background(), fixture.store, filter)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// TestStatsGoldenOutput pins the `index stats --json` contract.
func TestStatsGoldenOutput(t *testing.T) {
	fixture := newIntrospectFixture(t)
	goldenJSON(t, "stats_all", fixture.stats(t, StatsOptions{}))
	goldenJSON(t, "stats_by_tier_type", fixture.stats(t, StatsOptions{GroupBy: []string{"tier", "type"}}))
	goldenJSON(t, "stats_by_state", fixture.stats(t, StatsOptions{GroupBy: []string{"state"}}))
}

// TestPacksGoldenOutput pins the `index packs --json` contract.
func TestPacksGoldenOutput(t *testing.T) {
	fixture := newIntrospectFixture(t)
	goldenJSON(t, "packs_all", fixture.query(t, PackFilter{Sort: "id"}))
	goldenJSON(t, "packs_count_only", fixture.query(t, PackFilter{CountOnly: true}))
}

// TestStatsUsesAggregatesUntilAQuestionNeedsTheCatalog is the constant-time
// guarantee: composition without filters must not scan the catalog, and the
// answer must say which path produced it.
func TestStatsUsesAggregatesUntilAQuestionNeedsTheCatalog(t *testing.T) {
	fixture := newIntrospectFixture(t)
	if source := fixture.stats(t, StatsOptions{}).Source; source != SourceAggregates {
		t.Fatalf("unfiltered stats scanned the catalog: %s", source)
	}
	if source := fixture.stats(t, StatsOptions{GroupBy: []string{"type"}}).Source; source != SourceAggregates {
		t.Fatalf("type grouping is an aggregate dimension but scanned the catalog: %s", source)
	}
	for name, options := range map[string]StatsOptions{
		"state grouping": {GroupBy: []string{"state"}},
		"tier grouping":  {GroupBy: []string{"tier"}},
		"tier filter":    {Tier: "hot"},
		"type filter":    {Type: "data"},
		"verify":         {Verify: true},
	} {
		if source := fixture.stats(t, options).Source; source != SourceCatalog {
			t.Fatalf("%s should require the catalog but reported %s", name, source)
		}
	}
}

// TestStatsReportsLogicalAndStoredTotalsSeparately keeps the two totals
// distinct so that, once a pack may have several placements, the difference is
// not mistaken for drift.
func TestStatsReportsLogicalAndStoredTotalsSeparately(t *testing.T) {
	result := newIntrospectFixture(t).stats(t, StatsOptions{})
	if result.Totals.PhysicalSize != 3000 {
		t.Fatalf("logical physical size = %d, want 3000", result.Totals.PhysicalSize)
	}
	if result.StoredPhysicalSize != result.Totals.PhysicalSize {
		t.Fatalf("stored (%d) and logical (%d) differ before placements exist",
			result.StoredPhysicalSize, result.Totals.PhysicalSize)
	}
}

// TestStatsSurfacesUnknownsExplicitly is Phase 11 step 7: an unknown must be
// visible as unknown, never absorbed into a total.
func TestStatsSurfacesUnknownsExplicitly(t *testing.T) {
	result := newIntrospectFixture(t).stats(t, StatsOptions{GroupBy: []string{"state"}})
	if result.UnknownTierPacks != 1 {
		t.Fatalf("unknown tier packs = %d, want 1", result.UnknownTierPacks)
	}
	if result.UnknownTypePacks != 1 {
		t.Fatalf("unknown type packs = %d, want 1", result.UnknownTypePacks)
	}
	if result.RetentionUnknownPacks != 1 {
		t.Fatalf("retention unknown packs = %d, want 1", result.RetentionUnknownPacks)
	}
	if result.UsageUnaccountedPacks != 1 {
		t.Fatalf("usage unaccounted packs = %d, want 1", result.UsageUnaccountedPacks)
	}
	if !result.RetentionCounted {
		t.Fatal("a catalog scan did not mark the retention count as measured")
	}
	if result.CreationTimeUnknownPacks != 1 {
		t.Fatalf("creation time unknown packs = %d, want 1", result.CreationTimeUnknownPacks)
	}
	if result.PhysicalSizeUnknownPacks != 1 {
		t.Fatalf("physical size unknown packs = %d, want 1", result.PhysicalSizeUnknownPacks)
	}
	// The unused ratio must be derived from accounted packs only, so the
	// unaccounted pack does not silently dilute it.
	if result.Totals.AccountedPackCount != 2 {
		t.Fatalf("accounted pack count = %d, want 2", result.Totals.AccountedPackCount)
	}
	if want := 300.0 / 2700.0; result.Totals.UnusedRatio < want-1e-9 || result.Totals.UnusedRatio > want+1e-9 {
		t.Fatalf("unused ratio = %f, want %f (accounted payload only)", result.Totals.UnusedRatio, want)
	}
}

// TestAggregatePathDoesNotClaimZeroUnknownRetention: no aggregate carries the
// retention dimension, so the constant-time path must report the count as
// not measured rather than as zero.
func TestAggregatePathDoesNotClaimZeroUnknownRetention(t *testing.T) {
	result := newIntrospectFixture(t).stats(t, StatsOptions{})
	if result.Source != SourceAggregates {
		t.Fatalf("expected the aggregate path, got %s", result.Source)
	}
	if result.RetentionCounted {
		t.Fatal("the aggregate path claimed to have measured retention")
	}
	if result.RetentionUnknownPacks != 0 {
		t.Fatalf("unmeasured retention count was populated: %d", result.RetentionUnknownPacks)
	}
}

// TestPackFiltersSelectExactly covers each catalog filter dimension.
func TestPackFiltersSelectExactly(t *testing.T) {
	fixture := newIntrospectFixture(t)
	moment := time.Unix(0, introspectNow).UTC()
	for name, testCase := range map[string]struct {
		filter PackFilter
		want   uint64
	}{
		"tier":              {PackFilter{Tier: "hot"}, 1},
		"type":              {PackFilter{Type: "tree"}, 1},
		"state":             {PackFilter{State: "delete-pending"}, 1},
		"delete-pending":    {PackFilter{DeletePending: true}, 1},
		"min-size":          {PackFilter{MinSize: 1500}, 1},
		"max-size":          {PackFilter{MaxSize: 1500}, 2},
		"unused-ratio":      {PackFilter{UnusedRatioAbove: 0.2}, 1},
		"retention-expired": {PackFilter{RetentionExpired: true}, 1},
		"retention-unknown": {PackFilter{RetentionUnknown: true}, 1},
		"created-after":     {PackFilter{CreatedAfter: moment.Add(-90 * time.Minute)}, 1},
		"created-before":    {PackFilter{CreatedBefore: moment.Add(-90 * time.Minute)}, 1},
	} {
		if got := fixture.query(t, testCase.filter).Matched; got != testCase.want {
			t.Errorf("filter %s matched %d packs, want %d", name, got, testCase.want)
		}
	}
}

// TestUnknownCreationTimeNeverSatisfiesATimeFilter: a pack whose creation time
// was never recorded is not evidence for either side of a time question, so it
// must fall out of both.
func TestUnknownCreationTimeNeverSatisfiesATimeFilter(t *testing.T) {
	fixture := newIntrospectFixture(t)
	unknown := deterministicID(3).String()
	moment := time.Unix(0, introspectNow).UTC()
	for name, filter := range map[string]PackFilter{
		"before": {CreatedBefore: moment.Add(time.Hour)},
		"after":  {CreatedAfter: moment.AddDate(-1, 0, 0)},
	} {
		for _, entry := range fixture.query(t, filter).Packs {
			if entry.ID == unknown {
				t.Fatalf("%s filter returned the pack with an unknown creation time", name)
			}
		}
	}
}

// TestRetentionExpiredRequiresRetentionToHaveBeenKnown: treating an unknown
// deadline as expired would authorise deleting data that a backend lock still
// protects.
func TestRetentionExpiredRequiresRetentionToHaveBeenKnown(t *testing.T) {
	fixture := newIntrospectFixture(t)
	unknown := deterministicID(3).String()
	for _, entry := range fixture.query(t, PackFilter{RetentionExpired: true}).Packs {
		if entry.ID == unknown {
			t.Fatal("a pack with unknown retention was reported as retention-expired")
		}
	}
}

// TestFiltersReportWhatTheyCouldNotDecide: excluding a pack because the fact
// the filter asks about was never recorded is not the same as excluding it on
// the merits, and an operator reading "matched 0" must be able to tell the
// difference.
func TestFiltersReportWhatTheyCouldNotDecide(t *testing.T) {
	fixture := newIntrospectFixture(t)
	moment := time.Unix(0, introspectNow).UTC()

	timed := fixture.query(t, PackFilter{CreatedAfter: moment.AddDate(-1, 0, 0)})
	if timed.UndecidableCreatedTime != 1 || timed.Undecidable != 1 {
		t.Fatalf("time filter did not report the pack it could not judge: %#v", timed)
	}

	retention := fixture.query(t, PackFilter{RetentionExpired: true})
	if retention.UndecidableRetention != 1 || retention.Undecidable != 1 {
		t.Fatalf("retention filter did not report the pack it could not judge: %#v", retention)
	}

	usage := fixture.query(t, PackFilter{UnusedRatioAbove: 0.1})
	if usage.UndecidableUsage != 1 || usage.Undecidable != 1 {
		t.Fatalf("usage filter did not report the pack it could not judge: %#v", usage)
	}

	// A filter over a dimension where "unknown" is itself a value always has
	// an answer, so nothing is undecidable there.
	tiered := fixture.query(t, PackFilter{Tier: "hot"})
	if tiered.Undecidable != 0 {
		t.Fatalf("a tier filter reported undecidable packs: %#v", tiered)
	}
}

// TestPackSortOrdersAreDeterministic covers every sort key and confirms the
// fallback to pack ID makes ties stable.
func TestPackSortOrdersAreDeterministic(t *testing.T) {
	fixture := newIntrospectFixture(t)
	for _, order := range []string{"id", "size", "created", "unused", "unused-ratio", "delete-after"} {
		first := fixture.query(t, PackFilter{Sort: order})
		second := fixture.query(t, PackFilter{Sort: order})
		for index := range first.Packs {
			if first.Packs[index].ID != second.Packs[index].ID {
				t.Fatalf("sort %s is not deterministic at position %d", order, index)
			}
		}
	}
	sized := fixture.query(t, PackFilter{Sort: "size"})
	for index := 1; index < len(sized.Packs); index++ {
		if sized.Packs[index-1].PhysicalSize < sized.Packs[index].PhysicalSize {
			t.Fatalf("size sort is not descending: %#v", sized.Packs)
		}
	}
	if _, err := QueryPacks(context.Background(), fixture.store, PackFilter{Sort: "nonsense"}); err == nil {
		t.Fatal("an unknown sort key was accepted")
	}
}

// TestCountOnlySkipsMaterialisingPacks keeps the cheap answer cheap.
func TestCountOnlySkipsMaterialisingPacks(t *testing.T) {
	fixture := newIntrospectFixture(t)
	result := fixture.query(t, PackFilter{CountOnly: true})
	if result.Matched != 3 {
		t.Fatalf("matched = %d, want 3", result.Matched)
	}
	if len(result.Packs) != 0 || result.Returned != 0 {
		t.Fatalf("--count-only returned pack entries: %#v", result)
	}
}

// TestLimitCapsReturnedWithoutDistortingMatched: an operator paging through
// results must still see the true size of the answer.
func TestLimitCapsReturnedWithoutDistortingMatched(t *testing.T) {
	result := newIntrospectFixture(t).query(t, PackFilter{Limit: 1})
	if result.Matched != 3 {
		t.Fatalf("limit changed matched to %d, want 3", result.Matched)
	}
	if result.Returned != 1 || len(result.Packs) != 1 {
		t.Fatalf("limit did not cap returned packs: %#v", result)
	}
}

// TestStatsRejectsUnknownDimensions: an unrecognised grouping or filter value
// must fail rather than quietly report an empty answer.
func TestStatsRejectsUnknownDimensions(t *testing.T) {
	fixture := newIntrospectFixture(t)
	for name, options := range map[string]StatsOptions{
		"grouping": {GroupBy: []string{"placement"}},
		"tier":     {Tier: "glacier"},
		"type":     {Type: "blob"},
		"state":    {State: "archived"},
	} {
		if _, err := Stats(context.Background(), fixture.store, options); err == nil {
			t.Errorf("unknown %s was accepted", name)
		}
	}
}

// TestStatsVerifyDetectsAggregateDrift makes --verify meaningful: a corrupted
// aggregate must be reported as drift rather than returned as truth.
func TestStatsVerifyDetectsAggregateDrift(t *testing.T) {
	fixture := newIntrospectFixture(t)
	fixture.store.set(t, schema.PackAggregateKey(schema.AggregateAll), schema.PackAggregate{
		PackCount: 99, PayloadSize: 1, UpdateSequence: 2,
	})
	result := fixture.stats(t, StatsOptions{Verify: true})
	if !result.HasDrift() {
		t.Fatalf("corrupted aggregate was not reported as drift: %#v", result)
	}
	// The reported totals come from the catalog, not the corrupted aggregate.
	if result.Totals.PackCount != 3 {
		t.Fatalf("verified totals came from the corrupt aggregate: %#v", result.Totals)
	}
}

// TestStatsRebuildRepairsDrift closes the loop: --rebuild must make a
// subsequent --verify clean.
func TestStatsRebuildRepairsDrift(t *testing.T) {
	fixture := newIntrospectFixture(t)
	fixture.store.set(t, schema.PackAggregateKey(schema.AggregateAll), schema.PackAggregate{
		PackCount: 99, PayloadSize: 1, UpdateSequence: 2,
	})
	if result := fixture.stats(t, StatsOptions{Rebuild: true, DryRun: true}); result.Rebuilt == nil {
		t.Fatal("dry-run rebuild reported nothing")
	}
	if result := fixture.stats(t, StatsOptions{Verify: true}); !result.HasDrift() {
		t.Fatal("dry-run rebuild wrote changes")
	}
	if result := fixture.stats(t, StatsOptions{Rebuild: true}); result.Rebuilt == nil {
		t.Fatal("rebuild reported nothing")
	}
	if result := fixture.stats(t, StatsOptions{Verify: true}); result.HasDrift() {
		t.Fatalf("drift survived a rebuild: %#v", result.Drift)
	}
}

// TestEveryJSONContractIsVersioned is Phase 11 step 6: when the placement
// model replaces tiers with backends, consumers must see a version bump rather
// than a silently reinterpreted field.
func TestEveryJSONContractIsVersioned(t *testing.T) {
	fixture := newIntrospectFixture(t)
	store := newHistoryStore(t)

	series, err := HistorySeries(context.Background(), store, SeriesOptions{Metric: "bytes", Bucket: "day"})
	if err != nil {
		t.Fatal(err)
	}
	pruned, err := PruneHistory(context.Background(), store, HistoryRetentionOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	for name, version := range map[string]int{
		"stats":         fixture.stats(t, StatsOptions{}).SchemaVersion,
		"packs":         fixture.query(t, PackFilter{}).SchemaVersion,
		"history":       series.SchemaVersion,
		"history prune": pruned.SchemaVersion,
	} {
		if version != IntrospectSchemaVersion {
			t.Errorf("%s reported schema version %d, want %d", name, version, IntrospectSchemaVersion)
		}
	}
}
