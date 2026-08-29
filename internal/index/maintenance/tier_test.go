package maintenance

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type tierFixture struct {
	store       *memoryStore
	destination *memoryDestination
	records     []schema.PackRecord
	packIDs     []vaultic.ID
}

// newTierFixture builds a consistent catalog holding one pack per requested
// tier, with matching blob records and both aggregate dimensions written, then
// exports it so a differential check starts clean.
func newTierFixture(t *testing.T, tiers ...schema.PackTier) *tierFixture {
	t.Helper()
	fixture := &tierFixture{store: &memoryStore{values: make(map[string][]byte)}, destination: &memoryDestination{}}
	for index, tier := range tiers {
		packType, blobType := schema.PackData, schema.BlobData
		if tier == schema.TierMirrored {
			packType, blobType = schema.PackTree, schema.BlobTree
		}
		payload := uint64(10 * (index + 1))
		packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID()
		record := schema.PackRecord{
			Type: packType, PayloadSize: payload, BlobCount: 1,
			Lifecycle: schema.PackPublished, Tier: tier,
			CreationTime: 1, CreationTimeKnown: true, RetentionSource: schema.RetentionUnknown,
		}
		fixture.store.set(t, schema.PackKey(schema.ID(packID)), record)
		fixture.store.set(t, schema.BlobKey(schema.ID(blobID)), schema.BlobRecord{
			Locations: []schema.BlobLocation{{PackID: schema.ID(packID), Offset: 0, Length: uint32(payload), Type: blobType}},
		})
		fixture.records = append(fixture.records, record)
		fixture.packIDs = append(fixture.packIDs, packID)
	}
	fixture.writeAggregates(t, fixture.records)
	if _, err := Export(context.Background(), fixture.store, fixture.destination, ExportOptions{Full: true}); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture *tierFixture) writeAggregates(t *testing.T, records []schema.PackRecord) {
	t.Helper()
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
}

func (fixture *tierFixture) check(t *testing.T) CheckResult {
	t.Helper()
	result, err := Check(context.Background(), fixture.destination, fixture.store, 10)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// TestTierAggregatesReportPerTierTotals is the Phase 9 exit criterion: a
// hot/cold repository reports correct per-tier totals and checks clean.
func TestTierAggregatesReportPerTierTotals(t *testing.T) {
	fixture := newTierFixture(t, schema.TierCold, schema.TierCold, schema.TierMirrored)

	tiers, err := schema.RebuildTierAggregates(fixture.records, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tiers[schema.TierCold].PackCount != 2 || tiers[schema.TierCold].PayloadSize != 10+20 {
		t.Fatalf("cold aggregate = %+v", tiers[schema.TierCold])
	}
	if tiers[schema.TierMirrored].PackCount != 1 || tiers[schema.TierMirrored].PayloadSize != 30 {
		t.Fatalf("mirrored aggregate = %+v", tiers[schema.TierMirrored])
	}
	for _, tier := range []schema.PackTier{schema.TierUnknown, schema.TierHot, schema.TierSingle} {
		if tiers[tier].PackCount != 0 {
			t.Fatalf("tier %v = %+v", tier, tiers[tier])
		}
	}
	// The type dimension still totals every pack independently of tier.
	types, err := schema.RebuildPackAggregates(fixture.records, 1)
	if err != nil {
		t.Fatal(err)
	}
	if types[schema.AggregateAll].PackCount != 3 || types[schema.AggregateAll].PayloadSize != 60 {
		t.Fatalf("all aggregate = %+v", types[schema.AggregateAll])
	}

	result := fixture.check(t)
	if !result.Clean() || result.AggregateMismatch != 0 {
		t.Fatalf("check = %#v", result)
	}
	if result.UnknownTierPacks != 0 {
		t.Fatalf("unknown tier packs = %d", result.UnknownTierPacks)
	}
	if result.RetentionUnknownPacks != 3 || result.UsageUnaccountedPacks != 3 {
		t.Fatalf("lifetime counters = %#v", result)
	}
	rebuilt, err := RebuildPackAggregates(context.Background(), fixture.store, false)
	if err != nil || rebuilt.AggregatesChanged != 0 {
		t.Fatalf("rebuild over clean catalog = %#v, %v", rebuilt, err)
	}
}

// TestTierAggregateDriftIsDetectedAndRepaired covers drift in the tier
// dimension specifically: the type dimension can be perfectly consistent while
// a tier record is wrong.
func TestTierAggregateDriftIsDetectedAndRepaired(t *testing.T) {
	fixture := newTierFixture(t, schema.TierCold, schema.TierMirrored)
	fixture.store.set(t, schema.TierAggregateKey(schema.TierCold), schema.PackAggregate{PackCount: 99, PayloadSize: 12345, UpdateSequence: 1})

	result := fixture.check(t)
	if result.AggregateMismatch != 1 || result.Clean() {
		t.Fatalf("drifted tier check = %#v", result)
	}

	rebuilt, err := RebuildPackAggregates(context.Background(), fixture.store, false)
	if err != nil || rebuilt.AggregatesChanged != 1 {
		t.Fatalf("tier rebuild = %#v, %v", rebuilt, err)
	}
	if len(rebuilt.Deltas) != 1 || rebuilt.Deltas[0].Tier != schema.TierCold || rebuilt.Deltas[0].Before == nil {
		t.Fatalf("tier delta = %#v", rebuilt.Deltas)
	}
	if after := fixture.check(t); !after.Clean() || after.AggregateMismatch != 0 {
		t.Fatalf("check after tier repair = %#v", after)
	}
}

// TestTierAggregateRebuildIsAtomicAndDryRunnable mirrors the type-dimension
// guarantees: nothing is written on a dry run, and a real rebuild writes every
// aggregate in exactly one batch.
func TestTierAggregateRebuildIsAtomicAndDryRunnable(t *testing.T) {
	fixture := newTierFixture(t, schema.TierCold, schema.TierMirrored)
	mirroredKey := string(schema.TierAggregateKey(schema.TierMirrored))
	fixture.store.set(t, schema.TierAggregateKey(schema.TierMirrored), schema.PackAggregate{PackCount: 99, UpdateSequence: 1})
	before := append([]byte(nil), fixture.store.values[mirroredKey]...)

	writes := fixture.store.batchWrites
	dry, err := RebuildPackAggregates(context.Background(), fixture.store, true)
	if err != nil || dry.AggregatesChanged != 1 {
		t.Fatalf("dry run = %#v, %v", dry, err)
	}
	if fixture.store.batchWrites != writes {
		t.Fatal("dry run wrote to the store")
	}
	if string(fixture.store.values[mirroredKey]) != string(before) {
		t.Fatal("dry run mutated the aggregate")
	}

	if _, err := RebuildPackAggregates(context.Background(), fixture.store, false); err != nil {
		t.Fatal(err)
	}
	if fixture.store.batchWrites != writes+1 {
		t.Fatalf("rebuild used %d batches, want exactly one", fixture.store.batchWrites-writes)
	}
	if after := fixture.check(t); !after.Clean() {
		t.Fatalf("check after rebuild = %#v", after)
	}
}

// TestMalformedTierAggregateIsRepaired ensures a corrupt tier record is
// tolerated and rewritten rather than aborting the check.
func TestMalformedTierAggregateIsRepaired(t *testing.T) {
	fixture := newTierFixture(t, schema.TierCold)
	fixture.store.values[string(schema.TierAggregateKey(schema.TierCold))] = []byte{0}

	result := fixture.check(t)
	if result.AggregateMismatch != 1 || result.Clean() {
		t.Fatalf("malformed tier check = %#v", result)
	}
	if _, err := RebuildPackAggregates(context.Background(), fixture.store, false); err != nil {
		t.Fatal(err)
	}
	if after := fixture.check(t); !after.Clean() {
		t.Fatalf("check after malformed tier repair = %#v", after)
	}
}

// TestRepositoryPredatingTierAggregatesIsNotDrift covers the upgrade path. A
// repository written before the tier dimension existed has no a:tier: records
// at all; that is a pending rebuild, not corruption, and must not fail a check
// that is otherwise clean.
func TestRepositoryPredatingTierAggregatesIsNotDrift(t *testing.T) {
	fixture := newTierFixture(t, schema.TierCold, schema.TierMirrored)
	for _, tier := range schema.TierAggregateKinds() {
		delete(fixture.store.values, string(schema.TierAggregateKey(tier)))
	}

	result := fixture.check(t)
	if result.AggregateMismatch != 0 || !result.Clean() {
		t.Fatalf("unbuilt tier dimension reported as drift = %#v", result)
	}
	if !result.TierAggregatesUnbuilt {
		t.Fatal("unbuilt tier dimension was not reported at all")
	}

	rebuilt, err := RebuildPackAggregates(context.Background(), fixture.store, false)
	if err != nil || rebuilt.AggregatesChanged != 2 {
		t.Fatalf("rebuild after upgrade = %#v, %v", rebuilt, err)
	}

	after := fixture.check(t)
	if !after.Clean() || after.TierAggregatesUnbuilt {
		t.Fatalf("check after upgrade rebuild = %#v", after)
	}
	expected, err := schema.RebuildTierAggregates(fixture.records, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, tier := range []schema.PackTier{schema.TierCold, schema.TierMirrored} {
		value, found, getErr := fixture.store.Get(context.Background(), schema.TierAggregateKey(tier))
		if getErr != nil || !found {
			t.Fatalf("tier %v missing after rebuild", tier)
		}
		stored, decodeErr := schema.UnmarshalPackAggregate(value)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		stored.UpdateSequence = 0
		if stored != expected[tier] {
			t.Fatalf("tier %v = %+v, want %+v", tier, stored, expected[tier])
		}
	}
	// Empty tiers must not be materialized by a rebuild.
	for _, tier := range []schema.PackTier{schema.TierHot, schema.TierSingle, schema.TierUnknown} {
		if _, found, _ := fixture.store.Get(context.Background(), schema.TierAggregateKey(tier)); found {
			t.Fatalf("rebuild materialized empty tier %v", tier)
		}
	}
}

// TestPartiallyBuiltTierDimensionIsPendingNotDrift covers an upgraded
// repository that has published a new pack: that publish bootstraps one tier
// record, leaving the others absent. The present record is correct, so the
// repository is reported as needing a rebuild rather than as corrupt.
func TestPartiallyBuiltTierDimensionIsPendingNotDrift(t *testing.T) {
	fixture := newTierFixture(t, schema.TierCold, schema.TierMirrored)
	delete(fixture.store.values, string(schema.TierAggregateKey(schema.TierMirrored)))

	result := fixture.check(t)
	if result.AggregateMismatch != 0 || !result.Clean() {
		t.Fatalf("partially built tier dimension reported as drift = %#v", result)
	}
	if !result.TierAggregatesUnbuilt {
		t.Fatal("partially built tier dimension was not reported")
	}

	if _, err := RebuildPackAggregates(context.Background(), fixture.store, false); err != nil {
		t.Fatal(err)
	}
	if after := fixture.check(t); !after.Clean() || after.TierAggregatesUnbuilt {
		t.Fatalf("check after rebuild = %#v", after)
	}
}

// TestUnknownTierAndRetentionArePlainCounts asserts that an imported catalog,
// which is legitimately tier-unknown and retention-unknown forever, still
// checks clean and does not flood the findings list.
func TestUnknownTierAndRetentionArePlainCounts(t *testing.T) {
	fixture := newTierFixture(t, schema.TierUnknown, schema.TierUnknown, schema.TierUnknown)

	result := fixture.check(t)
	if !result.Clean() {
		t.Fatalf("tier-unknown catalog did not check clean = %#v", result)
	}
	if result.UnknownTierPacks != 3 || result.RetentionUnknownPacks != 3 {
		t.Fatalf("counters = %#v", result)
	}
	for _, finding := range result.Findings {
		if finding.Kind == "unknown_tier_pack" {
			t.Fatal("unknown tier emitted per-pack findings")
		}
	}
}
