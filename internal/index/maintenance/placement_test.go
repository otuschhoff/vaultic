package maintenance

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const (
	testPrimaryBackend  = uint64(0x1111)
	testArchivalBackend = uint64(0x2222)
	testCacheBackend    = uint64(0x3333)
)

func testPlacementModel() PlacementModel {
	return PlacementModel{
		Backends: []PlacementBackend{
			{ID: "primary", Hash: testPrimaryBackend, Role: "primary", FailureDomain: "rack-a"},
			{ID: "archive", Hash: testArchivalBackend, Role: "archival", Offsite: true, FailureDomain: "provider-b"},
			{ID: "cache", Hash: testCacheBackend, Role: "cache", FailureDomain: "rack-a"},
		},
		Policy: DurabilityPolicy{MinCopies: 2, MinDomains: 2, MinOffsite: 1},
	}
}

func placementPack(tier schema.PackTier) schema.PackRecord {
	return schema.PackRecord{
		Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
		PayloadSize: 80, HeaderSize: 20, BlobCount: 1,
		CreationTime: 123, CreationTimeKnown: true,
		Lifecycle: schema.PackPublished, Tier: tier,
		RetentionSource: schema.RetentionUnknown,
	}
}

func storePlacementPack(t *testing.T, store *memoryStore, id vaultic.ID, tier schema.PackTier) {
	t.Helper()
	store.set(t, schema.PackKey(schema.ID(id)), placementPack(tier))
}

func TestPlacementMigrationFromEveryPhase9Tier(t *testing.T) {
	model := testPlacementModel()
	for _, testCase := range []struct {
		name  string
		tier  schema.PackTier
		want  []uint64
		empty bool
	}{
		{"mirrored", schema.TierMirrored, []uint64{testPrimaryBackend, testArchivalBackend}, false},
		{"cold", schema.TierCold, []uint64{testArchivalBackend}, false},
		{"hot", schema.TierHot, []uint64{testPrimaryBackend}, false},
		{"single", schema.TierSingle, []uint64{testPrimaryBackend}, false},
		{"unknown", schema.TierUnknown, nil, true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			placements := placementsFromTier(placementPack(testCase.tier), model)
			if testCase.empty {
				if len(placements) != 0 {
					t.Fatalf("unknown tier invented placements: %#v", placements)
				}
				return
			}
			if len(placements) != len(testCase.want) {
				t.Fatalf("placements = %#v, want %d entries", placements, len(testCase.want))
			}
			for _, backend := range testCase.want {
				placement, ok := placements[backend]
				if !ok {
					t.Fatalf("missing backend %x in %#v", backend, placements)
				}
				if placement.State != schema.PlacementLive {
					t.Fatalf("migrated placement state = %v, want live", placement.State)
				}
			}
		})
	}
}

func TestRebuildPlacementRecordsWritesPLAndBP(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	cold := deterministicID(1)
	unknown := deterministicID(2)
	storePlacementPack(t, store, cold, schema.TierCold)
	storePlacementPack(t, store, unknown, schema.TierUnknown)

	changed, err := RebuildPlacementRecords(context.Background(), store, testPlacementModel(), true)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("dry-run changed = %d, want 1", changed)
	}
	if _, found, _ := store.Get(context.Background(), schema.PackPlacementKey(schema.ID(cold), testArchivalBackend)); found {
		t.Fatal("dry-run wrote a placement")
	}

	changed, err = RebuildPlacementRecords(context.Background(), store, testPlacementModel(), false)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("changed = %d, want 1", changed)
	}
	if _, found, err := store.Get(context.Background(), schema.PackPlacementKey(schema.ID(cold), testArchivalBackend)); err != nil || !found {
		t.Fatalf("placement not written: found=%v err=%v", found, err)
	}
	if _, found, err := store.Get(context.Background(), schema.BackendPackKey(testArchivalBackend, schema.ID(cold))); err != nil || !found {
		t.Fatalf("backend-pack record not written: found=%v err=%v", found, err)
	}
	if _, found, err := store.Get(context.Background(), schema.PackPlacementKey(schema.ID(unknown), testArchivalBackend)); err != nil || found {
		t.Fatalf("unknown tier invented a placement: found=%v err=%v", found, err)
	}
}

func TestBackendPackRebuildFromPlacementRecords(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	packID := deterministicID(3)
	store.set(t, schema.PackPlacementKey(schema.ID(packID), testPrimaryBackend), schema.PlacementRecord{
		State: schema.PlacementLive, PlacedAt: 123, PlacementTimeKnown: true,
		Bytes: 100, RetentionSource: schema.RetentionUnknown,
	})
	changed, err := RebuildBackendPackIndex(context.Background(), store, false)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("changed = %d, want 1", changed)
	}
	if _, found, err := store.Get(context.Background(), schema.BackendPackKey(testPrimaryBackend, schema.ID(packID))); err != nil || !found {
		t.Fatalf("backend-pack index was not rebuilt: found=%v err=%v", found, err)
	}
	stalePack := deterministicID(33)
	store.set(t, schema.BackendPackKey(testArchivalBackend, schema.ID(stalePack)), schema.BackendPackRecord{State: schema.PlacementLive})
	changed, err = RebuildBackendPackIndex(context.Background(), store, false)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("stale rebuild changed = %d, want 1", changed)
	}
	if _, found, err := store.Get(context.Background(), schema.BackendPackKey(testArchivalBackend, schema.ID(stalePack))); err != nil || found {
		t.Fatalf("stale backend-pack record survived: found=%v err=%v", found, err)
	}
}

func TestRebuildDerivedTierSummaryFromPlacements(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	packID := deterministicID(5)
	storePlacementPack(t, store, packID, schema.TierCold)
	store.set(t, schema.PackPlacementKey(schema.ID(packID), testPrimaryBackend), schema.PlacementRecord{
		State: schema.PlacementLive, PlacedAt: 123, PlacementTimeKnown: true,
		Bytes: 100, RetentionSource: schema.RetentionUnknown,
	})

	changed, err := RebuildDerivedTierSummary(context.Background(), store, testPlacementModel(), true)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("dry-run tier summaries changed = %d, want 1", changed)
	}
	value, found, err := store.Get(context.Background(), schema.PackKey(schema.ID(packID)))
	if err != nil || !found {
		t.Fatalf("pack missing: found=%v err=%v", found, err)
	}
	pack, err := schema.UnmarshalPackRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	if pack.Tier != schema.TierCold {
		t.Fatalf("dry-run rewrote tier to %s", pack.Tier)
	}

	changed, err = RebuildDerivedTierSummary(context.Background(), store, testPlacementModel(), false)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("tier summaries changed = %d, want 1", changed)
	}
	value, _, err = store.Get(context.Background(), schema.PackKey(schema.ID(packID)))
	if err != nil {
		t.Fatal(err)
	}
	pack, err = schema.UnmarshalPackRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	if pack.Tier != schema.TierHot {
		t.Fatalf("tier = %s, want hot", pack.Tier)
	}
}

func TestDurabilityCountsFailureDomainsNotBackends(t *testing.T) {
	model := PlacementModel{
		Backends: []PlacementBackend{
			{ID: "a", Hash: 1, Role: "primary", FailureDomain: "same-room"},
			{ID: "b", Hash: 2, Role: "cache", FailureDomain: "same-room"},
			{ID: "c", Hash: 3, Role: "archival", Offsite: true, FailureDomain: "cloud"},
		},
		Policy: DurabilityPolicy{MinCopies: 2, MinDomains: 2, MinOffsite: 1},
	}
	backends := map[uint64]PlacementBackend{}
	for _, backend := range model.Backends {
		backends[backend.Hash] = backend
	}
	live := func() schema.PlacementRecord {
		return schema.PlacementRecord{State: schema.PlacementLive, Bytes: 1, RetentionSource: schema.RetentionUnknown}
	}
	if durable(placementSet{1: live(), 2: live()}, backends, model.Policy) {
		t.Fatal("two live placements in one failure domain were counted as durable")
	}
	if durable(placementSet{1: live(), 3: {State: schema.PlacementPending, Bytes: 1, RetentionSource: schema.RetentionUnknown}}, backends, model.Policy) {
		t.Fatal("pending placement counted as live durability")
	}
	if !durable(placementSet{1: live(), 3: live()}, backends, model.Policy) {
		t.Fatal("independent primary+offsite live placements were not durable")
	}
}

func TestPlacementCheckReportsBPAndTierDrift(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	packID := deterministicID(4)
	storePlacementPack(t, store, packID, schema.TierCold)
	store.set(t, schema.PackPlacementKey(schema.ID(packID), testPrimaryBackend), schema.PlacementRecord{
		State: schema.PlacementLive, PlacedAt: 123, PlacementTimeKnown: true,
		Bytes: 100, RetentionSource: schema.RetentionUnknown,
	})
	result := CheckResult{}
	packs := map[vaultic.ID]schema.PackRecord{packID: placementPack(schema.TierCold)}
	if err := checkPlacementRecords(context.Background(), store, packs, testPlacementModel(), &result, 10); err != nil {
		t.Fatal(err)
	}
	if result.BackendPackMismatch != 1 {
		t.Fatalf("backend-pack mismatch = %d, want 1", result.BackendPackMismatch)
	}
	if result.DerivedTierMismatch != 1 {
		t.Fatalf("derived tier mismatch = %d, want 1", result.DerivedTierMismatch)
	}
	if result.PacksBelowDurability != 1 {
		t.Fatalf("below durability = %d, want 1", result.PacksBelowDurability)
	}
}
