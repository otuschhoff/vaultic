package repository

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// recomputeUsage is an independent, deliberately naive recomputation of the
// used/unused split straight from the blob index. Pack usage must always agree
// with it; that is what "rebuildable from the blob index" means.
func recomputeUsage(
	packMemberBytes map[vaultic.ID]map[vaultic.ID]uint64,
	unreachable map[vaultic.ID]struct{},
) map[vaultic.ID][2]uint64 {
	result := make(map[vaultic.ID][2]uint64)
	for packID, members := range packMemberBytes {
		var used, unused uint64
		for blobID, length := range members {
			if _, gone := unreachable[blobID]; gone {
				unused += length
				continue
			}
			used += length
		}
		result[packID] = [2]uint64{used, unused}
	}
	return result
}

func TestComputePackUsageMatchesFullRecomputation(t *testing.T) {
	packLive, packMixed, packDead := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	live, deadA, deadB, alsoLive := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()

	packs := map[vaultic.ID]schema.PackRecord{
		packLive:  {Type: schema.PackData, Lifecycle: schema.PackPublished, PayloadSize: 10, BlobCount: 1},
		packMixed: {Type: schema.PackData, Lifecycle: schema.PackPublished, PayloadSize: 30, BlobCount: 2},
		packDead:  {Type: schema.PackData, Lifecycle: schema.PackPublished, PayloadSize: 40, BlobCount: 1},
	}
	packMemberBytes := map[vaultic.ID]map[vaultic.ID]uint64{
		packLive:  {live: 10},
		packMixed: {deadA: 20, alsoLive: 10},
		packDead:  {deadB: 40},
	}
	unreachable := map[vaultic.ID]struct{}{deadA: {}, deadB: {}}

	updates, inconsistent := computePackUsage(packs, packMemberBytes, unreachable)
	if inconsistent != 0 {
		t.Fatalf("consistent catalog reported %d unaccountable packs", inconsistent)
	}
	// Every pack gets an accounting entry, including the fully live one:
	// leaving it unaccounted would be indistinguishable from wholly unused.
	if len(updates) != 3 {
		t.Fatalf("usage updates = %d, want 3", len(updates))
	}
	want := recomputeUsage(packMemberBytes, unreachable)
	for packID, expected := range want {
		got := updates[schema.ID(packID)]
		if got.Used != expected[0] || got.Unused != expected[1] {
			t.Fatalf("pack %s usage = %+v, want used=%d unused=%d", packID.Str(), got, expected[0], expected[1])
		}
		if got.Used+got.Unused != packs[packID].PayloadSize {
			t.Fatalf("pack %s split does not sum to its payload size", packID.Str())
		}
	}
	if updates[schema.ID(packLive)].Unused != 0 {
		t.Fatal("fully live pack reported unused bytes")
	}
	if updates[schema.ID(packDead)].Used != 0 {
		t.Fatal("wholly unreachable pack reported used bytes")
	}
}

// TestComputePackUsageLeavesInconsistentPacksUnknown checks the conservative
// path: when the blob index disagrees with the catalog payload size, no split
// is invented.
func TestComputePackUsageLeavesInconsistentPacksUnknown(t *testing.T) {
	packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID()
	packs := map[vaultic.ID]schema.PackRecord{
		packID: {Type: schema.PackData, Lifecycle: schema.PackPublished, PayloadSize: 99, BlobCount: 1},
	}
	packMemberBytes := map[vaultic.ID]map[vaultic.ID]uint64{packID: {blobID: 10}}

	updates, inconsistent := computePackUsage(packs, packMemberBytes, nil)
	if len(updates) != 0 || inconsistent != 1 {
		t.Fatalf("updates=%d inconsistent=%d, want 0/1", len(updates), inconsistent)
	}
}

// TestComputePackUsageSkipsUnchangedAndDeletedPacks keeps a steady-state GC
// run from rewriting every pack record it scanned.
func TestComputePackUsageSkipsUnchangedAndDeletedPacks(t *testing.T) {
	unchanged, deleted := vaultic.NewRandomID(), vaultic.NewRandomID()
	live, gone := vaultic.NewRandomID(), vaultic.NewRandomID()
	packs := map[vaultic.ID]schema.PackRecord{
		unchanged: {
			Type: schema.PackData, Lifecycle: schema.PackPublished, PayloadSize: 10, BlobCount: 1,
			UsageKnown: true, UsedPayloadBytes: 10, UnusedPayloadBytes: 0,
		},
		deleted: {Type: schema.PackData, Lifecycle: schema.PackDeleted, PayloadSize: 20, BlobCount: 1},
	}
	packMemberBytes := map[vaultic.ID]map[vaultic.ID]uint64{
		unchanged: {live: 10},
		deleted:   {gone: 20},
	}

	updates, inconsistent := computePackUsage(packs, packMemberBytes, map[vaultic.ID]struct{}{gone: {}})
	if len(updates) != 0 || inconsistent != 0 {
		t.Fatalf("updates=%d inconsistent=%d, want 0/0", len(updates), inconsistent)
	}
}

// TestUsageAccountingFollowsReachabilityChange simulates a forget: a blob that
// was reachable becomes unreachable, and the recorded split must follow.
func TestUsageAccountingFollowsReachabilityChange(t *testing.T) {
	store := newFakeGCStore()
	packID := vaultic.NewRandomID()
	keep, drop := vaultic.NewRandomID(), vaultic.NewRandomID()
	record := schema.PackRecord{Type: schema.PackData, Lifecycle: schema.PackPublished, PayloadSize: 30, BlobCount: 2}
	store.set(t, schema.PackKey(schema.ID(packID)), record)
	store.set(t, schema.BlobKey(schema.ID(keep)), schema.BlobRecord{
		Locations: []schema.BlobLocation{{PackID: schema.ID(packID), Offset: 0, Length: 10, Type: schema.BlobData}},
	})
	store.set(t, schema.BlobKey(schema.ID(drop)), schema.BlobRecord{
		Locations: []schema.BlobLocation{{PackID: schema.ID(packID), Offset: 10, Length: 20, Type: schema.BlobData}},
	})

	_, _, packMemberBytes, err := scanBlobCatalog(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	packs := map[vaultic.ID]schema.PackRecord{packID: record}

	// Everything reachable.
	updates, _ := computePackUsage(packs, packMemberBytes, nil)
	if _, err := store.UpdatePackUsage(context.Background(), updates); err != nil {
		t.Fatal(err)
	}
	stored := readPack(t, store, packID)
	if !stored.UsageKnown || stored.UsedPayloadBytes != 30 || stored.UnusedPayloadBytes != 0 {
		t.Fatalf("initial usage = %+v", stored)
	}

	// One blob is forgotten.
	packs[packID] = stored
	updates, _ = computePackUsage(packs, packMemberBytes, map[vaultic.ID]struct{}{drop: {}})
	applied, err := store.UpdatePackUsage(context.Background(), updates)
	if err != nil || applied != 1 {
		t.Fatalf("applied=%d err=%v", applied, err)
	}
	stored = readPack(t, store, packID)
	if stored.UsedPayloadBytes != 10 || stored.UnusedPayloadBytes != 20 {
		t.Fatalf("usage after forget = %+v", stored)
	}
	if stored.UsedPayloadBytes+stored.UnusedPayloadBytes != stored.PayloadSize {
		t.Fatal("usage split does not sum to the payload size")
	}

	// Recomputing from the blob index must reproduce the same answer.
	want := recomputeUsage(packMemberBytes, map[vaultic.ID]struct{}{drop: {}})[packID]
	if stored.UsedPayloadBytes != want[0] || stored.UnusedPayloadBytes != want[1] {
		t.Fatalf("stored usage disagrees with a full recomputation: %+v vs %v", stored, want)
	}
}

func readPack(t *testing.T, store *fakeGCStore, packID vaultic.ID) schema.PackRecord {
	t.Helper()
	value, found, err := store.Get(context.Background(), schema.PackKey(schema.ID(packID)))
	if err != nil || !found {
		t.Fatalf("pack %s missing", packID.Str())
	}
	record, err := schema.UnmarshalPackRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

// TestScanBlobCatalogReportsMemberBytesOnce guards the deduplication that the
// usage split depends on: a blob listed twice for the same pack must be
// counted once, or a repacked pack would appear to hold more payload than it
// does.
func TestScanBlobCatalogReportsMemberBytesOnce(t *testing.T) {
	store := newFakeGCStore()
	packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID()
	store.set(t, schema.BlobKey(schema.ID(blobID)), schema.BlobRecord{Locations: []schema.BlobLocation{
		{PackID: schema.ID(packID), Offset: 0, Length: 10, Type: schema.BlobData},
		{PackID: schema.ID(packID), Offset: 0, Length: 10, Type: schema.BlobData},
	}})

	_, packMembers, packMemberBytes, err := scanBlobCatalog(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if len(packMembers[packID]) != 1 {
		t.Fatalf("duplicate membership = %v", packMembers[packID])
	}
	if packMemberBytes[packID][blobID] != 10 {
		t.Fatalf("member bytes = %d, want 10", packMemberBytes[packID][blobID])
	}
}
