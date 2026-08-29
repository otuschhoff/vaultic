package daemon

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func tierTestStore(t *testing.T, repositoryID string) (*SchemaStore, context.Context) {
	t.Helper()
	client, err := Ensure(context.Background(), Options{Socket: testSocket(t), RepositoryID: repositoryID, DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(context.Background()) })
	return NewSchemaStore(client), context.Background()
}

func readAggregate(t *testing.T, store *SchemaStore, ctx context.Context, key []byte) (schema.PackAggregate, bool) {
	t.Helper()
	value, found, err := store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		return schema.PackAggregate{}, false
	}
	aggregate, err := schema.UnmarshalPackAggregate(value)
	if err != nil {
		t.Fatal(err)
	}
	return aggregate, true
}

// TestPublishMaintainsTierAggregatesAtomically verifies that publishing packs
// updates the tier dimension in the same transaction as the pack record and
// the type dimension, and that only the tiers actually in use are created.
func TestPublishMaintainsTierAggregatesAtomically(t *testing.T) {
	store, ctx := tierTestStore(t, "phase9-tier-publish")
	coldPack, mirroredPack := daemonTestID(60), daemonTestID(61)
	coldBlob, treeBlob := daemonTestID(62), daemonTestID(63)

	for _, published := range []PublishedPack{
		{PackID: coldPack, Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 5, BlobCount: 1, Lifecycle: schema.PackExportPending, Tier: schema.TierCold},
			Blobs: map[schema.ID]schema.BlobRecord{coldBlob: {Locations: []schema.BlobLocation{{PackID: coldPack, Length: 5, Type: schema.BlobData}}}}},
		{PackID: mirroredPack, Record: schema.PackRecord{Type: schema.PackTree, PayloadSize: 7, BlobCount: 1, Lifecycle: schema.PackExportPending, Tier: schema.TierMirrored},
			Blobs: map[schema.ID]schema.BlobRecord{treeBlob: {Locations: []schema.BlobLocation{{PackID: mirroredPack, Length: 7, Type: schema.BlobTree}}}}},
	} {
		if err := store.PublishPack(ctx, published); err != nil {
			t.Fatal(err)
		}
	}

	cold, found := readAggregate(t, store, ctx, schema.TierAggregateKey(schema.TierCold))
	if !found || cold.PackCount != 1 || cold.PayloadSize != 5 {
		t.Fatalf("cold tier aggregate = %+v found=%t", cold, found)
	}
	mirrored, found := readAggregate(t, store, ctx, schema.TierAggregateKey(schema.TierMirrored))
	if !found || mirrored.PackCount != 1 || mirrored.PayloadSize != 7 {
		t.Fatalf("mirrored tier aggregate = %+v found=%t", mirrored, found)
	}
	// Tiers this repository does not use must not be materialized.
	for _, tier := range []schema.PackTier{schema.TierHot, schema.TierSingle, schema.TierUnknown} {
		if _, found := readAggregate(t, store, ctx, schema.TierAggregateKey(tier)); found {
			t.Fatalf("unused tier %v was materialized", tier)
		}
	}
	// The type dimension still totals both packs.
	all, found := readAggregate(t, store, ctx, schema.PackAggregateKey(schema.AggregateAll))
	if !found || all.PackCount != 2 || all.PayloadSize != 12 {
		t.Fatalf("all aggregate = %+v", all)
	}
}

// TestPackDeletionUpdatesTierAggregates checks that removing a pack decrements
// its tier, and that deletion still succeeds on a repository whose tier
// dimension was never built.
func TestPackDeletionUpdatesTierAggregates(t *testing.T) {
	store, ctx := tierTestStore(t, "phase9-tier-delete")
	packID, blobID := daemonTestID(70), daemonTestID(71)
	published := PublishedPack{
		PackID: packID,
		Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 9, BlobCount: 1, Lifecycle: schema.PackExportPending, Tier: schema.TierCold},
		Blobs:  map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packID, Length: 9, Type: schema.BlobData}}}},
	}
	if err := store.PublishPack(ctx, published); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackPublished(ctx, packID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackDeletePending(ctx, packID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackDeleted(ctx, packID, []schema.ID{blobID}); err != nil {
		t.Fatal(err)
	}
	cold, found := readAggregate(t, store, ctx, schema.TierAggregateKey(schema.TierCold))
	if !found || cold.PackCount != 0 || cold.PayloadSize != 0 {
		t.Fatalf("cold tier aggregate after deletion = %+v found=%t", cold, found)
	}
}

// TestPackDeletionToleratesUnbuiltTierDimension is the upgrade path: a
// repository written before the tier dimension existed has no a:tier: records.
// Deleting one of its packs must still succeed, because a missing accelerator
// may never block a destructive operation from completing correctly.
func TestPackDeletionToleratesUnbuiltTierDimension(t *testing.T) {
	client, err := Ensure(context.Background(), Options{Socket: testSocket(t), RepositoryID: "phase9-tier-unbuilt-delete", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store, ctx := NewSchemaStore(client), context.Background()

	packID, blobID := daemonTestID(80), daemonTestID(81)
	published := PublishedPack{
		PackID: packID,
		Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 4, BlobCount: 1, Lifecycle: schema.PackExportPending, Tier: schema.TierCold},
		Blobs:  map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packID, Length: 4, Type: schema.BlobData}}}},
	}
	if err := store.PublishPack(ctx, published); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackPublished(ctx, packID); err != nil {
		t.Fatal(err)
	}

	// Reproduce a catalog written before the tier dimension existed. The
	// SchemaStore deliberately refuses to delete aggregate keys, so this goes
	// through the raw client to forge the older on-disk state.
	if _, err := client.WriteBatch(ctx, nil, [][]byte{schema.TierAggregateKey(schema.TierCold)}, true, ""); err != nil {
		t.Fatal(err)
	}
	if _, found := readAggregate(t, store, ctx, schema.TierAggregateKey(schema.TierCold)); found {
		t.Fatal("tier aggregate was not removed by the test setup")
	}

	if err := store.MarkPackDeletePending(ctx, packID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackDeleted(ctx, packID, []schema.ID{blobID}); err != nil {
		t.Fatalf("deletion failed on a repository without tier aggregates: %v", err)
	}
	// The type dimension must still have been decremented correctly.
	all, found := readAggregate(t, store, ctx, schema.PackAggregateKey(schema.AggregateAll))
	if !found || all.PackCount != 0 {
		t.Fatalf("all aggregate after deletion = %+v found=%t", all, found)
	}
}

// TestPackTierTransitionMovesAggregates covers an imported pack, which is
// tier-unknown, later being republished by vaultic with a known tier. The pack
// must move between tier records rather than being counted in both.
func TestPackTierTransitionMovesAggregates(t *testing.T) {
	store, ctx := tierTestStore(t, "phase9-tier-transition")
	packID, blobID, sourceIndex := daemonTestID(100), daemonTestID(101), daemonTestID(102)

	imported := LegacyPackImport{
		PackID:      packID,
		SourceIndex: sourceIndex,
		Record:      schema.PackRecord{Type: schema.PackData, PayloadSize: 6, BlobCount: 1, Lifecycle: schema.PackImported},
		Blobs:       map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packID, Length: 6, Type: schema.BlobData}}}},
	}
	if err := store.ImportLegacyPack(ctx, imported); err != nil {
		t.Fatal(err)
	}
	unknown, found := readAggregate(t, store, ctx, schema.TierAggregateKey(schema.TierUnknown))
	if !found || unknown.PackCount != 1 {
		t.Fatalf("unknown tier aggregate = %+v found=%t", unknown, found)
	}

	// The same pack is published by vaultic, this time with its routing known.
	routed := PublishedPack{
		PackID: packID,
		Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 6, BlobCount: 1, Lifecycle: schema.PackExportPending, Tier: schema.TierCold},
		Blobs:  imported.Blobs,
	}
	if err := store.PublishPack(ctx, routed); err != nil {
		t.Fatal(err)
	}

	unknown, _ = readAggregate(t, store, ctx, schema.TierAggregateKey(schema.TierUnknown))
	if unknown.PackCount != 0 || unknown.PayloadSize != 0 {
		t.Fatalf("pack was not removed from the unknown tier: %+v", unknown)
	}
	cold, found := readAggregate(t, store, ctx, schema.TierAggregateKey(schema.TierCold))
	if !found || cold.PackCount != 1 {
		t.Fatalf("pack did not move to the cold tier: %+v found=%t", cold, found)
	}
	// The pack must be counted exactly once across the tier dimension.
	var total uint64
	for _, tier := range schema.TierAggregateKinds() {
		aggregate, _ := readAggregate(t, store, ctx, schema.TierAggregateKey(tier))
		total += aggregate.PackCount
	}
	if total != 1 {
		t.Fatalf("tier dimension counts the pack %d times", total)
	}
}

// TestTierTransitionToleratesUnbuiltTierDimension is the same transition on a
// repository that predates the tier dimension: the record the pack is leaving
// does not exist, and the publish must still succeed rather than failing on an
// aggregate underflow.
func TestTierTransitionToleratesUnbuiltTierDimension(t *testing.T) {
	client, err := Ensure(context.Background(), Options{Socket: testSocket(t), RepositoryID: "phase9-tier-transition-unbuilt", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store, ctx := NewSchemaStore(client), context.Background()

	packID, blobID, sourceIndex := daemonTestID(110), daemonTestID(111), daemonTestID(112)
	imported := LegacyPackImport{
		PackID:      packID,
		SourceIndex: sourceIndex,
		Record:      schema.PackRecord{Type: schema.PackData, PayloadSize: 6, BlobCount: 1, Lifecycle: schema.PackImported},
		Blobs:       map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packID, Length: 6, Type: schema.BlobData}}}},
	}
	if err := store.ImportLegacyPack(ctx, imported); err != nil {
		t.Fatal(err)
	}
	// Forge a catalog written before the tier dimension existed.
	if _, err := client.WriteBatch(ctx, nil, [][]byte{schema.TierAggregateKey(schema.TierUnknown)}, true, ""); err != nil {
		t.Fatal(err)
	}

	routed := PublishedPack{
		PackID: packID,
		Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 6, BlobCount: 1, Lifecycle: schema.PackExportPending, Tier: schema.TierCold},
		Blobs:  imported.Blobs,
	}
	if err := store.PublishPack(ctx, routed); err != nil {
		t.Fatalf("publish failed on a repository without tier aggregates: %v", err)
	}
	cold, found := readAggregate(t, store, ctx, schema.TierAggregateKey(schema.TierCold))
	if !found || cold.PackCount != 1 {
		t.Fatalf("cold tier aggregate = %+v found=%t", cold, found)
	}
}

// TestUpdatePackUsageIsAtomicAcrossPacks verifies that a usage batch touching
// several packs folds every delta into the aggregates. Applying the deltas one
// pack at a time inside a single transaction would lose all but the last,
// because the reads would not observe the pending writes.
func TestUpdatePackUsageIsAtomicAcrossPacks(t *testing.T) {
	store, ctx := tierTestStore(t, "phase9-usage-batch")
	packA, packB := daemonTestID(90), daemonTestID(91)
	blobA, blobB := daemonTestID(92), daemonTestID(93)
	for _, published := range []PublishedPack{
		{PackID: packA, Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 10, BlobCount: 1, Lifecycle: schema.PackExportPending, Tier: schema.TierCold},
			Blobs: map[schema.ID]schema.BlobRecord{blobA: {Locations: []schema.BlobLocation{{PackID: packA, Length: 10, Type: schema.BlobData}}}}},
		{PackID: packB, Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 20, BlobCount: 1, Lifecycle: schema.PackExportPending, Tier: schema.TierCold},
			Blobs: map[schema.ID]schema.BlobRecord{blobB: {Locations: []schema.BlobLocation{{PackID: packB, Length: 20, Type: schema.BlobData}}}}},
	} {
		if err := store.PublishPack(ctx, published); err != nil {
			t.Fatal(err)
		}
	}

	applied, err := store.UpdatePackUsage(ctx, map[schema.ID]PackUsage{
		packA: {Used: 4, Unused: 6},
		packB: {Used: 20, Unused: 0},
	})
	if err != nil || applied != 2 {
		t.Fatalf("applied=%d err=%v", applied, err)
	}

	cold, found := readAggregate(t, store, ctx, schema.TierAggregateKey(schema.TierCold))
	if !found {
		t.Fatal("cold tier aggregate missing")
	}
	// Both packs must be reflected, not just the last one processed.
	if cold.AccountedPackCount != 2 || cold.UsedPayloadBytes != 24 || cold.UnusedPayloadBytes != 6 {
		t.Fatalf("cold tier usage = %+v", cold)
	}
	all, _ := readAggregate(t, store, ctx, schema.PackAggregateKey(schema.AggregateAll))
	if all.AccountedPackCount != 2 || all.UsedPayloadBytes != 24 || all.UnusedPayloadBytes != 6 {
		t.Fatalf("all aggregate usage = %+v", all)
	}

	// Re-applying the same split changes nothing and writes nothing.
	applied, err = store.UpdatePackUsage(ctx, map[schema.ID]PackUsage{packA: {Used: 4, Unused: 6}})
	if err != nil || applied != 0 {
		t.Fatalf("idempotent update applied=%d err=%v", applied, err)
	}

	// A split that disagrees with the payload size is rejected rather than
	// recorded, so usage never contradicts the catalog.
	applied, err = store.UpdatePackUsage(ctx, map[schema.ID]PackUsage{packA: {Used: 999, Unused: 0}})
	if err != nil || applied != 0 {
		t.Fatalf("inconsistent split applied=%d err=%v", applied, err)
	}
	after, _ := readAggregate(t, store, ctx, schema.TierAggregateKey(schema.TierCold))
	if after.UsedPayloadBytes != 24 {
		t.Fatalf("rejected split mutated the aggregate: %+v", after)
	}
}
