package repository

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type fakeGCStore struct {
	values map[string][]byte
	events []daemon.PackEvent
}

func newFakeGCStore() *fakeGCStore { return &fakeGCStore{values: make(map[string][]byte)} }

func (store *fakeGCStore) set(t *testing.T, key []byte, record interface{ MarshalBinary() ([]byte, error) }) {
	t.Helper()
	value, err := record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	store.values[string(key)] = value
}

func (store *fakeGCStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	value, found := store.values[string(key)]
	return append([]byte(nil), value...), found, nil
}

func (store *fakeGCStore) ScanPrefix(_ context.Context, prefix, after []byte, limit uint32) ([]daemon.KeyValue, bool, error) {
	var keys []string
	for key := range store.values {
		if len(key) >= len(prefix) && key[:len(prefix)] == string(prefix) && (len(after) == 0 || key > string(after)) {
			keys = append(keys, key)
		}
	}
	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	done := len(keys) <= int(limit)
	if !done {
		keys = keys[:limit]
	}
	entries := make([]daemon.KeyValue, len(keys))
	for index, key := range keys {
		entries[index] = daemon.KeyValue{Key: []byte(key), Value: append([]byte(nil), store.values[key]...)}
	}
	return entries, done, nil
}

func (store *fakeGCStore) WriteMutableBatch(_ context.Context, puts []daemon.Mutation, _ [][]byte, _ bool) error {
	for _, put := range puts {
		store.values[string(put.Key)] = append([]byte(nil), put.Value...)
	}
	return nil
}

func (store *fakeGCStore) MarkPackDeletePending(context.Context, schema.ID) error { return nil }
func (store *fakeGCStore) MarkPackDeleted(context.Context, schema.ID, []schema.ID) error {
	return nil
}

// UpdatePackUsage applies the same invariants as the real store: a split that
// disagrees with the recorded payload size is skipped rather than written.
func (store *fakeGCStore) UpdatePackUsage(_ context.Context, usage map[schema.ID]daemon.PackUsage) (uint64, error) {
	var applied uint64
	for id, split := range usage {
		key := schema.PackKey(id)
		value, found := store.values[string(key)]
		if !found {
			continue
		}
		record, err := schema.UnmarshalPackRecord(value)
		if err != nil {
			return applied, err
		}
		if split.Used > record.PayloadSize || split.Used+split.Unused != record.PayloadSize {
			continue
		}
		record.UsageKnown, record.UsedPayloadBytes, record.UnusedPayloadBytes = true, split.Used, split.Unused
		encoded, err := record.MarshalBinary()
		if err != nil {
			return applied, err
		}
		store.values[string(key)] = encoded
		applied++
	}
	return applied, nil
}

// RecordPackEvents captures advisory history events so tests can assert on
// lineage and failure reporting.
func (store *fakeGCStore) RecordPackEvents(_ context.Context, events []daemon.PackEvent) error {
	store.events = append(store.events, events...)
	return nil
}

var _ GCStore = (*fakeGCStore)(nil)

func TestScanReferencedDataBlobsCollectsAnyReferenceState(t *testing.T) {
	store := newFakeGCStore()
	current, unresolved := vaultic.NewRandomID(), vaultic.NewRandomID()
	store.set(t, schema.ReverseInodeKey(schema.ID(current), 1, 2), schema.ReverseInodeRecord{LatestRevision: 1, State: schema.ReferenceCurrent})
	store.set(t, schema.ReverseManifestKey(schema.ID(unresolved), schema.ID(vaultic.NewRandomID())), schema.ReverseManifestRecord{State: schema.ReferenceUnresolved})
	referenced, err := scanReferencedDataBlobs(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := referenced[current]; !found {
		t.Fatal("current reference not scanned")
	}
	if _, found := referenced[unresolved]; !found {
		t.Fatal("unresolved reference not scanned")
	}
	if len(referenced) != 2 {
		t.Fatalf("referenced = %#v", referenced)
	}
}

func TestScanBlobCatalogBuildsPackMembership(t *testing.T) {
	store := newFakeGCStore()
	packA, packB, blobShared, blobOnlyA := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	store.set(t, schema.BlobKey(schema.ID(blobShared)), schema.BlobRecord{Locations: []schema.BlobLocation{
		{PackID: schema.ID(packA), Length: 5, Type: schema.BlobData},
		{PackID: schema.ID(packB), Offset: 5, Length: 5, Type: schema.BlobData},
	}})
	store.set(t, schema.BlobKey(schema.ID(blobOnlyA)), schema.BlobRecord{Locations: []schema.BlobLocation{
		{PackID: schema.ID(packA), Offset: 10, Length: 3, Type: schema.BlobTree},
	}})
	blobTypes, packMembers, _, err := scanBlobCatalog(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if blobTypes[blobShared] != schema.BlobData || blobTypes[blobOnlyA] != schema.BlobTree {
		t.Fatalf("blob types = %#v", blobTypes)
	}
	if len(packMembers[packA]) != 2 || len(packMembers[packB]) != 1 {
		t.Fatalf("pack members = %#v", packMembers)
	}
}

func TestClassifyPacksWholeMixedAndSkip(t *testing.T) {
	packWhole, packMixed, packLive := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	deadA, deadB, live := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	packs := map[vaultic.ID]schema.PackRecord{
		packWhole: {Type: schema.PackData, Lifecycle: schema.PackPublished, PhysicalSize: 10},
		packMixed: {Type: schema.PackData, Lifecycle: schema.PackPublished, PhysicalSize: 20},
		packLive:  {Type: schema.PackData, Lifecycle: schema.PackPublished, PhysicalSize: 30},
	}
	packMembers := map[vaultic.ID][]vaultic.ID{
		packWhole: {deadA},
		packMixed: {deadB, live},
		packLive:  {live},
	}
	blobTypes := map[vaultic.ID]schema.BlobType{deadA: schema.BlobData, deadB: schema.BlobData, live: schema.BlobData}
	unreachable := map[vaultic.ID]struct{}{deadA: {}, deadB: {}}

	result, err := classifyPacks(packs, packMembers, blobTypes, unreachable, nil, time.Now(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := result.wholePacks[packWhole]; !found {
		t.Fatalf("wholly unreachable pack was not classified: %#v", result.wholePacks)
	}
	if _, found := result.mixedPacks[packMixed]; !found {
		t.Fatalf("mixed pack was not classified: %#v", result.mixedPacks)
	}
	if !result.mixedPacks[packMixed].Has(vaultic.BlobHandle{ID: live, Type: vaultic.DataBlob}) {
		t.Fatal("mixed pack did not keep its live blob")
	}
	if _, found := result.wholePacks[packLive]; found {
		t.Fatal("fully live pack was incorrectly classified")
	}
	if _, found := result.mixedPacks[packLive]; found {
		t.Fatal("fully live pack was incorrectly classified as mixed")
	}
	if result.wholePackCandidates != 1 || result.mixedPackCandidates != 1 || result.packsScanned != 3 {
		t.Fatalf("classification stats = %#v", result)
	}
	if len(result.gcPuts) != 2 {
		t.Fatalf("gc bookkeeping puts = %d", len(result.gcPuts))
	}
}

func TestClassifyPacksHonorsMinCandidateAge(t *testing.T) {
	packID, dead := vaultic.NewRandomID(), vaultic.NewRandomID()
	packs := map[vaultic.ID]schema.PackRecord{packID: {Type: schema.PackData, Lifecycle: schema.PackPublished}}
	packMembers := map[vaultic.ID][]vaultic.ID{packID: {dead}}
	blobTypes := map[vaultic.ID]schema.BlobType{dead: schema.BlobData}
	unreachable := map[vaultic.ID]struct{}{dead: {}}

	now := time.Now()
	result, err := classifyPacks(packs, packMembers, blobTypes, unreachable, nil, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.wholePacks) != 0 || result.pendingAge != 1 {
		t.Fatalf("freshly discovered candidate should wait out the age gate: %#v", result)
	}

	key := string(schemaGCPackKeyForTest(packID))
	existing := map[string]int64{key: now.Add(-2 * time.Hour).UnixNano()}
	aged, err := classifyPacks(packs, packMembers, blobTypes, unreachable, existing, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := aged.wholePacks[packID]; !found {
		t.Fatalf("aged candidate should be swept: %#v", aged)
	}
}

// TestClassifyPacksHonorsPriorDiscoverOnlyBlobTimestamp proves that a
// blob-level timestamp written by an earlier --discover-only run primes the
// pack-level age gate once a full run first classifies the pack, rather than
// restarting the age clock from that later run's own time.
func TestClassifyPacksHonorsPriorDiscoverOnlyBlobTimestamp(t *testing.T) {
	packID, dead := vaultic.NewRandomID(), vaultic.NewRandomID()
	packs := map[vaultic.ID]schema.PackRecord{packID: {Type: schema.PackData, Lifecycle: schema.PackPublished}}
	packMembers := map[vaultic.ID][]vaultic.ID{packID: {dead}}
	blobTypes := map[vaultic.ID]schema.BlobType{dead: schema.BlobData}
	unreachable := map[vaultic.ID]struct{}{dead: {}}

	now := time.Now()
	blobKey := string(schema.GarbageCollectionKey(schema.GCBlob, schema.ID(dead)))
	existing := map[string]int64{blobKey: now.Add(-2 * time.Hour).UnixNano()}

	result, err := classifyPacks(packs, packMembers, blobTypes, unreachable, existing, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := result.wholePacks[packID]; !found {
		t.Fatalf("pack primed by a prior discover-only blob timestamp should be swept: %#v", result)
	}
}

func schemaGCPackKeyForTest(packID vaultic.ID) []byte {
	return schema.GarbageCollectionKey(schema.GCPack, schema.ID(packID))
}

func TestGCBlobSetSatisfiesFindAndRepackInterfaces(t *testing.T) {
	set := newGCBlobSet()
	handle := vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}
	if set.Has(handle) {
		t.Fatal("empty set reports handle present")
	}
	set.Insert(handle)
	if !set.Has(handle) || set.Len() != 1 {
		t.Fatalf("insert not reflected: has=%t len=%d", set.Has(handle), set.Len())
	}
	set.Delete(handle)
	if set.Has(handle) || set.Len() != 0 {
		t.Fatalf("delete not reflected: has=%t len=%d", set.Has(handle), set.Len())
	}
}

func testGCDaemonPath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "vaulticdb", "target", "debug", "vaulticdb"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	return path
}

// gcTestSocket returns a short-path Unix socket location. Go's per-test
// temporary directory can exceed the platform socket path length limit,
// which manifests as an opaque daemon-readiness timeout.
func gcTestSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "vd-gc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "d.sock")
}

// TestPlanGCRepacksMixedPackAndDeletesWhollyUnreachablePack forces a live and
// a dead blob into the same physical pack (packerCount=1), gives the live
// blob a manufactured reverse edge, and confirms GC repacks the live blob
// into a replacement pack and removes the original pack and the dead blob's
// catalog entry entirely.
func TestPlanGCRepacksMixedPackAndDeletesWhollyUnreachablePack(t *testing.T) {
	daemonPath := testGCDaemonPath(t)
	repo, _, _ := TestRepositoryWithVersion(t, 0)
	repo.packerCount = 1

	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: gcTestSocket(t), RepositoryID: "gc-repack-test",
		DaemonPath: daemonPath, DataDir: t.TempDir(), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	repo.SetEngine(metadataindex.NewDaemonEngine(client))
	store := daemon.NewSchemaStore(client)

	var liveID, deadID vaultic.ID
	if err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var saveErr error
		liveID, _, _, saveErr = uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("live content"), vaultic.ID{}, true)
		if saveErr != nil {
			return saveErr
		}
		deadID, _, _, saveErr = uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("dead content"), vaultic.ID{}, true)
		return saveErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	// A manufactured reverse edge is enough to mark the live blob referenced
	// without needing a real snapshot/tree for this focused test.
	edgeValue, err := (schema.ReverseInodeRecord{LatestRevision: 1, State: schema.ReferenceCurrent}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteMutableBatch(context.Background(), []daemon.Mutation{
		{Key: schema.ReverseInodeKey(schema.ID(liveID), 1, 1), Value: edgeValue},
	}, nil, true); err != nil {
		t.Fatal(err)
	}

	packsBefore := vaultic.NewIDSet()
	if err := repo.List(context.Background(), vaultic.PackFile, func(id vaultic.ID, _ int64) error {
		packsBefore.Insert(id)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(packsBefore) != 1 {
		t.Fatalf("expected exactly one shared pack, got %d", len(packsBefore))
	}

	printer := vaultic.NewNoopPrinter()
	plan, err := PlanGC(context.Background(), GCOptions{}, repo, printer)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.mixedPacks) != 1 {
		t.Fatalf("expected exactly one mixed pack, got %#v", plan.mixedPacks)
	}
	if err := plan.Execute(context.Background(), printer); err != nil {
		t.Fatal(err)
	}
	if plan.Stats.PacksRepacked != 1 {
		t.Fatalf("stats = %#v", plan.Stats)
	}

	packsAfter := vaultic.NewIDSet()
	if err := repo.List(context.Background(), vaultic.PackFile, func(id vaultic.ID, _ int64) error {
		packsAfter.Insert(id)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for id := range packsBefore {
		if packsAfter.Has(id) {
			t.Fatalf("original mixed pack %s was not removed", id.Str())
		}
	}
	if len(packsAfter) != 1 {
		t.Fatalf("expected exactly one replacement pack, got %d", len(packsAfter))
	}

	if err := repo.LoadIndex(context.Background(), vaultic.NoopTerminalCounterFactory); err != nil {
		t.Fatal(err)
	}
	buf, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: liveID, Type: vaultic.DataBlob}, nil)
	if err != nil || string(buf) != "live content" {
		t.Fatalf("live blob after repack = %q, %v", buf, err)
	}
	if _, found, err := store.Get(context.Background(), schema.BlobKey(schema.ID(deadID))); err != nil || found {
		t.Fatalf("dead blob catalog entry survived: found=%t err=%v", found, err)
	}

	var replacementPack vaultic.ID
	for id := range packsAfter {
		replacementPack = id
	}
	packValue, found, err := store.Get(context.Background(), schema.PackKey(schema.ID(replacementPack)))
	if err != nil || !found {
		t.Fatalf("replacement pack record: found=%t err=%v", found, err)
	}
	packRecord, err := schema.UnmarshalPackRecord(packValue)
	if err != nil {
		t.Fatal(err)
	}
	if packRecord.BlobCount != 1 || packRecord.PayloadSize == 0 || packRecord.PayloadSize+packRecord.HeaderSize != packRecord.PhysicalSize {
		t.Fatalf("replacement pack record = %#v", packRecord)
	}
}

// TestPlanGCRepackFailureLeavesOriginalPackUntouched proves that a repack
// failure (here, the live blob's only physical copy has vanished from the
// backend) surfaces as an error and never marks the original mixed pack
// delete-pending, since its blobs were never actually relocated.
func TestPlanGCRepackFailureLeavesOriginalPackUntouched(t *testing.T) {
	daemonPath := testGCDaemonPath(t)
	repo, _, _ := TestRepositoryWithVersion(t, 0)
	repo.packerCount = 1

	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: gcTestSocket(t), RepositoryID: "gc-repack-failure-test",
		DaemonPath: daemonPath, DataDir: t.TempDir(), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	repo.SetEngine(metadataindex.NewDaemonEngine(client))
	store := daemon.NewSchemaStore(client)

	var liveID vaultic.ID
	if err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var saveErr error
		liveID, _, _, saveErr = uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("live content"), vaultic.ID{}, true)
		if saveErr != nil {
			return saveErr
		}
		_, _, _, saveErr = uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("dead content"), vaultic.ID{}, true)
		return saveErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	edgeValue, err := (schema.ReverseInodeRecord{LatestRevision: 1, State: schema.ReferenceCurrent}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteMutableBatch(context.Background(), []daemon.Mutation{
		{Key: schema.ReverseInodeKey(schema.ID(liveID), 1, 1), Value: edgeValue},
	}, nil, true); err != nil {
		t.Fatal(err)
	}

	printer := vaultic.NewNoopPrinter()
	plan, err := PlanGC(context.Background(), GCOptions{}, repo, printer)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.mixedPacks) != 1 {
		t.Fatalf("expected exactly one mixed pack, got %#v", plan.mixedPacks)
	}
	var mixedPack vaultic.ID
	for packID := range plan.mixedPacks {
		mixedPack = packID
	}

	// Corrupt the shared pack out-of-band so CopyBlobs cannot read the live
	// blob it needs to relocate.
	if err := (&internalRepository{repo}).RemoveUnpacked(context.Background(), vaultic.PackFile, mixedPack); err != nil {
		t.Fatal(err)
	}

	if err := plan.Execute(context.Background(), printer); err == nil {
		t.Fatal("repack of a pack with a missing backend object unexpectedly succeeded")
	}
	if plan.Stats.PacksRepacked != 0 {
		t.Fatalf("stats = %#v, want no packs repacked", plan.Stats)
	}
	value, found, err := store.Get(context.Background(), schema.PackKey(schema.ID(mixedPack)))
	if err != nil || !found {
		t.Fatalf("original pack catalog record missing after failed repack: found=%t err=%v", found, err)
	}
	record, err := schema.UnmarshalPackRecord(value)
	if err != nil || record.Lifecycle != schema.PackPublished {
		t.Fatalf("original pack record = %#v, err=%v, want unchanged published lifecycle", record, err)
	}
}

// TestPlanGCDeletesWhollyUnreachablePackAndRetriesAfterInterruption exercises
// the two-phase published -> delete_pending -> deleted lifecycle directly,
// including retrying a deletion that was interrupted after delete-pending was
// durably recorded but before the physical object was removed.
func TestPlanGCDeletesWhollyUnreachablePackAndRetriesAfterInterruption(t *testing.T) {
	daemonPath := testGCDaemonPath(t)
	repo, _, _ := TestRepositoryWithVersion(t, 0)
	repo.packerCount = 1

	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: gcTestSocket(t), RepositoryID: "gc-delete-test",
		DaemonPath: daemonPath, DataDir: t.TempDir(), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	repo.SetEngine(metadataindex.NewDaemonEngine(client))
	store := daemon.NewSchemaStore(client)

	var deadID vaultic.ID
	if err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var saveErr error
		deadID, _, _, saveErr = uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("wholly unreachable"), vaultic.ID{}, true)
		return saveErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	printer := vaultic.NewNoopPrinter()
	plan, err := PlanGC(context.Background(), GCOptions{}, repo, printer)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.wholePacks) != 1 {
		t.Fatalf("expected exactly one whole pack, got %#v", plan.wholePacks)
	}
	var doomedPack vaultic.ID
	var members []vaultic.ID
	for packID, blobs := range plan.wholePacks {
		doomedPack, members = packID, blobs
	}

	// Simulate a crash: durably record delete-pending, but do not delete the
	// backend object or purge the catalog yet.
	if err := store.MarkPackDeletePending(context.Background(), schema.ID(doomedPack)); err != nil {
		t.Fatal(err)
	}

	// A fresh PlanGC picks the interrupted pack up as a retry candidate.
	resumed, err := PlanGC(context.Background(), GCOptions{}, repo, printer)
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed.retryPacks) != 1 {
		t.Fatalf("expected the interrupted pack to be queued for retry, got %#v", resumed.retryPacks)
	}
	if err := resumed.Execute(context.Background(), printer); err != nil {
		t.Fatal(err)
	}
	if resumed.Stats.PacksRetried != 1 || resumed.Stats.PacksDeleted != 1 {
		t.Fatalf("retry stats = %#v", resumed.Stats)
	}

	if _, found, err := store.Get(context.Background(), schema.PackKey(schema.ID(doomedPack))); err != nil || found {
		t.Fatalf("retried pack catalog record survived: found=%t err=%v", found, err)
	}
	for _, blobID := range members {
		if blobID != deadID {
			continue
		}
		if _, found, err := store.Get(context.Background(), schema.BlobKey(schema.ID(blobID))); err != nil || found {
			t.Fatalf("retried blob catalog record survived: found=%t err=%v", found, err)
		}
	}
	if err := repo.List(context.Background(), vaultic.PackFile, func(id vaultic.ID, _ int64) error {
		if id == doomedPack {
			t.Fatalf("deleted pack object still present in backend")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestPlanGCRetryToleratesAlreadyRemovedBackendObject proves the symmetric
// crash point to the interruption test above: the physical object was
// already removed by a prior interrupted run, but MarkPackDeleted never
// committed. Retrying must treat the missing object as already deleted
// (matching object-store idempotent-DELETE semantics) and still complete
// the catalog cleanup, rather than getting permanently stuck retrying a
// physical removal that can never succeed again.
func TestPlanGCRetryToleratesAlreadyRemovedBackendObject(t *testing.T) {
	daemonPath := testGCDaemonPath(t)
	repo, _, _ := TestRepositoryWithVersion(t, 0)
	repo.packerCount = 1

	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: gcTestSocket(t), RepositoryID: "gc-retry-already-removed-test",
		DaemonPath: daemonPath, DataDir: t.TempDir(), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	repo.SetEngine(metadataindex.NewDaemonEngine(client))
	store := daemon.NewSchemaStore(client)

	if err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		_, _, _, saveErr := uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("wholly unreachable"), vaultic.ID{}, true)
		return saveErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	printer := vaultic.NewNoopPrinter()
	plan, err := PlanGC(context.Background(), GCOptions{}, repo, printer)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.wholePacks) != 1 {
		t.Fatalf("expected exactly one whole pack, got %#v", plan.wholePacks)
	}
	var doomedPack vaultic.ID
	for packID := range plan.wholePacks {
		doomedPack = packID
	}

	// Simulate a crash after the physical object was removed but before
	// MarkPackDeleted committed: delete-pending is recorded and the backend
	// object is already gone, yet the catalog record still exists.
	if err := store.MarkPackDeletePending(context.Background(), schema.ID(doomedPack)); err != nil {
		t.Fatal(err)
	}
	if err := (&internalRepository{repo}).RemoveUnpacked(context.Background(), vaultic.PackFile, doomedPack); err != nil {
		t.Fatal(err)
	}

	resumed, err := PlanGC(context.Background(), GCOptions{}, repo, printer)
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed.retryPacks) != 1 {
		t.Fatalf("expected the interrupted pack to be queued for retry, got %#v", resumed.retryPacks)
	}
	if err := resumed.Execute(context.Background(), printer); err != nil {
		t.Fatal(err)
	}
	if resumed.Stats.PacksRetryFailed != 0 || resumed.Stats.PacksRetried != 1 || resumed.Stats.PacksDeleted != 1 {
		t.Fatalf("retry stats = %#v, want a clean retry despite the already-missing object", resumed.Stats)
	}
	if _, found, err := store.Get(context.Background(), schema.PackKey(schema.ID(doomedPack))); err != nil || found {
		t.Fatalf("retried pack catalog record survived: found=%t err=%v", found, err)
	}
}

// TestPlanGCRetriesMultiplePendingPacksIndependently proves that one pack's
// retry failure does not block another pack's retry from succeeding in the
// same sweep.
func TestPlanGCRetriesMultiplePendingPacksIndependently(t *testing.T) {
	daemonPath := testGCDaemonPath(t)
	repo, _, _ := TestRepositoryWithVersion(t, 0)
	repo.packerCount = 1

	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: gcTestSocket(t), RepositoryID: "gc-multi-retry-test",
		DaemonPath: daemonPath, DataDir: t.TempDir(), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	repo.SetEngine(metadataindex.NewDaemonEngine(client))
	store := daemon.NewSchemaStore(client)

	makeDoomedPack := func(content string) {
		if err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
			_, _, _, saveErr := uploader.SaveBlob(ctx, vaultic.DataBlob, []byte(content), vaultic.ID{}, true)
			return saveErr
		}); err != nil {
			t.Fatal(err)
		}
		if err := repo.flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	makeDoomedPack("first unreachable pack")
	makeDoomedPack("second unreachable pack")

	printer := vaultic.NewNoopPrinter()
	plan, err := PlanGC(context.Background(), GCOptions{}, repo, printer)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.wholePacks) != 2 {
		t.Fatalf("expected exactly two whole packs, got %#v", plan.wholePacks)
	}
	var packIDs []vaultic.ID
	for packID := range plan.wholePacks {
		packIDs = append(packIDs, packID)
	}
	brokenPack, healthyPack := packIDs[0], packIDs[1]

	for _, packID := range packIDs {
		if err := store.MarkPackDeletePending(context.Background(), schema.ID(packID)); err != nil {
			t.Fatal(err)
		}
	}

	resumed, err := PlanGC(context.Background(), GCOptions{}, repo, printer)
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed.retryPacks) != 2 {
		t.Fatalf("expected both interrupted packs queued for retry, got %#v", resumed.retryPacks)
	}
	// Simulate the broken pack's catalog record having already been purged
	// out-of-band (e.g. a concurrent, unexpected cleanup) after planning
	// captured it as a retry candidate but before this sweep's retry
	// actually runs, so its retry fails at the MarkPackDeleted step while
	// the healthy pack's retry succeeds normally.
	if err := store.MarkPackDeleted(context.Background(), schema.ID(brokenPack), toSchemaIDs(resumed.retryPacks[brokenPack])); err != nil {
		t.Fatal(err)
	}

	if err := resumed.Execute(context.Background(), printer); err != nil {
		t.Fatal(err)
	}
	if resumed.Stats.PacksRetryFailed != 1 || resumed.Stats.PacksRetried != 1 || resumed.Stats.PacksDeleted != 1 {
		t.Fatalf("retry stats = %#v", resumed.Stats)
	}
	if _, found, err := store.Get(context.Background(), schema.PackKey(schema.ID(healthyPack))); err != nil || found {
		t.Fatalf("healthy pack catalog record survived: found=%t err=%v", found, err)
	}
	if _, found, err := store.Get(context.Background(), schema.PackKey(schema.ID(brokenPack))); err != nil || found {
		t.Fatalf("broken pack catalog record = found=%t err=%v, want already gone from the out-of-band deletion", found, err)
	}
}

// TestPruneStaleLegacyIndexesRemovesCheckpointsReferencingDeletedPacks proves
// the cleanup step gc relies on after a sweep: a checkpoint referencing a
// pack that no longer exists is removed along with its physical index
// object, while a checkpoint that only references surviving packs is left
// untouched.
func TestPruneStaleLegacyIndexesRemovesCheckpointsReferencingDeletedPacks(t *testing.T) {
	daemonPath := testGCDaemonPath(t)
	repo, _, _ := TestRepositoryWithVersion(t, 0)
	repo.packerCount = 1

	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: gcTestSocket(t), RepositoryID: "gc-stale-index-test",
		DaemonPath: daemonPath, DataDir: t.TempDir(), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	repo.SetEngine(metadataindex.NewDaemonEngine(client))
	store := daemon.NewSchemaStore(client)

	var doomedBlob, survivingBlob vaultic.ID
	if err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var saveErr error
		doomedBlob, _, _, saveErr = uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("doomed"), vaultic.ID{}, true)
		return saveErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var saveErr error
		survivingBlob, _, _, saveErr = uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("stays"), vaultic.ID{}, true)
		return saveErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	packs, err := scanPacks(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if len(packs) != 2 {
		t.Fatalf("expected exactly two packs, got %d", len(packs))
	}
	_, packMembers, _, err := scanBlobCatalog(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	var doomedPack, survivingPack vaultic.ID
	first := true
	for id := range packs {
		if first {
			doomedPack, first = id, false
		} else {
			survivingPack = id
		}
	}

	staleIndex := legacyindex.NewIndex()
	staleIndex.StorePack(doomedPack, pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: doomedBlob, Type: vaultic.DataBlob}, Length: 6}})
	staleIndex.StorePack(survivingPack, pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: survivingBlob, Type: vaultic.DataBlob}, Length: 5}})
	staleIndex.Finalize()
	staleIndexID, err := repo.SaveLegacyIndex(context.Background(), staleIndex)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkIndexPublished(context.Background(), schema.ID(staleIndexID), []schema.ID{schema.ID(doomedPack), schema.ID(survivingPack)}); err != nil {
		t.Fatal(err)
	}

	if err := store.MarkPackDeletePending(context.Background(), schema.ID(doomedPack)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackDeleted(context.Background(), schema.ID(doomedPack), toSchemaIDs(packMembers[doomedPack])); err != nil {
		t.Fatal(err)
	}

	freshIndex := legacyindex.NewIndex()
	freshIndex.StorePack(survivingPack, pack.Blobs{{BlobHandle: vaultic.BlobHandle{ID: survivingBlob, Type: vaultic.DataBlob}, Length: 5}})
	freshIndex.Finalize()
	freshIndexID, err := repo.SaveLegacyIndex(context.Background(), freshIndex)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkIndexPublished(context.Background(), schema.ID(freshIndexID), []schema.ID{schema.ID(survivingPack)}); err != nil {
		t.Fatal(err)
	}

	removed, err := PruneStaleLegacyIndexes(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	// Normal pack flushing also writes its own native legacy index, so the
	// doomed pack's flush produced an additional stale index beyond the one
	// manufactured above; only exact record-level assertions matter here.
	if removed == 0 {
		t.Fatalf("removed = %d, want at least 1", removed)
	}
	if _, found, err := store.Get(context.Background(), schema.ExportIndexCheckpointKey(schema.ID(staleIndexID))); err != nil || found {
		t.Fatalf("stale checkpoint survived: found=%t err=%v", found, err)
	}
	if _, found, err := store.Get(context.Background(), schema.ExportIndexCheckpointKey(schema.ID(freshIndexID))); err != nil || !found {
		t.Fatalf("fresh checkpoint incorrectly removed: found=%t err=%v", found, err)
	}
	if _, err := repo.LoadUnpacked(context.Background(), vaultic.IndexFile, staleIndexID); err == nil {
		t.Fatal("stale legacy index object survived")
	}
	if _, err := repo.LoadUnpacked(context.Background(), vaultic.IndexFile, freshIndexID); err != nil {
		t.Fatalf("fresh legacy index object was incorrectly removed: %v", err)
	}
}
