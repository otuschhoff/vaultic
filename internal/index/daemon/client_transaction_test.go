package daemon

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func TestSchemaStorePublishesAuthoritativePacksAndDuplicateLocations(t *testing.T) {
	client, err := Ensure(
		context.Background(),
		Options{Socket: testSocket(t), RepositoryID: "phase6-packs", DaemonPath: daemonBinary(t), DataDir: t.TempDir()},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	blobID, packOne, packTwo := daemonTestID(31), daemonTestID(32), daemonTestID(33)
	for _, published := range []PublishedPack{
		{PackID: packOne,
			Record: schema.PackRecord{Type: schema.PackData,
				PayloadSize: 10,
				BlobCount:   1,
				Lifecycle:   schema.PackExportPending},
			Blobs: map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packOne,
				Length: 10,
				Type:   schema.BlobData}}}}},

		{PackID: packTwo,
			Record: schema.PackRecord{Type: schema.PackData,
				PayloadSize: 10,
				BlobCount:   1,
				Lifecycle:   schema.PackExportPending},
			Blobs: map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packTwo,
				Length: 10,
				Type:   schema.BlobData}}}}},
	} {
		if err := store.PublishPack(context.Background(), published); err != nil {
			t.Fatal(err)
		}
	}
	value, found, err := store.Get(context.Background(), schema.BlobKey(blobID))
	if err != nil || !found {
		t.Fatalf("blob record: found=%t err=%v", found, err)
	}
	record, err := schema.UnmarshalBlobRecord(value)
	if err != nil || len(record.Locations) != 2 {
		t.Fatalf("blob locations = %#v, err=%v", record.Locations, err)
	}
	aggregateValue, found, err := store.Get(context.Background(), schema.PackAggregateKey(schema.AggregateAll))
	if err != nil || !found {
		t.Fatalf("aggregate: found=%t err=%v", found, err)
	}
	aggregate, err := schema.UnmarshalPackAggregate(aggregateValue)
	if err != nil || aggregate.PackCount != 2 || aggregate.BlobCount != 2 || aggregate.PayloadSize != 20 {
		t.Fatalf("aggregate = %#v, err=%v", aggregate, err)
	}
}

func TestSchemaStoreTwoPhasePackDeletion(t *testing.T) {
	client, err := Ensure(
		context.Background(),
		Options{
			Socket:       testSocket(t),
			RepositoryID: "phase8-gc-delete",
			DaemonPath:   daemonBinary(t),
			DataDir:      t.TempDir(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	blobShared, blobOnlyB, packA, packB := daemonTestID(40), daemonTestID(41), daemonTestID(42), daemonTestID(43)
	for _, published := range []PublishedPack{
		{PackID: packA, Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 5, BlobCount: 1, Lifecycle: schema.PackExportPending},
			Blobs: map[schema.ID]schema.BlobRecord{blobShared: {Locations: []schema.BlobLocation{{PackID: packA, Length: 5, Type: schema.BlobData}}}}},
		{PackID: packB, Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 12, BlobCount: 2, Lifecycle: schema.PackExportPending},
			Blobs: map[schema.ID]schema.BlobRecord{
				blobShared: {Locations: []schema.BlobLocation{{PackID: packB, Offset: 7, Length: 5, Type: schema.BlobData}}},
				blobOnlyB:  {Locations: []schema.BlobLocation{{PackID: packB, Length: 7, Type: schema.BlobData}}},
			}},
	} {
		if err := store.PublishPack(ctx, published); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkPackPublished(ctx, packA); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackPublished(ctx, packB); err != nil {
		t.Fatal(err)
	}

	// A pack that has not been published cannot enter delete-pending.
	if err := store.MarkPackDeletePending(ctx, blobOnlyB); err == nil {
		t.Fatal("delete-pending accepted a nonexistent pack")
	}
	if err := store.MarkPackDeleted(ctx, packB, nil); err == nil {
		t.Fatal("deletion accepted a pack that is not delete-pending")
	}

	if err := store.MarkPackDeletePending(ctx, packB); err != nil {
		t.Fatal(err)
	}
	// Idempotent: repeating delete-pending on an already delete-pending pack is a no-op.
	if err := store.MarkPackDeletePending(ctx, packB); err != nil {
		t.Fatalf("repeated delete-pending: %v", err)
	}
	pendingValue, found, err := store.Get(ctx, schema.PackKey(packB))
	if err != nil || !found {
		t.Fatalf("delete-pending pack: found=%t err=%v", found, err)
	}
	pendingRecord, err := schema.UnmarshalPackRecord(pendingValue)
	if err != nil || pendingRecord.Lifecycle != schema.PackDeletePending {
		t.Fatalf("delete-pending lifecycle = %#v, err=%v", pendingRecord, err)
	}

	// Seed stale GC bookkeeping that deletion must clean up.
	gcValue, err := (schema.GarbageCollectionRecord{State: schema.GCRevalidated, ObservedCommit: 1}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteMutableBatch(ctx, []Mutation{
		{Key: schema.GarbageCollectionKey(schema.GCPack, packB), Value: gcValue},
		{Key: schema.GarbageCollectionKey(schema.GCBlob, blobOnlyB), Value: gcValue},
	}, nil, true); err != nil {
		t.Fatal(err)
	}

	if err := store.MarkPackDeleted(ctx, packB, []schema.ID{blobShared, blobOnlyB}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(ctx, schema.PackKey(packB)); err != nil || found {
		t.Fatalf("deleted pack still present: found=%t err=%v", found, err)
	}
	if _, found, err := store.Get(ctx, schema.BlobKey(blobOnlyB)); err != nil || found {
		t.Fatalf("blob unique to deleted pack still present: found=%t err=%v", found, err)
	}
	sharedValue, found, err := store.Get(ctx, schema.BlobKey(blobShared))
	if err != nil || !found {
		t.Fatalf("shared blob missing: found=%t err=%v", found, err)
	}
	sharedRecord, err := schema.UnmarshalBlobRecord(sharedValue)
	if err != nil || len(sharedRecord.Locations) != 1 || sharedRecord.Locations[0].PackID != packA {
		t.Fatalf("shared blob locations = %#v, err=%v", sharedRecord.Locations, err)
	}
	if _, found, err := store.Get(ctx, schema.GarbageCollectionKey(schema.GCPack, packB)); err != nil || found {
		t.Fatalf("stale pack GC record survived deletion: found=%t err=%v", found, err)
	}
	if _, found, err := store.Get(ctx, schema.GarbageCollectionKey(schema.GCBlob, blobOnlyB)); err != nil || found {
		t.Fatalf("stale blob GC record survived deletion: found=%t err=%v", found, err)
	}
	aggregateValue, found, err := store.Get(ctx, schema.PackAggregateKey(schema.AggregateAll))
	if err != nil || !found {
		t.Fatalf("aggregate: found=%t err=%v", found, err)
	}
	aggregate, err := schema.UnmarshalPackAggregate(aggregateValue)
	if err != nil || aggregate.PackCount != 1 || aggregate.BlobCount != 1 || aggregate.PayloadSize != 5 {
		t.Fatalf("aggregate after deletion = %#v, err=%v", aggregate, err)
	}

	// Re-deleting the same (now absent) pack fails closed rather than silently succeeding.
	if err := store.MarkPackDeleted(ctx, packB, nil); err == nil {
		t.Fatal("deletion accepted an already-deleted pack")
	}
}

func TestSchemaStoreCompletesSnapshotExportAtomically(t *testing.T) {
	client, err := Ensure(
		context.Background(),
		Options{
			Socket:       testSocket(t),
			RepositoryID: "phase6-snapshot",
			DaemonPath:   daemonBinary(t),
			DataDir:      t.TempDir(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	revision, err := store.AllocateRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rootKey := schema.DirectoryRevisionKey(0, 0, revision)
	rootValue := encodeSchemaRecord(
		t,
		schema.DirectoryRevision{
			Children: []schema.DirectoryChild{
				{Name: "file", Inode: 2, Type: schema.NodeFile, MetadataKey: schema.InodeRevisionKey(1, 2, 1)},
			},
			SourcePath: "/",
			Known:      schema.KnownPath,
			Freshness:  schema.FreshnessVerified,
		},
	)
	if err := store.PublishRevision(context.Background(), schema.CurrentDirectoryKey(0, 0), rootKey, rootValue, revision); err != nil {
		t.Fatal(err)
	}
	snapshotID := daemonTestID(41)
	if err := store.MarkExportPending(context.Background(), snapshotID, rootKey); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishSnapshotScope(context.Background(),
		SnapshotScope{SnapshotID: snapshotID,
			RootKey:      rootKey,
			OriginalJSON: []byte(("{\"time\":\"2026-08-29T12:34:56Z\",\"tree\":\"test\"}"))}); err != nil {
		t.Fatal(err)
	}
	checkpointValue, found, err := store.Get(context.Background(), schema.ExportCheckpointKey(snapshotID))
	if err != nil || !found {
		t.Fatalf("checkpoint: found=%t err=%v", found, err)
	}
	checkpoint, err := schema.UnmarshalExportCheckpointRecord(checkpointValue)
	if err != nil || checkpoint.State != schema.ExportComplete || checkpoint.CommitSequence == 0 ||
		!bytes.Equal(checkpoint.RootKey, rootKey) {
		t.Fatalf("checkpoint = %#v, err=%v", checkpoint, err)
	}
	if _, found, err := store.Get(context.Background(), schema.SnapshotKey(snapshotID)); err != nil || !found {
		t.Fatalf("snapshot scope: found=%t err=%v", found, err)
	}
	commitValue, found, err := store.Get(
		context.Background(),
		schema.SnapshotCommitKey(checkpoint.CommitSequence, snapshotID),
	)
	if err != nil || !found {
		t.Fatalf("snapshot commit index: found=%t err=%v", found, err)
	}
	commit, err := schema.UnmarshalSnapshotCommitRecord(commitValue)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(commit.RootKey, rootKey) || commit.SnapshotTimeUnixNano == 0 {
		t.Fatalf("snapshot commit record = %#v", commit)
	}
}

func TestSchemaStoreSnapshotMembershipDeltasPublishAndForget(t *testing.T) {
	client, err := Ensure(
		context.Background(),
		Options{
			Socket:       testSocket(t),
			RepositoryID: "phase16-snapshot-membership",
			DaemonPath:   daemonBinary(t),
			DataDir:      t.TempDir(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	metadata := schema.AnalyticsMetadataRecord{
		Enabled:    true,
		Generation: 1,
		BuiltAt:    time.Now().UnixNano(),
		ConfigJSON: "{}",
	}
	if err := store.Put(ctx, schema.AnalyticsMetadataKey(), encodeSchemaRecord(t, metadata), true); err != nil {
		t.Fatal(err)
	}
	inodeRevision, err := store.AllocateRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	inodeKey := schema.InodeRevisionKey(1, 2, inodeRevision)
	inode := schema.InodeRevision{Known: schema.KnownPath, SourcePath: "/file", Freshness: schema.FreshnessVerified}
	if err := store.PublishRevision(ctx, schema.CurrentInodeKey(1, 2), inodeKey, encodeSchemaRecord(t, inode), inodeRevision); err != nil {
		t.Fatal(err)
	}
	rootRevision, err := store.AllocateRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rootKey := schema.DirectoryRevisionKey(1, 1, rootRevision)
	root := schema.DirectoryRevision{
		Known:      schema.KnownPath,
		SourcePath: "/",
		Freshness:  schema.FreshnessVerified,
		Children:   []schema.DirectoryChild{{Name: "file", Inode: 2, Type: schema.NodeFile, MetadataKey: inodeKey}},
	}
	if err := store.PublishRevision(ctx, schema.CurrentDirectoryKey(1, 1), rootKey, encodeSchemaRecord(t, root), rootRevision); err != nil {
		t.Fatal(err)
	}
	snapshotID := daemonTestID(42)
	if err := store.MarkExportPending(ctx, snapshotID, rootKey); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishSnapshotScope(ctx,
		SnapshotScope{SnapshotID: snapshotID,
			RootKey:      rootKey,
			OriginalJSON: []byte(("{\"time\":\"2026-08-30T12:00:00Z\"}"))}); err != nil {
		t.Fatal(err)
	}
	snapshotValue, found, err := store.Get(ctx, schema.SnapshotKey(snapshotID))
	if err != nil || !found {
		t.Fatalf("published snapshot: found=%t err=%v", found, err)
	}
	snapshot, err := schema.UnmarshalSnapshotRecord(snapshotValue)
	if err != nil {
		t.Fatal(err)
	}
	deltaValue, found, err := store.Get(ctx, schema.AnalyticsDeltaKey(snapshot.CommitSequence, 0))
	if err != nil || !found {
		t.Fatalf("snapshot publish delta: found=%t err=%v", found, err)
	}
	delta, err := schema.UnmarshalAnalyticsDeltaRecord(deltaValue)
	if err != nil || delta.Kind != schema.AnalyticsDeltaRetainedReferences ||
		delta.IdentityGeneration != inodeRevision ||
		delta.RetainedSnapshotRefs != 1 {
		t.Fatalf("snapshot publish delta = %#v, err=%v", delta, err)
	}
	if err := store.ForgetSnapshot(ctx, snapshotID); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(ctx, schema.SnapshotKey(snapshotID)); err != nil || found {
		t.Fatalf("forgotten snapshot remains: found=%t err=%v", found, err)
	}
	forgetDeltaValue, found, err := store.Get(ctx, schema.AnalyticsDeltaKey(snapshot.CommitSequence+1, 0))
	if err != nil || !found {
		t.Fatalf("snapshot forget delta: found=%t err=%v", found, err)
	}
	forgetDelta, err := schema.UnmarshalAnalyticsDeltaRecord(forgetDeltaValue)
	if err != nil || forgetDelta.RetainedSnapshotRefs != 0 || forgetDelta.IdentityGeneration != inodeRevision {
		t.Fatalf("snapshot forget delta = %#v, err=%v", forgetDelta, err)
	}
}

func TestAuthoritativeCrawlProofControlsAbsenceAndIdentityGeneration(t *testing.T) {
	client, err := Ensure(
		context.Background(),
		Options{
			Socket:       testSocket(t),
			RepositoryID: "phase16-crawl-proof",
			DaemonPath:   daemonBinary(t),
			DataDir:      t.TempDir(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	metadata := schema.AnalyticsMetadataRecord{
		Enabled:    true,
		Generation: 1,
		BuiltAt:    time.Now().UnixNano(),
		ConfigJSON: "{}",
	}
	if err := store.Put(ctx, schema.AnalyticsMetadataKey(), encodeSchemaRecord(t, metadata), true); err != nil {
		t.Fatal(err)
	}
	publishInode := func(fsid uint32, inode uint64, path string) uint64 {
		t.Helper()
		revision, err := store.AllocateRevision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		record := schema.InodeRevision{Known: schema.KnownPath, SourcePath: path, Freshness: schema.FreshnessVerified}
		if err := store.PublishRevision(ctx,
			schema.CurrentInodeKey(fsid,
				inode),
			schema.InodeRevisionKey(fsid,
				inode,
				revision),
			encodeSchemaRecord(t,
				record),
			revision); err != nil {
			t.Fatal(err)
		}
		return revision
	}
	publishCrawl := func(snapshotByte byte, scope schema.ID, inode, inodeRevision, startFence uint64, complete bool, debtKeys [][]byte) uint64 {
		t.Helper()
		rootRevision, err := store.AllocateRevision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rootKey := schema.DirectoryRevisionKey(1, uint64(snapshotByte)+100, rootRevision)
		root := schema.DirectoryRevision{Known: schema.KnownPath, SourcePath: "/", Freshness: schema.FreshnessVerified}
		if inodeRevision != 0 {
			root.Children = []schema.DirectoryChild{
				{
					Name:        "file",
					Inode:       inode,
					Type:        schema.NodeFile,
					MetadataKey: schema.InodeRevisionKey(1, inode, inodeRevision),
				},
			}
		}
		if err := store.PublishRevision(ctx,
			schema.CurrentDirectoryKey(1,
				uint64(snapshotByte)+100),
			rootKey,
			encodeSchemaRecord(t,
				root),
			rootRevision); err != nil {
			t.Fatal(err)
		}
		snapshotID := daemonTestID(snapshotByte)
		if err := store.MarkExportPending(ctx, snapshotID, rootKey); err != nil {
			t.Fatal(err)
		}
		claim := &AuthoritativeCrawlClaim{
			ScopeID:    scope,
			RootFSID:   1,
			RootInode:  uint64(snapshotByte) + 100,
			StartFence: startFence,
			Complete:   complete,
			DebtKeys:   debtKeys,
		}
		if err := store.PublishSnapshotScope(ctx,
			SnapshotScope{SnapshotID: snapshotID,
				RootKey:      rootKey,
				OriginalJSON: []byte(("{\"time\":\"2026-08-30T12:00:00Z\"}")),
				Crawl:        claim}); err != nil {
			t.Fatal(err)
		}
		value, found, err := store.Get(ctx, schema.SnapshotKey(snapshotID))
		if err != nil || !found {
			t.Fatalf("snapshot commit: found=%t err=%v", found, err)
		}
		snapshot, err := schema.UnmarshalSnapshotRecord(value)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot.CommitSequence
	}
	readBinding := func(scope schema.ID, inode, generation uint64) schema.AuthoritativeSourceBindingRecord {
		t.Helper()
		value, found, err := store.Get(ctx, schema.AuthoritativeSourceBindingKey(scope, 1, inode, generation))
		if err != nil || !found {
			t.Fatalf("binding %d:%d: found=%t err=%v", inode, generation, found, err)
		}
		record, err := schema.UnmarshalAuthoritativeSourceBindingRecord(value)
		if err != nil {
			t.Fatal(err)
		}
		return record
	}

	scopeA, scopeB := daemonTestID(201), daemonTestID(202)
	first := publishInode(1, 2, "/file")
	firstA := publishCrawl(101, scopeA, 2, first, 1, true, nil)
	publishCrawl(102, scopeB, 2, first, 1, true, nil)
	deletedCommit := publishCrawl(103, scopeA, 0, 0, firstA, true, nil)
	deleted := readBinding(scopeA, 2, first)
	if deleted.State != schema.AuthoritativeSourceDeleted || deleted.Continuity != schema.AnalyticsContinuityProven {
		t.Fatalf("complete absence binding = %#v", deleted)
	}
	if isolated := readBinding(scopeB, 2, first); isolated.State != schema.AuthoritativeSourceLive {
		t.Fatalf("scope B was changed by scope A proof: %#v", isolated)
	}
	proofValue, found, err := store.Get(ctx, schema.AuthoritativeCrawlProofKey(scopeA, deletedCommit))
	if err != nil || !found {
		t.Fatalf("crawl proof: found=%t err=%v", found, err)
	}
	proof, err := schema.UnmarshalAuthoritativeCrawlProofRecord(proofValue)
	if err != nil || !proof.Complete || !proof.DebtFree {
		t.Fatalf("crawl proof = %#v, err=%v", proof, err)
	}
	reappeared := publishInode(1, 2, "/replacement")
	publishCrawl(104, scopeA, 2, reappeared, deletedCommit, true, nil)
	if current := readBinding(scopeA, 2, reappeared); current.State != schema.AuthoritativeSourceLive ||
		current.Continuity != schema.AnalyticsContinuityProven ||
		current.Generation == first {
		t.Fatalf("proven reappearance binding = %#v", current)
	}
	if old := readBinding(scopeA, 2, first); old.State != schema.AuthoritativeSourceDeleted {
		t.Fatalf("old generation was overwritten: %#v", old)
	}
	nextValue, found, err := store.Get(ctx, schema.NextRevisionKey())
	if err != nil || !found {
		t.Fatalf("next revision before forget: found=%t err=%v", found, err)
	}
	forgetCommit, err := schema.UnmarshalNextRevision(nextValue)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ForgetSnapshot(ctx, daemonTestID(104)); err != nil {
		t.Fatal(err)
	}
	forgetValue, found, err := store.Get(ctx, schema.AnalyticsDeltaKey(forgetCommit, 0))
	if err != nil || !found {
		t.Fatalf("forget delta: found=%t err=%v", found, err)
	}
	forgetDelta, err := schema.UnmarshalAnalyticsDeltaRecord(forgetValue)
	if err != nil || forgetDelta.IdentityGeneration != reappeared || forgetDelta.RetainedSnapshotRefs != 0 {
		t.Fatalf("reappeared forget delta = %#v, err=%v", forgetDelta, err)
	}

	scopeGap := daemonTestID(203)
	gapFirst := publishInode(1, 3, "/gap")
	gapObserved := publishCrawl(105, scopeGap, 3, gapFirst, 1, true, nil)
	publishCrawl(106, scopeGap, 0, 0, gapObserved, false, nil)
	if uncertain := readBinding(scopeGap, 3, gapFirst); uncertain.State != schema.AuthoritativeSourceUnknown ||
		uncertain.Continuity != schema.AnalyticsContinuityUnknown {
		t.Fatalf("incomplete absence binding = %#v", uncertain)
	}
	gapReappeared := publishInode(1, 3, "/gap-replacement")
	publishCrawl(107, scopeGap, 3, gapReappeared, gapObserved, false, nil)
	if uncertain := readBinding(scopeGap, 3, gapReappeared); uncertain.State != schema.AuthoritativeSourceLive ||
		uncertain.Continuity != schema.AnalyticsContinuityUnknown ||
		uncertain.Generation == gapFirst {
		t.Fatalf("gap reappearance binding = %#v", uncertain)
	}
	if old := readBinding(scopeGap, 3, gapFirst); old.State != schema.AuthoritativeSourceUnknown {
		t.Fatalf("gap generation was merged: %#v", old)
	}

	scopeDebt := daemonTestID(204)
	debtFirst := publishInode(1, 4, "/debt")
	debtObserved := publishCrawl(108, scopeDebt, 4, debtFirst, 1, true, nil)
	debtKey := schema.CrawlDebtKey(schema.ID{}, daemonTestID(205))
	debt := schema.CrawlDebtRecord{
		PathOrTree: []byte("debt"),
		Reason:     schema.DebtMissingInode,
		Status:     schema.DebtPending,
	}
	if err := store.Put(ctx, debtKey, encodeSchemaRecord(t, debt), true); err != nil {
		t.Fatal(err)
	}
	debtCommit := publishCrawl(109, scopeDebt, 0, 0, debtObserved, true, [][]byte{debtKey})
	if uncertain := readBinding(scopeDebt, 4, debtFirst); uncertain.State != schema.AuthoritativeSourceUnknown {
		t.Fatalf("debt-bearing absence proved deletion: %#v", uncertain)
	}
	proofValue, _, _ = store.Get(ctx, schema.AuthoritativeCrawlProofKey(scopeDebt, debtCommit))
	proof, err = schema.UnmarshalAuthoritativeCrawlProofRecord(proofValue)
	if err != nil || !proof.Complete || proof.DebtFree {
		t.Fatalf("debt-bearing proof = %#v, err=%v", proof, err)
	}

	scopeFence := daemonTestID(206)
	fenceFirst := publishInode(1, 5, "/fenced")
	fenceObserved := publishCrawl(110, scopeFence, 5, fenceFirst, 1, true, nil)
	publishCrawl(111, scopeFence, 0, 0, fenceObserved-1, true, nil)
	if fenced := readBinding(scopeFence, 5, fenceFirst); fenced.State != schema.AuthoritativeSourceUnknown {
		t.Fatalf("stale start fence proved deletion: %#v", fenced)
	}
}

func TestSchemaStoreImportsLegacyPacksIdempotently(t *testing.T) {
	options := Options{
		Socket:       testSocket(t),
		RepositoryID: "phase4-pack-import",
		DaemonPath:   daemonBinary(t),
		DataDir:      t.TempDir(),
	}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	source1, source2 := daemonTestID(1), daemonTestID(2)
	pack1, pack2, blobID := daemonTestID(3), daemonTestID(4), daemonTestID(5)
	location := func(packID schema.ID, offset uint64) map[schema.ID]schema.BlobRecord {
		return map[schema.ID]schema.BlobRecord{
			blobID: {
				Locations: []schema.BlobLocation{
					{PackID: packID, Offset: offset, Length: 8, UncompressedSize: 7, Type: schema.BlobData},
				},
			},
		}
	}
	debtKey := schema.CrawlDebtKey(schema.ID{}, pack1)
	debt := schema.CrawlDebtRecord{
		SourceIndexOrPack: pack1,
		SourceKnown:       true,
		Reason:            schema.DebtUnavailablePack,
		Status:            schema.DebtPending,
		ErrorClass:        "offline",
	}
	first := LegacyPackImport{
		SourceIndex: source1, PackID: pack1,
		Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 8, BlobCount: 1, Lifecycle: schema.PackImported},
		Blobs:  location(pack1, 1), DebtKey: debtKey, Debt: &debt,
	}
	if err := store.ImportLegacyPack(ctx, first); err != nil {
		t.Fatal(err)
	}
	first.Record.PhysicalSize, first.Record.HeaderSize, first.Record.PhysicalSizeKnown, first.Debt = 10, 2, true, nil
	if err := store.ImportLegacyPack(ctx, first); err != nil {
		t.Fatal(err)
	}
	first.SourceIndex = source2
	if err := store.ImportLegacyPack(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := LegacyPackImport{
		SourceIndex: source2, PackID: pack2,
		Record: schema.PackRecord{
			Type:              schema.PackData,
			PhysicalSize:      12,
			PhysicalSizeKnown: true,
			PayloadSize:       8,
			HeaderSize:        4,
			BlobCount:         1,
			Lifecycle:         schema.PackImported,
		},
		Blobs: location(pack2, 2),
	}
	if err := store.ImportLegacyPack(ctx, second); err != nil {
		t.Fatal(err)
	}

	blobValue, found, err := store.Get(ctx, schema.BlobKey(blobID))
	if err != nil || !found {
		t.Fatalf("read imported blob: found=%t err=%v", found, err)
	}
	blob, err := schema.UnmarshalBlobRecord(blobValue)
	if err != nil || len(blob.Locations) != 2 || blob.Locations[0].PackID != pack1 ||
		blob.Locations[1].PackID != pack2 {
		t.Fatalf("imported blob locations = %#v, err=%v", blob.Locations, err)
	}
	packValue, found, err := store.Get(ctx, schema.PackKey(pack1))
	if err != nil || !found {
		t.Fatalf("read imported pack: found=%t err=%v", found, err)
	}
	packRecord, err := schema.UnmarshalPackRecord(packValue)
	if err != nil || len(packRecord.SourceIndexIDs) != 2 || packRecord.SourceIndexIDs[0] != source1 ||
		packRecord.SourceIndexIDs[1] != source2 ||
		packRecord.PhysicalSize != 10 {
		t.Fatalf("imported pack = %#v, err=%v", packRecord, err)
	}
	if err := store.MarkPackPublished(ctx, pack1); err != nil {
		t.Fatalf("mark imported pack published: %v", err)
	}
	packValue, found, err = store.Get(ctx, schema.PackKey(pack1))
	if err != nil || !found {
		t.Fatalf("read published pack: found=%t err=%v", found, err)
	}
	packRecord, err = schema.UnmarshalPackRecord(packValue)
	if err != nil || packRecord.Lifecycle != schema.PackPublished || len(packRecord.SourceIndexIDs) != 2 ||
		packRecord.PhysicalSize != 10 {
		t.Fatalf("published imported pack = %#v, err=%v", packRecord, err)
	}
	aggregateValue, found, err := store.Get(ctx, schema.PackAggregateKey(schema.AggregateAll))
	if err != nil || !found {
		t.Fatalf("read aggregate: found=%t err=%v", found, err)
	}
	aggregate, err := schema.UnmarshalPackAggregate(aggregateValue)
	if err != nil || aggregate.PackCount != 2 || aggregate.PhysicalSize != 22 || aggregate.PayloadSize != 16 ||
		aggregate.HeaderSize != 6 ||
		aggregate.BlobCount != 2 {
		t.Fatalf("imported aggregate = %#v, err=%v", aggregate, err)
	}
	debtValue, found, err := store.Get(ctx, debtKey)
	if err != nil || !found {
		t.Fatalf("read pack debt: found=%t err=%v", found, err)
	}
	resolvedDebt, err := schema.UnmarshalCrawlDebtRecord(debtValue)
	if err != nil || resolvedDebt.Status != schema.DebtResolved || resolvedDebt.ErrorClass != "" {
		t.Fatalf("resolved pack debt = %#v, err=%v", resolvedDebt, err)
	}
}
