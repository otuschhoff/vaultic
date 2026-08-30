package daemon

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func TestExecuteGDPRForgetRedactsReferencesQueuesExclusivePackAndReplays(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "gdpr-forget", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	targetUID := uint32(600)
	exclusiveBlob, sharedBlob := testSchemaID(1), testSchemaID(2)
	exclusivePack, sharedPack := testSchemaID(3), testSchemaID(4)
	runID := testSchemaID(5)
	targetKey := schema.InodeRevisionKey(1, 10, 1)
	externalKey := schema.InodeRevisionKey(1, 11, 1)
	directoryKey := schema.DirectoryRevisionKey(1, 1, 1)
	target := schema.InodeRevision{Size: 20, UID: targetUID, Known: schema.KnownSize | schema.KnownUID | schema.KnownPath, ContentMode: schema.ContentInline, ContentCount: 2, ContentIDs: []schema.ID{exclusiveBlob, sharedBlob}, SourcePath: "/home/alice/data", FileContentHash: testSchemaID(9), HashKnown: true, Freshness: schema.FreshnessVerified}
	external := schema.InodeRevision{Size: 10, UID: 601, Known: schema.KnownSize | schema.KnownUID | schema.KnownPath, ContentMode: schema.ContentInline, ContentCount: 1, ContentIDs: []schema.ID{sharedBlob}, SourcePath: "/home/bob/shared", Freshness: schema.FreshnessVerified}
	for key, record := range map[string]schema.InodeRevision{string(targetKey): target, string(externalKey): external} {
		if err := store.CreateImmutable(ctx, []byte(key), encodeSchemaRecord(t, record)); err != nil {
			t.Fatal(err)
		}
	}
	directory := schema.DirectoryRevision{Children: []schema.DirectoryChild{{Name: "alice-secret", Inode: 10, Type: schema.NodeFile, MetadataKey: targetKey}}, Freshness: schema.FreshnessVerified}
	if err := store.CreateImmutable(ctx, directoryKey, encodeSchemaRecord(t, directory)); err != nil {
		t.Fatal(err)
	}
	blobs := []Mutation{
		{Key: schema.BlobKey(exclusiveBlob), Value: encodeSchemaRecord(t, schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: exclusivePack, Type: schema.BlobData}}})},
		{Key: schema.BlobKey(sharedBlob), Value: encodeSchemaRecord(t, schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: sharedPack, Type: schema.BlobData}}})},
		{Key: schema.PackKey(exclusivePack), Value: encodeSchemaRecord(t, schema.PackRecord{Type: schema.PackData, PhysicalSize: 100, PayloadSize: 20, HeaderSize: 80, BlobCount: 1, PhysicalSizeKnown: true, Lifecycle: schema.PackPublished})},
		{Key: schema.PackKey(sharedPack), Value: encodeSchemaRecord(t, schema.PackRecord{Type: schema.PackData, PhysicalSize: 100, PayloadSize: 10, HeaderSize: 90, BlobCount: 1, PhysicalSizeKnown: true, Lifecycle: schema.PackPublished})},
	}
	if err := store.PublishSchemaBatch(ctx, blobs, nil); err != nil {
		t.Fatal(err)
	}
	mutable := []Mutation{
		{Key: schema.ReverseInodeKey(exclusiveBlob, 1, 10), Value: encodeSchemaRecord(t, schema.ReverseInodeRecord{LatestRevision: 1, State: schema.ReferenceCurrent})},
		{Key: schema.ReverseInodeKey(sharedBlob, 1, 10), Value: encodeSchemaRecord(t, schema.ReverseInodeRecord{LatestRevision: 1, State: schema.ReferenceCurrent})},
		{Key: schema.ReverseInodeKey(sharedBlob, 1, 11), Value: encodeSchemaRecord(t, schema.ReverseInodeRecord{LatestRevision: 1, State: schema.ReferenceCurrent})},
		{Key: schema.ReferenceCountKey(exclusiveBlob), Value: encodeSchemaRecord(t, schema.ReferenceCountRecord{TotalReferences: 1, DistinctInodes: 1, DistinctRevisions: 1})},
		{Key: schema.ReferenceCountKey(sharedBlob), Value: encodeSchemaRecord(t, schema.ReferenceCountRecord{TotalReferences: 2, DistinctInodes: 2, DistinctRevisions: 2})},
		{Key: schema.PackPlacementKey(exclusivePack, 7), Value: encodeSchemaRecord(t, schema.PlacementRecord{State: schema.PlacementLive, Bytes: 100, RetentionSource: schema.RetentionBackend, MinRetentionUntil: 200})},
		{Key: schema.PackPlacementKey(sharedPack, 7), Value: encodeSchemaRecord(t, schema.PlacementRecord{State: schema.PlacementLive, Bytes: 100, RetentionSource: schema.RetentionUnknown})},
		{Key: schema.UserInodeKey(targetUID, 1, 10), Value: encodeSchemaRecord(t, schema.AnalyticsUserInodeRecord{LatestRevision: 1, PathSample: "/home/alice/data"})},
		{Key: schema.PathVersionKey(1, "home/alice/data", 1), Value: encodeSchemaRecord(t, schema.PathVersionRecord{State: schema.PathBound, NodeType: schema.NodeFile, Inode: 10, Revision: 1})},
	}
	if err := store.WriteMutableBatch(ctx, mutable, nil, true); err != nil {
		t.Fatal(err)
	}

	signingSeed := sha256.Sum256([]byte("operator GDPR signing identity"))
	request := GDPRForgetRequest{UID: targetUID, ExecutedAt: 100, RunID: runID, SigningKey: ed25519.NewKeyFromSeed(signingSeed[:])}
	deletionDeadline := time.Unix(request.ExecutedAt, 0).UnixNano()
	certificate, err := store.ExecuteGDPRForget(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	signingBytes, err := certificate.SigningBytes()
	if err != nil || certificate.SigningAlgorithm != "Ed25519" || !ed25519.Verify(certificate.PublicKey, signingBytes, certificate.Signature) {
		t.Fatalf("invalid certificate signature: %+v, %v", certificate, err)
	}
	if len(certificate.PurgedReferenceHashes) != 2 || len(certificate.PendingDeletion) != 1 || certificate.PendingDeletion[0].PackID != exclusivePack || certificate.PendingDeletion[0].DeleteAfter != deletionDeadline {
		t.Fatalf("unexpected certificate: %+v", certificate)
	}
	value, found, err := store.Get(ctx, targetKey)
	if err != nil || !found {
		t.Fatal(err)
	}
	redacted, err := schema.UnmarshalInodeRevision(value)
	if err != nil || redacted.Known&(schema.KnownUID|schema.KnownPath|schema.KnownSize) != 0 || redacted.ContentCount != 0 || redacted.HashKnown {
		t.Fatalf("target was not redacted: %+v, %v", redacted, err)
	}
	value, found, err = store.Get(ctx, externalKey)
	if err != nil || !found {
		t.Fatal(err)
	}
	retained, err := schema.UnmarshalInodeRevision(value)
	if err != nil || retained.UID != 601 || len(retained.ContentIDs) != 1 || retained.ContentIDs[0] != sharedBlob {
		t.Fatalf("external reference changed: %+v, %v", retained, err)
	}
	value, found, err = store.Get(ctx, directoryKey)
	if err != nil || !found {
		t.Fatal(err)
	}
	directory, err = schema.UnmarshalDirectoryRevision(value)
	if err != nil || len(directory.Children) != 0 {
		t.Fatalf("directory retained redacted child name: %+v, %v", directory, err)
	}
	if _, found, err := store.Get(ctx, schema.PathVersionKey(1, "home/alice/data", 1)); err != nil || found {
		t.Fatalf("path binding survived: found=%t err=%v", found, err)
	}
	if _, found, err := store.Get(ctx, schema.ReferenceCountKey(exclusiveBlob)); err != nil || found {
		t.Fatalf("exclusive reference count survived: found=%t err=%v", found, err)
	}
	value, found, err = store.Get(ctx, schema.ReferenceCountKey(sharedBlob))
	if err != nil || !found {
		t.Fatal(err)
	}
	sharedCount, err := schema.UnmarshalReferenceCountRecord(value)
	if err != nil || sharedCount.TotalReferences != 1 || sharedCount.DistinctInodes != 1 {
		t.Fatalf("shared count = %+v, %v", sharedCount, err)
	}
	if _, found, err := store.Get(ctx, schema.PlacementDeleteQueueKey(deletionDeadline, exclusivePack, 7)); err != nil || !found {
		t.Fatalf("exclusive pack not queued: found=%t err=%v", found, err)
	}
	value, found, err = store.Get(ctx, schema.PackKey(exclusivePack))
	if err != nil || !found {
		t.Fatal(err)
	}
	pack, err := schema.UnmarshalPackRecord(value)
	if err != nil || pack.Lifecycle != schema.PackDeletePending {
		t.Fatalf("exclusive pack lifecycle = %+v, %v", pack, err)
	}
	value, found, err = store.Get(ctx, schema.PackPlacementKey(exclusivePack, 7))
	if err != nil || !found {
		t.Fatal(err)
	}
	placement, err := schema.UnmarshalPlacementRecord(value)
	if err != nil || placement.State != schema.PlacementEvicting || placement.DeleteAfter != deletionDeadline {
		t.Fatalf("exclusive placement = %+v, %v", placement, err)
	}
	value, found, err = store.Get(ctx, schema.BackendPackKey(7, exclusivePack))
	if err != nil || !found {
		t.Fatal(err)
	}
	backendPack, err := schema.UnmarshalBackendPackRecord(value)
	if err != nil || backendPack.State != schema.PlacementEvicting {
		t.Fatalf("exclusive backend pack = %+v, %v", backendPack, err)
	}
	if items, _, err := store.ScanPrefix(ctx, []byte("dq:"), nil, 10); err != nil || len(items) != 1 {
		t.Fatalf("unexpected deletion queue: %d, %v", len(items), err)
	}
	if _, found, err := store.Get(ctx, schema.UserInodeKey(targetUID, 1, 10)); err != nil || found {
		t.Fatalf("analytics PII survived: found=%t err=%v", found, err)
	}
	replayRequest := request
	replayRequest.ExecutedAt++
	replayed, err := store.ExecuteGDPRForget(ctx, replayRequest)
	if err != nil || string(replayed.Signature) != string(certificate.Signature) {
		t.Fatalf("replay changed certificate: %+v, %v", replayed, err)
	}
}
