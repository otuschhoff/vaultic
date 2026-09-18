package daemon

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func TestReadSessionPinsSnapshotAndClosesTransaction(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase33-read-session",
		DaemonPath: daemonBinary(t), DataDir: t.TempDir(), ObjectStore: "memory",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	store := NewSchemaStore(client)
	firstPack, firstBlob := daemonTestID(221), daemonTestID(222)
	if err := store.PublishPack(ctx, readSessionTestPack(firstPack, firstBlob)); err != nil {
		t.Fatal(err)
	}
	session, err := store.BeginReadSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if session.Identity.RepositoryID != "phase33-read-session" || session.Identity.Generation == 0 || session.Identity.SessionID == "" {
		t.Fatalf("invalid session identity: %+v", session.Identity)
	}
	secondPack, secondBlob := daemonTestID(223), daemonTestID(224)
	if err := store.PublishPack(ctx, readSessionTestPack(secondPack, secondBlob)); err != nil {
		t.Fatal(err)
	}
	if _, found, err := session.Get(ctx, schema.PackKey(firstPack)); err != nil || !found {
		t.Fatalf("original snapshot value: found=%t err=%v", found, err)
	}
	if _, found, err := session.Get(ctx, schema.PackKey(secondPack)); err != nil || found {
		t.Fatalf("post-snapshot value: found=%t err=%v", found, err)
	}
	if err := session.Validate(ctx); err != nil {
		t.Fatalf("validate pinned session: %v", err)
	}
	entries, _, err := session.ScanPrefix(ctx, []byte("p:"), nil, 100)
	if err != nil || len(entries) != 1 {
		t.Fatalf("snapshot pack scan: entries=%d err=%v", len(entries), err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := session.Get(ctx, schema.PackKey(firstPack)); err == nil {
		t.Fatal("read succeeded after session close")
	}
}

func readSessionTestPack(packID, blobID schema.ID) PublishedPack {
	return PublishedPack{
		PackID: packID,
		Record: schema.PackRecord{
			Type: schema.PackData, PayloadSize: 7, BlobCount: 1, Lifecycle: schema.PackExportPending,
		},
		Blobs: map[schema.ID]schema.BlobRecord{blobID: {
			Locations: []schema.BlobLocation{{PackID: packID, Length: 7, Type: schema.BlobData}},
		}},
	}
}
