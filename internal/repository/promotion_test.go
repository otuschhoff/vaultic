package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func promotionTestRepository(t *testing.T) (*Repository, *metadataindex.DaemonEngine, *daemon.SchemaStore, vaultic.ID, vaultic.ID) {
	t.Helper()
	repo, _, _ := TestRepositoryWithVersion(t, 0)
	repo.packerCount = 1
	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: gcTestSocket(t), RepositoryID: t.Name(), DaemonPath: testGCDaemonPath(t),
		DataDir: t.TempDir(), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	engine := metadataindex.NewDaemonEngine(client)
	engine.SetTierPolicy(repo.tierPolicy())
	repo.SetEngine(engine)
	var blobID vaultic.ID
	if err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var saveErr error
		blobID, _, _, saveErr = uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("retained promotion content"), vaultic.ID{}, true)
		return saveErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	packs, err := scanPacks(context.Background(), engine.SchemaStore())
	if err != nil {
		t.Fatal(err)
	}
	if len(packs) != 1 {
		t.Fatalf("packs = %d, want 1", len(packs))
	}
	var packID vaultic.ID
	for packID = range packs {
	}
	return repo, engine, engine.SchemaStore(), packID, blobID
}

func TestPromotePackSkipsSourceWithoutRetainedBlobs(t *testing.T) {
	repo, _, _, packID, _ := promotionTestRepository(t)
	model, err := repo.PlacementModel()
	if err != nil {
		t.Fatal(err)
	}
	_, err = PromotePack(context.Background(), repo, packID, model.Backends[0].Hash, vaultic.NewNoopPrinter())
	if !errors.Is(err, schema.ErrPlacementObsolete) {
		t.Fatalf("promotion error = %v, want obsolete", err)
	}
}

func TestPromotePackRewritesRetainedBlobToNewPack(t *testing.T) {
	repo, engine, store, sourcePack, blobID := promotionTestRepository(t)
	keep := newGCBlobSet()
	keep.Insert(vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob})
	model, err := repo.PlacementModel()
	if err != nil {
		t.Fatal(err)
	}
	successors, err := promotePackBlobs(context.Background(), repo, engine, sourcePack, model.Backends[0].Hash, keep, vaultic.NewNoopPrinter(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(successors) == 0 {
		t.Fatal("promotion returned no successor")
	}
	for _, successor := range successors {
		if successor == sourcePack {
			t.Fatal("promotion copied the source pack ID instead of repacking")
		}
	}
	value, found, err := store.Get(context.Background(), schema.PackKey(schema.ID(sourcePack)))
	if err != nil || !found {
		t.Fatalf("source pack missing: found=%v err=%v", found, err)
	}
	source, err := schema.UnmarshalPackRecord(value)
	if err != nil || source.Lifecycle != schema.PackDeletePending {
		t.Fatalf("source lifecycle = %v, err=%v", source.Lifecycle, err)
	}
	value, found, err = store.Get(context.Background(), schema.BlobKey(schema.ID(blobID)))
	if err != nil || !found {
		t.Fatalf("blob record missing: found=%v err=%v", found, err)
	}
	blob, err := schema.UnmarshalBlobRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	var hasSuccessor bool
	for _, location := range blob.Locations {
		for _, successor := range successors {
			hasSuccessor = hasSuccessor || location.PackID == schema.ID(successor)
		}
	}
	if !hasSuccessor {
		t.Fatalf("blob locations do not contain successor: %#v", blob.Locations)
	}
}

func TestPromotePackResumesAfterCrashFollowingPublish(t *testing.T) {
	repo, engine, store, sourcePack, blobID := promotionTestRepository(t)
	keep := newGCBlobSet()
	keep.Insert(vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob})
	model, err := repo.PlacementModel()
	if err != nil {
		t.Fatal(err)
	}
	crashErr := errors.New("injected crash after publish")
	var published []vaultic.ID
	_, err = promotePackBlobs(context.Background(), repo, engine, sourcePack, model.Backends[0].Hash, keep, vaultic.NewNoopPrinter(), func(successors []vaultic.ID) error {
		published = append(published, successors...)
		return crashErr
	})
	if !errors.Is(err, crashErr) || len(published) == 0 {
		t.Fatalf("crash result successors=%v err=%v", published, err)
	}
	value, found, err := store.Get(context.Background(), schema.PackKey(schema.ID(sourcePack)))
	if err != nil || !found {
		t.Fatalf("source missing after crash: found=%v err=%v", found, err)
	}
	source, err := schema.UnmarshalPackRecord(value)
	if err != nil || source.Lifecycle != schema.PackPublished {
		t.Fatalf("source not reachable after crash: lifecycle=%v err=%v", source.Lifecycle, err)
	}
	resumed, err := PromotePack(context.Background(), repo, sourcePack, model.Backends[0].Hash, vaultic.NewNoopPrinter())
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed) != len(published) || resumed[0] != published[0] {
		t.Fatalf("resumed successors=%v, published=%v", resumed, published)
	}
	packs, err := scanPacks(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if len(packs) != 2 {
		t.Fatalf("promotion retry created duplicate packs: %d", len(packs))
	}
}

func TestPromotionWritesReplacementToAttachedArchivalBackend(t *testing.T) {
	repo, _, primary := TestRepositoryWithVersion(t, 0)
	repo.packerCount = 1
	config := repo.Config()
	config.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "local", Role: PlacementRolePrimary, FailureDomain: "local"},
		{ID: "archive", Role: PlacementRoleArchival, Offsite: true, FailureDomain: "archive", MinRetentionSeconds: 3600},
	}
	repo.setConfig(config)
	archive := mem.New()
	archiveHash := PlacementBackendHash("archive")
	repo.AttachPlacementBackend(archiveHash, archive)
	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: gcTestSocket(t), RepositoryID: t.Name(), DaemonPath: testGCDaemonPath(t),
		DataDir: t.TempDir(), ObjectStore: "local",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	engine := metadataindex.NewDaemonEngine(client)
	engine.SetTierPolicy(repo.tierPolicy())
	repo.SetEngine(engine)
	var blobID vaultic.ID
	if err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var saveErr error
		blobID, _, _, saveErr = uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("archival target content"), vaultic.ID{}, true)
		return saveErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	packs, err := scanPacks(context.Background(), engine.SchemaStore())
	if err != nil {
		t.Fatal(err)
	}
	var source vaultic.ID
	for source = range packs {
	}
	keep := newGCBlobSet()
	keep.Insert(vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob})
	successors, err := promotePackBlobs(context.Background(), repo, engine, source, archiveHash, keep, vaultic.NewNoopPrinter(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(successors) != 1 {
		t.Fatalf("successors = %v", successors)
	}
	handle := backend.Handle{Type: backend.PackFile, Name: successors[0].String()}
	if _, err := archive.Stat(context.Background(), handle); err != nil {
		t.Fatalf("replacement missing from archive: %v", err)
	}
	if _, err := primary.Stat(context.Background(), handle); err == nil || !primary.IsNotExist(err) {
		t.Fatalf("replacement unexpectedly written to primary: %v", err)
	}
	value, found, err := engine.SchemaStore().Get(context.Background(), schema.PackPlacementKey(schema.ID(successors[0]), archiveHash))
	if err != nil || !found {
		t.Fatalf("archive placement missing: found=%v err=%v", found, err)
	}
	placement, err := schema.UnmarshalPlacementRecord(value)
	if err != nil || placement.State != schema.PlacementLive || placement.MinRetentionUntil == 0 {
		t.Fatalf("archive placement=%#v err=%v", placement, err)
	}
}
