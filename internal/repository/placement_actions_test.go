package repository

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestPlacePackCopiesToWarmBackendAndReadFallsBack(t *testing.T) {
	repo, _, store, packID, blobID := promotionTestRepository(t)
	config := repo.Config()
	config.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "local", Role: PlacementRolePrimary, FailureDomain: "local", RetrievalClass: "standard"},
		{ID: "warm", Role: PlacementRolePrimary, Offsite: true, FailureDomain: "warm", RetrievalClass: "standard", PricePerGBEgress: 1},
	}
	repo.setConfig(config)
	warm := mem.New()
	localHash, warmHash := PlacementBackendHash("local"), PlacementBackendHash("warm")
	repo.AttachPlacementBackend(warmHash, warm)
	if err := repo.PlacePack(context.Background(), packID, warmHash); err != nil {
		t.Fatal(err)
	}
	handle := backend.Handle{Type: backend.PackFile, Name: packID.String()}
	if _, err := warm.Stat(context.Background(), handle); err != nil {
		t.Fatalf("warm copy missing: %v", err)
	}
	placementValue, err := (schema.PlacementRecord{State: schema.PlacementLive, Bytes: 1, RetentionSource: schema.RetentionUnknown}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteMutableBatch(context.Background(), []daemon.Mutation{
		{Key: schema.PackPlacementKey(schema.ID(packID), localHash), Value: placementValue},
		{Key: schema.PackPlacementKey(schema.ID(packID), warmHash), Value: placementValue},
	}, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := repo.Backend().Remove(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	loaded, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob}, nil)
	if err != nil || string(loaded) != "retained promotion content" {
		t.Fatalf("fallback load = %q, err=%v", loaded, err)
	}
}
