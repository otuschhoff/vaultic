//go:build rados

package repository

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/rados"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/options"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestLivePlacePackCopiesToRADOS(t *testing.T) {
	monitors, key := os.Getenv("VAULTIC_RADOS_TEST_MONITORS"), os.Getenv("VAULTIC_RADOS_TEST_KEY")
	if monitors == "" || key == "" {
		t.Skip("set VAULTIC_RADOS_TEST_MONITORS and VAULTIC_RADOS_TEST_KEY")
	}
	repo, _, catalog, packID, blobID := promotionTestRepository(t)
	placement, err := rados.Open(t.Context(), rados.Config{
		Monitors: monitors, ClusterFSID: "2f525d6a-8f31-4f79-b731-82a6acb235f5",
		Pool: "vaultic", Namespace: "repo", Prefix: fmt.Sprintf("placement-%d/", time.Now().UnixNano()),
		Client: "client.vaultic", Key: options.NewSecretString(key), OperationTTL: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateConfig(t.Context(), func(config *vaultic.Config) error {
		config.PlacementBackends = []vaultic.PlacementBackend{
			{ID: "local", Role: PlacementRolePrimary, FailureDomain: "local"},
			{ID: "ceph", Role: PlacementRoleArchival, Offsite: true, FailureDomain: "ceph"},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cephHash := PlacementBackendHash("ceph")
	repo.AttachPlacementBackend(cephHash, placement)
	if err := repo.PlacePack(t.Context(), packID, cephHash); err != nil {
		t.Fatal(err)
	}
	handle := backend.Handle{Type: backend.PackFile, Name: packID.String()}
	if _, err := placement.Stat(t.Context(), handle); err != nil {
		t.Fatalf("placed RADOS pack is unavailable: %v", err)
	}
	placementValue, err := (schema.PlacementRecord{
		State: schema.PlacementLive, Bytes: 1, RetentionSource: schema.RetentionUnknown,
	}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.WriteMutableBatch(t.Context(), []daemon.Mutation{
		{Key: schema.PackPlacementKey(schema.ID(packID), PlacementBackendHash("local")), Value: placementValue},
		{Key: schema.PackPlacementKey(schema.ID(packID), cephHash), Value: placementValue},
	}, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := repo.Backend().Remove(t.Context(), handle); err != nil {
		t.Fatal(err)
	}
	loaded, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{Type: vaultic.DataBlob, ID: blobID}, nil)
	if err != nil || string(loaded) != "retained promotion content" {
		t.Fatalf("placement fallback = %q, %v", loaded, err)
	}
}
