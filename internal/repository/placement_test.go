package repository

import (
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestPlacementModelSingleBackendDefaultsToCurrentBehavior(t *testing.T) {
	repo, err := New(mem.New(), Options{PackSize: MinPackSize})
	if err != nil {
		t.Fatal(err)
	}
	repo.setConfig(vaultic.Config{ID: "repo"})
	model, err := repo.PlacementModel()
	if err != nil {
		t.Fatal(err)
	}
	if model.HotCold {
		t.Fatal("single backend was reported as hot/cold")
	}
	if len(model.Backends) != 1 {
		t.Fatalf("backends = %d, want 1", len(model.Backends))
	}
	if model.Backends[0].ID != "single" || model.Backends[0].Role != PlacementRolePrimary || model.Backends[0].FailureDomain != "single" {
		t.Fatalf("single backend default = %#v", model.Backends[0])
	}
	if model.Policy.MinCopies != 1 || model.Policy.MinDomains != 1 {
		t.Fatalf("default policy = %#v", model.Policy)
	}
}

func TestPlacementModelUsesDeclaredBackendRegistry(t *testing.T) {
	repo, err := New(mem.New(), Options{PackSize: MinPackSize})
	if err != nil {
		t.Fatal(err)
	}
	repo.setConfig(vaultic.Config{
		ID: "repo",
		PlacementBackends: []vaultic.PlacementBackend{
			{ID: "local", Role: PlacementRolePrimary},
			{ID: "s3", Role: PlacementRoleArchival, Ingest: boolPtr(false), ReadEnabled: boolPtr(true), Offsite: true, FailureDomain: "aws", MinRetentionSeconds: 86400},
		},
		PlacementPolicy: vaultic.PlacementPolicy{MinCopies: 2, MinDomains: 2, MinOffsite: 1},
	})
	model, err := repo.PlacementModel()
	if err != nil {
		t.Fatal(err)
	}
	if len(model.Backends) != 2 {
		t.Fatalf("backends = %d, want 2", len(model.Backends))
	}
	if model.Backends[0].Hash == 0 || model.Backends[0].Hash == model.Backends[1].Hash {
		t.Fatalf("backend hashes are invalid: %#v", model.Backends)
	}
	if model.Backends[0].FailureDomain != "local" {
		t.Fatalf("empty failure domain was not defaulted to id: %#v", model.Backends[0])
	}
	if got := model.Backends[1].MinRetention().Hours(); got != 24 {
		t.Fatalf("min retention hours = %f, want 24", got)
	}
	if model.Backends[1].IngestEnabled() || !model.Backends[1].ReadAllowed() {
		t.Fatalf("read-only backend flags = ingest %v read %v", model.Backends[1].IngestEnabled(), model.Backends[1].ReadAllowed())
	}
	if model.Policy.MinCopies != 2 || model.Policy.MinOffsite != 1 {
		t.Fatalf("policy = %#v", model.Policy)
	}
}

func TestPlacementModelRejectsInvalidRegistry(t *testing.T) {
	for name, backends := range map[string][]vaultic.PlacementBackend{
		"empty id":  {{ID: ""}},
		"duplicate": {{ID: "a"}, {ID: "a"}},
	} {
		if _, err := placementModelFromConfig(backends, vaultic.PlacementPolicy{}, false); err == nil {
			t.Fatalf("%s registry was accepted", name)
		}
	}
	if _, err := placementModelFromConfig([]vaultic.PlacementBackend{{ID: "a"}}, vaultic.PlacementPolicy{MinCopies: 1, MinOffsite: 2}, false); err == nil {
		t.Fatal("min_offsite > min_copies was accepted")
	}
	if _, err := placementModelFromConfig([]vaultic.PlacementBackend{{ID: "a", Ingest: boolPtr(false), ReadEnabled: boolPtr(true)}}, vaultic.PlacementPolicy{}, false); err == nil {
		t.Fatal("all-read-only placement model was accepted")
	}
}

func boolPtr(value bool) *bool { return &value }
