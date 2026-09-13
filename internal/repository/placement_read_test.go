package repository

import (
	"context"
	"io"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type fixedReadAuthorizationBackend struct {
	backend.Backend
	authorized bool
}

func (be fixedReadAuthorizationBackend) ReadAuthorizedNow() bool {
	return be.authorized
}

func TestPlacementReadCandidatesPreferCheapestLivePlacement(t *testing.T) {
	candidates := []placementReadCandidate{
		{policy: PlacementBackend{vaulticPlacementBackend("archive", "hours", 0.01), 3}},
		{policy: PlacementBackend{vaulticPlacementBackend("warm", "standard", 0.02), 2}},
		{policy: PlacementBackend{vaulticPlacementBackend("local", "standard", 0), 1}},
	}
	sortPlacementReadCandidates(candidates)
	if candidates[0].policy.ID != "local" || candidates[1].policy.ID != "warm" || candidates[2].policy.ID != "archive" {
		t.Fatalf("candidate order = %q, %q, %q", candidates[0].policy.ID, candidates[1].policy.ID, candidates[2].policy.ID)
	}
}

func vaulticPlacementBackend(id, retrievalClass string, egress float64) vaultic.PlacementBackend {
	return vaultic.PlacementBackend{ID: id, RetrievalClass: retrievalClass, PricePerGBEgress: egress}
}

func TestPlacementReadFallsBackWhenPreferredBackendFails(t *testing.T) {
	missing := mem.New()
	fallback := mem.New()
	handle := backend.Handle{Type: backend.PackFile, Name: "pack"}
	if err := fallback.Save(context.Background(), handle, backend.NewByteReader([]byte("fallback"), fallback.Hasher())); err != nil {
		t.Fatal(err)
	}
	candidates := []placementReadCandidate{{backend: missing}, {backend: fallback}}
	var loaded []byte
	err := loadPackFromCandidates(context.Background(), candidates, handle, 0, 0, func(reader io.Reader) error {
		var err error
		loaded, err = io.ReadAll(reader)
		return err
	})
	if err != nil || string(loaded) != "fallback" {
		t.Fatalf("loaded = %q, err=%v", loaded, err)
	}
}

func TestAuthorizedPlacementReadCandidatesFiltersUnauthorizedLeaseBackends(t *testing.T) {
	candidates := []placementReadCandidate{
		{
			backend: fixedReadAuthorizationBackend{Backend: mem.New(), authorized: false},
			policy:  PlacementBackend{PlacementBackend: vaultic.PlacementBackend{ID: "expired"}},
		},
		{
			backend: fixedReadAuthorizationBackend{Backend: mem.New(), authorized: true},
			policy:  PlacementBackend{PlacementBackend: vaultic.PlacementBackend{ID: "live"}},
		},
		{backend: mem.New(), policy: PlacementBackend{PlacementBackend: vaultic.PlacementBackend{ID: "static"}}},
	}

	filtered := authorizedPlacementReadCandidates(candidates)
	if len(filtered) != 2 {
		t.Fatalf("expected two authorized candidates, got %d", len(filtered))
	}
	if filtered[0].policy.ID == "expired" || filtered[1].policy.ID == "expired" {
		t.Fatal("expired lease candidate should be filtered")
	}
}

func TestPlacementReadCandidatesPropagatesMetadataReadFailure(t *testing.T) {
	repo, _, _, packID, _ := promotionTestRepository(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	candidates, err := repo.placementReadCandidates(ctx, packID)
	if err == nil {
		t.Fatalf("placementReadCandidates() = %+v, nil; want metadata read error", candidates)
	}
	if candidates != nil {
		t.Fatalf("placementReadCandidates() returned candidates on metadata error: %+v", candidates)
	}
}

func TestPlacementFallbackWithoutMetadataDoesNotAuthorizeCache(t *testing.T) {
	fallback := placementReadCandidate{backend: mem.New()}
	if placementCandidatesAuthorizeCache([]placementReadCandidate{fallback}) {
		t.Fatal("fallback without placement metadata authorized cache data")
	}
	fallback.metadataConfirmed = true
	if !placementCandidatesAuthorizeCache([]placementReadCandidate{fallback}) {
		t.Fatal("metadata-confirmed placement did not authorize cache data")
	}
}
