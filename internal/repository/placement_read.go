package repository

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/otuschhoff/vaultic/internal/backend"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type placementReadCandidate struct {
	backend backend.Backend
	policy  PlacementBackend
}

func (r *Repository) placementReadCandidates(ctx context.Context, packID vaultic.ID) []placementReadCandidate {
	engine, ok := r.Engine().(*metadataindex.DaemonEngine)
	if !ok {
		return []placementReadCandidate{{backend: r.be}}
	}
	model, err := r.PlacementModel()
	if err != nil {
		return []placementReadCandidate{{backend: r.be}}
	}
	candidates := make([]placementReadCandidate, 0, len(model.Backends))
	observedPlacement := false
	for _, policy := range model.Backends {
		value, found, err := engine.SchemaStore().Get(ctx, schema.PackPlacementKey(schema.ID(packID), policy.Hash))
		if err != nil || !found {
			continue
		}
		observedPlacement = true
		if !policy.ReadAllowed() {
			continue
		}
		placement, err := schema.UnmarshalPlacementRecord(value)
		if err != nil || placement.State != schema.PlacementLive {
			continue
		}
		candidate, found := r.backendForPlacement(model, policy)
		if !found {
			continue
		}
		candidates = append(candidates, placementReadCandidate{backend: candidate, policy: policy})
	}
	sortPlacementReadCandidates(candidates)
	if len(candidates) == 0 {
		if observedPlacement {
			return nil
		}
		return []placementReadCandidate{{backend: r.be}}
	}
	return candidates
}

func sortPlacementReadCandidates(candidates []placementReadCandidate) {
	sort.SliceStable(candidates, func(left, right int) bool {
		leftRank, rightRank := retrievalClassRank(candidates[left].policy.RetrievalClass), retrievalClassRank(candidates[right].policy.RetrievalClass)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		if candidates[left].policy.PricePerGBEgress != candidates[right].policy.PricePerGBEgress {
			return candidates[left].policy.PricePerGBEgress < candidates[right].policy.PricePerGBEgress
		}
		return candidates[left].policy.ID < candidates[right].policy.ID
	})
}

func retrievalClassRank(class string) int {
	switch strings.ToLower(class) {
	case "", "immediate", "hot", "standard":
		return 0
	case "warm", "cool", "infrequent-access", "infrequent_access":
		return 1
	case "hours", "cold", "archive":
		return 2
	case "deep-archive", "deep_archive":
		return 3
	default:
		return 2
	}
}

func (r *Repository) loadPackFromPlacements(ctx context.Context, handle backend.Handle, length int, offset int64, fn func(io.Reader) error) error {
	packID, err := vaultic.ParseID(handle.Name)
	if err != nil {
		return r.be.Load(ctx, handle, length, offset, fn)
	}
	return loadPackFromCandidates(ctx, r.placementReadCandidates(ctx, packID), handle, length, offset, fn)
}

func loadPackFromCandidates(
	ctx context.Context,
	candidates []placementReadCandidate,
	handle backend.Handle,
	length int,
	offset int64,
	fn func(io.Reader) error,
) error {
	var lastErr error
	for _, candidate := range candidates {
		warming, err := candidate.backend.Warmup(ctx, []backend.Handle{handle})
		if err == nil && len(warming) != 0 {
			err = candidate.backend.WarmupWait(ctx, warming)
		}
		if err != nil {
			lastErr = err
			continue
		}
		if err := candidate.backend.Load(ctx, handle, length, offset, fn); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if lastErr == nil {
		return fmt.Errorf("no readable placement for pack %s", handle.Name)
	}
	return lastErr
}

func (r *Repository) readPackAtFromPlacements(ctx context.Context, handle backend.Handle, offset int64, buffer []byte) (int, error) {
	read := 0
	err := r.loadPackFromPlacements(ctx, handle, len(buffer), offset, func(reader io.Reader) error {
		var err error
		read, err = io.ReadFull(reader, buffer)
		return err
	})
	return read, err
}
