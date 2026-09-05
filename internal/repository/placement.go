package repository

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const (
	PlacementRoleMetadata = "metadata"
	PlacementRolePrimary  = "primary"
	PlacementRoleArchival = "archival"
	PlacementRoleCache    = "cache"
)

type PlacementBackend struct {
	vaultic.PlacementBackend
	Hash uint64
}

type PlacementModel struct {
	Backends []PlacementBackend
	Policy   vaultic.PlacementPolicy
	HotCold  bool
}

func PlacementBackendHash(id string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(id)) // hash.Hash writes are specified to return a nil error.
	value := hash.Sum64()
	if value == 0 {
		return 1
	}
	return value
}

func (r *Repository) PlacementModel() (PlacementModel, error) {
	cfg := r.Config()
	if len(cfg.PlacementBackends) > 0 {
		return placementModelFromConfig(cfg.PlacementBackends, cfg.PlacementPolicy, false)
	}

	_, _, hotCold := r.HotCold()
	if hotCold {
		return placementModelFromConfig([]vaultic.PlacementBackend{
			{ID: "hot", Role: PlacementRolePrimary, FailureDomain: "hot"},
			{ID: "cold", Role: PlacementRoleArchival, Offsite: true, FailureDomain: "cold"},
		}, cfg.PlacementPolicy, true)
	}
	return placementModelFromConfig([]vaultic.PlacementBackend{{
		ID: "single", Role: PlacementRolePrimary, FailureDomain: "single",
	}}, cfg.PlacementPolicy, false)
}

func placementModelFromConfig(entries []vaultic.PlacementBackend, policy vaultic.PlacementPolicy, hotCold bool) (PlacementModel, error) {
	if len(entries) == 0 {
		return PlacementModel{}, fmt.Errorf("placement model requires at least one backend")
	}
	seen := map[string]struct{}{}
	backends := make([]PlacementBackend, 0, len(entries))
	for _, entry := range entries {
		if entry.ID == "" {
			return PlacementModel{}, fmt.Errorf("placement backend id must not be empty")
		}
		if _, ok := seen[entry.ID]; ok {
			return PlacementModel{}, fmt.Errorf("duplicate placement backend id %q", entry.ID)
		}
		seen[entry.ID] = struct{}{}
		if entry.Role == "" {
			entry.Role = PlacementRolePrimary
		}
		if entry.FailureDomain == "" {
			entry.FailureDomain = entry.ID
		}
		backends = append(backends, PlacementBackend{PlacementBackend: entry, Hash: PlacementBackendHash(entry.ID)})
	}
	if len(entries) > 0 {
		var ingesting bool
		for _, backend := range backends {
			if backend.IngestEnabled() {
				ingesting = true
				break
			}
		}
		if !ingesting {
			return PlacementModel{}, fmt.Errorf("placement model requires at least one ingest-enabled backend")
		}
	}
	if policy.MinCopies == 0 {
		policy.MinCopies = 1
	}
	if policy.MinDomains == 0 {
		policy.MinDomains = min(policy.MinCopies, uint(len(backends)))
	}
	if policy.MinOffsite > policy.MinCopies {
		return PlacementModel{}, fmt.Errorf("placement min_offsite cannot exceed min_copies")
	}
	return PlacementModel{Backends: backends, Policy: policy, HotCold: hotCold}, nil
}

func (model PlacementModel) BackendByRole(role string) (PlacementBackend, bool) {
	for _, backend := range model.Backends {
		if backend.Role == role {
			return backend, true
		}
	}
	return PlacementBackend{}, false
}

func (model PlacementModel) BackendByIDHash(hash uint64) (PlacementBackend, bool) {
	for _, backend := range model.Backends {
		if backend.Hash == hash {
			return backend, true
		}
	}
	return PlacementBackend{}, false
}

func (backend PlacementBackend) MinRetention() time.Duration {
	return time.Duration(backend.MinRetentionSeconds) * time.Second
}

func (backend PlacementBackend) IngestEnabled() bool {
	return backend.Ingest == nil || *backend.Ingest
}

func (backend PlacementBackend) ReadAllowed() bool {
	return backend.ReadEnabled == nil || *backend.ReadEnabled
}
