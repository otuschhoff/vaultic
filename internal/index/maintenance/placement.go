package maintenance

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type PlacementBackend struct {
	ID                   string
	Hash                 uint64
	Role                 string
	Offsite              bool
	FailureDomain        string
	RetrievalClass       string
	PricePerGBEgress     float64
	MinRetentionSeconds  uint64
	MaxBandwidthBytes    uint64
	MaxRequestsPerSecond uint64
}

type DurabilityPolicy struct {
	MinCopies                 uint
	MinDomains                uint
	MinOffsite                uint
	OffsiteDeadline           int64
	PromotionCrossoverSeconds int64
}

type PlacementModel struct {
	Backends []PlacementBackend
	Policy   DurabilityPolicy
}

type placementSet map[uint64]schema.PlacementRecord

type BackendPlacementCount struct {
	Objects uint64
	Bytes   uint64
}

func BackendPlacementCounts(ctx context.Context, store Store, model PlacementModel) (map[string]BackendPlacementCount, error) {
	placements, _, err := loadPlacements(ctx, store)
	if err != nil {
		return nil, err
	}
	backendByHash := map[uint64]PlacementBackend{}
	for _, backend := range model.Backends {
		backendByHash[backend.Hash] = backend
	}
	counts := map[string]BackendPlacementCount{}
	for _, packPlacements := range placements {
		for backendHash, placement := range packPlacements {
			if !placementShouldExistOnBackend(placement.State) {
				continue
			}
			backend := fmt.Sprintf("%016x", backendHash)
			if resolved, ok := backendByHash[backendHash]; ok {
				backend = resolved.ID
			}
			count := counts[backend]
			count.Objects++
			count.Bytes += placement.Bytes
			counts[backend] = count
		}
	}
	return counts, nil
}

func ExpectedBackendPackIDs(ctx context.Context, store Store, model PlacementModel) (map[string]struct{}, uint64, error) {
	placements, _, err := loadPlacements(ctx, store)
	if err != nil {
		return nil, 0, err
	}
	backendByHash := map[uint64]PlacementBackend{}
	for _, backend := range model.Backends {
		backendByHash[backend.Hash] = backend
	}
	known := map[string]struct{}{}
	var expectedAbsent uint64
	for packID, packPlacements := range placements {
		for backendHash, placement := range packPlacements {
			if !placementShouldExistOnBackend(placement.State) {
				expectedAbsent++
				continue
			}
			if len(model.Backends) != 0 {
				if _, ok := backendByHash[backendHash]; !ok {
					continue
				}
			}
			known[packID.String()] = struct{}{}
		}
	}
	return known, expectedAbsent, nil
}

func placementShouldExistOnBackend(state schema.PlacementState) bool {
	return state == schema.PlacementLive || state == schema.PlacementEvicting
}

func loadPlacements(ctx context.Context, store Store) (map[vaultic.ID]placementSet, uint64, error) {
	placements := map[vaultic.ID]placementSet{}
	var malformed uint64
	err := scan(ctx, store, []byte("pl:"), func(entry daemon.KeyValue) error {
		parsed, parseErr := schema.ParseKey(entry.Key)
		if parseErr != nil || parsed.Kind != schema.KeyPackPlacement {
			malformed++
			return nil
		}
		record, decodeErr := schema.UnmarshalPlacementRecord(entry.Value)
		if decodeErr != nil {
			malformed++
			return nil
		}
		packID := vaultic.ID(parsed.ID)
		if placements[packID] == nil {
			placements[packID] = placementSet{}
		}
		placements[packID][parsed.Backend] = record
		return nil
	})
	return placements, malformed, err
}

func expectedBackendPackMutations(placements map[vaultic.ID]placementSet) ([]daemon.Mutation, error) {
	keys := make([]string, 0)
	values := map[string][]byte{}
	for packID, packPlacements := range placements {
		for backend, placement := range packPlacements {
			value, err := (schema.BackendPackRecord{
				State: placement.State, Bytes: placement.Bytes, PlacedAt: placement.PlacedAt,
			}).MarshalBinary()
			if err != nil {
				return nil, err
			}
			key := schema.BackendPackKey(backend, schema.ID(packID))
			keys = append(keys, string(key))
			values[string(key)] = value
		}
	}
	sort.Strings(keys)
	mutations := make([]daemon.Mutation, 0, len(keys))
	for _, key := range keys {
		mutations = append(mutations, daemon.Mutation{Key: []byte(key), Value: values[key]})
	}
	return mutations, nil
}

func RebuildBackendPackIndex(ctx context.Context, store Store, dryRun bool) (uint64, error) {
	placements, _, err := loadPlacements(ctx, store)
	if err != nil {
		return 0, err
	}
	expected, err := expectedBackendPackMutations(placements)
	if err != nil {
		return 0, err
	}
	expectedKeys := make(map[string]daemon.Mutation, len(expected))
	for _, mutation := range expected {
		expectedKeys[string(mutation.Key)] = mutation
	}
	deletes := make([][]byte, 0)
	if err := scan(ctx, store, []byte("bp:"), func(entry daemon.KeyValue) error {
		if _, ok := expectedKeys[string(entry.Key)]; !ok {
			deletes = append(deletes, append([]byte(nil), entry.Key...))
		}
		return nil
	}); err != nil {
		return 0, err
	}
	var changes uint64
	for _, mutation := range expected {
		value, found, getErr := store.Get(ctx, mutation.Key)
		if getErr != nil {
			return 0, getErr
		}
		if !found || !bytes.Equal(value, mutation.Value) {
			changes++
		}
	}
	changes += uint64(len(deletes))
	if dryRun || changes == 0 {
		return changes, nil
	}
	return changes, store.WriteMutableBatch(ctx, expected, deletes, false)
}

func RebuildDerivedTierSummary(ctx context.Context, store Store, model PlacementModel, dryRun bool) (uint64, error) {
	packs, err := loadPacks(ctx, store)
	if err != nil {
		return 0, err
	}
	placements, _, err := loadPlacements(ctx, store)
	if err != nil {
		return 0, err
	}
	backendByHash := map[uint64]PlacementBackend{}
	for _, backend := range model.Backends {
		backendByHash[backend.Hash] = backend
	}
	puts := make([]daemon.Mutation, 0)
	for packID, pack := range packs {
		if len(placements[packID]) == 0 {
			continue
		}
		derived := derivedTier(placements[packID], backendByHash, len(model.Backends))
		if pack.Tier == derived {
			continue
		}
		pack.Tier = derived
		encoded, encodeErr := pack.MarshalBinary()
		if encodeErr != nil {
			return 0, encodeErr
		}
		puts = append(puts, daemon.Mutation{Key: schema.PackKey(schema.ID(packID)), Value: encoded})
	}
	if dryRun || len(puts) == 0 {
		return uint64(len(puts)), nil
	}
	return uint64(len(puts)), store.WriteMutableBatch(ctx, puts, nil, false)
}

func RebuildPlacementRecords(ctx context.Context, store Store, model PlacementModel, dryRun bool) (uint64, error) {
	packs, err := loadPacks(ctx, store)
	if err != nil {
		return 0, err
	}
	placements, _, err := loadPlacements(ctx, store)
	if err != nil {
		return 0, err
	}
	puts := make([]daemon.Mutation, 0)
	for packID, pack := range packs {
		if len(placements[packID]) != 0 {
			continue
		}
		migrated := placementsFromTier(pack, model)
		if len(migrated) == 0 {
			continue
		}
		mutations, mutationErr := placementMutationsForMaintenance(schema.ID(packID), migrated)
		if mutationErr != nil {
			return 0, mutationErr
		}
		puts = append(puts, mutations...)
	}
	if dryRun || len(puts) == 0 {
		return uint64(len(puts) / 2), nil
	}
	return uint64(len(puts) / 2), store.WriteMutableBatch(ctx, puts, nil, false)
}

func placementMutationsForMaintenance(packID schema.ID, placements placementSet) ([]daemon.Mutation, error) {
	puts := make([]daemon.Mutation, 0, 2*len(placements))
	for backend, placement := range placements {
		value, err := placement.MarshalBinary()
		if err != nil {
			return nil, err
		}
		puts = append(puts, daemon.Mutation{Key: schema.PackPlacementKey(packID, backend), Value: value})
		reverse, err := (schema.BackendPackRecord{State: placement.State, Bytes: placement.Bytes, PlacedAt: placement.PlacedAt}).MarshalBinary()
		if err != nil {
			return nil, err
		}
		puts = append(puts, daemon.Mutation{Key: schema.BackendPackKey(backend, packID), Value: reverse})
	}
	return puts, nil
}

func placementsFromTier(pack schema.PackRecord, model PlacementModel) placementSet {
	if pack.Tier == schema.TierUnknown || len(model.Backends) == 0 {
		return nil
	}
	selected := make([]PlacementBackend, 0, len(model.Backends))
	switch pack.Tier {
	case schema.TierSingle:
		if backend, ok := model.backendByRole("primary"); ok {
			selected = append(selected, backend)
		} else {
			selected = append(selected, model.Backends[0])
		}
	case schema.TierHot:
		if backend, ok := model.backendByRole("primary"); ok {
			selected = append(selected, backend)
		}
	case schema.TierCold:
		if backend, ok := model.backendByRole("archival"); ok {
			selected = append(selected, backend)
		} else {
			selected = append(selected, model.Backends[len(model.Backends)-1])
		}
	case schema.TierMirrored:
		if backend, ok := model.backendByRole("primary"); ok {
			selected = append(selected, backend)
		}
		if backend, ok := model.backendByRole("archival"); ok {
			selected = append(selected, backend)
		}
	}
	result := placementSet{}
	for _, backend := range selected {
		if backend.Hash == 0 {
			continue
		}
		placement := schema.PlacementRecord{
			State: schema.PlacementLive, PlacedAt: pack.CreationTime,
			PlacementTimeKnown: pack.CreationTimeKnown, Bytes: pack.PhysicalSize,
			StorageClass:      pack.StorageClass,
			MinRetentionUntil: pack.MinRetentionUntil,
			RetentionSource:   pack.RetentionSource,
		}
		if placement.RetentionSource == 0 {
			placement.RetentionSource = schema.RetentionUnknown
		}
		result[backend.Hash] = placement
	}
	return result
}

func checkPlacementRecords(ctx context.Context, store Store, packs map[vaultic.ID]schema.PackRecord, model PlacementModel, result *CheckResult, maxFindings uint) error {
	placements, malformed, err := loadPlacements(ctx, store)
	if err != nil {
		return err
	}
	result.PlacementRecordsMalformed = malformed
	backendByHash := map[uint64]PlacementBackend{}
	for _, backend := range model.Backends {
		backendByHash[backend.Hash] = backend
	}
	for packID, pack := range packs {
		if len(model.Backends) != 0 && len(placements[packID]) == 0 && pack.Tier != schema.TierUnknown {
			result.MissingPlacementRecords++
			addFinding(result, maxFindings, Finding{Kind: "missing_placement_records", Key: packID.String(), Got: pack.Tier.String()})
		}
	}
	for packID, packPlacements := range placements {
		for backend, placement := range packPlacements {
			value, found, getErr := store.Get(ctx, schema.BackendPackKey(backend, schema.ID(packID)))
			if getErr != nil {
				return getErr
			}
			expected, encodeErr := (schema.BackendPackRecord{State: placement.State, Bytes: placement.Bytes, PlacedAt: placement.PlacedAt}).MarshalBinary()
			if encodeErr != nil {
				return encodeErr
			}
			if !found || !bytes.Equal(value, expected) {
				result.BackendPackMismatch++
				addFinding(result, maxFindings, Finding{Kind: "backend_pack_mismatch", Key: packID.String(), Want: fmt.Sprintf("backend=%016x", backend)})
			}
			if _, ok := backendByHash[backend]; len(model.Backends) != 0 && !ok {
				result.UnknownPlacementBackends++
				addFinding(result, maxFindings, Finding{Kind: "unknown_placement_backend", Key: packID.String(), Got: fmt.Sprintf("backend=%016x", backend)})
			}
		}
		pack, found := packs[packID]
		if !found {
			continue
		}
		derived := derivedTier(packPlacements, backendByHash, len(model.Backends))
		if pack.Tier != derived {
			result.DerivedTierMismatch++
			addFinding(result, maxFindings, Finding{Kind: "derived_tier_mismatch", Key: packID.String(), Want: derived.String(), Got: pack.Tier.String()})
		}
		if !durable(packPlacements, backendByHash, model.Policy) {
			result.PacksBelowDurability++
			addFinding(result, maxFindings, Finding{Kind: "below_durability", Key: packID.String()})
		}
	}
	if err := scan(ctx, store, []byte("bp:"), func(entry daemon.KeyValue) error {
		parsed, parseErr := schema.ParseKey(entry.Key)
		if parseErr != nil || parsed.Kind != schema.KeyBackendPack {
			result.BackendPackMismatch++
			return nil
		}
		packID := vaultic.ID(parsed.ID)
		if placements[packID] == nil {
			result.BackendPackMismatch++
			addFinding(result, maxFindings, Finding{Kind: "orphan_backend_pack", Key: packID.String(), Got: fmt.Sprintf("backend=%016x", parsed.Backend)})
			return nil
		}
		if _, ok := placements[packID][parsed.Backend]; !ok {
			result.BackendPackMismatch++
			addFinding(result, maxFindings, Finding{Kind: "orphan_backend_pack", Key: packID.String(), Got: fmt.Sprintf("backend=%016x", parsed.Backend)})
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (model PlacementModel) backendByRole(role string) (PlacementBackend, bool) {
	for _, backend := range model.Backends {
		if backend.Role == role {
			return backend, true
		}
	}
	return PlacementBackend{}, false
}

func derivedTier(placements placementSet, backends map[uint64]PlacementBackend, backendCount int) schema.PackTier {
	live := make([]PlacementBackend, 0, len(placements))
	unknownLive := 0
	for backend, placement := range placements {
		if placement.State != schema.PlacementLive {
			continue
		}
		resolved, ok := backends[backend]
		if !ok {
			unknownLive++
			continue
		}
		live = append(live, resolved)
	}
	if len(live) == 0 && unknownLive == 0 {
		return schema.TierUnknown
	}
	if len(live)+unknownLive > 1 {
		return schema.TierMirrored
	}
	if unknownLive == 1 {
		if backendCount == 1 {
			return schema.TierSingle
		}
		return schema.TierUnknown
	}
	switch live[0].Role {
	case "archival":
		return schema.TierCold
	case "primary":
		if backendCount == 1 {
			return schema.TierSingle
		}
		return schema.TierHot
	default:
		return schema.TierUnknown
	}
}

func durable(placements placementSet, backends map[uint64]PlacementBackend, policy DurabilityPolicy) bool {
	minCopies := policy.MinCopies
	if minCopies == 0 {
		minCopies = 1
	}
	minDomains := policy.MinDomains
	if minDomains == 0 {
		minDomains = minCopies
	}
	domains := map[string]struct{}{}
	var copies, offsite uint
	for backendHash, placement := range placements {
		if placement.State != schema.PlacementLive {
			continue
		}
		backend, ok := backends[backendHash]
		if !ok {
			continue
		}
		copies++
		domains[backend.FailureDomain] = struct{}{}
		if backend.Offsite {
			offsite++
		}
	}
	return copies >= minCopies && uint(len(domains)) >= minDomains && offsite >= policy.MinOffsite
}
