package verification

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type memoryStore struct {
	values   map[string][]byte
	outcomes []daemon.VerificationOutcome
}

func (store *memoryStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	value, found := store.values[string(key)]
	return append([]byte(nil), value...), found, nil
}

func (store *memoryStore) ScanPrefix(_ context.Context, prefix, after []byte, limit uint32) ([]daemon.KeyValue, bool, error) {
	var keys [][]byte
	for key := range store.values {
		if bytes.HasPrefix([]byte(key), prefix) && (len(after) == 0 || bytes.Compare([]byte(key), after) > 0) {
			keys = append(keys, []byte(key))
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	done := len(keys) <= int(limit)
	if !done {
		keys = keys[:limit]
	}
	items := make([]daemon.KeyValue, len(keys))
	for index, key := range keys {
		items[index] = daemon.KeyValue{Key: key, Value: store.values[string(key)]}
	}
	return items, done, nil
}

func (store *memoryStore) RecordVerification(_ context.Context, outcome daemon.VerificationOutcome) error {
	store.outcomes = append(store.outcomes, outcome)
	return nil
}

func TestPlanDeterministicFiltersAndFreshness(t *testing.T) {
	store := &memoryStore{values: map[string][]byte{}}
	for number := byte(1); number <= 10; number++ {
		var id schema.ID
		id[0] = number
		pack := schema.PackRecord{
			Type:              schema.PackData,
			PhysicalSize:      100,
			PhysicalSizeKnown: true,
			PayloadSize:       90,
			HeaderSize:        10,
			CreationTime:      100,
			CreationTimeKnown: true,
			Lifecycle:         schema.PackPublished,
			Tier:              schema.TierCold,
			RetentionSource:   schema.RetentionUnknown,
		}
		placement := schema.PlacementRecord{
			State:              schema.PlacementLive,
			StorageClass:       "GLACIER",
			PlacedAt:           100,
			PlacementTimeKnown: true,
			Bytes:              100,
			RetentionSource:    schema.RetentionUnknown,
		}
		packValue, _ := pack.MarshalBinary()
		placementValue, _ := placement.MarshalBinary()
		store.values[string(schema.PackKey(id))] = packValue
		store.values[string(schema.PackPlacementKey(id, 7))] = placementValue
	}
	cutoff := int64(200)
	options := Options{
		Level: schema.VerificationChecksum,
		Tiers: map[schema.PackTier]bool{
			schema.TierUnknown:  false,
			schema.TierHot:      false,
			schema.TierCold:     true,
			schema.TierMirrored: false,
			schema.TierSingle:   false,
		},
		Backends:         map[uint64]bool{7: true},
		StorageClasses:   map[string]bool{"GLACIER": true},
		NotVerifiedSince: &cutoff,
		SampleCount:      3,
		Seed:             99,
	}
	first, err := Plan(context.Background(), store, options, time.Unix(300, 0))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Plan(context.Background(), store, options, time.Unix(300, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 || len(second) != 3 {
		t.Fatalf("sample lengths = %d, %d", len(first), len(second))
	}
	for index := range first {
		if first[index].PackID != second[index].PackID {
			t.Fatal("sample is not deterministic")
		}
	}
	state := schema.VerificationStateRecord{
		LastAttemptAt:      250,
		LastAttemptLevel:   schema.VerificationChecksum,
		HeaderVerifiedAt:   250,
		ChecksumVerifiedAt: 250,
		Result:             schema.VerificationHealthy,
		LastRunID:          schema.ID{1},
	}
	value, _ := state.MarshalBinary()
	store.values[string(schema.VerificationStateKey(first[0].PackID, 7))] = value
	allOptions := options
	allOptions.SampleCount = 0
	filtered, err := Plan(context.Background(), store, allOptions, time.Unix(300, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 9 {
		t.Fatalf("stale candidates = %d, want 9", len(filtered))
	}
}

func TestPlanPhysicalSizeFiltersExcludeUnknownAndOutOfRange(t *testing.T) {
	store := &memoryStore{values: map[string][]byte{}}
	for number, size := range []uint64{0, 99, 100, 150, 151} {
		id := schema.ID{byte(number + 1)}
		pack := schema.PackRecord{Type: schema.PackData, Lifecycle: schema.PackPublished}
		if number != 0 {
			pack.PhysicalSize, pack.PayloadSize, pack.HeaderSize, pack.PhysicalSizeKnown = size, size-10, 10, true
		}
		placement := schema.PlacementRecord{State: schema.PlacementLive, Bytes: max(size, 1), RetentionSource: schema.RetentionUnknown}
		packValue, err := pack.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		placementValue, err := placement.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		store.values[string(schema.PackKey(id))] = packValue
		store.values[string(schema.PackPlacementKey(id, 7))] = placementValue
	}
	minimum, maximum := uint64(100), uint64(150)
	candidates, err := Plan(context.Background(), store, Options{Level: schema.VerificationHeader, MinSize: &minimum, MaxSize: &maximum}, time.Unix(1, 0))
	if err != nil || len(candidates) != 2 || candidates[0].Pack.PhysicalSize != 100 || candidates[1].Pack.PhysicalSize != 150 {
		t.Fatalf("size-filtered candidates = %+v, %v", candidates, err)
	}
}

type failingWarmer struct{ err error }

func (warmer failingWarmer) WarmupPlacements(context.Context, []Candidate) error { return warmer.err }

func TestRunRecordsColdWarmupFailures(t *testing.T) {
	store := &memoryStore{values: map[string][]byte{}}
	candidates := []Candidate{{PackID: schema.ID{1}, Backend: 7}, {PackID: schema.ID{2}, Backend: 8}}
	result, err := Run(
		context.Background(),
		store,
		nil,
		failingWarmer{err: context.DeadlineExceeded},
		candidates,
		schema.VerificationChecksum,
		1,
		func() time.Time { return time.Unix(100, 0) },
	)
	if err == nil || result.OperationalErrors != 2 || len(store.outcomes) != 2 {
		t.Fatalf("warm-up result = %+v, outcomes=%+v, err=%v", result, store.outcomes, err)
	}
	for _, outcome := range store.outcomes {
		if outcome.Classification != schema.VerificationWarmupTimeout || outcome.RunID == (schema.ID{}) {
			t.Fatalf("unexpected warm-up outcome %+v", outcome)
		}
	}
	if store.outcomes[0].RunID == store.outcomes[1].RunID {
		t.Fatal("warm-up outcomes reused a run ID")
	}
}
