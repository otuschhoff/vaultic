package maintenance

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type placementActionCall struct {
	operation schema.PlacementRequestOperation
	packID    vaultic.ID
	backend   uint64
}

type fakePlacementActions struct {
	calls    []placementActionCall
	failures map[vaultic.ID]error
}

func (actions *fakePlacementActions) call(operation schema.PlacementRequestOperation, packID vaultic.ID, backend PlacementBackend) error {
	actions.calls = append(actions.calls, placementActionCall{operation: operation, packID: packID, backend: backend.Hash})
	if err := actions.failures[packID]; err != nil {
		return err
	}
	return nil
}

func (actions *fakePlacementActions) Place(_ context.Context, packID vaultic.ID, backend PlacementBackend) error {
	return actions.call(schema.PlacementRequestPlace, packID, backend)
}

func (actions *fakePlacementActions) Promote(_ context.Context, packID vaultic.ID, backend PlacementBackend) error {
	return actions.call(schema.PlacementRequestPromote, packID, backend)
}

func (actions *fakePlacementActions) Evict(_ context.Context, packID vaultic.ID, backend PlacementBackend) error {
	return actions.call(schema.PlacementRequestEvict, packID, backend)
}

func TestPlacementPlanQueuesUnsatisfiedOffsiteDeadline(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	packID := deterministicID(200)
	now := time.Unix(1_700_000_000, 0)
	store.set(t, schema.PackKey(schema.ID(packID)), schema.PackRecord{
		Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
		PayloadSize: 80, HeaderSize: 20, BlobCount: 1,
		CreationTime: now.Add(-2 * time.Hour).UnixNano(), CreationTimeKnown: true,
		Lifecycle: schema.PackPublished, Tier: schema.TierHot, RetentionSource: schema.RetentionUnknown,
	})
	store.set(t, schema.PackPlacementKey(schema.ID(packID), testPrimaryBackend), schema.PlacementRecord{
		State: schema.PlacementLive, Bytes: 100, PlacedAt: now.Add(-2 * time.Hour).UnixNano(), PlacementTimeKnown: true,
		RetentionSource: schema.RetentionUnknown,
	})
	model := testPlacementModel()
	model.Backends = append(model.Backends, PlacementBackend{ID: "warm", Hash: 3, Role: "primary", Offsite: true, FailureDomain: "warm"})
	model.Policy.OffsiteDeadline = int64(time.Hour / time.Second)
	result, err := PlanPlacement(context.Background(), store, PlacementSchedulerOptions{Model: model, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.Unsatisfied != 1 || result.Overdue != 1 || result.RequestsWritten != 1 {
		t.Fatalf("placement result = %#v", result)
	}
	value, found, err := store.Get(
		context.Background(),
		schema.PlacementRequestKey(uint64(result.OldestUnsatisfiedDeadline/int64(time.Second)), schema.ID(packID)),
	)
	if err != nil || !found {
		t.Fatalf("rq record missing: found=%v err=%v", found, err)
	}
	request, err := schema.UnmarshalPlacementRequestRecord(value)
	if err != nil || request.Operation != schema.PlacementRequestPlace || request.TargetBackend != 3 {
		t.Fatalf("request = %#v, err = %v", request, err)
	}
	writes := store.batchWrites
	result, err = PlanPlacement(context.Background(), store, PlacementSchedulerOptions{Model: model, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequestsWritten != 0 || store.batchWrites != writes {
		t.Fatalf("unchanged plan rewrote queue: result=%#v writes=%d want=%d", result, store.batchWrites, writes)
	}
}

func TestPlacementPlanSkipsReadOnlyLegacyBackend(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	packID := deterministicID(220)
	now := time.Unix(1_700_000_000, 0)
	store.set(t, schema.PackKey(schema.ID(packID)), schema.PackRecord{
		Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
		PayloadSize: 80, HeaderSize: 20, BlobCount: 1,
		CreationTime: now.UnixNano(), CreationTimeKnown: true,
		Lifecycle: schema.PackPublished, Tier: schema.TierCold, RetentionSource: schema.RetentionUnknown,
	})
	readOnly := false
	model := PlacementModel{
		Backends: []PlacementBackend{
			{ID: "legacy", Hash: 1, Role: "primary", Ingest: &readOnly, ReadEnabled: boolPtr(true), Offsite: true, FailureDomain: "old-cloud"},
			{ID: "active", Hash: 2, Role: "primary", Offsite: true, FailureDomain: "new-cloud"},
		},
		Policy: DurabilityPolicy{MinCopies: 1, MinDomains: 1, MinOffsite: 1},
	}
	result, err := PlanPlacement(context.Background(), store, PlacementSchedulerOptions{Model: model, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequestsWritten != 1 {
		t.Fatalf("requests written = %d, want 1", result.RequestsWritten)
	}
	value, found, err := store.Get(context.Background(), schema.PlacementRequestKey(uint64(now.Unix()), schema.ID(packID)))
	if err != nil || !found {
		t.Fatalf("placement request missing: found=%v err=%v", found, err)
	}
	request, err := schema.UnmarshalPlacementRequestRecord(value)
	if err != nil || request.TargetBackend != 2 {
		t.Fatalf("request target = %016x err=%v, want active backend", request.TargetBackend, err)
	}
}

func TestPlanPoolMigrationQueuesCopiesToActiveTarget(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	packID := deterministicID(221)
	now := time.Unix(1_700_000_000, 0)
	storePlacementPack(t, store, packID, schema.TierCold)
	store.set(
		t,
		schema.PackPlacementKey(schema.ID(packID), 1),
		schema.PlacementRecord{State: schema.PlacementLive, Bytes: 100, RetentionSource: schema.RetentionUnknown},
	)
	readOnly := false
	model := PlacementModel{Backends: []PlacementBackend{
		{ID: "legacy", Hash: 1, Ingest: &readOnly, ReadEnabled: boolPtr(true)},
		{ID: "active", Hash: 2},
	}}
	result, err := PlanPoolMigration(context.Background(), store, PlacementMigrationOptions{Model: model, From: "legacy", To: "active", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequestsWritten != 1 || result.PacksScanned != 1 {
		t.Fatalf("migration result = %#v", result)
	}
	value, found, err := store.Get(context.Background(), schema.PlacementRequestKey(uint64(now.Unix()), schema.ID(packID)))
	if err != nil || !found {
		t.Fatalf("migration request missing: found=%v err=%v", found, err)
	}
	request, err := schema.UnmarshalPlacementRequestRecord(value)
	if err != nil || request.Operation != schema.PlacementRequestPlace || request.TargetBackend != 2 {
		t.Fatalf("migration request = %#v err=%v", request, err)
	}
}

func TestPlanPoolMigrationIgnoresStalePlacementWithoutLivePack(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	packID := deterministicID(222)
	store.set(
		t,
		schema.PackPlacementKey(schema.ID(packID), 1),
		schema.PlacementRecord{State: schema.PlacementLive, Bytes: 100, RetentionSource: schema.RetentionUnknown},
	)
	readOnly := false
	model := PlacementModel{Backends: []PlacementBackend{
		{ID: "legacy", Hash: 1, Ingest: &readOnly, ReadEnabled: boolPtr(true)},
		{ID: "active", Hash: 2},
	}}
	result, err := PlanPoolMigration(context.Background(), store, PlacementMigrationOptions{Model: model, From: "legacy", To: "active"})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequestsWritten != 0 {
		t.Fatalf("stale placement queued migration work: %#v", result)
	}
}

func TestPlanPoolMigrationRejectsNonIngestTarget(t *testing.T) {
	readOnly := false
	model := PlacementModel{Backends: []PlacementBackend{
		{ID: "legacy", Hash: 1, Ingest: &readOnly, ReadEnabled: boolPtr(true)},
		{ID: "inactive", Hash: 2, Ingest: &readOnly, ReadEnabled: boolPtr(true)},
	}}
	_, err := PlanPoolMigration(
		context.Background(),
		&memoryStore{values: make(map[string][]byte)},
		PlacementMigrationOptions{Model: model, From: "legacy", To: "inactive"},
	)
	if err == nil {
		t.Fatal("migration to non-ingest backend was accepted")
	}
}

func TestPlacementPlanRemovesSatisfiedRequest(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	packID := deterministicID(201)
	now := time.Unix(1_700_000_000, 0)
	store.set(t, schema.PackKey(schema.ID(packID)), schema.PackRecord{
		Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
		PayloadSize: 80, HeaderSize: 20, BlobCount: 1, CreationTime: now.UnixNano(),
		CreationTimeKnown: true, Lifecycle: schema.PackPublished, Tier: schema.TierMirrored,
		RetentionSource: schema.RetentionUnknown,
	})
	model := PlacementModel{
		Backends: []PlacementBackend{
			{ID: "local", Hash: 1, Role: "primary", FailureDomain: "local"},
			{ID: "warm", Hash: 2, Role: "primary", Offsite: true, FailureDomain: "warm"},
		},
		Policy: DurabilityPolicy{MinCopies: 2, MinDomains: 2, MinOffsite: 1},
	}
	for _, backend := range model.Backends {
		store.set(t, schema.PackPlacementKey(schema.ID(packID), backend.Hash), schema.PlacementRecord{
			State: schema.PlacementLive, Bytes: 100, RetentionSource: schema.RetentionUnknown,
		})
	}
	requestKey := schema.PlacementRequestKey(uint64(now.Unix()), schema.ID(packID))
	store.set(t, requestKey, schema.PlacementRequestRecord{
		Classes: []string{"recent-data"}, Operation: schema.PlacementRequestPlace, TargetBackend: 2,
	})
	result, err := PlanPlacement(context.Background(), store, PlacementSchedulerOptions{Model: model, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequestsWritten != 0 {
		t.Fatalf("requests written = %d, want 0", result.RequestsWritten)
	}
	if _, found, err := store.Get(context.Background(), requestKey); err != nil || found {
		t.Fatalf("stale request remains: found=%v err=%v", found, err)
	}
}

func TestSingleBackendPlacementPlanDoesNoSchedulerWork(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	packID := vaultic.NewRandomID()
	now := time.Unix(1_700_000_000, 0)
	store.set(t, schema.PackKey(schema.ID(packID)), schema.PackRecord{
		Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
		PayloadSize: 80, HeaderSize: 20, BlobCount: 1,
		CreationTime: now.UnixNano(), CreationTimeKnown: true,
		Lifecycle: schema.PackPublished, Tier: schema.TierSingle, RetentionSource: schema.RetentionUnknown,
	})
	model := PlacementModel{
		Backends: []PlacementBackend{{ID: "single", Hash: 1, Role: "primary", FailureDomain: "single"}},
		Policy:   DurabilityPolicy{MinCopies: 1, MinDomains: 1},
	}
	store.set(
		t,
		schema.PackPlacementKey(schema.ID(packID), 1),
		schema.PlacementRecord{State: schema.PlacementLive, Bytes: 100, RetentionSource: schema.RetentionUnknown},
	)
	result, err := PlanPlacement(context.Background(), store, PlacementSchedulerOptions{Model: model, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.Unsatisfied != 0 || result.Overdue != 0 || result.RequestsWritten != 0 {
		t.Fatalf("single backend scheduled work: %#v", result)
	}
}

func TestPlacementPlanQueuesObservedSurvivorForPromotion(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	now := time.Unix(1_700_000_000, 0)
	packID := deterministicID(205)
	store.set(t, schema.PackKey(schema.ID(packID)), schema.PackRecord{
		Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
		PayloadSize: 80, HeaderSize: 20, BlobCount: 1,
		CreationTime: now.Add(-9 * 24 * time.Hour).UnixNano(), CreationTimeKnown: true,
		Lifecycle: schema.PackPublished, Tier: schema.TierHot, RetentionSource: schema.RetentionUnknown,
		UsageKnown: true, UsedPayloadBytes: 80,
	})
	model := PlacementModel{
		Backends: []PlacementBackend{
			{ID: "local", Hash: 1, Role: "primary", FailureDomain: "local"},
			{ID: "archive", Hash: 2, Role: "archival", Offsite: true, FailureDomain: "archive"},
		},
		Policy: DurabilityPolicy{MinCopies: 1, MinDomains: 1},
	}
	store.set(t, schema.PackPlacementKey(schema.ID(packID), 1), schema.PlacementRecord{
		State: schema.PlacementLive, Bytes: 100, RetentionSource: schema.RetentionUnknown,
	})
	store.set(t, schema.PromotionEligibilityKey(schema.ID(packID)), schema.PromotionEligibilityRecord{
		EvaluatedAt: now.Add(-time.Hour).UnixNano(), Indefinite: true,
	})
	result, err := PlanPlacement(context.Background(), store, PlacementSchedulerOptions{Model: model, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.PendingPromotion != 1 || result.RequestsWritten != 1 {
		t.Fatalf("result = %#v", result)
	}
	key := schema.PlacementRequestKey(math.MaxUint64-1, schema.ID(packID))
	value, found, err := store.Get(context.Background(), key)
	if err != nil || !found {
		t.Fatalf("promotion request missing: found=%v err=%v", found, err)
	}
	request, err := schema.UnmarshalPlacementRequestRecord(value)
	if err != nil || request.Operation != schema.PlacementRequestPromote || request.TargetBackend != 2 {
		t.Fatalf("promotion request = %#v, err=%v", request, err)
	}
}

func TestPlacementPlanQueuesSafeExcessPlacementLast(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	now := time.Unix(1_700_000_000, 0)
	packID := deterministicID(209)
	store.set(t, schema.PackKey(schema.ID(packID)), schema.PackRecord{
		Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
		PayloadSize: 80, HeaderSize: 20, BlobCount: 1, CreationTime: now.UnixNano(),
		CreationTimeKnown: true, Lifecycle: schema.PackPublished, Tier: schema.TierMirrored,
		RetentionSource: schema.RetentionUnknown,
	})
	model := PlacementModel{
		Backends: []PlacementBackend{
			{ID: "local", Hash: 1, Role: "primary", FailureDomain: "local"},
			{ID: "warm", Hash: 2, Role: "primary", Offsite: true, FailureDomain: "warm"},
			{ID: "cache", Hash: 3, Role: "cache", FailureDomain: "local"},
		},
		Policy: DurabilityPolicy{MinCopies: 2, MinDomains: 2, MinOffsite: 1},
	}
	for _, backend := range model.Backends {
		store.set(t, schema.PackPlacementKey(schema.ID(packID), backend.Hash), schema.PlacementRecord{
			State: schema.PlacementLive, Bytes: 100, RetentionSource: schema.RetentionUnknown,
		})
	}
	result, err := PlanPlacement(context.Background(), store, PlacementSchedulerOptions{Model: model, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequestsWritten != 1 || len(result.Statuses[0].ExcessBackends) != 1 || result.Statuses[0].ExcessBackends[0] != "cache" {
		t.Fatalf("result = %#v", result)
	}
	value, found, err := store.Get(context.Background(), schema.PlacementRequestKey(math.MaxUint64, schema.ID(packID)))
	if err != nil || !found {
		t.Fatalf("eviction request missing: found=%v err=%v", found, err)
	}
	request, err := schema.UnmarshalPlacementRequestRecord(value)
	if err != nil || request.Operation != schema.PlacementRequestEvict || request.TargetBackend != 3 {
		t.Fatalf("eviction request = %#v, err=%v", request, err)
	}
}

func TestPlacementPlanDoesNotPromoteUnknownOrDeadPack(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	model := PlacementModel{
		Backends: []PlacementBackend{{ID: "archive", Hash: 2, Role: "archival", Offsite: true, FailureDomain: "archive"}},
	}
	for name, usage := range map[string]struct {
		known bool
		used  uint64
	}{"unknown": {false, 0}, "dead": {true, 0}} {
		t.Run(name, func(t *testing.T) {
			pack := schema.PackRecord{
				Type:              schema.PackData,
				CreationTimeKnown: true,
				CreationTime:      now.Add(-30 * 24 * time.Hour).UnixNano(),
				UsageKnown:        usage.known,
				UsedPayloadBytes:  usage.used,
			}
			if archivalPromotionDue(pack, schema.PromotionEligibilityRecord{EvaluatedAt: now.UnixNano(), Indefinite: true}, model, now) {
				t.Fatal("pack was promoted without known surviving bytes")
			}
		})
	}
}

func TestArchivalPromotionRequiresSufficientCurrentPolicyHorizon(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	created := now.Add(-9 * 24 * time.Hour)
	pack := schema.PackRecord{
		Type: schema.PackData, CreationTimeKnown: true, CreationTime: created.UnixNano(),
		UsageKnown: true, UsedPayloadBytes: 1,
	}
	model := PlacementModel{
		Backends: []PlacementBackend{{ID: "archive", Hash: 2, Role: "archival", MinRetentionSeconds: uint64((90 * 24 * time.Hour) / time.Second)}},
	}
	for name, eligibility := range map[string]schema.PromotionEligibilityRecord{
		"absent":    {},
		"too short": {EvaluatedAt: now.UnixNano(), SurvivalUntil: now.Add(89 * 24 * time.Hour).UnixNano()},
		"stale":     {EvaluatedAt: created.Add(-time.Second).UnixNano(), Indefinite: true},
	} {
		t.Run(name, func(t *testing.T) {
			if archivalPromotionDue(pack, eligibility, model, now) {
				t.Fatal("promotion allowed without a sufficient current policy horizon")
			}
		})
	}
	eligible := schema.PromotionEligibilityRecord{
		EvaluatedAt: now.UnixNano(), SurvivalUntil: now.Add(90 * 24 * time.Hour).UnixNano(),
	}
	if !archivalPromotionDue(pack, eligible, model, now) {
		t.Fatal("promotion blocked despite policy covering archival minimum retention")
	}
}

func boolPtr(value bool) *bool { return &value }

func TestPlacementPlanKeepsPromotedSuccessorArchival(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	now := time.Unix(1_700_000_000, 0)
	source, successor := deterministicID(206), deterministicID(207)
	store.set(t, schema.PackKey(schema.ID(successor)), schema.PackRecord{
		Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
		PayloadSize: 80, HeaderSize: 20, BlobCount: 1, CreationTime: now.UnixNano(),
		CreationTimeKnown: true, Lifecycle: schema.PackPublished, Tier: schema.TierCold,
		RetentionSource: schema.RetentionUnknown,
	})
	store.set(t, schema.RepackLineageKey(schema.ID(source), schema.ID(successor)), schema.RepackLineageRecord{
		RunID: schema.ID(deterministicID(208)), Kind: schema.LineagePromotion,
	})
	store.set(t, schema.PackPlacementKey(schema.ID(successor), 2), schema.PlacementRecord{
		State: schema.PlacementLive, Bytes: 100, RetentionSource: schema.RetentionUnknown,
	})
	model := PlacementModel{
		Backends: []PlacementBackend{{ID: "archive", Hash: 2, Role: "archival", Offsite: true, FailureDomain: "archive"}},
		Policy:   DurabilityPolicy{MinCopies: 1, MinDomains: 1, MinOffsite: 1},
	}
	result, err := PlanPlacement(context.Background(), store, PlacementSchedulerOptions{Model: model, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Statuses) != 1 || result.Statuses[0].Class != "archival-data" || result.RequestsWritten != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestPlacementWorkerHonorsDeadlineOrderAndBandwidth(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	now := time.Unix(1_700_000_000, 0)
	first, second := deterministicID(210), deterministicID(211)
	backend := PlacementBackend{ID: "warm", Hash: 2, Role: "primary", Offsite: true, FailureDomain: "warm", MaxBandwidthBytes: 100}
	for index, packID := range []vaultic.ID{first, second} {
		store.set(t, schema.PackKey(schema.ID(packID)), schema.PackRecord{
			Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
			PayloadSize: 80, HeaderSize: 20, BlobCount: 1, CreationTime: now.UnixNano(),
			CreationTimeKnown: true, Lifecycle: schema.PackPublished, Tier: schema.TierHot,
			RetentionSource: schema.RetentionUnknown,
		})
		store.set(t, schema.PlacementRequestKey(uint64(now.Unix())+uint64(index), schema.ID(packID)), schema.PlacementRequestRecord{
			Classes: []string{"recent-data"}, Operation: schema.PlacementRequestPlace, TargetBackend: backend.Hash,
		})
	}
	actions := &fakePlacementActions{}
	result, err := ExecutePlacement(context.Background(), store, actions, PlacementWorkerOptions{
		Model: PlacementModel{Backends: []PlacementBackend{backend}}, Now: now, Window: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Placed != 1 || result.Deferred != 1 || len(actions.calls) != 1 || actions.calls[0].packID != first {
		t.Fatalf("result=%#v calls=%#v", result, actions.calls)
	}
}

func TestPlacementWorkerRetriesOutageAndResumes(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	now := time.Unix(1_700_000_000, 0)
	packID := deterministicID(212)
	backend := PlacementBackend{ID: "warm", Hash: 2, Role: "primary", Offsite: true, FailureDomain: "warm"}
	store.set(t, schema.PackKey(schema.ID(packID)), schema.PackRecord{
		Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
		PayloadSize: 80, HeaderSize: 20, BlobCount: 1, CreationTime: now.UnixNano(),
		CreationTimeKnown: true, Lifecycle: schema.PackPublished, Tier: schema.TierHot,
		RetentionSource: schema.RetentionUnknown,
	})
	requestKey := schema.PlacementRequestKey(uint64(now.Unix()), schema.ID(packID))
	store.set(t, requestKey, schema.PlacementRequestRecord{
		Classes: []string{"recent-data"}, Operation: schema.PlacementRequestPlace, TargetBackend: backend.Hash,
	})
	actions := &fakePlacementActions{failures: map[vaultic.ID]error{packID: errors.New("backend offline")}}
	result, err := ExecutePlacement(context.Background(), store, actions, PlacementWorkerOptions{
		Model: PlacementModel{Backends: []PlacementBackend{backend}}, Now: now, RetryBase: time.Second,
	})
	if err != nil || result.Failed != 1 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	value, found, err := store.Get(context.Background(), requestKey)
	if err != nil || !found {
		t.Fatalf("retry request missing: found=%v err=%v", found, err)
	}
	request, err := schema.UnmarshalPlacementRequestRecord(value)
	if err != nil || request.Attempts != 1 || request.LastError == "" || request.NotBefore <= now.UnixNano() {
		t.Fatalf("retry request=%#v err=%v", request, err)
	}
	delete(actions.failures, packID)
	result, err = ExecutePlacement(context.Background(), store, actions, PlacementWorkerOptions{
		Model: PlacementModel{Backends: []PlacementBackend{backend}}, Now: time.Unix(0, request.NotBefore), RetryBase: time.Second,
	})
	if err != nil || result.Placed != 1 {
		t.Fatalf("resume result=%#v err=%v", result, err)
	}
	if _, found, err := store.Get(context.Background(), requestKey); err != nil || found {
		t.Fatalf("completed request remains: found=%v err=%v", found, err)
	}
}

func TestPlacementWorkerFinalizesPromotionAfterSourceBecameDeletePending(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	now := time.Unix(1_700_000_000, 0)
	packID := deterministicID(215)
	store.set(t, schema.PackKey(schema.ID(packID)), schema.PackRecord{
		Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
		PayloadSize: 80, HeaderSize: 20, BlobCount: 1,
		Lifecycle: schema.PackDeletePending, RetentionSource: schema.RetentionUnknown,
	})
	requestKey := schema.PlacementRequestKey(math.MaxUint64-1, schema.ID(packID))
	store.set(t, requestKey, schema.PlacementRequestRecord{
		Classes: []string{"archival-data"}, Operation: schema.PlacementRequestPromote, TargetBackend: 2, Attempts: 1,
	})
	store.set(t, schema.PackPlacementKey(schema.ID(packID), 2), schema.PlacementRecord{
		State: schema.PlacementPending, Bytes: 100, RetentionSource: schema.RetentionUnknown,
	})
	model := PlacementModel{Backends: []PlacementBackend{{ID: "archive", Hash: 2, Role: "archival"}}}
	result, err := ExecutePlacement(context.Background(), store, &fakePlacementActions{}, PlacementWorkerOptions{Model: model, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempted != 0 {
		t.Fatalf("already-completed promotion was executed again: %#v", result)
	}
	if _, found, err := store.Get(context.Background(), requestKey); err != nil || found {
		t.Fatalf("request remains: found=%v err=%v", found, err)
	}
	value, found, err := store.Get(context.Background(), schema.PackPlacementKey(schema.ID(packID), 2))
	if err != nil || !found {
		t.Fatalf("source placement missing: found=%v err=%v", found, err)
	}
	placement, err := schema.UnmarshalPlacementRecord(value)
	if err != nil || placement.State != schema.PlacementEvicted || placement.DeleteAfter != now.UnixNano() {
		t.Fatalf("source placement = %#v, err=%v", placement, err)
	}
}

func TestPlacementWorkerRefusesDurabilityBreakingEviction(t *testing.T) {
	store := &memoryStore{values: make(map[string][]byte)}
	now := time.Unix(1_700_000_000, 0)
	packID := deterministicID(213)
	local := PlacementBackend{ID: "local", Hash: 1, Role: "primary", FailureDomain: "local"}
	warm := PlacementBackend{ID: "warm", Hash: 2, Role: "primary", Offsite: true, FailureDomain: "warm"}
	store.set(t, schema.PackKey(schema.ID(packID)), schema.PackRecord{
		Type: schema.PackData, PhysicalSize: 100, PhysicalSizeKnown: true,
		PayloadSize: 80, HeaderSize: 20, BlobCount: 1, CreationTime: now.UnixNano(),
		CreationTimeKnown: true, Lifecycle: schema.PackPublished, Tier: schema.TierMirrored,
		RetentionSource: schema.RetentionUnknown,
	})
	for _, backend := range []PlacementBackend{local, warm} {
		store.set(t, schema.PackPlacementKey(schema.ID(packID), backend.Hash), schema.PlacementRecord{
			State: schema.PlacementLive, Bytes: 100, RetentionSource: schema.RetentionUnknown,
		})
	}
	requestKey := schema.PlacementRequestKey(uint64(now.Unix()), schema.ID(packID))
	store.set(t, requestKey, schema.PlacementRequestRecord{
		Classes: []string{"recent-data"}, Operation: schema.PlacementRequestEvict, TargetBackend: warm.Hash,
	})
	actions := &fakePlacementActions{}
	result, err := ExecutePlacement(context.Background(), store, actions, PlacementWorkerOptions{
		Model: PlacementModel{Backends: []PlacementBackend{local, warm}, Policy: DurabilityPolicy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}},
		Now:   now, RetryBase: time.Second,
	})
	if err != nil || result.Failed != 1 || len(actions.calls) != 0 {
		t.Fatalf("result=%#v calls=%#v err=%v", result, actions.calls, err)
	}
}
