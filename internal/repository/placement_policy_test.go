package repository

import (
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestPlacementEvictionRefusesToDropBelowDurability(t *testing.T) {
	backends := map[uint64]PlacementBackend{
		1: {PlacementBackend: vaultic.PlacementBackend{ID: "local", FailureDomain: "room", Role: PlacementRolePrimary}, Hash: 1},
		2: {PlacementBackend: vaultic.PlacementBackend{ID: "archive", FailureDomain: "cloud", Role: PlacementRoleArchival, Offsite: true}, Hash: 2},
		3: {PlacementBackend: vaultic.PlacementBackend{ID: "cache", FailureDomain: "room", Role: PlacementRoleCache}, Hash: 3},
	}
	live := func() schema.PlacementRecord {
		return schema.PlacementRecord{State: schema.PlacementLive, Bytes: 1, RetentionSource: schema.RetentionUnknown}
	}
	policy := vaultic.PlacementPolicy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}
	if placementEvictionAllowed(map[uint64]schema.PlacementRecord{1: live(), 2: live()}, backends, policy, 2) {
		t.Fatal("eviction of the only offsite copy was allowed")
	}
	if placementEvictionAllowed(map[uint64]schema.PlacementRecord{1: live(), 2: {State: schema.PlacementPending, Bytes: 1, RetentionSource: schema.RetentionUnknown}}, backends, policy, 1) {
		t.Fatal("pending placement was counted as durable during eviction")
	}
	if placementEvictionAllowed(map[uint64]schema.PlacementRecord{1: live(), 2: live(), 3: live()}, backends, policy, 2) {
		t.Fatal("two remaining placements in one failure domain were counted as durable")
	}
	if !placementEvictionAllowed(map[uint64]schema.PlacementRecord{1: live(), 2: live(), 3: live()}, backends, vaultic.PlacementPolicy{MinCopies: 1, MinDomains: 1}, 2) {
		t.Fatal("eviction that preserves the configured durability was refused")
	}
}

func TestPlacementDeleteAfterUsesPerPlacementRetention(t *testing.T) {
	now := time.Unix(0, 1_000)
	local := schema.PlacementRecord{State: schema.PlacementLive, RetentionSource: schema.RetentionUnknown}
	if got := placementDeleteAfter(now, 10*time.Second, local); got != now.Add(10*time.Second).UnixNano() {
		t.Fatalf("local delete-after = %d", got)
	}
	archival := schema.PlacementRecord{
		State: schema.PlacementLive, RetentionSource: schema.RetentionBackend,
		MinRetentionUntil: now.Add(90 * 24 * time.Hour).UnixNano(),
	}
	if got := placementDeleteAfter(now, 10*time.Second, archival); got != archival.MinRetentionUntil {
		t.Fatalf("archival delete-after = %d, want retention %d", got, archival.MinRetentionUntil)
	}
}

func TestPlacementRepackCostModelDefaultsToNoRepack(t *testing.T) {
	for name, input := range map[string]PlacementDecisionInputs{
		"zero price":   {PhysicalSize: 1 << 30, UnusedPayloadBytes: 1 << 30, Horizon: 180 * 24 * time.Hour},
		"zero horizon": {PhysicalSize: 1 << 30, UnusedPayloadBytes: 1 << 30, PricePerGBMonth: 1},
	} {
		if decision := placementRepackDecision(input); decision.Repack || decision.Saving != 0 || decision.Cost != 0 {
			t.Fatalf("%s defaulted to %#v, want no repack and no claimed economics", name, decision)
		}
	}
}

func TestPlacementRepackCostModelBoundary(t *testing.T) {
	beneficial := placementRepackDecision(PlacementDecisionInputs{
		PhysicalSize: 1 << 30, UnusedPayloadBytes: 900 << 20,
		PricePerGBMonth: 20, RetentionSource: schema.RetentionConfig,
		Horizon: 12 * 30 * 24 * time.Hour,
	})
	if !beneficial.Repack {
		t.Fatalf("obvious storage saving was not repacked: %#v", beneficial)
	}
	expensive := placementRepackDecision(PlacementDecisionInputs{
		PhysicalSize: 1 << 30, UnusedPayloadBytes: 1 << 20,
		PricePerGBMonth: 1, PricePerGBEgress: 50, PricePer1KRequests: 1,
		RetentionSource: schema.RetentionBackend, Requests: 2,
		RemainingRetention: 180 * 24 * time.Hour, Horizon: 30 * 24 * time.Hour,
	})
	if expensive.Repack {
		t.Fatalf("expensive archival movement was repacked: %#v", expensive)
	}
	unknown := placementRepackDecision(PlacementDecisionInputs{
		PhysicalSize: 1 << 30, UnusedPayloadBytes: 900 << 20,
		PricePerGBMonth: 20, RetentionSource: schema.RetentionUnknown,
		Horizon: 12 * 30 * 24 * time.Hour,
	})
	if unknown.Repack || unknown.Saving != 0 || unknown.Cost != 0 {
		t.Fatalf("retention-unknown placement received economic credit: %#v", unknown)
	}
}
