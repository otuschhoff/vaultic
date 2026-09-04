package repository

import (
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type PlacementDecisionInputs struct {
	PhysicalSize        uint64
	UnusedPayloadBytes  uint64
	PricePerGBMonth     float64
	PricePerGBEgress    float64
	PricePer1KRequests  float64
	RemainingRetention  time.Duration
	RetentionSource     schema.RetentionSource
	Horizon             time.Duration
	ObjectOverheadBytes uint64
	Requests            uint64
}

type PlacementDecision struct {
	Saving float64
	Cost   float64
	Repack bool
}

func placementDeleteAfter(now time.Time, keepDelete time.Duration, placement schema.PlacementRecord) int64 {
	deadline := now.Add(keepDelete).UnixNano()
	if placement.RetentionSource != schema.RetentionUnknown && placement.MinRetentionUntil > deadline {
		deadline = placement.MinRetentionUntil
	}
	return deadline
}

func placementEvictionAllowed(
	placements map[uint64]schema.PlacementRecord,
	backends map[uint64]PlacementBackend,
	policy vaultic.PlacementPolicy,
	removeBackend uint64,
) bool {
	remaining := make(map[uint64]schema.PlacementRecord, len(placements))
	for backend, placement := range placements {
		if backend == removeBackend {
			continue
		}
		remaining[backend] = placement
	}
	return placementDurable(remaining, backends, policy)
}

func placementDurable(placements map[uint64]schema.PlacementRecord, backends map[uint64]PlacementBackend, policy vaultic.PlacementPolicy) bool {
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
	for backendID, placement := range placements {
		if placement.State != schema.PlacementLive {
			continue
		}
		backend, ok := backends[backendID]
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

func placementRepackDecision(input PlacementDecisionInputs) PlacementDecision {
	if input.PricePerGBMonth <= 0 || input.Horizon <= 0 {
		return PlacementDecision{}
	}
	if input.RetentionSource == schema.RetentionUnknown {
		return PlacementDecision{}
	}
	billableBytes := input.PhysicalSize + input.ObjectOverheadBytes
	unusedGB := float64(input.UnusedPayloadBytes) / (1024 * 1024 * 1024)
	billableGB := float64(billableBytes) / (1024 * 1024 * 1024)
	horizonMonths := input.Horizon.Hours() / (24 * 30)
	remainingMonths := input.RemainingRetention.Hours() / (24 * 30)
	requestUnits := float64(input.Requests) / 1000
	decision := PlacementDecision{
		Saving: unusedGB * input.PricePerGBMonth * horizonMonths,
		Cost:   billableGB*input.PricePerGBEgress + requestUnits*input.PricePer1KRequests + billableGB*input.PricePerGBMonth*remainingMonths,
	}
	decision.Repack = decision.Saving > decision.Cost
	return decision
}
