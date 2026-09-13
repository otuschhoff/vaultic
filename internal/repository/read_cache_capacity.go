package repository

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	readCacheBudgetModeFixed         = "fixed"
	readCacheBudgetModeCephFreeSpace = "ceph-free-space"

	readCacheHealthHealthy      = "healthy"
	readCacheHealthNearfull     = "nearfull"
	readCacheHealthBackfillfull = "backfillfull"
	readCacheHealthFull         = "full"

	readCacheTelemetryStateFixed        = "fixed"
	readCacheTelemetryStateFresh        = "fresh"
	readCacheTelemetryStateStale        = "stale"
	readCacheTelemetryStateDenied       = "denied"
	readCacheTelemetryStateInconsistent = "inconsistent"
	readCacheTelemetryStateUnavailable  = "unavailable"
)

type readCacheCapacityTelemetry interface {
	Sample(context.Context) (readCacheCapacitySample, error)
}

type readCacheCapacitySample struct {
	TotalRawBytes           uint64
	FreeRawBytes            uint64
	EligibleTotalRawBytes   uint64
	EligibleFreeRawBytes    uint64
	PoolMaxAvailRawBytes    uint64
	PoolQuotaRawBytes       uint64
	PoolMaxAvailBytes       uint64
	PoolMaxAvailKnown       bool
	PoolQuotaAvailableBytes uint64
	PoolQuotaKnown          bool
	ObjectHeadroomRawBytes  uint64
	RawAmplification        float64
	Health                  string
	Timestamp               time.Time
	SourceGeneration        uint64
	Denied                  bool
	Inconsistent            bool
}

type readCacheCapacityControllerOptions struct {
	Mode                    string
	FixedMaxBytes           uint64
	ReserveFraction         float64
	MinFreeRawBytes         uint64
	SafetyMarginRawBytes    uint64
	OperatorMaxRawBytes     uint64
	TelemetryPollInterval   time.Duration
	TelemetryMaxAge         time.Duration
	GrowthRateLogicalBytes  uint64
	GrowthHysteresisBytes   uint64
	StableSamplesForGrowth  uint32
	FallbackLogicalBytes    uint64
	DefaultRawAmplification float64
}

type readCacheCapacityDecision struct {
	EffectiveLogicalBytes uint64
	FillAllowed           bool
	TelemetryState        string
	Health                string
	SourceGeneration      uint64
	TelemetryAgeMS        uint64
}

type readCacheCapacityController struct {
	telemetry readCacheCapacityTelemetry
	opts      readCacheCapacityControllerOptions

	mu                     sync.Mutex
	lastPolledAt           time.Time
	pollInFlight           bool
	lastSampleAt           time.Time
	lastSourceGeneration   uint64
	stableFreshSamples     uint32
	effectiveLogicalBudget uint64
	fillAllowed            bool
	telemetryState         string
	health                 string
	initialized            bool
}

func newReadCacheCapacityController(opts readCacheCapacityControllerOptions, telemetry readCacheCapacityTelemetry) *readCacheCapacityController {
	if opts.Mode == "" {
		opts.Mode = readCacheBudgetModeFixed
	}
	if opts.ReserveFraction < 0 {
		opts.ReserveFraction = 0
	}
	if opts.ReserveFraction > 1 {
		opts.ReserveFraction = 1
	}
	if opts.TelemetryPollInterval <= 0 {
		opts.TelemetryPollInterval = 5 * time.Second
	}
	if opts.TelemetryMaxAge <= 0 {
		opts.TelemetryMaxAge = 30 * time.Second
	}
	if opts.GrowthRateLogicalBytes == 0 {
		opts.GrowthRateLogicalBytes = 64 * 1024 * 1024
	}
	if opts.StableSamplesForGrowth == 0 {
		opts.StableSamplesForGrowth = 3
	}
	if opts.DefaultRawAmplification < 1 {
		opts.DefaultRawAmplification = 1
	}
	return &readCacheCapacityController{opts: opts, telemetry: telemetry}
}

func (controller *readCacheCapacityController) snapshot(now time.Time) readCacheCapacityDecision {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if !controller.initialized {
		if controller.opts.Mode != readCacheBudgetModeFixed {
			return readCacheCapacityDecision{
				EffectiveLogicalBytes: controller.opts.FallbackLogicalBytes,
				FillAllowed:           false,
				TelemetryState:        readCacheTelemetryStateUnavailable,
				Health:                readCacheHealthHealthy,
			}
		}
		return readCacheCapacityDecision{
			EffectiveLogicalBytes: controller.opts.FixedMaxBytes,
			FillAllowed:           true,
			TelemetryState:        readCacheTelemetryStateFixed,
			Health:                readCacheHealthHealthy,
		}
	}
	age := uint64(0)
	if !controller.lastSampleAt.IsZero() && now.After(controller.lastSampleAt) {
		age = uint64(now.Sub(controller.lastSampleAt) / time.Millisecond)
	}
	return readCacheCapacityDecision{
		EffectiveLogicalBytes: controller.effectiveLogicalBudget,
		FillAllowed:           controller.fillAllowed,
		TelemetryState:        controller.telemetryState,
		Health:                controller.health,
		SourceGeneration:      controller.lastSourceGeneration,
		TelemetryAgeMS:        age,
	}
}

func (controller *readCacheCapacityController) maybeRefresh(
	ctx context.Context,
	now time.Time,
	cacheRawBytes uint64,
	reservationsSinceSampleRaw uint64,
) readCacheCapacityDecision {
	if controller.opts.Mode == readCacheBudgetModeFixed {
		controller.mu.Lock()
		controller.initialized = true
		controller.effectiveLogicalBudget = controller.opts.FixedMaxBytes
		controller.fillAllowed = controller.opts.FixedMaxBytes > 0
		controller.telemetryState = readCacheTelemetryStateFixed
		controller.health = readCacheHealthHealthy
		controller.mu.Unlock()
		return controller.snapshot(now)
	}

	controller.mu.Lock()
	shouldPoll := !controller.initialized || controller.lastPolledAt.IsZero() || now.Sub(controller.lastPolledAt) >= controller.opts.TelemetryPollInterval
	attemptPoll := false
	if shouldPoll && controller.telemetry != nil && !controller.pollInFlight {
		controller.pollInFlight = true
		controller.lastPolledAt = now
		attemptPoll = true
	}
	controller.enforceFreshnessFallbackLocked(now)
	controller.mu.Unlock()

	if attemptPoll {
		sample, err := controller.telemetry.Sample(ctx)
		controller.consumeSample(now, sample, err, cacheRawBytes, reservationsSinceSampleRaw)
		controller.mu.Lock()
		controller.pollInFlight = false
		controller.enforceFreshnessFallbackLocked(now)
		controller.mu.Unlock()
	}

	return controller.snapshot(now)
}

func (controller *readCacheCapacityController) enforceFreshnessFallbackLocked(now time.Time) {
	if controller.opts.Mode == readCacheBudgetModeFixed || !controller.initialized {
		return
	}
	if controller.lastSampleAt.IsZero() || now.Sub(controller.lastSampleAt) > controller.opts.TelemetryMaxAge {
		if controller.telemetryState == readCacheTelemetryStateFresh {
			controller.telemetryState = readCacheTelemetryStateStale
		}
		if controller.effectiveLogicalBudget > controller.opts.FallbackLogicalBytes {
			controller.effectiveLogicalBudget = controller.opts.FallbackLogicalBytes
		}
		controller.fillAllowed = false
	}
}

//nolint:gocognit,funlen // Capacity decisions keep all raw-space constraints in one auditable calculation.
func (controller *readCacheCapacityController) consumeSample(
	now time.Time,
	sample readCacheCapacitySample,
	sampleErr error,
	cacheRawBytes uint64,
	reservationsSinceSampleRaw uint64,
) {
	controller.mu.Lock()
	defer controller.mu.Unlock()

	if sampleErr != nil {
		controller.telemetryState = readCacheTelemetryStateUnavailable
		controller.fillAllowed = false
		controller.initialized = true
		return
	}
	if sample.Denied {
		controller.telemetryState = readCacheTelemetryStateDenied
		controller.fillAllowed = false
		controller.initialized = true
		return
	}
	if sample.Inconsistent || sample.TotalRawBytes < sample.FreeRawBytes {
		controller.telemetryState = readCacheTelemetryStateInconsistent
		controller.fillAllowed = false
		controller.initialized = true
		return
	}
	if sample.Timestamp.IsZero() || now.Sub(sample.Timestamp) > controller.opts.TelemetryMaxAge {
		controller.telemetryState = readCacheTelemetryStateStale
		controller.fillAllowed = false
		controller.lastSampleAt = sample.Timestamp
		controller.initialized = true
		return
	}

	health := normalizeCacheHealth(sample.Health)
	controller.health = health
	controller.lastSampleAt = sample.Timestamp
	controller.lastSourceGeneration = sample.SourceGeneration
	controller.telemetryState = readCacheTelemetryStateFresh
	if health != readCacheHealthHealthy {
		controller.fillAllowed = false
		controller.stableFreshSamples = 0
	} else {
		controller.fillAllowed = true
		if controller.stableFreshSamples < math.MaxUint32 {
			controller.stableFreshSamples++
		}
	}

	reserveRaw := uint64(math.Ceil(controller.opts.ReserveFraction * float64(sample.TotalRawBytes)))
	if reserveRaw < controller.opts.MinFreeRawBytes {
		reserveRaw = controller.opts.MinFreeRawBytes
	}
	targetRaw := sampleTelemetryTargetRaw(sample.TotalRawBytes, sample.FreeRawBytes, cacheRawBytes, reserveRaw, controller.opts.SafetyMarginRawBytes)
	if sample.EligibleTotalRawBytes > 0 {
		eligibleReserveRaw := uint64(math.Ceil(controller.opts.ReserveFraction * float64(sample.EligibleTotalRawBytes)))
		if eligibleReserveRaw < controller.opts.MinFreeRawBytes {
			eligibleReserveRaw = controller.opts.MinFreeRawBytes
		}
		eligibleTarget := sampleTelemetryTargetRaw(
			sample.EligibleTotalRawBytes,
			sample.EligibleFreeRawBytes,
			cacheRawBytes,
			eligibleReserveRaw,
			controller.opts.SafetyMarginRawBytes,
		)
		if eligibleTarget < targetRaw {
			targetRaw = eligibleTarget
		}
	}
	if controller.opts.OperatorMaxRawBytes > 0 && targetRaw > controller.opts.OperatorMaxRawBytes {
		targetRaw = controller.opts.OperatorMaxRawBytes
	}
	amplification := sample.RawAmplification
	if amplification < 1 {
		amplification = controller.opts.DefaultRawAmplification
	}
	if amplification < 1 {
		amplification = 1
	}
	poolQuotaRaw, poolQuotaKnown := sample.PoolQuotaRawBytes, sample.PoolQuotaRawBytes > 0
	if sample.PoolQuotaKnown {
		poolQuotaRaw = logicalToRawBytes(sample.PoolQuotaAvailableBytes, amplification)
		poolQuotaKnown = true
	}
	poolMaxAvailRaw, poolMaxAvailKnown := sample.PoolMaxAvailRawBytes, sample.PoolMaxAvailRawBytes > 0
	if sample.PoolMaxAvailKnown {
		poolMaxAvailRaw = logicalToRawBytes(sample.PoolMaxAvailBytes, amplification)
		poolMaxAvailKnown = true
	}
	targetRaw = clampRawByKnownHeadroom(targetRaw, cacheRawBytes, poolQuotaRaw, poolQuotaKnown)
	targetRaw = clampRawByKnownHeadroom(targetRaw, cacheRawBytes, poolMaxAvailRaw, poolMaxAvailKnown)
	targetRaw = clampRawByHeadroom(targetRaw, cacheRawBytes, sample.ObjectHeadroomRawBytes)
	if targetRaw > reservationsSinceSampleRaw {
		targetRaw -= reservationsSinceSampleRaw
	} else {
		targetRaw = 0
	}

	targetLogical := uint64(float64(targetRaw) / amplification)

	if health != readCacheHealthHealthy {
		if targetLogical > controller.opts.FallbackLogicalBytes {
			targetLogical = controller.opts.FallbackLogicalBytes
		}
	}
	if !controller.initialized {
		controller.effectiveLogicalBudget = targetLogical
		controller.initialized = true
		return
	}
	if targetLogical <= controller.effectiveLogicalBudget {
		controller.effectiveLogicalBudget = targetLogical
		return
	}

	if !controller.fillAllowed || controller.stableFreshSamples < controller.opts.StableSamplesForGrowth {
		return
	}
	if targetLogical-controller.effectiveLogicalBudget < controller.opts.GrowthHysteresisBytes {
		return
	}
	growth := targetLogical - controller.effectiveLogicalBudget
	if growth > controller.opts.GrowthRateLogicalBytes {
		growth = controller.opts.GrowthRateLogicalBytes
	}
	controller.effectiveLogicalBudget = saturatingAddUint64(controller.effectiveLogicalBudget, growth)
}

func normalizeCacheHealth(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case readCacheHealthHealthy:
		return readCacheHealthHealthy
	case readCacheHealthNearfull:
		return readCacheHealthNearfull
	case readCacheHealthBackfillfull:
		return readCacheHealthBackfillfull
	case readCacheHealthFull:
		return readCacheHealthFull
	default:
		return readCacheHealthHealthy
	}
}

func sampleTelemetryTargetRaw(totalRaw, freeRaw, cacheRaw, reserveRaw, safetyRaw uint64) uint64 {
	if reserveRaw >= totalRaw {
		return 0
	}
	target := saturatingAddUint64(cacheRaw, freeRaw)
	target = saturatingSubUint64(target, reserveRaw)
	target = saturatingSubUint64(target, safetyRaw)
	return target
}

func clampRawByHeadroom(targetRaw, cacheRaw, headroomRaw uint64) uint64 {
	if headroomRaw == 0 {
		return targetRaw
	}
	limit := saturatingAddUint64(cacheRaw, headroomRaw)
	if targetRaw > limit {
		return limit
	}
	return targetRaw
}

func clampRawByKnownHeadroom(targetRaw, cacheRaw, headroomRaw uint64, known bool) uint64 {
	if !known {
		return targetRaw
	}
	limit := saturatingAddUint64(cacheRaw, headroomRaw)
	if targetRaw > limit {
		return limit
	}
	return targetRaw
}

func logicalToRawBytes(logical uint64, amplification float64) uint64 {
	if float64(logical) > float64(^uint64(0))/amplification {
		return ^uint64(0)
	}
	return uint64(math.Floor(float64(logical) * amplification))
}

func saturatingAddUint64(left, right uint64) uint64 {
	const maxUint64 = ^uint64(0)
	if left > maxUint64-right {
		return maxUint64
	}
	return left + right
}

func saturatingSubUint64(left, right uint64) uint64 {
	if left <= right {
		return 0
	}
	return left - right
}

func sumExceedsUint64(limit uint64, values ...uint64) bool {
	remaining := limit
	for _, value := range values {
		if value > remaining {
			return true
		}
		remaining -= value
	}
	return false
}

func readCacheCapacityModeForBackend(location string, mode string) (string, error) {
	normalized := strings.TrimSpace(strings.ToLower(mode))
	if normalized == "" {
		return readCacheBudgetModeFixed, nil
	}
	if normalized == readCacheBudgetModeFixed {
		return normalized, nil
	}
	if normalized != readCacheBudgetModeCephFreeSpace {
		return "", fmt.Errorf("unsupported read-cache budget mode %q", mode)
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(location)), "rados:") {
		return "", fmt.Errorf("read-cache budget mode %q requires a Ceph RADOS backend", normalized)
	}
	return normalized, nil
}
