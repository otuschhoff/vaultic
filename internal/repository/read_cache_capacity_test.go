package repository

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
)

type fakeCapacityTelemetry struct {
	sample readCacheCapacitySample
	err    error

	blockCh chan struct{}
	calls   atomic.Uint64
}

func (telemetry *fakeCapacityTelemetry) Sample(context.Context) (readCacheCapacitySample, error) {
	telemetry.calls.Add(1)
	if telemetry.blockCh != nil {
		<-telemetry.blockCh
	}
	if telemetry.err != nil {
		return readCacheCapacitySample{}, telemetry.err
	}
	return telemetry.sample, nil
}

func (telemetry *fakeCapacityTelemetry) callCount() uint64 {
	return telemetry.calls.Load()
}

func tib(value uint64) uint64 {
	return value * (1 << 40)
}

func TestReadCacheCapacityFormulaExample(t *testing.T) {
	now := time.Now().UTC()
	sample := readCacheCapacitySample{
		TotalRawBytes:        tib(100),
		FreeRawBytes:         tib(20),
		RawAmplification:     1,
		Health:               readCacheHealthHealthy,
		Timestamp:            now,
		SourceGeneration:     7,
		PoolMaxAvailRawBytes: 0,
	}
	controller := newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                   readCacheBudgetModeCephFreeSpace,
		ReserveFraction:        0.10,
		StableSamplesForGrowth: 1,
		GrowthRateLogicalBytes: tib(200),
	}, &fakeCapacityTelemetry{sample: sample})
	decision := controller.maybeRefresh(context.Background(), now, tib(30), 0)
	if decision.EffectiveLogicalBytes != tib(40) {
		t.Fatalf("expected raw target 40TiB, got %d", decision.EffectiveLogicalBytes)
	}
	controller.consumeSample(now.Add(time.Second), sample, nil, tib(30), tib(10))
	decision = controller.snapshot(now.Add(time.Second))
	if decision.EffectiveLogicalBytes != tib(30) {
		t.Fatalf("expected reservation-adjusted target 30TiB, got %d", decision.EffectiveLogicalBytes)
	}
}

func TestReadCacheCapacityEligibleConstraintAndAmplification(t *testing.T) {
	now := time.Now().UTC()
	controller := newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                    readCacheBudgetModeCephFreeSpace,
		ReserveFraction:         0.10,
		StableSamplesForGrowth:  1,
		GrowthRateLogicalBytes:  tib(200),
		DefaultRawAmplification: 3,
	}, &fakeCapacityTelemetry{sample: readCacheCapacitySample{
		TotalRawBytes:         tib(100),
		FreeRawBytes:          tib(20),
		EligibleTotalRawBytes: tib(40),
		EligibleFreeRawBytes:  tib(5),
		RawAmplification:      2,
		Health:                readCacheHealthHealthy,
		Timestamp:             now,
	}})
	decision := controller.maybeRefresh(context.Background(), now, tib(30), 0)
	if decision.EffectiveLogicalBytes != tib(15)+tib(1)/2 {
		t.Fatalf("expected eligible+amplification target 15TiB, got %d", decision.EffectiveLogicalBytes)
	}
}

func TestReadCacheCapacityExternalPressureAndHeadroomClamps(t *testing.T) {
	now := time.Now().UTC()
	telemetry := &fakeCapacityTelemetry{sample: readCacheCapacitySample{
		TotalRawBytes:        tib(100),
		FreeRawBytes:         tib(20),
		PoolQuotaRawBytes:    tib(4),
		PoolMaxAvailRawBytes: tib(8),
		RawAmplification:     1,
		Health:               readCacheHealthHealthy,
		Timestamp:            now,
	}}

	controller := newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                   readCacheBudgetModeCephFreeSpace,
		ReserveFraction:        0.10,
		StableSamplesForGrowth: 1,
		GrowthRateLogicalBytes: tib(200),
	}, telemetry)
	decision := controller.maybeRefresh(context.Background(), now, tib(30), 0)
	if decision.EffectiveLogicalBytes != tib(34) {
		t.Fatalf("expected clamp to cache+quota-headroom (34TiB), got %d", decision.EffectiveLogicalBytes)
	}

	telemetry.sample.FreeRawBytes = tib(5)
	decision = controller.maybeRefresh(context.Background(), now.Add(6*time.Second), tib(30), 0)
	if decision.EffectiveLogicalBytes != tib(25) {
		t.Fatalf("expected shrink target 25TiB under external pressure, got %d", decision.EffectiveLogicalBytes)
	}
}

func TestReadCacheCapacityLogicalPoolHeadroomUsesDefaultAmplification(t *testing.T) {
	now := time.Now().UTC()
	telemetry := &fakeCapacityTelemetry{sample: readCacheCapacitySample{
		TotalRawBytes:           tib(100),
		FreeRawBytes:            tib(80),
		PoolQuotaAvailableBytes: tib(4),
		PoolQuotaKnown:          true,
		RawAmplification:        0,
		Health:                  readCacheHealthHealthy,
		Timestamp:               now,
	}}
	controller := newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                    readCacheBudgetModeCephFreeSpace,
		ReserveFraction:         0.10,
		StableSamplesForGrowth:  1,
		GrowthRateLogicalBytes:  tib(200),
		DefaultRawAmplification: 3,
	}, telemetry)
	decision := controller.maybeRefresh(context.Background(), now, tib(3), 0)
	if decision.EffectiveLogicalBytes != tib(5) {
		t.Fatalf("expected cache plus 4TiB logical quota headroom, got %d", decision.EffectiveLogicalBytes)
	}

	telemetry.sample.PoolQuotaAvailableBytes = 0
	decision = controller.maybeRefresh(context.Background(), now.Add(6*time.Second), tib(3), 0)
	if decision.EffectiveLogicalBytes != tib(1) {
		t.Fatalf("expected exhausted known quota to prevent growth, got %d", decision.EffectiveLogicalBytes)
	}
}

func TestReadCacheCapacityStaleDeniedUnavailableFreezeAndFallback(t *testing.T) {
	now := time.Now().UTC()
	telemetry := &fakeCapacityTelemetry{sample: readCacheCapacitySample{
		TotalRawBytes:    tib(100),
		FreeRawBytes:     tib(20),
		RawAmplification: 1,
		Health:           readCacheHealthHealthy,
		Timestamp:        now,
		SourceGeneration: 11,
	}}
	controller := newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                   readCacheBudgetModeCephFreeSpace,
		ReserveFraction:        0.10,
		StableSamplesForGrowth: 1,
		GrowthRateLogicalBytes: tib(200),
		TelemetryMaxAge:        2 * time.Second,
		TelemetryPollInterval:  time.Second,
		FallbackLogicalBytes:   0,
	}, telemetry)
	decision := controller.maybeRefresh(context.Background(), now, tib(30), 0)
	if !decision.FillAllowed {
		t.Fatal("fresh healthy telemetry should allow fill")
	}

	telemetry.sample.Timestamp = now.Add(-10 * time.Second)
	decision = controller.maybeRefresh(context.Background(), now.Add(6*time.Second), tib(30), 0)
	if decision.TelemetryState != readCacheTelemetryStateStale || decision.FillAllowed {
		t.Fatalf("expected stale telemetry freeze, got state=%q fill=%v", decision.TelemetryState, decision.FillAllowed)
	}

	telemetry.sample.Timestamp = now.Add(7 * time.Second)
	telemetry.sample.Denied = true
	decision = controller.maybeRefresh(context.Background(), now.Add(12*time.Second), tib(30), 0)
	if decision.TelemetryState != readCacheTelemetryStateDenied || decision.FillAllowed {
		t.Fatalf("expected denied telemetry freeze, got state=%q fill=%v", decision.TelemetryState, decision.FillAllowed)
	}

	telemetry.sample.Denied = false
	telemetry.err = nil
	telemetry.sample.Inconsistent = true
	telemetry.sample.Timestamp = now.Add(13 * time.Second)
	decision = controller.maybeRefresh(context.Background(), now.Add(13*time.Second), tib(30), 0)
	if decision.TelemetryState != readCacheTelemetryStateInconsistent || decision.FillAllowed {
		t.Fatalf("expected inconsistent telemetry freeze, got state=%q fill=%v", decision.TelemetryState, decision.FillAllowed)
	}

	telemetry.sample.Inconsistent = false
	telemetry.err = errors.New("telemetry unavailable")
	decision = controller.maybeRefresh(context.Background(), now.Add(18*time.Second), tib(30), 0)
	if decision.TelemetryState != readCacheTelemetryStateUnavailable || decision.FillAllowed {
		t.Fatalf("expected unavailable telemetry freeze, got state=%q fill=%v", decision.TelemetryState, decision.FillAllowed)
	}
}

func TestReadCacheCapacityPollAttemptTimestampAndBoundary(t *testing.T) {
	now := time.Now().UTC()
	telemetry := &fakeCapacityTelemetry{sample: readCacheCapacitySample{
		TotalRawBytes:    tib(100),
		FreeRawBytes:     tib(20),
		RawAmplification: 1,
		Health:           readCacheHealthHealthy,
		Timestamp:        now,
	}}
	controller := newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                   readCacheBudgetModeCephFreeSpace,
		ReserveFraction:        0.10,
		StableSamplesForGrowth: 1,
		GrowthRateLogicalBytes: tib(200),
		TelemetryPollInterval:  5 * time.Second,
		TelemetryMaxAge:        30 * time.Second,
	}, telemetry)

	controller.maybeRefresh(context.Background(), now, tib(30), 0)
	if telemetry.callCount() != 1 {
		t.Fatalf("expected initial poll attempt, got %d", telemetry.callCount())
	}
	controller.mu.Lock()
	firstPolled := controller.lastPolledAt
	controller.mu.Unlock()
	if !firstPolled.Equal(now) {
		t.Fatalf("expected first poll timestamp %v, got %v", now, firstPolled)
	}

	controller.maybeRefresh(context.Background(), now.Add(4*time.Second), tib(30), 0)
	if telemetry.callCount() != 1 {
		t.Fatalf("expected no poll before interval boundary, got %d", telemetry.callCount())
	}
	controller.mu.Lock()
	secondPolled := controller.lastPolledAt
	controller.mu.Unlock()
	if !secondPolled.Equal(firstPolled) {
		t.Fatalf("expected lastPolledAt to remain unchanged without poll attempt, before=%v after=%v", firstPolled, secondPolled)
	}

	nextNow := now.Add(5 * time.Second)
	telemetry.sample.Timestamp = nextNow
	controller.maybeRefresh(context.Background(), nextNow, tib(30), 0)
	if telemetry.callCount() != 2 {
		t.Fatalf("expected poll at interval boundary, got %d", telemetry.callCount())
	}
	controller.mu.Lock()
	thirdPolled := controller.lastPolledAt
	controller.mu.Unlock()
	if !thirdPolled.Equal(nextNow) {
		t.Fatalf("expected boundary poll timestamp %v, got %v", nextNow, thirdPolled)
	}
}

func TestReadCacheCapacityStaleFallbackWithoutPollAttempt(t *testing.T) {
	now := time.Now().UTC()
	telemetry := &fakeCapacityTelemetry{sample: readCacheCapacitySample{
		TotalRawBytes:    tib(100),
		FreeRawBytes:     tib(20),
		RawAmplification: 1,
		Health:           readCacheHealthHealthy,
		Timestamp:        now,
	}}
	controller := newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                   readCacheBudgetModeCephFreeSpace,
		ReserveFraction:        0.10,
		StableSamplesForGrowth: 1,
		GrowthRateLogicalBytes: tib(200),
		TelemetryPollInterval:  time.Hour,
		TelemetryMaxAge:        2 * time.Second,
		FallbackLogicalBytes:   tib(1),
	}, telemetry)

	fresh := controller.maybeRefresh(context.Background(), now, tib(30), 0)
	if fresh.TelemetryState != readCacheTelemetryStateFresh || !fresh.FillAllowed {
		t.Fatalf("expected fresh initial sample, got state=%q fill=%v", fresh.TelemetryState, fresh.FillAllowed)
	}

	staleNow := now.Add(3 * time.Second)
	stale := controller.maybeRefresh(context.Background(), staleNow, tib(30), 0)
	if telemetry.callCount() != 1 {
		t.Fatalf("expected no additional poll attempt before interval, got %d", telemetry.callCount())
	}
	if stale.TelemetryState != readCacheTelemetryStateStale || stale.FillAllowed {
		t.Fatalf("expected stale fallback without poll attempt, got state=%q fill=%v", stale.TelemetryState, stale.FillAllowed)
	}
	if stale.EffectiveLogicalBytes != tib(1) {
		t.Fatalf("expected fallback limit after staleness, got %d", stale.EffectiveLogicalBytes)
	}
}

func TestReadCacheCapacityConcurrentPollCoalescingBounded(t *testing.T) {
	now := time.Now().UTC()
	release := make(chan struct{})
	telemetry := &fakeCapacityTelemetry{sample: readCacheCapacitySample{
		TotalRawBytes:    tib(100),
		FreeRawBytes:     tib(20),
		RawAmplification: 1,
		Health:           readCacheHealthHealthy,
		Timestamp:        now,
	}, blockCh: release}
	controller := newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                   readCacheBudgetModeCephFreeSpace,
		ReserveFraction:        0.10,
		StableSamplesForGrowth: 1,
		GrowthRateLogicalBytes: tib(200),
		TelemetryPollInterval:  5 * time.Second,
		TelemetryMaxAge:        30 * time.Second,
		FallbackLogicalBytes:   tib(1),
	}, telemetry)

	const callers = 16
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			controller.maybeRefresh(context.Background(), now, tib(30), 0)
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	if telemetry.callCount() != 1 {
		t.Fatalf("expected one coalesced poll attempt for concurrent callers, got %d", telemetry.callCount())
	}

	nextNow := now.Add(5 * time.Second)
	nextRelease := make(chan struct{})
	telemetry.blockCh = nextRelease
	telemetry.sample.Timestamp = nextNow
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			controller.maybeRefresh(context.Background(), nextNow, tib(30), 0)
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(nextRelease)
	wg.Wait()

	if telemetry.callCount() != 2 {
		t.Fatalf("expected one additional bounded poll attempt at next boundary, got %d", telemetry.callCount())
	}
}

func TestReadCacheCapacityHealthOverrideAndStableGrowthResume(t *testing.T) {
	now := time.Now().UTC()
	telemetry := &fakeCapacityTelemetry{sample: readCacheCapacitySample{
		TotalRawBytes:    tib(100),
		FreeRawBytes:     tib(20),
		RawAmplification: 1,
		Health:           readCacheHealthNearfull,
		Timestamp:        now,
	}}
	controller := newReadCacheCapacityController(readCacheCapacityControllerOptions{
		Mode:                   readCacheBudgetModeCephFreeSpace,
		ReserveFraction:        0.10,
		StableSamplesForGrowth: 3,
		GrowthRateLogicalBytes: tib(5),
		GrowthHysteresisBytes:  tib(2),
		FallbackLogicalBytes:   tib(5),
	}, telemetry)
	decision := controller.maybeRefresh(context.Background(), now, tib(30), 0)
	if decision.FillAllowed || decision.EffectiveLogicalBytes != tib(5) {
		t.Fatalf("expected nearfull override fallback, got fill=%v limit=%d", decision.FillAllowed, decision.EffectiveLogicalBytes)
	}

	telemetry.sample.Health = readCacheHealthHealthy
	telemetry.sample.Timestamp = now.Add(6 * time.Second)
	decision = controller.maybeRefresh(context.Background(), now.Add(6*time.Second), tib(30), 0)
	if decision.EffectiveLogicalBytes != tib(5) {
		t.Fatalf("expected hysteresis/stability hold after first healthy sample, got %d", decision.EffectiveLogicalBytes)
	}
	telemetry.sample.Timestamp = now.Add(12 * time.Second)
	decision = controller.maybeRefresh(context.Background(), now.Add(12*time.Second), tib(30), 0)
	if decision.EffectiveLogicalBytes != tib(5) {
		t.Fatalf("expected hold before stable sample threshold, got %d", decision.EffectiveLogicalBytes)
	}
	telemetry.sample.Timestamp = now.Add(18 * time.Second)
	decision = controller.maybeRefresh(context.Background(), now.Add(18*time.Second), tib(30), 0)
	if decision.EffectiveLogicalBytes != tib(10) {
		t.Fatalf("expected growth-rate-limited resume step to 10TiB, got %d", decision.EffectiveLogicalBytes)
	}
}

func TestReadCacheCapacityModeValidation(t *testing.T) {
	mode, err := readCacheCapacityModeForBackend("rados://cluster/pool", "ceph-free-space")
	if err != nil || mode != readCacheBudgetModeCephFreeSpace {
		t.Fatalf("expected ceph mode acceptance for rados backend, mode=%q err=%v", mode, err)
	}
	if _, err := readCacheCapacityModeForBackend("s3:https://s3.example/bucket", "ceph-free-space"); err == nil {
		t.Fatal("expected generic s3 to reject ceph-free-space mode")
	}
}

func TestReadCacheCapacityTargetRawSaturatesArithmetic(t *testing.T) {
	if got := sampleTelemetryTargetRaw(100, 2, 5, 10, 1); got != 0 {
		t.Fatalf("expected low-free boundary to clamp at zero, got %d", got)
	}

	const maxUint64 = ^uint64(0)
	if got := sampleTelemetryTargetRaw(maxUint64, 10, maxUint64-5, 0, 0); got != maxUint64 {
		t.Fatalf("expected saturating add to clamp at max uint64, got %d", got)
	}

	if got := clampRawByHeadroom(maxUint64, maxUint64-3, 10); got != maxUint64 {
		t.Fatalf("expected headroom limit add to saturate at max uint64, got %d", got)
	}
}

func TestReadCacheCheckedSumBoundaries(t *testing.T) {
	if !sumExceedsUint64(math.MaxUint64, math.MaxUint64-1, 2) {
		t.Fatal("expected overflow-boundary checked sum to exceed limit")
	}
	if sumExceedsUint64(math.MaxUint64, math.MaxUint64-1, 1) {
		t.Fatal("expected exact-fit checked sum to be accepted")
	}
}

type fakeBackendCapacityTelemetry struct {
	sample backend.CapacityTelemetrySample
	err    error
}

func (telemetry fakeBackendCapacityTelemetry) SampleCapacity(context.Context) (backend.CapacityTelemetrySample, error) {
	if telemetry.err != nil {
		return backend.CapacityTelemetrySample{}, telemetry.err
	}
	return telemetry.sample, nil
}

func TestBackendCapacityTelemetryAdapterValidation(t *testing.T) {
	adapter := backendCapacityTelemetryAdapter{source: fakeBackendCapacityTelemetry{sample: backend.CapacityTelemetrySample{
		TotalRawBytes:         100,
		FreeRawBytes:          101,
		EligibleTotalRawBytes: 10,
		EligibleFreeRawBytes:  11,
		RawAmplification:      0,
		Health:                "HEALTH_WARN",
	}}}
	sample, err := adapter.Sample(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !sample.Inconsistent {
		t.Fatalf("expected inconsistent sample: %#v", sample)
	}
	if sample.RawAmplification != 0 {
		t.Fatalf("expected unknown amplification to remain zero, got %f", sample.RawAmplification)
	}
	if sample.Health != readCacheHealthHealthy {
		t.Fatalf("expected normalized fallback health, got %q", sample.Health)
	}
}
