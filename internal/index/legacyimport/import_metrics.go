package legacyimport

import (
	"math"
	"math/bits"
	"sync"
	"time"
)

const durationHistogramBuckets = 64

type DurationDistribution struct {
	Count uint64
	Sum   time.Duration
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
}

type durationHistogram struct {
	counts [durationHistogramBuckets]uint64
	count  uint64
	sum    uint64
}

func (histogram *durationHistogram) observe(elapsed time.Duration) {
	nanos := uint64(max(elapsed, 0))
	bucket := min(bits.Len64(nanos), durationHistogramBuckets-1)
	histogram.counts[bucket]++
	histogram.count++
	histogram.sum += nanos
}

func (histogram durationHistogram) snapshot() DurationDistribution {
	return DurationDistribution{
		Count: histogram.count,
		Sum:   time.Duration(histogram.sum),
		P50:   histogram.quantile(50),
		P95:   histogram.quantile(95),
		P99:   histogram.quantile(99),
	}
}

func (histogram durationHistogram) quantile(percent uint64) time.Duration {
	if histogram.count == 0 {
		return 0
	}
	target := (histogram.count*percent + 99) / 100
	var cumulative uint64
	for bucket, count := range histogram.counts {
		cumulative += count
		if cumulative >= target {
			if bucket == 0 {
				return 0
			}
			if bucket == durationHistogramBuckets-1 {
				return time.Duration(math.MaxInt64)
			}
			return time.Duration((uint64(1) << bucket) - 1)
		}
	}
	return time.Duration(math.MaxInt64)
}

type SchedulerSnapshot struct {
	Phase                   string
	PhaseTime               map[string]time.Duration
	LaneTime                [maxPublicationLanes + 1]time.Duration
	ActiveLanes             int
	ReadyBatches            int
	PendingReductionBatches int
	RetainedPreparedBytes   uint64
	UnreducedPreparedBytes  uint64
	OldestUnreducedAge      time.Duration
	Operations              map[string]DurationDistribution
}

type SchedulerTelemetry struct {
	mu         sync.Mutex
	last       time.Time
	state      SchedulerSnapshot
	operations map[string]durationHistogram
	oldest     time.Time
	published  int
}

func NewSchedulerTelemetry() *SchedulerTelemetry {
	return &SchedulerTelemetry{last: time.Now(), operations: make(map[string]durationHistogram), state: SchedulerSnapshot{
		Phase: "source", PhaseTime: make(map[string]time.Duration),
	}}
}

func (telemetry *SchedulerTelemetry) account(now time.Time) {
	elapsed := now.Sub(telemetry.last)
	telemetry.state.PhaseTime[telemetry.state.Phase] += elapsed
	telemetry.state.LaneTime[telemetry.state.ActiveLanes] += elapsed
	telemetry.last = now
}

func (telemetry *SchedulerTelemetry) phase(phase string) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.account(time.Now())
	telemetry.state.Phase = phase
}

func (telemetry *SchedulerTelemetry) lane(delta int) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.account(time.Now())
	telemetry.state.ActiveLanes += delta
}

func (telemetry *SchedulerTelemetry) queues(ready, pending int, retained, unreduced uint64, oldest time.Time) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.state.ReadyBatches = ready
	telemetry.state.PendingReductionBatches = pending
	telemetry.state.RetainedPreparedBytes = retained
	telemetry.state.UnreducedPreparedBytes = unreduced
	telemetry.oldest = oldest
}

func (telemetry *SchedulerTelemetry) observe(operation string, elapsed time.Duration) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	histogram := telemetry.operations[operation]
	histogram.observe(elapsed)
	telemetry.operations[operation] = histogram
}

func (telemetry *SchedulerTelemetry) pendingCompletion(delta int) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.published += delta
}

func (telemetry *SchedulerTelemetry) Snapshot() SchedulerSnapshot {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.account(time.Now())
	result := telemetry.state
	result.PendingReductionBatches += telemetry.published
	if !telemetry.oldest.IsZero() {
		result.OldestUnreducedAge = max(time.Since(telemetry.oldest), 0)
	}
	result.PhaseTime = make(map[string]time.Duration, len(telemetry.state.PhaseTime))
	for phase, duration := range telemetry.state.PhaseTime {
		result.PhaseTime[phase] = duration
	}
	result.Operations = make(map[string]DurationDistribution, len(telemetry.operations))
	for operation, histogram := range telemetry.operations {
		result.Operations[operation] = histogram.snapshot()
	}
	return result
}
