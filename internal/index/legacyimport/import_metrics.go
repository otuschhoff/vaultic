package legacyimport

import (
	"errors"
	"math"
	"math/bits"
	"sync"
	"time"

	monitor "github.com/otuschhoff/vaultic/internal/telemetry"
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
	action     *monitor.ActionMetric
	operation  *monitor.ActionGuard
	waits      map[string]*monitor.WaitMetric
}

func NewSchedulerTelemetry() *SchedulerTelemetry {
	return NewSchedulerTelemetryEnabled(true)

}

func NewSchedulerTelemetryEnabled(enabled bool) *SchedulerTelemetry {
	return &SchedulerTelemetry{
		last: time.Now(), operations: make(map[string]durationHistogram),
		state:  SchedulerSnapshot{Phase: "source", PhaseTime: make(map[string]time.Duration)},
		action: monitor.NewActionMetric("legacy_import", 1, enabled),
		waits: map[string]*monitor.WaitMetric{
			"dependency_wait": monitor.NewWaitMetric("legacy_import", "database", "capacity", maxPublicationLanes, enabled),
			"ingest_wait":     monitor.NewWaitMetric("legacy_import", "rpc", "concurrency", maxPublicationLanes, enabled),
			"reducer_wait":    monitor.NewWaitMetric("legacy_import", "database", "concurrency", 1, enabled),
			"prepare_wait":    monitor.NewWaitMetric("legacy_import", "source", "concurrency", maxDefaultPackWorkers, enabled),
		},
	}
}

func (telemetry *SchedulerTelemetry) startAction() bool {
	if telemetry == nil {
		return false
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	if telemetry.operation != nil {
		return false
	}
	telemetry.operation = telemetry.action.Start("source", "")
	return true
}

func (telemetry *SchedulerTelemetry) StartAction() { telemetry.startAction() }

func (telemetry *SchedulerTelemetry) FinishAction(result Result, err error) {
	telemetry.phase("finished")
	telemetry.finishAction(result, err)
}

func (telemetry *SchedulerTelemetry) finishAction(result Result, err error) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	operation := telemetry.operation
	telemetry.operation = nil
	telemetry.mu.Unlock()
	if operation == nil {
		return
	}
	operation.Progress("complete", "", result.IndexesSeen+result.SnapshotsSeen, result.IndexesTotal+result.SnapshotsTotal)
	outcome := monitor.ClassifyOutcome(err)
	if errors.Is(err, errPackTimeout) {
		outcome = monitor.OutcomeTimeout
	}
	switch outcome {
	case monitor.OutcomeSuccess:
		operation.Done(monitor.OutcomeSuccess)
	case monitor.OutcomeCancellation:
		operation.Done(monitor.OutcomeCancellation)
	case monitor.OutcomeTimeout:
		operation.Done(monitor.OutcomeTimeout)
	default:
		operation.Done(monitor.OutcomeFailure)
	}
}

func (telemetry *SchedulerTelemetry) processed(role string, bytes uint64) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	operation := telemetry.operation
	telemetry.mu.Unlock()
	if operation != nil {
		operation.Processed(role, bytes)
	}
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
	if telemetry.operation != nil {
		operationPhase, blocking := schedulerOperationPhase(phase)
		telemetry.operation.Progress(operationPhase, blocking, 0, 0)
	}
}

func schedulerOperationPhase(phase string) (string, string) {
	switch phase {
	case "dependency_wait", "ingest_wait", "reducer_wait", "prepare_wait":
		return "wait", "concurrency"
	case "ingest":
		return "ingest", ""
	case "reduce":
		return "reduce", ""
	case "cleanup":
		return "cleanup", ""
	case "finalize":
		return "finalize", ""
	case "finished":
		return "complete", ""
	default:
		return "source", ""
	}
}

func (telemetry *SchedulerTelemetry) progress(progress Progress) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	operation := telemetry.operation
	telemetry.mu.Unlock()
	if operation == nil {
		return
	}
	phase := "source"
	blocking := ""
	switch {
	case progress.CheckpointPending:
		phase, blocking = "finalize", "durability"
	case progress.BatchesReduced < progress.BatchesIngested:
		phase = "reduce"
	case progress.InFlightLanes > 0:
		phase = "ingest"
	case progress.QueuedPreparedPacks > 0:
		phase, blocking = "admission", "concurrency"
	}
	operation.Progress(phase, blocking, progress.IndexesCompleted+progress.SnapshotsCompleted, progress.IndexesTotal+progress.SnapshotsTotal)
}

func (telemetry *SchedulerTelemetry) beginWait(phase string) *monitor.WaitGuard {
	if telemetry == nil {
		return nil
	}
	wait := telemetry.waits[phase]
	if wait == nil {
		return nil
	}
	guard := wait.Start()
	guard.Contended()
	return guard
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

func (telemetry *SchedulerTelemetry) Component(now time.Time) monitor.ComponentSnapshot {
	component := monitor.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: monitor.ProcessStartID(), CapturedUnixMS: now.UnixMilli(),
		Availability: monitor.AvailabilityExact,
	}
	if telemetry == nil {
		component.Availability = monitor.AvailabilityUnavailable
		return component
	}
	scheduler := telemetry.Snapshot()
	metrics, operations, overflow := telemetry.action.Snapshot()
	component.Metrics = append(component.Metrics, metrics...)
	component.Operations = operations
	component.OperationOverflow = overflow
	for _, phase := range []string{"dependency_wait", "ingest_wait", "reducer_wait", "prepare_wait"} {
		wait := telemetry.waits[phase]
		component.Metrics = append(component.Metrics, wait.Metrics()...)
		component.CardinalityDropped = saturatingAddLocal(component.CardinalityDropped, wait.Dropped())
	}
	ingestCount := scheduler.Operations["ingest_service"].Count
	reduceCount := scheduler.Operations["reduce_service"].Count
	component.Queues = []monitor.QueueSnapshot{
		{Name: "legacy_import_ingest", Availability: monitor.AvailabilityEstimated, CapacityAvailability: monitor.AvailabilityUnavailable, Depth: uint64(max(scheduler.ReadyBatches, 0)), ActiveWorkers: uint64(max(scheduler.ActiveLanes, 0)), Admitted: ingestCount, OldestItemAgeUS: uint64(max(scheduler.OldestUnreducedAge.Microseconds(), 0)), Backpressure: queueBackpressure(scheduler.ReadyBatches)},
		{Name: "legacy_import_reduce", Availability: monitor.AvailabilityEstimated, CapacityAvailability: monitor.AvailabilityUnavailable, Depth: uint64(max(scheduler.PendingReductionBatches, 0)), ActiveWorkers: boolUint64(scheduler.PendingReductionBatches > 0), Admitted: reduceCount, OldestItemAgeUS: uint64(max(scheduler.OldestUnreducedAge.Microseconds(), 0)), Backpressure: queueBackpressure(scheduler.PendingReductionBatches)},
	}
	return component
}

func queueBackpressure(depth int) string {
	if depth > 0 {
		return "concurrency"
	}
	return "none"
}

func boolUint64(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func saturatingAddLocal(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
