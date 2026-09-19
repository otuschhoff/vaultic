package telemetry

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const MaxActiveWaits = MaxMonitorOperations

type Outcome string

const (
	OutcomeSuccess      Outcome = "success"
	OutcomeFailure      Outcome = "failure"
	OutcomeCancellation Outcome = "cancellation"
	OutcomeTimeout      Outcome = "timeout"
)

var accountingOutcomes = [...]Outcome{OutcomeSuccess, OutcomeFailure, OutcomeCancellation, OutcomeTimeout}

type WaitSnapshot struct {
	Attempts      uint64
	Contentions   uint64
	Completed     [len(accountingOutcomes)]uint64
	Active        uint64
	OldestAgeUS   uint64
	ActiveDropped uint64
	Duration      [len(accountingOutcomes)]DistributionSnapshot
}

type WaitMetric struct {
	enabled     bool
	operation   string
	role        string
	throttle    string
	attempts    Counter
	contentions Counter
	completed   [len(accountingOutcomes)]Counter
	duration    [len(accountingOutcomes)]*FixedDistribution
	active      []atomic.Int64
	activeTotal atomic.Uint64
	dropped     Counter
	now         func() int64
}

type WaitGuard struct {
	metric  *WaitMetric
	started int64
	slot    int
	outcome atomic.Uint32
	settled atomic.Bool
}

var accountingEpoch = time.Now()
var accountingProcessStartID = strconv.FormatInt(time.Now().UnixNano(), 36)
var disabledWaitGuard = &WaitGuard{slot: -1}

func ProcessStartID() string { return accountingProcessStartID }

func ClassifyOutcome(err error) Outcome {
	switch {
	case err == nil:
		return OutcomeSuccess
	case errors.Is(err, context.Canceled), status.Code(err) == codes.Canceled:
		return OutcomeCancellation
	case errors.Is(err, context.DeadlineExceeded), status.Code(err) == codes.DeadlineExceeded:
		return OutcomeTimeout
	default:
		return OutcomeFailure
	}
}

func NewWaitMetric(operation, role, throttle string, activeCapacity int, enabled bool) *WaitMetric {
	if !validOperationRole(operation, role) {
		panic("telemetry wait metric requires a supported operation and role")
	}
	if _, known := monitorValues("throttle")[throttle]; !known {
		panic("telemetry wait metric requires a bounded throttle identity")
	}
	if activeCapacity <= 0 || activeCapacity > MaxActiveWaits {
		panic("telemetry wait metric active capacity is out of bounds")
	}
	metric := &WaitMetric{
		enabled: enabled, operation: operation, role: role, throttle: throttle,
		active: make([]atomic.Int64, activeCapacity),
		now:    func() int64 { return max(time.Since(accountingEpoch).Microseconds()+1, 1) },
	}
	for index := range metric.duration {
		metric.duration[index] = NewFixedDistribution(vaulticLatencyBounds())
	}
	return metric
}

func (metric *WaitMetric) Start() *WaitGuard {
	if metric == nil || !metric.enabled {
		return disabledWaitGuard
	}
	started := metric.now()
	metric.attempts.Add(1)
	metric.activeTotal.Add(1)
	slot := -1
	for index := range metric.active {
		if metric.active[index].CompareAndSwap(0, started) {
			slot = index
			break
		}
	}
	if slot < 0 {
		metric.dropped.Add(1)
	}
	guard := &WaitGuard{metric: metric, started: started, slot: slot}
	guard.outcome.Store(uint32(outcomeIndex(OutcomeCancellation)))
	return guard
}

func (guard *WaitGuard) Contended() {
	if guard != nil && guard.metric != nil && !guard.settled.Load() {
		guard.metric.contentions.Add(1)
	}
}

func (guard *WaitGuard) Succeeded() { guard.setOutcome(OutcomeSuccess) }
func (guard *WaitGuard) Failed()    { guard.setOutcome(OutcomeFailure) }
func (guard *WaitGuard) TimedOut()  { guard.setOutcome(OutcomeTimeout) }

func (guard *WaitGuard) Finish(err error) {
	guard.setOutcome(ClassifyOutcome(err))
	guard.Done()
}

func (guard *WaitGuard) setOutcome(outcome Outcome) {
	if guard != nil && guard.metric != nil && !guard.settled.Load() {
		guard.outcome.Store(uint32(outcomeIndex(outcome)))
	}
}

func (guard *WaitGuard) Done() {
	if guard == nil || guard.metric == nil || !guard.settled.CompareAndSwap(false, true) {
		return
	}
	metric := guard.metric
	metric.activeTotal.Add(^uint64(0))
	if guard.slot >= 0 {
		metric.active[guard.slot].CompareAndSwap(guard.started, 0)
	}
	index := int(guard.outcome.Load())
	metric.completed[index].Add(1)
	elapsed := metric.now() - guard.started
	metric.duration[index].Observe(uint64(max(elapsed, 0)))
}

func (metric *WaitMetric) Snapshot() WaitSnapshot {
	var snapshot WaitSnapshot
	for index := range snapshot.Duration {
		snapshot.Duration[index] = emptyDistribution(vaulticLatencyBounds())
	}
	if metric == nil || !metric.enabled {
		return snapshot
	}
	snapshot.Attempts = metric.attempts.Load()
	snapshot.Contentions = metric.contentions.Load()
	snapshot.ActiveDropped = metric.dropped.Load()
	now := metric.now()
	oldest := int64(math.MaxInt64)
	for index := range metric.active {
		started := metric.active[index].Load()
		if started == 0 {
			continue
		}
		snapshot.Active = saturatingAdd(snapshot.Active, 1)
		if started < oldest {
			oldest = started
		}
	}
	if oldest != math.MaxInt64 {
		snapshot.OldestAgeUS = uint64(max(now-oldest, 0))
	}
	snapshot.Active = metric.activeTotal.Load()
	for index := range snapshot.Completed {
		snapshot.Completed[index] = metric.completed[index].Load()
		snapshot.Duration[index] = metric.duration[index].Snapshot()
	}
	return snapshot
}

func (metric *WaitMetric) Metrics() []Metric {
	if metric == nil || !metric.enabled {
		return nil
	}
	snapshot := metric.Snapshot()
	labels := []Label{{Name: "operation", Value: metric.operation}, {Name: "role", Value: metric.role}, {Name: "throttle", Value: metric.throttle}}
	oldestAvailability := AvailabilityExact
	if snapshot.ActiveDropped != 0 {
		oldestAvailability = AvailabilityEstimated
	}
	metrics := []Metric{
		{Name: "wait_attempts", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Labels: labels, Value: snapshot.Attempts},
		{Name: "wait_contentions", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Labels: labels, Value: snapshot.Contentions},
		{Name: "wait_active", Kind: MetricGauge, Unit: "operations", Availability: AvailabilityExact, Labels: labels, Value: snapshot.Active},
		{Name: "wait_oldest_age", Kind: MetricGauge, Unit: "microseconds", Availability: oldestAvailability, Labels: labels, Value: snapshot.OldestAgeUS},
	}
	for index, outcome := range accountingOutcomes {
		outcomeLabels := append(append([]Label(nil), labels...), Label{Name: "outcome", Value: string(outcome)})
		distribution := snapshot.Duration[index]
		metrics = append(metrics,
			Metric{Name: "wait_completed", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Labels: outcomeLabels, Value: snapshot.Completed[index]},
			Metric{Name: "wait_duration", Kind: MetricHistogram, Unit: "microseconds", Availability: AvailabilityExact, Labels: outcomeLabels, Count: distribution.Count, Sum: distribution.Sum, Maximum: distribution.Maximum, BucketUpper: distribution.BucketUpper, BucketCounts: distribution.BucketCounts},
		)
	}
	return metrics
}

func (metric *WaitMetric) Dropped() uint64 {
	if metric == nil {
		return 0
	}
	return metric.dropped.Load()
}

func (metric *WaitMetric) touched() bool {
	return metric != nil && metric.enabled && metric.attempts.Load() != 0
}

func outcomeIndex(outcome Outcome) int {
	for index := range accountingOutcomes {
		if accountingOutcomes[index] == outcome {
			return index
		}
	}
	return 2
}

func emptyDistribution(bounds []uint64) DistributionSnapshot {
	return DistributionSnapshot{BucketUpper: append([]uint64(nil), bounds...), BucketCounts: make([]uint64, len(bounds))}
}
