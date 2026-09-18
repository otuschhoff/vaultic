package telemetry

import (
	"sort"
	"sync/atomic"
	"time"
)

type roleCounter struct {
	role  string
	bytes Counter
}

type ActionMetric struct {
	enabled   bool
	operation string
	registry  *OperationRegistry
	started   Counter
	active    atomic.Uint64
	completed [len(accountingOutcomes)]Counter
	processed []roleCounter
}

type ActionGuard struct {
	metric  *ActionMetric
	handle  *OperationHandle
	settled atomic.Bool
}

var disabledActionGuard = &ActionGuard{}

func NewActionMetric(operation string, activeCapacity int, enabled bool) *ActionMetric {
	var roles []string
	for role := range monitorValues("role") {
		if validOperationRole(operation, role) {
			roles = append(roles, role)
		}
	}
	if len(roles) == 0 {
		panic("telemetry action metric requires a bounded operation")
	}
	if activeCapacity <= 0 || activeCapacity > MaxMonitorOperations {
		panic("telemetry action metric active capacity is out of bounds")
	}
	sort.Strings(roles)
	metric := &ActionMetric{enabled: enabled, operation: operation, processed: make([]roleCounter, len(roles))}
	for index, role := range roles {
		metric.processed[index].role = role
	}
	if enabled {
		metric.registry = NewOperationRegistry(activeCapacity)
	}
	return metric
}

func (metric *ActionMetric) Start(phase, parentID string) *ActionGuard {
	if metric == nil || !metric.enabled {
		return disabledActionGuard
	}
	handle := metric.registry.Start(metric.operation, phase, parentID)
	metric.started.Add(1)
	metric.active.Add(1)
	return &ActionGuard{metric: metric, handle: handle}
}

func (guard *ActionGuard) Progress(phase, blockingReason string, completed, expected uint64) {
	if guard != nil && guard.handle != nil && !guard.settled.Load() {
		guard.handle.Progress(phase, blockingReason, completed, expected)
	}
}

func (guard *ActionGuard) Processed(role string, bytes uint64) {
	if guard == nil || guard.metric == nil || guard.settled.Load() {
		return
	}
	for index := range guard.metric.processed {
		if guard.metric.processed[index].role == role {
			guard.metric.processed[index].bytes.Add(bytes)
			return
		}
	}
}

func (guard *ActionGuard) Done(outcome Outcome) {
	if guard == nil || guard.metric == nil || !guard.settled.CompareAndSwap(false, true) {
		return
	}
	guard.metric.completed[outcomeIndex(outcome)].Add(1)
	guard.metric.active.Add(^uint64(0))
	guard.handle.Done()
}

func (metric *ActionMetric) Snapshot() ([]Metric, []ActiveOperation, []OperationOverflowSnapshot) {
	if metric == nil || !metric.enabled {
		return nil, nil, nil
	}
	operations, overflow := metric.registry.Snapshot()
	metrics := []Metric{
		{Name: "operation_started", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Labels: []Label{{Name: "operation", Value: metric.operation}}, Value: metric.started.Load()},
		{Name: "operation_active", Kind: MetricGauge, Unit: "operations", Availability: AvailabilityExact, Labels: []Label{{Name: "operation", Value: metric.operation}}, Value: metric.active.Load()},
	}
	for index, outcome := range accountingOutcomes {
		metrics = append(metrics, Metric{Name: "operation_completed", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Labels: []Label{{Name: "operation", Value: metric.operation}, {Name: "outcome", Value: string(outcome)}}, Value: metric.completed[index].Load()})
	}
	for index := range metric.processed {
		metrics = append(metrics, Metric{Name: "operation_processed_bytes", Kind: MetricCounter, Unit: "bytes", Availability: AvailabilityExact, Labels: []Label{{Name: "operation", Value: metric.operation}, {Name: "role", Value: metric.processed[index].role}}, Value: metric.processed[index].bytes.Load()})
	}
	return metrics, operations, overflow
}

type DependencyMetric struct {
	enabled   bool
	operation string
	role      string
	requests  [len(accountingOutcomes)]Counter
	bytes     [len(accountingOutcomes)]Counter
	duration  [len(accountingOutcomes)]*FixedDistribution
	now       func() int64
}

type DependencyGuard struct {
	metric  *DependencyMetric
	started int64
	bytes   uint64
	outcome atomic.Uint32
	settled atomic.Bool
}

var disabledDependencyGuard = &DependencyGuard{}

func NewDependencyMetric(operation, role string, enabled bool) *DependencyMetric {
	if !validOperationRole(operation, role) {
		panic("telemetry dependency metric requires a supported operation and role")
	}
	metric := &DependencyMetric{enabled: enabled, operation: operation, role: role, now: func() int64 { return max(time.Since(accountingEpoch).Microseconds()+1, 1) }}
	for index := range metric.duration {
		metric.duration[index] = NewFixedDistribution(vaulticLatencyBounds())
	}
	return metric
}

func (metric *DependencyMetric) Start() *DependencyGuard {
	if metric == nil || !metric.enabled {
		return disabledDependencyGuard
	}
	guard := &DependencyGuard{metric: metric, started: metric.now()}
	guard.outcome.Store(uint32(outcomeIndex(OutcomeCancellation)))
	return guard
}

func (guard *DependencyGuard) AddBytes(bytes uint64) {
	if guard != nil && guard.metric != nil && !guard.settled.Load() {
		guard.bytes = saturatingAdd(guard.bytes, bytes)
	}
}

func (guard *DependencyGuard) Succeeded() { guard.setOutcome(OutcomeSuccess) }
func (guard *DependencyGuard) Failed()    { guard.setOutcome(OutcomeFailure) }
func (guard *DependencyGuard) TimedOut()  { guard.setOutcome(OutcomeTimeout) }

func (guard *DependencyGuard) setOutcome(outcome Outcome) {
	if guard != nil && guard.metric != nil && !guard.settled.Load() {
		guard.outcome.Store(uint32(outcomeIndex(outcome)))
	}
}

func (guard *DependencyGuard) Done() {
	if guard == nil || guard.metric == nil || !guard.settled.CompareAndSwap(false, true) {
		return
	}
	index := int(guard.outcome.Load())
	guard.metric.requests[index].Add(1)
	guard.metric.bytes[index].Add(guard.bytes)
	guard.metric.duration[index].Observe(uint64(max(guard.metric.now()-guard.started, 0)))
}

func (metric *DependencyMetric) Metrics() []Metric {
	if metric == nil || !metric.enabled {
		return nil
	}
	metrics := make([]Metric, 0, len(accountingOutcomes)*3)
	for index, outcome := range accountingOutcomes {
		labels := []Label{{Name: "operation", Value: metric.operation}, {Name: "role", Value: metric.role}, {Name: "outcome", Value: string(outcome)}}
		distribution := metric.duration[index].Snapshot()
		metrics = append(metrics,
			Metric{Name: "dependency_requests", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Labels: labels, Value: metric.requests[index].Load()},
			Metric{Name: "dependency_bytes", Kind: MetricCounter, Unit: "bytes", Availability: AvailabilityExact, Labels: labels, Value: metric.bytes[index].Load()},
			Metric{Name: "dependency_latency", Kind: MetricHistogram, Unit: "microseconds", Availability: AvailabilityExact, Labels: labels, Count: distribution.Count, Sum: distribution.Sum, Maximum: distribution.Maximum, BucketUpper: distribution.BucketUpper, BucketCounts: distribution.BucketCounts},
		)
	}
	return metrics
}
