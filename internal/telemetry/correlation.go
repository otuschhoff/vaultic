package telemetry

import (
	"sync/atomic"
	"time"
)

const MaxSampledCorrelations = 128

type Correlation struct {
	ID         string  `json:"id"`
	ParentID   string  `json:"parent_id,omitempty"`
	Operation  string  `json:"operation"`
	Role       string  `json:"role"`
	Outcome    Outcome `json:"outcome"`
	DurationUS uint64  `json:"duration_us"`
}

type CorrelationSampler struct {
	enabled   bool
	operation string
	role      string
	every     uint64
	sequence  atomic.Uint64
	dropped   Counter
	completed chan Correlation
	now       func() int64
}

type CorrelationGuard struct {
	sampler *CorrelationSampler
	id      string
	parent  string
	started int64
	outcome atomic.Uint32
	settled atomic.Bool
}

var disabledCorrelationGuard = &CorrelationGuard{}

func NewCorrelationSampler(operation, role string, every uint64, capacity int, enabled bool) *CorrelationSampler {
	if !validOperationRole(operation, role) {
		panic("telemetry correlation sampler requires a supported operation and role")
	}
	if every == 0 || capacity <= 0 || capacity > MaxSampledCorrelations {
		panic("telemetry correlation sampler bounds are invalid")
	}
	return &CorrelationSampler{
		enabled: enabled, operation: operation, role: role, every: every,
		completed: make(chan Correlation, capacity),
		now:       func() int64 { return max(time.Since(accountingEpoch).Microseconds()+1, 1) },
	}
}

func (sampler *CorrelationSampler) Start(parentID string) *CorrelationGuard {
	if sampler == nil || !sampler.enabled || parentID != "" && !validOpaqueID(parentID) {
		return disabledCorrelationGuard
	}
	sequence := sampler.sequence.Add(1)
	if sequence%sampler.every != 0 {
		return disabledCorrelationGuard
	}
	guard := &CorrelationGuard{sampler: sampler, id: operationID(sequence), parent: parentID, started: sampler.now()}
	guard.outcome.Store(uint32(outcomeIndex(OutcomeCancellation)))
	return guard
}

func (guard *CorrelationGuard) Succeeded() { guard.setOutcome(OutcomeSuccess) }
func (guard *CorrelationGuard) Failed()    { guard.setOutcome(OutcomeFailure) }
func (guard *CorrelationGuard) TimedOut()  { guard.setOutcome(OutcomeTimeout) }

func (guard *CorrelationGuard) setOutcome(outcome Outcome) {
	if guard != nil && guard.sampler != nil && !guard.settled.Load() {
		guard.outcome.Store(uint32(outcomeIndex(outcome)))
	}
}

func (guard *CorrelationGuard) Done() {
	if guard == nil || guard.sampler == nil || !guard.settled.CompareAndSwap(false, true) {
		return
	}
	correlation := Correlation{
		ID: guard.id, ParentID: guard.parent, Operation: guard.sampler.operation, Role: guard.sampler.role,
		Outcome: accountingOutcomes[guard.outcome.Load()], DurationUS: uint64(max(guard.sampler.now()-guard.started, 0)),
	}
	select {
	case guard.sampler.completed <- correlation:
	default:
		guard.sampler.dropped.Add(1)
	}
}

func (sampler *CorrelationSampler) Drain(limit int) []Correlation {
	if sampler == nil || limit <= 0 {
		return nil
	}
	limit = min(limit, cap(sampler.completed))
	result := make([]Correlation, 0, limit)
	for len(result) < limit {
		select {
		case correlation := <-sampler.completed:
			result = append(result, correlation)
		default:
			return result
		}
	}
	return result
}

func (sampler *CorrelationSampler) Dropped() uint64 {
	if sampler == nil {
		return 0
	}
	return sampler.dropped.Load()
}
