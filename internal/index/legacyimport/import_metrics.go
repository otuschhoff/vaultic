package legacyimport

import (
	"sync"
	"time"
)

type SchedulerSnapshot struct {
	Phase                   string
	PhaseTime               map[string]time.Duration
	LaneTime                [maxPublicationLanes + 1]time.Duration
	ActiveLanes             int
	ReadyBatches            int
	PendingReductionBatches int
	RetainedPreparedBytes   uint64
}

type SchedulerTelemetry struct {
	mu    sync.Mutex
	last  time.Time
	state SchedulerSnapshot
}

func NewSchedulerTelemetry() *SchedulerTelemetry {
	return &SchedulerTelemetry{last: time.Now(), state: SchedulerSnapshot{
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

func (telemetry *SchedulerTelemetry) queues(ready, pending int, retained uint64) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.state.ReadyBatches = ready
	telemetry.state.PendingReductionBatches = pending
	telemetry.state.RetainedPreparedBytes = retained
}

func (telemetry *SchedulerTelemetry) Snapshot() SchedulerSnapshot {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.account(time.Now())
	result := telemetry.state
	result.PhaseTime = make(map[string]time.Duration, len(telemetry.state.PhaseTime))
	for phase, duration := range telemetry.state.PhaseTime {
		result.PhaseTime[phase] = duration
	}
	return result
}
