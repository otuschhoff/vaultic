package maintenance

import (
	"time"

	monitor "github.com/otuschhoff/vaultic/internal/telemetry"
)

type CheckTelemetry struct {
	action             *monitor.ActionMetric
	rpcWait            *monitor.WaitMetric
	database           *monitor.DependencyMetric
	scratch            *monitor.DependencyMetric
	scratchSort        *monitor.DependencyMetric
	scratchEncodeWrite *monitor.DependencyMetric
	scratchFlush       *monitor.DependencyMetric
	scratchSync        *monitor.DependencyMetric
}

func NewCheckTelemetry() *CheckTelemetry { return NewCheckTelemetryEnabled(true) }

func NewCheckTelemetryEnabled(enabled bool) *CheckTelemetry {
	return &CheckTelemetry{
		action:             monitor.NewActionMetric("check", 1, enabled),
		rpcWait:            monitor.NewWaitMetric("check", "database", "concurrency", monitor.MaxActiveWaits, enabled),
		database:           monitor.NewDependencyMetric("check", "database", enabled),
		scratch:            monitor.NewDependencyMetric("check", "scratch", enabled),
		scratchSort:        monitor.NewDependencyMetric("check", "scratch", enabled),
		scratchEncodeWrite: monitor.NewDependencyMetric("check", "scratch", enabled),
		scratchFlush:       monitor.NewDependencyMetric("check", "scratch", enabled),
		scratchSync:        monitor.NewDependencyMetric("check", "scratch", enabled),
	}
}

func (telemetry *CheckTelemetry) start() *monitor.ActionGuard {
	if telemetry == nil {
		return nil
	}
	return telemetry.action.Start("planning", "")
}

func (telemetry *CheckTelemetry) progress(operation *monitor.ActionGuard, update CheckProgress) {
	if operation == nil {
		return
	}
	phase := "verify"
	switch update.Stage {
	case "inventory":
		phase = "planning"
	case "legacy_scan", "slatedb_scan", "encryption_audit":
		phase = "read"
	case "catalog_join", "parallel_validation":
		phase = "verify"
	case "slatedb_finalize", "finalization":
		phase = "finalize"
	}
	operation.Progress(phase, "", update.ScratchPeakBytes, update.ScratchLimitBytes)
}

func (telemetry *CheckTelemetry) process(operation *monitor.ActionGuard, role string, bytes uint64) {
	if operation != nil {
		operation.Processed(role, bytes)
	}
}

func (telemetry *CheckTelemetry) startScratch() *monitor.DependencyGuard {
	if telemetry == nil {
		return nil
	}
	return telemetry.scratch.Start()
}

func (telemetry *CheckTelemetry) processScratch(operation *monitor.ActionGuard, request *monitor.DependencyGuard, bytes uint64) {
	telemetry.process(operation, "scratch", bytes)
	request.AddBytes(bytes)
}

func (telemetry *CheckTelemetry) finish(operation *monitor.ActionGuard, result CheckResult, err error) {
	if operation == nil {
		return
	}
	operation.Progress("complete", "", result.Resources.ScratchPeakBytes, result.Resources.ScratchLimitBytes)
	operation.Done(monitor.ClassifyOutcome(err))
}

func (telemetry *CheckTelemetry) Component(now time.Time) monitor.ComponentSnapshot {
	component := monitor.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: monitor.ProcessStartID(), CapturedUnixMS: now.UnixMilli(), Availability: monitor.AvailabilityExact,
	}
	if telemetry == nil {
		component.Availability = monitor.AvailabilityUnavailable
		return component
	}
	metrics, operations, overflow := telemetry.action.Snapshot()
	component.Metrics = append(component.Metrics, metrics...)
	component.Metrics = append(component.Metrics, telemetry.rpcWait.Metrics()...)
	component.Metrics = append(component.Metrics, telemetry.database.Metrics()...)
	component.Metrics = append(component.Metrics, telemetry.scratch.Metrics()...)
	for _, stage := range []struct {
		name   string
		metric *monitor.DependencyMetric
	}{
		{"sort", telemetry.scratchSort}, {"encode_write", telemetry.scratchEncodeWrite},
		{"flush", telemetry.scratchFlush}, {"sync", telemetry.scratchSync},
	} {
		for _, metric := range stage.metric.Metrics() {
			metric.Name = "check_scratch_" + stage.name + "_" + metric.Name
			component.Metrics = append(component.Metrics, metric)
		}
	}
	component.Operations = operations
	component.OperationOverflow = overflow
	component.CardinalityDropped = telemetry.rpcWait.Dropped()
	return component
}

func settleDependency(guard *monitor.DependencyGuard, err error) {
	switch monitor.ClassifyOutcome(err) {
	case monitor.OutcomeSuccess:
		guard.Succeeded()
	case monitor.OutcomeFailure:
		guard.Failed()
	case monitor.OutcomeTimeout:
		guard.TimedOut()
	}
	guard.Done()
}
