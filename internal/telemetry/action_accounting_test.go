package telemetry

import (
	"sync/atomic"
	"testing"
)

var accountingBenchmarkSink any

func TestActionMetricPublishesBoundedLifecycleAndProgress(t *testing.T) {
	metric := NewActionMetric("legacy_import", 1, true)
	guard := metric.Start("source", "")
	guard.Progress("ingest", "rpc_response", 4, 10)
	guard.Processed("database", 128)
	guard.Processed("broker", 999)
	metrics, operations, overflow := metric.Snapshot()
	if len(operations) != 1 || operations[0].Phase != "ingest" || operations[0].BlockingReason != "rpc_response" || operations[0].CompletedUnits != 4 || len(overflow) != 0 {
		t.Fatalf("active operation = %+v overflow=%+v", operations, overflow)
	}
	guard.Done(OutcomeSuccess)
	guard.Done(OutcomeFailure)
	metrics, operations, overflow = metric.Snapshot()
	component := ComponentSnapshot{Component: "vaultic", ProcessStartID: "test", CapturedUnixMS: 1, Availability: AvailabilityExact, Metrics: metrics, Operations: operations, OperationOverflow: overflow}
	if err := ValidateVaulticDBComponent(component); err != nil {
		t.Fatal(err)
	}
	if metricValue(metrics, "operation_started", "") != 1 || metricValue(metrics, "operation_completed", "success") != 1 || metricValue(metrics, "operation_completed", "failure") != 0 || metricValue(metrics, "operation_processed_bytes", "database") != 128 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestActionMetricOverflowDoesNotCreateLifecycleStart(t *testing.T) {
	metric := NewActionMetric("check", 1, true)
	first := metric.Start("planning", "")
	second := metric.Start("planning", "")
	second.Done(OutcomeSuccess)
	metrics, operations, overflow := metric.Snapshot()
	if len(operations) != 1 || len(overflow) != 1 || overflow[0].Class != "check" || overflow[0].Count != 1 || metricValue(metrics, "operation_started", "") != 2 || metricValue(metrics, "operation_active", "") != 1 || metricValue(metrics, "operation_completed", "success") != 1 {
		t.Fatalf("metrics=%+v operations=%+v overflow=%+v", metrics, operations, overflow)
	}
	first.Done(OutcomeCancellation)
}

func TestDependencyMetricSeparatesNestedOutcomeAndBytes(t *testing.T) {
	metric := NewDependencyMetric("check", "database", true)
	var now atomic.Int64
	now.Store(1)
	metric.now = now.Load
	outer := metric.Start()
	now.Store(11)
	inner := metric.Start()
	inner.AddBytes(7)
	inner.Failed()
	now.Store(21)
	inner.Done()
	outer.AddBytes(11)
	outer.Succeeded()
	now.Store(31)
	outer.Done()
	metrics := metric.Metrics()
	component := ComponentSnapshot{Component: "vaultic", ProcessStartID: "test", CapturedUnixMS: 1, Availability: AvailabilityExact, Metrics: metrics}
	if err := ValidateVaulticDBComponent(component); err != nil {
		t.Fatal(err)
	}
	if metricValue(metrics, "dependency_requests", "success") != 1 || metricValue(metrics, "dependency_requests", "failure") != 1 || metricValue(metrics, "dependency_bytes", "success") != 11 || metricValue(metrics, "dependency_bytes", "failure") != 7 {
		t.Fatalf("metrics = %+v", metrics)
	}
	if histogramCount(metrics, "dependency_latency", "success") != 1 || histogramCount(metrics, "dependency_latency", "failure") != 1 {
		t.Fatalf("dependency histograms = %+v", metrics)
	}
}

func TestDependencyGuardDefaultsToCancellationAndSettlesOnce(t *testing.T) {
	metric := NewDependencyMetric("check", "scratch", true)
	guard := metric.Start()
	guard.Done()
	guard.Done()
	if got := metricValue(metric.Metrics(), "dependency_requests", "cancellation"); got != 1 {
		t.Fatalf("cancellations = %d", got)
	}
}

func TestDisabledActionAndDependencyAreAllocationFree(t *testing.T) {
	action := NewActionMetric("check", 1, false)
	dependency := NewDependencyMetric("check", "database", false)
	if allocations := testing.AllocsPerRun(100, func() {
		operation := action.Start("planning", "")
		operation.Progress("read", "", 1, 2)
		operation.Processed("database", 1)
		operation.Done(OutcomeSuccess)
		request := dependency.Start()
		request.AddBytes(1)
		request.Succeeded()
		request.Done()
	}); allocations != 0 {
		t.Fatalf("disabled allocations = %v", allocations)
	}
}

func BenchmarkActionAccounting(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(b *testing.B) {
			metric := NewActionMetric("check", MaxMonitorOperations, enabled)
			b.ReportAllocs()
			for b.Loop() {
				guard := metric.Start("read", "")
				guard.Progress("verify", "", 1, 1)
				guard.Processed("database", 128)
				guard.Done(OutcomeSuccess)
			}
		})
	}
}

func BenchmarkDependencyAccounting(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(b *testing.B) {
			metric := NewDependencyMetric("check", "database", enabled)
			b.ReportAllocs()
			for b.Loop() {
				guard := metric.Start()
				guard.AddBytes(128)
				guard.Succeeded()
				guard.Done()
			}
		})
	}
}

func BenchmarkAccountingConstruction(b *testing.B) {
	constructors := map[string]func(bool) any{
		"action": func(enabled bool) any { return NewActionMetric("check", MaxMonitorOperations, enabled) },
		"correlation": func(enabled bool) any {
			return NewCorrelationSampler("check", "database", 1, MaxSampledCorrelations, enabled)
		},
		"dependency": func(enabled bool) any { return NewDependencyMetric("check", "database", enabled) },
		"wait": func(enabled bool) any {
			return NewWaitMetric("check", "database", "concurrency", MaxActiveWaits, enabled)
		},
	}
	for name, constructor := range constructors {
		for _, enabled := range []bool{false, true} {
			b.Run(name+"/"+map[bool]string{false: "disabled", true: "enabled"}[enabled], func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					accountingBenchmarkSink = constructor(enabled)
				}
			})
		}
	}
}

func metricValue(metrics []Metric, name, discriminator string) uint64 {
	for _, metric := range metrics {
		if metric.Name != name {
			continue
		}
		for _, label := range metric.Labels {
			if label.Value == discriminator {
				return metric.Value
			}
		}
		if discriminator == "" {
			return metric.Value
		}
	}
	return 0
}

func histogramCount(metrics []Metric, name, outcome string) uint64 {
	for _, metric := range metrics {
		if metric.Name == name {
			for _, label := range metric.Labels {
				if label.Name == "outcome" && label.Value == outcome {
					return metric.Count
				}
			}
		}
	}
	return 0
}
