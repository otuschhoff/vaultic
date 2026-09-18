package telemetry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClassifyOutcomeRecognizesContextAndGRPCStatus(t *testing.T) {
	tests := []struct {
		err  error
		want Outcome
	}{
		{nil, OutcomeSuccess},
		{context.Canceled, OutcomeCancellation},
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded), OutcomeTimeout},
		{status.Error(codes.Canceled, "canceled"), OutcomeCancellation},
		{status.Error(codes.DeadlineExceeded, "deadline"), OutcomeTimeout},
		{errors.New("failed"), OutcomeFailure},
	}
	for _, test := range tests {
		if got := ClassifyOutcome(test.err); got != test.want {
			t.Errorf("ClassifyOutcome(%v) = %q, want %q", test.err, got, test.want)
		}
	}
}

func TestWaitMetricTracksStallsOutcomesAndOverflow(t *testing.T) {
	metric := NewWaitMetric("legacy_import", "database", "capacity", 1, true)
	var now atomic.Int64
	now.Store(1)
	metric.now = now.Load
	first := metric.Start()
	first.Contended()
	now.Store(101)
	second := metric.Start()
	snapshot := metric.Snapshot()
	if snapshot.Attempts != 2 || snapshot.Contentions != 1 || snapshot.Active != 2 || snapshot.OldestAgeUS != 100 || snapshot.ActiveDropped != 1 {
		t.Fatalf("active snapshot = %+v", snapshot)
	}
	second.TimedOut()
	second.Done()
	first.Succeeded()
	first.Done()
	first.Done()
	snapshot = metric.Snapshot()
	if snapshot.Active != 0 || snapshot.Completed[outcomeIndex(OutcomeSuccess)] != 1 || snapshot.Completed[outcomeIndex(OutcomeTimeout)] != 1 {
		t.Fatalf("settled snapshot = %+v", snapshot)
	}
	component := ComponentSnapshot{Component: "vaultic", ProcessStartID: "test", CapturedUnixMS: 1, Availability: AvailabilityExact, Metrics: metric.Metrics()}
	if err := ValidateVaulticDBComponent(component); err != nil {
		t.Fatal(err)
	}
}

func TestWaitMetricDroppedGuardDefaultsToCancellationUnderConcurrency(t *testing.T) {
	metric := NewWaitMetric("check", "scratch", "capacity", MaxActiveWaits, true)
	var workers sync.WaitGroup
	for range MaxActiveWaits * 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			guard := metric.Start()
			guard.Done()
		}()
	}
	workers.Wait()
	snapshot := metric.Snapshot()
	if snapshot.Attempts != MaxActiveWaits*4 || snapshot.Completed[outcomeIndex(OutcomeCancellation)] != MaxActiveWaits*4 || snapshot.Active != 0 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestDisabledWaitMetricIsAllocationFreeNoop(t *testing.T) {
	metric := NewWaitMetric("check", "database", "none", 1, false)
	allocations := testing.AllocsPerRun(100, func() {
		guard := metric.Start()
		guard.Contended()
		guard.Succeeded()
		guard.Done()
	})
	if allocations != 0 {
		t.Fatalf("disabled wait allocations = %v", allocations)
	}
	if snapshot := metric.Snapshot(); snapshot.Attempts != 0 || snapshot.Active != 0 {
		t.Fatalf("disabled snapshot = %+v", snapshot)
	}
}

func TestWaitMetricRetainedStateIsBounded(t *testing.T) {
	metric := NewWaitMetric("check", "database", "concurrency", MaxActiveWaits, true)
	if len(metric.active) != MaxActiveWaits || cap(metric.active) != MaxActiveWaits {
		t.Fatalf("active wait capacity = %d/%d, want %d", len(metric.active), cap(metric.active), MaxActiveWaits)
	}
}

func TestWaitMetricOverflowKeepsActiveExactAndMarksAgeEstimated(t *testing.T) {
	metric := NewWaitMetric("check", "database", "concurrency", 1, true)
	first := metric.Start()
	second := metric.Start()
	metrics := metric.Metrics()
	if metricValue(metrics, "wait_active", "") != 2 {
		t.Fatalf("wait metrics = %+v", metrics)
	}
	for _, value := range metrics {
		if value.Name == "wait_oldest_age" && value.Availability != AvailabilityEstimated {
			t.Fatalf("oldest availability = %q, want estimated", value.Availability)
		}
	}
	second.Done()
	first.Done()
}

func BenchmarkWaitAccounting(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(b *testing.B) {
			metric := NewWaitMetric("check", "database", "concurrency", MaxActiveWaits, enabled)
			b.ReportAllocs()
			for b.Loop() {
				guard := metric.Start()
				guard.Contended()
				guard.Succeeded()
				guard.Done()
			}
		})
	}
}
