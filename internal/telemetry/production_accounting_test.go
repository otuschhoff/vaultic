package telemetry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProductionAccountingAttributesOnlyContextOwner(t *testing.T) {
	accounting := NewProductionAccounting(true)
	ctx, action := accounting.StartOperation(context.Background(), "backup", "upload", "")
	dependency := accounting.StartDependency(ctx, "repository")
	dependency.AddBytes(4096)

	metrics, active, _, dropped := accounting.Snapshot(MaxMonitorMetrics)
	if dropped != 0 {
		t.Fatalf("cardinality dropped = %d", dropped)
	}
	if len(active) != 1 || active[0].Class != "backup" || active[0].Phase != "upload" {
		t.Fatalf("active operation = %#v", active)
	}
	if value := productionMetricValue(metrics, "dependency_requests", "operation", "backup", "role", "repository", "outcome", "success"); value != 0 {
		t.Fatalf("completed requests while dependency active = %d", value)
	}

	dependency.Finish(nil)
	action.Processed("repository", 4096)
	action.Done(OutcomeSuccess)
	metrics, active, _, _ = accounting.Snapshot(MaxMonitorMetrics)
	if len(active) != 0 {
		t.Fatalf("active operation after completion = %#v", active)
	}
	if value := productionMetricValue(metrics, "dependency_requests", "operation", "backup", "role", "repository", "outcome", "success"); value != 1 {
		t.Fatalf("successful repository requests = %d", value)
	}
	if value := productionMetricValue(metrics, "dependency_bytes", "operation", "backup", "role", "repository", "outcome", "success"); value != 4096 {
		t.Fatalf("successful repository bytes = %d", value)
	}

	unowned := accounting.StartDependency(context.Background(), "repository")
	unowned.AddBytes(99)
	unowned.Finish(errors.New("unowned"))
	metrics, _, _, _ = accounting.Snapshot(MaxMonitorMetrics)
	if value := productionMetricValue(metrics, "dependency_requests", "operation", "backup", "role", "repository", "outcome", "failure"); value != 0 {
		t.Fatalf("unowned failure requests = %d", value)
	}
}

func TestProductionAccountingCapsSnapshotAndDisclosesDroppedSeries(t *testing.T) {
	accounting := NewProductionAccounting(true)
	ctx, action := accounting.StartOperation(context.Background(), "backup", "upload", "")
	dependency := accounting.StartDependency(ctx, "repository")
	dependency.Finish(nil)
	action.Done(OutcomeSuccess)
	metrics, _, _, dropped := accounting.Snapshot(1)
	if len(metrics) != 1 || dropped == 0 {
		t.Fatalf("metrics=%d dropped=%d", len(metrics), dropped)
	}
}

func TestProductionAccountingSharesOperationCapacityAcrossClasses(t *testing.T) {
	accounting := NewProductionAccounting(true)
	guards := make([]*ActionGuard, 0, MaxMonitorOperations+2)
	for index := 0; index < MaxMonitorOperations/2+1; index++ {
		_, guard := accounting.StartOperation(context.Background(), "backup", "source", "")
		guards = append(guards, guard)
	}
	for index := 0; index < MaxMonitorOperations/2+1; index++ {
		_, guard := accounting.StartOperation(context.Background(), "restore", "read", "")
		guards = append(guards, guard)
	}

	_, active, overflow, _ := accounting.Snapshot(MaxMonitorMetrics)
	if len(active) != MaxMonitorOperations {
		t.Fatalf("active operations = %d, want %d", len(active), MaxMonitorOperations)
	}
	seen := make(map[string]struct{}, len(active))
	for _, operation := range active {
		if _, duplicate := seen[operation.ID]; duplicate {
			t.Fatalf("duplicate operation ID %q", operation.ID)
		}
		seen[operation.ID] = struct{}{}
	}
	if len(overflow) != 1 || overflow[0].Class != "restore" || overflow[0].Count != 2 {
		t.Fatalf("operation overflow = %#v", overflow)
	}
	component := ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "test", CapturedUnixMS: time.Now().UnixMilli(),
		Availability: AvailabilityExact, Operations: active, OperationOverflow: overflow,
	}
	if err := NewMonitorSnapshot(time.Now(), component).Validate(); err != nil {
		t.Fatalf("maximum-pressure monitor snapshot: %v", err)
	}
	for _, guard := range guards {
		guard.Done(OutcomeSuccess)
	}
}

func TestProductionAccountingDistinguishesHumanConfirmationWait(t *testing.T) {
	accounting := NewProductionAccounting(true)
	ctx, action := accounting.StartOperation(context.Background(), "key_management", "wait", "")
	done := accounting.StartBlocking(ctx, "wait", "human_confirmation")
	wait := accounting.StartWait(ctx, "coordination", "none")
	_, active, _, _ := accounting.Snapshot(MaxMonitorMetrics)
	if len(active) != 1 || active[0].BlockingReason != "human_confirmation" {
		t.Fatalf("active human confirmation = %#v", active)
	}
	done.Done()
	_, active, _, _ = accounting.Snapshot(MaxMonitorMetrics)
	if len(active) != 1 || active[0].BlockingReason != "" {
		t.Fatalf("settled human confirmation = %#v", active)
	}
	wait.Finish(nil)
	action.Done(OutcomeSuccess)
	metrics, _, _, _ := accounting.Snapshot(MaxMonitorMetrics)
	if value := productionMetricValue(metrics, "wait_completed", "operation", "key_management", "role", "coordination", "throttle", "none", "outcome", "success"); value != 1 {
		t.Fatalf("completed human confirmation waits = %d", value)
	}
	if value := productionMetricValue(metrics, "wait_completed", "operation", "key_management", "role", "broker", "throttle", "none", "outcome", "success"); value != 0 {
		t.Fatalf("broker response waits = %d", value)
	}
}

func TestProductionAccountingRetainsOverlappingBlocker(t *testing.T) {
	accounting := NewProductionAccounting(true)
	ctx, action := accounting.StartOperation(context.Background(), "backup", "planning", "")
	sourceDone := accounting.StartBlocking(ctx, "source", "source_io")
	backendDone := accounting.StartBlocking(ctx, "upload", "backend_io")
	backendDone.Done()
	_, active, _, _ := accounting.Snapshot(MaxMonitorMetrics)
	if len(active) != 1 || active[0].Phase != "source" || active[0].BlockingReason != "source_io" {
		t.Fatalf("fallback blocker = %#v", active)
	}
	sourceDone.Done()
	action.Done(OutcomeSuccess)
}

func TestProductionAccountingRetainsPhaseForSameBlockingReason(t *testing.T) {
	accounting := NewProductionAccounting(true)
	ctx, action := accounting.StartOperation(context.Background(), "backup", "planning", "")
	readDone := accounting.StartBlocking(ctx, "read", "backend_io")
	uploadDone := accounting.StartBlocking(ctx, "upload", "backend_io")
	uploadDone.Done()
	_, active, _, _ := accounting.Snapshot(MaxMonitorMetrics)
	if len(active) != 1 || active[0].Phase != "read" || active[0].BlockingReason != "backend_io" {
		t.Fatalf("same-reason fallback blocker = %#v", active)
	}
	readDone.Done()
	action.Done(OutcomeSuccess)
}

func BenchmarkProductionDependencyAccounting(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(b *testing.B) {
			accounting := NewProductionAccounting(enabled)
			ctx, action := accounting.StartOperation(context.Background(), "backup", "upload", "")
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				dependency := accounting.StartDependency(ctx, "repository")
				dependency.AddBytes(4096)
				dependency.Finish(nil)
			}
			b.StopTimer()
			action.Done(OutcomeSuccess)
		})
	}
}

func productionMetricValue(metrics []Metric, name string, labels ...string) uint64 {
	for _, metric := range metrics {
		if metric.Name != name {
			continue
		}
		matches := true
		for index := 0; index < len(labels); index += 2 {
			if labelValue(metric.Labels, labels[index]) != labels[index+1] {
				matches = false
				break
			}
		}
		if matches {
			return metric.Value
		}
	}
	return 0
}
