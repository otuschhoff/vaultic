package telemetry

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/fs"
)

func TestProductionFSAttributesSourceBytesAndPreservesResult(t *testing.T) {
	accounting := NewProductionAccounting(true)
	ctx, action := accounting.StartOperation(context.Background(), "backup", "source", "")
	source, err := fs.NewReader("fixture", io.NopCloser(bytes.NewReader([]byte("payload"))), fs.ReaderOptions{Size: 7})
	if err != nil {
		t.Fatal(err)
	}
	file, err := WrapProductionFS(ctx, source, accounting).OpenFile("fixture", fs.O_RDONLY, false)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "payload" {
		t.Fatalf("payload = %q", payload)
	}
	action.Done(OutcomeSuccess)
	metrics, _, _, _ := accounting.Snapshot(MaxMonitorMetrics)
	if value := productionMetricValue(metrics, "dependency_bytes", "operation", "backup", "role", "source", "outcome", "success"); value != 7 {
		t.Fatalf("source dependency bytes = %d", value)
	}
	if value := productionMetricValue(metrics, "operation_processed_bytes", "operation", "backup", "role", "source"); value != 7 {
		t.Fatalf("source processed bytes = %d", value)
	}
	if value := productionMetricValue(metrics, "dependency_requests", "operation", "backup", "role", "source", "outcome", "failure"); value != 0 {
		t.Fatalf("source failure requests = %d", value)
	}
}

func TestProductionFSPreservesLocalCapability(t *testing.T) {
	accounting := NewProductionAccounting(true)
	ctx, action := accounting.StartOperation(context.Background(), "backup", "source", "")
	wrapped := WrapProductionFS(ctx, fs.NewLocal(), accounting)
	if !fs.IsLocal(wrapped) {
		t.Fatal("production wrapper hid local filesystem capability")
	}
	action.Done(OutcomeSuccess)
}

func TestProductionFSAttributesM2ServiceStall(t *testing.T) {
	profile := validExperimentProfile()
	profile.Scenario = "local"
	profile.Backend = "local"
	profile.Operation = "backup"
	profile.Role = "source"
	profile.Method = "read"
	profile.AccessPattern = "sequential"
	profile.TargetID = "source-fixture"
	profile.ResourceID = "source-device"
	profile.Mode = DelayService
	profile.Placement = "inside_service"
	profile.Latency = LatencyServiceCompletion
	profile.Endpoint = "dependency"
	profile.Acknowledgement = "unknown"
	profile.DelayUS = 2_000
	profile.Concurrency = 1
	profile.Holds = []string{"backend_capacity"}
	controller, err := NewScenarioHarness().Controller(profile, ExperimentTarget{ID: profile.TargetID, Disposable: true, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	accounting := NewProductionAccounting(true)
	ctx, action := accounting.StartOperation(context.Background(), "backup", "source", "")
	source, err := fs.NewReader("fixture", io.NopCloser(bytes.NewReader([]byte("payload"))), fs.ReaderOptions{Size: 7})
	if err != nil {
		t.Fatal(err)
	}
	source = WrapExperimentFS(ctx, source, controller)
	file, err := WrapProductionFS(ctx, source, accounting).OpenFile("fixture", fs.O_RDONLY, false)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	action.Done(OutcomeSuccess)
	if string(payload) != "payload" {
		t.Fatalf("payload = %q", payload)
	}
	if observation := controller.Observation(); observation.Completed != 1 || observation.Active != 0 {
		t.Fatalf("scenario observation = %+v", observation)
	}
	metrics, _, _, _ := accounting.Snapshot(MaxMonitorMetrics)
	latency := productionMetric(metrics, "dependency_latency", "operation", "backup", "role", "source", "outcome", "success")
	if latency.Count == 0 || latency.Sum < uint64((2*time.Millisecond)/time.Microsecond) {
		t.Fatalf("source latency = count:%d sum:%d", latency.Count, latency.Sum)
	}
}

func productionMetric(metrics []Metric, name string, labels ...string) Metric {
	for _, metric := range metrics {
		if metric.Name != name {
			continue
		}
		matched := true
		for index := 0; index < len(labels); index += 2 {
			if labelValue(metric.Labels, labels[index]) != labels[index+1] {
				matched = false
				break
			}
		}
		if matched {
			return metric
		}
	}
	return Metric{}
}

func BenchmarkProductionFSRead(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(b *testing.B) {
			accounting := NewProductionAccounting(enabled)
			ctx, action := accounting.StartOperation(context.Background(), "backup", "source", "")
			buffer := make([]byte, 7)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				source, err := fs.NewReader("fixture", io.NopCloser(bytes.NewReader([]byte("payload"))), fs.ReaderOptions{Size: 7})
				if err != nil {
					b.Fatal(err)
				}
				file, err := WrapProductionFS(ctx, source, accounting).OpenFile("fixture", fs.O_RDONLY, false)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(file, buffer); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			action.Done(OutcomeSuccess)
		})
	}
}
