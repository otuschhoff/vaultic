package repository

import (
	"context"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestPhase34M3RepositoryReadAttributesM2ServiceStall(t *testing.T) {
	profile := telemetry.ExperimentProfile{
		SchemaVersion: telemetry.ExperimentSchemaVersion, ProfileID: "m3-repository-read", Enabled: true, TestOnly: true,
		Scenario: "local", Backend: "local", Mode: telemetry.DelayService, Operation: "restore", Role: "repository", Method: "get",
		AccessPattern: "sequential", TargetID: "repository-fixture", ResourceID: "repository-device", Placement: "inside_service",
		Latency: telemetry.LatencyServiceCompletion, Interpretation: "additive", Endpoint: "dependency", Acknowledgement: "unknown",
		DelayUS: 2_000, Concurrency: 1, DeadlineMS: 30_000, RetryError: "none", Seed: 34, Holds: []string{"backend_capacity"},
	}
	controller, err := telemetry.NewScenarioHarness().Controller(profile, telemetry.ExperimentTarget{ID: profile.TargetID, Disposable: true, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	storage := mem.New()
	payload := []byte("repository payload")
	id := vaultic.Hash(payload)
	handle := backend.Handle{Type: backend.PackFile, Name: id.String()}
	if err := storage.Save(t.Context(), handle, backend.NewByteReader(payload, storage.Hasher())); err != nil {
		t.Fatal(err)
	}
	accounting := telemetry.NewProductionAccounting(true)
	repo, err := New(telemetry.WrapExperimentBackend(storage, controller), Options{Accounting: accounting})
	if err != nil {
		t.Fatal(err)
	}
	ctx, action := accounting.StartOperation(context.Background(), "restore", "read", "")
	loaded, err := repo.LoadRaw(ctx, vaultic.PackFile, id)
	action.Done(telemetry.ClassifyOutcome(err))
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded) != string(payload) {
		t.Fatalf("payload = %q", loaded)
	}
	if observation := controller.Observation(); observation.Completed != 1 || observation.Active != 0 {
		t.Fatalf("scenario observation = %+v", observation)
	}
	metrics, active, _, _ := accounting.Snapshot(telemetry.MaxMonitorMetrics)
	if len(active) != 0 {
		t.Fatalf("active operations = %#v", active)
	}
	latency := phase34M3Metric(metrics, "dependency_latency", "operation", "restore", "role", "repository", "outcome", "success")
	if latency.Count != 1 || latency.Sum < uint64((2*time.Millisecond)/time.Microsecond) {
		t.Fatalf("repository latency = count:%d sum:%d", latency.Count, latency.Sum)
	}
	if bytes := phase34M3Metric(metrics, "dependency_bytes", "operation", "restore", "role", "repository", "outcome", "success").Value; bytes != uint64(len(payload)) {
		t.Fatalf("repository bytes = %d", bytes)
	}
}

func phase34M3Metric(metrics []telemetry.Metric, name string, labels ...string) telemetry.Metric {
	for _, metric := range metrics {
		if metric.Name != name {
			continue
		}
		matched := true
		for index := 0; index < len(labels); index += 2 {
			found := false
			for _, label := range metric.Labels {
				if label.Name == labels[index] && label.Value == labels[index+1] {
					found = true
					break
				}
			}
			if !found {
				matched = false
				break
			}
		}
		if matched {
			return metric
		}
	}
	return telemetry.Metric{}
}
