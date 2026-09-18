package telemetry

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/fs"
)

func TestExperimentFSPreservesReadContent(t *testing.T) {
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
	profile.DelayUS = 1_000
	profile.Concurrency = 1
	profile.DeadlineMS = 0
	profile.MaxRetries = 0
	profile.Holds = []string{"backend_capacity"}
	controller, err := NewScenarioHarness().Controller(profile, ExperimentTarget{ID: "source-fixture", Disposable: true, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	source, err := fs.NewReader("source", io.NopCloser(bytes.NewReader([]byte("payload"))), fs.ReaderOptions{Size: 7})
	if err != nil {
		t.Fatal(err)
	}
	wrapped := WrapExperimentFS(context.Background(), source, controller)
	file, err := wrapped.OpenFile("source", fs.O_RDONLY, false)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "payload" {
		t.Fatalf("content = %q", content)
	}
	if observation := controller.Observation(); observation.Completed == 0 || observation.Active != 0 {
		t.Fatalf("observation = %+v", observation)
	}
}

func TestExperimentFSReadHonorsCancellation(t *testing.T) {
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
	profile.DelayUS = uint64(time.Second / time.Microsecond)
	profile.Concurrency = 1
	profile.DeadlineMS = 0
	profile.MaxRetries = 0
	profile.Holds = []string{"backend_capacity"}
	controller, err := NewScenarioHarness().Controller(profile, ExperimentTarget{ID: "source-fixture", Disposable: true, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	source, err := fs.NewReader("source", io.NopCloser(bytes.NewReader([]byte("payload"))), fs.ReaderOptions{Size: 7})
	if err != nil {
		t.Fatal(err)
	}
	file, err := WrapExperimentFS(ctx, source, controller).OpenFile("source", fs.O_RDONLY, false)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_, err = file.Read(make([]byte, 7))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("read error = %v", err)
	}
	if observation := controller.Observation(); observation.Canceled != 1 || observation.Active != 0 {
		t.Fatalf("observation = %+v", observation)
	}
}
