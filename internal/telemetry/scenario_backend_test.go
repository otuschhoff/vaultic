package telemetry

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	backendcache "github.com/otuschhoff/vaultic/internal/backend/cache"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
)

func backendScenarioProfile(role, method string) ExperimentProfile {
	profile := scenarioProfile()
	profile.Operation = "backup"
	profile.Role = role
	profile.Method = method
	profile.Backend = "local"
	profile.Scenario = "local"
	profile.Acknowledgement = "unknown"
	profile.Mode = DelayService
	profile.Latency = LatencyServiceCompletion
	profile.Placement = "inside_service"
	profile.Endpoint = "dependency"
	profile.Holds = []string{"backend_capacity"}
	profile.DelayUS = 1
	profile.JitterUS, profile.TailDelayUS, profile.TailEvery, profile.CorrelatedFor = 0, 0, 0, 0
	return profile
}

func TestExperimentBackendDisabledReturnsOriginal(t *testing.T) {
	inner := mem.New()
	profile := backendScenarioProfile("repository", "put")
	profile.Enabled, profile.Mode = false, DelayDisabled
	profile.DelayUS, profile.Concurrency, profile.DeadlineMS, profile.MaxRetries = 0, 0, 0, 0
	profile.RetryError = "none"
	profile.Holds = []string{"none"}
	controller, err := NewScenarioHarness().Controller(profile, ExperimentTarget{})
	if err != nil {
		t.Fatal(err)
	}
	if got := WrapExperimentBackend(inner, controller); got != inner {
		t.Fatal("disabled backend was wrapped")
	}
}

func TestExperimentBackendCoversStreamConsumptionAndPreservesBytes(t *testing.T) {
	ctx := context.Background()
	inner := mem.New()
	handle := backend.Handle{Type: backend.PackFile, Name: "private-object-name"}
	payload := []byte("streamed payload")
	if err := inner.Save(ctx, handle, backend.NewByteReader(payload, inner.Hasher())); err != nil {
		t.Fatal(err)
	}
	profile := backendScenarioProfile("repository", "get")
	controller, err := NewScenarioHarness().Controller(profile, confirmedTarget(profile))
	if err != nil {
		t.Fatal(err)
	}
	wrapped := WrapExperimentBackend(inner, controller)
	var got []byte
	err = wrapped.Load(ctx, handle, 0, 0, func(reader io.Reader) error {
		got, err = io.ReadAll(reader)
		return err
	})
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("load bytes=%q err=%v", got, err)
	}
	if observation := controller.Observation(); observation.Completed != 1 || observation.Active != 0 {
		t.Fatalf("stream observation = %+v", observation)
	}
}

func TestExperimentBackendOriginDelayIsBypassedOnCacheHit(t *testing.T) {
	ctx := context.Background()
	inner := mem.New()
	profile := backendScenarioProfile("repository", "get")
	controller, err := NewScenarioHarness().Controller(profile, confirmedTarget(profile))
	if err != nil {
		t.Fatal(err)
	}
	cached := backendcache.TestNewCache(t).Wrap(WrapExperimentBackend(inner, controller), t.Logf)
	handle := backend.Handle{Type: backend.IndexFile, Name: "cached-index"}
	payload := []byte("cached payload")
	if err := cached.Save(ctx, handle, backend.NewByteReader(payload, inner.Hasher())); err != nil {
		t.Fatal(err)
	}
	var got []byte
	if err := cached.Load(ctx, handle, 0, 0, func(reader io.Reader) error {
		got, err = io.ReadAll(reader)
		return err
	}); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("cached load bytes=%q err=%v", got, err)
	}
	if observation := controller.Observation(); observation.Started != 0 || observation.Active != 0 {
		t.Fatalf("cache hit reached injected origin: %+v", observation)
	}
}

func TestExperimentBackendAcknowledgementTimeoutPreservesSave(t *testing.T) {
	inner := mem.New()
	profile := backendScenarioProfile("repository", "put")
	profile.Mode = DelayAcknowledgement
	profile.Latency = LatencyAcknowledgementWindow
	profile.Placement = "after_completion"
	profile.Endpoint = "caller"
	profile.Holds = []string{"none"}
	profile.DelayUS = 50_000
	controller, err := NewScenarioHarness().Controller(profile, confirmedTarget(profile))
	if err != nil {
		t.Fatal(err)
	}
	handle := backend.Handle{Type: backend.PackFile, Name: "saved-before-timeout"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err = WrapExperimentBackend(inner, controller).Save(ctx, handle, backend.NewByteReader([]byte("value"), inner.Hasher()))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("save error = %v", err)
	}
	if _, err := inner.Stat(context.Background(), handle); err != nil {
		t.Fatalf("completed save was lost: %v", err)
	}
}

func TestExperimentBackendPreservesConditionalWrite(t *testing.T) {
	inner := mem.New()
	conditional := backend.AsCapability[backend.ConditionalWriter](inner)
	if conditional == nil {
		t.Skip("memory backend does not expose conditional writes")
	}
	profile := backendScenarioProfile("coordination", "conditional_write")
	profile.Operation = "legacy_import"
	controller, err := NewScenarioHarness().Controller(profile, confirmedTarget(profile))
	if err != nil {
		t.Fatal(err)
	}
	wrapped := backend.AsCapability[backend.ConditionalWriter](WrapExperimentBackend(inner, controller))
	handle := backend.Handle{Type: backend.SlateDBFile, Name: "coordination"}
	if _, swapped, err := wrapped.CompareAndSwap(context.Background(), handle, nil, []byte("one")); err != nil || !swapped {
		t.Fatalf("create swapped=%t err=%v", swapped, err)
	}
	if current, swapped, err := wrapped.CompareAndSwap(context.Background(), handle, nil, []byte("two")); err != nil || swapped || string(current) != "one" {
		t.Fatalf("conflict current=%q swapped=%t err=%v", current, swapped, err)
	}
}
