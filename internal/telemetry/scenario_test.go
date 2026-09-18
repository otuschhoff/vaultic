package telemetry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func scenarioProfile() ExperimentProfile {
	profile := validExperimentProfile()
	profile.ProfileID = "m2-controller"
	profile.DelayUS = 1_000
	profile.JitterUS = 500
	profile.TailDelayUS = 2_000
	profile.TailEvery = 4
	profile.CorrelatedFor = 1
	profile.RetryError = "none"
	profile.Concurrency = 1
	return profile
}

func confirmedTarget(profile ExperimentProfile) ExperimentTarget {
	return ExperimentTarget{ID: profile.TargetID, Disposable: true, Confirmed: true}
}

func TestScenarioControllerDisabledPreservesResult(t *testing.T) {
	profile := scenarioProfile()
	profile.Enabled = false
	profile.Mode = DelayDisabled
	profile.DelayUS, profile.JitterUS, profile.TailDelayUS = 0, 0, 0
	profile.TailEvery, profile.CorrelatedFor = 0, 0
	profile.Concurrency, profile.DeadlineMS, profile.MaxRetries = 0, 0, 0
	controller, err := NewScenarioHarness().Controller(profile, ExperimentTarget{})
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("original")
	if got := controller.Run(context.Background(), ExperimentEvent{}, func(context.Context) error { return want }); !errors.Is(got, want) {
		t.Fatalf("disabled result = %v", got)
	}
	if observation := controller.Observation(); observation != (ExperimentObservation{}) {
		t.Fatalf("disabled observation = %+v", observation)
	}
}

func TestScenarioControllerSamplingUsesStableIdentity(t *testing.T) {
	profile := scenarioProfile()
	controller, err := NewScenarioHarness().Controller(profile, confirmedTarget(profile))
	if err != nil {
		t.Fatal(err)
	}
	event := ExperimentEvent{Identity: "pack-ordinal-7", Attempt: 2, Bytes: 4096}
	first := controller.Sample(event)
	if first == 0 || first != controller.Sample(event) {
		t.Fatalf("unstable sample = %s", first)
	}
	if first == controller.Sample(ExperimentEvent{Identity: event.Identity, Attempt: 3, Bytes: event.Bytes}) {
		t.Fatal("attempt identity did not affect deterministic sample")
	}
}

func TestScenarioControllerAcknowledgementTimeoutFollowsCompletion(t *testing.T) {
	profile := scenarioProfile()
	profile.DelayUS, profile.JitterUS, profile.TailDelayUS = 50_000, 0, 0
	profile.TailEvery, profile.CorrelatedFor = 0, 0
	controller, err := NewScenarioHarness().Controller(profile, confirmedTarget(profile))
	if err != nil {
		t.Fatal(err)
	}
	var completed atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err = controller.Run(ctx, ExperimentEvent{Identity: "commit-1"}, func(context.Context) error {
		completed.Store(true)
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) || !completed.Load() {
		t.Fatalf("acknowledgement result=%v completed=%t", err, completed.Load())
	}
	observation := controller.Observation()
	if observation.Completed != 1 || observation.Canceled != 1 || observation.Active != 0 {
		t.Fatalf("observation = %+v", observation)
	}
}

func TestScenarioControllerConfiguredDeadlineCleansUpCapacity(t *testing.T) {
	profile := validExperimentProfile()
	profile.Mode = DelayService
	profile.Placement = "inside_service"
	profile.Latency = LatencyServiceCompletion
	profile.Endpoint = "dependency"
	profile.Holds = []string{"backend_capacity"}
	profile.DelayUS = 100_000
	profile.DeadlineMS = 5
	profile.MaxRetries = 0
	controller, err := NewScenarioHarness().Controller(profile, ExperimentTarget{ID: profile.TargetID, Disposable: true, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	err = controller.Run(context.Background(), ExperimentEvent{Identity: "deadline"}, func(context.Context) error {
		t.Fatal("operation ran after injected deadline")
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error = %v", err)
	}
	observation := controller.Observation()
	if observation.Canceled != 1 || observation.Active != 0 {
		t.Fatalf("deadline observation = %+v", observation)
	}
}

func TestScenarioControllersShareResourceCapacityAndCleanUp(t *testing.T) {
	profile := scenarioProfile()
	profile.Mode = DelayService
	profile.Placement = "inside_service"
	profile.Latency = LatencyServiceCompletion
	profile.Endpoint = "dependency"
	profile.Holds = []string{"backend_capacity"}
	profile.DelayUS, profile.JitterUS, profile.TailDelayUS = 0, 0, 0
	profile.TailEvery, profile.CorrelatedFor = 0, 0
	harness := NewScenarioHarness()
	first, err := harness.Controller(profile, confirmedTarget(profile))
	if err != nil {
		t.Fatal(err)
	}
	second, err := harness.Controller(profile, confirmedTarget(profile))
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- first.Run(context.Background(), ExperimentEvent{Identity: "first"}, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	called := false
	err = second.Run(ctx, ExperimentEvent{Identity: "second"}, func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) || called {
		t.Fatalf("shared capacity result=%v called=%t", err, called)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if first.Observation().Active != 0 || second.Observation().Active != 0 {
		t.Fatal("capacity cancellation leaked an active operation")
	}
}

func TestScenarioControllerResponseDeliveryIsMethodSelective(t *testing.T) {
	profile := scenarioProfile()
	profile.DelayUS, profile.JitterUS, profile.TailDelayUS = 0, 0, 0
	profile.TailEvery, profile.CorrelatedFor = 0, 0
	controller, err := NewScenarioHarness().Controller(profile, confirmedTarget(profile))
	if err != nil {
		t.Fatal(err)
	}
	deliver := controller.ResponseDelivery("commit")
	if err := deliver(context.Background(), "/vaulticdb.v1.VaulticDB/Get"); err != nil {
		t.Fatal(err)
	}
	if controller.Observation().Started != 0 {
		t.Fatal("unselected RPC method was delayed")
	}
	if err := deliver(context.Background(), "/vaulticdb.v1.VaulticDB/Commit"); err != nil {
		t.Fatal(err)
	}
	if controller.Observation().Completed != 1 {
		t.Fatalf("selected observation = %+v", controller.Observation())
	}
}

func TestScenarioAcknowledgementWithoutHeldResourcesDoesNotSerialize(t *testing.T) {
	profile := validExperimentProfile()
	profile.Concurrency = 1
	profile.DelayUS = 25_000
	profile.MaxRetries = 0
	controller, err := NewScenarioHarness().Controller(profile, ExperimentTarget{ID: profile.TargetID, Disposable: true, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var workers sync.WaitGroup
	for index := range 2 {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			err := controller.Run(context.Background(), ExperimentEvent{Identity: fmt.Sprintf("ack-%d", index)}, func(context.Context) error {
				started <- struct{}{}
				<-release
				return nil
			})
			if err != nil {
				t.Errorf("acknowledgement run: %v", err)
			}
		}(index)
	}
	<-started
	<-started
	close(release)
	workers.Wait()
	if observation := controller.Observation(); observation.Completed != 2 || observation.Active != 0 {
		t.Fatalf("acknowledgement observation = %+v", observation)
	}
}
