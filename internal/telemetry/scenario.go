package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type ExperimentEvent struct {
	Identity string
	Attempt  uint32
	Bytes    uint64
}

type ExperimentObservation struct {
	Started         uint64
	Completed       uint64
	Canceled        uint64
	InjectedErrors  uint64
	SampledDelayUS  uint64
	ObservedDelayUS uint64
	Active          uint64
}

type experimentCounters struct {
	started         Counter
	completed       Counter
	canceled        Counter
	injectedErrors  Counter
	sampledDelayUS  Counter
	observedDelayUS Counter
	active          atomic.Uint64
}

type experimentCapacity struct {
	semaphore chan struct{}
}

type ScenarioHarness struct {
	mu        sync.Mutex
	resources map[string]*experimentCapacity
}

type ExperimentController struct {
	profile  ExperimentProfile
	capacity *experimentCapacity
	counters experimentCounters
	now      func() time.Time
}

var ErrExperimentUnavailable = errors.New("injected dependency unavailable")
var ErrExperimentThrottled = errors.New("injected dependency throttled")

func NewScenarioHarness() *ScenarioHarness {
	return &ScenarioHarness{resources: make(map[string]*experimentCapacity)}
}

func (harness *ScenarioHarness) Controller(profile ExperimentProfile, target ExperimentTarget) (*ExperimentController, error) {
	if err := profile.ValidateTarget(target); err != nil {
		return nil, err
	}
	controller := &ExperimentController{profile: profile, now: time.Now}
	if profile.Mode == DelayDisabled || !profile.Enabled {
		return controller, nil
	}
	capacity := max(int(profile.Concurrency), 1)
	harness.mu.Lock()
	defer harness.mu.Unlock()
	shared := harness.resources[profile.ResourceID]
	if shared == nil {
		shared = &experimentCapacity{semaphore: make(chan struct{}, capacity)}
		harness.resources[profile.ResourceID] = shared
	} else if cap(shared.semaphore) != capacity {
		return nil, fmt.Errorf("experiment resource %q has conflicting concurrency", profile.ResourceID)
	}
	controller.capacity = shared
	return controller, nil
}

func (controller *ExperimentController) Enabled() bool {
	return controller != nil && controller.profile.Enabled && controller.profile.Mode != DelayDisabled
}

func (controller *ExperimentController) Matches(role, method string) bool {
	return controller.Enabled() && controller.profile.Role == role && controller.profile.Method == method
}

func (controller *ExperimentController) Sample(event ExperimentEvent) time.Duration {
	if !controller.Enabled() || event.Identity == "" {
		return 0
	}
	digest := sha256.New()
	var encoded [12]byte
	binary.BigEndian.PutUint64(encoded[:8], controller.profile.Seed)
	binary.BigEndian.PutUint32(encoded[8:], event.Attempt)
	_, _ = digest.Write(encoded[:])
	_, _ = digest.Write([]byte(controller.profile.ProfileID))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(controller.profile.ResourceID))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(event.Identity))
	sum := digest.Sum(nil)
	delay := controller.profile.DelayUS
	if controller.profile.JitterUS != 0 {
		delay = saturatingAdd(delay, binary.BigEndian.Uint64(sum[:8])%(controller.profile.JitterUS+1))
	}
	if controller.profile.TailEvery != 0 {
		width := max(controller.profile.CorrelatedFor, uint64(1))
		if binary.BigEndian.Uint64(sum[8:16])%controller.profile.TailEvery < min(width, controller.profile.TailEvery) {
			delay = saturatingAdd(delay, controller.profile.TailDelayUS)
		}
	}
	if controller.profile.BandwidthBPS != 0 && event.Bytes != 0 {
		transferUS := saturatingMul(event.Bytes, 1_000_000) / controller.profile.BandwidthBPS
		delay = saturatingAdd(delay, transferUS)
	}
	return time.Duration(min(delay, uint64(MaxExperimentDelayUS))) * time.Microsecond
}

func (controller *ExperimentController) Run(ctx context.Context, event ExperimentEvent, operation func(context.Context) error) error {
	if operation == nil {
		return errors.New("experiment operation is required")
	}
	if !controller.Enabled() {
		return operation(ctx)
	}
	if event.Identity == "" || len(event.Identity) > MaxMonitorStringLength {
		return errors.New("experiment event requires a bounded stable identity")
	}
	if controller.profile.DeadlineMS != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(controller.profile.DeadlineMS)*time.Millisecond)
		defer cancel()
	}
	controller.counters.started.Add(1)
	controller.counters.active.Add(1)
	defer controller.counters.active.Add(^uint64(0))
	acquired := false
	for _, held := range controller.profile.Holds {
		if held != "none" {
			if err := controller.acquire(ctx); err != nil {
				controller.counters.canceled.Add(1)
				return err
			}
			acquired = true
			break
		}
	}
	if acquired {
		defer controller.release()
	}
	delay := controller.Sample(event)
	if controller.profile.Mode != DelayAcknowledgement {
		if err := controller.wait(ctx, delay); err != nil {
			controller.counters.canceled.Add(1)
			return err
		}
		if err := controller.injectedError(event); err != nil {
			controller.counters.injectedErrors.Add(1)
			return err
		}
	}
	err := operation(ctx)
	if err != nil {
		return err
	}
	controller.counters.completed.Add(1)
	if controller.profile.Mode == DelayAcknowledgement {
		if err := controller.wait(ctx, delay); err != nil {
			controller.counters.canceled.Add(1)
			return err
		}
	}
	return nil
}

func (controller *ExperimentController) acquire(ctx context.Context) error {
	select {
	case controller.capacity.semaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (controller *ExperimentController) release() {
	<-controller.capacity.semaphore
}

func (controller *ExperimentController) wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	controller.counters.sampledDelayUS.Add(uint64(delay / time.Microsecond))
	started := controller.now()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		controller.counters.observedDelayUS.Add(uint64(max(controller.now().Sub(started).Microseconds(), 0)))
		return nil
	case <-ctx.Done():
		controller.counters.observedDelayUS.Add(uint64(max(controller.now().Sub(started).Microseconds(), 0)))
		return ctx.Err()
	}
}

func (controller *ExperimentController) injectedError(event ExperimentEvent) error {
	if controller.profile.RetryError == "" || controller.profile.RetryError == "none" {
		return nil
	}
	if controller.profile.TailEvery != 0 && controller.Sample(event) < time.Duration(controller.profile.DelayUS+controller.profile.TailDelayUS)*time.Microsecond {
		return nil
	}
	switch controller.profile.RetryError {
	case "timeout":
		return context.DeadlineExceeded
	case "unavailable":
		return ErrExperimentUnavailable
	case "throttled":
		return ErrExperimentThrottled
	default:
		return nil
	}
}

func (controller *ExperimentController) Observation() ExperimentObservation {
	if controller == nil {
		return ExperimentObservation{}
	}
	return ExperimentObservation{
		Started: controller.counters.started.Load(), Completed: controller.counters.completed.Load(),
		Canceled: controller.counters.canceled.Load(), InjectedErrors: controller.counters.injectedErrors.Load(),
		SampledDelayUS: controller.counters.sampledDelayUS.Load(), ObservedDelayUS: controller.counters.observedDelayUS.Load(),
		Active: controller.counters.active.Load(),
	}
}

func (controller *ExperimentController) ResponseDelivery(method string) func(context.Context, string) error {
	return func(ctx context.Context, rpcMethod string) error {
		if !controller.Matches("rpc", method) || rpcMethodName(rpcMethod) != method {
			return nil
		}
		return controller.Run(ctx, ExperimentEvent{Identity: "rpc-" + method}, func(context.Context) error { return nil })
	}
}

func rpcMethodName(method string) string {
	for index := len(method) - 1; index >= 0; index-- {
		if method[index] == '/' {
			name := method[index+1:]
			result := make([]byte, 0, len(name)+4)
			for offset, character := range []byte(name) {
				if character >= 'A' && character <= 'Z' {
					if offset != 0 {
						result = append(result, '_')
					}
					character += 'a' - 'A'
				}
				result = append(result, character)
			}
			return string(result)
		}
	}
	return method
}

func saturatingMul(left, right uint64) uint64 {
	if left != 0 && right > ^uint64(0)/left {
		return ^uint64(0)
	}
	return left * right
}
