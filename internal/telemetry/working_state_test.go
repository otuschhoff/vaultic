package telemetry

import (
	"encoding/json"
	"math"
	runtimemetrics "runtime/metrics"
	"strings"
	"sync"
	"testing"
)

func TestWorkingStateCounters(t *testing.T) {
	metric := NewWorkingStateMetric(WorkingWrittenBlobs, "pebble")
	metric.Activate()
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 100 {
				metric.Committed(1, 148)
				metric.ObserveBuffer(256)
				metric.Snapshot(0, false)
			}
		})
	}
	workers.Wait()
	state := metric.Snapshot(1234, true)
	if state.CommittedEntries != 800 || state.CommittedEncodedBytes != 800*148 ||
		state.RetainedEntriesUpperBound != 800 || state.PeakObservedBufferBytes != 256 ||
		!state.ScratchBytesKnown || state.ScratchBytes != 1234 || state.ReservationEnforced {
		t.Fatalf("unexpected working state: %+v", state)
	}
	metric.Close()
	metric.Close()
	metric.Committed(1, 1)
	metric.ObserveBuffer(1)
	state = metric.Snapshot(0, true)
	if !state.Closed || state.RetainedEntriesUpperBound != 0 || state.RetainedEncodedBytesBound != 0 ||
		state.ObservedBufferBytes != 0 || state.CommittedEntries != 800 {
		t.Fatalf("closed state: %+v", state)
	}
}

func TestWorkingRuntimeAvailabilityAndPrivacy(t *testing.T) {
	state := ReadWorkingRuntime()
	if state.HeapBytes == 0 || state.RuntimeBytes < state.HeapBytes || state.AllocatedBytes == 0 {
		t.Fatalf("missing runtime observations: %+v", state)
	}
	if _, err := json.Marshal(state); err != nil {
		t.Fatal(err)
	}
	samples := make([]runtimemetrics.Sample, len(workingRuntimeNames))
	for index, name := range workingRuntimeNames {
		samples[index].Name = name
	}
	missing := workingRuntimeSnapshot(samples)
	if len(missing.Unavailable) != len(workingRuntimeNames) || missing.HeapBytes != 0 {
		t.Fatalf("unsupported metrics are not explicit: %+v", missing)
	}
}

func TestWorkingStateBoundedIdentityAndOverflow(t *testing.T) {
	for _, identity := range []struct {
		kind    WorkingStateKind
		backend string
	}{{"/private/source", "pebble"}, {WorkingMarkers, "secret-engine-path"}} {
		t.Run(string(identity.kind)+identity.backend, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("unbounded identity accepted")
				}
			}()
			NewWorkingStateMetric(identity.kind, identity.backend)
		})
	}
	metric := NewWorkingStateMetric(WorkingMarkers, "pebble")
	metric.Committed(math.MaxUint64, math.MaxUint64)
	metric.Committed(1, 1)
	state := metric.Snapshot(0, false)
	if state.CommittedEntries != math.MaxUint64 || state.CommittedEncodedBytes != math.MaxUint64 {
		t.Fatal("counters wrapped")
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"path", "secret", "key", "value", "repository"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("sensitive field in working-state snapshot: %s", encoded)
		}
	}
}
