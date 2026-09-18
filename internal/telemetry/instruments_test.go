package telemetry

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

func TestCounterSaturatesUnderConcurrency(t *testing.T) {
	var counter Counter
	counter.Add(math.MaxUint64 - 100)
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 100 {
				counter.Add(1)
			}
		}()
	}
	workers.Wait()
	if counter.Load() != math.MaxUint64 {
		t.Fatalf("counter = %d", counter.Load())
	}
}

func TestFixedDistributionUsesCumulativeBuckets(t *testing.T) {
	distribution := NewFixedDistribution([]uint64{10, 100})
	distribution.Observe(5)
	distribution.Observe(50)
	distribution.Observe(500)
	snapshot := distribution.Snapshot()
	if snapshot.Count != 3 || snapshot.Sum != 555 || snapshot.Maximum != 500 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	want := []uint64{1, 2, 3}
	for index := range want {
		if snapshot.BucketCounts[index] != want[index] {
			t.Fatalf("bucket counts = %v", snapshot.BucketCounts)
		}
	}
}

func TestFixedDistributionSnapshotIsCoherentDuringObservation(t *testing.T) {
	distribution := NewFixedDistribution([]uint64{10, 100})
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 1000 {
				distribution.Observe(50)
				snapshot := distribution.Snapshot()
				if snapshot.BucketCounts[len(snapshot.BucketCounts)-1] != snapshot.Count ||
					snapshot.Count == 0 && (snapshot.Sum != 0 || snapshot.Maximum != 0) ||
					snapshot.Maximum > snapshot.Sum {
					t.Errorf("incoherent snapshot = %+v", snapshot)
					return
				}
			}
		}()
	}
	workers.Wait()
}

func TestRotatingDistributionExpiresOldSlots(t *testing.T) {
	distribution := NewRotatingDistribution([]uint64{10}, time.Minute, 3)
	now := time.Unix(600, 0)
	distribution.now = func() time.Time { return now }
	distribution.Observe(5)
	now = now.Add(2 * time.Minute)
	distribution.Observe(20)
	if snapshot := distribution.Snapshot(time.Minute); snapshot.Count != 1 || snapshot.Sum != 20 {
		t.Fatalf("recent snapshot = %+v", snapshot)
	}
	if snapshot := distribution.Snapshot(5 * time.Minute); snapshot.Count != 2 || snapshot.Sum != 25 {
		t.Fatalf("wide snapshot = %+v", snapshot)
	}
	now = now.Add(2 * time.Minute)
	distribution.Observe(7)
	if len(distribution.buckets) != 3 {
		t.Fatalf("retained slots = %d", len(distribution.buckets))
	}
}

func TestTimerSettlesExactlyOnce(t *testing.T) {
	distribution := NewFixedDistribution(nil)
	timer := StartTimer(distribution)
	timer.Settle()
	timer.Settle()
	if snapshot := distribution.Snapshot(); snapshot.Count != 1 {
		t.Fatalf("timer count = %d", snapshot.Count)
	}
}

func TestOperationRegistryIsBoundedAndCleanupSafe(t *testing.T) {
	registry := NewOperationRegistry(1)
	now := time.Unix(10, 0)
	registry.clock = func() time.Time { return now }
	handle := registry.Start("backup", "source", "")
	overflowed := registry.Start("restore", "source", "")
	overflowed.Done()
	now = now.Add(time.Second)
	handle.Progress("write", "retry_backoff", 4, 10)
	operations, overflow := registry.Snapshot()
	if len(operations) != 1 || len(overflow) != 1 || overflow[0].Class != "restore" || overflow[0].Count != 1 {
		t.Fatalf("operations = %+v, overflow = %+v", operations, overflow)
	}
	if operations[0].Phase != "write" || operations[0].CompletedUnits != 4 || operations[0].UpdatedUnixMS != now.UnixMilli() {
		t.Fatalf("operation = %+v", operations[0])
	}
	handle.Done()
	handle.Done()
	operations, _ = registry.Snapshot()
	if len(operations) != 0 {
		t.Fatalf("operations after cleanup = %+v", operations)
	}
}

func TestOperationRegistryRejectsUnboundedIdentityBeforeOverflow(t *testing.T) {
	registry := NewOperationRegistry(1)
	registry.Start("restore", "source", "")
	for index := range 1000 {
		registry.Start(fmt.Sprintf("custom-%d", index), "source", "")
	}
	_, overflow := registry.Snapshot()
	if len(overflow) != 0 {
		t.Fatalf("invalid identities entered overflow: %+v", overflow)
	}
}

func TestOperationRegistryRejectsInvalidParentAndProgressIdentity(t *testing.T) {
	registry := NewOperationRegistry(2)
	if handle := registry.Start("restore", "source", "/private/path"); handle.registry != nil {
		t.Fatal("invalid parent identity was admitted")
	}
	handle := registry.Start("restore", "source", "")
	handle.Progress("custom_phase", "", 1, 2)
	handle.Progress("write", "custom_blocker", 1, 2)
	operations, _ := registry.Snapshot()
	if len(operations) != 1 || operations[0].Phase != "source" || operations[0].BlockingReason != "" {
		t.Fatalf("invalid progress changed operation: %+v", operations)
	}
}

func TestOperationRegistryRejectsTokenExhaustion(t *testing.T) {
	registry := NewOperationRegistry(1)
	registry.next = ^uint64(0)
	handle := registry.Start("backup", "planning", "")
	active, overflow := registry.Snapshot()
	if handle.registry != nil || len(active) != 0 {
		t.Fatal("exhausted operation token created an active operation")
	}
	if len(overflow) != 1 || overflow[0].Class != "backup" || overflow[0].Count != 1 {
		t.Fatalf("overflow = %#v", overflow)
	}
}
