package telemetry

import (
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
	handle.Progress("write", "backend_retry", 4, 10)
	operations, overflow := registry.Snapshot()
	if len(operations) != 1 || overflow != 1 {
		t.Fatalf("operations = %+v, overflow = %d", operations, overflow)
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
