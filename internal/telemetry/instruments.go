package telemetry

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct {
	value atomic.Uint64
}

func (counter *Counter) Add(delta uint64) {
	for {
		current := counter.value.Load()
		next := current + delta
		if next < current {
			next = math.MaxUint64
		}
		if counter.value.CompareAndSwap(current, next) {
			return
		}
	}
}

func (counter *Counter) Load() uint64 { return counter.value.Load() }

type Gauge struct {
	value atomic.Uint64
}

func (gauge *Gauge) Set(value uint64) { gauge.value.Store(value) }
func (gauge *Gauge) Load() uint64     { return gauge.value.Load() }

type DistributionSnapshot struct {
	Count        uint64
	Sum          uint64
	Maximum      uint64
	BucketUpper  []uint64
	BucketCounts []uint64
}

type FixedDistribution struct {
	bounds  []uint64
	buckets []atomic.Uint64
	count   Counter
	sum     Counter
	maximum atomic.Uint64
}

func NewFixedDistribution(bounds []uint64) *FixedDistribution {
	checked := append([]uint64(nil), bounds...)
	if len(checked) == 0 || checked[len(checked)-1] != math.MaxUint64 {
		checked = append(checked, math.MaxUint64)
	}
	for index := 1; index < len(checked); index++ {
		if checked[index] <= checked[index-1] {
			panic("telemetry distribution bounds must be strictly increasing")
		}
	}
	return &FixedDistribution{bounds: checked, buckets: make([]atomic.Uint64, len(checked))}
}

func (distribution *FixedDistribution) Observe(value uint64) {
	distribution.sum.Add(value)
	for {
		current := distribution.maximum.Load()
		if value <= current || distribution.maximum.CompareAndSwap(current, value) {
			break
		}
	}
	index := sort.Search(len(distribution.bounds), func(index int) bool { return value <= distribution.bounds[index] })
	for ; index < len(distribution.buckets); index++ {
		incrementAtomic(&distribution.buckets[index])
	}
	distribution.count.Add(1)
}

func (distribution *FixedDistribution) Snapshot() DistributionSnapshot {
	count := distribution.count.Load()
	sum := distribution.sum.Load()
	maximum := min(distribution.maximum.Load(), sum)
	if count == 0 {
		sum = 0
		maximum = 0
	}
	counts := make([]uint64, len(distribution.buckets))
	for index := range distribution.buckets {
		counts[index] = min(distribution.buckets[index].Load(), count)
		if index > 0 && counts[index] < counts[index-1] {
			counts[index] = counts[index-1]
		}
	}
	return DistributionSnapshot{
		Count: count, Sum: sum, Maximum: maximum,
		BucketUpper: append([]uint64(nil), distribution.bounds...), BucketCounts: counts,
	}
}

type rotatingBucket struct {
	start time.Time
	count uint64
	sum   uint64
	max   uint64
	items []uint64
}

type RotatingDistribution struct {
	mu       sync.Mutex
	bounds   []uint64
	slot     time.Duration
	retained int
	buckets  []rotatingBucket
	now      func() time.Time
}

func NewRotatingDistribution(bounds []uint64, slot time.Duration, retained int) *RotatingDistribution {
	if slot <= 0 || retained <= 0 {
		panic("telemetry rotating distribution requires positive slot and retention")
	}
	cumulative := NewFixedDistribution(bounds)
	return &RotatingDistribution{
		bounds: cumulative.bounds, slot: slot, retained: retained, now: time.Now,
	}
}

func (distribution *RotatingDistribution) Observe(value uint64) {
	distribution.observeAt(distribution.now(), value)
}

func (distribution *RotatingDistribution) observeAt(now time.Time, value uint64) {
	distribution.mu.Lock()
	defer distribution.mu.Unlock()
	start := now.Truncate(distribution.slot)
	if len(distribution.buckets) == 0 || !distribution.buckets[len(distribution.buckets)-1].start.Equal(start) {
		distribution.buckets = append(distribution.buckets, rotatingBucket{
			start: start, items: make([]uint64, len(distribution.bounds)),
		})
		if len(distribution.buckets) > distribution.retained {
			distribution.buckets = append([]rotatingBucket(nil), distribution.buckets[len(distribution.buckets)-distribution.retained:]...)
		}
	}
	bucket := &distribution.buckets[len(distribution.buckets)-1]
	bucket.count = saturatingAdd(bucket.count, 1)
	bucket.sum = saturatingAdd(bucket.sum, value)
	if value > bucket.max {
		bucket.max = value
	}
	index := sort.Search(len(distribution.bounds), func(index int) bool { return value <= distribution.bounds[index] })
	for ; index < len(bucket.items); index++ {
		bucket.items[index] = saturatingAdd(bucket.items[index], 1)
	}
}

func (distribution *RotatingDistribution) Snapshot(window time.Duration) DistributionSnapshot {
	distribution.mu.Lock()
	defer distribution.mu.Unlock()
	now := distribution.now()
	cutoff := now.Add(-window)
	snapshot := DistributionSnapshot{
		BucketUpper:  append([]uint64(nil), distribution.bounds...),
		BucketCounts: make([]uint64, len(distribution.bounds)),
	}
	for _, bucket := range distribution.buckets {
		if !bucket.start.Add(distribution.slot).After(cutoff) || bucket.start.After(now) {
			continue
		}
		snapshot.Count = saturatingAdd(snapshot.Count, bucket.count)
		snapshot.Sum = saturatingAdd(snapshot.Sum, bucket.sum)
		if bucket.max > snapshot.Maximum {
			snapshot.Maximum = bucket.max
		}
		for index := range snapshot.BucketCounts {
			snapshot.BucketCounts[index] = saturatingAdd(snapshot.BucketCounts[index], bucket.items[index])
		}
	}
	return snapshot
}

type Timer struct {
	distribution *FixedDistribution
	started      time.Time
	settled      atomic.Bool
}

func StartTimer(distribution *FixedDistribution) *Timer {
	return &Timer{distribution: distribution, started: time.Now()}
}

func (timer *Timer) Settle() {
	if timer == nil || timer.distribution == nil || !timer.settled.CompareAndSwap(false, true) {
		return
	}
	elapsed := time.Since(timer.started).Microseconds()
	if elapsed < 0 {
		elapsed = 0
	}
	timer.distribution.Observe(uint64(elapsed))
}

type OperationRegistry struct {
	mu       sync.Mutex
	capacity int
	next     uint64
	active   map[uint64]ActiveOperation
	overflow map[string]uint64
	clock    func() time.Time
}

type OperationHandle struct {
	registry *OperationRegistry
	token    uint64
	settled  atomic.Bool
}

func NewOperationRegistry(capacity int) *OperationRegistry {
	if capacity <= 0 || capacity > MaxMonitorOperations {
		panic("telemetry operation registry capacity is out of bounds")
	}
	return &OperationRegistry{capacity: capacity, active: make(map[uint64]ActiveOperation, capacity), overflow: make(map[string]uint64), clock: time.Now}
}

func (registry *OperationRegistry) Start(class, phase, parentID string) *OperationHandle {
	if _, known := monitorValues("operation")[class]; !known {
		return &OperationHandle{}
	}
	if _, known := monitorValues("phase")[phase]; !known {
		return &OperationHandle{}
	}
	if parentID != "" && !validOpaqueID(parentID) {
		return &OperationHandle{}
	}
	now := registry.clock().UnixMilli()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.active) >= registry.capacity {
		registry.overflow[class] = saturatingAdd(registry.overflow[class], 1)
		return &OperationHandle{}
	}
	if registry.next == ^uint64(0) {
		registry.overflow[class] = saturatingAdd(registry.overflow[class], 1)
		return &OperationHandle{}
	}
	registry.next++
	token := registry.next
	registry.active[token] = ActiveOperation{
		ID: operationID(token), ParentID: parentID, Class: class, Phase: phase,
		StartedUnixMS: now, UpdatedUnixMS: now,
	}
	return &OperationHandle{registry: registry, token: token}
}

func (handle *OperationHandle) Progress(phase, blockingReason string, completed, expected uint64) {
	if handle == nil || handle.registry == nil || handle.settled.Load() {
		return
	}
	if _, known := monitorValues("phase")[phase]; !known {
		return
	}
	if blockingReason != "" {
		if _, known := monitorValues("blocking")[blockingReason]; !known {
			return
		}
	}
	registry := handle.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	operation, exists := registry.active[handle.token]
	if !exists {
		return
	}
	operation.Phase = phase
	operation.BlockingReason = blockingReason
	operation.CompletedUnits = completed
	operation.ExpectedUnits = expected
	operation.UpdatedUnixMS = registry.clock().UnixMilli()
	registry.active[handle.token] = operation
}

func (handle *OperationHandle) Done() {
	if handle == nil || handle.registry == nil || !handle.settled.CompareAndSwap(false, true) {
		return
	}
	handle.registry.mu.Lock()
	delete(handle.registry.active, handle.token)
	handle.registry.mu.Unlock()
}

func (registry *OperationRegistry) Snapshot() ([]ActiveOperation, []OperationOverflowSnapshot) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	operations := make([]ActiveOperation, 0, len(registry.active))
	for _, operation := range registry.active {
		operations = append(operations, operation)
	}
	sort.Slice(operations, func(left, right int) bool { return operations[left].ID < operations[right].ID })
	overflow := make([]OperationOverflowSnapshot, 0, len(registry.overflow))
	for class, count := range registry.overflow {
		overflow = append(overflow, OperationOverflowSnapshot{Class: class, Count: count})
	}
	sort.Slice(overflow, func(left, right int) bool { return overflow[left].Class < overflow[right].Class })
	return operations, overflow
}

func operationID(token uint64) string {
	const digits = "0123456789abcdef"
	var encoded [16]byte
	for index := len(encoded) - 1; index >= 0; index-- {
		encoded[index] = digits[token&15]
		token >>= 4
	}
	return string(encoded[:])
}

func incrementAtomic(value *atomic.Uint64) {
	for {
		current := value.Load()
		if current == math.MaxUint64 || value.CompareAndSwap(current, current+1) {
			return
		}
	}
}

func saturatingAdd(left, right uint64) uint64 {
	result := left + right
	if result < left {
		return math.MaxUint64
	}
	return result
}
