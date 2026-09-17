package telemetry

import (
	"math"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
)

func TestVaulticDBComponentMapsBoundedStatus(t *testing.T) {
	timing := daemon.TimingSnapshot{
		Completed: 2, Successes: 2, TotalUS: 12, MaxUS: 8,
		LatencyBucketUpperUS: []uint64{10, math.MaxUint64}, LatencyBucketCounts: []uint64{2, 2},
	}
	writer := daemon.WriterStatus{
		ProcessStartedUnixMS: 100, CapturedUnixMS: 200,
		Attribution: daemon.AttributionSnapshot{
			AdmissionWait: timing, AdmissionLockHold: timing, FenceCheck: timing,
			WriteBatchRequest: timing, TransactionBegin: timing, EngineSubmit: timing,
			DurableWait: timing, Finalization: timing, EngineBackpressure: timing,
			EngineBatchQueue: timing, EngineBatchService: timing,
		},
	}
	cache := daemon.ReadCacheStatus{
		Configured: true, AggregateMaxBytesKnown: true, AggregateMaxBytes: 1000,
		UsedBytes: 500, ReservedBytes: 100, InflightBytes: 8, Metrics: daemon.ReadCacheMetrics{Hits: 3, Misses: 1},
	}
	component := VaulticDBComponent(writer, cache, daemon.WALInfo{Target: "separate", Durability: "persistent", OldestSegmentUnixMS: 125})
	if err := ValidateVaulticDBComponent(component); err != nil {
		t.Fatal(err)
	}
	if component.ProcessStartID != "100" || len(component.Caches) != 1 || component.Caches[0].AvailableBytes != 400 {
		t.Fatalf("component = %+v", component)
	}
	if len(component.Storage) != 2 || component.Storage[1].Role != "wal" || component.Storage[1].Acknowledgement != "persistent" {
		t.Fatalf("storage = %+v", component.Storage)
	}
	if component.Caches[0].StagingBytes != 8 || component.Caches[0].InflightFills != 0 {
		t.Fatalf("cache in-flight units = %+v", component.Caches[0])
	}
	if component.WAL.OldestUncheckpointedMS != 75 {
		t.Fatalf("WAL age = %d", component.WAL.OldestUncheckpointedMS)
	}
	attribution := daemon.AttributionSnapshot{EngineBackpressure: daemon.TimingSnapshot{Active: 1}}
	if got := vaulticDBBackpressure(attribution); got != "capacity" {
		t.Fatalf("backpressure = %q, want capacity", got)
	}
	attribution.EngineBackpressure.Active = 0
	if got := vaulticDBBackpressure(attribution); got != "none" {
		t.Fatalf("inactive backpressure = %q, want none", got)
	}
}

func TestVaulticDBComponentMarksLegacyIdentityEstimated(t *testing.T) {
	component := VaulticDBComponent(daemon.WriterStatus{}, daemon.ReadCacheStatus{}, daemon.WALInfo{})
	if component.Availability != AvailabilityEstimated || component.ProcessStartID != "legacy" {
		t.Fatalf("component = %+v", component)
	}
}

func TestVaulticDBComponentDoesNotTreatQueueDepthAsWALThrottle(t *testing.T) {
	writer := daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200}
	writer.Attribution.EngineBatchQueueDepth = 1
	component := VaulticDBComponent(writer, daemon.ReadCacheStatus{}, daemon.WALInfo{})
	if component.Queues[0].Backpressure != "none" || component.WAL.ThrottleReason != "none" {
		t.Fatalf("component = %+v", component)
	}
}

func TestVaulticDBComponentMarksUnknownAggregateCacheLimitEstimated(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{Configured: true, UsedBytes: 10},
		daemon.WALInfo{},
	)
	if component.Caches[0].Availability != AvailabilityEstimated || component.Caches[0].UsedBytes != 10 {
		t.Fatalf("cache = %+v", component.Caches[0])
	}
}
