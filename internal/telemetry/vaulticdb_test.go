package telemetry

import (
	"fmt"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
)

func TestTimingMetricRejectsIncoherentConcurrentSnapshot(t *testing.T) {
	for _, timing := range []daemon.TimingSnapshot{
		{Completed: 2, TotalUS: 12, MaxUS: 8, LatencyBucketUpperUS: vaulticLatencyBounds(), LatencyBucketCounts: []uint64{1, 0, 0, 0, 0, 0, 0, 0}},
		{Completed: 0, TotalUS: 12, MaxUS: 8, LatencyBucketUpperUS: vaulticLatencyBounds(), LatencyBucketCounts: make([]uint64, 8)},
		{Completed: 1, TotalUS: 4, MaxUS: 8, LatencyBucketUpperUS: vaulticLatencyBounds(), LatencyBucketCounts: []uint64{1, 0, 0, 0, 0, 0, 0, 0}},
	} {
		metric := timingMetric("object_get_latency", timing, Label{Name: "role", Value: "database"})
		if metric.Availability != AvailabilityUnavailable {
			t.Fatalf("incoherent snapshot reported as exact: %+v", metric)
		}
		if err := validateMetrics([]Metric{metric}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestVaulticDBComponentMapsBoundedStatus(t *testing.T) {
	timing := daemon.TimingSnapshot{
		Completed: 2, Successes: 2, TotalUS: 12, MaxUS: 8,
		LatencyBucketUpperUS: vaulticLatencyBounds(), LatencyBucketCounts: []uint64{1, 1, 0, 0, 0, 0, 0, 0},
	}
	engineTiming := timing
	engineTiming.LatencyBucketUpperUS = slateDBLatencyBounds()
	engineTiming.LatencyBucketCounts = []uint64{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	writer := daemon.WriterStatus{
		ProcessStartedUnixMS: 100, CapturedUnixMS: 200,
		EngineFlushIntervalMS: 100, EngineMaxUnflushedBytes: 200, EngineL0SSTSizeBytes: 300,
		EngineBlockCacheBytes: 400, EngineMetaCacheBytes: 500,
		EngineTuningAvailable: true,
		Attribution: daemon.AttributionSnapshot{
			AdmissionWait: timing, AdmissionLockHold: timing, FenceCheck: timing,
			WriteBatchRequest: timing, BeginRequest: timing, CommitRequest: timing, RollbackRequest: timing,
			TransactionBegin: timing, EngineSubmit: timing,
			DurableWait: timing, Finalization: timing, EngineBackpressure: engineTiming,
			EngineBatchQueue: engineTiming, EngineBatchService: engineTiming,
			EngineImmutableFlushes: 3, EngineL0FlushBytes: 4, EngineCompactedSSTs: 5,
			EngineL0StallsSSTCount: 6, EngineL0StallsSSTsPerKey: 7,
			EngineGetKeys: 8, EngineFilterPointPositive: 9, EngineFilterPointNegative: 10,
			EngineFilterPointFalsePositive: 11, EngineReadMetricsAvailable: true,
			EngineMultiGetMetricsAvailable: true,
			EngineMultiGetCalls:            12, EngineMultiGetInputKeys: 13, EngineMultiGetUniqueKeys: 14,
			EngineMultiGetSSTVisits: 15, EngineMultiGetCandidateKeys: 16,
			EngineMultiGetNeededBlocks: 17, EngineMultiGetCoalescedReads: 18,
			EngineMultiGetNeededBlockBytes: 19, EngineMultiGetCoalescedReadBytes: 20,
			EngineMultiGetProjectedReadsGap8: 21, EngineMultiGetProjectedReadBytesGap8: 22,
			EngineMultiGetProjectedReadsGap32: 23, EngineMultiGetProjectedReadBytesGap32: 24,
			EngineMultiGetProjectedReadsGap128: 25, EngineMultiGetProjectedReadBytesGap128: 26,
		},
	}
	cache := daemon.ReadCacheStatus{
		Configured: true, AggregateMaxBytesKnown: true, AggregateMaxBytes: 1000,
		QuotaCoordinationHealthy: true,
		UsedBytes:                500, ReservedBytes: 100, InflightBytes: 8, DeletionPendingBytes: 25, DeletionPendingKnown: true,
		Metrics: daemon.ReadCacheMetrics{
			Hits: 3, Misses: 1, OriginReads: 1, OriginReadsAvoided: 2, Admissions: 3,
			AdmissionRejections: 15, AdmissionRejectionsReservation: 4,
			AdmissionRejectionsBackgroundBudget: 5, AdmissionRejectionsBackgroundTask: 6,
			AdmissionRejectionReasonsAvailable: true,
			ReadLatencyTotalUS:                 4, ReadLatencyCount: 1,
		},
	}
	component := VaulticDBComponent(writer, cache, daemon.WALInfo{Target: "s3", Durability: "shared-remote", OldestSegmentUnixMS: 125})
	if err := ValidateVaulticDBComponent(component); err != nil {
		t.Fatal(err)
	}
	if component.ProcessStartID != "100" || len(component.Caches) != 1 || component.Caches[0].AvailableBytes != 400 {
		t.Fatalf("component = %+v", component)
	}
	if len(component.Storage) != 2 || component.Storage[1].Role != "wal" || component.Storage[1].Acknowledgement != "shared-remote" {
		t.Fatalf("storage = %+v", component.Storage)
	}
	if component.Caches[0].StagingBytes != 8 || component.Caches[0].InflightFills != 0 {
		t.Fatalf("cache in-flight units = %+v", component.Caches[0])
	}
	if component.Caches[0].DeletionPendingBytes != 25 || component.Caches[0].DeletionAvailability != AvailabilityExact {
		t.Fatalf("cache deletion-pending state = %+v", component.Caches[0])
	}
	if component.WAL.OldestUncheckpointedMS != 75 {
		t.Fatalf("WAL age = %d", component.WAL.OldestUncheckpointedMS)
	}
	for name, want := range map[string]uint64{
		"engine_immutable_memtable_flushes":             3,
		"engine_l0_flush_bytes":                         4,
		"engine_compacted_ssts":                         5,
		"engine_l0_stalls_sst_count":                    6,
		"engine_l0_stalls_ssts_per_key":                 7,
		"engine_get_keys":                               8,
		"engine_filter_point_positives":                 9,
		"engine_filter_point_negatives":                 10,
		"engine_filter_point_false_positives":           11,
		"engine_multi_get_calls":                        12,
		"engine_multi_get_input_keys":                   13,
		"engine_multi_get_unique_keys":                  14,
		"engine_multi_get_sst_visits":                   15,
		"engine_multi_get_candidate_keys":               16,
		"engine_multi_get_needed_blocks":                17,
		"engine_multi_get_coalesced_reads":              18,
		"engine_multi_get_needed_block_bytes":           19,
		"engine_multi_get_coalesced_read_bytes":         20,
		"engine_multi_get_projected_reads_gap_8":        21,
		"engine_multi_get_projected_read_bytes_gap_8":   22,
		"engine_multi_get_projected_reads_gap_32":       23,
		"engine_multi_get_projected_read_bytes_gap_32":  24,
		"engine_multi_get_projected_reads_gap_128":      25,
		"engine_multi_get_projected_read_bytes_gap_128": 26,
		"cache_origin_reads_avoided":                    2,
		"cache_admissions":                              3,
		"cache_admission_rejections":                    15,
		"cache_admission_rejections_reservation":        4,
		"cache_admission_rejections_background_budget":  5,
		"cache_admission_rejections_background_task":    6,
		"cache_read_latency_total":                      4,
	} {
		found := false
		for _, metric := range component.Metrics {
			if metric.Name == name {
				found = true
				if metric.Value != want {
					t.Errorf("metric %s = %d, want %d", name, metric.Value, want)
				}
			}
		}
		if !found {
			t.Errorf("metric %s was not exported", name)
		}
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

func TestCumulativeBucketCountsSaturates(t *testing.T) {
	counts := cumulativeBucketCounts([]uint64{1, 2, ^uint64(0), 1})
	want := []uint64{1, 3, ^uint64(0), ^uint64(0)}
	for index := range want {
		if counts[index] != want[index] {
			t.Fatalf("bucket %d = %d, want %d", index, counts[index], want[index])
		}
	}
}

func TestVaulticDBComponentMarksLegacyDeletionMetricUnavailable(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{Configured: true, AggregateMaxBytesKnown: true, QuotaCoordinationHealthy: true},
		daemon.WALInfo{},
	)
	if component.Caches[0].DeletionAvailability != AvailabilityUnavailable {
		t.Fatalf("legacy deletion availability = %q", component.Caches[0].DeletionAvailability)
	}
}

func TestVaulticDBComponentMarksLegacyAdditiveMetricsUnavailable(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200, Attribution: daemon.AttributionSnapshot{EngineReadMetricsAvailable: true}},
		daemon.ReadCacheStatus{Configured: true, Metrics: daemon.ReadCacheMetrics{AdmissionRejections: 3}},
		daemon.WALInfo{},
	)
	for _, metric := range component.Metrics {
		if strings.HasPrefix(metric.Name, "engine_filter_point_") || metric.Name == "engine_get_keys" {
			if metric.Availability != AvailabilityExact {
				t.Fatalf("legacy point-read metric %s availability = %q", metric.Name, metric.Availability)
			}
			continue
		}
		if metric.Name == "engine_flush_interval" || strings.HasPrefix(metric.Name, "engine_multi_get_") || strings.HasPrefix(metric.Name, "cache_admission_rejections_") {
			if metric.Availability != AvailabilityUnavailable {
				t.Fatalf("legacy metric %s availability = %q", metric.Name, metric.Availability)
			}
		}
	}
}

func TestVaulticDBComponentMarksDeletionMetricStaleWithCoordination(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{Configured: true, AggregateMaxBytesKnown: true, DeletionPendingKnown: true},
		daemon.WALInfo{},
	)
	if component.Caches[0].DeletionAvailability != AvailabilityStale {
		t.Fatalf("stale deletion availability = %q", component.Caches[0].DeletionAvailability)
	}
}

func TestVaulticDBComponentMarksLegacyIdentityEstimated(t *testing.T) {
	component := VaulticDBComponent(daemon.WriterStatus{}, daemon.ReadCacheStatus{}, daemon.WALInfo{})
	if component.Availability != AvailabilityEstimated || component.ProcessStartID != "legacy" || component.Storage[1].Availability != AvailabilityUnavailable || component.Storage[1].ObjectCountAvailability != AvailabilityUnavailable || component.Storage[1].PhysicalAvailability != AvailabilityUnavailable {
		t.Fatalf("component = %+v", component)
	}
}

func TestVaulticDBComponentMarksOpenCacheCircuitStale(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{Configured: true, AggregateMaxBytesKnown: true, QuotaCoordinationHealthy: true, Tiers: []daemon.ReadCacheTierStatus{{ID: "disk", Enabled: true, CircuitOpen: true}}},
		daemon.WALInfo{},
	)
	if component.Caches[0].Availability != AvailabilityStale || component.Caches[0].ControllerState != "degraded" || component.Caches[1].Availability != AvailabilityStale || component.Caches[1].ControllerState != "degraded" {
		t.Fatalf("cache states = %+v", component.Caches)
	}
}

func TestVaulticDBComponentClampsTierCapacityToAggregateLimit(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{
			Configured: true, AggregateMaxBytesKnown: true, AggregateMaxBytes: 100,
			UsedBytes: 90, ReservedBytes: 10, QuotaCoordinationHealthy: true,
			Tiers: []daemon.ReadCacheTierStatus{{ID: "disk", Enabled: true, RequestedMaxBytes: 100}},
		},
		daemon.WALInfo{},
	)
	if component.Caches[1].AvailableBytes != 0 {
		t.Fatalf("tier available bytes = %d", component.Caches[1].AvailableBytes)
	}
}

func TestVaulticDBComponentSaturatesOverflowingTierCapacity(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{
			Configured: true, AggregateMaxBytesKnown: true, AggregateMaxBytes: ^uint64(0),
			QuotaCoordinationHealthy: true,
			Tiers:                    []daemon.ReadCacheTierStatus{{ID: "disk", Enabled: true, RequestedMaxBytes: ^uint64(0), UsedBytes: ^uint64(0), ReservedBytes: 1}},
		},
		daemon.WALInfo{},
	)
	if component.Caches[1].EffectiveBytes != ^uint64(0) {
		t.Fatalf("tier effective bytes = %d", component.Caches[1].EffectiveBytes)
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
		daemon.ReadCacheStatus{Configured: true, QuotaCoordinationHealthy: true, UsedBytes: 10},
		daemon.WALInfo{},
	)
	if component.Caches[0].Availability != AvailabilityEstimated || component.Caches[0].UsedBytes != 10 {
		t.Fatalf("cache = %+v", component.Caches[0])
	}
}

func TestVaulticDBComponentMarksLaggingCacheStatusStale(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{
			Configured: true, AggregateMaxBytesKnown: true, AggregateMaxBytes: 1000, UsedBytes: 40, ReservedBytes: 10, QuotaCoordinationHealthy: true,
			PolicySyncLag: 1, Tiers: []daemon.ReadCacheTierStatus{{ID: "memory", Enabled: true}},
		},
		daemon.WALInfo{},
	)
	if len(component.Caches) != 2 || component.Caches[0].Availability != AvailabilityStale || component.Caches[1].Availability != AvailabilityStale {
		t.Fatalf("cache availability = %+v", component.Caches)
	}
}

func TestVaulticDBComponentPreservesStaleWhenAggregateLimitUnknown(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{Configured: true, QuotaCoordinationHealthy: false},
		daemon.WALInfo{},
	)
	if component.Caches[0].Availability != AvailabilityStale {
		t.Fatalf("cache availability = %q", component.Caches[0].Availability)
	}
}

func TestVaulticDBComponentMarksTierReconciliationLagStale(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{
			Configured: true, AggregateMaxBytesKnown: true, AggregateMaxBytes: 1000,
			UsedBytes: 40, ReservedBytes: 10, QuotaCoordinationHealthy: true,
			Tiers: []daemon.ReadCacheTierStatus{{ID: "memory", Enabled: true, RequestedMaxBytes: 100, UsedBytes: 40, ReservedBytes: 10, ReconciliationLag: 1}, {ID: "disk", Enabled: true}},
		},
		daemon.WALInfo{},
	)
	if component.Caches[0].Availability != AvailabilityStale || component.Caches[1].Availability != AvailabilityStale || component.Caches[2].Availability != AvailabilityExact {
		t.Fatalf("cache availability = %+v", component.Caches)
	}
	if component.Caches[0].ReconciliationLag != 1 || component.Caches[0].ControllerState != "degraded" || component.Caches[1].ReconciliationLag != 1 || component.Caches[1].AvailableBytes != 50 {
		t.Fatalf("cache reconciliation/capacity = %+v", component.Caches)
	}
}

func TestVaulticDBComponentAggregatesAllTierReconciliationLag(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{
			Configured: true, AggregateMaxBytesKnown: true, QuotaCoordinationHealthy: true,
			QuotaReconciliationLag: 1,
			Tiers:                  []daemon.ReadCacheTierStatus{{ID: "memory", Enabled: true, ReconciliationLag: 2}, {ID: "disk", Enabled: true, ReconciliationLag: 3}},
		},
		daemon.WALInfo{},
	)
	if component.Caches[0].ReconciliationLag != 6 {
		t.Fatalf("aggregate reconciliation lag = %d", component.Caches[0].ReconciliationLag)
	}
}

func TestVaulticDBComponentIncludesInstanceInProcessIdentity(t *testing.T) {
	first := VaulticDBComponent(daemon.WriterStatus{ProcessStartedUnixMS: 100, InstanceID: "first"}, daemon.ReadCacheStatus{}, daemon.WALInfo{})
	second := VaulticDBComponent(daemon.WriterStatus{ProcessStartedUnixMS: 100, InstanceID: "second"}, daemon.ReadCacheStatus{}, daemon.WALInfo{})
	if first.ProcessStartID != "100-first" || second.ProcessStartID != "100-second" || first.ProcessStartID == second.ProcessStartID {
		t.Fatalf("process identities = %q, %q", first.ProcessStartID, second.ProcessStartID)
	}
}

func TestVaulticDBComponentPreservesDisabledTierCleanupState(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{
			Configured: true, AggregateMaxBytesKnown: true, QuotaCoordinationHealthy: true,
			Tiers: []daemon.ReadCacheTierStatus{{ID: "disabled", RequestedMaxBytes: 100, UsedBytes: 20}},
		},
		daemon.WALInfo{},
	)
	tier := component.Caches[1]
	if tier.Availability != AvailabilityExact || tier.ControllerState != "not_applicable" || tier.Enabled || tier.EffectiveBytes != 0 || tier.AvailableBytes != 0 || tier.RequestedBytes != 100 || tier.UsedBytes != 20 {
		t.Fatalf("disabled tier = %+v", tier)
	}
}

func TestVaulticDBComponentDisablesAggregateWhenAllTiersDisabled(t *testing.T) {
	component := VaulticDBComponent(
		daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200},
		daemon.ReadCacheStatus{
			Configured: true, AggregateMaxBytesKnown: true, AggregateMaxBytes: 100,
			QuotaCoordinationHealthy: true, UsedBytes: 20,
			Tiers: []daemon.ReadCacheTierStatus{{ID: "disk", UsedBytes: 20}},
		},
		daemon.WALInfo{},
	)
	aggregate := component.Caches[0]
	if aggregate.Enabled || aggregate.EffectiveBytes != 0 || aggregate.AvailableBytes != 0 || aggregate.ControllerState != "not_applicable" || aggregate.UsedBytes != 20 {
		t.Fatalf("aggregate cache = %+v", aggregate)
	}
}

func TestVaulticDBComponentBoundsCacheTierHandoff(t *testing.T) {
	tiers := make([]daemon.ReadCacheTierStatus, MaxMonitorCaches)
	for index := range tiers {
		tiers[index] = daemon.ReadCacheTierStatus{ID: fmt.Sprintf("tier-%02d", index), Enabled: true}
	}
	writer := daemon.WriterStatus{ProcessStartedUnixMS: 100, CapturedUnixMS: 200}
	cache := daemon.ReadCacheStatus{
		Configured: true, AggregateMaxBytesKnown: true, QuotaCoordinationHealthy: true,
		Tiers: tiers[:MaxMonitorCaches-1],
	}
	component := VaulticDBComponent(writer, cache, daemon.WALInfo{})
	if len(component.Caches) != MaxMonitorCaches || component.CardinalityDropped != 0 || component.Availability != AvailabilityExact {
		t.Fatalf("at-limit cache handoff = caches:%d dropped:%d availability:%s", len(component.Caches), component.CardinalityDropped, component.Availability)
	}
	if err := ValidateVaulticDBComponent(component); err != nil {
		t.Fatal(err)
	}

	cache.Tiers = tiers
	component = VaulticDBComponent(writer, cache, daemon.WALInfo{})
	if len(component.Caches) != MaxMonitorCaches || component.CardinalityDropped != 1 || component.Availability != AvailabilityEstimated {
		t.Fatalf("overflow cache handoff = caches:%d dropped:%d availability:%s", len(component.Caches), component.CardinalityDropped, component.Availability)
	}
	if component.Caches[len(component.Caches)-1].ID != "tier-62" {
		t.Fatalf("last retained cache = %q, want deterministic prefix", component.Caches[len(component.Caches)-1].ID)
	}
	if err := ValidateVaulticDBComponent(component); err != nil {
		t.Fatal(err)
	}
}
