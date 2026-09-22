package telemetry

import (
	"fmt"
	"strconv"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
)

func VaulticDBComponent(writer daemon.WriterStatus, cache daemon.ReadCacheStatus, wal daemon.WALInfo) ComponentSnapshot {
	captured := writer.CapturedUnixMS
	if captured <= 0 {
		captured = time.Now().UnixMilli()
	}
	batchQueueAvailability := AvailabilityExact
	if writer.Attribution.EngineBatchQueue.ActiveOverflow != 0 || writer.Attribution.EngineBatchService.ActiveOverflow != 0 {
		batchQueueAvailability = AvailabilityEstimated
	}
	component := ComponentSnapshot{
		Component: "vaulticdb", ProcessStartID: strconv.FormatInt(writer.ProcessStartedUnixMS, 10),
		CapturedUnixMS: captured, Availability: AvailabilityExact,
		Metrics: append(vaulticDBEngineMetrics(writer.Attribution), vaulticDBObjectMetrics(writer.Attribution)...),
		Queues: []QueueSnapshot{{
			Name: "batch_write", Availability: batchQueueAvailability, CapacityAvailability: AvailabilityUnavailable,
			Depth:            writer.Attribution.EngineBatchQueueDepth,
			ActiveWorkers:    writer.Attribution.EngineBatchService.Active,
			Admitted:         writer.Attribution.EngineBatchQueue.Successes,
			Rejected:         writer.Attribution.EngineBatchQueue.Failures,
			OldestItemAgeUS:  writer.Attribution.EngineBatchQueue.OldestActiveUS,
			Backpressure:     vaulticDBBackpressure(writer.Attribution),
			BackpressureTime: writer.Attribution.EngineBackpressure.TotalUS,
		}},
		WAL: &WALSnapshot{
			Target: wal.Target, Durability: wal.Durability, Availability: AvailabilityExact,
			UploadedBytes: wal.UploadedBytes, OutstandingFlushes: wal.OutstandingFlushes,
			RetainedBytes: wal.RetainedBytes, RetainedSegments: wal.RetainedSegments,
			OldestUncheckpointedMS: walOldestAgeMS(captured, wal.OldestSegmentUnixMS),
			DurabilityFailures:     wal.DurabilityFailures, CleanupFailures: wal.CleanupFailures,
			ThrottleReason: vaulticDBBackpressure(writer.Attribution),
		},
		Storage: []StorageSnapshot{
			{
				BackendID: "database", Role: "database", Availability: AvailabilityEstimated,
				ObjectCount: writer.Attribution.EngineSSTCount, Acknowledgement: "unknown",
				ObjectClass: "sst", PlacementState: "primary", Representation: "sst",
				ObjectCountAvailability: AvailabilityEstimated, PayloadAvailability: AvailabilityUnavailable,
				PhysicalAvailability: AvailabilityUnavailable, ReconciliationAvailability: AvailabilityUnavailable,
			},
			{
				BackendID: "wal", Role: "wal", Availability: AvailabilityExact,
				ObjectCount: wal.RetainedSegments, PhysicalBytes: wal.RetainedBytes,
				Acknowledgement: monitorAcknowledgement(wal.Durability), ObjectClass: "wal_segment",
				PlacementState: "primary", Representation: "metadata",
				ObjectCountAvailability: AvailabilityExact, PayloadAvailability: AvailabilityUnavailable,
				PhysicalAvailability: AvailabilityExact, ReconciliationAvailability: AvailabilityUnavailable,
			},
		},
	}
	component.Metrics = append(component.Metrics, vaulticDBProcessMetrics(writer)...)
	if cache.Configured {
		component.Metrics = append(component.Metrics, vaulticDBCacheMetrics(cache.Metrics)...)
	}
	if writer.InstanceID != "" {
		component.ProcessStartID += "-" + writer.InstanceID
	}
	if wal.Target == "" || wal.Durability == "" {
		component.WAL.Availability = AvailabilityUnavailable
		component.Storage[1].Availability = AvailabilityUnavailable
		component.Storage[1].ObjectCountAvailability = AvailabilityUnavailable
		component.Storage[1].PhysicalAvailability = AvailabilityUnavailable
	}
	if writer.ProcessStartedUnixMS <= 0 {
		component.ProcessStartID = "legacy"
		if writer.InstanceID != "" {
			component.ProcessStartID += "-" + writer.InstanceID
		}
		component.Availability = AvailabilityEstimated
	}
	if cache.Configured {
		tiers := cache.Tiers
		maxTiers := MaxMonitorCaches - 1
		if len(tiers) > maxTiers {
			component.CardinalityDropped += uint64(len(tiers) - maxTiers)
			component.Availability = AvailabilityEstimated
			tiers = tiers[:maxTiers]
		}
		globalAvailability := AvailabilityExact
		if !cache.QuotaCoordinationHealthy || cache.QuotaReconciliationLag != 0 || cache.PolicySyncLag != 0 || cache.PolicySyncError != "" {
			globalAvailability = AvailabilityStale
		}
		aggregateAvailability := globalAvailability
		aggregateLag := cache.QuotaReconciliationLag
		aggregateEnabled := len(cache.Tiers) == 0
		aggregateCircuitOpen := false
		aggregateAvailable := uint64(0)
		if cache.AggregateMaxBytesKnown && cache.UsedBytes <= cache.AggregateMaxBytes && cache.ReservedBytes <= cache.AggregateMaxBytes-cache.UsedBytes {
			aggregateAvailable = cache.AggregateMaxBytes - cache.UsedBytes - cache.ReservedBytes
		}
		for _, tier := range cache.Tiers {
			aggregateCircuitOpen = aggregateCircuitOpen || tier.CircuitOpen
			aggregateEnabled = aggregateEnabled || tier.Enabled
			aggregateLag = saturatingAdd(aggregateLag, tier.ReconciliationLag)
			if tier.ReconciliationLag != 0 || tier.CircuitOpen {
				aggregateAvailability = AvailabilityStale
			}
		}
		component.Caches = append(component.Caches, vaulticDBCacheSnapshot("slatedb", cache, aggregateAvailability, aggregateLag, aggregateEnabled, aggregateCircuitOpen))
		for _, tier := range tiers {
			tierAvailability := globalAvailability
			if tier.ReconciliationLag != 0 || tier.CircuitOpen {
				tierAvailability = AvailabilityStale
			}
			effectiveBytes := tier.RequestedMaxBytes
			availableBytes := uint64(0)
			if tier.UsedBytes <= tier.RequestedMaxBytes && tier.ReservedBytes < tier.RequestedMaxBytes-tier.UsedBytes {
				availableBytes = tier.RequestedMaxBytes - tier.UsedBytes - tier.ReservedBytes
			}
			if cache.AggregateMaxBytesKnown && availableBytes > aggregateAvailable {
				availableBytes = aggregateAvailable
			}
			effectiveBytes = saturatingAdd(saturatingAdd(tier.UsedBytes, tier.ReservedBytes), availableBytes)
			controllerState := "healthy"
			circuitState := "closed"
			if globalAvailability == AvailabilityStale || tier.ReconciliationLag != 0 || tier.CircuitOpen {
				controllerState = "degraded"
			}
			if !tier.Enabled {
				effectiveBytes = 0
				availableBytes = 0
				controllerState = "not_applicable"
				circuitState = "not_applicable"
			} else if tier.CircuitOpen {
				circuitState = "open"
			}
			component.Caches = append(component.Caches, CacheSnapshot{
				ID: tier.ID, Availability: tierAvailability, RequestedBytes: tier.RequestedMaxBytes,
				EffectiveBytes: effectiveBytes, UsedBytes: tier.UsedBytes,
				ReservedBytes: tier.ReservedBytes, PinnedBytes: tier.PinnedBytes,
				DeletionPendingBytes: tier.DeletionPendingBytes,
				ReclaimPendingBytes:  tier.PendingReclaimBytes, Hits: tier.Metrics.Hits,
				Misses: tier.Metrics.Misses, OriginReadsAvoided: tier.Metrics.OriginReadsAvoided,
				AvailableBytes: availableBytes, ReconciliationLag: tier.ReconciliationLag,
				ControllerState: controllerState, CircuitState: circuitState, Enabled: tier.Enabled, Family: "unknown",
				Representation: "block", TrafficAvailability: AvailabilityExact,
				TrafficBytesAvailability: AvailabilityUnavailable, FillAvailability: AvailabilityUnavailable, InventoryAvailability: AvailabilityUnavailable,
				DeletionAvailability:          inheritedAvailability(tierAvailability, availabilityFromKnown(tier.DeletionPendingKnown)),
				ReconciliationAgeAvailability: AvailabilityUnavailable, ReconciliationLagAvailability: AvailabilityExact,
			})
		}
	}
	return component
}

func monitorAcknowledgement(value string) string {
	switch value {
	case "persistent", "memory", "inherited", "local-process", "shared-remote", "durable-object-store", "test", "unsupported", "unknown":
		return value
	default:
		return "unknown"
	}
}

func vaulticDBCacheSnapshot(id string, status daemon.ReadCacheStatus, availability Availability, reconciliationLag uint64, enabled, circuitOpen bool) CacheSnapshot {
	if !status.AggregateMaxBytesKnown && availability == AvailabilityExact {
		availability = AvailabilityEstimated
	}
	available := uint64(0)
	effectiveBytes := status.AggregateMaxBytes
	if enabled && status.AggregateMaxBytesKnown && status.UsedBytes <= status.AggregateMaxBytes && status.ReservedBytes < status.AggregateMaxBytes-status.UsedBytes {
		available = status.AggregateMaxBytes - status.UsedBytes - status.ReservedBytes
	}
	if !enabled {
		effectiveBytes = 0
	}
	return CacheSnapshot{
		ID: id, Availability: availability, RequestedBytes: status.AggregateMaxBytes,
		EffectiveBytes: effectiveBytes, UsedBytes: status.UsedBytes,
		ReservedBytes: status.ReservedBytes, PinnedBytes: status.PinnedBytes,
		StagingBytes: status.InflightBytes, DeletionPendingBytes: status.DeletionPendingBytes, ReclaimPendingBytes: status.PendingReclaimBytes,
		AvailableBytes: available, Hits: status.Metrics.Hits, Misses: status.Metrics.Misses,
		OriginReadsAvoided:            status.Metrics.OriginReadsAvoided,
		ReconciliationLag:             reconciliationLag,
		Enabled:                       enabled,
		Family:                        "aggregate",
		Representation:                "block",
		TrafficAvailability:           AvailabilityExact,
		TrafficBytesAvailability:      AvailabilityUnavailable,
		FillAvailability:              AvailabilityUnavailable,
		InventoryAvailability:         AvailabilityUnavailable,
		DeletionAvailability:          inheritedAvailability(availability, availabilityFromKnown(status.DeletionPendingKnown)),
		ReconciliationAgeAvailability: AvailabilityUnavailable,
		ReconciliationLagAvailability: AvailabilityExact,
		ControllerState: func() string {
			if !enabled {
				return "not_applicable"
			}
			if availability != AvailabilityStale && status.QuotaCoordinationHealthy && status.PolicySyncLag == 0 && status.PolicySyncError == "" {
				return "healthy"
			}
			return "degraded"
		}(),
		CircuitState: func() string {
			if !enabled {
				return "not_applicable"
			}
			if circuitOpen {
				return "open"
			}
			return "closed"
		}(),
	}
}

func availabilityFromKnown(known bool) Availability {
	if known {
		return AvailabilityExact
	}
	return AvailabilityUnavailable
}

func vaulticDBEngineMetrics(attribution daemon.AttributionSnapshot) []Metric {
	readAvailability := AvailabilityUnavailable
	if attribution.EngineReadMetricsAvailable {
		readAvailability = AvailabilityExact
	}
	multiGetAvailability := AvailabilityUnavailable
	if attribution.EngineMultiGetMetricsAvailable {
		multiGetAvailability = AvailabilityExact
	}
	metrics := []Metric{
		counterMetric("engine_write_batches", "operations", attribution.EngineWriteBatches),
		counterMetric("engine_write_operations", "operations", attribution.EngineWriteOps),
		counterMetric("engine_backpressure_events", "operations", attribution.EngineBackpressureCount),
		counterMetric("engine_immutable_memtable_flushes", "operations", attribution.EngineImmutableFlushes),
		counterMetric("engine_memtable_write_bytes", "bytes", attribution.EngineMemtableWriteBytes),
		counterMetric("engine_wal_flush_bytes", "bytes", attribution.EngineWALFlushBytes),
		counterMetric("engine_l0_flush_bytes", "bytes", attribution.EngineL0FlushBytes),
		counterMetric("engine_compacted_bytes", "bytes", attribution.EngineCompactedBytes),
		counterMetric("engine_compacted_ssts", "objects", attribution.EngineCompactedSSTs),
		counterMetric("engine_l0_stalls_sst_count", "operations", attribution.EngineL0StallsSSTCount),
		counterMetric("engine_l0_stalls_ssts_per_key", "operations", attribution.EngineL0StallsSSTsPerKey),
		{Name: "engine_get_keys", Kind: MetricCounter, Unit: "operations", Availability: readAvailability, Value: attribution.EngineGetKeys},
		{Name: "engine_filter_point_positives", Kind: MetricCounter, Unit: "operations", Availability: readAvailability, Value: attribution.EngineFilterPointPositive},
		{Name: "engine_filter_point_negatives", Kind: MetricCounter, Unit: "operations", Availability: readAvailability, Value: attribution.EngineFilterPointNegative},
		{Name: "engine_filter_point_false_positives", Kind: MetricCounter, Unit: "operations", Availability: readAvailability, Value: attribution.EngineFilterPointFalsePositive},
		{Name: "engine_multi_get_calls", Kind: MetricCounter, Unit: "operations", Availability: multiGetAvailability, Value: attribution.EngineMultiGetCalls},
		{Name: "engine_multi_get_input_keys", Kind: MetricCounter, Unit: "operations", Availability: multiGetAvailability, Value: attribution.EngineMultiGetInputKeys},
		{Name: "engine_multi_get_unique_keys", Kind: MetricCounter, Unit: "operations", Availability: multiGetAvailability, Value: attribution.EngineMultiGetUniqueKeys},
		{Name: "engine_multi_get_sst_visits", Kind: MetricCounter, Unit: "operations", Availability: multiGetAvailability, Value: attribution.EngineMultiGetSSTVisits},
		{Name: "engine_multi_get_candidate_keys", Kind: MetricCounter, Unit: "operations", Availability: multiGetAvailability, Value: attribution.EngineMultiGetCandidateKeys},
		{Name: "engine_multi_get_needed_blocks", Kind: MetricCounter, Unit: "operations", Availability: multiGetAvailability, Value: attribution.EngineMultiGetNeededBlocks},
		{Name: "engine_multi_get_coalesced_reads", Kind: MetricCounter, Unit: "operations", Availability: multiGetAvailability, Value: attribution.EngineMultiGetCoalescedReads},
		{Name: "engine_multi_get_needed_block_bytes", Kind: MetricCounter, Unit: "bytes", Availability: multiGetAvailability, Value: attribution.EngineMultiGetNeededBlockBytes},
		{Name: "engine_multi_get_coalesced_read_bytes", Kind: MetricCounter, Unit: "bytes", Availability: multiGetAvailability, Value: attribution.EngineMultiGetCoalescedReadBytes},
		{Name: "engine_multi_get_projected_reads_gap_8", Kind: MetricCounter, Unit: "operations", Availability: multiGetAvailability, Value: attribution.EngineMultiGetProjectedReadsGap8},
		{Name: "engine_multi_get_projected_read_bytes_gap_8", Kind: MetricCounter, Unit: "bytes", Availability: multiGetAvailability, Value: attribution.EngineMultiGetProjectedReadBytesGap8},
		{Name: "engine_multi_get_projected_reads_gap_32", Kind: MetricCounter, Unit: "operations", Availability: multiGetAvailability, Value: attribution.EngineMultiGetProjectedReadsGap32},
		{Name: "engine_multi_get_projected_read_bytes_gap_32", Kind: MetricCounter, Unit: "bytes", Availability: multiGetAvailability, Value: attribution.EngineMultiGetProjectedReadBytesGap32},
		{Name: "engine_multi_get_projected_reads_gap_128", Kind: MetricCounter, Unit: "operations", Availability: multiGetAvailability, Value: attribution.EngineMultiGetProjectedReadsGap128},
		{Name: "engine_multi_get_projected_read_bytes_gap_128", Kind: MetricCounter, Unit: "bytes", Availability: multiGetAvailability, Value: attribution.EngineMultiGetProjectedReadBytesGap128},
		gaugeMetric("engine_memtable_bytes", "bytes", attribution.EngineMemtableBytes),
		gaugeMetric("engine_l0_sst_objects", "objects", attribution.EngineL0SSTCount),
		gaugeMetric("engine_sst_objects", "objects", attribution.EngineSSTCount),
		gaugeMetric("engine_sorted_runs", "objects", attribution.EngineSortedRunCount),
		gaugeMetric("engine_running_compactions", "operations", attribution.EngineRunningCompactions),
	}
	timings := []struct {
		name   string
		value  daemon.TimingSnapshot
		labels []Label
	}{
		{name: "admission_wait_latency", value: attribution.AdmissionWait},
		{name: "admission_lock_hold_latency", value: attribution.AdmissionLockHold},
		{name: "fence_check_latency", value: attribution.FenceCheck},
		{name: "write_batch_request_latency", value: attribution.WriteBatchRequest},
		{name: "begin_request_latency", value: attribution.BeginRequest},
		{name: "commit_request_latency", value: attribution.CommitRequest},
		{name: "rollback_request_latency", value: attribution.RollbackRequest},
		{name: "transaction_begin_latency", value: attribution.TransactionBegin},
		{name: "transaction_map_lock_wait_latency", value: attribution.TransactionMapLockWait},
		{name: "transaction_slot_lock_wait_latency", value: attribution.TransactionSlotLockWait},
		{name: "engine_submit_latency", value: attribution.EngineSubmit},
		{name: "durable_wait_latency", value: attribution.DurableWait},
		{name: "finalization_latency", value: attribution.Finalization},
		{name: "engine_backpressure_latency", value: attribution.EngineBackpressure},
		{name: "engine_batch_queue_latency", value: attribution.EngineBatchQueue},
		{name: "engine_batch_service_latency", value: attribution.EngineBatchService},
	}
	for _, timing := range timings {
		metrics = append(metrics, timingMetric(timing.name, timing.value, timing.labels...))
	}
	return metrics
}

func vaulticDBCacheMetrics(cache daemon.ReadCacheMetrics) []Metric {
	reasonAvailability := AvailabilityUnavailable
	if cache.AdmissionRejectionReasonsAvailable {
		reasonAvailability = AvailabilityExact
	}
	return []Metric{
		counterMetric("cache_hits", "operations", cache.Hits),
		counterMetric("cache_misses", "operations", cache.Misses),
		counterMetric("cache_origin_reads", "operations", cache.OriginReads),
		counterMetric("cache_origin_reads_avoided", "operations", cache.OriginReadsAvoided),
		counterMetric("cache_corruptions", "operations", cache.Corruptions),
		counterMetric("cache_timeouts", "operations", cache.Timeouts),
		counterMetric("cache_failures", "operations", cache.Failures),
		counterMetric("cache_bypasses", "operations", cache.Bypasses),
		counterMetric("cache_admissions", "operations", cache.Admissions),
		counterMetric("cache_admission_rejections", "operations", cache.AdmissionRejections),
		{Name: "cache_admission_rejections_reservation", Kind: MetricCounter, Unit: "operations", Availability: reasonAvailability, Value: cache.AdmissionRejectionsReservation},
		{Name: "cache_admission_rejections_background_budget", Kind: MetricCounter, Unit: "operations", Availability: reasonAvailability, Value: cache.AdmissionRejectionsBackgroundBudget},
		{Name: "cache_admission_rejections_background_task", Kind: MetricCounter, Unit: "operations", Availability: reasonAvailability, Value: cache.AdmissionRejectionsBackgroundTask},
		counterMetric("cache_capacity_evictions", "operations", cache.CapacityEvictions),
		counterMetric("cache_idle_evictions", "operations", cache.IdleEvictions),
		counterMetric("cache_absolute_evictions", "operations", cache.AbsoluteEvictions),
		counterMetric("cache_corruption_evictions", "operations", cache.CorruptionEvictions),
		counterMetric("cache_read_latency_total", "microseconds", cache.ReadLatencyTotalUS),
		counterMetric("cache_read_latency_count", "operations", cache.ReadLatencyCount),
		counterMetric("cache_write_latency_total", "microseconds", cache.WriteLatencyTotalUS),
		counterMetric("cache_write_latency_count", "operations", cache.WriteLatencyCount),
	}
}

func vaulticDBProcessMetrics(writer daemon.WriterStatus) []Metric {
	availability := func(available bool) Availability {
		if available {
			return AvailabilityExact
		}
		return AvailabilityUnavailable
	}
	engineAvailability := availability(writer.EngineTuningAvailable)
	return []Metric{
		{Name: "engine_flush_interval", Kind: MetricGauge, Unit: "milliseconds", Availability: engineAvailability, Value: writer.EngineFlushIntervalMS},
		{Name: "engine_max_unflushed_bytes", Kind: MetricGauge, Unit: "bytes", Availability: engineAvailability, Value: writer.EngineMaxUnflushedBytes},
		{Name: "engine_l0_sst_size_bytes", Kind: MetricGauge, Unit: "bytes", Availability: engineAvailability, Value: writer.EngineL0SSTSizeBytes},
		{Name: "engine_block_cache_bytes", Kind: MetricGauge, Unit: "bytes", Availability: engineAvailability, Value: writer.EngineBlockCacheBytes},
		{Name: "engine_meta_cache_bytes", Kind: MetricGauge, Unit: "bytes", Availability: engineAvailability, Value: writer.EngineMetaCacheBytes},
		{Name: "process_cpu_user", Kind: MetricCounter, Unit: "microseconds", Availability: availability(writer.ProcessCPUAvailable), Value: writer.ProcessCPUUserUS},
		{Name: "process_cpu_system", Kind: MetricCounter, Unit: "microseconds", Availability: availability(writer.ProcessCPUAvailable), Value: writer.ProcessCPUSystemUS},
		{Name: "process_rss_bytes", Kind: MetricGauge, Unit: "bytes", Availability: availability(writer.ProcessMemAvailable), Value: writer.ProcessRSSBytes},
		{Name: "process_threads", Kind: MetricGauge, Unit: "operations", Availability: availability(writer.ProcessMemAvailable), Value: writer.ProcessThreads},
		{Name: "process_read_bytes", Kind: MetricCounter, Unit: "bytes", Availability: availability(writer.ProcessIOAvailable), Value: writer.ProcessReadBytes},
		{Name: "process_write_bytes", Kind: MetricCounter, Unit: "bytes", Availability: availability(writer.ProcessIOAvailable), Value: writer.ProcessWriteBytes},
	}
}

func vaulticDBObjectMetrics(attribution daemon.AttributionSnapshot) []Metric {
	roles := []struct {
		name     string
		snapshot daemon.ObjectStoreRoleSnapshot
	}{
		{name: "database", snapshot: attribution.ObjectStoreMain},
		{name: "wal", snapshot: attribution.ObjectStoreWAL},
		{name: "coordination", snapshot: attribution.ObjectStoreCoordination},
	}
	var metrics []Metric
	for _, role := range roles {
		operations := []struct {
			name     string
			snapshot daemon.ObjectOperationSnapshot
		}{
			{name: "put", snapshot: role.snapshot.Put},
			{name: "multipart_init", snapshot: role.snapshot.MultipartInit},
			{name: "multipart_part", snapshot: role.snapshot.MultipartPart},
			{name: "multipart_complete", snapshot: role.snapshot.MultipartComplete},
			{name: "multipart_abort", snapshot: role.snapshot.MultipartAbort},
			{name: "get", snapshot: role.snapshot.Get},
			{name: "head", snapshot: role.snapshot.Head},
			{name: "get_body", snapshot: role.snapshot.GetBody},
			{name: "get_ranges", snapshot: role.snapshot.GetRanges},
			{name: "delete", snapshot: role.snapshot.Delete},
			{name: "list", snapshot: role.snapshot.List},
			{name: "list_with_offset", snapshot: role.snapshot.ListWithOffset},
			{name: "list_with_delimiter", snapshot: role.snapshot.ListWithDelimiter},
			{name: "copy", snapshot: role.snapshot.Copy},
			{name: "rename", snapshot: role.snapshot.Rename},
		}
		for _, operation := range operations {
			labels := []Label{{Name: "role", Value: role.name}}
			metrics = append(metrics, timingMetric("object_"+operation.name+"_latency", operation.snapshot.Timing, labels...))
			availability := AvailabilityUnavailable
			if operation.snapshot.TransferredBytesAvailable {
				availability = AvailabilityExact
			}
			metrics = append(metrics, Metric{
				Name: "object_" + operation.name + "_bytes", Kind: MetricCounter, Unit: "bytes",
				Availability: availability, Labels: labels, Value: operation.snapshot.TransferredBytes,
			})
		}
	}
	return metrics
}

func timingMetric(name string, timing daemon.TimingSnapshot, labels ...Label) Metric {
	availability := AvailabilityExact
	counts := cumulativeBucketCounts(timing.LatencyBucketCounts)
	if len(timing.LatencyBucketUpperUS) == 0 || len(timing.LatencyBucketUpperUS) != len(counts) ||
		counts[len(counts)-1] != timing.Completed || timing.MaxUS > timing.TotalUS ||
		timing.Completed == 0 && (timing.TotalUS != 0 || timing.MaxUS != 0) {
		availability = AvailabilityUnavailable
		return Metric{Name: name, Kind: MetricHistogram, Unit: "microseconds", Availability: availability, Labels: labels, BucketUpper: []uint64{^uint64(0)}, BucketCounts: []uint64{0}}
	}
	return Metric{
		Name: name, Kind: MetricHistogram, Unit: "microseconds", Availability: availability, Labels: labels,
		Count: timing.Completed, Sum: timing.TotalUS, Maximum: timing.MaxUS,
		BucketUpper:  append([]uint64(nil), timing.LatencyBucketUpperUS...),
		BucketCounts: counts,
	}
}

func cumulativeBucketCounts(counts []uint64) []uint64 {
	cumulative := make([]uint64, len(counts))
	var total uint64
	for index, count := range counts {
		total = saturatingAdd(total, count)
		cumulative[index] = total
	}
	return cumulative
}

func counterMetric(name, unit string, value uint64) Metric {
	return Metric{Name: name, Kind: MetricCounter, Unit: unit, Availability: AvailabilityExact, Value: value}
}

func gaugeMetric(name, unit string, value uint64) Metric {
	return Metric{Name: name, Kind: MetricGauge, Unit: unit, Availability: AvailabilityExact, Value: value}
}

func vaulticDBBackpressure(attribution daemon.AttributionSnapshot) string {
	if attribution.EngineBackpressure.Active != 0 {
		return "capacity"
	}
	return "none"
}

func walOldestAgeMS(captured int64, oldest uint64) uint64 {
	if captured <= 0 || oldest == 0 || uint64(captured) <= oldest {
		return 0
	}
	return uint64(captured) - oldest
}

func ValidateVaulticDBComponent(component ComponentSnapshot) error {
	snapshot := NewMonitorSnapshot(time.UnixMilli(component.CapturedUnixMS), component)
	if err := snapshot.Validate(); err != nil {
		return fmt.Errorf("vaulticdb monitoring snapshot: %w", err)
	}
	return nil
}
