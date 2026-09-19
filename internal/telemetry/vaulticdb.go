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
	metrics := []Metric{
		counterMetric("engine_write_batches", "operations", attribution.EngineWriteBatches),
		counterMetric("engine_write_operations", "operations", attribution.EngineWriteOps),
		counterMetric("engine_backpressure_events", "operations", attribution.EngineBackpressureCount),
		counterMetric("engine_memtable_write_bytes", "bytes", attribution.EngineMemtableWriteBytes),
		counterMetric("engine_wal_flush_bytes", "bytes", attribution.EngineWALFlushBytes),
		counterMetric("engine_compacted_bytes", "bytes", attribution.EngineCompactedBytes),
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
		{name: "transaction_begin_latency", value: attribution.TransactionBegin},
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
	if len(timing.LatencyBucketUpperUS) == 0 || len(timing.LatencyBucketUpperUS) != len(timing.LatencyBucketCounts) {
		availability = AvailabilityUnavailable
		return Metric{Name: name, Kind: MetricHistogram, Unit: "microseconds", Availability: availability, Labels: labels, BucketUpper: []uint64{^uint64(0)}, BucketCounts: []uint64{0}}
	}
	return Metric{
		Name: name, Kind: MetricHistogram, Unit: "microseconds", Availability: availability, Labels: labels,
		Count: timing.Completed, Sum: timing.TotalUS, Maximum: timing.MaxUS,
		BucketUpper:  append([]uint64(nil), timing.LatencyBucketUpperUS...),
		BucketCounts: append([]uint64(nil), timing.LatencyBucketCounts...),
	}
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
