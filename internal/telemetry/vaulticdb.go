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
	component := ComponentSnapshot{
		Component: "vaulticdb", ProcessStartID: strconv.FormatInt(writer.ProcessStartedUnixMS, 10),
		CapturedUnixMS: captured, Availability: AvailabilityExact,
		Metrics: append(vaulticDBEngineMetrics(writer.Attribution), vaulticDBObjectMetrics(writer.Attribution)...),
		Queues: []QueueSnapshot{{
			Name: "batch_write", Availability: AvailabilityExact,
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
				ObjectCount: writer.Attribution.EngineSSTCount,
			},
			{
				BackendID: "wal", Role: "wal", Availability: AvailabilityExact,
				ObjectCount: wal.RetainedSegments, PhysicalBytes: wal.RetainedBytes,
				Acknowledgement: monitorAcknowledgement(wal.Durability),
			},
		},
	}
	if writer.ProcessStartedUnixMS <= 0 {
		component.ProcessStartID = "legacy"
		component.Availability = AvailabilityEstimated
	}
	if cache.Configured {
		component.Caches = append(component.Caches, vaulticDBCacheSnapshot("slatedb", cache))
		for _, tier := range cache.Tiers {
			component.Caches = append(component.Caches, CacheSnapshot{
				ID: tier.ID, Availability: AvailabilityExact, RequestedBytes: tier.RequestedMaxBytes,
				EffectiveBytes: tier.RequestedMaxBytes, UsedBytes: tier.UsedBytes,
				ReservedBytes: tier.ReservedBytes, PinnedBytes: tier.PinnedBytes,
				ReclaimPendingBytes: tier.PendingReclaimBytes, Hits: tier.Metrics.Hits,
				Misses: tier.Metrics.Misses, OriginReadsAvoided: tier.Metrics.OriginReadsAvoided,
			})
		}
	}
	return component
}

func monitorAcknowledgement(value string) string {
	switch value {
	case "persistent", "memory", "inherited", "unknown":
		return value
	default:
		return "unknown"
	}
}

func vaulticDBCacheSnapshot(id string, status daemon.ReadCacheStatus) CacheSnapshot {
	availability := AvailabilityExact
	if !status.AggregateMaxBytesKnown {
		availability = AvailabilityEstimated
	}
	available := uint64(0)
	if status.AggregateMaxBytesKnown && status.AggregateMaxBytes > status.UsedBytes+status.ReservedBytes {
		available = status.AggregateMaxBytes - status.UsedBytes - status.ReservedBytes
	}
	return CacheSnapshot{
		ID: id, Availability: availability, RequestedBytes: status.AggregateMaxBytes,
		EffectiveBytes: status.AggregateMaxBytes, UsedBytes: status.UsedBytes,
		ReservedBytes: status.ReservedBytes, PinnedBytes: status.PinnedBytes,
		StagingBytes: status.InflightBytes, ReclaimPendingBytes: status.PendingReclaimBytes,
		AvailableBytes: available, Hits: status.Metrics.Hits, Misses: status.Metrics.Misses,
		OriginReadsAvoided: status.Metrics.OriginReadsAvoided,
	}
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
