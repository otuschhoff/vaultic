package telemetry

import (
	"strings"
	"testing"
	"time"
)

func validMonitorSnapshot() MonitorSnapshot {
	return NewMonitorSnapshot(time.Unix(1, 0), ComponentSnapshot{
		Component: "vaulticdb", ProcessStartID: "process-1", CapturedUnixMS: 1000,
		Availability: AvailabilityExact,
		Metrics: []Metric{{
			Name: "commit_latency", Kind: MetricHistogram, Unit: "microseconds",
			Availability: AvailabilityExact, Labels: []Label{{Name: "outcome", Value: "success"}},
			Count: 2, Sum: 12, Maximum: 8, BucketUpper: []uint64{10, 100}, BucketCounts: []uint64{2, 2},
		}},
		Queues: []QueueSnapshot{{Name: "batch_write", Availability: AvailabilityExact, Depth: 1, Capacity: 8}},
		Operations: []ActiveOperation{{
			ID: "generated-1", Class: "legacy_import", Phase: "reduce", StartedUnixMS: 1, UpdatedUnixMS: 2,
		}},
	})
}

func TestMonitorSnapshotValidationAcceptsBoundedSchema(t *testing.T) {
	if err := validMonitorSnapshot().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMonitorSnapshotValidationRejectsUnboundedAndMalformedValues(t *testing.T) {
	tests := []struct {
		name string
		edit func(*MonitorSnapshot)
		want string
	}{
		{name: "version", edit: func(snapshot *MonitorSnapshot) { snapshot.SchemaVersion++ }, want: "unsupported"},
		{name: "unsafe-process-start-id", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].ProcessStartID = "process\\tag"
		}, want: "invalid process start ID"},
		{name: "secret-label-name", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].Labels[0].Name = "access-token"
		}, want: "bounded schema name"},
		{name: "label-newline", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].Labels[0].Value = "secret\nvalue"
		}, want: "invalid label"},
		{name: "unknown-label", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].Labels[0].Name = "snapshot"
		}, want: "unbounded label"},
		{name: "unbounded-label-value", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].Labels[0].Value = "snapshot-1234"
		}, want: "unbounded value"},
		{name: "histogram-shape", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].BucketCounts = []uint64{1}
		}, want: "invalid histogram"},
		{name: "histogram-terminal", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].BucketCounts = []uint64{1, 1}
		}, want: "terminal bucket"},
		{name: "histogram-bucket-limit", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Metrics[0].BucketUpper = make([]uint64, MaxHistogramBuckets+1)
			snapshot.Components[0].Metrics[0].BucketCounts = make([]uint64, MaxHistogramBuckets+1)
		}, want: "buckets exceed"},
		{name: "queue-capacity", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Queues[0].Depth = 9
		}, want: "depth exceeds"},
		{name: "operation-limit", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Operations = make([]ActiveOperation, MaxMonitorOperations+1)
		}, want: "operations exceed"},
		{name: "unsafe-operation-id", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Operations[0].ID = "/repository/private/path"
		}, want: "invalid ID"},
		{name: "unbounded-operation-class", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Operations[0].Class = "custom_user_value"
		}, want: "unbounded class"},
		{name: "cache-limit", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Caches = make([]CacheSnapshot, MaxMonitorCaches+1)
		}, want: "caches exceed"},
		{name: "duplicate-cache", edit: func(snapshot *MonitorSnapshot) {
			snapshot.Components[0].Caches = []CacheSnapshot{{ID: "slatedb", Availability: AvailabilityExact}, {ID: "slatedb", Availability: AvailabilityExact}}
		}, want: "duplicate cache"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := validMonitorSnapshot()
			test.edit(&snapshot)
			if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestMonitorSnapshotValidationRejectsDuplicateMetricIdentity(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Metrics = append(snapshot.Components[0].Metrics, snapshot.Components[0].Metrics[0])
	if err := snapshot.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate metric identity") {
		t.Fatalf("validation error = %v", err)
	}
}
