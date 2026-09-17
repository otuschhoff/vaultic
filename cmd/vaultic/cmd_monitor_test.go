package main

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/telemetry"
)

func TestMonitorCommandRegistersStableViews(t *testing.T) {
	command := newMonitorCommand(&global.Options{})
	want := map[string]bool{"status": false, "storage": false, "operations": false, "caches": false, "watch": false, "export": false}
	for _, child := range command.Commands() {
		if _, known := want[child.Name()]; known {
			want[child.Name()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Fatalf("monitor command is missing %q", name)
		}
	}
}

func TestMonitorInfluxExportExposesOnlyProtectedTokenSources(t *testing.T) {
	command := newMonitorCommand(&global.Options{})
	influx, _, err := command.Find([]string{"export", "influxdb"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"url", "org", "bucket", "deployment-id", "token-file", "token-env", "interval", "timeout", "queue-capacity", "batch-limit", "retry-limit", "retry-backoff"} {
		if influx.Flags().Lookup(name) == nil {
			t.Fatalf("monitor export influxdb is missing --%s", name)
		}
	}
	if influx.Flags().Lookup("token") != nil {
		t.Fatal("monitor export accepts a token value on the command line")
	}
}

func TestMonitorExportBounds(t *testing.T) {
	valid := monitorExportOptions{Interval: time.Second, Timeout: time.Second, Queue: 1, BatchLimit: 1, RetryLimit: 0}
	if err := validateMonitorExportOptions(valid); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.Queue = 0
	if err := validateMonitorExportOptions(invalid); err == nil {
		t.Fatal("zero queue capacity accepted")
	}
	invalid = valid
	invalid.RetryLimit = 1
	if err := validateMonitorExportOptions(invalid); err == nil {
		t.Fatal("retry-enabled zero backoff accepted")
	}
}

func TestMonitorCommandsExposeDocumentedFilters(t *testing.T) {
	command := newMonitorCommand(&global.Options{})
	for _, test := range []struct {
		command string
		flags   []string
	}{
		{command: "status", flags: []string{"component", "backend", "json"}},
		{command: "storage", flags: []string{"backend", "reconcile", "json"}},
		{command: "operations", flags: []string{"active", "json"}},
		{command: "caches", flags: []string{"cache", "json"}},
	} {
		child, _, err := command.Find([]string{test.command})
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range test.flags {
			if child.Flags().Lookup(name) == nil {
				t.Fatalf("monitor %s is missing --%s", test.command, name)
			}
		}
	}
}

func TestFilterMonitorSnapshotKeepsExplicitUnavailableComponent(t *testing.T) {
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(1, 0),
		telemetry.ComponentSnapshot{
			Component: "vaultic", ProcessStartID: "one", CapturedUnixMS: 1000,
			Availability: telemetry.AvailabilityExact,
			Caches:       []telemetry.CacheSnapshot{{ID: "repository", Availability: telemetry.AvailabilityExact}},
		},
		telemetry.ComponentSnapshot{
			Component: "vaulticdb", ProcessStartID: "unavailable", CapturedUnixMS: 1000,
			Availability: telemetry.AvailabilityUnavailable, Stale: true,
		},
	)
	filterMonitorSnapshot(&snapshot, "caches", monitorOptions{})
	if len(snapshot.Components) != 2 || snapshot.Components[1].Availability != telemetry.AvailabilityUnavailable {
		t.Fatalf("filtered snapshot = %+v", snapshot)
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestWatchMonitorValidatesBeforeCollection(t *testing.T) {
	err := watchMonitor(context.Background(), &global.Options{}, monitorOptions{Interval: time.Millisecond, View: "status"})
	if err == nil || !strings.Contains(err.Error(), "interval") {
		t.Fatalf("watch error = %v", err)
	}
	err = watchMonitor(context.Background(), &global.Options{}, monitorOptions{Interval: time.Second, View: "unknown"})
	if err == nil || !strings.Contains(err.Error(), "unknown monitor view") {
		t.Fatalf("watch error = %v", err)
	}
}

func TestBrokerMonitorWithoutSocketIsExplicitlyUnavailable(t *testing.T) {
	component := collectBrokerMonitorComponent(context.Background(), "", time.Unix(1, 0))
	if component.Component != "key_broker" || component.Availability != telemetry.AvailabilityUnavailable || !component.Stale {
		t.Fatalf("component = %+v", component)
	}
}

func TestActiveOperationFilterRemovesQueueSummaries(t *testing.T) {
	snapshot := telemetry.NewMonitorSnapshot(time.Unix(1, 0), telemetry.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "one", CapturedUnixMS: 1000,
		Availability: telemetry.AvailabilityExact,
		Queues:       []telemetry.QueueSnapshot{{Name: "work", Availability: telemetry.AvailabilityExact}},
		Operations:   []telemetry.ActiveOperation{{ID: "one", Class: "backup", Phase: "write", StartedUnixMS: 1, UpdatedUnixMS: 1}},
	})
	filterMonitorSnapshot(&snapshot, "operations", monitorOptions{Active: true})
	if len(snapshot.Components[0].Queues) != 0 || len(snapshot.Components[0].Operations) != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestMonitorLinesRenderOperationsAndStorage(t *testing.T) {
	captured := time.Unix(10, 0)
	snapshot := telemetry.NewMonitorSnapshot(captured, telemetry.ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "one", CapturedUnixMS: captured.Add(time.Second).UnixMilli(),
		Availability: telemetry.AvailabilityExact,
		Operations:   []telemetry.ActiveOperation{{ID: "op-one", Class: "backup", Phase: "write", StartedUnixMS: captured.Add(-time.Second).UnixMilli(), UpdatedUnixMS: captured.UnixMilli()}},
		Storage:      []telemetry.StorageSnapshot{{BackendID: "primary", Role: "repository", Availability: telemetry.AvailabilityExact, ObjectCount: 2}},
	})
	if output := strings.Join(monitorLines(snapshot, "operations"), "\n"); !strings.Contains(output, "operation op-one") || !strings.Contains(output, "class=backup") || !strings.Contains(output, "age=2s") {
		t.Fatalf("operations output = %q", output)
	}
	if output := strings.Join(monitorLines(snapshot, "storage"), "\n"); !strings.Contains(output, "storage primary") || !strings.Contains(output, "objects=2") {
		t.Fatalf("storage output = %q", output)
	}
}

func TestPlacementStorageSnapshotsAreSorted(t *testing.T) {
	storage := placementStorageSnapshots(map[string]maintenance.BackendPlacementCount{
		"z-backend": {Objects: 1}, "a-backend": {Objects: 2},
	}, time.Unix(1, 0))
	if len(storage) != 2 || storage[0].BackendID != "a-backend" || storage[1].BackendID != "z-backend" {
		t.Fatalf("storage = %+v", storage)
	}
}

func TestRepositoryAggregateStoragePreservesUnknownCoverage(t *testing.T) {
	storage := repositoryAggregateStorage(maintenance.StatsResult{
		Totals: maintenance.StatsGroup{PackCount: 3, PayloadSize: 20}, StoredPhysicalSize: 30,
		PhysicalSizeUnknownPacks: 1,
	})
	if len(storage) != 1 || storage[0].ObjectCount != 3 || storage[0].PhysicalBytes != 30 || storage[0].Availability != telemetry.AvailabilityEstimated {
		t.Fatalf("storage = %+v", storage)
	}
}

func TestMonitorDerivedLinesHandleRatesPercentilesAndResets(t *testing.T) {
	previous := telemetry.NewMonitorSnapshot(time.Unix(1, 0), telemetry.ComponentSnapshot{
		Component: "vaulticdb", ProcessStartID: "one", CapturedUnixMS: 1000, Availability: telemetry.AvailabilityExact,
		Metrics: []telemetry.Metric{
			{Name: "requests", Kind: telemetry.MetricCounter, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: 10},
			{Name: "latency", Kind: telemetry.MetricHistogram, Unit: "microseconds", Availability: telemetry.AvailabilityExact, Count: 2, BucketUpper: []uint64{10, 100, 1000}, BucketCounts: []uint64{1, 2, 2}},
		},
	})
	current := telemetry.NewMonitorSnapshot(time.Unix(3, 0), telemetry.ComponentSnapshot{
		Component: "vaulticdb", ProcessStartID: "one", CapturedUnixMS: 3000, Availability: telemetry.AvailabilityExact,
		Metrics: []telemetry.Metric{
			{Name: "requests", Kind: telemetry.MetricCounter, Unit: "operations", Availability: telemetry.AvailabilityExact, Value: 16},
			{Name: "latency", Kind: telemetry.MetricHistogram, Unit: "microseconds", Availability: telemetry.AvailabilityExact, Count: 6, BucketUpper: []uint64{10, 100, 1000}, BucketCounts: []uint64{1, 5, 6}},
		},
	})
	if output := strings.Join(monitorDerivedLines(&previous, current, "status"), "\n"); !strings.Contains(output, "=3.00 operations/s reset=false") {
		t.Fatalf("rate output = %q", output)
	}
	if output := strings.Join(monitorDerivedLines(&previous, current, "latency"), "\n"); !strings.Contains(output, "p95<=1000") || !strings.Contains(output, "reset=false") {
		t.Fatalf("latency output = %q", output)
	}
	current.Components[0].ProcessStartID = "two"
	if output := strings.Join(monitorDerivedLines(&previous, current, "status"), "\n"); !strings.Contains(output, "reset=true") {
		t.Fatalf("reset output = %q", output)
	}
	current.Components[0].ProcessStartID = "legacy"
	previous.Components[0].ProcessStartID = "legacy"
	if output := strings.Join(monitorDerivedLines(&previous, current, "status"), "\n"); !strings.Contains(output, "reset=true") {
		t.Fatalf("legacy identity output = %q", output)
	}
	current.Components[0].ProcessStartID = "one"
	previous.Components[0].ProcessStartID = "one"
	current.Components[0].Metrics[0].Availability = telemetry.AvailabilityUnavailable
	if output := strings.Join(monitorDerivedLines(&previous, current, "status"), "\n"); !strings.Contains(output, "reset=true") || strings.Contains(output, "operations/s") {
		t.Fatalf("unavailable metric output = %q", output)
	}
	current.Components[0].Metrics[0].Availability = telemetry.AvailabilityExact
	current.Components[0].Stale = true
	if output := strings.Join(monitorDerivedLines(&previous, current, "status"), "\n"); !strings.Contains(output, "reset=true") || strings.Contains(output, "operations/s") {
		t.Fatalf("stale component output = %q", output)
	}
}

func TestHistogramPercentileHandlesSaturatedCount(t *testing.T) {
	if got := histogramPercentile([]uint64{10, math.MaxUint64}, []uint64{0, math.MaxUint64}, 99); got != math.MaxUint64 {
		t.Fatalf("p99 = %d", got)
	}
}
