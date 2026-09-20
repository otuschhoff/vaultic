package telemetry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/fs"
)

type transientExporter struct {
	mu       sync.Mutex
	attempts int
}

type failingExporter struct{}

func (failingExporter) Export(context.Context, MonitorSnapshot) error {
	return errors.New("unavailable")
}

func (exporter *transientExporter) Export(context.Context, MonitorSnapshot) error {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	exporter.attempts++
	if exporter.attempts < 3 {
		return errors.New("temporarily unavailable")
	}
	return nil
}

type blockingExporter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	count   int
}

type snapshotCaptureExporter struct {
	release chan struct{}
	result  chan MonitorSnapshot
}

func (exporter snapshotCaptureExporter) Export(_ context.Context, snapshot MonitorSnapshot) error {
	<-exporter.release
	exporter.result <- snapshot
	return nil
}

func (exporter *blockingExporter) Export(context.Context, MonitorSnapshot) error {
	exporter.once.Do(func() { close(exporter.started) })
	<-exporter.release
	exporter.mu.Lock()
	exporter.count++
	exporter.mu.Unlock()
	return nil
}

func TestAsyncExporterDropsOldestWithoutBlocking(t *testing.T) {
	exporter := &blockingExporter{started: make(chan struct{}), release: make(chan struct{})}
	worker := NewAsyncExporter(exporter, 1)
	snapshot := validMonitorSnapshot()
	if !worker.Submit(snapshot) {
		t.Fatal("first submit failed")
	}
	<-exporter.started
	if !worker.Submit(snapshot) {
		t.Fatal("queued submit failed")
	}
	if !worker.Submit(snapshot) {
		t.Fatal("bounded replacement submit failed")
	}
	if worker.Dropped() != 1 {
		t.Fatalf("dropped = %d", worker.Dropped())
	}
	close(exporter.release)
	worker.Close()
}

func TestAsyncExporterStatsExposeBoundedQueueStaleness(t *testing.T) {
	exporter := &blockingExporter{started: make(chan struct{}), release: make(chan struct{})}
	worker := NewAsyncExporter(exporter, 2)
	now := time.Unix(10, 0)
	worker.now = func() time.Time { return now }
	snapshot := validMonitorSnapshot()
	if !worker.Submit(snapshot) {
		t.Fatal("initial submit failed")
	}
	<-exporter.started
	if !worker.Submit(snapshot) || !worker.Submit(snapshot) {
		t.Fatal("queue fill failed")
	}
	now = now.Add(time.Second)
	stats := worker.Stats()
	if stats.Pending != 3 || stats.Capacity != 3 || !stats.InFlight || stats.OldestAge != time.Second {
		t.Fatalf("stats = %+v", stats)
	}
	close(exporter.release)
	worker.Close()
}

func TestAsyncInfluxExporterBlockedEndpointRemainsBounded(t *testing.T) {
	requestStarted := make(chan struct{}, 1)
	releaseRequests := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRequests) }) }
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case requestStarted <- struct{}{}:
		default:
		}
		select {
		case <-request.Context().Done():
		case <-releaseRequests:
		}
	}))
	defer func() {
		release()
		server.Close()
	}()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("secret-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	exporter, err := NewInfluxExporter(InfluxConfig{
		URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile,
		Timeout: 10 * time.Second, BatchLimit: 100, AllowInsecureHTTP: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker := NewAsyncExporterWithConfig(exporter, AsyncExporterConfig{Capacity: 2, Timeout: 5 * time.Second})
	snapshot := maximumCardinalityMonitorSnapshot()
	if !worker.Submit(snapshot) {
		t.Fatal("initial submit failed")
	}
	select {
	case <-requestStarted:
	case <-time.After(5 * time.Second):
		worker.CloseWithin(0)
		t.Fatal("maximum-cardinality export did not reach blocked endpoint")
	}
	for range 3 {
		if !worker.Submit(snapshot) {
			t.Fatal("maximum-cardinality replacement submit failed")
		}
	}
	maximumStats := worker.Stats()
	if maximumStats.Pending != maximumStats.Capacity || maximumStats.Dropped != 1 || !maximumStats.InFlight {
		t.Fatalf("maximum-cardinality overflow stats = %+v", maximumStats)
	}
	queuedSnapshot := validMonitorSnapshot()
	started := time.Now()
	for range 100 {
		if !worker.Submit(queuedSnapshot) {
			t.Fatal("bounded replacement submit failed")
		}
	}
	elapsed := time.Since(started)
	if elapsed > time.Second {
		t.Fatalf("blocked endpoint delayed submissions by %s", elapsed)
	}
	stats := worker.Stats()
	if stats.Pending > stats.Capacity || stats.Capacity != 3 || !stats.InFlight || stats.Dropped < 98 || stats.OldestAge < 0 {
		t.Fatalf("blocked endpoint stats = %+v", stats)
	}
	t.Logf("blocked export: submissions=100 elapsed=%s pending=%d capacity=%d dropped=%d oldest_age=%s", elapsed, stats.Pending, stats.Capacity, stats.Dropped, stats.OldestAge)
	accounting := NewProductionAccounting(true)
	ctx, action := accounting.StartOperation(context.Background(), "backup", "source", "")
	source, err := fs.NewReader("fixture", io.NopCloser(bytes.NewReader([]byte("payload"))), fs.ReaderOptions{Size: 7})
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	readStarted := time.Now()
	go func() {
		file, openErr := WrapProductionFS(ctx, source, accounting).OpenFile("fixture", fs.O_RDONLY, false)
		if openErr != nil {
			readDone <- openErr
			return
		}
		payload, readErr := io.ReadAll(file)
		if readErr == nil && string(payload) != "payload" {
			readErr = fmt.Errorf("payload = %q", payload)
		}
		readDone <- readErr
	}()
	select {
	case readErr := <-readDone:
		if readErr != nil {
			t.Fatal(readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked exporter delayed production filesystem read")
	}
	t.Logf("instrumented filesystem read while export blocked: elapsed=%s", time.Since(readStarted))
	action.Done(OutcomeSuccess)
	release()
	worker.CloseWithin(5 * time.Second)
}

func TestInfluxExporterMaximumCardinalityBatchesRemainBounded(t *testing.T) {
	snapshot := maximumCardinalityMonitorSnapshot()
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
	body, _ := influxSnapshot(snapshot, "maximum", nil)
	lines := strings.Count(strings.TrimSpace(body), "\n") + 1
	if lines < len(snapshot.Components)*(MaxMonitorMetrics+MaxMonitorOperations+MaxMonitorStorage+MaxMonitorCaches) || len(body) > 32*1024*1024 {
		t.Fatalf("maximum export lines=%d bytes=%d", lines, len(body))
	}
	if !strings.Contains(body, "vaultic_monitor_v2_cardinality_dropped") {
		t.Fatal("cardinality loss counter was not exported")
	}
	t.Logf("maximum-cardinality export: components=%d metrics=%d operations=%d overflow=%d queues=%d storage=%d caches=%d points=%d bytes=%d",
		len(snapshot.Components), len(snapshot.Components[0].Metrics), len(snapshot.Components[0].Operations), len(snapshot.Components[0].OperationOverflow),
		len(snapshot.Components[0].Queues), len(snapshot.Components[0].Storage), len(snapshot.Components[0].Caches), lines, len(body))
}

func BenchmarkInfluxSnapshotMaximumCardinality(benchmark *testing.B) {
	snapshot := maximumCardinalityMonitorSnapshot()
	body, _ := influxSnapshot(snapshot, "maximum", nil)
	benchmark.ReportAllocs()
	benchmark.SetBytes(int64(len(body)))
	for benchmark.Loop() {
		influxSnapshot(snapshot, "maximum", nil)
	}
}

func maximumCardinalityMonitorSnapshot() MonitorSnapshot {
	components := make([]ComponentSnapshot, 0, len(monitorValues("component")))
	componentNames := make([]string, 0, len(monitorValues("component")))
	for name := range monitorValues("component") {
		componentNames = append(componentNames, name)
	}
	sort.Strings(componentNames)
	for _, name := range componentNames {
		component := maximumCardinalityMonitorComponent(name)
		components = append(components, component)
	}
	return NewMonitorSnapshot(time.Unix(1, 0), components...)
}

func maximumCardinalityMonitorComponent(name string) ComponentSnapshot {
	component := ComponentSnapshot{
		Component: name, ProcessStartID: "maximum", CapturedUnixMS: 1000,
		Availability: AvailabilityExact, CardinalityDropped: 7,
		Metrics: maximumCardinalityMetrics(),
		WAL:     &WALSnapshot{Target: "test", Durability: "test", Availability: AvailabilityExact},
	}
	component.Queues = []QueueSnapshot{
		{Name: "batch_write", Availability: AvailabilityExact, CapacityAvailability: AvailabilityExact, Capacity: 8},
		{Name: "legacy_import_ingest", Availability: AvailabilityExact, CapacityAvailability: AvailabilityExact, Capacity: 8},
		{Name: "legacy_import_reduce", Availability: AvailabilityExact, CapacityAvailability: AvailabilityExact, Capacity: 8},
	}
	operationClasses := sortedMonitorValues("operation")
	component.Operations = make([]ActiveOperation, MaxMonitorOperations)
	for index := range component.Operations {
		component.Operations[index] = ActiveOperation{
			ID: fmt.Sprintf("operation-%03d", index), Class: operationClasses[index%len(operationClasses)], Phase: "queued",
			StartedUnixMS: 1, UpdatedUnixMS: 1,
		}
	}
	component.OperationOverflow = make([]OperationOverflowSnapshot, len(operationClasses))
	for index, class := range operationClasses {
		component.OperationOverflow[index] = OperationOverflowSnapshot{Class: class, Count: uint64(index + 1)}
	}
	component.Storage = make([]StorageSnapshot, MaxMonitorStorage)
	for index := range component.Storage {
		component.Storage[index] = StorageSnapshot{
			BackendID: fmt.Sprintf("backend-%03d", index), Role: "repository", Availability: AvailabilityExact,
			ObjectCountAvailability: AvailabilityExact, PayloadAvailability: AvailabilityExact,
			PhysicalAvailability: AvailabilityExact, ReconciliationAvailability: AvailabilityUnavailable,
		}
	}
	component.Caches = make([]CacheSnapshot, MaxMonitorCaches)
	for index := range component.Caches {
		component.Caches[index] = CacheSnapshot{
			ID: fmt.Sprintf("cache-%03d", index), Availability: AvailabilityExact, CircuitState: "closed",
			TrafficAvailability: AvailabilityExact, TrafficBytesAvailability: AvailabilityExact,
			FillAvailability: AvailabilityExact, InventoryAvailability: AvailabilityExact,
			DeletionAvailability: AvailabilityExact, ReconciliationAgeAvailability: AvailabilityExact,
			ReconciliationLagAvailability: AvailabilityExact,
		}
	}
	return component
}

func maximumCardinalityMetrics() []Metric {
	specs := newMonitorMetricSpecs()
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	sort.Strings(names)
	metrics := make([]Metric, 0, MaxMonitorMetrics)
	for _, name := range names {
		spec := specs[name]
		labelSets := maximumMetricLabelSets(spec.labels, MaxMonitorMetrics*2)
		for _, labels := range labelSets {
			if operation, role := labelValue(labels, "operation"), labelValue(labels, "role"); operation != "" && role != "" && !validOperationRole(operation, role) {
				continue
			}
			metric := Metric{Name: name, Kind: spec.kind, Unit: spec.unit, Availability: AvailabilityExact, Labels: labels, Value: 1}
			if spec.kind == MetricHistogram {
				metric.Value = 0
				metric.Count, metric.Sum, metric.Maximum = 1, 1, 1
				metric.BucketUpper = append([]uint64(nil), spec.bounds[0]...)
				metric.BucketCounts = make([]uint64, len(metric.BucketUpper))
				for index := range metric.BucketCounts {
					metric.BucketCounts[index] = 1
				}
			}
			metrics = append(metrics, metric)
			if len(metrics) == MaxMonitorMetrics {
				return metrics
			}
		}
	}
	panic(fmt.Sprintf("monitor schema provides only %d metric identities", len(metrics)))
}

func maximumMetricLabelSets(names []string, limit int) [][]Label {
	if len(names) == 0 {
		return [][]Label{nil}
	}
	sets := [][]Label{{}}
	for _, name := range names {
		values := sortedMonitorValues(name)
		next := make([][]Label, 0, len(sets)*len(values))
		for _, set := range sets {
			for _, value := range values {
				labels := append(append([]Label(nil), set...), Label{Name: name, Value: value})
				next = append(next, labels)
				if len(next) == limit {
					break
				}
			}
			if len(next) == limit {
				break
			}
		}
		sets = next
	}
	return sets
}

func sortedMonitorValues(kind string) []string {
	values := make([]string, 0, len(monitorValues(kind)))
	for value := range monitorValues(kind) {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func TestAsyncExporterConcurrentCloseDoesNotPanic(t *testing.T) {
	exporter := &blockingExporter{started: make(chan struct{}), release: make(chan struct{})}
	worker := NewAsyncExporter(exporter, 8)
	snapshot := validMonitorSnapshot()
	worker.Submit(snapshot)
	<-exporter.started
	var submitters sync.WaitGroup
	for range 8 {
		submitters.Add(1)
		go func() {
			defer submitters.Done()
			for range 100 {
				worker.Submit(snapshot)
			}
		}()
	}
	closed := make(chan struct{})
	go func() {
		worker.Close()
		close(closed)
	}()
	submitters.Wait()
	close(exporter.release)
	<-closed
	if worker.Submit(snapshot) {
		t.Fatal("submit after close succeeded")
	}
}

func TestAsyncExporterCloseDrainsQueue(t *testing.T) {
	exporter := &blockingExporter{started: make(chan struct{}), release: make(chan struct{})}
	worker := NewAsyncExporter(exporter, 2)
	worker.Submit(validMonitorSnapshot())
	<-exporter.started
	worker.Submit(validMonitorSnapshot())
	closed := make(chan struct{})
	go func() {
		worker.Close()
		close(closed)
	}()
	close(exporter.release)
	<-closed
	exporter.mu.Lock()
	count := exporter.count
	exporter.mu.Unlock()
	if count != 2 {
		t.Fatalf("exported snapshots = %d", count)
	}
}

func TestAsyncExporterOwnsSubmittedSnapshot(t *testing.T) {
	exporter := snapshotCaptureExporter{release: make(chan struct{}), result: make(chan MonitorSnapshot, 1)}
	worker := NewAsyncExporter(exporter, 1)
	snapshot := validMonitorSnapshot()
	if !worker.Submit(snapshot) {
		t.Fatal("submit failed")
	}
	snapshot.Components[0].Metrics[0].BucketCounts[0] = 99
	snapshot.Components[0].WAL = &WALSnapshot{Target: "memory"}
	close(exporter.release)
	got := <-exporter.result
	worker.Close()
	if got.Components[0].Metrics[0].BucketCounts[0] == 99 || got.Components[0].WAL != nil {
		t.Fatalf("exported snapshot retained caller ownership: %+v", got.Components[0])
	}
}

func TestAsyncExporterCloseWithinInterruptsRetryBackoff(t *testing.T) {
	worker := NewAsyncExporterWithConfig(failingExporter{}, AsyncExporterConfig{Capacity: 2, RetryLimit: 16, RetryBackoff: time.Minute})
	worker.Submit(validMonitorSnapshot())
	worker.Submit(validMonitorSnapshot())
	started := time.Now()
	worker.CloseWithin(20 * time.Millisecond)
	if time.Since(started) > time.Second {
		t.Fatal("bounded close waited for retry backoff")
	}
}

func TestAsyncExporterRetriesWithinBound(t *testing.T) {
	exporter := &transientExporter{}
	worker := NewAsyncExporterWithConfig(exporter, AsyncExporterConfig{Capacity: 1, RetryLimit: 2})
	if !worker.Submit(validMonitorSnapshot()) {
		t.Fatal("submit failed")
	}
	worker.Close()
	if exporter.attempts != 3 || worker.Failures() != 2 || worker.Dropped() != 0 {
		t.Fatalf("attempts=%d failures=%d dropped=%d", exporter.attempts, worker.Failures(), worker.Dropped())
	}
}

func TestRetryableExportErrorClassifiesHTTPStatus(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError} {
		if !retryableExportError(&influxHTTPError{statusCode: status}) {
			t.Fatalf("status %d was not retryable", status)
		}
	}
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden} {
		if retryableExportError(&influxHTTPError{statusCode: status}) {
			t.Fatalf("status %d was retryable", status)
		}
	}
}

func TestInfluxExporterUsesTokenFileAndBoundedTags(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var body, authorization string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		authorization = request.Header.Get("Authorization")
		encoded, _ := io.ReadAll(request.Body)
		body = string(encoded)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	exporter, err := NewInfluxExporter(InfluxConfig{URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := exporter.Export(context.Background(), validMonitorSnapshot()); err != nil {
		t.Fatal(err)
	}
	if authorization != "Token secret-token" || !strings.Contains(body, "component=vaulticdb") || !strings.Contains(body, "deployment=default") || strings.Contains(body, "generated-1") {
		t.Fatalf("authorization=%q body=%q", authorization, body)
	}
}

func TestInfluxExporterEmitsAggregateSectionsAndCounterResets(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("secret-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		encoded, _ := io.ReadAll(request.Body)
		bodies = append(bodies, string(encoded))
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	exporter, err := NewInfluxExporter(InfluxConfig{URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Caches = []CacheSnapshot{{
		ID: "db", Availability: AvailabilityExact, Hits: 10, Enabled: true, CircuitState: "closed",
		TrafficAvailability: AvailabilityExact, TrafficBytesAvailability: AvailabilityUnavailable, FillAvailability: AvailabilityUnavailable,
		InventoryAvailability: AvailabilityUnavailable, DeletionAvailability: AvailabilityUnavailable, ReconciliationAgeAvailability: AvailabilityUnavailable, ReconciliationLagAvailability: AvailabilityUnavailable,
	}}
	snapshot.Components[0].Storage = []StorageSnapshot{{
		BackendID: "primary", Role: "database", Acknowledgement: "local-process", Availability: AvailabilityExact, ObjectCount: 2,
		ObjectCountAvailability: AvailabilityExact, PayloadAvailability: AvailabilityUnavailable,
		PhysicalAvailability: AvailabilityUnavailable, ReconciliationAvailability: AvailabilityUnavailable,
	}}
	snapshot.Components[0].WAL = &WALSnapshot{Target: "local", Durability: "local-process", ThrottleReason: "capacity", UploadedBytes: 10, Availability: AvailabilityExact}
	if err := exporter.Export(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Components[0].Caches[0].Hits = 13
	snapshot.Components[0].WAL.UploadedBytes = 13
	if err := exporter.Export(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || !strings.Contains(bodies[0], "vaultic_monitor_v2_storage") || !strings.Contains(bodies[0], "acknowledgement=local-process") || !strings.Contains(bodies[0], "vaultic_monitor_v2_wal_uploaded_bytes") || !strings.Contains(bodies[0], "durability=local-process") || !strings.Contains(bodies[0], `throttle="capacity"`) || !strings.Contains(bodies[0], "reset=true") || !strings.Contains(bodies[1], "vaultic_monitor_v2_wal_uploaded_bytes") || !strings.Contains(bodies[1], "delta=3u,reset=false") {
		t.Fatalf("bodies = %#v", bodies)
	}
}

func TestInfluxExporterOmitsUnavailableWALIdentityAndKeepsThrottleOutOfCounterSeries(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].WAL = &WALSnapshot{Availability: AvailabilityUnavailable}
	body, _ := influxSnapshot(snapshot, "default", nil)
	if strings.Contains(body, ",target=,") || strings.Contains(body, ",durability=,") {
		t.Fatalf("empty WAL tag emitted: %q", body)
	}

	snapshot.Components[0].WAL = &WALSnapshot{
		Target: "local", Durability: "local-process", ThrottleReason: "none",
		DurabilityFailures: 10, Availability: AvailabilityExact,
	}
	_, previous := influxSnapshot(snapshot, "default", nil)
	snapshot.Components[0].WAL.ThrottleReason = "capacity"
	snapshot.Components[0].WAL.DurabilityFailures = 13
	body, _ = influxSnapshot(snapshot, "default", previous)
	if !strings.Contains(body, "vaultic_monitor_v2_wal_durability_failures") || !strings.Contains(body, "delta=3u,reset=false") {
		t.Fatalf("throttle change reset WAL counter identity: %q", body)
	}
}

func TestInfluxExporterEncodesMaximumUint64(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Metrics = []Metric{{
		Name: "engine_memtable_bytes", Kind: MetricGauge, Unit: "bytes",
		Availability: AvailabilityExact, Value: ^uint64(0),
	}}
	body, _ := influxSnapshot(snapshot, "default", nil)
	if !strings.Contains(body, "value=18446744073709551615u") {
		t.Fatalf("maximum uint64 was not encoded unsigned: %q", body)
	}
}

func TestInfluxCounterKeepsLastExactBaselineAcrossUnavailableSample(t *testing.T) {
	series := "vaultic_requests,component=vaultic"
	previous := map[string]uint64{series: 10}
	var unavailable bytes.Buffer
	next := make(map[string]uint64)
	writeCounter(&unavailable, previous, next, series, 0, AvailabilityUnavailable, 2)
	if next[series] != 10 || !strings.Contains(unavailable.String(), "delta=0u,reset=false,available=false") {
		t.Fatalf("unavailable output=%q next=%v", unavailable.String(), next)
	}
	var recovered bytes.Buffer
	recoveredNext := make(map[string]uint64)
	writeCounter(&recovered, next, recoveredNext, series, 13, AvailabilityExact, 3)
	if recoveredNext[series] != 13 || !strings.Contains(recovered.String(), "delta=3u,reset=false,available=true") {
		t.Fatalf("recovered output=%q next=%v", recovered.String(), recoveredNext)
	}
}

func TestInfluxMetricIdentityIsIndependentOfLabelOrder(t *testing.T) {
	metric := Metric{
		Name: "dependency_requests", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact,
		Labels: []Label{{Name: "role", Value: "database"}, {Name: "operation", Value: "backup"}, {Name: "outcome", Value: "success"}}, Value: 10,
	}
	snapshot := NewMonitorSnapshot(time.Unix(1, 0), ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "one", CapturedUnixMS: 1000, Availability: AvailabilityExact, Metrics: []Metric{metric},
	})
	_, previous := influxSnapshot(snapshot, "default", nil)
	snapshot.Components[0].Metrics[0].Labels[0], snapshot.Components[0].Metrics[0].Labels[2] = snapshot.Components[0].Metrics[0].Labels[2], snapshot.Components[0].Metrics[0].Labels[0]
	snapshot.Components[0].Metrics[0].Value = 13
	body, _ := influxSnapshot(snapshot, "default", previous)
	if !strings.Contains(body, "delta=3u,reset=false") {
		t.Fatalf("reordered labels changed identity: %q", body)
	}
}

func TestInfluxExporterPreservesAvailabilityState(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Metrics[0].Availability = AvailabilityStale
	snapshot.Components[0].Metrics[0].BucketUpper = []uint64{^uint64(0)}
	snapshot.Components[0].Metrics[0].BucketCounts = []uint64{0}
	snapshot.Components[0].Metrics[0].Count = 0
	snapshot.Components[0].Metrics[0].Sum = 0
	snapshot.Components[0].Metrics[0].Maximum = 0
	body, _ := influxSnapshot(snapshot, "default", nil)
	bucketAvailability := false
	for line := range strings.SplitSeq(body, "\n") {
		if strings.Contains(line, "vaultic_monitor_v2_durable_wait_latency_bucket") && strings.Contains(line, `availability="stale"`) {
			bucketAvailability = true
		}
	}
	if !strings.Contains(body, `availability="stale"`) || !bucketAvailability {
		t.Fatalf("availability state was lost: %q", body)
	}
}

func TestInfluxExporterDoesNotBaselineExactChildOfEstimatedComponent(t *testing.T) {
	snapshot := NewMonitorSnapshot(time.Unix(1, 0), ComponentSnapshot{
		Component: "vaulticdb", ProcessStartID: "legacy", CapturedUnixMS: 1000,
		Availability: AvailabilityEstimated,
		Metrics:      []Metric{{Name: "engine_write_batches", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Value: 10}},
	})
	body, next := influxSnapshot(snapshot, "default", nil)
	if !strings.Contains(body, `availability="estimated"`) || len(next) != 0 {
		t.Fatalf("estimated component established counter baseline: body=%q next=%v", body, next)
	}
}

func TestInfluxExporterPreservesQueueBackpressureState(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Queues[0].Backpressure = "capacity"
	snapshot.Components[0].Queues[0].BackpressureTime = 42
	body, _ := influxSnapshot(snapshot, "default", nil)
	if !strings.Contains(body, `backpressure="capacity"`) || !strings.Contains(body, "backpressure_time_us=42u") {
		t.Fatalf("queue backpressure was lost: %q", body)
	}
}

func TestInfluxExporterPreservesCacheReconciliationState(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Caches = []CacheSnapshot{{
		ID: "slatedb", Availability: AvailabilityStale, ReconciliationLag: 2,
		ReconciliationAgeMS: 30, ControllerState: "degraded", CircuitState: "open",
		TrafficAvailability: AvailabilityUnavailable, TrafficBytesAvailability: AvailabilityUnavailable, FillAvailability: AvailabilityUnavailable,
		InventoryAvailability: AvailabilityUnavailable, DeletionAvailability: AvailabilityExact, ReconciliationAgeAvailability: AvailabilityExact, ReconciliationLagAvailability: AvailabilityExact,
	}}
	body, _ := influxSnapshot(snapshot, "default", nil)
	if !strings.Contains(body, "reconciliation_lag=2u") || !strings.Contains(body, "reconciliation_age_ms=30u") || !strings.Contains(body, `controller_state="degraded"`) || !strings.Contains(body, `circuit_state="open"`) {
		t.Fatalf("cache reconciliation state was lost: %q", body)
	}
}

func TestInfluxExporterPreservesInventoryAndFieldAvailability(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Storage = []StorageSnapshot{{
		BackendID: "primary", Role: "repository", Availability: AvailabilityExact,
		ObjectClass: "pack", PlacementState: "reconciled", Representation: "encrypted_pack", ReconciledAtMS: 900,
		ObjectCountAvailability: AvailabilityExact, PayloadAvailability: AvailabilityExact,
		PhysicalAvailability: AvailabilityUnavailable, ReconciliationAvailability: AvailabilityExact,
	}}
	snapshot.Components[0].Caches = []CacheSnapshot{{
		ID: "slatedb", Availability: AvailabilityExact, Enabled: true, Family: "slatedb", Representation: "block",
		ControllerState: "healthy", CircuitState: "closed", TrafficAvailability: AvailabilityUnavailable, TrafficBytesAvailability: AvailabilityUnavailable, FillAvailability: AvailabilityUnavailable,
		InventoryAvailability: AvailabilityUnavailable, DeletionAvailability: AvailabilityUnavailable, ReconciliationAgeAvailability: AvailabilityUnavailable, ReconciliationLagAvailability: AvailabilityUnavailable,
	}}
	body, next := influxSnapshot(snapshot, "default", nil)
	if !strings.Contains(body, "object_class=pack") || !strings.Contains(body, "placement_state=reconciled") || !strings.Contains(body, "reconciled_at_ms=900i") || !strings.Contains(body, `inflight_fills_availability="unavailable"`) || !strings.Contains(body, `deletion_pending_availability="unavailable"`) {
		t.Fatalf("inventory dimensions were lost: %q", body)
	}
	for series := range next {
		if strings.Contains(series, "cache_origin_bytes") || strings.Contains(series, "cache_cache_bytes") || strings.Contains(series, "cache_hits") || strings.Contains(series, "cache_misses") || strings.Contains(series, "cache_origin_reads_avoided") {
			t.Fatalf("unavailable traffic established baseline: %q", series)
		}
	}
}

func TestInfluxExporterRequiresProtectedTokenSource(t *testing.T) {
	_, err := NewInfluxExporter(InfluxConfig{URL: "https://localhost", Org: "ops", Bucket: "monitor"})
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("error = %v", err)
	}
	_, err = NewInfluxExporter(InfluxConfig{URL: "https://localhost", Org: "ops", Bucket: "monitor", TokenEnv: "MISSING_" + time.Now().Format("150405.000")})
	if err == nil || !strings.Contains(err.Error(), "not set") {
		t.Fatalf("error = %v", err)
	}
	_, err = NewInfluxExporter(InfluxConfig{URL: "https://localhost", Org: "ops", Bucket: "monitor", TokenEnv: "PATH", BatchLimit: 10001})
	if err == nil || !strings.Contains(err.Error(), "batch limit") {
		t.Fatalf("error = %v", err)
	}
}

func TestInfluxExporterRejectsOversizedEnvironmentToken(t *testing.T) {
	t.Setenv("VAULTIC_TEST_INFLUX_TOKEN", strings.Repeat("x", maxTokenFileBytes+1))
	if _, err := NewInfluxExporter(InfluxConfig{URL: "https://localhost", Org: "ops", Bucket: "monitor", TokenEnv: "VAULTIC_TEST_INFLUX_TOKEN"}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized environment token error = %v", err)
	}
}

func TestInfluxExporterClearsInactiveOperationClasses(t *testing.T) {
	snapshot := validMonitorSnapshot()
	body, _ := influxSnapshot(snapshot, "default", nil)
	if !strings.Contains(body, "active_operations,component=vaulticdb,deployment=default,operation=backup count=0u") || !strings.Contains(body, "operation=legacy_import count=1u") {
		t.Fatalf("active operation gauges = %q", body)
	}
}

func TestInfluxExporterMarksUnavailableOperationAndQueueCapacity(t *testing.T) {
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Availability = AvailabilityUnavailable
	body, _ := influxSnapshot(snapshot, "default", nil)
	if !strings.Contains(body, `active_operations,component=vaulticdb,deployment=default,operation=backup count=0u,oldest_age_us=0u,available=false,availability="unavailable"`) || !strings.Contains(body, `capacity_available=false,capacity_availability="unavailable"`) {
		t.Fatalf("unavailable operation or capacity = %q", body)
	}
}

func TestInfluxSnapshotUsesStableSeriesAndResetsOnProcessChange(t *testing.T) {
	snapshot := NewMonitorSnapshot(time.Unix(1, 0), ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "one", CapturedUnixMS: 1000, Availability: AvailabilityExact,
		Metrics: []Metric{{Name: "engine_write_batches", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Value: 10}},
	})
	body, previous, processes := influxSnapshotWithProcesses(snapshot, "default", nil, map[string]string{})
	if strings.Contains(body, ",process_start=") || !strings.Contains(body, `process_start_id="one"`) {
		t.Fatalf("process identity encoding = %q", body)
	}
	snapshot.Components[0].ProcessStartID = "two"
	snapshot.Components[0].Metrics[0].Value = 13
	body, _, _ = influxSnapshotWithProcesses(snapshot, "default", previous, processes)
	if !strings.Contains(body, "delta=0u,reset=true") {
		t.Fatalf("restart counter = %q", body)
	}
}

func TestInfluxExporterInvalidatesBaselineAfterPartialDelivery(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("secret-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	requests := 0
	failAt := 0
	var finalBody string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, incoming *http.Request) {
		requests++
		encoded, _ := io.ReadAll(incoming.Body)
		if requests == failAt {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if failAt != 0 && requests > failAt {
			finalBody += string(encoded)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	exporter, err := NewInfluxExporter(InfluxConfig{URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile, BatchLimit: 1000, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := NewMonitorSnapshot(time.Unix(1, 0), ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "one", CapturedUnixMS: 1000, Availability: AvailabilityExact,
		Metrics: []Metric{{Name: "engine_write_batches", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Value: 10}},
	})
	if err := exporter.Export(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	exporter.batchLimit = 1
	failAt = requests + 2
	snapshot.Components[0].Metrics[0].Value = 13
	if err := exporter.Export(context.Background(), snapshot); err == nil {
		t.Fatal("partial delivery failure was ignored")
	}
	exporter.batchLimit = 1000
	snapshot.Components[0].Metrics[0].Value = 15
	if err := exporter.Export(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(finalBody, "vaultic_monitor_v2_engine_write_batches") || !strings.Contains(finalBody, "delta=0u,reset=true") {
		t.Fatalf("final body = %q", finalBody)
	}
}

func TestInfluxExporterRejectsInsecureTokenFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewInfluxExporter(InfluxConfig{URL: "https://localhost", Org: "ops", Bucket: "monitor", TokenFile: tokenFile}); err == nil || !strings.Contains(err.Error(), "owner-accessible only") {
		t.Fatalf("insecure token error = %v", err)
	}
}

func TestInfluxExporterRequiresHTTPS(t *testing.T) {
	t.Setenv("VAULTIC_TEST_INFLUX_TOKEN", "secret")
	if _, err := NewInfluxExporter(InfluxConfig{URL: "http://influx.example", Org: "ops", Bucket: "monitor", TokenEnv: "VAULTIC_TEST_INFLUX_TOKEN"}); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("HTTP endpoint error = %v", err)
	}
	if _, err := NewInfluxExporter(InfluxConfig{URL: "http://influx.example", Org: "ops", Bucket: "monitor", TokenEnv: "VAULTIC_TEST_INFLUX_TOKEN", AllowInsecureHTTP: true}); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("non-loopback HTTP endpoint error = %v", err)
	}
}

func TestInfluxExporterRejectsRedirectWithoutForwardingToken(t *testing.T) {
	var targetAuthorization string
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetAuthorization = request.Header.Get("Authorization")
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	t.Setenv("VAULTIC_TEST_INFLUX_TOKEN", "secret")
	exporter, err := NewInfluxExporter(InfluxConfig{URL: redirect.URL, Org: "ops", Bucket: "monitor", TokenEnv: "VAULTIC_TEST_INFLUX_TOKEN", AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := exporter.Export(context.Background(), validMonitorSnapshot()); err == nil {
		t.Fatal("redirect was accepted")
	}
	if targetAuthorization != "" {
		t.Fatalf("redirect target received authorization %q", targetAuthorization)
	}
}

func TestInfluxExporterRejectsTokenSymlink(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	link := filepath.Join(directory, "token")
	if err := os.WriteFile(target, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := NewInfluxExporter(InfluxConfig{URL: "https://localhost", Org: "ops", Bucket: "monitor", TokenFile: link}); err == nil {
		t.Fatal("token symlink was accepted")
	}
}

func TestInfluxExporterRejectsOversizedTokenFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, make([]byte, maxTokenFileBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewInfluxExporter(InfluxConfig{URL: "https://localhost", Org: "ops", Bucket: "monitor", TokenFile: tokenFile}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized token error = %v", err)
	}
}

func TestInheritedAvailabilityUsesMostSevereApplicableState(t *testing.T) {
	if got := inheritedAvailability(AvailabilityUnavailable, AvailabilityStale); got != AvailabilityUnavailable {
		t.Fatalf("availability = %q", got)
	}
	if got := inheritedAvailability(AvailabilityStale, AvailabilityEstimated); got != AvailabilityStale {
		t.Fatalf("availability = %q", got)
	}
	if got := inheritedAvailability(AvailabilityUnavailable, AvailabilityNotApplicable); got != AvailabilityNotApplicable {
		t.Fatalf("not-applicable availability = %q", got)
	}
}

func TestInfluxExporterBoundsRequestBatch(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("secret-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		encoded, _ := io.ReadAll(request.Body)
		if strings.Count(strings.TrimSpace(string(encoded)), "\n")+1 > 2 {
			t.Errorf("batch exceeded two points: %q", encoded)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	exporter, err := NewInfluxExporter(InfluxConfig{URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile, BatchLimit: 2, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := exporter.Export(context.Background(), validMonitorSnapshot()); err != nil {
		t.Fatal(err)
	}
	if requests < 2 {
		t.Fatalf("requests = %d", requests)
	}
}

func TestInfluxExporterMarksResetAfterPartialBatchFailure(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("secret-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := 0
	var finalBody string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, incoming *http.Request) {
		request++
		encoded, _ := io.ReadAll(incoming.Body)
		if request == 3 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if request > 3 {
			finalBody += string(encoded)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	exporter, err := NewInfluxExporter(InfluxConfig{URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile, BatchLimit: 1, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := NewMonitorSnapshot(time.Unix(1, 0), ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "one", CapturedUnixMS: 1000, Availability: AvailabilityExact,
		Metrics: []Metric{{Name: "engine_write_batches", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Value: 10}},
		Storage: []StorageSnapshot{{
			BackendID: "primary", Role: "repository", Availability: AvailabilityExact,
			ObjectCountAvailability: AvailabilityUnavailable, PayloadAvailability: AvailabilityUnavailable,
			PhysicalAvailability: AvailabilityUnavailable, ReconciliationAvailability: AvailabilityUnavailable,
		}},
	})
	if err := exporter.Export(context.Background(), snapshot); err == nil {
		t.Fatal("partial batch failure was ignored")
	}
	snapshot.Components[0].Metrics[0].Value = 13
	if err := exporter.Export(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(finalBody, "vaultic_monitor_v2_engine_write_batches") || !strings.Contains(finalBody, "reset=true") {
		t.Fatalf("final body = %q", finalBody)
	}
}

func TestInfluxExporterMarksResetAfterFirstBatchFailure(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("secret-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	failNext := false
	var finalBody string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, incoming *http.Request) {
		encoded, _ := io.ReadAll(incoming.Body)
		if failNext {
			failNext = false
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		finalBody = string(encoded)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	exporter, err := NewInfluxExporter(InfluxConfig{URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile, BatchLimit: 1000, AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := NewMonitorSnapshot(time.Unix(1, 0), ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "one", CapturedUnixMS: 1000, Availability: AvailabilityExact,
		Metrics: []Metric{{Name: "engine_write_batches", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Value: 10}},
	})
	if err := exporter.Export(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	failNext = true
	snapshot.Components[0].Metrics[0].Value = 13
	if err := exporter.Export(context.Background(), snapshot); err == nil {
		t.Fatal("first batch failure was ignored")
	}
	snapshot.Components[0].Metrics[0].Value = 15
	if err := exporter.Export(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(finalBody, "vaultic_monitor_v2_engine_write_batches") || !strings.Contains(finalBody, "delta=0u,reset=true") {
		t.Fatalf("final body = %q", finalBody)
	}
}
