package telemetry

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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
	exporter, err := NewInfluxExporter(InfluxConfig{URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile})
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
	exporter, err := NewInfluxExporter(InfluxConfig{URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := validMonitorSnapshot()
	snapshot.Components[0].Caches = []CacheSnapshot{{ID: "db", Availability: AvailabilityExact, Hits: 10}}
	snapshot.Components[0].Storage = []StorageSnapshot{{BackendID: "primary", Role: "database", Availability: AvailabilityExact, ObjectCount: 2}}
	snapshot.Components[0].WAL = &WALSnapshot{Target: "local", Durability: "persistent", Availability: AvailabilityExact}
	if err := exporter.Export(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Components[0].Caches[0].Hits = 13
	if err := exporter.Export(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || !strings.Contains(bodies[0], "vaultic_storage") || !strings.Contains(bodies[0], "vaultic_wal") || !strings.Contains(bodies[0], "reset=true") || !strings.Contains(bodies[1], "delta=3i,reset=false") {
		t.Fatalf("bodies = %#v", bodies)
	}
}

func TestInfluxCounterKeepsLastExactBaselineAcrossUnavailableSample(t *testing.T) {
	series := "vaultic_requests,component=vaultic"
	previous := map[string]uint64{series: 10}
	var unavailable bytes.Buffer
	next := make(map[string]uint64)
	writeCounter(&unavailable, previous, next, series, 0, false, 2)
	if next[series] != 10 || !strings.Contains(unavailable.String(), "delta=0i,reset=true,available=false") {
		t.Fatalf("unavailable output=%q next=%v", unavailable.String(), next)
	}
	var recovered bytes.Buffer
	recoveredNext := make(map[string]uint64)
	writeCounter(&recovered, next, recoveredNext, series, 13, true, 3)
	if recoveredNext[series] != 13 || !strings.Contains(recovered.String(), "delta=3i,reset=false,available=true") {
		t.Fatalf("recovered output=%q next=%v", recovered.String(), recoveredNext)
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
	exporter, err := NewInfluxExporter(InfluxConfig{URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile, BatchLimit: 2})
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
	exporter, err := NewInfluxExporter(InfluxConfig{URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile, BatchLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := NewMonitorSnapshot(time.Unix(1, 0), ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "one", CapturedUnixMS: 1000, Availability: AvailabilityExact,
		Metrics: []Metric{{Name: "requests", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Value: 10}},
		Storage: []StorageSnapshot{{BackendID: "primary", Role: "repository", Availability: AvailabilityExact}},
	})
	if err := exporter.Export(context.Background(), snapshot); err == nil {
		t.Fatal("partial batch failure was ignored")
	}
	snapshot.Components[0].Metrics[0].Value = 13
	if err := exporter.Export(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(finalBody, "vaultic_requests") || !strings.Contains(finalBody, "reset=true") {
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
	exporter, err := NewInfluxExporter(InfluxConfig{URL: server.URL, Org: "ops", Bucket: "monitor", TokenFile: tokenFile, BatchLimit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := NewMonitorSnapshot(time.Unix(1, 0), ComponentSnapshot{
		Component: "vaultic", ProcessStartID: "one", CapturedUnixMS: 1000, Availability: AvailabilityExact,
		Metrics: []Metric{{Name: "requests", Kind: MetricCounter, Unit: "operations", Availability: AvailabilityExact, Value: 10}},
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
	if !strings.Contains(finalBody, "vaultic_requests") || !strings.Contains(finalBody, "reset=true") {
		t.Fatalf("final body = %q", finalBody)
	}
}
