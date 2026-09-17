package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type SnapshotExporter interface {
	Export(context.Context, MonitorSnapshot) error
}

type AsyncExporter struct {
	exporter SnapshotExporter
	queue    chan MonitorSnapshot
	dropped  Counter
	failures Counter
	closed   atomic.Bool
	done     chan struct{}
	once     sync.Once
	mu       sync.RWMutex
	config   AsyncExporterConfig
	ctx      context.Context
	cancel   context.CancelFunc
}

type AsyncExporterConfig struct {
	Capacity     int
	RetryLimit   int
	RetryBackoff time.Duration
	Timeout      time.Duration
}

func NewAsyncExporter(exporter SnapshotExporter, capacity int) *AsyncExporter {
	return NewAsyncExporterWithConfig(exporter, AsyncExporterConfig{Capacity: capacity})
}

func NewAsyncExporterWithConfig(exporter SnapshotExporter, config AsyncExporterConfig) *AsyncExporter {
	if exporter == nil || config.Capacity <= 0 || config.Capacity > 1024 || config.RetryLimit < 0 || config.RetryLimit > 16 || config.RetryBackoff < 0 || config.Timeout < 0 {
		panic("telemetry exporter requires an implementation and bounded positive capacity")
	}
	ctx, cancel := context.WithCancel(context.Background())
	worker := &AsyncExporter{exporter: exporter, queue: make(chan MonitorSnapshot, config.Capacity), done: make(chan struct{}), config: config, ctx: ctx, cancel: cancel}
	go worker.run()
	return worker
}

func (worker *AsyncExporter) Submit(snapshot MonitorSnapshot) bool {
	worker.mu.RLock()
	defer worker.mu.RUnlock()
	if worker.closed.Load() || snapshot.Validate() != nil {
		return false
	}
	select {
	case worker.queue <- snapshot:
		return true
	default:
		select {
		case <-worker.queue:
			worker.dropped.Add(1)
		default:
		}
		select {
		case worker.queue <- snapshot:
			return true
		default:
			worker.dropped.Add(1)
			return false
		}
	}
}

func (worker *AsyncExporter) Dropped() uint64  { return worker.dropped.Load() }
func (worker *AsyncExporter) Failures() uint64 { return worker.failures.Load() }

func (worker *AsyncExporter) Close() {
	worker.CloseWithin(30 * time.Second)
}

func (worker *AsyncExporter) CloseWithin(timeout time.Duration) {
	worker.once.Do(func() {
		worker.mu.Lock()
		worker.closed.Store(true)
		close(worker.queue)
		worker.mu.Unlock()
	})
	if timeout <= 0 {
		worker.cancel()
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-worker.done:
		return
	case <-timer.C:
		worker.cancel()
	}
}

func (worker *AsyncExporter) run() {
	defer close(worker.done)
	for snapshot := range worker.queue {
		for attempt := 0; ; attempt++ {
			ctx := worker.ctx
			cancel := func() {}
			if worker.config.Timeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, worker.config.Timeout)
			}
			err := worker.exporter.Export(ctx, snapshot)
			cancel()
			if err == nil {
				break
			}
			worker.failures.Add(1)
			if attempt >= worker.config.RetryLimit {
				worker.dropped.Add(1)
				break
			}
			delay := worker.config.RetryBackoff << attempt
			if delay <= 0 {
				continue
			}
			timer := time.NewTimer(delay)
			select {
			case <-worker.ctx.Done():
				timer.Stop()
				worker.dropped.Add(1)
				for range worker.queue {
					worker.dropped.Add(1)
				}
				return
			case <-timer.C:
			}
		}
	}
}

type InfluxConfig struct {
	URL          string
	Org          string
	Bucket       string
	DeploymentID string
	TokenFile    string
	TokenEnv     string
	Timeout      time.Duration
	BatchLimit   int
	Client       *http.Client
}

type InfluxExporter struct {
	endpoint     string
	token        string
	client       *http.Client
	deploymentID string
	mu           sync.Mutex
	previous     map[string]uint64
	batchLimit   int
}

func NewInfluxExporter(config InfluxConfig) (*InfluxExporter, error) {
	if config.URL == "" || config.Org == "" || config.Bucket == "" {
		return nil, fmt.Errorf("InfluxDB URL, organization, and bucket are required")
	}
	if (config.TokenFile == "") == (config.TokenEnv == "") {
		return nil, fmt.Errorf("exactly one InfluxDB token file or protected environment source is required")
	}
	var encoded []byte
	var err error
	if config.TokenFile != "" {
		encoded, err = os.ReadFile(config.TokenFile)
		if err != nil {
			return nil, fmt.Errorf("read InfluxDB token file: %w", err)
		}
	} else {
		value, present := os.LookupEnv(config.TokenEnv)
		if !present {
			return nil, fmt.Errorf("InfluxDB token environment source is not set")
		}
		encoded = []byte(value)
	}
	token := strings.TrimSpace(string(encoded))
	if token == "" || strings.ContainsAny(token, "\r\n\x00") {
		return nil, fmt.Errorf("InfluxDB token source is empty or malformed")
	}
	endpoint, err := url.Parse(config.URL)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, fmt.Errorf("invalid InfluxDB URL")
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/")
	if !strings.HasSuffix(endpoint.Path, "/api/v2/write") {
		endpoint.Path += "/api/v2/write"
	}
	query := endpoint.Query()
	query.Set("org", config.Org)
	query.Set("bucket", config.Bucket)
	query.Set("precision", "ms")
	endpoint.RawQuery = query.Encode()
	client := config.Client
	if client == nil {
		timeout := config.Timeout
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	}
	deploymentID := config.DeploymentID
	if deploymentID == "" {
		deploymentID = "default"
	}
	if !validConfiguredID(deploymentID) {
		return nil, fmt.Errorf("invalid InfluxDB deployment ID")
	}
	batchLimit := config.BatchLimit
	if batchLimit == 0 {
		batchLimit = 1000
	}
	if batchLimit < 1 || batchLimit > 10000 {
		return nil, fmt.Errorf("InfluxDB batch limit must be between 1 and 10000")
	}
	return &InfluxExporter{endpoint: endpoint.String(), token: token, client: client, deploymentID: deploymentID, previous: make(map[string]uint64), batchLimit: batchLimit}, nil
}

func (exporter *InfluxExporter) Export(ctx context.Context, snapshot MonitorSnapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	body, next := influxSnapshot(snapshot, exporter.deploymentID, exporter.previous)
	lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	for start := 0; start < len(lines); start += exporter.batchLimit {
		end := start + exporter.batchLimit
		if end > len(lines) {
			end = len(lines)
		}
		if err := exporter.write(ctx, strings.Join(lines[start:end], "\n")+"\n"); err != nil {
			exporter.previous = make(map[string]uint64)
			return err
		}
	}
	exporter.previous = next
	return nil
}

func (exporter *InfluxExporter) write(ctx context.Context, body string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, exporter.endpoint, strings.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Token "+exporter.token)
	request.Header.Set("Content-Type", "text/plain; charset=utf-8")
	response, err := exporter.client.Do(request)
	if err != nil {
		return fmt.Errorf("export monitoring snapshot to InfluxDB: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return fmt.Errorf("export monitoring snapshot to InfluxDB: server returned %s", response.Status)
	}
	return nil
}

func influxSnapshot(snapshot MonitorSnapshot, deploymentID string, previous map[string]uint64) (string, map[string]uint64) {
	var output bytes.Buffer
	next := make(map[string]uint64, MaxMonitorComponents*MaxMonitorMetrics)
	for _, component := range snapshot.Components {
		baseTags := ",component=" + escapeTag(component.Component) + ",deployment=" + escapeTag(deploymentID) + ",process_start=" + escapeTag(component.ProcessStartID)
		fmt.Fprintf(&output, "vaultic_component%s available=%v,stale=%v %d\n", baseTags, component.Availability == AvailabilityExact, component.Stale, component.CapturedUnixMS)
		for _, metric := range component.Metrics {
			tags := baseTags
			for _, label := range metric.Labels {
				tags += "," + escapeTag(label.Name) + "=" + escapeTag(label.Value)
			}
			measurement := "vaultic_" + metric.Name + tags
			switch metric.Kind {
			case MetricGauge:
				fmt.Fprintf(&output, "%s value=%si,available=%v %d\n", measurement, strconv.FormatUint(metric.Value, 10), metric.Availability == AvailabilityExact, component.CapturedUnixMS)
			case MetricCounter:
				writeCounter(&output, previous, next, measurement, metric.Value, metric.Availability == AvailabilityExact, component.CapturedUnixMS)
			case MetricHistogram:
				fmt.Fprintf(&output, "%s count=%si,sum=%si,maximum=%si,available=%v %d\n", measurement, strconv.FormatUint(metric.Count, 10), strconv.FormatUint(metric.Sum, 10), strconv.FormatUint(metric.Maximum, 10), metric.Availability == AvailabilityExact, component.CapturedUnixMS)
				for index := range metric.BucketUpper {
					fmt.Fprintf(&output, "vaultic_%s_bucket%s,le=%s count=%si %d\n", metric.Name, tags, strconv.FormatUint(metric.BucketUpper[index], 10), strconv.FormatUint(metric.BucketCounts[index], 10), component.CapturedUnixMS)
				}
			}
		}
		for _, queue := range component.Queues {
			tags := baseTags + ",queue=" + escapeTag(queue.Name)
			fmt.Fprintf(&output, "vaultic_queue%s depth=%si,capacity=%si,active_workers=%si,oldest_age_us=%si,available=%v %d\n", tags, u64(queue.Depth), u64(queue.Capacity), u64(queue.ActiveWorkers), u64(queue.OldestItemAgeUS), queue.Availability == AvailabilityExact, component.CapturedUnixMS)
			writeCounter(&output, previous, next, "vaultic_queue_admitted"+tags, queue.Admitted, queue.Availability == AvailabilityExact, component.CapturedUnixMS)
			writeCounter(&output, previous, next, "vaultic_queue_rejected"+tags, queue.Rejected, queue.Availability == AvailabilityExact, component.CapturedUnixMS)
		}
		operationCounts := make(map[string]uint64)
		operationOldest := make(map[string]uint64)
		for _, operation := range component.Operations {
			operationCounts[operation.Class]++
			age := uint64(0)
			if component.CapturedUnixMS > operation.StartedUnixMS {
				age = uint64(component.CapturedUnixMS-operation.StartedUnixMS) * 1000
			}
			if age > operationOldest[operation.Class] {
				operationOldest[operation.Class] = age
			}
		}
		classes := make([]string, 0, len(operationCounts))
		for class := range operationCounts {
			classes = append(classes, class)
		}
		sort.Strings(classes)
		for _, class := range classes {
			count := operationCounts[class]
			fmt.Fprintf(&output, "vaultic_active_operations%s,operation=%s count=%si,oldest_age_us=%si %d\n", baseTags, escapeTag(class), u64(count), u64(operationOldest[class]), component.CapturedUnixMS)
		}
		for _, storage := range component.Storage {
			tags := baseTags + ",backend=" + escapeTag(storage.BackendID) + ",role=" + escapeTag(storage.Role)
			fmt.Fprintf(&output, "vaultic_storage%s objects=%si,payload_bytes=%si,physical_bytes=%si,available=%v %d\n", tags, u64(storage.ObjectCount), u64(storage.PayloadBytes), u64(storage.PhysicalBytes), storage.Availability == AvailabilityExact, component.CapturedUnixMS)
		}
		for _, cache := range component.Caches {
			tags := baseTags + ",cache=" + escapeTag(cache.ID)
			fmt.Fprintf(&output, "vaultic_cache%s requested_bytes=%si,effective_bytes=%si,used_bytes=%si,reserved_bytes=%si,pinned_bytes=%si,staging_bytes=%si,reclaim_pending_bytes=%si,available_bytes=%si,inflight_fills=%si,available=%v %d\n", tags, u64(cache.RequestedBytes), u64(cache.EffectiveBytes), u64(cache.UsedBytes), u64(cache.ReservedBytes), u64(cache.PinnedBytes), u64(cache.StagingBytes), u64(cache.ReclaimPendingBytes), u64(cache.AvailableBytes), u64(cache.InflightFills), cache.Availability == AvailabilityExact, component.CapturedUnixMS)
			for _, counter := range []struct {
				name  string
				value uint64
			}{{"hits", cache.Hits}, {"misses", cache.Misses}, {"origin_bytes", cache.OriginBytes}, {"cache_bytes", cache.CacheBytes}, {"origin_reads_avoided", cache.OriginReadsAvoided}} {
				writeCounter(&output, previous, next, "vaultic_cache_"+counter.name+tags, counter.value, cache.Availability == AvailabilityExact, component.CapturedUnixMS)
			}
		}
		if wal := component.WAL; wal != nil {
			tags := baseTags + ",target=" + escapeTag(wal.Target)
			fmt.Fprintf(&output, "vaultic_wal%s uploaded_bytes=%si,outstanding_flushes=%si,retained_bytes=%si,retained_segments=%si,oldest_uncheckpointed_ms=%si,available=%v %d\n", tags, u64(wal.UploadedBytes), u64(wal.OutstandingFlushes), u64(wal.RetainedBytes), u64(wal.RetainedSegments), u64(wal.OldestUncheckpointedMS), wal.Availability == AvailabilityExact, component.CapturedUnixMS)
			writeCounter(&output, previous, next, "vaultic_wal_durability_failures"+tags, wal.DurabilityFailures, wal.Availability == AvailabilityExact, component.CapturedUnixMS)
			writeCounter(&output, previous, next, "vaultic_wal_cleanup_failures"+tags, wal.CleanupFailures, wal.Availability == AvailabilityExact, component.CapturedUnixMS)
		}
	}
	return output.String(), next
}

func writeCounter(output *bytes.Buffer, previous, next map[string]uint64, series string, value uint64, available bool, timestamp int64) {
	old, found := previous[series]
	if !available {
		if found {
			next[series] = old
		}
		fmt.Fprintf(output, "%s value=%si,delta=0i,reset=true,available=false %d\n", series, u64(value), timestamp)
		return
	}
	reset := !found || value < old
	delta := uint64(0)
	if found && !reset {
		delta = value - old
	}
	next[series] = value
	fmt.Fprintf(output, "%s value=%si,delta=%si,reset=%v,available=%v %d\n", series, u64(value), u64(delta), reset, available, timestamp)
}

func u64(value uint64) string { return strconv.FormatUint(value, 10) }
