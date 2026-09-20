package telemetry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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
	queue    chan queuedMonitorSnapshot
	dropped  Counter
	failures Counter
	activeAt atomic.Pointer[time.Time]
	closed   atomic.Bool
	done     chan struct{}
	once     sync.Once
	mu       sync.RWMutex
	config   AsyncExporterConfig
	ctx      context.Context
	cancel   context.CancelFunc
	now      func() time.Time
}

type queuedMonitorSnapshot struct {
	snapshot   MonitorSnapshot
	enqueuedAt time.Time
}

type ExporterStats struct {
	Failures  uint64
	Dropped   uint64
	Pending   int
	Capacity  int
	InFlight  bool
	OldestAge time.Duration
}

type AsyncExporterConfig struct {
	Capacity     int
	RetryLimit   int
	RetryBackoff time.Duration
	Timeout      time.Duration
}

const (
	maxExporterRetryBackoff  = time.Minute
	maxExporterRetryDuration = 5 * time.Minute
	maxTokenFileBytes        = 64 * 1024
)

func NewAsyncExporter(exporter SnapshotExporter, capacity int) *AsyncExporter {
	return NewAsyncExporterWithConfig(exporter, AsyncExporterConfig{Capacity: capacity})
}

func NewAsyncExporterWithConfig(exporter SnapshotExporter, config AsyncExporterConfig) *AsyncExporter {
	if exporter == nil || config.Capacity <= 0 || config.Capacity > 1024 || config.RetryLimit < 0 || config.RetryLimit > 16 || config.RetryBackoff < 0 || config.RetryBackoff > maxExporterRetryBackoff || config.Timeout < 0 {
		panic("telemetry exporter requires an implementation and bounded positive capacity")
	}
	ctx, cancel := context.WithCancel(context.Background())
	worker := &AsyncExporter{exporter: exporter, queue: make(chan queuedMonitorSnapshot, config.Capacity), done: make(chan struct{}), config: config, ctx: ctx, cancel: cancel, now: time.Now}
	go worker.run()
	return worker
}

func (worker *AsyncExporter) Submit(snapshot MonitorSnapshot) bool {
	worker.mu.RLock()
	defer worker.mu.RUnlock()
	if worker.closed.Load() || snapshot.Validate() != nil {
		return false
	}
	queued := queuedMonitorSnapshot{snapshot: cloneMonitorSnapshot(snapshot), enqueuedAt: worker.now()}
	select {
	case worker.queue <- queued:
		return true
	default:
		select {
		case <-worker.queue:
			worker.dropped.Add(1)
		default:
		}
		select {
		case worker.queue <- queued:
			return true
		default:
			worker.dropped.Add(1)
			return false
		}
	}
}

func cloneMonitorSnapshot(snapshot MonitorSnapshot) MonitorSnapshot {
	clone := snapshot
	clone.Components = append([]ComponentSnapshot(nil), snapshot.Components...)
	for componentIndex := range clone.Components {
		component := &clone.Components[componentIndex]
		component.Metrics = append([]Metric(nil), component.Metrics...)
		for metricIndex := range component.Metrics {
			metric := &component.Metrics[metricIndex]
			metric.Labels = append([]Label(nil), metric.Labels...)
			metric.BucketUpper = append([]uint64(nil), metric.BucketUpper...)
			metric.BucketCounts = append([]uint64(nil), metric.BucketCounts...)
		}
		component.Queues = append([]QueueSnapshot(nil), component.Queues...)
		component.Operations = append([]ActiveOperation(nil), component.Operations...)
		component.OperationOverflow = append([]OperationOverflowSnapshot(nil), component.OperationOverflow...)
		component.Storage = append([]StorageSnapshot(nil), component.Storage...)
		component.Caches = append([]CacheSnapshot(nil), component.Caches...)
		if component.WAL != nil {
			wal := *component.WAL
			component.WAL = &wal
		}
	}
	return clone
}

func (worker *AsyncExporter) Dropped() uint64  { return worker.dropped.Load() }
func (worker *AsyncExporter) Failures() uint64 { return worker.failures.Load() }

func (worker *AsyncExporter) Stats() ExporterStats {
	worker.mu.RLock()
	defer worker.mu.RUnlock()
	stats := ExporterStats{
		Failures: worker.failures.Load(), Dropped: worker.dropped.Load(),
		Pending: len(worker.queue), Capacity: cap(worker.queue) + 1,
	}
	if activeAt := worker.activeAt.Load(); activeAt != nil {
		stats.InFlight = true
		stats.Pending++
		stats.OldestAge = worker.now().Sub(*activeAt)
		if stats.OldestAge < 0 {
			stats.OldestAge = 0
		}
	}
	return stats
}

func (worker *AsyncExporter) Close() {
	worker.CloseWithin(30 * time.Second)
}

func (worker *AsyncExporter) CloseWithin(timeout time.Duration) bool {
	worker.once.Do(func() {
		worker.mu.Lock()
		worker.closed.Store(true)
		close(worker.queue)
		worker.mu.Unlock()
	})
	if timeout <= 0 {
		worker.cancel()
		return false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-worker.done:
		return true
	case <-timer.C:
		worker.cancel()
		return false
	}
}

func (worker *AsyncExporter) run() {
	defer close(worker.done)
	defer worker.activeAt.Store(nil)
	for queued := range worker.queue {
		worker.activeAt.Store(&queued.enqueuedAt)
		snapshot := queued.snapshot
		var retryBackoffTotal time.Duration
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
			if !retryableExportError(err) || attempt >= worker.config.RetryLimit || retryBackoffTotal >= maxExporterRetryDuration {
				worker.dropped.Add(1)
				break
			}
			delay := worker.config.RetryBackoff
			for exponent := 0; exponent < attempt && delay < maxExporterRetryBackoff; exponent++ {
				if delay > maxExporterRetryBackoff/2 {
					delay = maxExporterRetryBackoff
					break
				}
				delay *= 2
			}
			if remaining := maxExporterRetryDuration - retryBackoffTotal; delay > remaining {
				delay = remaining
			}
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
				retryBackoffTotal += delay
			}
		}
		worker.activeAt.Store(nil)
	}
}

type influxHTTPError struct {
	statusCode int
	status     string
}

func (err *influxHTTPError) Error() string {
	return "export monitoring snapshot to InfluxDB: server returned " + err.status
}

func retryableExportError(err error) bool {
	var httpErr *influxHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.statusCode == http.StatusRequestTimeout || httpErr.statusCode == http.StatusTooManyRequests || httpErr.statusCode >= http.StatusInternalServerError
	}
	return !errors.Is(err, context.Canceled)
}

type InfluxConfig struct {
	URL               string
	Org               string
	Bucket            string
	DeploymentID      string
	TokenFile         string
	TokenEnv          string
	Timeout           time.Duration
	BatchLimit        int
	Client            *http.Client
	AllowInsecureHTTP bool
}

type InfluxExporter struct {
	endpoint      string
	token         string
	client        *http.Client
	deploymentID  string
	mu            sync.Mutex
	previous      map[string]uint64
	processStarts map[string]string
	batchLimit    int
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
		encoded, err = readProtectedTokenFile(config.TokenFile)
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
	if len(encoded) > maxTokenFileBytes {
		return nil, fmt.Errorf("InfluxDB token source exceeds %d bytes", maxTokenFileBytes)
	}
	token := strings.TrimSpace(string(encoded))
	if token == "" || strings.ContainsAny(token, "\r\n\x00") {
		return nil, fmt.Errorf("InfluxDB token source is empty or malformed")
	}
	endpoint, err := url.Parse(config.URL)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, fmt.Errorf("invalid InfluxDB URL")
	}
	loopback := endpoint.Hostname() == "localhost"
	if address := net.ParseIP(endpoint.Hostname()); address != nil {
		loopback = address.IsLoopback()
	}
	if endpoint.Scheme != "https" && !(config.AllowInsecureHTTP && endpoint.Scheme == "http" && loopback) {
		return nil, fmt.Errorf("InfluxDB URL must use HTTPS")
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
	clientCopy := *client
	clientCopy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	client = &clientCopy
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
	return &InfluxExporter{endpoint: endpoint.String(), token: token, client: client, deploymentID: deploymentID, previous: make(map[string]uint64), processStarts: make(map[string]string), batchLimit: batchLimit}, nil
}

func (exporter *InfluxExporter) Export(ctx context.Context, snapshot MonitorSnapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	body, next, nextProcesses := influxSnapshotWithProcesses(snapshot, exporter.deploymentID, exporter.previous, exporter.processStarts)
	lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	for start := 0; start < len(lines); start += exporter.batchLimit {
		end := start + exporter.batchLimit
		if end > len(lines) {
			end = len(lines)
		}
		if err := exporter.write(ctx, strings.Join(lines[start:end], "\n")+"\n"); err != nil {
			exporter.previous = make(map[string]uint64)
			exporter.processStarts = make(map[string]string)
			return err
		}
	}
	exporter.previous = next
	exporter.processStarts = nextProcesses
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
		if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, 4096)); err != nil {
			return fmt.Errorf("drain InfluxDB error response: %w", err)
		}
		return &influxHTTPError{statusCode: response.StatusCode, status: response.Status}
	}
	return nil
}

func influxSnapshot(snapshot MonitorSnapshot, deploymentID string, previous map[string]uint64) (string, map[string]uint64) {
	body, next, _ := influxSnapshotWithProcesses(snapshot, deploymentID, previous, nil)
	return body, next
}

func influxSnapshotWithProcesses(snapshot MonitorSnapshot, deploymentID string, previous map[string]uint64, previousProcesses map[string]string) (string, map[string]uint64, map[string]string) {
	const measurementPrefix = "vaultic_monitor_v2_"
	var output bytes.Buffer
	next := make(map[string]uint64, MaxMonitorComponents*MaxMonitorMetrics)
	nextProcesses := make(map[string]string, len(snapshot.Components))
	for _, component := range snapshot.Components {
		baseTags := ",component=" + escapeTag(component.Component) + ",deployment=" + escapeTag(deploymentID)
		counterPrevious := previous
		if previousProcesses != nil {
			if prior, found := previousProcesses[component.Component]; !found || prior != component.ProcessStartID {
				counterPrevious = nil
			}
		}
		nextProcesses[component.Component] = component.ProcessStartID
		fmt.Fprintf(&output, measurementPrefix+"component%s process_start_id=\"%s\",available=%v,availability=\"%s\",stale=%v %d\n", baseTags, escapeFieldString(component.ProcessStartID), component.Availability == AvailabilityExact, component.Availability, component.Stale, component.CapturedUnixMS)
		writeCounter(&output, counterPrevious, next, measurementPrefix+"cardinality_dropped"+baseTags, component.CardinalityDropped, component.Availability, component.CapturedUnixMS)
		for _, metric := range component.Metrics {
			tags := baseTags
			labels := append([]Label(nil), metric.Labels...)
			sort.Slice(labels, func(left, right int) bool { return labels[left].Name < labels[right].Name })
			for _, label := range labels {
				tags += "," + escapeTag(label.Name) + "=" + escapeTag(label.Value)
			}
			measurement := measurementPrefix + metric.Name + tags
			metricAvailability := inheritedAvailability(component.Availability, metric.Availability)
			availability := escapeTag(string(metricAvailability))
			switch metric.Kind {
			case MetricGauge:
				fmt.Fprintf(&output, "%s value=%su,available=%v,availability=\"%s\" %d\n", measurement, strconv.FormatUint(metric.Value, 10), metricAvailability == AvailabilityExact, availability, component.CapturedUnixMS)
			case MetricCounter:
				writeCounter(&output, counterPrevious, next, measurement, metric.Value, metricAvailability, component.CapturedUnixMS)
			case MetricHistogram:
				fmt.Fprintf(&output, "%s count=%su,sum=%su,maximum=%su,available=%v,availability=\"%s\" %d\n", measurement, strconv.FormatUint(metric.Count, 10), strconv.FormatUint(metric.Sum, 10), strconv.FormatUint(metric.Maximum, 10), metricAvailability == AvailabilityExact, availability, component.CapturedUnixMS)
				for index := range metric.BucketUpper {
					fmt.Fprintf(&output, measurementPrefix+"%s_bucket%s,le=%s count=%su,available=%v,availability=\"%s\" %d\n", metric.Name, tags, strconv.FormatUint(metric.BucketUpper[index], 10), strconv.FormatUint(metric.BucketCounts[index], 10), metricAvailability == AvailabilityExact, availability, component.CapturedUnixMS)
				}
			}
		}
		for _, queue := range component.Queues {
			tags := baseTags + ",queue=" + escapeTag(queue.Name)
			availability := inheritedAvailability(component.Availability, queue.Availability)
			capacityAvailability := inheritedAvailability(component.Availability, queue.CapacityAvailability)
			fmt.Fprintf(&output, measurementPrefix+"queue%s depth=%su,capacity=%su,capacity_available=%v,capacity_availability=\"%s\",active_workers=%su,oldest_age_us=%su,backpressure=\"%s\",backpressure_time_us=%su,available=%v,availability=\"%s\" %d\n", tags, u64(queue.Depth), u64(queue.Capacity), capacityAvailability == AvailabilityExact, capacityAvailability, u64(queue.ActiveWorkers), u64(queue.OldestItemAgeUS), queue.Backpressure, u64(queue.BackpressureTime), availability == AvailabilityExact, availability, component.CapturedUnixMS)
			writeCounter(&output, counterPrevious, next, measurementPrefix+"queue_admitted"+tags, queue.Admitted, availability, component.CapturedUnixMS)
			writeCounter(&output, counterPrevious, next, measurementPrefix+"queue_rejected"+tags, queue.Rejected, availability, component.CapturedUnixMS)
		}
		operationCounts := make(map[string]uint64)
		operationOldest := make(map[string]uint64)
		for _, operation := range component.Operations {
			operationCounts[operation.Class]++
			age := uint64(0)
			if component.CapturedUnixMS > operation.StartedUnixMS {
				age = saturatingMultiply(uint64(component.CapturedUnixMS-operation.StartedUnixMS), 1000)
			}
			if age > operationOldest[operation.Class] {
				operationOldest[operation.Class] = age
			}
		}
		classes := make([]string, 0, len(monitorValues("operation")))
		for class := range monitorValues("operation") {
			classes = append(classes, class)
		}
		sort.Strings(classes)
		for _, class := range classes {
			count := operationCounts[class]
			fmt.Fprintf(&output, measurementPrefix+"active_operations%s,operation=%s count=%su,oldest_age_us=%su,available=%v,availability=\"%s\" %d\n", baseTags, escapeTag(class), u64(count), u64(operationOldest[class]), component.Availability == AvailabilityExact, component.Availability, component.CapturedUnixMS)
		}
		for _, overflow := range component.OperationOverflow {
			writeCounter(&output, counterPrevious, next, measurementPrefix+"operation_overflow"+baseTags+",operation="+escapeTag(overflow.Class), overflow.Count, component.Availability, component.CapturedUnixMS)
		}
		for _, storage := range component.Storage {
			tags := baseTags + ",backend=" + escapeTag(storage.BackendID) + ",role=" + escapeTag(storage.Role)
			if storage.Acknowledgement != "" {
				tags += ",acknowledgement=" + escapeTag(storage.Acknowledgement)
			}
			if storage.ObjectClass != "" {
				tags += ",object_class=" + escapeTag(storage.ObjectClass)
			}
			if storage.PlacementState != "" {
				tags += ",placement_state=" + escapeTag(storage.PlacementState)
			}
			if storage.Representation != "" {
				tags += ",representation=" + escapeTag(storage.Representation)
			}
			availability := inheritedAvailability(component.Availability, storage.Availability)
			objectAvailability := inheritedAvailability(availability, storage.ObjectCountAvailability)
			payloadAvailability := inheritedAvailability(availability, storage.PayloadAvailability)
			physicalAvailability := inheritedAvailability(availability, storage.PhysicalAvailability)
			reconciliationAvailability := inheritedAvailability(availability, storage.ReconciliationAvailability)
			fmt.Fprintf(&output, measurementPrefix+"storage%s objects=%su,objects_available=%v,objects_availability=\"%s\",payload_bytes=%su,payload_available=%v,payload_availability=\"%s\",physical_bytes=%su,physical_available=%v,physical_availability=\"%s\",reconciled_at_ms=%di,reconciliation_available=%v,reconciliation_availability=\"%s\",available=%v,availability=\"%s\" %d\n", tags, u64(storage.ObjectCount), objectAvailability == AvailabilityExact, objectAvailability, u64(storage.PayloadBytes), payloadAvailability == AvailabilityExact, payloadAvailability, u64(storage.PhysicalBytes), physicalAvailability == AvailabilityExact, physicalAvailability, storage.ReconciledAtMS, reconciliationAvailability == AvailabilityExact, reconciliationAvailability, availability == AvailabilityExact, availability, component.CapturedUnixMS)
		}
		for _, cache := range component.Caches {
			tags := baseTags + ",cache=" + escapeTag(cache.ID)
			if cache.Family != "" {
				tags += ",family=" + escapeTag(cache.Family)
			}
			if cache.Representation != "" {
				tags += ",representation=" + escapeTag(cache.Representation)
			}
			availability := inheritedAvailability(component.Availability, cache.Availability)
			trafficAvailability := inheritedAvailability(component.Availability, optionalAvailability(cache.TrafficAvailability))
			trafficBytesAvailability := inheritedAvailability(component.Availability, optionalAvailability(cache.TrafficBytesAvailability))
			fillAvailability := inheritedAvailability(component.Availability, optionalAvailability(cache.FillAvailability))
			inventoryAvailability := inheritedAvailability(component.Availability, optionalAvailability(cache.InventoryAvailability))
			deletionAvailability := inheritedAvailability(component.Availability, optionalAvailability(cache.DeletionAvailability))
			reconciliationAgeAvailability := inheritedAvailability(component.Availability, optionalAvailability(cache.ReconciliationAgeAvailability))
			reconciliationLagAvailability := inheritedAvailability(component.Availability, optionalAvailability(cache.ReconciliationLagAvailability))
			fmt.Fprintf(&output, measurementPrefix+"cache%s requested_bytes=%su,effective_bytes=%su,used_bytes=%su,reserved_bytes=%su,pinned_bytes=%su,staging_bytes=%su,deletion_pending_bytes=%su,deletion_pending_available=%v,deletion_pending_availability=\"%s\",reclaim_pending_bytes=%su,available_bytes=%su,object_count=%su,object_count_available=%v,object_count_availability=\"%s\",enabled=%v,inflight_fills=%su,inflight_fills_available=%v,inflight_fills_availability=\"%s\",reconciliation_age_ms=%su,reconciliation_age_available=%v,reconciliation_age_availability=\"%s\",reconciliation_lag=%su,reconciliation_lag_available=%v,reconciliation_lag_availability=\"%s\",controller_state=\"%s\",circuit_state=\"%s\",available=%v,availability=\"%s\" %d\n", tags, u64(cache.RequestedBytes), u64(cache.EffectiveBytes), u64(cache.UsedBytes), u64(cache.ReservedBytes), u64(cache.PinnedBytes), u64(cache.StagingBytes), u64(cache.DeletionPendingBytes), deletionAvailability == AvailabilityExact, deletionAvailability, u64(cache.ReclaimPendingBytes), u64(cache.AvailableBytes), u64(cache.ObjectCount), inventoryAvailability == AvailabilityExact, inventoryAvailability, cache.Enabled, u64(cache.InflightFills), fillAvailability == AvailabilityExact, fillAvailability, u64(cache.ReconciliationAgeMS), reconciliationAgeAvailability == AvailabilityExact, reconciliationAgeAvailability, u64(cache.ReconciliationLag), reconciliationLagAvailability == AvailabilityExact, reconciliationLagAvailability, cache.ControllerState, cache.CircuitState, availability == AvailabilityExact, availability, component.CapturedUnixMS)
			for _, counter := range []struct {
				name  string
				value uint64
			}{{"hits", cache.Hits}, {"misses", cache.Misses}, {"origin_reads_avoided", cache.OriginReadsAvoided}} {
				writeCounter(&output, counterPrevious, next, measurementPrefix+"cache_"+counter.name+tags, counter.value, trafficAvailability, component.CapturedUnixMS)
			}
			writeCounter(&output, counterPrevious, next, measurementPrefix+"cache_origin_bytes"+tags, cache.OriginBytes, trafficBytesAvailability, component.CapturedUnixMS)
			writeCounter(&output, counterPrevious, next, measurementPrefix+"cache_cache_bytes"+tags, cache.CacheBytes, trafficBytesAvailability, component.CapturedUnixMS)
		}
		if wal := component.WAL; wal != nil {
			availability := inheritedAvailability(component.Availability, wal.Availability)
			tags := baseTags
			if wal.Target != "" {
				tags += ",target=" + escapeTag(wal.Target)
			}
			if wal.Durability != "" {
				tags += ",durability=" + escapeTag(wal.Durability)
			}
			fmt.Fprintf(&output, measurementPrefix+"wal%s outstanding_flushes=%su,retained_bytes=%su,retained_segments=%su,oldest_uncheckpointed_ms=%su,throttle=\"%s\",available=%v,availability=\"%s\" %d\n", tags, u64(wal.OutstandingFlushes), u64(wal.RetainedBytes), u64(wal.RetainedSegments), u64(wal.OldestUncheckpointedMS), wal.ThrottleReason, availability == AvailabilityExact, availability, component.CapturedUnixMS)
			writeCounter(&output, counterPrevious, next, measurementPrefix+"wal_uploaded_bytes"+tags, wal.UploadedBytes, availability, component.CapturedUnixMS)
			writeCounter(&output, counterPrevious, next, measurementPrefix+"wal_durability_failures"+tags, wal.DurabilityFailures, availability, component.CapturedUnixMS)
			writeCounter(&output, counterPrevious, next, measurementPrefix+"wal_cleanup_failures"+tags, wal.CleanupFailures, availability, component.CapturedUnixMS)
		}
	}
	return output.String(), next, nextProcesses
}

func escapeFieldString(value string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value)
}

func optionalAvailability(value Availability) Availability {
	if value == "" {
		return AvailabilityUnavailable
	}
	return value
}

func inheritedAvailability(parent, child Availability) Availability {
	if child == AvailabilityNotApplicable {
		return child
	}
	severity := map[Availability]int{
		AvailabilityExact: 0, AvailabilityEstimated: 1, AvailabilityStale: 2, AvailabilityUnavailable: 3,
	}
	if severity[parent] > severity[child] {
		return parent
	}
	return child
}

func writeCounter(output *bytes.Buffer, previous, next map[string]uint64, series string, value uint64, availability Availability, timestamp int64) {
	old, found := previous[series]
	if availability != AvailabilityExact {
		if found {
			next[series] = old
		}
		fmt.Fprintf(output, "%s value=%su,delta=0u,reset=false,available=false,availability=\"%s\" %d\n", series, u64(value), availability, timestamp)
		return
	}
	reset := !found || value < old
	delta := uint64(0)
	if found && !reset {
		delta = value - old
	}
	next[series] = value
	fmt.Fprintf(output, "%s value=%su,delta=%su,reset=%v,available=true,availability=\"%s\" %d\n", series, u64(value), u64(delta), reset, availability, timestamp)
}

func u64(value uint64) string { return strconv.FormatUint(value, 10) }

func saturatingMultiply(value, multiplier uint64) uint64 {
	if multiplier != 0 && value > ^uint64(0)/multiplier {
		return ^uint64(0)
	}
	return value * multiplier
}
