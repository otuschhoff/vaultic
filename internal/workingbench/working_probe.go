package workingbench

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/shirou/gopsutil/v4/process"
)

type WorkingProbeOS struct {
	RSSBytes          uint64   `json:"rss_bytes"`
	SwapBytes         uint64   `json:"swap_bytes"`
	MappedBytes       uint64   `json:"mapped_bytes"`
	MappedRSSBytes    uint64   `json:"mapped_rss_bytes"`
	ScratchBytes      uint64   `json:"scratch_bytes"`
	ReadBytes         uint64   `json:"logical_read_bytes"`
	WriteBytes        uint64   `json:"logical_write_bytes"`
	StorageReadBytes  uint64   `json:"storage_read_bytes"`
	StorageWriteBytes uint64   `json:"storage_write_bytes"`
	ReadCalls         uint64   `json:"read_calls"`
	WriteCalls        uint64   `json:"write_calls"`
	CPUSeconds        float64  `json:"cpu_seconds"`
	ChildRSSBytes     uint64   `json:"child_rss_bytes"`
	Unavailable       []string `json:"unavailable,omitempty"`
}

type WorkingProbeResult struct {
	WallSeconds            float64                          `json:"wall_seconds"`
	Before                 telemetry.WorkingRuntimeSnapshot `json:"before"`
	After                  telemetry.WorkingRuntimeSnapshot `json:"after"`
	RuntimeDelta           telemetry.WorkingRuntimeInterval `json:"runtime_delta"`
	OSBefore               WorkingProbeOS                   `json:"os_before"`
	OSAfter                WorkingProbeOS                   `json:"os_after"`
	Peak                   WorkingProbeOS                   `json:"sampled_os_peak"`
	PeakHeapBytes          uint64                           `json:"sampled_heap_peak_bytes"`
	PeakRuntimeBytes       uint64                           `json:"sampled_runtime_peak_bytes"`
	PeakScanBytes          uint64                           `json:"sampled_scan_peak_bytes"`
	PeakObjects            uint64                           `json:"sampled_objects_peak"`
	Samples                uint64                           `json:"samples"`
	LatencyCount           int                              `json:"latency_count"`
	LatencyP50Seconds      float64                          `json:"latency_p50_seconds"`
	LatencyP95Seconds      float64                          `json:"latency_p95_seconds"`
	LatencyP99Seconds      float64                          `json:"latency_p99_seconds"`
	LatencyP99LowerSeconds float64                          `json:"latency_p99_lower_seconds"`
	LatencyHistogram       bool                             `json:"latency_histogram"`
	LatencyMaxSeconds      float64                          `json:"latency_max_seconds"`
	LookupLatency          WorkingProbeLatency              `json:"lookup_latency"`
	SemanticSHA256         string                           `json:"semantic_sha256"`
	State                  any                              `json:"working_state,omitempty"`
}

type WorkingProbeLatency struct {
	Count           int     `json:"count"`
	P50Seconds      float64 `json:"p50_seconds"`
	P95Seconds      float64 `json:"p95_seconds"`
	P99Seconds      float64 `json:"p99_seconds"`
	P99LowerSeconds float64 `json:"p99_lower_seconds"`
	MaxSeconds      float64 `json:"max_seconds"`
}

type WorkingProbe struct {
	root, output string
	process      *process.Process
	started      time.Time
	result       WorkingProbeResult
	cpu          *os.File
	stop, done   chan struct{}
	mutex        sync.Mutex
	latencies    []time.Duration
	lookups      []time.Duration
	histogram    *hdr.Histogram
	finishOnce   sync.Once
	finished     WorkingProbeResult
	finishErr    error
	closed       bool
}

func StartWorkingProbe(root, output string) (*WorkingProbe, error) {
	current, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(output, 0700); err != nil {
		return nil, err
	}
	cpu, err := os.Create(filepath.Join(output, "cpu.pprof"))
	if err != nil {
		return nil, err
	}
	if err := pprof.StartCPUProfile(cpu); err != nil {
		return nil, errors.Join(err, cpu.Close())
	}
	probe := &WorkingProbe{root: root, output: output, process: current, cpu: cpu,
		stop: make(chan struct{}), done: make(chan struct{}), latencies: make([]time.Duration, 0, 8192),
		lookups: make([]time.Duration, 0, 8192)}
	probe.started = time.Now()
	probe.result.Before = telemetry.ReadWorkingRuntime()
	probe.result.OSBefore = probe.readOS()
	probe.observe()
	go func() {
		defer close(probe.done)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-probe.stop:
				return
			case <-ticker.C:
				probe.observe()
			}
		}
	}()
	return probe, nil
}

func (probe *WorkingProbe) readOS() WorkingProbeOS {
	var result WorkingProbeOS
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if info, err := probe.process.MemoryInfoWithContext(ctx); err == nil {
		result.RSSBytes, result.SwapBytes = info.RSS, info.Swap
	} else {
		result.Unavailable = append(result.Unavailable, "process_memory")
	}
	if times, err := probe.process.TimesWithContext(ctx); err == nil {
		result.CPUSeconds = times.User + times.System
	} else {
		result.Unavailable = append(result.Unavailable, "process_cpu")
	}
	if counters, err := probe.process.IOCountersWithContext(ctx); err == nil {
		result.ReadBytes, result.WriteBytes = counters.ReadBytes, counters.WriteBytes
		result.ReadCalls, result.WriteCalls = counters.ReadCount, counters.WriteCount
	} else {
		result.Unavailable = append(result.Unavailable, "process_io")
	}
	if runtime.GOOS == "linux" {
		file, err := os.Open("/proc/self/io")
		if err != nil {
			result.Unavailable = append(result.Unavailable, "storage_io")
		} else {
			scanner := bufio.NewScanner(file)
			found := 0
			for scanner.Scan() {
				name, text, ok := strings.Cut(scanner.Text(), ":")
				if !ok || (name != "read_bytes" && name != "write_bytes") {
					continue
				}
				value, err := strconv.ParseUint(strings.TrimSpace(text), 10, 64)
				if err != nil {
					continue
				}
				found++
				if name == "read_bytes" {
					result.StorageReadBytes = value
				} else {
					result.StorageWriteBytes = value
				}
			}
			if scanner.Err() != nil || found != 2 {
				result.Unavailable = append(result.Unavailable, "storage_io")
			}
			_ = file.Close()
		}
	} else {
		result.Unavailable = append(result.Unavailable, "storage_io")
	}
	if runtime.GOOS != "linux" {
		result.Unavailable = append(result.Unavailable, "mapping_residency")
	} else if ownedMapping, err := workingProbeHasOwnedMapping(probe.root); err != nil {
		result.Unavailable = append(result.Unavailable, "mapping_residency")
	} else if ownedMapping {
		if mappings, err := probe.process.MemoryMapsWithContext(ctx, false); err == nil && mappings != nil {
			encoded, encodeErr := json.Marshal(mappings)
			var owned []struct {
				Path string `json:"path"`
				Size uint64 `json:"size"`
				RSS  uint64 `json:"rss"`
			}
			if encodeErr != nil || json.Unmarshal(encoded, &owned) != nil {
				result.Unavailable = append(result.Unavailable, "mapping_residency")
			} else {
				for _, mapping := range owned {
					if withinWorkingProbe(probe.root, mapping.Path) {
						result.MappedBytes += mapping.Size * 1024
						result.MappedRSSBytes += mapping.RSS * 1024
					}
				}
			}
		} else {
			result.Unavailable = append(result.Unavailable, "mapping_residency")
		}
	}
	if children, err := probe.process.ChildrenWithContext(ctx); err == nil {
		for _, child := range children {
			if info, err := child.MemoryInfoWithContext(ctx); err == nil {
				result.ChildRSSBytes += info.RSS
			} else {
				result.Unavailable = append(result.Unavailable, "child_memory")
			}
		}
	} else if !errors.Is(err, process.ErrorNoChildren) {
		result.Unavailable = append(result.Unavailable, "child_memory")
	}
	var err error
	result.ScratchBytes, err = workingProbeScratchSize(probe.root)
	if err != nil {
		result.Unavailable = append(result.Unavailable, "scratch_inventory")
	}
	return result
}

func workingProbeHasOwnedMapping(root string) (bool, error) {
	file, err := os.Open("/proc/self/maps")
	if err != nil {
		return false, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 6 && withinWorkingProbe(root, strings.Join(fields[5:], " ")) {
			return true, nil
		}
	}
	return false, scanner.Err()
}

func workingProbeScratchSize(root string) (uint64, error) {
	var size uint64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if path != root && errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				return err
			}
			size += uint64(info.Size())
		}
		return nil
	})
	return size, err
}

func withinWorkingProbe(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && filepath.IsLocal(relative)
}

func (probe *WorkingProbe) observe() {
	current := telemetry.ReadWorkingRuntime()
	observed := probe.readOS()
	probe.mutex.Lock()
	defer probe.mutex.Unlock()
	probe.result.Samples++
	probe.result.PeakHeapBytes = max(probe.result.PeakHeapBytes, current.HeapBytes)
	probe.result.PeakRuntimeBytes = max(probe.result.PeakRuntimeBytes, current.RuntimeBytes)
	probe.result.PeakScanBytes = max(probe.result.PeakScanBytes, current.ScannableHeapBytes)
	probe.result.PeakObjects = max(probe.result.PeakObjects, current.HeapObjects)
	probe.result.Peak.RSSBytes = max(probe.result.Peak.RSSBytes, observed.RSSBytes)
	probe.result.Peak.SwapBytes = max(probe.result.Peak.SwapBytes, observed.SwapBytes)
	probe.result.Peak.MappedBytes = max(probe.result.Peak.MappedBytes, observed.MappedBytes)
	probe.result.Peak.MappedRSSBytes = max(probe.result.Peak.MappedRSSBytes, observed.MappedRSSBytes)
	probe.result.Peak.ScratchBytes = max(probe.result.Peak.ScratchBytes, observed.ScratchBytes)
	probe.result.Peak.ChildRSSBytes = max(probe.result.Peak.ChildRSSBytes, observed.ChildRSSBytes)
	for _, names := range [][]string{observed.Unavailable, current.Unavailable} {
		for _, name := range names {
			if !slices.Contains(probe.result.Peak.Unavailable, name) {
				probe.result.Peak.Unavailable = append(probe.result.Peak.Unavailable, name)
			}
		}
	}
}

func (probe *WorkingProbe) Latency(duration time.Duration) error {
	return probe.recordLatency(&probe.latencies, duration)
}

func (probe *WorkingProbe) LookupLatency(duration time.Duration) error {
	return probe.recordLatency(&probe.lookups, duration)
}

func (probe *WorkingProbe) EnableLatencyHistogram() error {
	probe.mutex.Lock()
	defer probe.mutex.Unlock()
	if probe.closed || len(probe.latencies) != 0 || probe.histogram != nil {
		return errors.New("histogram must be enabled before latency observations")
	}
	probe.histogram = hdr.New(1, int64(24*time.Hour), 3)
	return nil
}

func (probe *WorkingProbe) recordLatency(samples *[]time.Duration, duration time.Duration) error {
	probe.mutex.Lock()
	defer probe.mutex.Unlock()
	if samples == &probe.latencies && probe.histogram != nil {
		if probe.closed || duration < 0 || duration > 24*time.Hour {
			return errors.New("invalid histogram latency observation")
		}
		return probe.histogram.RecordValue(int64(duration))
	}
	if probe.closed || duration < 0 || len(*samples) == cap(*samples) {
		return errors.New("invalid or excess benchmark latency observations")
	}
	*samples = append(*samples, duration)
	return nil
}

func workingProbeLatency(samples []time.Duration) WorkingProbeLatency {
	result := WorkingProbeLatency{Count: len(samples)}
	if len(samples) == 0 {
		return result
	}
	sort.Slice(samples, func(left, right int) bool { return samples[left] < samples[right] })
	percentile := func(percent int) float64 {
		index := (len(samples)*percent+99)/100 - 1
		return samples[index].Seconds()
	}
	result.P50Seconds, result.P95Seconds = percentile(50), percentile(95)
	result.P99Seconds, result.MaxSeconds = percentile(99), percentile(100)
	result.P99LowerSeconds = result.P99Seconds
	return result
}

func workingProbeHistogramLatency(histogram *hdr.Histogram) WorkingProbeLatency {
	total := histogram.TotalCount()
	result := WorkingProbeLatency{Count: int(total)}
	bars := histogram.Distribution()
	for _, quantile := range []struct {
		percent int64
		upper   *float64
	}{{50, &result.P50Seconds}, {95, &result.P95Seconds}, {99, &result.P99Seconds}, {100, &result.MaxSeconds}} {
		target := total - total/100*(100-quantile.percent) - total%100*(100-quantile.percent)/100
		var seen int64
		for _, bar := range bars {
			seen += bar.Count
			if target == 0 || seen < target {
				continue
			}
			*quantile.upper = time.Duration(bar.To).Seconds()
			if quantile.percent == 99 {
				result.P99LowerSeconds = time.Duration(bar.From).Seconds()
			}
			break
		}
	}
	return result
}

func (probe *WorkingProbe) Finish(semanticSHA256 string, state any) (WorkingProbeResult, error) {
	probe.finishOnce.Do(func() {
		probe.finished, probe.finishErr = probe.finish(semanticSHA256, state)
	})
	return probe.finished, probe.finishErr
}

func (probe *WorkingProbe) finish(semanticSHA256 string, state any) (WorkingProbeResult, error) {
	close(probe.stop)
	<-probe.done
	probe.observe()
	probe.mutex.Lock()
	defer probe.mutex.Unlock()
	probe.closed = true
	probe.result.OSAfter = probe.readOS()
	probe.result.After = telemetry.ReadWorkingRuntime()
	probe.result.WallSeconds = time.Since(probe.started).Seconds()
	probe.result.RuntimeDelta = telemetry.WorkingRuntimeDifference(probe.result.Before, probe.result.After)
	probe.result.SemanticSHA256, probe.result.State = semanticSHA256, state
	latency := workingProbeLatency(probe.latencies)
	if probe.histogram != nil {
		latency = workingProbeHistogramLatency(probe.histogram)
		probe.result.LatencyHistogram = true
	}
	probe.result.LatencyCount, probe.result.LatencyP50Seconds = latency.Count, latency.P50Seconds
	probe.result.LatencyP95Seconds, probe.result.LatencyP99Seconds = latency.P95Seconds, latency.P99Seconds
	probe.result.LatencyMaxSeconds = latency.MaxSeconds
	probe.result.LatencyP99LowerSeconds = latency.P99LowerSeconds
	probe.result.LookupLatency = workingProbeLatency(probe.lookups)
	pprof.StopCPUProfile()
	err := probe.cpu.Close()
	heap, openErr := os.Create(filepath.Join(probe.output, "heap.pprof"))
	if openErr == nil {
		err = errors.Join(err, pprof.WriteHeapProfile(heap), heap.Close())
	} else {
		err = errors.Join(err, openErr)
	}
	encoded, encodeErr := json.MarshalIndent(probe.result, "", "  ")
	if encodeErr == nil {
		err = errors.Join(err, os.WriteFile(filepath.Join(probe.output, "measurement.json"), append(encoded, '\n'), 0600))
	}
	return probe.result, errors.Join(err, encodeErr)
}
