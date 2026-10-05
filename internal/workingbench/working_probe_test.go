package workingbench

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

func TestM5ProbeOwnershipAndBounds(t *testing.T) {
	root, output := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "owned"), make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	database, err := bbolt.Open(filepath.Join(root, "mapped.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.Update(func(transaction *bbolt.Tx) error {
		bucket, err := transaction.CreateBucket([]byte("fixture"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("key"), make([]byte, 1024))
	}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "mapped.db"))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "linux" {
		if owned, err := workingProbeHasOwnedMapping(root); err != nil || !owned {
			t.Fatal("owned mapping omitted by residency preflight", owned, err)
		}
		if owned, err := workingProbeHasOwnedMapping(output); err != nil || owned {
			t.Fatal("foreign mapping attributed to owned fixture", owned, err)
		}
	}
	probe, err := StartWorkingProbe(root, output)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = probe.Finish("fixture", nil) })
	for _, duration := range []time.Duration{time.Millisecond, 3 * time.Millisecond, 2 * time.Millisecond} {
		if err := probe.Latency(duration); err != nil {
			t.Fatal(err)
		}
	}
	if err := probe.Latency(-1); err == nil {
		t.Fatal("negative latency accepted")
	}
	for _, duration := range []time.Duration{4 * time.Millisecond, 6 * time.Millisecond, 5 * time.Millisecond} {
		if err := probe.LookupLatency(duration); err != nil {
			t.Fatal(err)
		}
	}
	if err := probe.LookupLatency(-1); err == nil {
		t.Fatal("negative lookup latency accepted")
	}
	result, err := probe.Finish("fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Peak.ScratchBytes != 4096+uint64(info.Size()) || result.Peak.RSSBytes == 0 || result.Samples == 0 ||
		result.LatencyCount != 3 || result.LatencyP50Seconds != .002 || result.LatencyP99Seconds != .003 {
		t.Fatalf("invalid probe: %+v", result)
	}
	if result.LookupLatency.Count != 3 || result.LookupLatency.P50Seconds != .005 || result.LookupLatency.P99Seconds != .006 {
		t.Fatal("lookup and write tails not independently measured", result.LookupLatency)
	}
	if runtime.GOOS == "linux" && (result.Peak.MappedBytes < uint64(info.Size()) || result.Peak.MappedRSSBytes == 0) {
		t.Fatal("mapped bytes/residency missing or reported in KiB", result.Peak)
	}
	if _, err := probe.Finish("ignored", nil); err != nil {
		t.Fatal(err)
	}
	if err := probe.Latency(time.Second); err == nil {
		t.Fatal("latency accepted after finish")
	}
	probe.result.Peak.Unavailable = []string{"process_io"}
	probe.root = filepath.Join(root, "missing")
	probe.observe()
	unavailableCount := len(probe.result.Peak.Unavailable)
	probe.observe()
	if !slices.Contains(probe.result.Peak.Unavailable, "process_io") ||
		!slices.Contains(probe.result.Peak.Unavailable, "scratch_inventory") || len(probe.result.Peak.Unavailable) != unavailableCount {
		t.Fatal("unavailable samples lost or retained without bounds", probe.result.Peak.Unavailable)
	}
	for _, filename := range []string{"measurement.json", "cpu.pprof", "heap.pprof"} {
		info, err := os.Stat(filepath.Join(output, filename))
		if err != nil || info.Size() == 0 {
			t.Fatal("missing measurement/profile", filename, err)
		}
	}
	encoded, err := json.Marshal(result)
	if err != nil || strings.Contains(string(encoded), root) || strings.Contains(string(encoded), output) {
		t.Fatal("invalid or sensitive measurement", err)
	}
	if withinWorkingProbe(root, root+"-foreign/file") || !withinWorkingProbe(root, filepath.Join(root, "owned")) {
		t.Fatal("mapping ownership boundary failed")
	}
	bounded := WorkingProbe{latencies: make([]time.Duration, 8192), lookups: make([]time.Duration, 8192)}
	if err := bounded.Latency(time.Second); err == nil {
		t.Fatal("unbounded latency retention")
	}
	if err := bounded.LookupLatency(time.Second); err == nil {
		t.Fatal("unbounded lookup latency retention")
	}
}

func TestWorkingScratchInventoryConcurrentRemoval(t *testing.T) {
	root := t.TempDir()
	stable := filepath.Join(root, "stable")
	if err := os.WriteFile(stable, make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	group.Go(func() {
		for range 2000 {
			path := filepath.Join(root, "compacting")
			if err := os.WriteFile(path, []byte("temporary"), 0600); err != nil {
				t.Error(err)
				return
			}
			if err := os.Remove(path); err != nil {
				t.Error(err)
				return
			}
		}
	})
	for range 500 {
		if size, err := workingProbeScratchSize(root); err != nil || size < 4096 {
			t.Fatal("transient removal invalidated sampled inventory", size, err)
		}
	}
	group.Wait()
	if size, err := workingProbeScratchSize(root); err != nil || size != 4096 {
		t.Fatal("stable inventory mismatch", size, err)
	}
	if _, err := workingProbeScratchSize(filepath.Join(root, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing root treated as valid inventory", err)
	}
}

func TestM5LargeLatencyHistogram(t *testing.T) {
	var probe WorkingProbe
	if err := probe.EnableLatencyHistogram(); err != nil {
		t.Fatal(err)
	}
	for ordinal := range 100000 {
		if err := probe.Latency(time.Duration(ordinal%1000) * time.Microsecond); err != nil {
			t.Fatal(err)
		}
	}
	result := workingProbeHistogramLatency(probe.histogram)
	if result.Count != 100000 || result.P99LowerSeconds > .000989 || result.P99Seconds < .000989 ||
		result.P99Seconds-result.P99LowerSeconds > .000002 || len(probe.latencies) != 0 {
		t.Fatal("unbounded or incorrect histogram quantiles", result)
	}
	for _, invalid := range []time.Duration{-1, 24*time.Hour + 1} {
		if err := probe.Latency(invalid); err == nil {
			t.Fatal("invalid histogram duration accepted", invalid)
		}
	}
	if probe.histogram.TotalCount() != 100000 || probe.EnableLatencyHistogram() == nil {
		t.Fatal("invalid observation or reconfiguration changed histogram")
	}
	probe.closed = true
	if err := probe.Latency(time.Second); err == nil {
		t.Fatal("histogram changed after finish")
	}
}
