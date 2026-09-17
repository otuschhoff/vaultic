package daemon

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

func BenchmarkProcessDurableCommitWALLatency(b *testing.B) {
	binaryPath := os.Getenv("VAULTICDB_FAILURE_TEST_BINARY")
	if binaryPath == "" {
		b.Skip("set VAULTICDB_FAILURE_TEST_BINARY to a test-failpoints-enabled daemon")
	}
	for _, delay := range []time.Duration{0, 10 * time.Millisecond, 50 * time.Millisecond, 200 * time.Millisecond} {
		b.Run(fmt.Sprintf("wal-put-delay=%s", delay), func(b *testing.B) {
			directory := b.TempDir()
			if err := os.Chmod(directory, 0o700); err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			profile := fmt.Sprintf(
				`{"version":1,"target":"isolated","role":"wal","operation":"put","delay_ms":%d}`,
				delay.Milliseconds(),
			)
			client, err := Ensure(ctx, Options{
				Socket: filepath.Join(directory, "daemon.sock"), RepositoryID: "phase32-p3b-" + delay.String(),
				DaemonPath: binaryPath, DataDir: filepath.Join(directory, "db"), ObjectStore: "local",
				WALStore: "local", WALDataDir: filepath.Join(directory, "wal"), WALFlushInterval: time.Millisecond,
				testEnvironment: []string{"VAULTICDB_TEST_OBJECT_DELAY_PROFILE=" + profile},
			})
			if err != nil {
				b.Fatal(err)
			}
			defer client.Close(ctx)
			before, err := client.WriterStatus(ctx)
			if err != nil {
				b.Fatal(err)
			}
			durations := make([]time.Duration, 0, b.N)
			b.ResetTimer()
			for iteration := range b.N {
				b.StopTimer()
				transaction, beginErr := client.Begin(ctx)
				if beginErr != nil {
					b.Fatal(beginErr)
				}
				key := fmt.Appendf(nil, "profiled/%08d", iteration)
				if writeErr := transaction.WriteBatch(ctx, []Mutation{{Key: key, Value: []byte("value")}}, nil); writeErr != nil {
					b.Fatal(writeErr)
				}
				started := time.Now()
				b.StartTimer()
				commitErr := transaction.Commit(ctx)
				b.StopTimer()
				durations = append(durations, time.Since(started))
				if commitErr != nil {
					b.Fatal(commitErr)
				}
			}
			after, err := client.WriterStatus(ctx)
			if err != nil {
				b.Fatal(err)
			}
			walBefore := before.Attribution.ObjectStoreWAL.Put.Timing
			walAfter := after.Attribution.ObjectStoreWAL.Put.Timing
			durableBefore := before.Attribution.DurableWait
			durableAfter := after.Attribution.DurableWait
			walAttempts := walAfter.Attempts - walBefore.Attempts
			durableAttempts := durableAfter.Attempts - durableBefore.Attempts
			if walAttempts == 0 || durableAttempts == 0 {
				b.Fatalf("durable workload did not reach WAL PUT and wait: before=%+v after=%+v", before.Attribution, after.Attribution)
			}
			sort.Slice(durations, func(left, right int) bool { return durations[left] < durations[right] })
			b.ReportMetric(float64(durations[percentileIndex(len(durations), 95)].Microseconds())/1_000, "commit-p95-ms")
			b.ReportMetric(float64(durations[percentileIndex(len(durations), 99)].Microseconds())/1_000, "commit-p99-ms")
			b.ReportMetric(float64(walAfter.TotalUS-walBefore.TotalUS)/float64(b.N), "wal-put-us/op")
			b.ReportMetric(float64(durableAfter.TotalUS-durableBefore.TotalUS)/float64(b.N), "durable-wait-us/op")
			b.ReportMetric(float64(walAttempts)/float64(b.N), "wal-puts/op")
			b.ReportMetric(float64(durableAttempts)/float64(b.N), "durable-waits/op")
			b.ReportMetric(float64(after.Attribution.EngineBatchQueue.TotalUS-before.Attribution.EngineBatchQueue.TotalUS)/float64(b.N), "queue-us/op")
			b.ReportMetric(float64(after.Attribution.EngineBatchService.TotalUS-before.Attribution.EngineBatchService.TotalUS)/float64(b.N), "service-us/op")
		})
	}
}

func BenchmarkProcessDurableCommitRADOSWAL(b *testing.B) {
	binaryPath := os.Getenv("VAULTICDB_BENCH_RADOS_BINARY")
	if binaryPath == "" {
		b.Skip("set VAULTICDB_BENCH_RADOS_BINARY to a RADOS-enabled daemon")
	}
	required := func(name string) string {
		b.Helper()
		value := os.Getenv(name)
		if value == "" {
			b.Fatalf("%s is required for the live RADOS WAL benchmark", name)
		}
		return value
	}
	clientID := required("VAULTICDB_BENCH_RADOS_CLIENT")
	key := benchmarkCephXKey(b, required("VAULTICDB_BENCH_RADOS_KEY_FILE"), clientID)
	directory := b.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: filepath.Join(directory, "daemon.sock"), RepositoryID: fmt.Sprintf("phase32-rados-wal-%d", time.Now().UnixNano()),
		DaemonPath: binaryPath, DataDir: filepath.Join(directory, "db"), ObjectStore: "local",
		WALStore: "rados", WALFlushInterval: time.Millisecond,
		WALRadosMonitors:  required("VAULTICDB_BENCH_RADOS_MONITORS"),
		WALRadosFSID:      required("VAULTICDB_BENCH_RADOS_FSID"),
		WALRadosPool:      required("VAULTICDB_BENCH_RADOS_POOL"),
		WALRadosNamespace: required("VAULTICDB_BENCH_RADOS_NAMESPACE"),
		WALRadosPrefix:    required("VAULTICDB_BENCH_RADOS_PREFIX"),
		WALRadosClient:    clientID,
		WALRadosKey:       key,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if closeErr := client.Close(ctx); closeErr != nil {
			b.Errorf("close RADOS benchmark daemon: %v", closeErr)
		}
	}()
	before, err := client.WriterStatus(ctx)
	if err != nil {
		b.Fatal(err)
	}
	durations := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for iteration := range b.N {
		b.StopTimer()
		transaction, beginErr := client.Begin(ctx)
		if beginErr != nil {
			b.Fatal(beginErr)
		}
		key := fmt.Appendf(nil, "rados/%08d", iteration)
		if writeErr := transaction.WriteBatch(ctx, []Mutation{{Key: key, Value: []byte("value")}}, nil); writeErr != nil {
			b.Fatal(writeErr)
		}
		started := time.Now()
		b.StartTimer()
		commitErr := transaction.Commit(ctx)
		b.StopTimer()
		durations = append(durations, time.Since(started))
		if commitErr != nil {
			b.Fatal(commitErr)
		}
	}
	after, err := client.WriterStatus(ctx)
	if err != nil {
		b.Fatal(err)
	}
	walBefore, walAfter := before.Attribution.ObjectStoreWAL.Put.Timing, after.Attribution.ObjectStoreWAL.Put.Timing
	durableBefore, durableAfter := before.Attribution.DurableWait, after.Attribution.DurableWait
	walAttempts := walAfter.Attempts - walBefore.Attempts
	durableAttempts := durableAfter.Attempts - durableBefore.Attempts
	if walAttempts == 0 || durableAttempts == 0 {
		b.Fatalf("durable workload did not reach RADOS WAL PUT and wait: before=%+v after=%+v", before.Attribution, after.Attribution)
	}
	sort.Slice(durations, func(left, right int) bool { return durations[left] < durations[right] })
	b.ReportMetric(float64(durations[percentileIndex(len(durations), 95)].Microseconds())/1_000, "commit-p95-ms")
	b.ReportMetric(float64(durations[percentileIndex(len(durations), 99)].Microseconds())/1_000, "commit-p99-ms")
	b.ReportMetric(float64(walAfter.TotalUS-walBefore.TotalUS)/float64(b.N), "wal-put-us/op")
	b.ReportMetric(float64(durableAfter.TotalUS-durableBefore.TotalUS)/float64(b.N), "durable-wait-us/op")
	b.ReportMetric(float64(walAttempts)/float64(b.N), "wal-puts/op")
	b.ReportMetric(float64(durableAttempts)/float64(b.N), "durable-waits/op")
	b.ReportMetric(float64(after.Attribution.EngineBatchQueue.TotalUS-before.Attribution.EngineBatchQueue.TotalUS)/float64(b.N), "queue-us/op")
	b.ReportMetric(float64(after.Attribution.EngineBatchService.TotalUS-before.Attribution.EngineBatchService.TotalUS)/float64(b.N), "service-us/op")
}

func benchmarkCephXKey(b *testing.B, path, client string) string {
	b.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		b.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		b.Fatal("RADOS key file must be a non-symlink regular file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		b.Fatalf("RADOS key file %s must not be accessible by group or others", path)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	value := strings.TrimSpace(string(encoded))
	if value == "" {
		b.Fatal("RADOS key file is empty")
	}
	keyring := false
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		keyring = strings.HasPrefix(line, "[")
		break
	}
	if !keyring {
		if strings.ContainsAny(value, "\r\n") {
			b.Fatal("RADOS raw key must be one line")
		}
		if decoded, decodeErr := base64.StdEncoding.DecodeString(value); decodeErr != nil || len(decoded) == 0 {
			b.Fatal("RADOS raw key is not valid Base64")
		}
		return value
	}
	section := ""
	key := ""
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, "["), "]"))
			continue
		}
		name, candidate, found := strings.Cut(line, "=")
		if section == client && found && strings.TrimSpace(name) == "key" {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" || key != "" {
				b.Fatalf("RADOS keyring section %q must contain exactly one non-empty key", client)
			}
			key = candidate
		}
	}
	if key != "" {
		if decoded, decodeErr := base64.StdEncoding.DecodeString(key); decodeErr != nil || len(decoded) == 0 {
			b.Fatalf("RADOS keyring section %q contains an invalid Base64 key", client)
		}
		return key
	}
	b.Fatalf("RADOS keyring has no key for section %q", client)
	return ""
}

func BenchmarkProcessDurableCommitResponseLatency(b *testing.B) {
	binaryPath := os.Getenv("VAULTICDB_FAILURE_TEST_BINARY")
	if binaryPath == "" {
		b.Skip("set VAULTICDB_FAILURE_TEST_BINARY to the benchmark daemon")
	}
	for _, delay := range []time.Duration{
		0,
		time.Millisecond,
		8 * time.Millisecond,
		25 * time.Millisecond,
		100 * time.Millisecond,
		250 * time.Millisecond,
	} {
		b.Run(fmt.Sprintf("commit-response-delay=%s", delay), func(b *testing.B) {
			directory := b.TempDir()
			if err := os.Chmod(directory, 0o700); err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			client, err := Ensure(ctx, Options{
				Socket: filepath.Join(directory, "daemon.sock"), RepositoryID: "phase32-p3b-response-" + delay.String(),
				DaemonPath: binaryPath, DataDir: filepath.Join(directory, "db"), ObjectStore: "local",
				WALStore: "local", WALDataDir: filepath.Join(directory, "wal"), WALFlushInterval: time.Millisecond,
				commitResponseDelayForTesting: delay,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer client.Close(ctx)
			before, err := client.WriterStatus(ctx)
			if err != nil {
				b.Fatal(err)
			}
			durations := make([]time.Duration, 0, b.N)
			b.ResetTimer()
			for iteration := range b.N {
				b.StopTimer()
				transaction, beginErr := client.Begin(ctx)
				if beginErr != nil {
					b.Fatal(beginErr)
				}
				key := fmt.Appendf(nil, "response-profiled/%08d", iteration)
				if writeErr := transaction.WriteBatch(ctx, []Mutation{{Key: key, Value: []byte("value")}}, nil); writeErr != nil {
					b.Fatal(writeErr)
				}
				started := time.Now()
				b.StartTimer()
				commitErr := transaction.Commit(ctx)
				b.StopTimer()
				durations = append(durations, time.Since(started))
				if commitErr != nil {
					b.Fatal(commitErr)
				}
			}
			after, err := client.WriterStatus(ctx)
			if err != nil {
				b.Fatal(err)
			}
			sort.Slice(durations, func(left, right int) bool { return durations[left] < durations[right] })
			b.ReportMetric(float64(durations[percentileIndex(len(durations), 95)].Microseconds())/1_000, "client-p95-ms")
			b.ReportMetric(float64(durations[percentileIndex(len(durations), 99)].Microseconds())/1_000, "client-p99-ms")
			b.ReportMetric(float64(after.Attribution.CommitRequest.TotalUS-before.Attribution.CommitRequest.TotalUS)/float64(b.N), "server-commit-us/op")
			b.ReportMetric(float64(after.Attribution.DurableWait.TotalUS-before.Attribution.DurableWait.TotalUS)/float64(b.N), "durable-wait-us/op")
		})
	}
}

func percentileIndex(length, percentile int) int {
	index := (length*percentile + 99) / 100
	if index == 0 {
		return 0
	}
	return index - 1
}
