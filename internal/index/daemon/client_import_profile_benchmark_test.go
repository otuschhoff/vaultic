package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
