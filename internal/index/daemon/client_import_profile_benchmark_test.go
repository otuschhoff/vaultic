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

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"google.golang.org/grpc"
)

type benchmarkBlobSizeRPC struct {
	vaulticdbv1.VaulticDBClient
	value []byte
}

func (rpc *benchmarkBlobSizeRPC) MultiGet(
	_ context.Context, request *vaulticdbv1.MultiGetRequest, _ ...grpc.CallOption,
) (*vaulticdbv1.MultiGetResponse, error) {
	results := make([]*vaulticdbv1.GetResponse, len(request.GetKeys()))
	for index, key := range request.GetKeys() {
		results[index] = &vaulticdbv1.GetResponse{Key: key, Value: rpc.value, Found: true}
	}
	return &vaulticdbv1.MultiGetResponse{Results: results}, nil
}

func BenchmarkBlobSizeLookupInProcess(b *testing.B) {
	packID, blobID := daemonTestID(40), daemonTestID(80)
	value, err := readSessionTestPack(packID, blobID).Blobs[blobID].MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	for _, count := range []int{1, 8} {
		b.Run(fmt.Sprintf("keys=%d", count), func(b *testing.B) {
			client := &Client{rpc: &benchmarkBlobSizeRPC{value: value}, limits: Limits{MaxBatchItems: 256, MaxMessageBytes: 1 << 20}}
			client.initializeSubclients()
			session := &ReadSession{SchemaStore: NewSchemaStore(client), transaction: &Transaction{client: client, id: "pinned"}, ctx: b.Context()}
			handles := make([]vaultic.BlobHandle, count)
			for index := range handles {
				handles[index] = vaultic.BlobHandle{ID: vaultic.ID(daemonTestID(byte(80 + index))), Type: vaultic.DataBlob}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				sizes, lookupErr := session.LookupBlobSizesContext(b.Context(), handles)
				if lookupErr != nil || len(sizes) != count {
					b.Fatalf("lookup sizes=%d error=%v", len(sizes), lookupErr)
				}
				for _, size := range sizes {
					if size != (vaultic.BlobSize{Size: 7, Found: true}) {
						b.Fatalf("unexpected blob size: %+v", size)
					}
				}
			}
		})
	}
}

func BenchmarkProcessBlobSizeMultiGetModes(b *testing.B) {
	benchmarkProcessBlobSizeMultiGetModes(b, false)
}

func BenchmarkProcessRawMultiGetModes(b *testing.B) {
	benchmarkProcessBlobSizeMultiGetModes(b, true)
}

func benchmarkProcessBlobSizeMultiGetModes(b *testing.B, raw bool) {
	binaryPath := os.Getenv("VAULTICDB_TEST_BINARY")
	if binaryPath == "" {
		b.Skip("set VAULTICDB_TEST_BINARY to an attribution-enabled daemon")
	}
	for _, count := range []int{1, 8} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("keys=%d/multiget=%t", count, enabled), func(b *testing.B) {
				directory, err := os.MkdirTemp("", "vd-size-")
				if err != nil {
					b.Fatal(err)
				}
				defer os.RemoveAll(directory)
				ctx := b.Context()
				client, err := Ensure(ctx, Options{
					Socket: filepath.Join(directory, "d.sock"), RepositoryID: "blob-size-benchmark",
					DaemonPath: binaryPath, DataDir: b.TempDir(), ObjectStore: "memory",
					testEnvironment: []string{fmt.Sprintf("VAULTICDB_SLATEDB_MULTIGET=%t", enabled)},
				})
				if err != nil {
					b.Fatal(err)
				}
				defer func() {
					if err := client.Close(context.Background()); err != nil {
						b.Error(err)
					}
				}()
				store := NewSchemaStore(client)
				handles := make([]vaultic.BlobHandle, count)
				keys := make([][]byte, count)
				for index := range handles {
					packID, blobID := daemonTestID(byte(40+index)), daemonTestID(byte(80+index))
					if err := store.PublishPack(ctx, readSessionTestPack(schema.ID(packID), schema.ID(blobID))); err != nil {
						b.Fatal(err)
					}
					handles[index] = vaultic.BlobHandle{ID: vaultic.ID(blobID), Type: vaultic.DataBlob}
					keys[index] = schema.BlobKey(blobID)
				}
				session, err := store.BeginReadSession(ctx)
				if err != nil {
					b.Fatal(err)
				}
				defer func() {
					if err := session.Close(context.Background()); err != nil {
						b.Error(err)
					}
				}()
				before, err := client.WriterStatus(ctx)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if raw {
						values, found, lookupErr := session.MultiGet(ctx, keys)
						if lookupErr != nil || len(values) != count || len(found) != count {
							b.Fatalf("raw lookup results=%d found=%d error=%v", len(values), len(found), lookupErr)
						}
						for index := range values {
							if !found[index] || len(values[index].Value) == 0 {
								b.Fatalf("missing raw blob record at %d", index)
							}
						}
						continue
					}
					sizes, lookupErr := session.LookupBlobSizesContext(ctx, handles)
					if lookupErr != nil || len(sizes) != count {
						b.Fatalf("lookup sizes=%d error=%v", len(sizes), lookupErr)
					}
					for _, size := range sizes {
						if size != (vaultic.BlobSize{Size: 7, Found: true}) {
							b.Fatalf("unexpected blob size: %+v", size)
						}
					}
				}
				b.StopTimer()
				after, err := client.WriterStatus(ctx)
				if err != nil {
					b.Fatal(err)
				}
				calls := after.Attribution.EngineMultiGetCalls - before.Attribution.EngineMultiGetCalls
				reads := after.Attribution.EngineGetKeys - before.Attribution.EngineGetKeys
				var wantCalls uint64
				if enabled {
					wantCalls = uint64(b.N)
				}
				if !after.Attribution.EngineMultiGetMetricsAvailable || calls != wantCalls ||
					reads != uint64(b.N*count) || after.Attribution.EngineWriteOps != before.Attribution.EngineWriteOps ||
					after.Attribution.CommitRequest.Attempts != before.Attribution.CommitRequest.Attempts {
					b.Fatalf("unexpected read work: calls=%d want=%d keys=%d want=%d", calls, wantCalls, reads, b.N*count)
				}
				b.ReportMetric(float64(calls)/float64(b.N), "engine-multiget/op")
				b.ReportMetric(float64(reads)/float64(b.N), "engine-keys/op")
			})
		}
	}
}

func BenchmarkProcessEmptyCrawlDebtResolution(b *testing.B) {
	binaryPath := os.Getenv("VAULTICDB_TEST_BINARY")
	if binaryPath == "" {
		b.Skip("set VAULTICDB_TEST_BINARY to an attribution-enabled daemon")
	}
	for _, transactional := range []bool{true, false} {
		b.Run(fmt.Sprintf("transactional=%t", transactional), func(b *testing.B) {
			socketDirectory, err := os.MkdirTemp("", "vd-debt-")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(socketDirectory)
			ctx := b.Context()
			client, err := Ensure(ctx, Options{
				Socket: filepath.Join(socketDirectory, "d.sock"), RepositoryID: "empty-debt-benchmark",
				DaemonPath: binaryPath, DataDir: b.TempDir(), ObjectStore: "memory",
			})
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				if err := client.Close(context.Background()); err != nil {
					b.Error(err)
				}
			}()
			store := NewSchemaStore(client)
			resolve := store.ResolveCrawlDebt
			var expectedTransactions uint64
			if transactional {
				resolve = store.resolveCrawlDebtOnce
				expectedTransactions = uint64(b.N)
			}
			before, err := client.WriterStatus(ctx)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := resolve(ctx, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			after, err := client.WriterStatus(ctx)
			if err != nil {
				b.Fatal(err)
			}
			beginBefore, beginAfter := before.Attribution.BeginRequest, after.Attribution.BeginRequest
			commitBefore, commitAfter := before.Attribution.CommitRequest, after.Attribution.CommitRequest
			if beginAfter.Attempts-beginBefore.Attempts != expectedTransactions ||
				beginAfter.Completed-beginBefore.Completed != expectedTransactions ||
				beginAfter.Successes-beginBefore.Successes != expectedTransactions ||
				beginAfter.Failures != beginBefore.Failures || beginAfter.Cancellations != beginBefore.Cancellations ||
				beginAfter.Timeouts != beginBefore.Timeouts || beginAfter.Active != 0 ||
				commitAfter.Attempts-commitBefore.Attempts != expectedTransactions ||
				commitAfter.Completed-commitBefore.Completed != expectedTransactions ||
				commitAfter.Successes-commitBefore.Successes != expectedTransactions ||
				commitAfter.Failures != commitBefore.Failures || commitAfter.Cancellations != commitBefore.Cancellations ||
				commitAfter.Timeouts != commitBefore.Timeouts || commitAfter.Active != 0 {
				b.Fatalf("unexpected transaction outcomes: begin=%+v -> %+v commit=%+v -> %+v", beginBefore, beginAfter, commitBefore, commitAfter)
			}
			if after.Attribution.RollbackRequest.Attempts != before.Attribution.RollbackRequest.Attempts ||
				after.Attribution.EngineWriteOps != before.Attribution.EngineWriteOps ||
				after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 {
				b.Fatalf("unexpected writes or cleanup: before=%+v after=%+v", before, after)
			}
			b.ReportMetric(float64(beginAfter.Attempts-beginBefore.Attempts)/float64(b.N), "begin/op")
			b.ReportMetric(float64(commitAfter.Attempts-commitBefore.Attempts)/float64(b.N), "commit/op")
			b.ReportMetric(float64(commitAfter.TotalUS-commitBefore.TotalUS)/float64(b.N), "commit-service-us/op")
		})
	}
}

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
