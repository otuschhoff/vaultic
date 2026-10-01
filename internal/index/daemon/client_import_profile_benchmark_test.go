package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type benchmarkBlobSizeRPC struct {
	vaulticdbv1.VaulticDBClient
	value []byte
}

func TestProcessClonedOwnedBeginReadiness(t *testing.T) {
	root := os.Getenv("VAULTICDB_TEST_BEGIN_READINESS_CLONE")
	if root == "" {
		t.Skip("set VAULTICDB_TEST_BEGIN_READINESS_CLONE to the validated disposable clone")
	}
	const cloneRoot = "/ncl1-1-vs-50/fme_dump/amakura/db.begin-readiness-20261001-fuU56R"
	const artifacts = "/volume2/NASDA2/rustic/db.test/begin-readiness-deploy-20261001-fuU56R"
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != cloneRoot {
		t.Fatalf("refuse non-clone data directory: %q error=%v", resolved, err)
	}
	var marker struct {
		Clone                                                  string
		ChecksumVerified, WALRelocated, ProductionWALUnchanged bool
	}
	encoded, err := os.ReadFile(artifacts + "/clone.validated.json")
	if err != nil || json.Unmarshal(encoded, &marker) != nil || marker.Clone != resolved ||
		!marker.ChecksumVerified || !marker.WALRelocated || !marker.ProductionWALUnchanged {
		t.Fatal("clone checksum and WAL-relocation marker is missing or invalid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	options := Options{
		RepositoryID: "c4d68689c785d02a28d6eec485c62132823dc9873ff7f627c2bac80258251528",
		DaemonPath:   daemonBinary(t), DataDir: root, ObjectStore: "local", StartTimeout: 90 * time.Second,
		WALStore: "local", WALDataDir: filepath.Join(root, "wal"), WALFlushInterval: 100 * time.Millisecond,
		MetaCacheBytes: 128 << 20, BlockCacheBytes: 512 << 20, EncryptionMode: "required",
		PassphraseFile: "/volume2/NASDA2/rustic/etc/metadata-recovery",
	}
	open := func() *Client {
		current := options
		current.Socket = testSocket(t)
		client, err := Ensure(ctx, current)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		if !client.Limits().BeginReconciliation {
			t.Fatal("owned Begin capability not negotiated")
		}
		return client
	}
	client := open()
	before, err := client.WriterStatus(ctx)
	if err != nil || before.ActiveTransactions != 0 || before.ActiveWriteIntents != 0 || before.EngineMetaCacheBytes != 128<<20 || before.EngineBlockCacheBytes != 512<<20 || before.EngineFlushIntervalMS != 100 {
		t.Fatalf("clone not idle on defaults: err=%v", err)
	}
	sample, _, err := client.ScanPage(ctx, []byte{'b', ':'}, nil, 128, "")
	if err != nil || len(sample) != 128 {
		t.Fatalf("clone sample: count=%d err=%v", len(sample), err)
	}
	started := time.Now()
	var latestDeadline int64
	var deadlineMutex sync.Mutex
	jobs := make(chan int)
	failures := make(chan error, 4096)
	var workers sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		workers.Go(func() {
			for ordinal := range jobs {
				request := &vaulticdbv1.BeginOwnedRequest{Context: requestContext(ctx), BeginRequestId: fmt.Sprintf("%032x", ordinal+1), BeginDeadlineUnixMs: time.Now().Add(10 * time.Second).UnixMilli()}
				deadlineMutex.Lock()
				latestDeadline = max(latestDeadline, request.BeginDeadlineUnixMs)
				deadlineMutex.Unlock()
				response, err := client.rpc.BeginOwned(ctx, request)
				if err != nil || response.GetTransactionId() == "" {
					failures <- fmt.Errorf("Begin %d: %w", ordinal, err)
					continue
				}
				if err := client.cancelBegin(ctx, request); err != nil {
					failures <- fmt.Errorf("cancel %d: %w", ordinal, err)
				}
			}
		})
	}
	for ordinal := 0; ordinal < 4096; ordinal++ {
		jobs <- ordinal
	}
	close(jobs)
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}
	elapsed := time.Since(started)
	t.Logf("owned_begin_load workers=32 calls=4096 seconds=%.6f calls_per_second=%.3f", elapsed.Seconds(), 4096/elapsed.Seconds())
	if elapsed >= 60*time.Second {
		t.Fatal("ledger did not reach capacity within retention window")
	}
	full, err := client.WriterStatus(ctx)
	if err != nil || full.ActiveTransactions != 0 || full.ActiveWriteIntents != 0 || client.CommitRPCStats().Attempts != 0 || full.Attribution.CommitRequest.Attempts != before.Attribution.CommitRequest.Attempts {
		t.Fatalf("load leaked transaction/Commit: transactions=%d err=%v", full.ActiveTransactions, err)
	}
	probe := &vaulticdbv1.BeginOwnedRequest{Context: requestContext(ctx), BeginRequestId: strings.Repeat("f", 32), BeginDeadlineUnixMs: time.Now().Add(10 * time.Second).UnixMilli()}
	if _, err := client.rpc.BeginOwned(ctx, probe); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("ledger capacity did not fail closed: %v", err)
	}
	if err := client.cancelBegin(ctx, probe); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("full ledger cleanup not explicit: %v", err)
	}
	retirement := time.NewTimer(time.Until(time.UnixMilli(latestDeadline).Add(60*time.Second + time.Millisecond)))
	defer retirement.Stop()
	select {
	case <-retirement.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	probe.BeginDeadlineUnixMs = time.Now().Add(10 * time.Second).UnixMilli()
	if _, err := client.rpc.BeginOwned(ctx, probe); err != nil {
		t.Fatalf("retired ledger did not recover capacity: %v", err)
	}
	if err := client.cancelBegin(ctx, probe); err != nil {
		t.Fatal(err)
	}
	first := &vaulticdbv1.CancelBeginRequest{Context: requestContext(ctx), BeginRequestId: fmt.Sprintf("%032x", 4096), BeginDeadlineUnixMs: latestDeadline}
	if _, err := client.rpc.CancelBegin(ctx, first); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("retired history acknowledged cleanup: %v", err)
	}
	sustainedStarted := time.Now()
	sustainedJobs := make(chan int)
	sustainedFailures := make(chan error, 2400)
	var sustainedWorkers sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		sustainedWorkers.Go(func() {
			for ordinal := range sustainedJobs {
				request := &vaulticdbv1.BeginOwnedRequest{Context: requestContext(ctx), BeginRequestId: fmt.Sprintf("%032x", ordinal+4097), BeginDeadlineUnixMs: time.Now().Add(10 * time.Second).UnixMilli()}
				response, err := client.rpc.BeginOwned(ctx, request)
				if err != nil || response.GetTransactionId() == "" {
					sustainedFailures <- fmt.Errorf("sustained Begin %d: %w", ordinal, err)
					continue
				}
				if err := client.cancelBegin(ctx, request); err != nil {
					sustainedFailures <- err
				}
			}
		})
	}
	ticker := time.NewTicker(time.Second / 20)
	for ordinal := 0; ordinal < 2400; ordinal++ {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		sustainedJobs <- ordinal
	}
	ticker.Stop()
	close(sustainedJobs)
	sustainedWorkers.Wait()
	close(sustainedFailures)
	for err := range sustainedFailures {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}
	t.Logf("owned_begin_sustained calls=2400 rate=20 seconds=%.6f failures=0", time.Since(sustainedStarted).Seconds())
	orphan := &vaulticdbv1.BeginOwnedRequest{Context: requestContext(ctx), BeginRequestId: strings.Repeat("e", 32), BeginDeadlineUnixMs: time.Now().Add(10 * time.Second).UnixMilli()}
	response, err := client.rpc.BeginOwned(ctx, orphan)
	if err != nil || response.GetTransactionId() == "" {
		t.Fatalf("restart fixture Begin: %v", err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := open()
	after, err := restarted.WriterStatus(ctx)
	if err != nil || after.CurrentEpoch <= before.CurrentEpoch || after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 {
		t.Fatalf("restart retained ownership: transactions=%d err=%v", after.ActiveTransactions, err)
	}
	if err := restarted.cancelBegin(ctx, orphan); err != nil {
		t.Fatalf("restart cleanup tombstone failed: %v", err)
	}
	if _, err := restarted.rpc.BeginOwned(ctx, orphan); status.Code(err) != codes.Canceled {
		t.Fatalf("restart allowed canceled identity: %v", err)
	}
	current, _, err := restarted.ScanPage(ctx, []byte{'b', ':'}, nil, 128, "")
	if err != nil || len(current) != len(sample) {
		t.Fatalf("restart sample count changed: %v", err)
	}
	for ordinal := range sample {
		if string(sample[ordinal].Key) != string(current[ordinal].Key) || string(sample[ordinal].Value) != string(current[ordinal].Value) {
			t.Fatal("read-only clone metadata changed")
		}
	}
	if after.Attribution.CommitRequest.Attempts != 0 {
		t.Fatal("restart attempted Commit")
	}
	t.Logf("readiness capacity=4096 reclaimed=true retired_history=failed-precondition restart_transactions=0 sample_records=%d before_epoch=%d after_epoch=%d", len(sample), before.CurrentEpoch, after.CurrentEpoch)
}

func TestProcessClonedBlobLookup(t *testing.T) {
	root := os.Getenv("VAULTICDB_TEST_CLONE_ROOT")
	if root == "" {
		t.Skip("set VAULTICDB_TEST_CLONE_ROOT to the validated disposable clone")
	}
	const cloneRoot = "/ncl1-1-vs-50/fme_dump/amakura/db.admission-20260930"
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != cloneRoot {
		t.Fatalf("refuse non-clone data directory: %q error=%v", resolved, err)
	}
	var marker struct{ Clone string }
	encoded, err := os.ReadFile("/volume2/NASDA2/rustic/db.test/admission-clone-20260930/clone.validated.json")
	if err != nil || json.Unmarshal(encoded, &marker) != nil || marker.Clone != resolved {
		t.Fatal("clone checksum-validation marker is missing or does not match")
	}
	ctx := t.Context()
	if _, deadline := ctx.Deadline(); deadline {
		t.Fatal("clone probe must preserve the default per-RPC deadline")
	}
	metaCacheMiB := uint64(128)
	if value := os.Getenv("VAULTICDB_TEST_CLONE_META_CACHE_MIB"); value != "" {
		metaCacheMiB, err = strconv.ParseUint(value, 10, 16)
		if err != nil || metaCacheMiB < 128 || metaCacheMiB > 1024 {
			t.Fatal("clone metadata cache must be between 128 and 1024 MiB")
		}
	}
	options := Options{
		RepositoryID: "c4d68689c785d02a28d6eec485c62132823dc9873ff7f627c2bac80258251528",
		DaemonPath:   daemonBinary(t), DataDir: root, ObjectStore: "local",
		WALStore: "local", WALDataDir: filepath.Join(root, "wal"), WALFlushInterval: 100 * time.Millisecond,
		MetaCacheBytes: metaCacheMiB << 20, BlockCacheBytes: 512 << 20,
		EncryptionMode: "required", PassphraseFile: "/volume2/NASDA2/rustic/etc/metadata-recovery",
	}
	open := func(t *testing.T) (*Client, *ReadSession) {
		t.Helper()
		current := options
		current.Socket = testSocket(t)
		client, err := Ensure(ctx, current)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		session, err := NewSchemaStore(client).BeginReadSession(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = session.Close(context.Background()) })
		return client, session
	}
	client, session := open(t)
	var handles []vaultic.BlobHandle
	var expected []vaultic.BlobSize
	for prefix := 0; prefix < 256; prefix += 4 {
		entries, _, err := session.ScanPrefix(ctx, []byte{'b', ':', byte(prefix)}, nil, 8)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			parsed, err := schema.ParseKey(entry.Key)
			if err != nil {
				t.Fatal(err)
			}
			sizes, err := decodeBlobSizes(entry.Value)
			if err != nil {
				t.Fatal(err)
			}
			for blobType, size := range sizes {
				if size.Found {
					handles = append(handles, vaultic.BlobHandle{ID: vaultic.ID(parsed.ID), Type: vaultic.BlobType(blobType)})
					expected = append(expected, size)
					break
				}
			}
		}
	}
	if len(handles) == 0 {
		t.Fatal("clone contains no sampled blob records")
	}
	presentCount := len(handles)
	for index := range 128 {
		id := sha256.Sum256(fmt.Appendf(nil, "vaultic-admission-clone-missing:%d", index))
		handles = append(handles, vaultic.BlobHandle{ID: vaultic.ID(id), Type: vaultic.DataBlob})
		expected = append(expected, vaultic.BlobSize{})
	}
	missing, err := session.LookupBlobSizesContext(ctx, handles[presentCount:])
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range missing {
		if size.Found {
			t.Fatal("deterministic absent probe unexpectedly exists")
		}
	}
	t.Logf("clone_samples present=%d absent=%d", presentCount, len(handles)-presentCount)
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	order := rand.New(rand.NewSource(33)).Perm(len(handles))
	for _, concurrency := range []int{1, 8, 32} {
		t.Run(fmt.Sprintf("workers=%d", concurrency), func(t *testing.T) {
			client, session := open(t)
			for _, pass := range []string{"engine-cold", "warm"} {
				before, err := client.WriterStatus(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if before.EngineMetaCacheBytes != options.MetaCacheBytes || before.EngineBlockCacheBytes != options.BlockCacheBytes || before.EngineFlushIntervalMS != 100 {
					t.Fatalf("clone configuration mismatch: metadata=%d block=%d flush=%d", before.EngineMetaCacheBytes, before.EngineBlockCacheBytes, before.EngineFlushIntervalMS)
				}
				type result struct {
					queue, elapsed time.Duration
					err            error
				}
				results := make([]result, len(order))
				type job struct {
					ordinal int
					queued  time.Time
				}
				jobs := make(chan job, len(order))
				started := time.Now()
				for ordinal := range order {
					jobs <- job{ordinal: ordinal, queued: time.Now()}
				}
				close(jobs)
				var workers sync.WaitGroup
				for range concurrency {
					workers.Go(func() {
						for pending := range jobs {
							index := order[pending.ordinal]
							issued := time.Now()
							sizes, err := session.LookupBlobSizesContext(ctx, handles[index:index+1])
							if err == nil && (len(sizes) != 1 || sizes[0] != expected[index]) {
								err = fmt.Errorf("size mismatch for sampled blob %s", handles[index].ID.Str())
							}
							results[pending.ordinal] = result{queue: issued.Sub(pending.queued), elapsed: time.Since(issued), err: err}
						}
					})
				}
				workers.Wait()
				wall := time.Since(started)
				after, err := client.WriterStatus(ctx)
				if err != nil {
					t.Fatal(err)
				}
				latencies := make([]time.Duration, len(results))
				classes := map[string][]time.Duration{"present": nil, "absent": nil}
				var queueTotal time.Duration
				failures := 0
				for ordinal, result := range results {
					latencies[ordinal] = result.elapsed
					class := "present"
					if order[ordinal] >= presentCount {
						class = "absent"
					}
					classes[class] = append(classes[class], result.elapsed)
					queueTotal += result.queue
					if result.err != nil {
						failures++
						t.Logf("lookup_failure sample=%d elapsed=%s error=%v", ordinal, result.elapsed, result.err)
					}
				}
				sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
				t.Logf("clone_lookup pass=%s workers=%d calls=%d seconds=%.6f queue_mean_ms=%.3f p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f max_ms=%.3f failures=%d",
					pass, concurrency, len(results), wall.Seconds(), float64(queueTotal.Microseconds())/float64(len(results))/1000,
					float64(latencies[len(results)/2].Microseconds())/1000, float64(latencies[(len(results)-1)*95/100].Microseconds())/1000,
					float64(latencies[(len(results)-1)*99/100].Microseconds())/1000, float64(latencies[len(results)-1].Microseconds())/1000, failures)
				for _, class := range []string{"present", "absent"} {
					values := classes[class]
					sort.Slice(values, func(left, right int) bool { return values[left] < values[right] })
					t.Logf("clone_lookup_class pass=%s workers=%d class=%s calls=%d p50_ms=%.3f p99_ms=%.3f max_ms=%.3f",
						pass, concurrency, class, len(values), float64(values[len(values)/2].Microseconds())/1000,
						float64(values[(len(values)-1)*99/100].Microseconds())/1000, float64(values[len(values)-1].Microseconds())/1000)
				}
				beforeJSON, _ := json.Marshal(before.Attribution)
				afterJSON, _ := json.Marshal(after.Attribution)
				t.Logf("clone_attribution_before=%s", beforeJSON)
				t.Logf("clone_attribution_after=%s", afterJSON)
				if failures != 0 || after.Attribution.EngineWriteOps != before.Attribution.EngineWriteOps ||
					after.Attribution.CommitRequest.Attempts != before.Attribution.CommitRequest.Attempts {
					t.Errorf("lookup failures=%d or unexpected writes/Commits", failures)
				}
			}
		})
	}
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
