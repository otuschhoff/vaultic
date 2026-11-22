package daemon

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"google.golang.org/grpc"
)

func TestReadSessionBlobSizeMultiGetModes(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			ctx := t.Context()
			client, err := Ensure(ctx, Options{
				Socket: testSocket(t), RepositoryID: "blob-size-multiget",
				DaemonPath: daemonBinary(t), DataDir: t.TempDir(), ObjectStore: "memory",
				testEnvironment: []string{fmt.Sprintf("VAULTICDB_SLATEDB_MULTIGET=%t", enabled)},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close(context.Background()) })
			store := NewSchemaStore(client)
			firstPack, firstBlob := daemonTestID(231), daemonTestID(232)
			secondPack, secondBlob := daemonTestID(233), daemonTestID(234)
			for _, pack := range []PublishedPack{readSessionTestPack(firstPack, firstBlob), readSessionTestPack(secondPack, secondBlob)} {
				if err := store.PublishPack(ctx, pack); err != nil {
					t.Fatal(err)
				}
			}
			session, err := store.BeginReadSession(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close(context.Background())
			if err := store.PublishPack(ctx, readSessionTestPack(daemonTestID(236), daemonTestID(235))); err != nil {
				t.Fatal(err)
			}
			before, err := client.WriterStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			handles := []vaultic.BlobHandle{{ID: vaultic.ID(firstBlob), Type: vaultic.DataBlob},
				{ID: vaultic.ID(secondBlob), Type: vaultic.DataBlob},
				{ID: vaultic.ID(daemonTestID(235)), Type: vaultic.DataBlob}}
			sizes, err := session.LookupBlobSizesContext(ctx, handles)
			if err != nil || len(sizes) != len(handles) || sizes[0] != (vaultic.BlobSize{Size: 7, Found: true}) ||
				sizes[1] != (vaultic.BlobSize{Size: 7, Found: true}) || sizes[2].Found {
				t.Fatalf("blob sizes=%+v error=%v", sizes, err)
			}
			after, err := client.WriterStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !after.Attribution.EngineMultiGetMetricsAvailable ||
				after.Attribution.EngineGetKeys-before.Attribution.EngineGetKeys != uint64(len(handles)) {
				t.Fatalf("engine read accounting: before=%+v after=%+v", before.Attribution, after.Attribution)
			}
			calls := after.Attribution.EngineMultiGetCalls - before.Attribution.EngineMultiGetCalls
			var want uint64
			if enabled {
				want = 1
			}
			if calls != want {
				t.Fatalf("engine multi-get calls=%d want=%d", calls, want)
			}
			catalog := make(map[vaultic.BlobHandle]vaultic.BlobSize)
			if err := session.ScanBlobSizes(ctx, func(batch []vaultic.BlobHandle, sizes []vaultic.BlobSize) error {
				for ordinal, handle := range batch {
					catalog[handle] = sizes[ordinal]
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(catalog) != 2 || catalog[handles[0]] != sizes[0] || catalog[handles[1]] != sizes[1] || catalog[handles[2]].Found {
				t.Fatalf("catalog escaped the pinned session: %+v", catalog)
			}
		})
	}
}

func TestEncryptedBlobSizeReadAttribution(t *testing.T) {
	if os.Getenv("VAULTICDB_TEST_BLOB_SIZE_READS") != "1" {
		t.Skip("set VAULTICDB_TEST_BLOB_SIZE_READS=1 to run the encrypted fixture")
	}
	ctx := t.Context()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	passphrase := filepath.Join(root, "passphrase")
	if err := os.WriteFile(passphrase, []byte("vaultic blob size fixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(passphrase, 0600); err != nil {
		t.Fatal(err)
	}
	options := Options{
		Socket: testSocket(t), DaemonPath: daemonBinary(t), RepositoryID: "encrypted-blob-size-reads",
		DataDir: root, EncryptionMode: "initialize", PassphraseFile: passphrase,
		EngineMultiGet: true, WALFlushInterval: 25 * time.Millisecond,
		BlockCacheBytes: 512 << 20, MetaCacheBytes: 2 << 30,
	}
	if os.Getenv("VAULTICDB_TEST_BLOB_WORK") == "1" {
		options.testEnvironment = []string{"VAULTICDB_DIAGNOSTIC_BLOB_WORK=1"}
	}
	open := func(t *testing.T, options Options) (*Client, func()) {
		t.Helper()
		client, err := Ensure(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		closed := false
		closeClient := func() {
			if !closed {
				closed = true
				if err := client.Close(context.Background()); err != nil {
					t.Error(err)
				}
			}
		}
		t.Cleanup(closeClient)
		return client, closeClient
	}
	dispersed := os.Getenv("VAULTICDB_TEST_BLOB_SIZE_DISPERSED") == "1"
	filtered := dispersed && os.Getenv("VAULTICDB_TEST_BLOB_SIZE_FILTERED") == "1"
	if filtered {
		options.L0SSTSizeBytes = 192 << 10
	} else if dispersed && os.Getenv("VAULTICDB_TEST_BLOB_SIZE_MULTI_SST") == "1" {
		options.L0SSTSizeBytes = 64 << 10
	}
	writer, closeWriter := open(t, options)
	store := NewSchemaStore(writer)
	if dispersed {
		width := 5
		switch value := os.Getenv("VAULTICDB_TEST_BLOB_SIZE_WIDTH"); value {
		case "", "5":
		case "20":
			width = 20
		default:
			t.Fatalf("unsupported dispersed blob size width %q", value)
		}
		const packCount, blobsPerPack = 40, 100
		keys := make([][]byte, 0, packCount*blobsPerPack)
		blobLength := uint32(crypto.CiphertextLength(7))
		for packOrdinal := range packCount {
			packID := daemonTestID(byte(packOrdinal + 128))
			published := PublishedPack{
				PackID: packID,
				Record: schema.PackRecord{Type: schema.PackData, PayloadSize: uint64(blobsPerPack * blobLength),
					BlobCount: blobsPerPack, Lifecycle: schema.PackExportPending},
				Blobs: make(map[schema.ID]schema.BlobRecord, blobsPerPack),
			}
			for ordinal := range blobsPerPack {
				blobID := sha256.Sum256(fmt.Appendf(nil, "dispersed-blob-%d", len(keys)))
				published.Blobs[blobID] = schema.BlobRecord{Locations: []schema.BlobLocation{{
					PackID: packID, Offset: uint64(ordinal) * uint64(blobLength), Length: blobLength, Type: schema.BlobData,
				}}}
				keys = append(keys, schema.BlobKey(blobID))
			}
			if err := store.PublishPack(ctx, published); err != nil {
				t.Fatal(err)
			}
		}
		closeWriter()
		options.EncryptionMode = "required"
		t.Setenv("VAULTICDB_CHUNK_CACHE_BYTES", "16777216")
		for leg, depth := range []int{1, 32, 32, 1} {
			t.Run(fmt.Sprintf("leg=%d/depth=%d", leg+1, depth), func(t *testing.T) {
				options.Socket = testSocket(t)
				reader, _ := open(t, options)
				measureDispersedBlobReads(t, reader, keys, width, depth, options.L0SSTSizeBytes > 0, filtered)
			})
		}
		return
	}
	var keys [][]byte
	for index := range 96 {
		blobID := daemonTestID(byte(index + 1))
		if err := store.PublishPack(ctx, readSessionTestPack(daemonTestID(byte(index+128)), blobID)); err != nil {
			t.Fatal(err)
		}
		if index%6 == 0 {
			keys = append(keys, schema.BlobKey(blobID))
		}
	}
	closeWriter()
	options.EncryptionMode = "required"
	for _, width := range []int{1, 5, 16} {
		for _, chunkBytes := range []int{16 << 20, 0} {
			t.Run(fmt.Sprintf("width=%d/chunk-bytes=%d", width, chunkBytes), func(t *testing.T) {
				options.Socket = testSocket(t)
				t.Setenv("VAULTICDB_CHUNK_CACHE_BYTES", fmt.Sprint(chunkBytes))
				reader, _ := open(t, options)
				store := NewSchemaStore(reader)
				snapshot := func() WriterStatus {
					status, err := reader.WriterStatus(ctx)
					if err != nil || status.ActiveTransactions != 0 || status.ActiveWriteIntents != 0 ||
						!status.Attribution.EngineMultiGetMetricsAvailable || status.Attribution.CacheReads == nil ||
						status.Attribution.EncryptionReads == nil || status.Attribution.EncryptionReads.CiphertextOriginFileRangeRead == nil {
						t.Fatalf("isolated read counters unavailable: %v", err)
					}
					return status
				}
				read := func(phase string) ReadTiming {
					t.Helper()
					var timing ReadTiming
					values, found, err := store.MultiGet(WithReadTiming(ctx, &timing), keys[:width])
					if err != nil || !timing.Valid || !timing.DatabaseTimingValid || timing.EngineNS == 0 ||
						len(values) != width || len(found) != width {
						t.Fatalf("%s did not isolate one timed engine batch: timing=%+v err=%v", phase, timing, err)
					}
					for ordinal, value := range values {
						sizes, err := decodeBlobSizes(value.Value)
						if !found[ordinal] || string(value.Key) != string(keys[ordinal]) || err != nil ||
							sizes[vaultic.DataBlob] != (vaultic.BlobSize{Size: 7, Found: true}) {
							t.Fatalf("%s unexpected blob record at %d: %v", phase, ordinal, err)
						}
					}
					return timing
				}
				measure := func(phase string, calls int) {
					t.Helper()
					before := snapshot()
					var total ReadTiming
					for range calls {
						timing := read(phase)
						total.ClientNS += timing.ClientNS
						total.HandlerNS += timing.HandlerNS
						total.StorageNS += timing.StorageNS
						total.DatabaseLockNS += timing.DatabaseLockNS
						total.EngineNS += timing.EngineNS
					}
					after := snapshot()
					if before.InstanceID != after.InstanceID ||
						after.Attribution.EngineMultiGetCalls-before.Attribution.EngineMultiGetCalls != uint64(calls) ||
						after.Attribution.EngineMultiGetInputKeys-before.Attribution.EngineMultiGetInputKeys != uint64(calls*width) {
						t.Fatalf("%s did not isolate %d engine batches of %d keys", phase, calls, width)
					}
					oldBlock, newBlock := (*before.Attribution.CacheReads)["block"], (*after.Attribution.CacheReads)["block"]
					oldEncryption, newEncryption := before.Attribution.EncryptionReads, after.Attribution.EncryptionReads
					oldFile, newFile := oldEncryption.CiphertextOriginFileRangeRead, newEncryption.CiphertextOriginFileRangeRead
					if newBlock.Hits < oldBlock.Hits || newBlock.Misses < oldBlock.Misses ||
						newEncryption.ChunkCacheHits < oldEncryption.ChunkCacheHits || newEncryption.ChunkCacheMisses < oldEncryption.ChunkCacheMisses ||
						newFile.ReadCalls < oldFile.ReadCalls || newFile.ReadBytes < oldFile.ReadBytes {
						t.Fatalf("%s counters reset", phase)
					}
					t.Logf("phase=%s calls=%d keys_per_call=%d block_hits=%d block_misses=%d chunk_hits=%d chunk_misses=%d file_reads=%d file_bytes=%d rpc_ns=%d handler_ns=%d storage_ns=%d lock_ns=%d engine_ns=%d",
						phase, calls, width, newBlock.Hits-oldBlock.Hits, newBlock.Misses-oldBlock.Misses,
						newEncryption.ChunkCacheHits-oldEncryption.ChunkCacheHits, newEncryption.ChunkCacheMisses-oldEncryption.ChunkCacheMisses,
						newFile.ReadCalls-oldFile.ReadCalls, newFile.ReadBytes-oldFile.ReadBytes,
						total.ClientNS, total.HandlerNS, total.StorageNS, total.DatabaseLockNS, total.EngineNS)
				}
				measure("first", 1)
				measure("repeat", 1)
				if os.Getenv("VAULTICDB_TEST_BLOB_SIZE_PROFILE") == "1" {
					started := time.Now()
					measure("profile-warm", 20000)
					t.Logf("profile_realtime_start_ns=%d profile_realtime_end_ns=%d", started.UnixNano(), time.Now().UnixNano())
				}
			})
		}
	}
}

func measureDispersedBlobReads(t *testing.T, client *Client, keys [][]byte, width, depth int, multiSST, filtered bool) {
	t.Helper()
	ctx := t.Context()
	if len(keys)%width != 0 {
		t.Fatalf("%d keys cannot be covered exactly at width %d", len(keys), width)
	}
	calls := len(keys) / width
	store := NewSchemaStore(client)
	requestWork := os.Getenv("VAULTICDB_TEST_BLOB_WORK") == "1"
	multiGet := store.MultiGet
	var activeTransactions uint64
	if requestWork {
		session, err := store.BeginReadSession(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close(context.Background())
		multiGet = session.MultiGet
		activeTransactions = 1
	}
	snapshot := func() WriterStatus {
		status, err := client.WriterStatus(ctx)
		if err != nil || uint64(status.ActiveTransactions) != activeTransactions || status.ActiveWriteIntents != 0 ||
			!status.Attribution.EngineMultiGetMetricsAvailable || status.Attribution.CacheReads == nil ||
			status.Attribution.EncryptionReads == nil || status.Attribution.EncryptionReads.CiphertextOriginFileRangeRead == nil {
			t.Fatalf("isolated read counters unavailable: %v", err)
		}
		return status
	}
	for _, phase := range []string{"first-pass", "repeat"} {
		before := snapshot()
		var next, active, peak atomic.Int64
		started := make(chan struct{})
		var workers sync.WaitGroup
		latencies := make([]time.Duration, calls)
		timings := make([]ReadTiming, calls)
		for range depth {
			workers.Go(func() {
				<-started
				for {
					index := int(next.Add(1) - 1)
					if index >= calls {
						return
					}
					batch := keys[index*width : (index+1)*width]
					inFlight := active.Add(1)
					for previous := peak.Load(); inFlight > previous && !peak.CompareAndSwap(previous, inFlight); previous = peak.Load() {
					}
					callStart := time.Now()
					callCtx := WithReadTiming(ctx, &timings[index])
					var work BlobReadWorkTotals
					if requestWork {
						callCtx = WithBlobReadWork(callCtx, &work)
					}
					values, found, err := multiGet(callCtx, batch)
					latencies[index] = time.Since(callStart)
					active.Add(-1)
					if err != nil || !timings[index].Valid || timings[index].DatabaseTimingValid == requestWork || len(values) != width || len(found) != width {
						t.Errorf("%s batch %d invalid timing or response: %v", phase, index, err)
						return
					}
					if requestWork {
						sample := timings[index].BlobWork
						if !sample.Valid || work != (BlobReadWorkTotals{Samples: 1, SSTVisits: sample.SSTVisits,
							FilterNegatives: sample.FilterNegatives, PlannedBlocks: sample.PlannedBlocks}) {
							t.Errorf("%s batch %d work sample and accumulator disagree: %+v / %+v", phase, index, sample, work)
							return
						}
					}
					for ordinal, value := range values {
						sizes, err := decodeBlobSizes(value.Value)
						if !found[ordinal] || string(value.Key) != string(batch[ordinal]) || err != nil ||
							sizes[vaultic.DataBlob] != (vaultic.BlobSize{Size: 7, Found: true}) {
							t.Errorf("%s batch %d unexpected blob record at %d: %v", phase, index, ordinal, err)
							return
						}
					}
				}
			})
		}
		wallStart := time.Now()
		close(started)
		workers.Wait()
		elapsed := time.Since(wallStart)
		after := snapshot()
		old, current := before.Attribution, after.Attribution
		if before.InstanceID != after.InstanceID || peak.Load() != int64(depth) ||
			current.EngineMultiGetCalls-old.EngineMultiGetCalls != uint64(calls) ||
			current.EngineMultiGetInputKeys-old.EngineMultiGetInputKeys != uint64(len(keys)) ||
			current.EngineMultiGetUniqueKeys-old.EngineMultiGetUniqueKeys != uint64(len(keys)) ||
			current.EngineWriteOps != old.EngineWriteOps || current.CommitRequest.Attempts != old.CommitRequest.Attempts {
			t.Fatalf("%s did not isolate %d complete batches at peak %d: peak=%d", phase, calls, depth, peak.Load())
		}
		oldBlock, newBlock := (*old.CacheReads)["block"], (*current.CacheReads)["block"]
		oldFile, newFile := old.EncryptionReads.CiphertextOriginFileRangeRead, current.EncryptionReads.CiphertextOriginFileRangeRead
		if current.EngineMultiGetSSTVisits < old.EngineMultiGetSSTVisits ||
			current.EngineMultiGetCandidateKeys < old.EngineMultiGetCandidateKeys ||
			current.EngineMultiGetNeededBlocks < old.EngineMultiGetNeededBlocks ||
			current.EngineMultiGetNeededBlockBytes < old.EngineMultiGetNeededBlockBytes ||
			current.EngineMultiGetCoalescedReads < old.EngineMultiGetCoalescedReads ||
			current.EngineFilterPointNegative < old.EngineFilterPointNegative ||
			newBlock.Hits < oldBlock.Hits || newBlock.Misses < oldBlock.Misses ||
			newFile.ReadCalls < oldFile.ReadCalls || newFile.ReadBytes < oldFile.ReadBytes {
			t.Fatalf("%s counters reset", phase)
		}
		visits := current.EngineMultiGetSSTVisits - old.EngineMultiGetSSTVisits
		if multiSST && visits <= uint64(calls) {
			t.Fatalf("%s multi-SST fixture did not exercise fan-out: visits=%d calls=%d", phase, visits, calls)
		}
		negatives := current.EngineFilterPointNegative - old.EngineFilterPointNegative
		if filtered && negatives == 0 {
			t.Fatalf("%s filtered fixture did not reject any candidate keys", phase)
		}
		var rpcNS, storageNS, lockNS, engineNS uint64
		var inFlightTime time.Duration
		var samples uint64
		var work BlobReadWork
		for index, timing := range timings {
			inFlightTime += latencies[index]
			rpcNS += timing.ClientNS
			storageNS += timing.StorageNS
			lockNS += timing.DatabaseLockNS
			engineNS += timing.EngineNS
			if timing.BlobWork.Valid {
				samples++
				work.SSTVisits += timing.BlobWork.SSTVisits
				work.FilterNegatives += timing.BlobWork.FilterNegatives
				work.PlannedBlocks += timing.BlobWork.PlannedBlocks
			}
		}
		if requestWork {
			if samples != uint64(calls) || work.SSTVisits != visits || work.FilterNegatives != negatives ||
				work.PlannedBlocks != current.EngineMultiGetNeededBlocks-old.EngineMultiGetNeededBlocks {
				t.Fatalf("%s per-request work does not reconcile: samples=%d counts=%+v", phase, samples, work)
			}
			t.Logf("phase=%s authenticated_blob_samples=%d sst_visits=%d filter_negatives=%d planned_blocks=%d",
				phase, samples, work.SSTVisits, work.FilterNegatives, work.PlannedBlocks)
		}
		slices.Sort(latencies)
		t.Logf("phase=%s calls=%d keys=%d peak=%d mean_inflight=%.3f wall_ns=%d p50_ns=%d p99_ns=%d rpc_ns=%d storage_ns=%d lock_ns=%d engine_ns=%d sst_visits=%d filter_negatives=%d candidates=%d needed_blocks=%d needed_bytes=%d coalesced_reads=%d block_hits=%d block_misses=%d file_reads=%d file_bytes=%d",
			phase, calls, len(keys), peak.Load(), float64(inFlightTime)/float64(elapsed), elapsed.Nanoseconds(),
			latencies[(calls-1)/2].Nanoseconds(), latencies[(calls*99-1)/100].Nanoseconds(),
			rpcNS, storageNS, lockNS, engineNS,
			visits, negatives, current.EngineMultiGetCandidateKeys-old.EngineMultiGetCandidateKeys,
			current.EngineMultiGetNeededBlocks-old.EngineMultiGetNeededBlocks, current.EngineMultiGetNeededBlockBytes-old.EngineMultiGetNeededBlockBytes,
			current.EngineMultiGetCoalescedReads-old.EngineMultiGetCoalescedReads, newBlock.Hits-oldBlock.Hits, newBlock.Misses-oldBlock.Misses,
			newFile.ReadCalls-oldFile.ReadCalls, newFile.ReadBytes-oldFile.ReadBytes)
	}
}

func TestReadSessionPinsSnapshotAndClosesTransaction(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase33-read-session",
		DaemonPath: daemonBinary(t), DataDir: t.TempDir(), ObjectStore: "memory",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	store := NewSchemaStore(client)
	firstPack, firstBlob := daemonTestID(221), daemonTestID(222)
	if err := store.PublishPack(ctx, readSessionTestPack(firstPack, firstBlob)); err != nil {
		t.Fatal(err)
	}
	session, err := store.BeginReadSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if session.Identity.RepositoryID != "phase33-read-session" || session.Identity.Generation == 0 || session.Identity.SessionID == "" {
		t.Fatalf("invalid session identity: %+v", session.Identity)
	}
	if session.transaction.idleTimeout <= 0 {
		t.Fatal("daemon did not advertise the transaction idle timeout; rebuild it")
	}
	secondPack, secondBlob := daemonTestID(223), daemonTestID(224)
	if err := store.PublishPack(ctx, readSessionTestPack(secondPack, secondBlob)); err != nil {
		t.Fatal(err)
	}
	if _, found, err := session.Get(ctx, schema.PackKey(firstPack)); err != nil || !found {
		t.Fatalf("original snapshot value: found=%t err=%v", found, err)
	}
	if _, found, err := session.Get(ctx, schema.PackKey(secondPack)); err != nil || found {
		t.Fatalf("post-snapshot value: found=%t err=%v", found, err)
	}
	pendingCount := 0
	totals, available, err := session.ScanPackInventory(ctx, func(_ context.Context, id schema.ID, _ schema.PackRecord) error {
		pendingCount++
		if id != firstPack {
			t.Fatal("pack inventory escaped pinned snapshot")
		}
		return nil
	})
	if err != nil || available || totals != ([vaultic.NumBlobTypes]uint64{}) || pendingCount != 1 {
		t.Fatalf("unknown-size inventory: totals=%v available=%v pending=%d err=%v", totals, available, pendingCount, err)
	}
	handles := []vaultic.BlobHandle{
		{ID: vaultic.ID(firstBlob), Type: vaultic.DataBlob},
		{ID: vaultic.ID(secondBlob), Type: vaultic.DataBlob},
		{ID: vaultic.ID(firstBlob), Type: vaultic.TreeBlob},
		{ID: vaultic.ID(daemonTestID(225)), Type: vaultic.DataBlob},
		{ID: vaultic.ID(firstBlob), Type: vaultic.DataBlob},
	}
	batchLimit := client.limits.MaxBatchItems
	client.limits.MaxBatchItems = 2
	sizes, err := session.LookupBlobSizesContext(ctx, handles)
	client.limits.MaxBatchItems = batchLimit
	if err != nil || len(sizes) != len(handles) {
		t.Fatalf("pinned batched lookup: %v, %v", sizes, err)
	}
	for ordinal, size := range sizes {
		want := vaultic.BlobSize{}
		if ordinal == 0 || ordinal == 4 {
			want = vaultic.BlobSize{Size: 7, Found: true}
		}
		if size != want {
			t.Fatalf("batch result %d=%v, want %v", ordinal, size, want)
		}
		blobs, err := session.LookupContext(ctx, handles[ordinal])
		if err != nil || (len(blobs) != 0) != want.Found {
			t.Fatalf("pinned location lookup %d=%v err=%v", ordinal, blobs, err)
		}
		if want.Found && (len(blobs) != 1 || blobs[0].PackID() != vaultic.ID(firstPack) || blobs[0].Handle() != handles[ordinal] || blobs[0].PlaintextLength() != want.Size) {
			t.Fatalf("incorrect packed location: %v", blobs)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := session.LookupBlobSizesContext(canceled, handles); !errors.Is(err, context.Canceled) {
		t.Fatalf("batch ignored cancellation: %v", err)
	}
	if _, err := session.LookupContext(canceled, handles[0]); !errors.Is(err, context.Canceled) {
		t.Fatalf("location lookup ignored cancellation: %v", err)
	}
	if _, err := session.LookupContext(ctx, vaultic.BlobHandle{Type: vaultic.InvalidBlob}); err == nil {
		t.Fatal("location lookup accepted invalid type")
	}
	if _, err := session.LookupBlobSizesContext(ctx, make([]vaultic.BlobHandle, vaultic.BlobLookupBatchSize+1)); err == nil {
		t.Fatal("oversized batch accepted")
	}
	if err := session.Validate(ctx); err != nil {
		t.Fatalf("validate pinned session: %v", err)
	}
	entries, _, err := session.ScanPrefix(ctx, []byte("p:"), nil, 100)
	if err != nil || len(entries) != 1 {
		t.Fatalf("snapshot pack scan: entries=%d err=%v", len(entries), err)
	}
	if !client.Limits().ScanStream {
		t.Fatal("test daemon does not advertise scan streaming; rebuild it")
	}
	for _, streaming := range []bool{true, false} {
		client.limits.ScanStream = streaming
		var streamed []KeyValue
		if err := session.ScanRange(ctx, []byte("p:"), 1, func(chunk []KeyValue) error {
			streamed = append(streamed, chunk...)
			return nil
		}); err != nil || len(streamed) != 1 {
			t.Fatalf("streaming=%t: entries=%d err=%v", streaming, len(streamed), err)
		}
	}
	client.limits.ScanStream = true
	if stats := session.ScanStats(); stats.Records != 2 || stats.Chunks != 2 || stats.Bytes == 0 || stats.IteratorSetupNS == 0 {
		t.Fatalf("stream counters: %+v", stats)
	}
	consumeErr := errors.New("stop consuming")
	if err := session.ScanRange(ctx, []byte("p:"), 1, func([]KeyValue) error { return consumeErr }); !errors.Is(err, consumeErr) {
		t.Fatalf("consumer error = %v", err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.renewed:
	default:
		t.Fatal("session close did not join renewal")
	}
	if _, _, err := session.Get(ctx, schema.PackKey(firstPack)); err == nil {
		t.Fatal("read succeeded after session close")
	}
	if _, err := session.LookupBlobSizesContext(ctx, handles); err == nil {
		t.Fatal("batch succeeded after session close")
	}
	if _, err := session.LookupContext(ctx, handles[0]); err == nil {
		t.Fatal("location lookup succeeded after session close")
	}
}

func TestDecodeBlobSizes(t *testing.T) {
	location := schema.BlobLocation{PackID: daemonTestID(1), Length: uint32(crypto.CiphertextLength(7)), Type: schema.BlobData}
	compressed := location
	compressed.UncompressedSize = 7
	tree := location
	tree.Type, tree.UncompressedSize = schema.BlobTree, 123
	encoded, err := (schema.BlobRecord{Locations: []schema.BlobLocation{location, compressed, tree}}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	sizes, err := decodeBlobSizes(encoded)
	if err != nil || sizes[vaultic.DataBlob] != (vaultic.BlobSize{Size: 7, Found: true}) || sizes[vaultic.TreeBlob] != (vaultic.BlobSize{Size: 123, Found: true}) {
		t.Fatalf("decoded sizes=%v err=%v", sizes, err)
	}
	for _, blobType := range []vaultic.BlobType{vaultic.DataBlob, vaultic.TreeBlob} {
		handle := vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: blobType}
		blobs, err := decodeBlobLocations(encoded, handle)
		wantCount := 2
		if blobType == vaultic.TreeBlob {
			wantCount = 1
		}
		if err != nil || len(blobs) != wantCount {
			t.Fatalf("locations=%v err=%v", blobs, err)
		}
		for _, blob := range blobs {
			if blob.Handle() != handle || blob.PackID() != vaultic.ID(location.PackID) || blob.PlaintextLength() != sizes[blobType].Size {
				t.Fatalf("incorrect location: %+v", blob)
			}
		}
		if blobType == vaultic.DataBlob && (blobs[0].IsCompressed() || !blobs[1].IsCompressed()) {
			t.Fatal("compression metadata lost")
		}
	}
	for _, invalid := range []schema.BlobLocation{
		{PackID: location.PackID, Length: 1, Type: schema.BlobData},
		{PackID: location.PackID, Length: location.Length, UncompressedSize: 8, Type: schema.BlobData},
		{PackID: location.PackID, Offset: uint64(math.MaxUint32) + 1, Length: location.Length, Type: schema.BlobData},
	} {
		encoded, err := (schema.BlobRecord{Locations: []schema.BlobLocation{location, invalid}}).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeBlobSizes(encoded); !errors.Is(err, schema.ErrMalformed) {
			t.Fatalf("invalid sizes accepted: %v", err)
		}
		if blobs, err := decodeBlobLocations(encoded, vaultic.BlobHandle{Type: vaultic.DataBlob}); !errors.Is(err, schema.ErrMalformed) || len(blobs) != 0 {
			t.Fatalf("invalid locations accepted: %v, %v", blobs, err)
		}
	}
	if _, err := decodeBlobSizes([]byte("corrupt")); err == nil {
		t.Fatal("corrupt blob record accepted")
	}
	if _, err := decodeBlobLocations([]byte("corrupt"), vaultic.BlobHandle{Type: vaultic.DataBlob}); err == nil {
		t.Fatal("corrupt locations accepted")
	}
}

func BenchmarkDecodeBlobLocations(b *testing.B) {
	for _, count := range []int{1, 16, 256} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			locations := make([]schema.BlobLocation, count)
			for ordinal := range locations {
				locations[ordinal] = schema.BlobLocation{PackID: daemonTestID(1), Offset: uint64(ordinal) * 100, Length: 100, UncompressedSize: 123, Type: schema.BlobTree}
			}
			encoded, err := (schema.BlobRecord{Locations: locations}).MarshalBinary()
			if err != nil {
				b.Fatal(err)
			}
			handle := vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.TreeBlob}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := decodeBlobLocations(encoded, handle); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type scanStreamRPC struct {
	vaulticdbv1.VaulticDBClient
	responses []*vaulticdbv1.ScanResponse
	context   context.Context
}

type blockedBatchRPC struct {
	vaulticdbv1.VaulticDBClient
	started chan struct{}
}

func (rpc *blockedBatchRPC) MultiGet(ctx context.Context, _ *vaulticdbv1.MultiGetRequest, _ ...grpc.CallOption) (*vaulticdbv1.MultiGetResponse, error) {
	close(rpc.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (rpc *blockedBatchRPC) Get(ctx context.Context, _ *vaulticdbv1.GetRequest, _ ...grpc.CallOption) (*vaulticdbv1.GetResponse, error) {
	close(rpc.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestReadSessionLocationFailureCancelsInFlightRPC(t *testing.T) {
	rpc := &blockedBatchRPC{started: make(chan struct{})}
	client := &Client{rpc: rpc, limits: Limits{MaxMessageBytes: 1 << 20}}
	client.initializeSubclients()
	sessionCtx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	session := &ReadSession{SchemaStore: NewSchemaStore(client), transaction: &Transaction{client: client, id: "pinned"}, ctx: sessionCtx}
	failure := errors.New("read session lease lost")
	caller, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	done := make(chan error, 1)
	go func() { _, err := session.LookupContext(caller, vaultic.NewRandomBlobHandle()); done <- err }()
	select {
	case <-rpc.started:
		cancel(failure)
	case <-caller.Done():
		t.Fatal("location RPC did not start")
	}
	if err := <-done; !errors.Is(err, failure) {
		t.Fatalf("location RPC lost session cause: %v", err)
	}
}

func TestReadSessionBatchFailureCancelsInFlightRPC(t *testing.T) {
	rpc := &blockedBatchRPC{started: make(chan struct{})}
	client := &Client{rpc: rpc, limits: Limits{MaxBatchItems: 256, MaxMessageBytes: 1 << 20}}
	client.initializeSubclients()
	sessionCtx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	session := &ReadSession{SchemaStore: NewSchemaStore(client), transaction: &Transaction{client: client, id: "pinned"}, ctx: sessionCtx}
	failure := errors.New("read session lease lost")
	caller, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	done := make(chan error, 1)
	go func() {
		_, err := session.LookupBlobSizesContext(caller, []vaultic.BlobHandle{vaultic.NewRandomBlobHandle()})
		done <- err
	}()
	select {
	case <-rpc.started:
		cancel(failure)
	case <-caller.Done():
		t.Fatal("batch RPC did not start")
	}
	if err := <-done; !errors.Is(err, failure) {
		t.Fatalf("session failure did not cancel RPC with its cause: %v", err)
	}
}

func (rpc *scanStreamRPC) ScanStream(ctx context.Context, _ *vaulticdbv1.ScanRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[vaulticdbv1.ScanResponse], error) {
	rpc.context = ctx
	return &scanResponseStream{responses: rpc.responses}, nil
}

type scanResponseStream struct {
	grpc.ServerStreamingClient[vaulticdbv1.ScanResponse]
	responses []*vaulticdbv1.ScanResponse
}

func (stream *scanResponseStream) Recv() (*vaulticdbv1.ScanResponse, error) {
	if len(stream.responses) == 0 {
		return nil, io.EOF
	}
	response := stream.responses[0]
	stream.responses = stream.responses[1:]
	return response, nil
}

func TestScanStreamRejectsIncompleteOrInvalidResponses(t *testing.T) {
	entry := &vaulticdbv1.KeyValue{Key: []byte("b:1"), Value: []byte("value")}
	for name, responses := range map[string][]*vaulticdbv1.ScanResponse{
		"early EOF":          {{Entries: []*vaulticdbv1.KeyValue{entry}}},
		"empty incomplete":   {{}},
		"duplicate boundary": {{Entries: []*vaulticdbv1.KeyValue{entry}}, {Entries: []*vaulticdbv1.KeyValue{entry}, Done: true}},
		"outside prefix":     {{Entries: []*vaulticdbv1.KeyValue{{Key: []byte("c:1")}}, Done: true}},
		"item limit":         {{Entries: []*vaulticdbv1.KeyValue{entry, entry}, Done: true}},
		"byte limit":         {{Entries: []*vaulticdbv1.KeyValue{{Key: []byte("b:1"), Value: make([]byte, 2048)}}, Done: true}},
		"after completion":   {{Done: true}, {Done: true}},
	} {
		t.Run(name, func(t *testing.T) {
			rpc := &scanStreamRPC{responses: responses}
			transaction := &Transaction{client: &Client{rpc: rpc, limits: Limits{ScanStream: true, MaxPageItems: 1, MaxMessageBytes: 1024}}, id: "pinned"}
			if err := transaction.scanRange(context.Background(), []byte("b:"), 1, func([]KeyValue, ScanStats) error { return nil }); err == nil {
				t.Fatal("invalid stream accepted")
			}
			if rpc.context.Err() == nil {
				t.Fatal("stream was not cancelled")
			}
		})
	}
}

type inventoryStreamRPC struct{ scanStreamRPC }

func (rpc *inventoryStreamRPC) Get(_ context.Context, request *vaulticdbv1.GetRequest, _ ...grpc.CallOption) (*vaulticdbv1.GetResponse, error) {
	return &vaulticdbv1.GetResponse{Key: request.Key}, nil
}

func (rpc *inventoryStreamRPC) GenerationStatus(context.Context, *vaulticdbv1.GenerationStatusRequest, ...grpc.CallOption) (*vaulticdbv1.GenerationStatusResponse, error) {
	return &vaulticdbv1.GenerationStatusResponse{}, nil
}

func TestReadSessionPackInventory(t *testing.T) {
	header := uint64(pack.CalculateHeaderSize(nil) + pack.CalculateEntrySize(false))
	base := schema.PackRecord{Type: schema.PackData, Lifecycle: schema.PackExportPending, BlobCount: 1,
		PayloadSize: 100, HeaderSize: header, PhysicalSize: 100 + header, PhysicalSizeKnown: true}
	for _, name := range []string{"complete", "unknown size", "unknown type", "mixed", "deleted", "header mismatch", "callback error", "canceled callback", "session failure", "overflow"} {
		t.Run(name, func(t *testing.T) {
			record := base
			available := true
			switch name {
			case "unknown size":
				record.PhysicalSizeKnown, record.PhysicalSize, record.HeaderSize, available = false, 0, 0, false
			case "unknown type":
				record.Type, available = schema.PackUnknown, false
			case "mixed":
				record.Type = schema.PackMixed
			case "deleted":
				record.Lifecycle, available = schema.PackDeleted, false
			case "header mismatch":
				record.HeaderSize++
				record.PhysicalSize++
				available = false
			case "overflow":
				record.PayloadSize = math.MaxUint64 - header
				record.PhysicalSize = math.MaxUint64
			}
			encoded, err := record.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			first := &vaulticdbv1.KeyValue{Key: schema.PackKey(daemonTestID(1)), Value: encoded}
			entries := []*vaulticdbv1.KeyValue{first}
			if name == "overflow" {
				entries = append(entries, &vaulticdbv1.KeyValue{Key: schema.PackKey(daemonTestID(2)), Value: encoded})
			}
			rpc := &inventoryStreamRPC{scanStreamRPC: scanStreamRPC{responses: []*vaulticdbv1.ScanResponse{{Entries: entries, Done: true}}}}
			client := &Client{rpc: rpc, limits: Limits{ScanStream: true, MaxPageItems: 1000, MaxMessageBytes: 1 << 20}}
			client.initializeSubclients()
			sessionCtx, fail := context.WithCancelCause(t.Context())
			defer fail(nil)
			session := &ReadSession{SchemaStore: NewSchemaStore(client), transaction: &Transaction{client: client, id: "pinned"}, ctx: sessionCtx}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("inventory failure")
			calls := 0
			sizes, found, err := session.ScanPackInventory(ctx, func(context.Context, schema.ID, schema.PackRecord) error {
				calls++
				switch name {
				case "callback error":
					return failure
				case "canceled callback":
					cancel()
				case "session failure":
					fail(failure)
				}
				return nil
			})
			if name == "callback error" || name == "canceled callback" || name == "session failure" || name == "overflow" {
				if err == nil || found || sizes != ([vaultic.NumBlobTypes]uint64{}) {
					t.Fatalf("partial result escaped: %v %v %v", sizes, found, err)
				}
				if (name == "callback error" || name == "session failure") && !errors.Is(err, failure) {
					t.Fatalf("failure cause lost: %v", err)
				}
				return
			}
			var want [vaultic.NumBlobTypes]uint64
			if available && name != "mixed" {
				want[vaultic.DataBlob] = base.PhysicalSize
			}
			if err != nil || found != available || sizes != want {
				t.Fatalf("sizes=%v available=%v err=%v want=%v", sizes, found, err, want)
			}
			if name != "deleted" && calls != 1 {
				t.Fatalf("pending callbacks=%d", calls)
			}
		})
	}
}

func readSessionTestPack(packID, blobID schema.ID) PublishedPack {
	return PublishedPack{
		PackID: packID,
		Record: schema.PackRecord{
			Type: schema.PackData, PayloadSize: uint64(crypto.CiphertextLength(7)), BlobCount: 1, Lifecycle: schema.PackExportPending,
		},
		Blobs: map[schema.ID]schema.BlobRecord{blobID: {
			Locations: []schema.BlobLocation{{PackID: packID, Length: uint32(crypto.CiphertextLength(7)), Type: schema.BlobData}},
		}},
	}
}

func TestReadSessionRenewalDuringIdleWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		session := &ReadSession{}
		var renewals int
		session.startRenewal(context.Background(), time.Second, time.Second, func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("renewal has no deadline")
			}
			renewals++
			return nil
		})
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if renewals < 9 || session.Context().Err() != nil {
			t.Fatalf("renewals=%d err=%v", renewals, session.Context().Err())
		}
		session.cancel(context.Canceled)
		<-session.renewed
		previous := renewals
		time.Sleep(3 * time.Second)
		if renewals != previous {
			t.Fatal("renewal continued after cancellation")
		}
	})
}

func TestReadSessionRenewalFailureIsSticky(t *testing.T) {
	for _, stalled := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			session := &ReadSession{}
			failure := errors.New("lease lost")
			var attempts int
			session.startRenewal(context.Background(), time.Second, time.Second, func(ctx context.Context) error {
				attempts++
				if stalled {
					<-ctx.Done()
					return ctx.Err()
				}
				return failure
			})
			time.Sleep(4 * time.Second)
			synctest.Wait()
			want := failure
			if stalled {
				want = context.DeadlineExceeded
			}
			if !errors.Is(context.Cause(session.Context()), want) || attempts != 1 {
				t.Fatalf("attempts=%d cause=%v", attempts, context.Cause(session.Context()))
			}
			if err := session.Validate(context.Background()); !errors.Is(err, want) {
				t.Fatalf("validation lost renewal failure: %v", err)
			}
			<-session.renewed
		})
	}
}
