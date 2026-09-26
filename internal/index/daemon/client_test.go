package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/observability"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEncryptionSecurityEventsRouteToSyslog(t *testing.T) {
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	target, err := observability.ParseSyslogTarget(
		"udp://" + listener.LocalAddr().String() + "?categories=auth,integrity&min-severity=warning",
	)
	if err != nil {
		t.Fatal(err)
	}
	observability.SetDefaultSyslog(
		observability.NewSyslogExporter([]observability.SyslogTarget{target}, "host", "vaultic"),
	)
	defer observability.SetDefaultSyslog(nil)
	client := &Client{
		options: Options{RepositoryID: "repo-a"},
		encryption: EncryptionInfo{
			Enabled: true, ActiveDEKVersion: 3, EnvelopeGeneration: 7,
			UnlockSlot: "offline", RecoveryUnlock: true,
		},
	}
	client.auditEncryptionUnlock(context.Background())
	client.auditRPCError(context.Background(), "get", status.Error(codes.DataLoss, "authentication failed"))
	messages := make([]string, 2)
	buffer := make([]byte, 4096)
	for index := range messages {
		if err := listener.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		count, _, err := listener.ReadFrom(buffer)
		if err != nil {
			t.Fatal(err)
		}
		messages[index] = string(buffer[:count])
	}
	joined := strings.Join(messages, "\n")
	if !strings.Contains(joined, `"category":"auth"`) || !strings.Contains(joined, `"recovery":true`) ||
		!strings.Contains(joined, `"category":"integrity"`) ||
		!strings.Contains(joined, `"operation":"get"`) {
		t.Fatalf("missing encryption security events: %s", joined)
	}
}

func TestClassifyRPCErrorPreservesStatusAndDetail(t *testing.T) {
	cause := errors.New("transport cause")
	detail := &vaulticdbv1.ErrorDetail{
		Code: "writer_fenced", Message: "writer is fenced", Generation: 42,
	}
	rpcStatus, err := status.New(codes.FailedPrecondition, "writer is fenced").WithDetails(detail)
	if err != nil {
		t.Fatal(err)
	}
	classified := classifyRPCError(rpcStatus.Err())
	if !errors.Is(classified, ErrWriterFenced) {
		t.Fatalf("classified error = %v, want ErrWriterFenced", classified)
	}
	wrapped := &RPCError{detail: detail, cause: cause, kind: ErrWriterFenced}
	if !errors.Is(wrapped, cause) || !errors.Is(wrapped, ErrWriterFenced) {
		t.Fatalf("RPCError did not preserve cause and kind: %v", wrapped)
	}
	if status.Code(classified) != codes.FailedPrecondition {
		t.Fatalf("status code = %v, want FailedPrecondition", status.Code(classified))
	}
	var daemonError *RPCError
	if !errors.As(classified, &daemonError) || daemonError.Detail().GetGeneration() != 42 {
		t.Fatalf("structured detail not preserved: %#v", daemonError)
	}
}

func TestEveryDaemonDetailCodeHasSentinel(t *testing.T) {
	tests := map[string]error{
		"writer_fenced": ErrWriterFenced, "writer_demoted": ErrWriterDemoted,
		"writer_transitioning": ErrWriterTransitioning, "generation_changed": ErrGenerationChanged,
		"namespace_mismatch": ErrNamespaceMismatch, "encryption_integrity": ErrEncryptionIntegrity,
		"idempotency_conflict": ErrIdempotencyConflict, "storage_unavailable": ErrStorageUnavailable,
		"generation_reconciliation_pending": ErrGenerationPending,
		"storage_conflict":                  ErrStorageConflict, "storage_data_loss": ErrStorageDataLoss,
		"authentication_failed": ErrAuthentication, "authorization_failed": ErrAuthorization,
		"invalid_request": ErrInvalidRequest, "key_management": ErrKeyManagement,
		"precondition_failed": ErrPrecondition, "deadline_exceeded": ErrDeadlineExceeded,
		"resource_exhausted": ErrResourceExhausted, "not_found": ErrNotFound,
		"writer_role": ErrWriterRole,
	}
	for code, want := range tests {
		t.Run(code, func(t *testing.T) {
			if got := daemonErrorKind(code); !errors.Is(got, want) {
				t.Fatalf("daemonErrorKind(%q) = %v, want %v", code, got, want)
			}
		})
	}
	if got := daemonErrorKind("future_code"); got != nil {
		t.Fatalf("unknown code classified as %v", got)
	}
}

func TestGenerationConflictReturnsTypedDaemonError(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "typed-generation-error",
		DaemonPath: daemonBinary(t), DataDir: t.TempDir(), ObjectStore: "memory",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(ctx); err != nil {
			t.Errorf("close daemon client: %v", err)
		}
	})
	current, err := client.GenerationStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.QuarantineGeneration(ctx, current.ActiveGeneration+1, strings.Repeat("a", 64))
	if !errors.Is(err, ErrGenerationChanged) {
		t.Fatalf("quarantine error = %v, want ErrGenerationChanged", err)
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("quarantine status = %v, want FailedPrecondition", status.Code(err))
	}
	var daemonError *RPCError
	if !errors.As(err, &daemonError) || daemonError.Detail().GetField() != "generation" {
		t.Fatalf("generation detail not preserved: %#v", daemonError)
	}
}

func TestRealDaemonAdmissionErrorsAreTyped(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "typed-admission-errors",
		DaemonPath: daemonBinary(t), DataDir: t.TempDir(), ObjectStore: "memory",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(ctx) })

	_, err = client.RPC().Get(ctx, &vaulticdbv1.GetRequest{Key: []byte("key")})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("missing context error = %v, want ErrInvalidRequest", err)
	}

	_, err = client.RPC().Get(ctx, &vaulticdbv1.GetRequest{
		Context: &vaulticdbv1.RequestContext{RequestId: "expired", DeadlineUnixMs: 1},
		Key:     []byte("key"),
	})
	if !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatalf("expired context error = %v, want ErrDeadlineExceeded", err)
	}

	deletes := make([][]byte, 10001)
	for index := range deletes {
		deletes[index] = []byte("key")
	}
	_, err = client.RPC().WriteBatch(ctx, &vaulticdbv1.WriteBatchRequest{
		Context: requestContext(ctx), Deletes: deletes,
	})
	if !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("batch limit error = %v, want ErrResourceExhausted", err)
	}

	_, err = client.RPC().Drain(ctx, &vaulticdbv1.Empty{Context: requestContext(ctx)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.RPC().Get(ctx, &vaulticdbv1.GetRequest{
		Context: requestContext(ctx), Key: []byte("key"),
	})
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("draining error = %v, want ErrStorageUnavailable", err)
	}
}

func TestMetadataRebuildInitializationRequiresBrokeredRequiredEncryption(t *testing.T) {
	_, err := Ensure(context.Background(), Options{RebuildInitialize: true, EncryptionMode: "required"})
	if err == nil || !strings.Contains(err.Error(), "requires brokered required encryption") {
		t.Fatalf("missing broker accepted for metadata rebuild: %v", err)
	}
	_, err = Ensure(
		context.Background(),
		Options{
			RebuildInitialize: true,
			EncryptionMode:    "initialize",
			BrokerSocket:      "/tmp/broker.sock",
			BrokerManifest:    "/tmp/manifest",
		},
	)
	if err == nil || !strings.Contains(err.Error(), "requires brokered required encryption") {
		t.Fatalf("wrong encryption mode accepted for metadata rebuild: %v", err)
	}
}

func TestDaemonStartupErrorReturnsCapturedStderr(t *testing.T) {
	cmd := &exec.Cmd{}
	var stderr bytes.Buffer
	cmd.Stderr = &limitedWriter{writer: &stderr, remaining: 64 * 1024}
	_, _ = cmd.Stderr.Write([]byte("startup failed\n"))

	if got := daemonStartupError(cmd); got != "startup failed" {
		t.Fatalf("daemonStartupError() = %q, want captured stderr", got)
	}
}

func TestStorageRoundTripTransactionsPaginationAndRestart(t *testing.T) {
	dataDir := t.TempDir()
	options := Options{
		Socket:       testSocket(t),
		RepositoryID: "phase3-storage",
		DaemonPath:   daemonBinary(t),
		DataDir:      dataDir,
	}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	puts := []Mutation{
		{Key: []byte("b:a"), Value: []byte("one")},
		{Key: []byte("b:b"), Value: []byte("two")},
		{Key: []byte("b:c"), Value: []byte("three")},
	}
	durable, err := client.WriteBatch(ctx, puts, nil, true, "")
	if err != nil || !durable {
		t.Fatalf("durable write = %t, %v", durable, err)
	}
	values, found, err := client.MultiGet(ctx, [][]byte{[]byte("b:a"), []byte("missing"), []byte("b:c")}, "")
	if err != nil {
		t.Fatal(err)
	}
	if !found[0] || found[1] || !found[2] || string(values[2].Value) != "three" {
		t.Fatalf("unexpected multi-get: %#v %#v", values, found)
	}
	first, done, err := client.ScanPage(ctx, []byte("b:"), nil, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if done || len(first) != 2 {
		t.Fatalf("first page = %#v, done=%t", first, done)
	}
	second, done, err := client.ScanPage(ctx, []byte("b:"), first[len(first)-1].Key, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if !done || len(second) != 1 || string(second[0].Key) != "b:c" {
		t.Fatalf("second page = %#v, done=%t", second, done)
	}
	empty, done, err := client.ScanPage(ctx, []byte("empty:"), nil, 2, "")
	if err != nil || !done || len(empty) != 0 {
		t.Fatalf("empty page = %#v, done=%t, err=%v", empty, done, err)
	}

	transaction, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.WriteBatch(ctx, []Mutation{{Key: []byte("tx:rollback"), Value: []byte("hidden")}}, nil); err != nil {
		t.Fatal(err)
	}
	if value, found, err := transaction.Get(ctx, []byte("tx:rollback")); err != nil || !found ||
		string(value) != "hidden" {
		t.Fatalf("transaction read = %q, %t, %v", value, found, err)
	}
	if _, found, err := client.Get(ctx, []byte("tx:rollback"), ""); err != nil || found {
		t.Fatalf("uncommitted value visible: %t, %v", found, err)
	}
	if err := transaction.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	transaction, err = client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := transaction.Rollback(canceled); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled rollback returned %v", err)
	}
	if err := transaction.Rollback(ctx); err != nil {
		t.Fatalf("retry rollback: %v", err)
	}
	transaction, err = client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	canceledCommit, cancelCommit := context.WithCancel(ctx)
	cancelCommit()
	if err := transaction.Commit(canceledCommit); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled commit returned %v", err)
	}
	if err := transaction.Commit(ctx); err == nil || !strings.Contains(err.Error(), "already closed") {
		t.Fatalf("ambiguous commit retry returned %v", err)
	}
	if err := transaction.Rollback(ctx); err != nil {
		t.Fatalf("rollback after ambiguous commit: %v", err)
	}
	transaction, err = client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.WriteBatch(ctx, []Mutation{{Key: []byte("tx:commit"), Value: []byte("visible")}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	client, err = Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	value, foundAfterRestart, err := client.Get(ctx, []byte("tx:commit"), "")
	if err != nil || !foundAfterRestart || string(value) != "visible" {
		t.Fatalf("restart read = %q, %t, %v", value, foundAfterRestart, err)
	}
}

func TestSchemaStoreRevisionAllocationContention(t *testing.T) {
	for _, mode := range []string{"concurrent", "serialized", "grouped", "grouped-10ms"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			dataDirectory := t.TempDir()
			if root := os.Getenv("VAULTICDB_TEST_DATA_ROOT"); root != "" {
				var err error
				dataDirectory, err = os.MkdirTemp(root, "allocation-")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(dataDirectory) })
			}
			options := Options{
				Socket: testSocket(t), RepositoryID: "allocation-contention",
				DaemonPath: daemonBinary(t), DataDir: dataDirectory,
				WALFlushInterval: 100 * time.Millisecond,
			}
			if mode == "grouped-10ms" {
				options.WALFlushInterval = 10 * time.Millisecond
			}
			client, err := Ensure(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close(context.Background())
			store := NewSchemaStore(client)
			before, err := client.WriterStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if before.EngineFlushIntervalMS != uint64(options.WALFlushInterval.Milliseconds()) {
				t.Fatalf("effective flush interval=%d", before.EngineFlushIntervalMS)
			}
			const count = 128
			concurrency, allocationSize := 4, 1
			if mode != "concurrent" {
				concurrency = 1
			}
			if strings.HasPrefix(mode, "grouped") {
				allocationSize = 4
			}
			revisions := make([]uint64, count)
			allocationErrors := make([]error, count)
			var workers sync.WaitGroup
			start := make(chan struct{})
			for worker := range concurrency {
				workers.Go(func() {
					<-start
					for index := worker * allocationSize; index < count; index += concurrency * allocationSize {
						first, err := store.AllocateRevisionBlock(ctx, uint64(allocationSize))
						for offset := range allocationSize {
							revisions[index+offset], allocationErrors[index+offset] = first+uint64(offset), err
						}
					}
				})
			}
			started := time.Now()
			close(start)
			workers.Wait()
			elapsed := time.Since(started)
			for _, err := range allocationErrors {
				if err != nil {
					t.Fatal(err)
				}
			}
			sort.Slice(revisions, func(left, right int) bool { return revisions[left] < revisions[right] })
			for index, revision := range revisions {
				if revision != uint64(index+1) {
					t.Fatalf("revision[%d]=%d", index, revision)
				}
			}
			after, err := client.WriterStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			attempts := after.Attribution.CommitRequest.Attempts - before.Attribution.CommitRequest.Attempts
			failures := after.Attribution.CommitRequest.Failures - before.Attribution.CommitRequest.Failures
			if after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 || attempts-failures != uint64(count/allocationSize) {
				t.Fatalf("allocation cleanup/accounting: attempts=%d failures=%d transactions=%d intents=%d",
					attempts, failures, after.ActiveTransactions, after.ActiveWriteIntents)
			}
			if mode != "concurrent" && failures != 0 {
				t.Fatalf("uncontended allocation had %d failed commits", failures)
			}
			durable := after.Attribution.DurableWait
			durableBefore := before.Attribution.DurableWait
			if durable.Failures != durableBefore.Failures || durable.Successes-durableBefore.Successes != uint64(count/allocationSize) {
				t.Fatalf("durable allocation waits: before=%+v after=%+v", durableBefore, durable)
			}
			t.Logf("revisions=%d seconds=%.6f attempts=%d failures=%d durable_us=%d wal_put_attempts=%d flush_ms=%d data_dir=%s",
				count, elapsed.Seconds(), attempts, failures, durable.TotalUS-durableBefore.TotalUS,
				after.Attribution.ObjectStoreWAL.Put.Timing.Attempts-before.Attribution.ObjectStoreWAL.Put.Timing.Attempts,
				before.EngineFlushIntervalMS, dataDirectory)
			if err := client.Close(ctx); err != nil {
				t.Fatal(err)
			}
			reopened, err := Ensure(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close(context.Background())
			if next, err := NewSchemaStore(reopened).AllocateRevision(ctx); err != nil || next != count+1 {
				t.Fatalf("reopened revision=%d err=%v", next, err)
			}
		})
	}
}

func TestSchemaStoreConcurrentRevisionAllocationAndImmutability(t *testing.T) {
	options := Options{
		Socket:       testSocket(t),
		RepositoryID: "phase3-schema",
		DaemonPath:   daemonBinary(t),
		DataDir:      t.TempDir(),
	}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	const count = 24
	revisions := make([]uint64, count)
	errs := make([]error, count)
	var group sync.WaitGroup
	for index := range count {
		group.Add(1)
		go func(index int) { defer group.Done(); revisions[index], errs[index] = store.AllocateRevision(ctx) }(index)
	}
	group.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i] < revisions[j] })
	for index, revision := range revisions {
		if revision != uint64(index+1) {
			t.Fatalf("revision[%d] = %d", index, revision)
		}
	}
	const blocks = 8
	const blockSize = 7
	starts := make([]uint64, blocks)
	errs = make([]error, blocks)
	for index := range blocks {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			starts[index], errs[index] = store.AllocateRevisionBlock(ctx, blockSize)
		}(index)
	}
	group.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	for index, start := range starts {
		if want := uint64(count + 1 + index*blockSize); start != want {
			t.Fatalf("revision block %d starts at %d, want %d", index, start, want)
		}
	}

	revision := revisions[len(revisions)-1]
	key := schema.InodeRevisionKey(1, 2, revision)
	record := schema.InodeRevision{ParentInode: 1, Known: schema.KnownParent, Freshness: schema.FreshnessImported}
	encoded, err := record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateImmutable(ctx, key, encoded); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateImmutable(ctx, key, encoded); err != nil {
		t.Fatal(err)
	}
	changed := append([]byte(nil), encoded...)
	changed[len(changed)-1] ^= 1
	if err := store.CreateImmutable(ctx, key, changed); err == nil {
		t.Fatal("immutable overwrite was accepted")
	}
	if err := store.Put(ctx, key, encoded, true); err == nil ||
		!strings.Contains(err.Error(), "dedicated transactional") {
		t.Fatalf("generic immutable write returned %v", err)
	}
	if err := store.Put(ctx, schema.PackKey(schema.ID{}), []byte("invalid"), true); err == nil {
		t.Fatal("generic write accepted an invalid pack value")
	}
	packKey := schema.PackKey(daemonTestID(9))
	aggregateKey := schema.PackAggregateKey(schema.AggregateAll)
	packValue := encodeSchemaRecord(t, schema.PackRecord{Type: schema.PackData, Lifecycle: schema.PackImported})
	aggregateValue := encodeSchemaRecord(t, schema.PackAggregate{PackCount: 1, UpdateSequence: revision})
	if err := store.WriteMutableBatch(ctx, []Mutation{{Key: packKey, Value: packValue}, {Key: aggregateKey, Value: aggregateValue}}, nil, true); err != nil {
		t.Fatal(err)
	}
	batchValues, batchFound, err := client.MultiGet(ctx, [][]byte{packKey, aggregateKey}, "")
	if err != nil || !batchFound[0] || !batchFound[1] || !bytes.Equal(batchValues[0].Value, packValue) ||
		!bytes.Equal(batchValues[1].Value, aggregateValue) {
		t.Fatalf("mutable schema batch = %#v, %#v, %v", batchValues, batchFound, err)
	}
	blobKey := schema.BlobKey(daemonTestID(8))
	blobValue := encodeSchemaRecord(
		t,
		schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: daemonTestID(9), Type: schema.BlobData}}},
	)
	packValue2 := encodeSchemaRecord(
		t,
		schema.PackRecord{
			Type:              schema.PackData,
			PhysicalSize:      2,
			PhysicalSizeKnown: true,
			HeaderSize:        2,
			Lifecycle:         schema.PackPublished,
		},
	)
	if err := store.PublishSchemaBatch(ctx, []Mutation{{Key: blobKey, Value: blobValue}, {Key: packKey, Value: packValue2}}, nil); err != nil {
		t.Fatal(err)
	}
	conflictingBlob := append([]byte(nil), blobValue...)
	conflictingBlob[44] ^= 1
	if err := store.PublishSchemaBatch(ctx, []Mutation{{Key: blobKey, Value: conflictingBlob}, {Key: packKey, Value: packValue}}, nil); err == nil {
		t.Fatal("mixed schema batch accepted conflicting immutable record")
	}
	packAfterConflict, foundAfterConflict, err := store.Get(ctx, packKey)
	if err != nil || !foundAfterConflict || !bytes.Equal(packAfterConflict, packValue2) {
		t.Fatalf("mixed schema batch was not atomic: %q, %t, %v", packAfterConflict, foundAfterConflict, err)
	}
	if err := store.WriteMutableBatch(ctx, nil, [][]byte{packKey}, true); err == nil ||
		!strings.Contains(err.Error(), "remain visible") {
		t.Fatalf("pack deletion returned %v", err)
	}
	if err := store.WriteMutableBatch(ctx, []Mutation{{Key: aggregateKey, Value: aggregateValue}}, [][]byte{aggregateKey}, true); err == nil ||
		!strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate mutable mutation returned %v", err)
	}
	if err := store.Put(ctx, schema.NextRevisionKey(), mustNextRevision(t, 100), true); err == nil ||
		!strings.Contains(err.Error(), "AllocateRevision") {
		t.Fatalf("generic revision-sequence write returned %v", err)
	}
	content := []schema.ID{daemonTestID(1), daemonTestID(2), daemonTestID(3)}
	manifestID := schema.ContentManifestID(content)
	reverseManifestKey := schema.ReverseManifestKey(content[0], manifestID)
	reverseManifestValue := encodeSchemaRecord(t, schema.ReverseManifestRecord{State: schema.ReferenceCurrent})
	manifestID, err = store.PublishContentManifest(
		ctx,
		content,
		[]Mutation{{Key: reverseManifestKey, Value: reverseManifestValue}},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if retryID, err := store.CreateContentManifest(ctx, content); err != nil || retryID != manifestID {
		t.Fatalf("content manifest retry = %x, %v", retryID, err)
	}
	if _, found, err := store.Get(ctx, reverseManifestKey); err != nil || !found {
		t.Fatalf("content manifest reverse reference: %t, %v", found, err)
	}
	currentKey := schema.CurrentInodeKey(1, 2)
	reverseInodeKey := schema.ReverseInodeKey(content[0], 1, 2)
	reverseInodeValue := encodeSchemaRecord(
		t,
		schema.ReverseInodeRecord{LatestRevision: revision, State: schema.ReferenceCurrent},
	)
	reverseMutation := Mutation{Key: reverseInodeKey, Value: reverseInodeValue}
	if err := store.PublishRevisionBatch(ctx, currentKey, key, encoded, revision, []Mutation{reverseMutation}, nil); err != nil {
		t.Fatal(err)
	}
	pointerBytes, found, err := store.Get(ctx, currentKey)
	if err != nil || !found {
		t.Fatalf("current pointer: %t, %v", found, err)
	}
	pointer, err := schema.UnmarshalCurrentPointer(pointerBytes)
	if err != nil || pointer.Revision != revision || !bytes.Equal(pointer.RecordKey, key) {
		t.Fatalf("current pointer = %#v, %v", pointer, err)
	}
	if _, found, err := store.Get(ctx, reverseInodeKey); err != nil || !found {
		t.Fatalf("revision reverse reference: %t, %v", found, err)
	}
	if err := store.PublishRevision(ctx, currentKey, key, []byte("different"), revision); err == nil {
		t.Fatal("conflicting revision publication was accepted")
	}
	if err := store.PublishRevision(ctx, currentKey, schema.InodeRevisionKey(1, 2, revision-1), encoded, revision-1); err == nil ||
		!strings.Contains(err.Error(), "newer") {
		t.Fatalf("current-pointer regression returned %v", err)
	}
	if err := store.PublishRevision(ctx, currentKey, schema.InodeRevisionKey(1, 2, revision+1), []byte("invalid"), revision+1); err == nil {
		t.Fatal("revision publication accepted invalid record bytes")
	}
	if err := store.PublishRevision(ctx, schema.BlobKey(schema.ID{}), key, encoded, revision); err == nil {
		t.Fatal("non-current key accepted as current pointer")
	}
}

func TestSchemaStoreAllocatedGroupReadYourWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "allocated-group", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	content := make([]schema.ID, schema.MaxInlineContentIDs+1)
	for index := range content {
		content[index] = daemonTestID(byte(index + 1))
	}
	value := encodeSchemaRecord(t, schema.InodeRevision{
		ParentInode: 7, Known: schema.KnownParent, Freshness: schema.FreshnessVerified,
		ContentMode: schema.ContentManifestRef, ContentManifestID: schema.ContentManifestID(content), ContentCount: uint32(len(content)),
	})
	before, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.PublishAllocatedReconciledRevisionGroup(ctx, 4, func(first uint64) ([]ReconciledRevision, error) {
		requests := make([]ReconciledRevision, 4)
		for index := range requests {
			inode, revision := uint64(10+index), first+uint64(index)
			requests[index] = ReconciledRevision{
				CurrentKey: schema.CurrentInodeKey(3, inode), RevisionKey: schema.InodeRevisionKey(3, inode, revision),
				RevisionValue: value, Revision: revision, ContentIDs: content,
			}
		}
		return requests, nil
	})
	if err != nil || first != 1 {
		t.Fatalf("group first=%d err=%v", first, err)
	}
	for _, id := range content {
		encoded, found, err := store.Get(ctx, schema.ReferenceCountKey(id))
		if err != nil || !found {
			t.Fatalf("reference found=%t err=%v", found, err)
		}
		references, err := schema.UnmarshalReferenceCountRecord(encoded)
		if err != nil || references.TotalReferences != 5 || references.DistinctInodes != 4 ||
			references.DistinctRevisions != 4 || references.DistinctManifests != 1 {
			t.Fatalf("group references=%+v err=%v", references, err)
		}
	}
	after, err := client.WriterStatus(ctx)
	if err != nil || after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 ||
		after.Attribution.CommitRequest.Successes-before.Attribution.CommitRequest.Successes != 1 {
		t.Fatalf("group accounting=%+v err=%v", after, err)
	}
	if next, err := store.AllocateRevision(ctx); err != nil || next != 5 {
		t.Fatalf("next revision=%d err=%v", next, err)
	}
}

func TestSchemaStoreAllocatedGroupRejectsPartialPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "allocated-group-failure", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	value := encodeSchemaRecord(t, schema.InodeRevision{ParentInode: 7, Known: schema.KnownParent, Freshness: schema.FreshnessVerified})
	for _, test := range []struct {
		name   string
		change func([]ReconciledRevision) []ReconciledRevision
	}{
		{name: "count", change: func(requests []ReconciledRevision) []ReconciledRevision { return requests[:3] }},
		{name: "late-invalid", change: func(requests []ReconciledRevision) []ReconciledRevision {
			requests[3].RevisionValue = []byte("invalid")
			return requests
		}},
		{name: "late-duplicate", change: func(requests []ReconciledRevision) []ReconciledRevision {
			requests[3].CurrentKey = requests[0].CurrentKey
			return requests
		}},
		{name: "late-counter-write", change: func(requests []ReconciledRevision) []ReconciledRevision {
			requests[3].RelatedPuts = []Mutation{{Key: schema.NextRevisionKey(), Value: []byte("invalid")}}
			return requests
		}},
		{name: "content-limit", change: func(requests []ReconciledRevision) []ReconciledRevision {
			requests[3].ContentIDs = make([]schema.ID, 4097)
			return requests
		}},
		{name: "byte-limit", change: func(requests []ReconciledRevision) []ReconciledRevision {
			requests[3].RevisionValue = make([]byte, (8<<20)+1)
			return requests
		}},
		{name: "related-limit", change: func(requests []ReconciledRevision) []ReconciledRevision {
			requests[3].DebtKeys = make([][]byte, 1025)
			return requests
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			first, err := store.PublishAllocatedReconciledRevisionGroup(ctx, 4, func(first uint64) ([]ReconciledRevision, error) {
				requests := make([]ReconciledRevision, 4)
				for index := range requests {
					inode, revision := uint64(10+index), first+uint64(index)
					requests[index] = ReconciledRevision{
						CurrentKey: schema.CurrentInodeKey(3, inode), RevisionKey: schema.InodeRevisionKey(3, inode, revision),
						RevisionValue: value, Revision: revision,
					}
				}
				return test.change(requests), nil
			})
			if err == nil || first != 0 {
				t.Fatalf("invalid group first=%d err=%v", first, err)
			}
			keys := [][]byte{schema.NextRevisionKey()}
			for index := range 4 {
				keys = append(keys, schema.CurrentInodeKey(3, uint64(10+index)), schema.InodeRevisionKey(3, uint64(10+index), uint64(index+1)))
			}
			for _, key := range keys {
				if _, found, err := store.Get(ctx, key); err != nil || found {
					t.Fatalf("partial group key=%x found=%t err=%v", key, found, err)
				}
			}
			writer, err := client.WriterStatus(ctx)
			if err != nil || writer.ActiveTransactions != 0 || writer.ActiveWriteIntents != 0 {
				t.Fatalf("failed group cleanup=%+v err=%v", writer, err)
			}
		})
	}
	for _, count := range []int{0, 5} {
		if _, err := store.PublishAllocatedReconciledRevisionGroup(ctx, count, func(uint64) ([]ReconciledRevision, error) {
			t.Fatal("invalid count reached builder")
			return nil, nil
		}); err == nil {
			t.Fatalf("invalid group count=%d accepted", count)
		}
	}
	if _, err := store.PublishAllocatedReconciledRevisionGroup(ctx, 4, nil); err == nil {
		t.Fatal("nil group builder accepted")
	}
	if next, err := store.AllocateRevision(ctx); err != nil || next != 1 {
		t.Fatalf("failed group consumed revisions: next=%d err=%v", next, err)
	}
	if _, err := store.AllocateRevisionBlock(ctx, math.MaxUint64-3); err != nil {
		t.Fatal(err)
	}
	if first, err := store.PublishAllocatedReconciledRevisionGroup(ctx, 4, func(uint64) ([]ReconciledRevision, error) {
		t.Fatal("exhausted group reached builder")
		return nil, nil
	}); err == nil || first != 0 {
		t.Fatalf("exhausted group first=%d err=%v", first, err)
	}
	if next, err := store.AllocateRevision(ctx); err != nil || next != math.MaxUint64-1 {
		t.Fatalf("exhausted group consumed revisions: next=%d err=%v", next, err)
	}
}

func TestSchemaStoreAllocatedPublicationSingleCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "allocated-publication",
		DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	before, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	failure := fmt.Errorf("build failed")
	if _, err := store.publishAllocatedReconciledRevisionOnce(ctx, func(uint64) (ReconciledRevision, error) {
		return ReconciledRevision{}, failure
	}); !errors.Is(err, failure) {
		t.Fatalf("builder failure=%v", err)
	}
	var request ReconciledRevision
	revision, err := store.publishAllocatedReconciledRevisionOnce(ctx, func(revision uint64) (ReconciledRevision, error) {
		record := schema.InodeRevision{ParentInode: 7, Known: schema.KnownParent, Freshness: schema.FreshnessVerified}
		request = ReconciledRevision{
			CurrentKey: schema.CurrentInodeKey(3, 10), RevisionKey: schema.InodeRevisionKey(3, 10, revision),
			RevisionValue: encodeSchemaRecord(t, record), Revision: revision,
		}
		return request, nil
	})
	if err != nil || revision != 1 {
		t.Fatalf("allocated publication revision=%d err=%v", revision, err)
	}
	after, err := client.WriterStatus(ctx)
	if err != nil || after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 ||
		after.Attribution.CommitRequest.Successes-before.Attribution.CommitRequest.Successes != 1 {
		t.Fatalf("atomic publication status=%+v err=%v", after, err)
	}
	value, found, err := store.Get(ctx, request.RevisionKey)
	if err != nil || !found || !bytes.Equal(value, request.RevisionValue) {
		t.Fatalf("published record found=%t err=%v", found, err)
	}
	if next, err := store.AllocateRevision(ctx); err != nil || next != 2 {
		t.Fatalf("next revision=%d err=%v", next, err)
	}
}

func TestSchemaStoreAllocatedPublicationRejectsInvalidRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "allocated-invalid", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	value := encodeSchemaRecord(t, schema.InodeRevision{ParentInode: 7, Known: schema.KnownParent, Freshness: schema.FreshnessVerified})
	build := func(revision uint64) (ReconciledRevision, error) {
		return ReconciledRevision{
			CurrentKey: schema.CurrentInodeKey(3, 10), RevisionKey: schema.InodeRevisionKey(3, 10, revision),
			RevisionValue: value, Revision: revision,
		}, nil
	}
	for _, test := range []struct {
		name  string
		build func(uint64) (ReconciledRevision, error)
	}{
		{name: "nil"},
		{name: "wrong-revision", build: func(revision uint64) (ReconciledRevision, error) { return build(revision + 1) }},
		{name: "invalid-record", build: func(revision uint64) (ReconciledRevision, error) {
			request, _ := build(revision)
			request.RevisionValue = []byte("invalid")
			return request, nil
		}},
		{name: "counter-overwrite", build: func(revision uint64) (ReconciledRevision, error) {
			request, _ := build(revision)
			request.RelatedPuts = []Mutation{{Key: schema.NextRevisionKey(), Value: []byte("invalid")}}
			return request, nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if revision, err := store.PublishAllocatedReconciledRevision(ctx, test.build); err == nil || revision != 0 {
				t.Fatalf("invalid publication revision=%d err=%v", revision, err)
			}
			for _, key := range [][]byte{schema.NextRevisionKey(), schema.CurrentInodeKey(3, 10), schema.InodeRevisionKey(3, 10, 1)} {
				if _, found, err := store.Get(ctx, key); err != nil || found {
					t.Fatalf("invalid request changed key=%x found=%t err=%v", key, found, err)
				}
			}
		})
	}
	exhausted, err := schema.MarshalNextRevision(math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	if first, err := store.AllocateRevisionBlock(ctx, math.MaxUint64-1); err != nil || first != 1 {
		t.Fatalf("reserve remaining revisions: first=%d err=%v", first, err)
	}
	if revision, err := store.PublishAllocatedReconciledRevision(ctx, build); err == nil || revision != 0 {
		t.Fatalf("exhausted publication revision=%d err=%v", revision, err)
	}
	if actual, _, err := store.Get(ctx, schema.NextRevisionKey()); err != nil || !bytes.Equal(actual, exhausted) {
		t.Fatalf("exhausted counter changed: %v", err)
	}
}

func TestSchemaStoreAllocatedPublicationFenceAndCancellation(t *testing.T) {
	for _, variant := range []struct{ canceled, group bool }{{}, {canceled: true}, {group: true}, {canceled: true, group: true}} {
		canceled := variant.canceled
		t.Run(fmt.Sprintf("canceled=%t/group=%t", canceled, variant.group), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, err := Ensure(ctx, Options{
				Socket: testSocket(t), RepositoryID: "allocated-fence", DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close(context.Background())
			store := NewSchemaStore(client)
			publicationCtx, cancelPublication := context.WithCancel(ctx)
			defer cancelPublication()
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			value := encodeSchemaRecord(t, schema.InodeRevision{ParentInode: 7, Known: schema.KnownParent, Freshness: schema.FreshnessVerified})
			type result struct {
				revision uint64
				err      error
			}
			done := make(chan result, 1)
			count := 1
			if variant.group {
				count = 4
			}
			go func() {
				first := true
				build := func(revision uint64) (ReconciledRevision, error) {
					if first {
						first = false
						close(entered)
						select {
						case <-publicationCtx.Done():
							return ReconciledRevision{}, publicationCtx.Err()
						case <-release:
						}
					}
					return ReconciledRevision{
						CurrentKey: schema.CurrentInodeKey(3, 10), RevisionKey: schema.InodeRevisionKey(3, 10, revision),
						RevisionValue: value, Revision: revision,
					}, nil
				}
				var revision uint64
				var err error
				if variant.group {
					revision, err = store.PublishAllocatedReconciledRevisionGroup(publicationCtx, count, func(first uint64) ([]ReconciledRevision, error) {
						requests := make([]ReconciledRevision, count)
						for index := range requests {
							request, err := build(first + uint64(index))
							if err != nil {
								return nil, err
							}
							request.CurrentKey = schema.CurrentInodeKey(3, uint64(10+index))
							request.RevisionKey = schema.InodeRevisionKey(3, uint64(10+index), request.Revision)
							requests[index] = request
						}
						return requests, nil
					})
				} else {
					revision, err = store.PublishAllocatedReconciledRevision(publicationCtx, build)
				}
				done <- result{revision: revision, err: err}
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("publication did not read the counter")
			}
			if fence, err := store.AllocateRevision(ctx); err != nil || fence != 1 {
				t.Fatalf("intervening fence=%d err=%v", fence, err)
			}
			if canceled {
				cancelPublication()
			} else {
				unblock()
			}
			var outcome result
			select {
			case outcome = <-done:
			case <-ctx.Done():
				t.Fatal("publication did not finish")
			}
			if canceled {
				if outcome.revision != 0 || !errors.Is(outcome.err, context.Canceled) {
					t.Fatalf("canceled publication=%+v", outcome)
				}
			} else if outcome.revision != 2 || outcome.err != nil {
				t.Fatalf("publication did not retry above fence: %+v", outcome)
			}
			for index := range count {
				inode := uint64(10 + index)
				if _, found, err := store.Get(ctx, schema.InodeRevisionKey(3, inode, uint64(1+index))); err != nil || found {
					t.Fatalf("losing revision found=%t err=%v", found, err)
				}
				encoded, found, err := store.Get(ctx, schema.CurrentInodeKey(3, inode))
				if err != nil || found == canceled {
					t.Fatalf("current pointer found=%t err=%v", found, err)
				}
				if !canceled {
					pointer, err := schema.UnmarshalCurrentPointer(encoded)
					if err != nil || pointer.Revision != uint64(2+index) {
						t.Fatalf("current pointer=%+v err=%v", pointer, err)
					}
				}
			}
			writer, err := client.WriterStatus(ctx)
			if err != nil || writer.ActiveTransactions != 0 || writer.ActiveWriteIntents != 0 {
				t.Fatalf("cleanup=%+v err=%v", writer, err)
			}
		})
	}
}

func TestSchemaStoreAllocatedPublicationCrashRecovery(t *testing.T) {
	for _, point := range []struct {
		name    string
		method  string
		durable bool
		group   bool
	}{
		{name: "buffered", method: "WriteBatch"},
		{name: "durable-response-withheld", method: "Commit", durable: true},
		{name: "acknowledged", durable: true},
		{name: "group-buffered", method: "WriteBatch", group: true},
		{name: "group-durable-response-withheld", method: "Commit", durable: true, group: true},
		{name: "group-acknowledged", durable: true, group: true},
	} {
		t.Run(point.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			dataDirectory := t.TempDir()
			if root := os.Getenv("VAULTICDB_TEST_DATA_ROOT"); root != "" {
				var err error
				dataDirectory, err = os.MkdirTemp(root, "atomic-crash-")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(dataDirectory) })
			}
			options := Options{
				Socket: testSocket(t), RepositoryID: "allocated-crash", DaemonPath: daemonBinary(t),
				DataDir: dataDirectory, WALFlushInterval: 100 * time.Millisecond,
			}
			reached := make(chan struct{}, 1)
			if point.method != "" {
				options.ResponseDeliveryForTesting = func(wait context.Context, method string) error {
					if method != "/vaulticdb.v1.VaulticDB/"+point.method {
						return nil
					}
					reached <- struct{}{}
					<-wait.Done()
					return wait.Err()
				}
			}
			client, err := Ensure(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close(context.Background())
			store := NewSchemaStore(client)
			content := []schema.ID{daemonTestID(1)}
			value := encodeSchemaRecord(t, schema.InodeRevision{
				ParentInode: 7, Known: schema.KnownParent, Freshness: schema.FreshnessVerified,
				ContentMode: schema.ContentInline, ContentIDs: content, ContentCount: 1,
			})
			publicationCtx, cancelPublication := context.WithCancel(ctx)
			defer cancelPublication()
			done := make(chan error, 1)
			count := 1
			if point.group {
				count = 4
			}
			go func() {
				build := func(inode, revision uint64) ReconciledRevision {
					return ReconciledRevision{
						CurrentKey: schema.CurrentInodeKey(3, inode), RevisionKey: schema.InodeRevisionKey(3, inode, revision),
						RevisionValue: value, Revision: revision, ContentIDs: content,
					}
				}
				var err error
				if point.group {
					_, err = store.PublishAllocatedReconciledRevisionGroup(publicationCtx, count, func(first uint64) ([]ReconciledRevision, error) {
						requests := make([]ReconciledRevision, count)
						for index := range requests {
							requests[index] = build(uint64(10+index), first+uint64(index))
						}
						return requests, nil
					})
				} else {
					_, err = store.PublishAllocatedReconciledRevision(publicationCtx, func(revision uint64) (ReconciledRevision, error) {
						return build(10, revision), nil
					})
				}
				done <- err
			}()
			if point.method == "" {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("publication did not acknowledge durability")
				}
			} else {
				select {
				case <-reached:
				case <-ctx.Done():
					t.Fatal("publication did not reach crash boundary")
				}
			}
			phase34M2KillDaemon(t, client)
			cancelPublication()
			if point.method != "" {
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("withheld response reported success")
					}
				case <-ctx.Done():
					t.Fatal("publication did not return after crash")
				}
			}
			options.Socket = testSocket(t)
			options.ResponseDeliveryForTesting = nil
			reopened, err := Ensure(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close(context.Background())
			store = NewSchemaStore(reopened)
			keys := [][]byte{
				schema.NextRevisionKey(), schema.CurrentInodeKey(3, 10), schema.InodeRevisionKey(3, 10, 1),
				schema.ReferenceCountKey(content[0]), schema.ReverseInodeKey(content[0], 3, 10),
			}
			for index := range count {
				keys = append(keys, schema.CurrentInodeKey(3, uint64(10+index)), schema.InodeRevisionKey(3, uint64(10+index), uint64(index+1)))
			}
			for _, key := range keys {
				_, found, err := store.Get(ctx, key)
				if err != nil || found != point.durable {
					t.Fatalf("recovered key=%x found=%t want=%t err=%v", key, found, point.durable, err)
				}
			}
			if point.durable {
				encoded, _, err := store.Get(ctx, schema.NextRevisionKey())
				if err != nil {
					t.Fatal(err)
				}
				if next, err := schema.UnmarshalNextRevision(encoded); err != nil || next != uint64(count+1) {
					t.Fatalf("recovered counter=%d err=%v", next, err)
				}
				encoded, _, err = store.Get(ctx, schema.InodeRevisionKey(3, 10, 1))
				if err != nil || !bytes.Equal(encoded, value) {
					t.Fatalf("recovered revision mismatch: %v", err)
				}
			}
		})
	}
}

func TestSchemaStoreConcurrentReconciledSharedContent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "concurrent-reconcile",
		DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	content := []schema.ID{daemonTestID(1)}
	start := make(chan struct{})
	results := make(chan error, 4)
	for index := range 4 {
		inode := uint64(index + 10)
		revision, err := store.AllocateRevision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		record := schema.InodeRevision{
			ParentInode: 7, Known: schema.KnownParent | schema.KnownPath,
			ContentMode: schema.ContentInline, ContentIDs: content, ContentCount: 1,
			SourcePath: fmt.Sprintf("dir/file%d", index), Freshness: schema.FreshnessVerified,
		}
		reconciled := ReconciledRevision{
			CurrentKey: schema.CurrentInodeKey(3, inode), RevisionKey: schema.InodeRevisionKey(3, inode, revision),
			RevisionValue: encodeSchemaRecord(t, record), Revision: revision, ContentIDs: content,
		}
		go func() {
			select {
			case <-start:
			case <-ctx.Done():
				results <- ctx.Err()
				return
			}
			err := store.PublishReconciledRevision(ctx, reconciled)
			if err == nil {
				err = store.PublishReconciledRevision(ctx, reconciled)
			}
			results <- err
		}()
	}
	close(start)
	for range 4 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	value, found, err := store.Get(ctx, schema.ReferenceCountKey(content[0]))
	if err != nil || !found {
		t.Fatalf("shared references: found=%t err=%v", found, err)
	}
	count, err := schema.UnmarshalReferenceCountRecord(value)
	if err != nil || count.TotalReferences != 4 || count.DistinctInodes != 4 || count.DistinctRevisions != 4 {
		t.Fatalf("shared reference count = %#v, err=%v", count, err)
	}
}

func TestSchemaStorePublishesReconciledRevisionAtomically(t *testing.T) {
	options := Options{
		Socket:       testSocket(t),
		RepositoryID: "phase5-reconcile",
		DaemonPath:   daemonBinary(t),
		DataDir:      t.TempDir(),
	}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	analyticsMetadata := schema.AnalyticsMetadataRecord{
		Enabled:    true,
		Generation: 1,
		BuiltAt:    time.Now().UnixNano(),
		ConfigJSON: "{}",
	}
	if err := store.Put(ctx, schema.AnalyticsMetadataKey(), encodeSchemaRecord(t, analyticsMetadata), true); err != nil {
		t.Fatal(err)
	}
	content := make([]schema.ID, schema.MaxInlineContentIDs+1)
	for index := range content {
		content[index] = daemonTestID(byte(index + 1))
	}
	manifestID := schema.ContentManifestID(content)
	revision, err := store.AllocateRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	record := schema.InodeRevision{
		ParentInode: 7, Known: schema.KnownParent | schema.KnownPath,
		ContentMode: schema.ContentManifestRef, ContentManifestID: manifestID, ContentCount: uint32(len(content)),
		SourcePath: "dir/file", Freshness: schema.FreshnessVerified,
	}
	currentKey := schema.CurrentInodeKey(3, 9)
	revisionKey := schema.InodeRevisionKey(3, 9, revision)
	debtKey := schema.CrawlDebtKey(daemonTestID(240), daemonTestID(241))
	debtValue := encodeSchemaRecord(
		t,
		schema.CrawlDebtRecord{
			PathOrTree: []byte("dir/file"),
			Reason:     schema.DebtUnknownFreshness,
			Status:     schema.DebtPending,
		},
	)
	if err := store.Put(ctx, debtKey, debtValue, true); err != nil {
		t.Fatal(err)
	}
	reconciled := ReconciledRevision{
		CurrentKey: currentKey, RevisionKey: revisionKey, RevisionValue: encodeSchemaRecord(t, record),
		Revision: revision, ContentIDs: content, DebtKeys: [][]byte{debtKey},
	}
	if err := store.PublishReconciledRevision(ctx, reconciled); err != nil {
		t.Fatal(err)
	}
	deltaValue, found, err := store.Get(ctx, schema.AnalyticsDeltaKey(revision, 0))
	if err != nil || !found {
		t.Fatalf("transactional analytics delta: found=%t err=%v", found, err)
	}
	delta, err := schema.UnmarshalAnalyticsDeltaRecord(deltaValue)
	if err != nil || delta.Kind != schema.AnalyticsDeltaCreation || delta.Revision != revision ||
		delta.IdentityGeneration != revision ||
		delta.State != schema.AnalyticsLive {
		t.Fatalf("transactional analytics delta = %#v, err=%v", delta, err)
	}
	if err := store.PublishReconciledRevision(ctx, reconciled); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	for _, key := range [][]byte{
		currentKey, revisionKey, schema.ContentManifestKey(manifestID, 0),
		schema.ReverseManifestKey(content[0], manifestID), schema.ReverseInodeKey(content[0], 3, 9),
		schema.ReferenceCountKey(content[0]), debtKey,
	} {
		if _, found, getErr := store.Get(ctx, key); getErr != nil || !found {
			t.Fatalf("reconciled key %q: found=%t err=%v", key, found, getErr)
		}
	}
	countValue, _, err := store.Get(ctx, schema.ReferenceCountKey(content[0]))
	if err != nil {
		t.Fatal(err)
	}
	count, err := schema.UnmarshalReferenceCountRecord(countValue)
	if err != nil || count.TotalReferences != 2 || count.DistinctInodes != 1 || count.DistinctRevisions != 1 ||
		count.DistinctManifests != 1 ||
		count.UpdateSequence != revision {
		t.Fatalf("reference count = %#v, err=%v", count, err)
	}
	resolvedValue, _, err := store.Get(ctx, debtKey)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := schema.UnmarshalCrawlDebtRecord(resolvedValue)
	if err != nil || resolved.Status != schema.DebtResolved {
		t.Fatalf("resolved debt = %#v, err=%v", resolved, err)
	}

	secondContent := append([]schema.ID(nil), content...)
	secondContent[0] = daemonTestID(239)
	secondManifestID := schema.ContentManifestID(secondContent)
	secondRevision, err := store.AllocateRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secondRecord := record
	secondRecord.ContentManifestID = secondManifestID
	secondKey := schema.InodeRevisionKey(3, 9, secondRevision)
	if err := store.PublishReconciledRevision(ctx, ReconciledRevision{
		CurrentKey: currentKey, RevisionKey: secondKey, RevisionValue: encodeSchemaRecord(t, secondRecord),
		Revision: secondRevision, ContentIDs: secondContent,
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(ctx, schema.AnalyticsDeltaKey(secondRevision, 0)); err != nil || found {
		t.Fatalf("later revision emitted a new creation delta: found=%t err=%v", found, err)
	}
	oldReverseValue, found, err := store.Get(ctx, schema.ReverseManifestKey(content[0], manifestID))
	if err != nil || !found {
		t.Fatalf("old manifest edge: found=%t err=%v", found, err)
	}
	oldReverse, err := schema.UnmarshalReverseManifestRecord(oldReverseValue)
	if err != nil || oldReverse.State != schema.ReferenceHistorical {
		t.Fatalf("old manifest edge = %#v, err=%v", oldReverse, err)
	}
	newReverseValue, found, err := store.Get(ctx, schema.ReverseManifestKey(secondContent[0], secondManifestID))
	if err != nil || !found {
		t.Fatalf("new manifest edge: found=%t err=%v", found, err)
	}
	newReverse, err := schema.UnmarshalReverseManifestRecord(newReverseValue)
	if err != nil || newReverse.State != schema.ReferenceCurrent {
		t.Fatalf("new manifest edge = %#v, err=%v", newReverse, err)
	}
	if _, found, err := store.Get(ctx, revisionKey); err != nil || !found {
		t.Fatalf("historical inode revision: found=%t err=%v", found, err)
	}
}
