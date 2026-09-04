package daemon

import (
	"bytes"
	"context"
	"errors"
	"net"
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
	target, err := observability.ParseSyslogTarget("udp://" + listener.LocalAddr().String() + "?categories=auth,integrity&min-severity=warning")
	if err != nil {
		t.Fatal(err)
	}
	observability.SetDefaultSyslog(observability.NewSyslogExporter([]observability.SyslogTarget{target}, "host", "vaultic"))
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
	if !strings.Contains(joined, `"category":"auth"`) || !strings.Contains(joined, `"recovery":true`) || !strings.Contains(joined, `"category":"integrity"`) || !strings.Contains(joined, `"operation":"get"`) {
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
		"invalid_request": ErrInvalidRequest, "key_management": ErrKeyManagement,
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

func TestMetadataRebuildInitializationRequiresBrokeredRequiredEncryption(t *testing.T) {
	_, err := Ensure(context.Background(), Options{RebuildInitialize: true, EncryptionMode: "required"})
	if err == nil || !strings.Contains(err.Error(), "requires brokered required encryption") {
		t.Fatalf("missing broker accepted for metadata rebuild: %v", err)
	}
	_, err = Ensure(context.Background(), Options{RebuildInitialize: true, EncryptionMode: "initialize", BrokerSocket: "/tmp/broker.sock", BrokerManifest: "/tmp/manifest"})
	if err == nil || !strings.Contains(err.Error(), "requires brokered required encryption") {
		t.Fatalf("wrong encryption mode accepted for metadata rebuild: %v", err)
	}
}

func TestStorageRoundTripTransactionsPaginationAndRestart(t *testing.T) {
	dataDir := t.TempDir()
	options := Options{Socket: testSocket(t), RepositoryID: "phase3-storage", DaemonPath: daemonBinary(t), DataDir: dataDir}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	puts := []Mutation{{Key: []byte("b:a"), Value: []byte("one")}, {Key: []byte("b:b"), Value: []byte("two")}, {Key: []byte("b:c"), Value: []byte("three")}}
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
	if value, found, err := transaction.Get(ctx, []byte("tx:rollback")); err != nil || !found || string(value) != "hidden" {
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

func TestSchemaStoreConcurrentRevisionAllocationAndImmutability(t *testing.T) {
	options := Options{Socket: testSocket(t), RepositoryID: "phase3-schema", DaemonPath: daemonBinary(t), DataDir: t.TempDir()}
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
	if err := store.Put(ctx, key, encoded, true); err == nil || !strings.Contains(err.Error(), "dedicated transactional") {
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
	if err != nil || !batchFound[0] || !batchFound[1] || !bytes.Equal(batchValues[0].Value, packValue) || !bytes.Equal(batchValues[1].Value, aggregateValue) {
		t.Fatalf("mutable schema batch = %#v, %#v, %v", batchValues, batchFound, err)
	}
	blobKey := schema.BlobKey(daemonTestID(8))
	blobValue := encodeSchemaRecord(t, schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: daemonTestID(9), Type: schema.BlobData}}})
	packValue2 := encodeSchemaRecord(t, schema.PackRecord{Type: schema.PackData, PhysicalSize: 2, PhysicalSizeKnown: true, HeaderSize: 2, Lifecycle: schema.PackPublished})
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
	if err := store.WriteMutableBatch(ctx, nil, [][]byte{packKey}, true); err == nil || !strings.Contains(err.Error(), "remain visible") {
		t.Fatalf("pack deletion returned %v", err)
	}
	if err := store.WriteMutableBatch(ctx, []Mutation{{Key: aggregateKey, Value: aggregateValue}}, [][]byte{aggregateKey}, true); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate mutable mutation returned %v", err)
	}
	if err := store.Put(ctx, schema.NextRevisionKey(), mustNextRevision(t, 100), true); err == nil || !strings.Contains(err.Error(), "AllocateRevision") {
		t.Fatalf("generic revision-sequence write returned %v", err)
	}
	content := []schema.ID{daemonTestID(1), daemonTestID(2), daemonTestID(3)}
	manifestID := schema.ContentManifestID(content)
	reverseManifestKey := schema.ReverseManifestKey(content[0], manifestID)
	reverseManifestValue := encodeSchemaRecord(t, schema.ReverseManifestRecord{State: schema.ReferenceCurrent})
	manifestID, err = store.PublishContentManifest(ctx, content, []Mutation{{Key: reverseManifestKey, Value: reverseManifestValue}}, nil)
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
	reverseInodeValue := encodeSchemaRecord(t, schema.ReverseInodeRecord{LatestRevision: revision, State: schema.ReferenceCurrent})
	if err := store.PublishRevisionBatch(ctx, currentKey, key, encoded, revision, []Mutation{{Key: reverseInodeKey, Value: reverseInodeValue}}, nil); err != nil {
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
	if err := store.PublishRevision(ctx, currentKey, schema.InodeRevisionKey(1, 2, revision-1), encoded, revision-1); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("current-pointer regression returned %v", err)
	}
	if err := store.PublishRevision(ctx, currentKey, schema.InodeRevisionKey(1, 2, revision+1), []byte("invalid"), revision+1); err == nil {
		t.Fatal("revision publication accepted invalid record bytes")
	}
	if err := store.PublishRevision(ctx, schema.BlobKey(schema.ID{}), key, encoded, revision); err == nil {
		t.Fatal("non-current key accepted as current pointer")
	}
}

func TestSchemaStorePublishesReconciledRevisionAtomically(t *testing.T) {
	options := Options{Socket: testSocket(t), RepositoryID: "phase5-reconcile", DaemonPath: daemonBinary(t), DataDir: t.TempDir()}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	analyticsMetadata := schema.AnalyticsMetadataRecord{Enabled: true, Generation: 1, BuiltAt: time.Now().UnixNano(), ConfigJSON: "{}"}
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
	debtValue := encodeSchemaRecord(t, schema.CrawlDebtRecord{PathOrTree: []byte("dir/file"), Reason: schema.DebtUnknownFreshness, Status: schema.DebtPending})
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
	if err != nil || delta.Kind != schema.AnalyticsDeltaCreation || delta.Revision != revision || delta.IdentityGeneration != revision || delta.State != schema.AnalyticsLive {
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
	if err != nil || count.TotalReferences != 2 || count.DistinctInodes != 1 || count.DistinctRevisions != 1 || count.DistinctManifests != 1 || count.UpdateSequence != revision {
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
