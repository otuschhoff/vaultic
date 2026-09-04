package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/observability"
	"google.golang.org/grpc"
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

func TestSchemaStorePublishesAuthoritativePacksAndDuplicateLocations(t *testing.T) {
	client, err := Ensure(context.Background(), Options{Socket: testSocket(t), RepositoryID: "phase6-packs", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	blobID, packOne, packTwo := daemonTestID(31), daemonTestID(32), daemonTestID(33)
	for _, published := range []PublishedPack{
		{PackID: packOne, Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 10, BlobCount: 1, Lifecycle: schema.PackExportPending}, Blobs: map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packOne, Length: 10, Type: schema.BlobData}}}}},
		{PackID: packTwo, Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 10, BlobCount: 1, Lifecycle: schema.PackExportPending}, Blobs: map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packTwo, Length: 10, Type: schema.BlobData}}}}},
	} {
		if err := store.PublishPack(context.Background(), published); err != nil {
			t.Fatal(err)
		}
	}
	value, found, err := store.Get(context.Background(), schema.BlobKey(blobID))
	if err != nil || !found {
		t.Fatalf("blob record: found=%t err=%v", found, err)
	}
	record, err := schema.UnmarshalBlobRecord(value)
	if err != nil || len(record.Locations) != 2 {
		t.Fatalf("blob locations = %#v, err=%v", record.Locations, err)
	}
	aggregateValue, found, err := store.Get(context.Background(), schema.PackAggregateKey(schema.AggregateAll))
	if err != nil || !found {
		t.Fatalf("aggregate: found=%t err=%v", found, err)
	}
	aggregate, err := schema.UnmarshalPackAggregate(aggregateValue)
	if err != nil || aggregate.PackCount != 2 || aggregate.BlobCount != 2 || aggregate.PayloadSize != 20 {
		t.Fatalf("aggregate = %#v, err=%v", aggregate, err)
	}
}

func TestSchemaStoreTwoPhasePackDeletion(t *testing.T) {
	client, err := Ensure(context.Background(), Options{Socket: testSocket(t), RepositoryID: "phase8-gc-delete", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	blobShared, blobOnlyB, packA, packB := daemonTestID(40), daemonTestID(41), daemonTestID(42), daemonTestID(43)
	for _, published := range []PublishedPack{
		{PackID: packA, Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 5, BlobCount: 1, Lifecycle: schema.PackExportPending},
			Blobs: map[schema.ID]schema.BlobRecord{blobShared: {Locations: []schema.BlobLocation{{PackID: packA, Length: 5, Type: schema.BlobData}}}}},
		{PackID: packB, Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 12, BlobCount: 2, Lifecycle: schema.PackExportPending},
			Blobs: map[schema.ID]schema.BlobRecord{
				blobShared: {Locations: []schema.BlobLocation{{PackID: packB, Offset: 7, Length: 5, Type: schema.BlobData}}},
				blobOnlyB:  {Locations: []schema.BlobLocation{{PackID: packB, Length: 7, Type: schema.BlobData}}},
			}},
	} {
		if err := store.PublishPack(ctx, published); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkPackPublished(ctx, packA); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPackPublished(ctx, packB); err != nil {
		t.Fatal(err)
	}

	// A pack that has not been published cannot enter delete-pending.
	if err := store.MarkPackDeletePending(ctx, blobOnlyB); err == nil {
		t.Fatal("delete-pending accepted a nonexistent pack")
	}
	if err := store.MarkPackDeleted(ctx, packB, nil); err == nil {
		t.Fatal("deletion accepted a pack that is not delete-pending")
	}

	if err := store.MarkPackDeletePending(ctx, packB); err != nil {
		t.Fatal(err)
	}
	// Idempotent: repeating delete-pending on an already delete-pending pack is a no-op.
	if err := store.MarkPackDeletePending(ctx, packB); err != nil {
		t.Fatalf("repeated delete-pending: %v", err)
	}
	pendingValue, found, err := store.Get(ctx, schema.PackKey(packB))
	if err != nil || !found {
		t.Fatalf("delete-pending pack: found=%t err=%v", found, err)
	}
	pendingRecord, err := schema.UnmarshalPackRecord(pendingValue)
	if err != nil || pendingRecord.Lifecycle != schema.PackDeletePending {
		t.Fatalf("delete-pending lifecycle = %#v, err=%v", pendingRecord, err)
	}

	// Seed stale GC bookkeeping that deletion must clean up.
	gcValue, err := (schema.GarbageCollectionRecord{State: schema.GCRevalidated, ObservedCommit: 1}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteMutableBatch(ctx, []Mutation{
		{Key: schema.GarbageCollectionKey(schema.GCPack, packB), Value: gcValue},
		{Key: schema.GarbageCollectionKey(schema.GCBlob, blobOnlyB), Value: gcValue},
	}, nil, true); err != nil {
		t.Fatal(err)
	}

	if err := store.MarkPackDeleted(ctx, packB, []schema.ID{blobShared, blobOnlyB}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(ctx, schema.PackKey(packB)); err != nil || found {
		t.Fatalf("deleted pack still present: found=%t err=%v", found, err)
	}
	if _, found, err := store.Get(ctx, schema.BlobKey(blobOnlyB)); err != nil || found {
		t.Fatalf("blob unique to deleted pack still present: found=%t err=%v", found, err)
	}
	sharedValue, found, err := store.Get(ctx, schema.BlobKey(blobShared))
	if err != nil || !found {
		t.Fatalf("shared blob missing: found=%t err=%v", found, err)
	}
	sharedRecord, err := schema.UnmarshalBlobRecord(sharedValue)
	if err != nil || len(sharedRecord.Locations) != 1 || sharedRecord.Locations[0].PackID != packA {
		t.Fatalf("shared blob locations = %#v, err=%v", sharedRecord.Locations, err)
	}
	if _, found, err := store.Get(ctx, schema.GarbageCollectionKey(schema.GCPack, packB)); err != nil || found {
		t.Fatalf("stale pack GC record survived deletion: found=%t err=%v", found, err)
	}
	if _, found, err := store.Get(ctx, schema.GarbageCollectionKey(schema.GCBlob, blobOnlyB)); err != nil || found {
		t.Fatalf("stale blob GC record survived deletion: found=%t err=%v", found, err)
	}
	aggregateValue, found, err := store.Get(ctx, schema.PackAggregateKey(schema.AggregateAll))
	if err != nil || !found {
		t.Fatalf("aggregate: found=%t err=%v", found, err)
	}
	aggregate, err := schema.UnmarshalPackAggregate(aggregateValue)
	if err != nil || aggregate.PackCount != 1 || aggregate.BlobCount != 1 || aggregate.PayloadSize != 5 {
		t.Fatalf("aggregate after deletion = %#v, err=%v", aggregate, err)
	}

	// Re-deleting the same (now absent) pack fails closed rather than silently succeeding.
	if err := store.MarkPackDeleted(ctx, packB, nil); err == nil {
		t.Fatal("deletion accepted an already-deleted pack")
	}
}

func TestSchemaStoreCompletesSnapshotExportAtomically(t *testing.T) {
	client, err := Ensure(context.Background(), Options{Socket: testSocket(t), RepositoryID: "phase6-snapshot", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	revision, err := store.AllocateRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rootKey := schema.DirectoryRevisionKey(0, 0, revision)
	rootValue := encodeSchemaRecord(t, schema.DirectoryRevision{Children: []schema.DirectoryChild{{Name: "file", Inode: 2, Type: schema.NodeFile, MetadataKey: schema.InodeRevisionKey(1, 2, 1)}}, SourcePath: "/", Known: schema.KnownPath, Freshness: schema.FreshnessVerified})
	if err := store.PublishRevision(context.Background(), schema.CurrentDirectoryKey(0, 0), rootKey, rootValue, revision); err != nil {
		t.Fatal(err)
	}
	snapshotID := daemonTestID(41)
	if err := store.MarkExportPending(context.Background(), snapshotID, rootKey); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishSnapshotScope(context.Background(), SnapshotScope{SnapshotID: snapshotID, RootKey: rootKey, OriginalJSON: []byte(`{"time":"2026-08-29T12:34:56Z","tree":"test"}`)}); err != nil {
		t.Fatal(err)
	}
	checkpointValue, found, err := store.Get(context.Background(), schema.ExportCheckpointKey(snapshotID))
	if err != nil || !found {
		t.Fatalf("checkpoint: found=%t err=%v", found, err)
	}
	checkpoint, err := schema.UnmarshalExportCheckpointRecord(checkpointValue)
	if err != nil || checkpoint.State != schema.ExportComplete || checkpoint.CommitSequence == 0 || !bytes.Equal(checkpoint.RootKey, rootKey) {
		t.Fatalf("checkpoint = %#v, err=%v", checkpoint, err)
	}
	if _, found, err := store.Get(context.Background(), schema.SnapshotKey(snapshotID)); err != nil || !found {
		t.Fatalf("snapshot scope: found=%t err=%v", found, err)
	}
	commitValue, found, err := store.Get(context.Background(), schema.SnapshotCommitKey(checkpoint.CommitSequence, snapshotID))
	if err != nil || !found {
		t.Fatalf("snapshot commit index: found=%t err=%v", found, err)
	}
	commit, err := schema.UnmarshalSnapshotCommitRecord(commitValue)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(commit.RootKey, rootKey) || commit.SnapshotTimeUnixNano == 0 {
		t.Fatalf("snapshot commit record = %#v", commit)
	}
}

func TestSchemaStoreSnapshotMembershipDeltasPublishAndForget(t *testing.T) {
	client, err := Ensure(context.Background(), Options{Socket: testSocket(t), RepositoryID: "phase16-snapshot-membership", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	metadata := schema.AnalyticsMetadataRecord{Enabled: true, Generation: 1, BuiltAt: time.Now().UnixNano(), ConfigJSON: "{}"}
	if err := store.Put(ctx, schema.AnalyticsMetadataKey(), encodeSchemaRecord(t, metadata), true); err != nil {
		t.Fatal(err)
	}
	inodeRevision, err := store.AllocateRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	inodeKey := schema.InodeRevisionKey(1, 2, inodeRevision)
	inode := schema.InodeRevision{Known: schema.KnownPath, SourcePath: "/file", Freshness: schema.FreshnessVerified}
	if err := store.PublishRevision(ctx, schema.CurrentInodeKey(1, 2), inodeKey, encodeSchemaRecord(t, inode), inodeRevision); err != nil {
		t.Fatal(err)
	}
	rootRevision, err := store.AllocateRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rootKey := schema.DirectoryRevisionKey(1, 1, rootRevision)
	root := schema.DirectoryRevision{Known: schema.KnownPath, SourcePath: "/", Freshness: schema.FreshnessVerified, Children: []schema.DirectoryChild{{Name: "file", Inode: 2, Type: schema.NodeFile, MetadataKey: inodeKey}}}
	if err := store.PublishRevision(ctx, schema.CurrentDirectoryKey(1, 1), rootKey, encodeSchemaRecord(t, root), rootRevision); err != nil {
		t.Fatal(err)
	}
	snapshotID := daemonTestID(42)
	if err := store.MarkExportPending(ctx, snapshotID, rootKey); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishSnapshotScope(ctx, SnapshotScope{SnapshotID: snapshotID, RootKey: rootKey, OriginalJSON: []byte(`{"time":"2026-08-30T12:00:00Z"}`)}); err != nil {
		t.Fatal(err)
	}
	snapshotValue, found, err := store.Get(ctx, schema.SnapshotKey(snapshotID))
	if err != nil || !found {
		t.Fatalf("published snapshot: found=%t err=%v", found, err)
	}
	snapshot, err := schema.UnmarshalSnapshotRecord(snapshotValue)
	if err != nil {
		t.Fatal(err)
	}
	deltaValue, found, err := store.Get(ctx, schema.AnalyticsDeltaKey(snapshot.CommitSequence, 0))
	if err != nil || !found {
		t.Fatalf("snapshot publish delta: found=%t err=%v", found, err)
	}
	delta, err := schema.UnmarshalAnalyticsDeltaRecord(deltaValue)
	if err != nil || delta.Kind != schema.AnalyticsDeltaRetainedReferences || delta.IdentityGeneration != inodeRevision || delta.RetainedSnapshotRefs != 1 {
		t.Fatalf("snapshot publish delta = %#v, err=%v", delta, err)
	}
	if err := store.ForgetSnapshot(ctx, snapshotID); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(ctx, schema.SnapshotKey(snapshotID)); err != nil || found {
		t.Fatalf("forgotten snapshot remains: found=%t err=%v", found, err)
	}
	forgetDeltaValue, found, err := store.Get(ctx, schema.AnalyticsDeltaKey(snapshot.CommitSequence+1, 0))
	if err != nil || !found {
		t.Fatalf("snapshot forget delta: found=%t err=%v", found, err)
	}
	forgetDelta, err := schema.UnmarshalAnalyticsDeltaRecord(forgetDeltaValue)
	if err != nil || forgetDelta.RetainedSnapshotRefs != 0 || forgetDelta.IdentityGeneration != inodeRevision {
		t.Fatalf("snapshot forget delta = %#v, err=%v", forgetDelta, err)
	}
}

func TestAuthoritativeCrawlProofControlsAbsenceAndIdentityGeneration(t *testing.T) {
	client, err := Ensure(context.Background(), Options{Socket: testSocket(t), RepositoryID: "phase16-crawl-proof", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	metadata := schema.AnalyticsMetadataRecord{Enabled: true, Generation: 1, BuiltAt: time.Now().UnixNano(), ConfigJSON: "{}"}
	if err := store.Put(ctx, schema.AnalyticsMetadataKey(), encodeSchemaRecord(t, metadata), true); err != nil {
		t.Fatal(err)
	}
	publishInode := func(fsid uint32, inode uint64, path string) uint64 {
		t.Helper()
		revision, err := store.AllocateRevision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		record := schema.InodeRevision{Known: schema.KnownPath, SourcePath: path, Freshness: schema.FreshnessVerified}
		if err := store.PublishRevision(ctx, schema.CurrentInodeKey(fsid, inode), schema.InodeRevisionKey(fsid, inode, revision), encodeSchemaRecord(t, record), revision); err != nil {
			t.Fatal(err)
		}
		return revision
	}
	publishCrawl := func(snapshotByte byte, scope schema.ID, inode, inodeRevision, startFence uint64, complete bool, debtKeys [][]byte) uint64 {
		t.Helper()
		rootRevision, err := store.AllocateRevision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rootKey := schema.DirectoryRevisionKey(1, uint64(snapshotByte)+100, rootRevision)
		root := schema.DirectoryRevision{Known: schema.KnownPath, SourcePath: "/", Freshness: schema.FreshnessVerified}
		if inodeRevision != 0 {
			root.Children = []schema.DirectoryChild{{Name: "file", Inode: inode, Type: schema.NodeFile, MetadataKey: schema.InodeRevisionKey(1, inode, inodeRevision)}}
		}
		if err := store.PublishRevision(ctx, schema.CurrentDirectoryKey(1, uint64(snapshotByte)+100), rootKey, encodeSchemaRecord(t, root), rootRevision); err != nil {
			t.Fatal(err)
		}
		snapshotID := daemonTestID(snapshotByte)
		if err := store.MarkExportPending(ctx, snapshotID, rootKey); err != nil {
			t.Fatal(err)
		}
		claim := &AuthoritativeCrawlClaim{ScopeID: scope, RootFSID: 1, RootInode: uint64(snapshotByte) + 100, StartFence: startFence, Complete: complete, DebtKeys: debtKeys}
		if err := store.PublishSnapshotScope(ctx, SnapshotScope{SnapshotID: snapshotID, RootKey: rootKey, OriginalJSON: []byte(`{"time":"2026-08-30T12:00:00Z"}`), Crawl: claim}); err != nil {
			t.Fatal(err)
		}
		value, found, err := store.Get(ctx, schema.SnapshotKey(snapshotID))
		if err != nil || !found {
			t.Fatalf("snapshot commit: found=%t err=%v", found, err)
		}
		snapshot, err := schema.UnmarshalSnapshotRecord(value)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot.CommitSequence
	}
	readBinding := func(scope schema.ID, inode, generation uint64) schema.AuthoritativeSourceBindingRecord {
		t.Helper()
		value, found, err := store.Get(ctx, schema.AuthoritativeSourceBindingKey(scope, 1, inode, generation))
		if err != nil || !found {
			t.Fatalf("binding %d:%d: found=%t err=%v", inode, generation, found, err)
		}
		record, err := schema.UnmarshalAuthoritativeSourceBindingRecord(value)
		if err != nil {
			t.Fatal(err)
		}
		return record
	}

	scopeA, scopeB := daemonTestID(201), daemonTestID(202)
	first := publishInode(1, 2, "/file")
	firstA := publishCrawl(101, scopeA, 2, first, 1, true, nil)
	publishCrawl(102, scopeB, 2, first, 1, true, nil)
	deletedCommit := publishCrawl(103, scopeA, 0, 0, firstA, true, nil)
	deleted := readBinding(scopeA, 2, first)
	if deleted.State != schema.AuthoritativeSourceDeleted || deleted.Continuity != schema.AnalyticsContinuityProven {
		t.Fatalf("complete absence binding = %#v", deleted)
	}
	if isolated := readBinding(scopeB, 2, first); isolated.State != schema.AuthoritativeSourceLive {
		t.Fatalf("scope B was changed by scope A proof: %#v", isolated)
	}
	proofValue, found, err := store.Get(ctx, schema.AuthoritativeCrawlProofKey(scopeA, deletedCommit))
	if err != nil || !found {
		t.Fatalf("crawl proof: found=%t err=%v", found, err)
	}
	proof, err := schema.UnmarshalAuthoritativeCrawlProofRecord(proofValue)
	if err != nil || !proof.Complete || !proof.DebtFree {
		t.Fatalf("crawl proof = %#v, err=%v", proof, err)
	}
	reappeared := publishInode(1, 2, "/replacement")
	publishCrawl(104, scopeA, 2, reappeared, deletedCommit, true, nil)
	if current := readBinding(scopeA, 2, reappeared); current.State != schema.AuthoritativeSourceLive || current.Continuity != schema.AnalyticsContinuityProven || current.Generation == first {
		t.Fatalf("proven reappearance binding = %#v", current)
	}
	if old := readBinding(scopeA, 2, first); old.State != schema.AuthoritativeSourceDeleted {
		t.Fatalf("old generation was overwritten: %#v", old)
	}
	nextValue, found, err := store.Get(ctx, schema.NextRevisionKey())
	if err != nil || !found {
		t.Fatalf("next revision before forget: found=%t err=%v", found, err)
	}
	forgetCommit, err := schema.UnmarshalNextRevision(nextValue)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ForgetSnapshot(ctx, daemonTestID(104)); err != nil {
		t.Fatal(err)
	}
	forgetValue, found, err := store.Get(ctx, schema.AnalyticsDeltaKey(forgetCommit, 0))
	if err != nil || !found {
		t.Fatalf("forget delta: found=%t err=%v", found, err)
	}
	forgetDelta, err := schema.UnmarshalAnalyticsDeltaRecord(forgetValue)
	if err != nil || forgetDelta.IdentityGeneration != reappeared || forgetDelta.RetainedSnapshotRefs != 0 {
		t.Fatalf("reappeared forget delta = %#v, err=%v", forgetDelta, err)
	}

	scopeGap := daemonTestID(203)
	gapFirst := publishInode(1, 3, "/gap")
	gapObserved := publishCrawl(105, scopeGap, 3, gapFirst, 1, true, nil)
	publishCrawl(106, scopeGap, 0, 0, gapObserved, false, nil)
	if uncertain := readBinding(scopeGap, 3, gapFirst); uncertain.State != schema.AuthoritativeSourceUnknown || uncertain.Continuity != schema.AnalyticsContinuityUnknown {
		t.Fatalf("incomplete absence binding = %#v", uncertain)
	}
	gapReappeared := publishInode(1, 3, "/gap-replacement")
	publishCrawl(107, scopeGap, 3, gapReappeared, gapObserved, false, nil)
	if uncertain := readBinding(scopeGap, 3, gapReappeared); uncertain.State != schema.AuthoritativeSourceLive || uncertain.Continuity != schema.AnalyticsContinuityUnknown || uncertain.Generation == gapFirst {
		t.Fatalf("gap reappearance binding = %#v", uncertain)
	}
	if old := readBinding(scopeGap, 3, gapFirst); old.State != schema.AuthoritativeSourceUnknown {
		t.Fatalf("gap generation was merged: %#v", old)
	}

	scopeDebt := daemonTestID(204)
	debtFirst := publishInode(1, 4, "/debt")
	debtObserved := publishCrawl(108, scopeDebt, 4, debtFirst, 1, true, nil)
	debtKey := schema.CrawlDebtKey(schema.ID{}, daemonTestID(205))
	debt := schema.CrawlDebtRecord{PathOrTree: []byte("debt"), Reason: schema.DebtMissingInode, Status: schema.DebtPending}
	if err := store.Put(ctx, debtKey, encodeSchemaRecord(t, debt), true); err != nil {
		t.Fatal(err)
	}
	debtCommit := publishCrawl(109, scopeDebt, 0, 0, debtObserved, true, [][]byte{debtKey})
	if uncertain := readBinding(scopeDebt, 4, debtFirst); uncertain.State != schema.AuthoritativeSourceUnknown {
		t.Fatalf("debt-bearing absence proved deletion: %#v", uncertain)
	}
	proofValue, _, _ = store.Get(ctx, schema.AuthoritativeCrawlProofKey(scopeDebt, debtCommit))
	proof, err = schema.UnmarshalAuthoritativeCrawlProofRecord(proofValue)
	if err != nil || !proof.Complete || proof.DebtFree {
		t.Fatalf("debt-bearing proof = %#v, err=%v", proof, err)
	}

	scopeFence := daemonTestID(206)
	fenceFirst := publishInode(1, 5, "/fenced")
	fenceObserved := publishCrawl(110, scopeFence, 5, fenceFirst, 1, true, nil)
	publishCrawl(111, scopeFence, 0, 0, fenceObserved-1, true, nil)
	if fenced := readBinding(scopeFence, 5, fenceFirst); fenced.State != schema.AuthoritativeSourceUnknown {
		t.Fatalf("stale start fence proved deletion: %#v", fenced)
	}
}

func TestSchemaStoreImportsLegacyPacksIdempotently(t *testing.T) {
	options := Options{Socket: testSocket(t), RepositoryID: "phase4-pack-import", DaemonPath: daemonBinary(t), DataDir: t.TempDir()}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	source1, source2 := daemonTestID(1), daemonTestID(2)
	pack1, pack2, blobID := daemonTestID(3), daemonTestID(4), daemonTestID(5)
	location := func(packID schema.ID, offset uint64) map[schema.ID]schema.BlobRecord {
		return map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packID, Offset: offset, Length: 8, UncompressedSize: 7, Type: schema.BlobData}}}}
	}
	debtKey := schema.CrawlDebtKey(schema.ID{}, pack1)
	debt := schema.CrawlDebtRecord{SourceIndexOrPack: pack1, SourceKnown: true, Reason: schema.DebtUnavailablePack, Status: schema.DebtPending, ErrorClass: "offline"}
	first := LegacyPackImport{
		SourceIndex: source1, PackID: pack1,
		Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 8, BlobCount: 1, Lifecycle: schema.PackImported},
		Blobs:  location(pack1, 1), DebtKey: debtKey, Debt: &debt,
	}
	if err := store.ImportLegacyPack(ctx, first); err != nil {
		t.Fatal(err)
	}
	first.Record.PhysicalSize, first.Record.HeaderSize, first.Record.PhysicalSizeKnown, first.Debt = 10, 2, true, nil
	if err := store.ImportLegacyPack(ctx, first); err != nil {
		t.Fatal(err)
	}
	first.SourceIndex = source2
	if err := store.ImportLegacyPack(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := LegacyPackImport{
		SourceIndex: source2, PackID: pack2,
		Record: schema.PackRecord{Type: schema.PackData, PhysicalSize: 12, PhysicalSizeKnown: true, PayloadSize: 8, HeaderSize: 4, BlobCount: 1, Lifecycle: schema.PackImported},
		Blobs:  location(pack2, 2),
	}
	if err := store.ImportLegacyPack(ctx, second); err != nil {
		t.Fatal(err)
	}

	blobValue, found, err := store.Get(ctx, schema.BlobKey(blobID))
	if err != nil || !found {
		t.Fatalf("read imported blob: found=%t err=%v", found, err)
	}
	blob, err := schema.UnmarshalBlobRecord(blobValue)
	if err != nil || len(blob.Locations) != 2 || blob.Locations[0].PackID != pack1 || blob.Locations[1].PackID != pack2 {
		t.Fatalf("imported blob locations = %#v, err=%v", blob.Locations, err)
	}
	packValue, found, err := store.Get(ctx, schema.PackKey(pack1))
	if err != nil || !found {
		t.Fatalf("read imported pack: found=%t err=%v", found, err)
	}
	packRecord, err := schema.UnmarshalPackRecord(packValue)
	if err != nil || len(packRecord.SourceIndexIDs) != 2 || packRecord.SourceIndexIDs[0] != source1 || packRecord.SourceIndexIDs[1] != source2 || packRecord.PhysicalSize != 10 {
		t.Fatalf("imported pack = %#v, err=%v", packRecord, err)
	}
	if err := store.MarkPackPublished(ctx, pack1); err != nil {
		t.Fatalf("mark imported pack published: %v", err)
	}
	packValue, found, err = store.Get(ctx, schema.PackKey(pack1))
	if err != nil || !found {
		t.Fatalf("read published pack: found=%t err=%v", found, err)
	}
	packRecord, err = schema.UnmarshalPackRecord(packValue)
	if err != nil || packRecord.Lifecycle != schema.PackPublished || len(packRecord.SourceIndexIDs) != 2 || packRecord.PhysicalSize != 10 {
		t.Fatalf("published imported pack = %#v, err=%v", packRecord, err)
	}
	aggregateValue, found, err := store.Get(ctx, schema.PackAggregateKey(schema.AggregateAll))
	if err != nil || !found {
		t.Fatalf("read aggregate: found=%t err=%v", found, err)
	}
	aggregate, err := schema.UnmarshalPackAggregate(aggregateValue)
	if err != nil || aggregate.PackCount != 2 || aggregate.PhysicalSize != 22 || aggregate.PayloadSize != 16 || aggregate.HeaderSize != 6 || aggregate.BlobCount != 2 {
		t.Fatalf("imported aggregate = %#v, err=%v", aggregate, err)
	}
	debtValue, found, err := store.Get(ctx, debtKey)
	if err != nil || !found {
		t.Fatalf("read pack debt: found=%t err=%v", found, err)
	}
	resolvedDebt, err := schema.UnmarshalCrawlDebtRecord(debtValue)
	if err != nil || resolvedDebt.Status != schema.DebtResolved || resolvedDebt.ErrorClass != "" {
		t.Fatalf("resolved pack debt = %#v, err=%v", resolvedDebt, err)
	}
}

func TestSchemaStoreImportsSameLegacyPackConcurrently(t *testing.T) {
	options := Options{Socket: testSocket(t), RepositoryID: "phase4-concurrent-pack", DaemonPath: daemonBinary(t), DataDir: t.TempDir()}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	packID := daemonTestID(20)
	imports := []LegacyPackImport{
		{
			SourceIndex: daemonTestID(21), PackID: packID,
			Record: schema.PackRecord{Type: schema.PackData, PhysicalSize: 20, PhysicalSizeKnown: true, PayloadSize: 8, HeaderSize: 12, BlobCount: 1, Lifecycle: schema.PackImported},
			Blobs:  map[schema.ID]schema.BlobRecord{daemonTestID(22): {Locations: []schema.BlobLocation{{PackID: packID, Offset: 0, Length: 8, Type: schema.BlobData}}}},
		},
		{
			SourceIndex: daemonTestID(23), PackID: packID,
			Record: schema.PackRecord{Type: schema.PackData, PhysicalSize: 20, PhysicalSizeKnown: true, PayloadSize: 8, HeaderSize: 12, BlobCount: 1, Lifecycle: schema.PackImported},
			Blobs:  map[schema.ID]schema.BlobRecord{daemonTestID(24): {Locations: []schema.BlobLocation{{PackID: packID, Offset: 8, Length: 8, Type: schema.BlobData}}}},
		},
	}
	errors := make([]error, len(imports))
	var group sync.WaitGroup
	for index := range imports {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			errors[index] = store.ImportLegacyPack(context.Background(), imports[index])
		}(index)
	}
	group.Wait()
	for _, err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	packValue, found, err := store.Get(context.Background(), schema.PackKey(packID))
	if err != nil || !found {
		t.Fatalf("read pack: found=%t err=%v", found, err)
	}
	record, err := schema.UnmarshalPackRecord(packValue)
	if err != nil || record.BlobCount != 2 || record.PayloadSize != 16 || record.HeaderSize != 4 || len(record.SourceIndexIDs) != 2 {
		t.Fatalf("concurrent pack record = %#v, err=%v", record, err)
	}
	aggregateValue, found, err := store.Get(context.Background(), schema.PackAggregateKey(schema.AggregateAll))
	if err != nil || !found {
		t.Fatalf("read aggregate: found=%t err=%v", found, err)
	}
	aggregate, err := schema.UnmarshalPackAggregate(aggregateValue)
	if err != nil || aggregate.PackCount != 1 || aggregate.PayloadSize != 16 || aggregate.BlobCount != 2 || aggregate.PhysicalSize != 20 {
		t.Fatalf("concurrent aggregate = %#v, err=%v", aggregate, err)
	}
}

func TestSchemaStoreImportsLargeLegacyPackAcrossBatches(t *testing.T) {
	options := Options{Socket: testSocket(t), RepositoryID: "phase7-large-pack", DaemonPath: daemonBinary(t), DataDir: t.TempDir()}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	packID := daemonTestID(100)
	blobs := make(map[schema.ID]schema.BlobRecord, 73)
	for index := range 73 {
		blobID := daemonTestID(byte(index + 101))
		blobs[blobID] = schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: packID, Offset: uint64(index), Length: 1, Type: schema.BlobData}}}
	}
	imported := LegacyPackImport{
		SourceIndex: daemonTestID(99), PackID: packID,
		Record: schema.PackRecord{Type: schema.PackData, PhysicalSize: 73, PhysicalSizeKnown: true, PayloadSize: 73, BlobCount: 73, Lifecycle: schema.PackImported},
		Blobs:  blobs, BatchSize: 1,
	}
	if err := store.ImportLegacyPack(context.Background(), imported); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(context.Background(), schema.PackKey(packID)); err != nil || !found {
		t.Fatalf("read pack after batched import: found=%t err=%v", found, err)
	}
	for blobID := range blobs {
		if _, found, err := store.Get(context.Background(), schema.BlobKey(blobID)); err != nil || !found {
			t.Fatalf("read blob %x after batched import: found=%t err=%v", blobID, found, err)
		}
	}
}

func TestManifestBatchingRespectsNegotiatedLimits(t *testing.T) {
	keys := make([][]byte, 200)
	values := make([][]byte, len(keys))
	for index := range keys {
		keys[index] = make([]byte, 39)
		values[index] = make([]byte, 128*1024)
	}
	limits := Limits{MaxBatchItems: 10_000, MaxMessageBytes: 16 * 1024 * 1024}
	batches := 0
	for start := 0; start < len(keys); batches++ {
		end, err := manifestBatchEnd(limits, keys, values, start)
		if err != nil {
			t.Fatal(err)
		}
		if end <= start || end > len(keys) {
			t.Fatalf("manifest batch %d made invalid progress %d -> %d", batches, start, end)
		}
		start = end
	}
	if batches < 2 {
		t.Fatalf("expected multiple manifest batches, got %d", batches)
	}
}

func TestS3CompatibleStorageRoundTrip(t *testing.T) {
	if os.Getenv("VAULTICDB_TEST_S3_ENDPOINT") == "" {
		t.Skip("VAULTICDB_TEST_S3_ENDPOINT is not configured")
	}
	options := Options{
		Socket: testSocket(t), RepositoryID: "phase3-s3", DaemonPath: daemonBinary(t),
		ObjectStore: "s3", S3Bucket: os.Getenv("VAULTICDB_TEST_S3_BUCKET"), S3Prefix: "phase3/roundtrip",
	}
	if options.S3Bucket == "" {
		options.S3Bucket = "vaulticdb-phase3"
	}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	durable, err := client.WriteBatch(ctx, []Mutation{{Key: []byte("s3:key"), Value: []byte("s3-value")}}, nil, true, "")
	if err != nil || !durable {
		t.Fatalf("S3 write = %t, %v", durable, err)
	}
	value, found, err := client.Get(ctx, []byte("s3:key"), "")
	if err != nil || !found || string(value) != "s3-value" {
		t.Fatalf("S3 read = %q, %t, %v", value, found, err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	options.Socket = testSocket(t)
	options.RepositoryID = "phase3-s3-other"
	client, err = Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	if value, found, err := client.Get(ctx, []byte("s3:key"), ""); err != nil || found {
		t.Fatalf("S3 repository isolation read = %q, %t, %v", value, found, err)
	}
}

type schemaRecord interface{ MarshalBinary() ([]byte, error) }

func daemonTestID(value byte) schema.ID { var id schema.ID; id[0], id[31] = value, value; return id }
func encodeSchemaRecord(t *testing.T, record schemaRecord) []byte {
	t.Helper()
	encoded, err := record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func mustNextRevision(t *testing.T, next uint64) []byte {
	t.Helper()
	encoded, err := schema.MarshalNextRevision(next)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestEverySchemaRecordPersistsByteForByteAcrossRestart(t *testing.T) {
	options := Options{Socket: testSocket(t), RepositoryID: "phase3-all-records", DaemonPath: daemonBinary(t), DataDir: t.TempDir()}
	ctx := context.Background()
	id1, id2, id3 := daemonTestID(1), daemonTestID(2), daemonTestID(3)
	inodeKey := schema.InodeRevisionKey(1, 2, 1)
	directoryKey := schema.DirectoryRevisionKey(1, 1, 1)
	inodePointer, _ := (schema.CurrentPointer{Revision: 1, RecordKey: inodeKey}).MarshalBinary()
	directoryPointer, _ := (schema.CurrentPointer{Revision: 1, RecordKey: directoryKey}).MarshalBinary()
	nextRevision, _ := schema.MarshalNextRevision(2)
	indexCheckpoint := encodeSchemaRecord(t, schema.ImportCheckpointRecord{PacksImported: 1, BlobsImported: 2})
	snapshotCheckpoint := encodeSchemaRecord(t, schema.SnapshotImportCheckpointRecord{TreesVisited: 1, NodesImported: 2, DebtsCreated: 3})
	records := []Mutation{
		{Key: schema.BlobKey(id1), Value: encodeSchemaRecord(t, schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: id2, Offset: 1, Length: 2, UncompressedSize: 3, Type: schema.BlobData}}})},
		{Key: schema.PackKey(id2), Value: encodeSchemaRecord(t, schema.PackRecord{Type: schema.PackData, PhysicalSize: 3, PhysicalSizeKnown: true, PayloadSize: 2, HeaderSize: 1, BlobCount: 1, Lifecycle: schema.PackPublished})},
		{Key: schema.PackAggregateKey(schema.AggregateAll), Value: encodeSchemaRecord(t, schema.PackAggregate{PackCount: 1, PhysicalSize: 3, PayloadSize: 2, HeaderSize: 1, BlobCount: 1, UpdateSequence: 1})},
		{Key: schema.CurrentInodeKey(1, 2), Value: inodePointer},
		{Key: inodeKey, Value: encodeSchemaRecord(t, schema.InodeRevision{ParentInode: 1, Known: schema.KnownParent, ContentMode: schema.ContentInline, ContentIDs: []schema.ID{id1}, ContentCount: 1, Freshness: schema.FreshnessVerified})},
		{Key: schema.CurrentDirectoryKey(1, 1), Value: directoryPointer},
		{Key: directoryKey, Value: encodeSchemaRecord(t, schema.DirectoryRevision{Children: []schema.DirectoryChild{{Name: "file", Inode: 2, Type: schema.NodeFile, MetadataKey: inodeKey}}})},
		{Key: schema.SnapshotKey(id3), Value: encodeSchemaRecord(t, schema.SnapshotRecord{CommitSequence: 1, RootFSID: 1, RootInode: 1, RootRevision: 1, OriginalJSON: []byte("{}")})},
		{Key: schema.ContentManifestKey(id3, 0), Value: encodeSchemaRecord(t, schema.ContentManifest{TotalCount: 1, SegmentCount: 1, ContentIDs: []schema.ID{id1}})},
		{Key: schema.ReverseManifestKey(id1, id3), Value: encodeSchemaRecord(t, schema.ReverseManifestRecord{State: schema.ReferenceCurrent})},
		{Key: schema.ReverseInodeKey(id1, 1, 2), Value: encodeSchemaRecord(t, schema.ReverseInodeRecord{LatestRevision: 1, State: schema.ReferenceCurrent})},
		{Key: schema.ReferenceCountKey(id1), Value: encodeSchemaRecord(t, schema.ReferenceCountRecord{TotalReferences: 2, DistinctInodes: 1, DistinctRevisions: 1, DistinctManifests: 1, ReachableSnapshots: 1, UpdateSequence: 1})},
		{Key: schema.GarbageCollectionKey(schema.GCBlob, id1), Value: encodeSchemaRecord(t, schema.GarbageCollectionRecord{State: schema.GCCandidate, ObservedCommit: 1})},
		{Key: schema.CrawlDebtKey(id3, id2), Value: encodeSchemaRecord(t, schema.CrawlDebtRecord{PathOrTree: []byte("tree"), Reason: schema.DebtMissingDirectory, Status: schema.DebtPending})},
		{Key: schema.ImportCheckpointKey(id1), Value: indexCheckpoint},
		{Key: schema.SnapshotImportCheckpointKey(id3), Value: snapshotCheckpoint},
		{Key: schema.NextRevisionKey(), Value: nextRevision},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := client.WriteBatch(ctx, records, nil, true, "")
	if err != nil || !durable {
		t.Fatalf("write all records = %t, %v", durable, err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	client, err = Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	keys := make([][]byte, len(records))
	for index := range records {
		keys[index] = records[index].Key
	}
	values, found, err := client.MultiGet(ctx, keys, "")
	if err != nil {
		t.Fatal(err)
	}
	for index := range records {
		if !found[index] || !bytes.Equal(values[index].Value, records[index].Value) {
			t.Fatalf("record %d changed across restart", index)
		}
	}
}

type testService struct {
	vaulticdbv1.UnimplementedVaulticDBServer
	protocol      string
	schema        string
	repo          string
	blockShutdown bool
	corruptKeys   bool
}

func testSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "vd-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "d.sock")
}

func daemonBinary(t *testing.T) string {
	t.Helper()
	if binary := os.Getenv("VAULTICDB_TEST_BINARY"); binary != "" {
		return binary
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	binary := filepath.Join(filepath.Dir(source), "..", "..", "..", "vaulticdb", "target", "debug", "vaulticdb")
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	return binary
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func (s testService) Health(_ context.Context, request *vaulticdbv1.HealthRequest) (*vaulticdbv1.HealthResponse, error) {
	return &vaulticdbv1.HealthResponse{Ready: true, ProtocolVersion: s.protocol, SchemaVersion: s.schema, RepositoryId: request.GetRepositoryId()}, nil
}

func (s testService) Capabilities(_ context.Context, request *vaulticdbv1.CapabilitiesRequest) (*vaulticdbv1.CapabilitiesResponse, error) {
	return &vaulticdbv1.CapabilitiesResponse{
		ProtocolVersion: s.protocol, SchemaVersion: s.schema, RepositoryId: request.GetRepositoryId(),
		MaxBatchItems: 10_000, MaxMessageBytes: 16 * 1024 * 1024, MaxPageItems: 1_000,
	}, nil
}

func (s testService) Shutdown(ctx context.Context, _ *vaulticdbv1.Empty) (*vaulticdbv1.Empty, error) {
	if s.blockShutdown {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &vaulticdbv1.Empty{}, nil
}

func (s testService) Get(_ context.Context, request *vaulticdbv1.GetRequest) (*vaulticdbv1.GetResponse, error) {
	key := request.GetKey()
	if s.corruptKeys {
		key = []byte("wrong-key")
	}
	return &vaulticdbv1.GetResponse{Found: true, Key: key, Value: []byte("value")}, nil
}

func (s testService) MultiGet(_ context.Context, request *vaulticdbv1.MultiGetRequest) (*vaulticdbv1.MultiGetResponse, error) {
	results := make([]*vaulticdbv1.GetResponse, len(request.GetKeys()))
	for index, requestKey := range request.GetKeys() {
		key := requestKey
		if s.corruptKeys {
			key = []byte("wrong-key")
		}
		results[index] = &vaulticdbv1.GetResponse{Found: true, Key: key, Value: []byte("value")}
	}
	return &vaulticdbv1.MultiGetResponse{Results: results}, nil
}

func TestOptionsDefaults(t *testing.T) {
	options := (Options{}).withDefaults()
	if options.Socket != DefaultSocket("") || options.StartTimeout != 10*time.Second || options.RetryInterval != 25*time.Millisecond {
		t.Fatalf("unexpected defaults: %#v", options)
	}
}

func TestDefaultRPCContextHasDeadline(t *testing.T) {
	ctx, cancel := withDefaultRPCDeadline(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	remaining := time.Until(deadline)
	if !ok || remaining <= 0 || remaining > defaultRPCDeadline {
		t.Fatalf("default RPC deadline = %v, %t", deadline, ok)
	}
}

func TestDefaultSocketIsRepositoryScoped(t *testing.T) {
	if DefaultSocket("first") == DefaultSocket("second") {
		t.Fatal("repository-scoped socket paths must differ")
	}
}

func TestConnectValidatesDaemon(t *testing.T) {
	socket := testSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	vaulticdbv1.RegisterVaulticDBServer(server, testService{protocol: ProtocolVersion, schema: SchemaVersion})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client, err := Connect(context.Background(), Options{Socket: socket, RepositoryID: "test-repo"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestClientRejectsWrongResponseKeys(t *testing.T) {
	socket := testSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	vaulticdbv1.RegisterVaulticDBServer(server, testService{protocol: ProtocolVersion, schema: SchemaVersion, corruptKeys: true})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client, err := Connect(context.Background(), Options{Socket: socket, RepositoryID: "test-repo"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	if _, _, err := client.Get(context.Background(), []byte("requested-key"), ""); err == nil || !strings.Contains(err.Error(), "wrong key") {
		t.Fatalf("expected wrong point-read key error, got %v", err)
	}
	if _, _, err := client.MultiGet(context.Background(), [][]byte{[]byte("first"), []byte("second")}, ""); err == nil || !strings.Contains(err.Error(), "out-of-order") {
		t.Fatalf("expected out-of-order multi-get error, got %v", err)
	}
}

func TestConnectRejectsIncompatibleDaemon(t *testing.T) {
	socket := testSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	vaulticdbv1.RegisterVaulticDBServer(server, testService{protocol: "vaulticdb.v0", schema: SchemaVersion})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	if _, err := Connect(context.Background(), Options{Socket: socket, RepositoryID: "test-repo"}); err == nil {
		t.Fatal("expected incompatible daemon error")
	}
}

func TestSocketDir(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "runtime", "vaulticdb.sock")
	if got, want := SocketDir(socket), filepath.Join(dir, "runtime"); got != want {
		t.Fatalf("SocketDir() = %q, want %q", got, want)
	}
}

func TestEnsureStartsDaemonRecoversStaleSocketAndCleansUp(t *testing.T) {
	socket := testSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	client, err := Ensure(context.Background(), Options{Socket: socket, RepositoryID: "test-repo", DaemonPath: daemonBinary(t)})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(socket)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket permissions: %v, %v", info.Mode(), err)
	}
	dir, err := os.Stat(filepath.Dir(socket))
	if err != nil || dir.Mode().Perm() != 0o700 {
		t.Fatalf("directory permissions: %v, %v", dir.Mode(), err)
	}
	capabilities, err := client.RPC().Capabilities(context.Background(), &vaulticdbv1.CapabilitiesRequest{RepositoryId: "test-repo", Context: requestContext(context.Background())})
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.GetUnixSocket() || capabilities.GetTcpEnabled() || capabilities.GetMaxConcurrentRequests() != 128 {
		t.Fatalf("unexpected Unix transport capabilities: %#v", capabilities)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("socket remains after shutdown: %v", err)
	}
}

func TestEnsureRacesToOneDaemon(t *testing.T) {
	socket := testSocket(t)
	options := Options{Socket: socket, RepositoryID: "race-repo", DaemonPath: daemonBinary(t)}
	clients := make([]*Client, 4)
	errs := make([]error, len(clients))
	var group sync.WaitGroup
	for index := range clients {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			clients[index], errs[index] = Ensure(context.Background(), options)
		}(index)
	}
	group.Wait()
	var owner *Client
	for index, client := range clients {
		if errs[index] != nil {
			t.Fatal(errs[index])
		}
		if client.process != nil {
			owner = client
		} else if err := client.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if owner == nil {
		t.Fatal("no client owned the daemon")
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureRejectsInsecureTCPConfiguration(t *testing.T) {
	address := freeTCPAddress(t)
	base := Options{TCPAddress: address, RepositoryID: "tcp-repo", DaemonPath: daemonBinary(t), StartTimeout: time.Second}
	if _, err := Ensure(context.Background(), base); err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("expected missing allowlist error, got %v", err)
	}
	base.TCPAllowlist = []string{"127.0.0.1/32"}
	if _, err := Ensure(context.Background(), base); err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("expected missing token error, got %v", err)
	}
	base.TCPAllowlist = []string{"not-a-network"}
	base.AuthToken = "token"
	if _, err := Ensure(context.Background(), base); err == nil || !strings.Contains(err.Error(), "invalid TCP allowlist") {
		t.Fatalf("expected invalid allowlist error, got %v", err)
	}
	if _, err := Ensure(context.Background(), Options{ObjectStore: "s3", DaemonPath: daemonBinary(t)}); err == nil || !strings.Contains(err.Error(), "bucket") {
		t.Fatalf("expected missing S3 bucket error, got %v", err)
	}

}

func TestDaemonEnvironmentFiltersAmbientSecrets(t *testing.T) {
	t.Setenv("VAULTIC_UNRELATED_SECRET", "must-not-be-inherited")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s3-credential")
	t.Setenv("PATH", "/test/bin")

	local := strings.Join(daemonEnvironment(Options{ObjectStore: "local"}), "\n")
	if strings.Contains(local, "VAULTIC_UNRELATED_SECRET") || strings.Contains(local, "AWS_SECRET_ACCESS_KEY") {
		t.Fatalf("local daemon inherited a secret-bearing environment: %s", local)
	}
	if !strings.Contains(local, "PATH=/test/bin") {
		t.Fatalf("local daemon lost required runtime environment: %s", local)
	}

	s3 := strings.Join(daemonEnvironment(Options{ObjectStore: "s3"}), "\n")
	if strings.Contains(s3, "VAULTIC_UNRELATED_SECRET") || !strings.Contains(s3, "AWS_SECRET_ACCESS_KEY=s3-credential") {
		t.Fatalf("S3 daemon environment is not credential-chain scoped: %s", s3)
	}
}

func TestEncryptedDaemonPersistsOnlyCiphertextAndReopens(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	passphraseFile := filepath.Join(t.TempDir(), "recovery-passphrase")
	if err := os.WriteFile(passphraseFile, []byte("correct horse battery staple\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := Options{
		Socket: testSocket(t), RepositoryID: "encrypted-daemon", DaemonPath: daemonBinary(t),
		DataDir: dataDir, EncryptionMode: "initialize", PassphraseFile: passphraseFile,
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	if info := client.Encryption(); !info.Enabled || info.Algorithm != "AES-256-GCM" || info.ActiveDEKVersion != 1 || info.UnlockSlot != "local-recovery" {
		t.Fatalf("unexpected encryption status: %+v", info)
	}
	secret := []byte("alice/private/metadata-value")
	masterKey := []byte("base64-encoded-repository-master-key-fixture")
	if _, err := client.WriteBatch(ctx, []Mutation{{Key: []byte("phase18/secret"), Value: secret}}, nil, true, ""); err != nil {
		t.Fatal(err)
	}
	if err := client.StoreMasterKey(ctx, masterKey); err != nil {
		t.Fatal(err)
	}
	if err := client.StoreMasterKey(ctx, []byte("different-key")); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("master key replacement was not rejected: %v", err)
	}
	keyStatus, err := client.AddLocalKeySlot(ctx, "replacement-recovery", []byte("temporary passphrase"), 10, true)
	if err != nil || keyStatus.EnvelopeGeneration != 2 || len(keyStatus.Slots) != 2 {
		t.Fatalf("add key slot = %+v, err=%v", keyStatus, err)
	}
	keyStatus, err = client.RotateLocalKeySlot(ctx, "replacement-recovery", []byte("replacement passphrase"))
	if err != nil || keyStatus.EnvelopeGeneration != 3 {
		t.Fatalf("rotate key slot = %+v, err=%v", keyStatus, err)
	}
	keyStatus, err = client.RemoveKeySlot(ctx, "local-recovery")
	if err != nil || keyStatus.EnvelopeGeneration != 4 || len(keyStatus.Slots) != 1 {
		t.Fatalf("remove key slot = %+v, err=%v", keyStatus, err)
	}
	if _, err := client.RemoveKeySlot(ctx, "replacement-recovery"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("final slot removal was not rejected: %v", err)
	}
	keyStatus, err = client.RotateDEK(ctx)
	if err != nil || keyStatus.EnvelopeGeneration != 5 || keyStatus.ActiveDEKVersion != 2 {
		t.Fatalf("rotate DEK = %+v, err=%v", keyStatus, err)
	}
	rotatedSecret := []byte("metadata-written-under-DEK-version-2")
	if _, err := client.WriteBatch(ctx, []Mutation{{Key: []byte("phase18/rotated"), Value: rotatedSecret}}, nil, true, ""); err != nil {
		t.Fatal(err)
	}
	var rewritten uint64
	for {
		progress, rewriteErr := client.RewriteDEK(ctx, 1)
		if rewriteErr != nil {
			t.Fatal(rewriteErr)
		}
		rewritten += progress.Rewritten
		if progress.Remaining == 0 {
			break
		}
	}
	if rewritten == 0 {
		t.Fatal("DEK rotation did not rewrite any old-version objects")
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := filepath.WalkDir(dataDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if bytes.Contains(data, secret) || bytes.Contains(data, masterKey) {
			return fmt.Errorf("plaintext metadata found in %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	options.Socket = testSocket(t)
	options.EncryptionMode = "required"
	if err := os.WriteFile(passphraseFile, []byte("replacement passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err = Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	if info := client.Encryption(); info.ActiveDEKVersion != 2 || info.EnvelopeGeneration != 6 {
		t.Fatalf("reopened rotated encryption status: %+v", info)
	}
	value, found, err := client.Get(ctx, []byte("phase18/secret"), "")
	if err != nil || !found || !bytes.Equal(value, secret) {
		t.Fatalf("reopened encrypted value = %q, found=%t, err=%v", value, found, err)
	}
	value, found, err = client.GetMasterKey(ctx)
	if err != nil || !found || !bytes.Equal(value, masterKey) {
		t.Fatalf("reopened master key = %q, found=%t, err=%v", value, found, err)
	}
	value, found, err = client.Get(ctx, []byte("phase18/rotated"), "")
	if err != nil || !found || !bytes.Equal(value, rotatedSecret) {
		t.Fatalf("reopened rotated value = %q, found=%t, err=%v", value, found, err)
	}
}

func TestEncryptedDaemonRefusesMissingPersistentPolicy(t *testing.T) {
	ctx := context.Background()
	passphraseFile := filepath.Join(t.TempDir(), "recovery-passphrase")
	if err := os.WriteFile(passphraseFile, []byte("policy test passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := Options{
		Socket: testSocket(t), RepositoryID: "encrypted-policy", DaemonPath: daemonBinary(t),
		DataDir: t.TempDir(), EncryptionMode: "initialize", PassphraseFile: passphraseFile,
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.WriteBatch(ctx, nil, [][]byte{[]byte("meta:encryption")}, true, ""); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	options.Socket = testSocket(t)
	options.EncryptionMode = "required"
	if client, err := Ensure(ctx, options); err == nil {
		_ = client.Close(ctx)
		t.Fatal("required encryption recreated a missing persistent policy")
	}
}

func TestFailedMetadataKeyUnwrapEmitsAuthEvent(t *testing.T) {
	ctx := context.Background()
	passphraseFile := filepath.Join(t.TempDir(), "recovery-passphrase")
	if err := os.WriteFile(passphraseFile, []byte("correct passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := Options{
		Socket: testSocket(t), RepositoryID: "failed-unwrap", DaemonPath: daemonBinary(t),
		DataDir: t.TempDir(), EncryptionMode: "initialize", PassphraseFile: passphraseFile,
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(passphraseFile, []byte("wrong passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	target, err := observability.ParseSyslogTarget("udp://" + listener.LocalAddr().String() + "?categories=auth&min-severity=warning")
	if err != nil {
		t.Fatal(err)
	}
	observability.SetDefaultSyslog(observability.NewSyslogExporter([]observability.SyslogTarget{target}, "host", "vaultic"))
	defer observability.SetDefaultSyslog(nil)
	options.Socket = testSocket(t)
	options.EncryptionMode = "required"
	if client, err := Ensure(ctx, options); err == nil {
		_ = client.Close(ctx)
		t.Fatal("wrong metadata passphrase unexpectedly unlocked the daemon")
	} else if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unexpected failed-unlock error: %v", err)
	}
	buffer := make([]byte, 4096)
	if err := listener.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	count, _, err := listener.ReadFrom(buffer)
	if err != nil {
		t.Fatal(err)
	}
	message := string(buffer[:count])
	categoryFound := strings.Contains(message, `"category":"auth"`)
	eventFound := strings.Contains(message, `"message":"encrypted metadata daemon failed during startup"`)
	if !categoryFound || !eventFound || strings.Contains(message, "passphrase") {
		t.Fatalf("unexpected failed-unlock event: %s", message)
	}
}

func TestTCPLifecycleAuthenticationDrainDeadlineAndLimit(t *testing.T) {
	options := Options{
		TCPAddress:   freeTCPAddress(t),
		TCPAllowlist: []string{"127.0.0.1/32"},
		AuthToken:    "phase1-secret",
		RepositoryID: "tcp-repo",
		DaemonPath:   daemonBinary(t),
		StartTimeout: 5 * time.Second,
	}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if client.process != nil {
			_ = client.Close(context.Background())
		}
	}()

	capabilities, err := client.RPC().Capabilities(context.Background(), &vaulticdbv1.CapabilitiesRequest{
		RepositoryId: options.RepositoryID,
		Context:      requestContext(context.Background()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.GetTcpEnabled() || capabilities.GetUnixSocket() {
		t.Fatalf("unexpected transport capabilities: %#v", capabilities)
	}
	if capabilities.GetMaxBatchItems() != 10_000 || capabilities.GetMaxPageItems() != 1_000 || capabilities.GetMaxMessageBytes() != 16*1024*1024 || capabilities.GetMaxConcurrentRequests() != 128 {
		t.Fatalf("unexpected bounded-work capabilities: %#v", capabilities)
	}

	for tokenName, token := range map[string]string{"missing": "", "wrong": "wrong-secret"} {
		rpc := vaulticdbv1.NewVaulticDBClient(client.conn)
		if token != "" {
			rpc = &authenticatedClient{VaulticDBClient: rpc, token: token}
		}
		checks := map[string]func() error{
			"health": func() error {
				_, err := rpc.Health(context.Background(), &vaulticdbv1.HealthRequest{RepositoryId: options.RepositoryID, Context: requestContext(context.Background())})
				return err
			},
			"capabilities": func() error {
				_, err := rpc.Capabilities(context.Background(), &vaulticdbv1.CapabilitiesRequest{RepositoryId: options.RepositoryID, Context: requestContext(context.Background())})
				return err
			},
			"drain": func() error {
				_, err := rpc.Drain(context.Background(), &vaulticdbv1.Empty{Context: requestContext(context.Background())})
				return err
			},
			"shutdown": func() error {
				_, err := rpc.Shutdown(context.Background(), &vaulticdbv1.Empty{Context: requestContext(context.Background())})
				return err
			},
			"get": func() error {
				_, err := rpc.Get(context.Background(), &vaulticdbv1.GetRequest{Context: requestContext(context.Background()), Key: []byte("key")})
				return err
			},
			"multi-get": func() error {
				_, err := rpc.MultiGet(context.Background(), &vaulticdbv1.MultiGetRequest{Context: requestContext(context.Background()), Keys: [][]byte{[]byte("key")}})
				return err
			},
			"scan": func() error {
				_, err := rpc.Scan(context.Background(), &vaulticdbv1.ScanRequest{Context: requestContext(context.Background()), Prefix: []byte("k"), PageSize: 1})
				return err
			},
			"write-batch": func() error {
				_, err := rpc.WriteBatch(context.Background(), &vaulticdbv1.WriteBatchRequest{Context: requestContext(context.Background()), Puts: []*vaulticdbv1.KeyValue{{Key: []byte("key"), Value: []byte("value")}}})
				return err
			},
			"begin": func() error {
				_, err := rpc.Begin(context.Background(), &vaulticdbv1.Empty{Context: requestContext(context.Background())})
				return err
			},
			"commit": func() error {
				_, err := rpc.Commit(context.Background(), &vaulticdbv1.TransactionRequest{Context: requestContext(context.Background()), TransactionId: "unknown"})
				return err
			},
			"rollback": func() error {
				_, err := rpc.Rollback(context.Background(), &vaulticdbv1.TransactionRequest{Context: requestContext(context.Background()), TransactionId: "unknown"})
				return err
			},
		}
		for rpcName, check := range checks {
			if err := check(); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("expected %s token rejection from %s, got %v", tokenName, rpcName, err)
			}
		}
	}

	expired := &vaulticdbv1.RequestContext{RequestId: "expired", DeadlineUnixMs: time.Now().Add(-time.Second).UnixMilli()}
	_, err = client.RPC().Health(context.Background(), &vaulticdbv1.HealthRequest{RepositoryId: options.RepositoryID, Context: expired})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected expired deadline rejection, got %v", err)
	}
	_, err = client.RPC().Health(context.Background(), &vaulticdbv1.HealthRequest{
		RepositoryId: options.RepositoryID,
		Context:      &vaulticdbv1.RequestContext{DeadlineUnixMs: time.Now().Add(time.Minute).UnixMilli()},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected missing request ID rejection, got %v", err)
	}

	oversized := &vaulticdbv1.RequestContext{RequestId: strings.Repeat("x", 17*1024*1024), DeadlineUnixMs: time.Now().Add(time.Minute).UnixMilli()}
	_, err = client.RPC().Health(context.Background(), &vaulticdbv1.HealthRequest{RepositoryId: options.RepositoryID, Context: oversized})
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("expected oversized request rejection, got %v", err)
	}

	if _, err := client.RPC().Drain(context.Background(), &vaulticdbv1.Empty{Context: requestContext(context.Background())}); err != nil {
		t.Fatal(err)
	}
	health, err := client.RPC().Health(context.Background(), &vaulticdbv1.HealthRequest{
		RepositoryId: options.RepositoryID,
		Context:      requestContext(context.Background()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if health.GetReady() {
		t.Fatal("daemon remained ready after drain")
	}
	if _, _, err := client.Get(context.Background(), []byte("after-drain"), ""); status.Code(err) != codes.Unavailable {
		t.Fatalf("storage request after drain returned %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureTCPRejectsPeerOutsideAllowlist(t *testing.T) {
	options := Options{
		TCPAddress:   freeTCPAddress(t),
		TCPAllowlist: []string{"192.0.2.0/24"},
		AuthToken:    "allowlist-secret",
		RepositoryID: "tcp-allowlist-repo",
		DaemonPath:   daemonBinary(t),
		StartTimeout: 750 * time.Millisecond,
	}
	_, err := Ensure(context.Background(), options)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected disallowed peer startup timeout, got %v", err)
	}
	if _, err := os.Stat(strings.TrimSuffix(tcpMetadataPath(options.TCPAddress), filepath.Ext(tcpMetadataPath(options.TCPAddress))) + ".pid"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("TCP metadata remained after failed startup: %v", err)
	}
}

func TestEnsureTCPRaceHasOneOwner(t *testing.T) {
	options := Options{
		TCPAddress:   freeTCPAddress(t),
		TCPAllowlist: []string{"127.0.0.1/32"},
		AuthToken:    "race-secret",
		RepositoryID: "tcp-race-repo",
		DaemonPath:   daemonBinary(t),
		StartTimeout: 5 * time.Second,
	}
	clients := make([]*Client, 4)
	errs := make([]error, len(clients))
	var group sync.WaitGroup
	for index := range clients {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			clients[index], errs[index] = Ensure(context.Background(), options)
		}(index)
	}
	group.Wait()
	owners := 0
	for index, client := range clients {
		if errs[index] != nil {
			t.Fatal(errs[index])
		}
		if client.process != nil {
			owners++
			defer client.Close(context.Background())
		} else if err := client.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if owners != 1 {
		t.Fatalf("got %d TCP daemon owners, want 1", owners)
	}
}

func TestEnsureCancellationKillsUnreadyChild(t *testing.T) {
	directory := t.TempDir()
	script := filepath.Join(directory, "unready-daemon")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(directory, "daemon.sock")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := Ensure(ctx, Options{Socket: socket, RepositoryID: "cancel-repo", DaemonPath: script, StartTimeout: 5 * time.Second})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected cancellation error, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("cancellation cleanup took %v", elapsed)
	}
}

func TestCloseBoundsShutdownAndKillsOwnedChild(t *testing.T) {
	socket := testSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	vaulticdbv1.RegisterVaulticDBServer(server, testService{protocol: ProtocolVersion, schema: SchemaVersion, blockShutdown: true})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client, err := Connect(context.Background(), Options{Socket: socket, RepositoryID: "shutdown-repo"})
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command("sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	client.process = child
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := client.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected bounded shutdown deadline, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("bounded shutdown took %v", elapsed)
	}
}
