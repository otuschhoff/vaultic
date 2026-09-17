package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func TestProcessFreshImportKillBeforeCloseRequiresReset(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	options := Options{
		Socket: testSocket(t), RepositoryID: "phase32-p3a0-kill-before-close",
		DaemonPath: daemonBinary(t), DataDir: directory, ObjectStore: "local",
		WALStore: "memory", WALDataDir: directory + "/wal", WALFlushInterval: 500 * time.Millisecond,
		MaxUnflushedBytes: 16 << 30, L0SSTSizeBytes: 256 << 20,
		RebuildReset: true, FreshBulkImport: true,
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	source, session, packID, blobID := daemonTestID(240), daemonTestID(241), daemonTestID(242), daemonTestID(243)
	imports := []LegacyPackImport{legacyPackImport(source, packID, map[schema.ID]schema.BlobRecord{
		blobID: {Locations: []schema.BlobLocation{{PackID: packID, Length: 1, Type: schema.BlobData}}},
	})}
	checkpoint := Mutation{
		Key: schema.ImportCheckpointKey(source),
		Value: encodeSchemaRecord(t, schema.ImportCheckpointRecord{
			PacksImported: 1,
			BlobsImported: 1,
		}),
	}
	if err := store.IngestLegacyPacks(ctx, session, 1, imports); err != nil {
		t.Fatal(err)
	}
	if err := store.ReduceLegacyImportBatch(ctx, session, 1, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkBulkImportComplete(ctx); err != nil {
		t.Fatal(err)
	}
	if client.process == nil {
		t.Fatal("test client does not own the daemon process")
	}
	if err := client.process.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := client.process.Wait(); err == nil {
		t.Fatal("killed daemon exited successfully")
	}
	client.process = nil
	if err := client.conn.Close(); err != nil {
		t.Fatal(err)
	}

	completed := options
	completed.Socket = testSocket(t)
	completed.WALStore = "local"
	completed.RebuildReset = false
	completed.FreshBulkImport = false
	completed.StartTimeout = 2 * time.Second
	if reopened, reopenErr := Ensure(ctx, completed); reopenErr == nil {
		_ = reopened.Close(ctx)
		t.Fatal("completed import reopened without a clean close and WAL handoff")
	}

	restart := options
	restart.Socket = testSocket(t)
	restarted, err := Ensure(ctx, restart)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	for _, key := range [][]byte{
		schema.PackKey(packID),
		schema.BlobKey(blobID),
		checkpoint.Key,
		[]byte(bulkImportCompleteKey),
	} {
		if _, found, readErr := restarted.Get(ctx, key, ""); readErr != nil || found {
			t.Fatalf("fresh reset retained killed import state: key=%x found=%t err=%v", key, found, readErr)
		}
	}
}

func TestProcessWALPutDelayExtendsDurableCommit(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-p3-wal-put-delay",
		DaemonPath: failureDaemonBinary(t), DataDir: directory, ObjectStore: "local",
		WALStore: "local", WALDataDir: directory + "/wal", WALFlushInterval: time.Millisecond,
		testEnvironment: []string{
			`VAULTICDB_TEST_OBJECT_DELAY_PROFILE={"version":1,"target":"isolated","role":"wal","operation":"put","delay_ms":100}`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	before, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.WriteBatch(ctx, []Mutation{{Key: []byte("profiled"), Value: []byte("value")}}, nil); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := transaction.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	commitElapsed := time.Since(started)
	after, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	walBefore := before.Attribution.ObjectStoreWAL.Put.Timing
	walAfter := after.Attribution.ObjectStoreWAL.Put.Timing
	if walAfter.Attempts <= walBefore.Attempts || walAfter.TotalUS < walBefore.TotalUS+100_000 {
		t.Fatalf("WAL PUT delay was not attributed: before=%+v after=%+v", walBefore, walAfter)
	}
	if commitElapsed < 100*time.Millisecond {
		t.Fatalf("durable commit returned before WAL persistence delay: %s", commitElapsed)
	}
	if after.Attribution.DurableWait.TotalUS < before.Attribution.DurableWait.TotalUS+100_000 {
		t.Fatalf("durable wait did not include WAL persistence delay: before=%+v after=%+v", before.Attribution.DurableWait, after.Attribution.DurableWait)
	}
}

func TestProcessDelayedCommitResponseRecoversCommittedSuccess(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-p3-response-timeout",
		DaemonPath: failureDaemonBinary(t), DataDir: directory, ObjectStore: "local",
		WALStore: "local", WALDataDir: directory + "/wal", WALFlushInterval: time.Millisecond,
		commitResponseDelayForTesting: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	before, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("delayed-commit-response")
	if err := transaction.WriteBatch(ctx, []Mutation{{Key: key, Value: []byte("value")}}, nil); err != nil {
		t.Fatal(err)
	}
	commitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	err = transaction.CommitWithIdempotency(commitCtx, "phase32-delayed-commit")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delayed committed response error = %v", err)
	}
	value, found, err := client.Get(ctx, key, "")
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("committed state after response timeout: value=%q found=%t err=%v", value, found, err)
	}
	afterTimeout, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if afterTimeout.LastDurableSequence != before.LastDurableSequence+1 {
		t.Fatalf("durable sequence after committed timeout = %d, want %d", afterTimeout.LastDurableSequence, before.LastDurableSequence+1)
	}
	if err := transaction.CommitWithIdempotency(ctx, "phase32-delayed-commit"); err != nil {
		t.Fatal(err)
	}
	afterRecovery, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if afterRecovery.LastDurableSequence != afterTimeout.LastDurableSequence {
		t.Fatalf("idempotent recovery republished commit: before=%d after=%d", afterTimeout.LastDurableSequence, afterRecovery.LastDurableSequence)
	}
}
