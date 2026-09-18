package daemon

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProcessDeferredCommitDurabilityTokens(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	options := Options{
		Socket: testSocket(t), RepositoryID: "phase34-m2g-durability-token",
		DaemonPath: failureDaemonBinary(t), DataDir: directory, ObjectStore: "local",
		WALStore: "local", WALDataDir: directory + "/wal", WALFlushInterval: time.Millisecond,
		RebuildReset: true,
		testEnvironment: []string{
			`VAULTICDB_TEST_OBJECT_DELAY_PROFILE={"version":1,"target":"isolated","role":"wal","operation":"put","delay_ms":100}`,
		},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(ctx) })

	commit := func(key string) DurabilityToken {
		transaction, beginErr := client.Begin(ctx)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		if writeErr := transaction.WriteBatch(ctx, []Mutation{{Key: []byte(key), Value: []byte("value")}}, nil); writeErr != nil {
			t.Fatal(writeErr)
		}
		token, commitErr := transaction.CommitDeferredWithToken(ctx)
		if commitErr != nil {
			t.Fatal(commitErr)
		}
		if token.RepositoryGeneration == 0 || token.WriterEpoch == 0 || token.AppliedSequence == 0 {
			t.Fatalf("incomplete durability token: %+v", token)
		}
		return token
	}

	first := commit("m2g-first")
	second := commit("m2g-second")
	if second.RepositoryGeneration != first.RepositoryGeneration || second.WriterEpoch != first.WriterEpoch || second.AppliedSequence <= first.AppliedSequence {
		t.Fatalf("tokens are not ordered in one generation and epoch: first=%+v second=%+v", first, second)
	}

	durableThrough, err := client.AwaitDurableThrough(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if durableThrough.AppliedSequence < second.AppliedSequence {
		t.Fatalf("durable through = %+v, want at least %+v", durableThrough, second)
	}
	if _, err := client.AwaitDurableThrough(ctx, first); err != nil {
		t.Fatalf("coalesced earlier fence: %v", err)
	}

	invalid := []DurabilityToken{
		{RepositoryGeneration: second.RepositoryGeneration + 1, WriterEpoch: second.WriterEpoch, AppliedSequence: second.AppliedSequence},
		{RepositoryGeneration: second.RepositoryGeneration, WriterEpoch: second.WriterEpoch + 1, AppliedSequence: second.AppliedSequence},
		{RepositoryGeneration: second.RepositoryGeneration, WriterEpoch: second.WriterEpoch, AppliedSequence: second.AppliedSequence + 1},
	}
	for _, token := range invalid {
		if _, fenceErr := client.AwaitDurableThrough(ctx, token); status.Code(fenceErr) != codes.FailedPrecondition {
			t.Errorf("invalid token %+v error = %v", token, fenceErr)
		}
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	options.Socket = testSocket(t)
	options.RebuildReset = false
	options.testEnvironment = nil
	client, err = Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"m2g-first", "m2g-second"} {
		value, found, readErr := client.Get(ctx, []byte(key), "")
		if readErr != nil || !found || string(value) != "value" {
			t.Fatalf("replayed durable prefix %q: value=%q found=%t err=%v", key, value, found, readErr)
		}
	}
	if _, err := client.AwaitDurableThrough(ctx, second); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("prior writer token after restart error = %v", err)
	}
}

func TestProcessDeferredCommitRefusesMemoryWAL(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase34-m2g-memory-wal",
		DaemonPath: failureDaemonBinary(t), DataDir: directory, ObjectStore: "local",
		WALStore: "memory", WALDataDir: directory + "/wal", WALFlushInterval: time.Millisecond,
		RebuildReset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	transaction, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.WriteBatch(ctx, []Mutation{{Key: []byte("memory-wal"), Value: []byte("value")}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.CommitDeferredWithToken(ctx); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("memory-WAL deferred commit error = %v", err)
	}
	if value, found, err := client.Get(ctx, []byte("memory-wal"), ""); err != nil || found {
		t.Fatalf("token-required memory-WAL commit applied before rejection: value=%q found=%t err=%v", value, found, err)
	}
	if err := transaction.CommitDeferred(ctx); err != nil {
		t.Fatalf("legacy applied-only memory-WAL commit: %v", err)
	}
}

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

func phase34M2Barrier(t *testing.T) (net.Listener, string) {
	t.Helper()
	path := testSocket(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener, path
}

func phase34M2AwaitBarrier(t *testing.T, listener net.Listener, expected string) net.Conn {
	t.Helper()
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	name, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	if strings.TrimSpace(name) != expected {
		_ = connection.Close()
		t.Fatalf("barrier = %q, want %q", name, expected)
	}
	return connection
}

func phase34M2KillDaemon(t *testing.T, client *Client) {
	t.Helper()
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
}

func phase34M2CrashOptions(t *testing.T, directory, repositoryID string, environment ...string) Options {
	t.Helper()
	return Options{
		Socket: testSocket(t), RepositoryID: repositoryID,
		DaemonPath: failureDaemonBinary(t), DataDir: directory, ObjectStore: "local",
		WALStore: "local", WALDataDir: directory + "/wal", WALFlushInterval: time.Millisecond,
		RebuildReset: true, testEnvironment: environment,
	}
}

func phase34M2DeferredToken(t *testing.T, ctx context.Context, client *Client, key string) DurabilityToken {
	t.Helper()
	transaction, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.WriteBatch(ctx, []Mutation{{Key: []byte(key), Value: []byte("value")}}, nil); err != nil {
		t.Fatal(err)
	}
	token, err := transaction.CommitDeferredWithToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func phase34M2ReopenAndRead(t *testing.T, ctx context.Context, options Options, key string) bool {
	t.Helper()
	options.Socket = testSocket(t)
	options.RebuildReset = false
	options.testEnvironment = nil
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	value, found, err := client.Get(ctx, []byte(key), "")
	if err != nil || found && string(value) != "value" {
		t.Fatalf("reopened value=%q found=%t err=%v", value, found, err)
	}
	return found
}

func TestProcessDurabilityCrashMatrix(t *testing.T) {
	ctx := context.Background()
	t.Run("before-apply", func(t *testing.T) {
		listener, path := phase34M2Barrier(t)
		options := phase34M2CrashOptions(t, t.TempDir(), "phase34-m2g-before-apply", "VAULTICDB_TEST_TRANSACTION_BEFORE_APPLY_BARRIER="+path)
		client, err := Ensure(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		transaction, err := client.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		key := "before-apply"
		if err := transaction.WriteBatch(ctx, []Mutation{{Key: []byte(key), Value: []byte("value")}}, nil); err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() { result <- transaction.Commit(ctx) }()
		connection := phase34M2AwaitBarrier(t, listener, "VAULTICDB_TEST_TRANSACTION_BEFORE_APPLY_BARRIER")
		select {
		case err := <-result:
			t.Fatalf("commit returned before apply release: %v", err)
		default:
		}
		phase34M2KillDaemon(t, client)
		_ = connection.Close()
		if phase34M2ReopenAndRead(t, ctx, options, key) {
			t.Fatal("pre-apply transaction survived restart")
		}
	})

	t.Run("after-applied-before-response", func(t *testing.T) {
		directory := t.TempDir()
		reached := make(chan struct{}, 1)
		commitCtx, cancel := context.WithCancel(ctx)
		options := phase34M2CrashOptions(t, directory, "phase34-m2g-after-applied")
		options.ResponseDeliveryForTesting = func(wait context.Context, method string) error {
			if method != "/vaulticdb.v1.VaulticDB/Commit" {
				return nil
			}
			reached <- struct{}{}
			<-wait.Done()
			return wait.Err()
		}
		client, err := Ensure(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		transaction, err := client.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := transaction.WriteBatch(ctx, []Mutation{{Key: []byte("after-applied"), Value: []byte("value")}}, nil); err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() { _, commitErr := transaction.CommitDeferredWithToken(commitCtx); result <- commitErr }()
		<-reached
		phase34M2KillDaemon(t, client)
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("withheld applied acknowledgement error = %v", err)
		}
		_ = phase34M2ReopenAndRead(t, ctx, options, "after-applied")
	})

	for _, point := range []struct {
		name       string
		variable   string
		mustReplay bool
	}{
		{name: "before-fence", variable: "VAULTICDB_TEST_DURABILITY_BEFORE_FENCE_BARRIER"},
		{name: "after-fence", variable: "VAULTICDB_TEST_DURABILITY_AFTER_FENCE_BARRIER", mustReplay: true},
	} {
		t.Run(point.name, func(t *testing.T) {
			listener, path := phase34M2Barrier(t)
			key := point.name
			options := phase34M2CrashOptions(t, t.TempDir(), "phase34-m2g-"+point.name, point.variable+"="+path)
			client, err := Ensure(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			token := phase34M2DeferredToken(t, ctx, client, key)
			result := make(chan error, 1)
			go func() { _, fenceErr := client.AwaitDurableThrough(ctx, token); result <- fenceErr }()
			connection := phase34M2AwaitBarrier(t, listener, point.variable)
			select {
			case err := <-result:
				t.Fatalf("fence returned before crash-point release: %v", err)
			default:
			}
			phase34M2KillDaemon(t, client)
			_ = connection.Close()
			found := phase34M2ReopenAndRead(t, ctx, options, key)
			if point.mustReplay && !found {
				t.Fatal("fenced prefix did not survive restart")
			}
		})
	}

	t.Run("after-fence-before-response", func(t *testing.T) {
		directory := t.TempDir()
		reached := make(chan struct{}, 1)
		fenceCtx, cancel := context.WithCancel(ctx)
		options := phase34M2CrashOptions(t, directory, "phase34-m2g-fence-response")
		options.ResponseDeliveryForTesting = func(wait context.Context, method string) error {
			if method != "/vaulticdb.v1.VaulticDB/AwaitDurableThrough" {
				return nil
			}
			reached <- struct{}{}
			<-wait.Done()
			return wait.Err()
		}
		client, err := Ensure(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		token := phase34M2DeferredToken(t, ctx, client, "fence-response")
		result := make(chan error, 1)
		go func() { _, fenceErr := client.AwaitDurableThrough(fenceCtx, token); result <- fenceErr }()
		<-reached
		phase34M2KillDaemon(t, client)
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("withheld fence response error = %v", err)
		}
		if !phase34M2ReopenAndRead(t, ctx, options, "fence-response") {
			t.Fatal("fenced prefix with lost response did not survive restart")
		}
	})

	t.Run("after-final-publication", func(t *testing.T) {
		options := phase34M2CrashOptions(t, t.TempDir(), "phase34-m2g-final-publication")
		client, err := Ensure(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		token := phase34M2DeferredToken(t, ctx, client, "final-publication")
		if _, err := client.AwaitDurableThrough(ctx, token); err != nil {
			t.Fatal(err)
		}
		phase34M2KillDaemon(t, client)
		if !phase34M2ReopenAndRead(t, ctx, options, "final-publication") {
			t.Fatal("published durable prefix did not survive restart")
		}
	})
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

func TestProcessResponseDeliveryBarrierRunsAfterCommitCompletion(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	completed := make(chan struct{}, 1)
	release := make(chan struct{})
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase34-m2-response-barrier",
		DaemonPath: failureDaemonBinary(t), DataDir: directory, ObjectStore: "local",
		WALStore: "local", WALDataDir: directory + "/wal", WALFlushInterval: time.Millisecond,
		RebuildReset: true,
		ResponseDeliveryForTesting: func(ctx context.Context, method string) error {
			if method != "/vaulticdb.v1.VaulticDB/Commit" {
				return nil
			}
			completed <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	transaction, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("response-barrier")
	if err := transaction.WriteBatch(ctx, []Mutation{{Key: key, Value: []byte("value")}}, nil); err != nil {
		t.Fatal(err)
	}
	commitResult := make(chan error, 1)
	go func() {
		_, commitErr := transaction.CommitDeferredWithToken(ctx)
		commitResult <- commitErr
	}()
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("commit response did not reach the delivery barrier")
	}
	value, found, err := client.Get(ctx, key, "")
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("applied state before response delivery: value=%q found=%t err=%v", value, found, err)
	}
	select {
	case err := <-commitResult:
		t.Fatalf("commit returned before response release: %v", err)
	default:
	}
	close(release)
	if err := <-commitResult; err != nil {
		t.Fatal(err)
	}

	_, err = client.rpc.Commit(ctx, &vaulticdbv1.TransactionRequest{
		Context: requestContext(ctx), TransactionId: "missing-transaction", DeferDurability: true,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("failure-before-execution error = %v", err)
	}
	select {
	case <-completed:
		t.Fatal("failed RPC entered post-success response delivery hook")
	default:
	}
}
