package daemon

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/topology"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	failureDaemonBuildOnce sync.Once
	failureDaemonPath      string
	failureDaemonBuildErr  error
)

func failureDaemonBinary(t *testing.T) string {
	t.Helper()
	if binary := os.Getenv("VAULTICDB_TEST_BINARY"); binary != "" {
		return binary
	}
	failureDaemonBuildOnce.Do(func() {
		_, source, _, ok := runtime.Caller(0)
		if !ok {
			failureDaemonBuildErr = errors.New("locate failure recovery test source")
			return
		}
		root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", "..", ".."))
		target := filepath.Join(root, "vaulticdb", "target", "process-tests")
		command := exec.Command("cargo", "build", "--manifest-path", filepath.Join(root, "vaulticdb", "Cargo.toml"), "--bin", "vaulticdb", "--features", "test-failpoints", "--target-dir", target)
		if output, err := command.CombinedOutput(); err != nil {
			failureDaemonBuildErr = errors.New(string(output))
			return
		}
		name := "vaulticdb"
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		failureDaemonPath = filepath.Join(target, "debug", name)
	})
	if failureDaemonBuildErr != nil {
		t.Fatal(failureDaemonBuildErr)
	}
	return failureDaemonPath
}

func requireRPCDetail(t *testing.T, err error, code codes.Code, detailCode string, retryable bool) *vaulticdbv1.ErrorDetail {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("RPC code = %v, want %v: %v", status.Code(err), code, err)
	}
	var rpcError *RPCError
	var detail *vaulticdbv1.ErrorDetail
	if errors.As(err, &rpcError) {
		detail = rpcError.Detail()
	} else {
		for _, candidate := range status.Convert(err).Details() {
			if decoded, ok := candidate.(*vaulticdbv1.ErrorDetail); ok {
				detail = decoded
				break
			}
		}
	}
	if detail == nil {
		t.Fatalf("RPC error has no decoded detail: %v", err)
	}
	if detail.GetCode() != detailCode || detail.GetRetryable() != retryable {
		t.Fatalf("RPC detail = %+v, want code=%q retryable=%t", detail, detailCode, retryable)
	}
	return detail
}

func daemonHealth(t *testing.T, client *Client) *vaulticdbv1.HealthResponse {
	t.Helper()
	health, err := client.RPC().Health(context.Background(), &vaulticdbv1.HealthRequest{
		RepositoryId: client.options.RepositoryID,
		Context:      requestContext(context.Background()),
	})
	if err != nil {
		t.Fatal(err)
	}
	return health
}

func TestProcessStartupRetriesIncompleteRuntimeMetadata(t *testing.T) {
	ctx := context.Background()
	barrierPath := testSocket(t)
	barrier, err := net.Listen("unix", barrierPath)
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	options := Options{
		Socket: testSocket(t), RepositoryID: "runtime-metadata-publication", DaemonPath: failureDaemonBinary(t),
		DataDir: t.TempDir(), ObjectStore: "local",
		testEnvironment: []string{"VAULTICDB_TEST_RUNTIME_METADATA_BARRIER=" + barrierPath},
	}
	type ensureResult struct {
		client *Client
		err    error
	}
	result := make(chan ensureResult, 1)
	go func() {
		client, ensureErr := Ensure(ctx, options)
		result <- ensureResult{client: client, err: ensureErr}
	}()
	connection, err := barrier.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	name, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil || strings.TrimSpace(name) != "VAULTICDB_TEST_RUNTIME_METADATA_BARRIER" {
		t.Fatalf("runtime metadata barrier = %q, err=%v", name, err)
	}
	if _, err := os.Stat(metadataPath(options.Socket, ".pid")); err != nil {
		t.Fatalf("PID metadata was not published: %v", err)
	}
	if _, err := os.Stat(metadataPath(options.Socket, ".cap")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("capability metadata exists before barrier release: %v", err)
	}
	select {
	case early := <-result:
		if early.client != nil {
			_ = early.client.Close(ctx)
		}
		t.Fatalf("Ensure returned during incomplete metadata publication: %v", early.err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := connection.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	started := <-result
	if started.err != nil {
		t.Fatal(started.err)
	}
	defer started.client.Close(ctx)
}

func TestProcessCommitDurabilityFailureRecoversAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	options := Options{
		Socket: testSocket(t), RepositoryID: "commit-durability", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
		testEnvironment: []string{"VAULTICDB_TEST_FAILPOINTS=before-transaction-durability"},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	before := daemonHealth(t, client)
	transaction, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.WriteBatch(ctx, []Mutation{{Key: []byte("committed-key"), Value: []byte("committed-value")}}, nil); err != nil {
		t.Fatal(err)
	}
	err = transaction.CommitWithIdempotency(ctx, "commit-durability-request")
	requireRPCDetail(t, err, codes.Unavailable, "storage_unavailable", true)
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("commit error = %v, want ErrStorageUnavailable", err)
	}
	after := daemonHealth(t, client)
	if !after.GetReady() || after.GetState() != "read_write" || after.GetStateSinceUnixMs() != before.GetStateSinceUnixMs() {
		t.Fatalf("lifecycle changed after durability failure: before=%+v after=%+v", before, after)
	}
	writer, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if writer.Role != "read-write" || writer.CurrentEpoch == 0 || writer.ActiveTransactions != 0 {
		t.Fatalf("writer status after durability failure = %+v", writer)
	}
	if committed, err := client.IdempotencyCommitted(ctx, "commit-durability-request"); err != nil || !committed {
		t.Fatalf("committed idempotency record = %t, %v", committed, err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	options.Socket = testSocket(t)
	options.testEnvironment = nil
	client, err = Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	value, found, err := client.Get(ctx, []byte("committed-key"), "")
	if err != nil || !found || string(value) != "committed-value" {
		t.Fatalf("restarted committed value = %q, found=%t, err=%v", value, found, err)
	}
	if committed, err := client.IdempotencyCommitted(ctx, "commit-durability-request"); err != nil || !committed {
		t.Fatalf("restarted idempotency record = %t, %v", committed, err)
	}
}

func TestProcessRollbackRejectsStaleWriterEpoch(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	ownerOptions := Options{
		Socket: testSocket(t), RepositoryID: "rollback-stale-writer", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
	}
	owner, err := Ensure(ctx, ownerOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(ctx)
	transaction, err := owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ownerStatus, err := owner.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}

	takeoverOptions := ownerOptions
	takeoverOptions.Socket = testSocket(t)
	takeover, err := Ensure(ctx, takeoverOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer takeover.Close(ctx)
	if _, err := takeover.PromoteWriterWithTakeover(ctx, "fence rollback owner", true, ownerStatus.CurrentEpoch); err != nil {
		t.Fatal(err)
	}

	_, err = owner.RPC().Rollback(ctx, &vaulticdbv1.TransactionRequest{
		Context: requestContext(ctx), TransactionId: transaction.ID(),
	})
	detail := requireRPCDetail(t, err, codes.Aborted, "writer_fenced", false)
	if detail.GetGeneration() <= ownerStatus.CurrentEpoch {
		t.Fatalf("writer fence generation = %d, want greater than %d", detail.GetGeneration(), ownerStatus.CurrentEpoch)
	}
}

func TestProcessPromotionOpenFailureReleasesClaimAndRetries(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	ownerOptions := Options{
		Socket: testSocket(t), RepositoryID: "promotion-open", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
	}
	owner, err := Ensure(ctx, ownerOptions)
	if err != nil {
		t.Fatal(err)
	}
	readerOptions := ownerOptions
	readerOptions.Socket = testSocket(t)
	readerOptions.testEnvironment = []string{"VAULTICDB_TEST_FAILPOINTS=open-writer"}
	reader, err := Ensure(ctx, readerOptions)
	if err != nil {
		_ = owner.Close(ctx)
		t.Fatal(err)
	}
	defer reader.Close(ctx)
	initial, err := reader.WriterStatus(ctx)
	if err != nil || initial.Role != "read-only" || initial.ObservedEpoch == 0 {
		t.Fatalf("initial reader status = %+v, err=%v", initial, err)
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = reader.PromoteWriter(ctx, "process failure test")
	requireRPCDetail(t, err, codes.Unavailable, "storage_unavailable", true)
	afterFailure, statusErr := reader.WriterStatus(ctx)
	if statusErr != nil {
		t.Fatal(statusErr)
	}
	if afterFailure.Role != "read-only" || afterFailure.CurrentEpoch != 0 || afterFailure.ObservedEpoch <= initial.ObservedEpoch {
		t.Fatalf("promotion failure status = %+v, initial=%+v", afterFailure, initial)
	}
	health := daemonHealth(t, reader)
	if !health.GetReady() || health.GetState() != "read_only" || health.GetStateDetail() != "writer promotion failed" {
		t.Fatalf("promotion failure lifecycle = %+v", health)
	}
	promoted, err := reader.PromoteWriter(ctx, "retry after injected open failure")
	if err != nil || promoted.Role != "read-write" || promoted.CurrentEpoch <= afterFailure.ObservedEpoch {
		t.Fatalf("promotion retry = %+v, err=%v", promoted, err)
	}
	if err := reader.Close(ctx); err != nil {
		t.Fatal(err)
	}

	restartOptions := ownerOptions
	restartOptions.Socket = testSocket(t)
	restarted, err := Ensure(ctx, restartOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	restartedStatus, err := restarted.WriterStatus(ctx)
	if err != nil || restartedStatus.Role != "read-write" || restartedStatus.CurrentEpoch <= promoted.CurrentEpoch {
		t.Fatalf("restarted writer status = %+v, err=%v", restartedStatus, err)
	}
}

func TestProcessPromotionOpenAndClaimReleaseFailureRequiresTakeover(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	ownerOptions := Options{
		Socket: testSocket(t), RepositoryID: "promotion-retained-claim", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
	}
	owner, err := Ensure(ctx, ownerOptions)
	if err != nil {
		t.Fatal(err)
	}
	readerOptions := ownerOptions
	readerOptions.Socket = testSocket(t)
	readerOptions.testEnvironment = []string{"VAULTICDB_TEST_FAILPOINTS=open-writer,release-writer-claim"}
	reader, err := Ensure(ctx, readerOptions)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := reader.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = reader.PromoteWriter(ctx, "retain failed promotion claim")
	requireRPCDetail(t, err, codes.FailedPrecondition, "writer_role", false)
	after, statusErr := reader.WriterStatus(ctx)
	health := daemonHealth(t, reader)
	if statusErr != nil || after.Role != "fenced" || after.CurrentEpoch != 0 || after.ObservedEpoch <= initial.ObservedEpoch || health.GetReady() || health.GetState() != "failed" {
		t.Fatalf("retained promotion claim: writer=%+v health=%+v initial=%+v err=%v", after, health, initial, statusErr)
	}
	if err := reader.Close(ctx); err != nil {
		t.Fatal(err)
	}

	restartOptions := ownerOptions
	restartOptions.Socket = testSocket(t)
	restarted, err := Ensure(ctx, restartOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	observed, err := restarted.WriterStatus(ctx)
	if err != nil || observed.Role != "read-only" || observed.ObservedEpoch != after.ObservedEpoch {
		t.Fatalf("retained claim after restart = %+v, failed=%+v, err=%v", observed, after, err)
	}
	promoted, err := restarted.PromoteWriterWithTakeover(ctx, "recover retained promotion claim", true, observed.ObservedEpoch)
	if err != nil || promoted.Role != "read-write" || promoted.CurrentEpoch <= observed.ObservedEpoch {
		t.Fatalf("retained claim takeover = %+v, observed=%+v, err=%v", promoted, observed, err)
	}
}

func TestProcessPromotionReaderCloseFailureReleasesClaimAndRetries(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	ownerOptions := Options{
		Socket: testSocket(t), RepositoryID: "promotion-reader-close", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
	}
	owner, err := Ensure(ctx, ownerOptions)
	if err != nil {
		t.Fatal(err)
	}
	readerOptions := ownerOptions
	readerOptions.Socket = testSocket(t)
	readerOptions.testEnvironment = []string{"VAULTICDB_TEST_FAILPOINTS=close-reader"}
	reader, err := Ensure(ctx, readerOptions)
	if err != nil {
		_ = owner.Close(ctx)
		t.Fatal(err)
	}
	defer reader.Close(ctx)
	initial, err := reader.WriterStatus(ctx)
	if err != nil || initial.Role != "read-only" {
		t.Fatalf("initial reader = %+v, err=%v", initial, err)
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = reader.PromoteWriter(ctx, "reader close failure")
	requireRPCDetail(t, err, codes.Unavailable, "storage_unavailable", true)
	after := daemonHealth(t, reader)
	if !after.GetReady() || after.GetState() != "read_only" || after.GetStateDetail() != "writer promotion failed" {
		t.Fatalf("reader close recovery lifecycle = %+v", after)
	}
	promoted, err := reader.PromoteWriter(ctx, "retry reader close")
	if err != nil || promoted.Role != "read-write" || promoted.CurrentEpoch <= initial.ObservedEpoch {
		t.Fatalf("reader close promotion retry = %+v, err=%v", promoted, err)
	}
}

func TestProcessDemotionReaderOpenFailureRecoversAfterRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	options := Options{
		Socket: testSocket(t), RepositoryID: "demotion-reader-open", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
		testEnvironment: []string{"VAULTICDB_TEST_FAILPOINTS=open-reader"},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	before := daemonHealth(t, client)
	beforeWriter, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DemoteWriter(ctx, "process failure test", true, time.Second)
	detail := requireRPCDetail(t, err, codes.FailedPrecondition, "writer_role", false)
	after := daemonHealth(t, client)
	if after.GetReady() || after.GetState() != "failed" || after.GetStateDetail() != "demotion failed" || after.GetStateSinceUnixMs() < before.GetStateSinceUnixMs() {
		t.Fatalf("reader-open failure lifecycle: writer=%+v detail=%+v before=%+v after=%+v", beforeWriter, detail, before, after)
	}
	afterWriter, err := client.WriterStatus(ctx)
	if err != nil || afterWriter.Role != "fenced" || afterWriter.CurrentEpoch != beforeWriter.CurrentEpoch || afterWriter.ObservedEpoch != beforeWriter.CurrentEpoch {
		t.Fatalf("reader-open failure writer = %+v, before=%+v, err=%v", afterWriter, beforeWriter, err)
	}
	observerOptions := options
	observerOptions.Socket = testSocket(t)
	observerOptions.testEnvironment = nil
	observer, err := Ensure(ctx, observerOptions)
	if err != nil {
		t.Fatal(err)
	}
	observerWriter, err := observer.WriterStatus(ctx)
	if err != nil || observerWriter.Role != "read-only" || observerWriter.ObservedEpoch != beforeWriter.CurrentEpoch {
		t.Fatalf("retained claim observer = %+v, writer=%+v, err=%v", observerWriter, beforeWriter, err)
	}
	if err := observer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	options.Socket = testSocket(t)
	options.testEnvironment = nil
	restarted, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	reader, err := restarted.WriterStatus(ctx)
	if err != nil || reader.Role != "read-only" || reader.ObservedEpoch == 0 {
		t.Fatalf("restarted status = %+v, err=%v", reader, err)
	}
	promoted, err := restarted.PromoteWriterWithTakeover(ctx, "recover failed demotion", true, reader.ObservedEpoch)
	if err != nil || promoted.Role != "read-write" || promoted.CurrentEpoch <= reader.ObservedEpoch {
		t.Fatalf("recovery promotion = %+v, err=%v", promoted, err)
	}
}

func TestProcessDemotionTimeoutObservesConcurrentTakeover(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	ownerOptions := Options{
		Socket: testSocket(t), RepositoryID: "demotion-timeout-takeover", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
	}
	owner, err := Ensure(ctx, ownerOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(ctx)
	if _, err := owner.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := owner.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	takeoverOptions := ownerOptions
	takeoverOptions.Socket = testSocket(t)
	takeover, err := Ensure(ctx, takeoverOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer takeover.Close(ctx)

	demotion := make(chan error, 1)
	go func() {
		_, demotionErr := owner.DemoteWriter(ctx, "timeout during takeover", true, 500*time.Millisecond)
		demotion <- demotionErr
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, statusErr := owner.WriterStatus(ctx)
		if statusErr != nil {
			t.Fatal(statusErr)
		}
		if status.Role == "demoting" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("writer never entered demoting state: %+v", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	winner, err := takeover.PromoteWriterWithTakeover(ctx, "take over demoting writer", true, before.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	detail := requireRPCDetail(t, <-demotion, codes.Aborted, "writer_fenced", false)
	if detail.GetGeneration() != winner.CurrentEpoch {
		t.Fatalf("writer fence generation = %d, want %d", detail.GetGeneration(), winner.CurrentEpoch)
	}
	after := daemonHealth(t, owner)
	afterWriter, statusErr := owner.WriterStatus(ctx)
	if statusErr != nil || after.GetState() != "fenced" || afterWriter.Role != "fenced" || afterWriter.ObservedEpoch != winner.CurrentEpoch {
		t.Fatalf("timed-out demotion lifecycle = %+v", after)
	}
}

func TestProcessDemotionFailureObservesConcurrentTakeover(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	barrierPath := testSocket(t)
	barrier, err := net.Listen("unix", barrierPath)
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	ownerOptions := Options{
		Socket: testSocket(t), RepositoryID: "demotion-failure-takeover", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
		testEnvironment: []string{
			"VAULTICDB_TEST_DEMOTION_BARRIER=" + barrierPath,
			"VAULTICDB_TEST_FAILPOINTS=flush-writer",
		},
	}
	owner, err := Ensure(ctx, ownerOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(ctx)
	before, err := owner.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	takeoverOptions := ownerOptions
	takeoverOptions.Socket = testSocket(t)
	takeoverOptions.testEnvironment = nil
	takeover, err := Ensure(ctx, takeoverOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer takeover.Close(ctx)

	demotion := make(chan error, 1)
	go func() {
		_, demotionErr := owner.DemoteWriter(ctx, "failure during takeover", true, time.Second)
		demotion <- demotionErr
	}()
	connection, err := barrier.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	name, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil || strings.TrimSpace(name) != "VAULTICDB_TEST_DEMOTION_BARRIER" {
		t.Fatalf("demotion barrier = %q, err=%v", name, err)
	}
	winner, err := takeover.PromoteWriterWithTakeover(ctx, "take over failing demotion", true, before.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	detail := requireRPCDetail(t, <-demotion, codes.Aborted, "writer_fenced", false)
	if detail.GetGeneration() != winner.CurrentEpoch {
		t.Fatalf("writer fence generation = %d, want %d", detail.GetGeneration(), winner.CurrentEpoch)
	}
	after := daemonHealth(t, owner)
	afterWriter, statusErr := owner.WriterStatus(ctx)
	if statusErr != nil || after.GetState() != "fenced" || afterWriter.Role != "fenced" || afterWriter.ObservedEpoch != winner.CurrentEpoch {
		t.Fatalf("failed demotion lifecycle = %+v", after)
	}
}

func TestProcessDemotionWriterFlushFailureRetries(t *testing.T) {
	ctx := context.Background()
	options := Options{
		Socket: testSocket(t), RepositoryID: "demotion-writer-flush", DaemonPath: failureDaemonBinary(t),
		DataDir: t.TempDir(), ObjectStore: "local",
		testEnvironment: []string{"VAULTICDB_TEST_FAILPOINTS=flush-writer"},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	before, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DemoteWriter(ctx, "flush failure", true, time.Second)
	requireRPCDetail(t, err, codes.Unavailable, "storage_unavailable", true)
	after, err := client.WriterStatus(ctx)
	if err != nil || after.Role != "read-write" || after.CurrentEpoch != before.CurrentEpoch {
		t.Fatalf("writer after flush failure = %+v, before=%+v, err=%v", after, before, err)
	}
	demoted, err := client.DemoteWriter(ctx, "retry flush", true, time.Second)
	if err != nil || demoted.Role != "read-only" {
		t.Fatalf("flush demotion retry = %+v, err=%v", demoted, err)
	}
}

func TestProcessDemotionWriterCloseFailureRecoversAfterRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	options := Options{
		Socket: testSocket(t), RepositoryID: "demotion-writer-close", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
		testEnvironment: []string{"VAULTICDB_TEST_FAILPOINTS=close-writer"},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DemoteWriter(ctx, "close failure", true, time.Second)
	requireRPCDetail(t, err, codes.FailedPrecondition, "writer_role", false)
	health := daemonHealth(t, client)
	if health.GetReady() || health.GetState() != "failed" {
		t.Fatalf("writer close failure lifecycle = %+v", health)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	options.Socket = testSocket(t)
	options.testEnvironment = nil
	restarted, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	reader, err := restarted.WriterStatus(ctx)
	if err != nil || reader.Role != "read-only" || reader.ObservedEpoch != writer.CurrentEpoch {
		t.Fatalf("writer close restart = %+v, before=%+v, err=%v", reader, writer, err)
	}
	promoted, err := restarted.PromoteWriterWithTakeover(ctx, "recover close uncertainty", true, reader.ObservedEpoch)
	if err != nil || promoted.Role != "read-write" || promoted.CurrentEpoch <= reader.ObservedEpoch {
		t.Fatalf("writer close takeover = %+v, err=%v", promoted, err)
	}
}

func TestProcessDemotionClaimReleaseFailureRecoversAfterRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	options := Options{
		Socket: testSocket(t), RepositoryID: "demotion-claim-release", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
		testEnvironment: []string{"VAULTICDB_TEST_FAILPOINTS=release-writer-claim"},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	before := daemonHealth(t, client)
	beforeWriter, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DemoteWriter(ctx, "process failure test", true, time.Second)
	detail := requireRPCDetail(t, err, codes.FailedPrecondition, "writer_role", false)
	after := daemonHealth(t, client)
	if !after.GetReady() || after.GetState() != "fenced" || after.GetStateDetail() != "demotion failed" || after.GetStateSinceUnixMs() < before.GetStateSinceUnixMs() {
		t.Fatalf("claim-release failure lifecycle: writer=%+v detail=%+v before=%+v after=%+v", beforeWriter, detail, before, after)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	options.Socket = testSocket(t)
	options.testEnvironment = nil
	restarted, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	reader, err := restarted.WriterStatus(ctx)
	if err != nil || reader.Role != "read-only" || reader.ObservedEpoch == 0 {
		t.Fatalf("restarted status = %+v, err=%v", reader, err)
	}
	promoted, err := restarted.PromoteWriterWithTakeover(ctx, "recover retained claim", true, reader.ObservedEpoch)
	if err != nil || promoted.Role != "read-write" || promoted.CurrentEpoch <= reader.ObservedEpoch {
		t.Fatalf("recovery promotion = %+v, err=%v", promoted, err)
	}
	demoted, err := restarted.DemoteWriter(ctx, "retry demotion", true, time.Second)
	if err != nil || demoted.Role != "read-only" || demoted.CurrentEpoch != 0 || demoted.ObservedEpoch == 0 {
		t.Fatalf("demotion retry = %+v, err=%v", demoted, err)
	}
}

func TestProcessRollbackFenceRefreshFailureReconciles(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	options := Options{
		Socket: testSocket(t), RepositoryID: "rollback-reconciliation", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := client.GenerationStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	quarantined, err := client.QuarantineGeneration(ctx, initial.ActiveGeneration, strings.Repeat("aa", 32))
	if err != nil {
		t.Fatal(err)
	}
	activated, err := client.ActivateGeneration(ctx, quarantined.ActiveGeneration, quarantined.ActiveGeneration+1, "candidate", strings.Repeat("bb", 32), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	options.Socket = testSocket(t)
	options.testEnvironment = []string{"VAULTICDB_TEST_FAILPOINTS=refresh-writer-fence"}
	client, err = Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.RollbackGeneration(ctx, activated.Decision, strings.Repeat("cc", 32), 0)
	detail := requireRPCDetail(t, err, codes.Unavailable, "generation_reconciliation_pending", true)
	if !errors.Is(err, ErrGenerationPending) || detail.GetGeneration() != initial.ActiveGeneration {
		t.Fatalf("rollback reconciliation detail = %+v, err=%v", detail, err)
	}
	health := daemonHealth(t, client)
	if !health.GetReady() || health.GetState() != "fenced" || health.GetStateDetail() != "generation rollback committed; writer fence refresh failed" {
		t.Fatalf("rollback reconciliation lifecycle = %+v", health)
	}
	committed, err := client.GenerationStatus(ctx)
	if err != nil || committed.State != "rollback-observation" || committed.ActiveGeneration != initial.ActiveGeneration || committed.Decision <= activated.Decision {
		t.Fatalf("committed rollback status = %+v, err=%v", committed, err)
	}
	writer, err := client.WriterStatus(ctx)
	if err != nil || writer.Role != "fenced" || writer.CurrentEpoch == 0 {
		t.Fatalf("fenced writer status = %+v, err=%v", writer, err)
	}
	reconciled, err := client.RollbackGeneration(ctx, activated.Decision, strings.Repeat("cc", 32), 0)
	if err != nil || reconciled.Decision != committed.Decision || reconciled.State != "rollback-observation" {
		t.Fatalf("same-process rollback reconciliation = %+v, err=%v", reconciled, err)
	}
	reconciledWriter, err := client.WriterStatus(ctx)
	if err != nil || reconciledWriter.Role != "read-write" || reconciledWriter.CurrentEpoch <= writer.CurrentEpoch {
		t.Fatalf("same-process reconciled writer = %+v, fenced=%+v, err=%v", reconciledWriter, writer, err)
	}
	readerOptions := options
	readerOptions.Socket = testSocket(t)
	readerOptions.testEnvironment = nil
	reader, err := Ensure(ctx, readerOptions)
	if err != nil {
		t.Fatal(err)
	}
	readerWriter, err := reader.WriterStatus(ctx)
	if err != nil || readerWriter.Role != "read-only" || readerWriter.CurrentEpoch != 0 {
		t.Fatalf("rollback reader writer before replay = %+v, err=%v", readerWriter, err)
	}
	replayed, err := reader.RollbackGeneration(ctx, activated.Decision, strings.Repeat("cc", 32), 0)
	if err != nil || replayed.Decision != committed.Decision || replayed.State != "rollback-observation" {
		t.Fatalf("rollback replay through reader = %+v, err=%v", replayed, err)
	}
	readerWriter, err = reader.WriterStatus(ctx)
	readerHealth := daemonHealth(t, reader)
	if err != nil || readerWriter.Role != "read-only" || readerWriter.CurrentEpoch != 0 || !readerHealth.GetReady() || readerHealth.GetState() != "read_only" {
		t.Fatalf("rollback reader after replay: writer=%+v health=%+v err=%v", readerWriter, readerHealth, err)
	}
	if err := reader.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	options.Socket = testSocket(t)
	options.testEnvironment = []string{"VAULTICDB_TEST_FAILPOINTS=refresh-writer-fence"}
	if failedStartup, startupErr := Ensure(ctx, options); startupErr == nil {
		_ = failedStartup.Close(ctx)
		t.Fatal("startup reconciliation unexpectedly succeeded")
	}

	drainBarrierPath := testSocket(t)
	drainBarrier, err := net.Listen("unix", drainBarrierPath)
	if err != nil {
		t.Fatal(err)
	}
	defer drainBarrier.Close()
	options.Socket = testSocket(t)
	options.testEnvironment = []string{"VAULTICDB_TEST_DRAIN_BARRIER=" + drainBarrierPath}
	restarted, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	afterRestart, err := restarted.GenerationStatus(ctx)
	if err != nil || afterRestart.Decision != committed.Decision || afterRestart.ActiveGeneration != initial.ActiveGeneration {
		t.Fatalf("restarted rollback status = %+v, err=%v", afterRestart, err)
	}
	restartedWriter, err := restarted.WriterStatus(ctx)
	if err != nil || restartedWriter.Role != "read-write" || restartedWriter.CurrentEpoch <= writer.CurrentEpoch || restartedWriter.CurrentEpoch != restartedWriter.ObservedEpoch {
		t.Fatalf("restarted rollback writer = %+v, previous=%+v, err=%v", restartedWriter, writer, err)
	}
	reconciled, err = restarted.RollbackGeneration(ctx, activated.Decision, strings.Repeat("cc", 32), 0)
	if err != nil || reconciled.Decision != committed.Decision || reconciled.State != "rollback-observation" {
		t.Fatalf("rollback retry after restart = %+v, err=%v", reconciled, err)
	}
	if recovered := daemonHealth(t, restarted); !recovered.GetReady() || recovered.GetState() != "read_write" {
		t.Fatalf("reconciled lifecycle = %+v", recovered)
	}
	beforeDrain, err := restarted.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	drainResult := make(chan error, 1)
	go func() {
		_, drainErr := restarted.RPC().Drain(ctx, &vaulticdbv1.Empty{Context: requestContext(ctx)})
		drainResult <- drainErr
	}()
	connection, err := drainBarrier.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	name, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil || strings.TrimSpace(name) != "VAULTICDB_TEST_DRAIN_BARRIER" {
		t.Fatalf("drain barrier = %q, err=%v", name, err)
	}
	rollbackResult := make(chan error, 1)
	go func() {
		_, rollbackErr := restarted.RollbackGeneration(ctx, activated.Decision, strings.Repeat("cc", 32), 0)
		rollbackResult <- rollbackErr
	}()
	if _, err := connection.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := <-drainResult; err != nil {
		t.Fatal(err)
	}
	requireRPCDetail(t, <-rollbackResult, codes.Unavailable, "storage_unavailable", true)
	afterDrain, err := restarted.WriterStatus(ctx)
	if err != nil || afterDrain.CurrentEpoch != beforeDrain.CurrentEpoch {
		t.Fatalf("drained rollback changed writer epoch: before=%+v after=%+v err=%v", beforeDrain, afterDrain, err)
	}
}

func failureTestSealedTopology(t *testing.T, repositoryID string, generation uint64) []byte {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate failure recovery test source")
	}
	encoded, err := os.ReadFile(filepath.Join(filepath.Dir(source), "..", "..", "..", "testdata", "topology-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	document, err := topology.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	document.RepositoryID = repositoryID
	document.TopologyGeneration = generation
	encoded, err = document.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestProcessCapsuleMigrationResumesAfterPublicationCrashes(t *testing.T) {
	for _, boundary := range []string{
		"capsule-intent-persisted",
		"capsule-local-visible",
		"capsule-local-recorded",
		"capsule-mirror-visible",
		"capsule-mirror-recorded",
	} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			dataDir := t.TempDir()
			capsuleDir := t.TempDir()
			passphraseFile := filepath.Join(t.TempDir(), "recovery-passphrase")
			if err := os.WriteFile(passphraseFile, []byte("process capsule recovery passphrase\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			repositoryID := "capsule-" + boundary
			options := Options{
				Socket: testSocket(t), RepositoryID: repositoryID, DaemonPath: failureDaemonBinary(t),
				DataDir: dataDir, ObjectStore: "local", EncryptionMode: "initialize", PassphraseFile: passphraseFile,
				testEnvironment: []string{"VAULTICDB_TEST_CRASHPOINTS=" + boundary},
			}
			client, err := Ensure(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			masterKey := bytes.Repeat([]byte{7}, 32)
			if err := client.StoreMasterKey(ctx, masterKey); err != nil {
				t.Fatal(err)
			}
			publicKey, _, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			members := []OfflineCapsuleMember{{ID: "operator", Provider: "offline-keyfile", Credential: bytes.Repeat([]byte{9}, 32)}}
			sealedTopology := failureTestSealedTopology(t, repositoryID, 1)
			_, err = client.PrepareCapsuleMigration(ctx, capsuleDir, 1, "operators", 1, publicKey, members, sealedTopology)
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("crashed migration RPC code = %v, want Unavailable: %v", status.Code(err), err)
			}
			_ = client.Close(ctx)

			options.EncryptionMode = "required"
			options.testEnvironment = nil
			resumed, err := Ensure(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := resumed.WriterStatus(ctx)
			if err != nil || reader.Role != "read-only" || reader.ObservedEpoch == 0 {
				t.Fatalf("post-crash writer claim = %+v, err=%v", reader, err)
			}
			if _, err := resumed.PromoteWriterWithTakeover(ctx, "resume capsule migration", true, reader.ObservedEpoch); err != nil {
				t.Fatal(err)
			}
			migration, err := resumed.PrepareCapsuleMigration(ctx, capsuleDir, 1, "operators", 1, publicKey, members, sealedTopology)
			if err != nil {
				t.Fatal(err)
			}
			localBytes, err := os.ReadFile(migration.LocalPath)
			if err != nil || !bytes.Equal(localBytes, migration.Capsule) {
				t.Fatalf("resumed local capsule differs: err=%v", err)
			}
			keyStatus, err := resumed.KeyStatus(ctx)
			if err != nil || keyStatus.PendingCapsuleMigrationSHA256 != migration.CapsuleSHA256 {
				t.Fatalf("pending migration status = %+v, err=%v", keyStatus, err)
			}
			if err := resumed.Close(ctx); err != nil {
				t.Fatal(err)
			}

			options.testEnvironment = []string{"VAULTICDB_TEST_FAILPOINTS=finalize-capsule-durability"}
			resumed, err = Ensure(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := resumed.PrepareCapsuleMigration(ctx, capsuleDir, 1, "operators", 1, publicKey, members, sealedTopology)
			if err != nil || !bytes.Equal(replayed.Capsule, migration.Capsule) || replayed.LocalPath != migration.LocalPath || replayed.MirrorPath != migration.MirrorPath {
				t.Fatalf("replayed migration = %+v, err=%v", replayed, err)
			}
			proofFor := func(digest string) []byte {
				proof := hmac.New(sha256.New, masterKey)
				_, _ = proof.Write([]byte("vaultic-capsule-migration-finalize-v1\x00"))
				_, _ = proof.Write([]byte(repositoryID))
				_, _ = proof.Write([]byte{0})
				_, _ = proof.Write([]byte(digest))
				return proof.Sum(nil)
			}
			wrongDigest := strings.Repeat("0", 64)
			if err := resumed.FinalizeCapsuleMigration(ctx, wrongDigest, proofFor(wrongDigest)); err == nil {
				t.Fatal("mismatched capsule digest unexpectedly finalized")
			} else {
				requireRPCDetail(t, err, codes.FailedPrecondition, "precondition_failed", false)
			}
			if err := os.WriteFile(migration.LocalPath, []byte("corrupted capsule\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := resumed.FinalizeCapsuleMigration(ctx, migration.CapsuleSHA256, proofFor(migration.CapsuleSHA256)); err == nil {
				t.Fatal("corrupted capsule artifact unexpectedly finalized")
			} else {
				requireRPCDetail(t, err, codes.FailedPrecondition, "key_management", false)
			}
			pending, err := resumed.KeyStatus(ctx)
			if err != nil || pending.PendingCapsuleMigrationSHA256 != migration.CapsuleSHA256 {
				t.Fatalf("corrupt artifact changed pending migration = %+v, err=%v", pending, err)
			}
			if err := os.Remove(migration.LocalPath); err != nil {
				t.Fatal(err)
			}
			if err := resumed.FinalizeCapsuleMigration(ctx, migration.CapsuleSHA256, proofFor(migration.CapsuleSHA256)); err != nil {
				requireRPCDetail(t, err, codes.Unavailable, "storage_unavailable", true)
			} else {
				t.Fatal("injected capsule finalization durability failure did not fire")
			}
			if localBytes, err := os.ReadFile(migration.LocalPath); err != nil || !bytes.Equal(localBytes, migration.Capsule) {
				t.Fatalf("finalization did not recreate exact local capsule: err=%v", err)
			}
			if err := resumed.FinalizeCapsuleMigration(ctx, migration.CapsuleSHA256, nil); err != nil {
				t.Fatalf("repeated finalization: %v", err)
			}
			if err := resumed.Close(ctx); err != nil {
				t.Fatal(err)
			}
			options.testEnvironment = nil
			resumed, err = Ensure(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			finalized, err := resumed.KeyStatus(ctx)
			if err != nil || finalized.PendingCapsuleMigrationSHA256 != "" || finalized.FinalizedCapsuleMigrationSHA256 != migration.CapsuleSHA256 {
				t.Fatalf("finalized migration after restart = %+v, err=%v", finalized, err)
			}
			if err := resumed.Close(ctx); err != nil {
				t.Fatal(err)
			}
			for _, entry := range []string{options.Socket, strings.TrimSuffix(options.Socket, filepath.Ext(options.Socket)) + ".pid", strings.TrimSuffix(options.Socket, filepath.Ext(options.Socket)) + ".cap"} {
				if _, err := os.Lstat(entry); !os.IsNotExist(err) {
					t.Fatalf("runtime artifact remained after recovery: %s (%v)", entry, err)
				}
			}
			entries, err := os.ReadDir(capsuleDir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.Contains(entry.Name(), ".tmp") || strings.Contains(entry.Name(), ".staging") {
					t.Fatalf("staging artifact remained after recovery: %s", entry.Name())
				}
			}
		})
	}
}

func TestProcessCapsuleMigrationRecordsRotatedDEKVersion(t *testing.T) {
	ctx := context.Background()
	passphraseFile := filepath.Join(t.TempDir(), "recovery-passphrase")
	if err := os.WriteFile(passphraseFile, []byte("rotated capsule passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	repositoryID := "capsule-rotated-dek"
	options := Options{
		Socket: testSocket(t), RepositoryID: repositoryID, DaemonPath: daemonBinary(t),
		DataDir: t.TempDir(), ObjectStore: "local", EncryptionMode: "initialize", PassphraseFile: passphraseFile,
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	if err := client.StoreMasterKey(ctx, bytes.Repeat([]byte{7}, 32)); err != nil {
		t.Fatal(err)
	}
	rotated, err := client.RotateDEK(ctx)
	if err != nil || rotated.ActiveDEKVersion != 2 {
		t.Fatalf("rotated key status = %+v, err=%v", rotated, err)
	}
	for {
		progress, rewriteErr := client.RewriteDEK(ctx, 1000)
		if rewriteErr != nil {
			t.Fatal(rewriteErr)
		}
		if progress.Remaining == 0 {
			break
		}
	}
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	migration, err := client.PrepareCapsuleMigration(
		ctx, t.TempDir(), 1, "operators", 1, publicKey,
		[]OfflineCapsuleMember{{ID: "operator", Provider: "offline-keyfile", Credential: bytes.Repeat([]byte{9}, 32)}},
		failureTestSealedTopology(t, repositoryID, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	var capsule struct {
		Header struct {
			MetadataDEKVersion uint32 `json:"metadata_dek_version"`
		} `json:"header"`
	}
	if err := json.Unmarshal(migration.Capsule, &capsule); err != nil {
		t.Fatal(err)
	}
	if capsule.Header.MetadataDEKVersion != rotated.ActiveDEKVersion {
		t.Fatalf("capsule DEK version = %d, want %d", capsule.Header.MetadataDEKVersion, rotated.ActiveDEKVersion)
	}
}

func TestProcessDrainRejectsEveryMutationClassAndRestarts(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	barrierPath := testSocket(t)
	barrier, err := net.Listen("unix", barrierPath)
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	options := Options{
		Socket: testSocket(t), RepositoryID: "drain-mutations", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
		testEnvironment: []string{
			"VAULTICDB_TEST_MUTATION_BARRIER=" + barrierPath,
			"VAULTICDB_TEST_DRAIN_BARRIER=" + barrierPath,
			"VAULTICDB_TEST_MUTATION_COMPLETE_BARRIER=" + barrierPath,
			"VAULTICDB_TEST_DRAIN_ACQUIRED_BARRIER=" + barrierPath,
		},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	writerBefore, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type barrierEvent struct {
		name string
		conn net.Conn
		err  error
	}
	accept := func() <-chan barrierEvent {
		result := make(chan barrierEvent, 1)
		go func() {
			connection, acceptErr := barrier.Accept()
			if acceptErr != nil {
				result <- barrierEvent{err: acceptErr}
				return
			}
			name, readErr := bufio.NewReader(connection).ReadString('\n')
			result <- barrierEvent{name: strings.TrimSpace(name), conn: connection, err: readErr}
		}()
		return result
	}
	writes := make(chan error, 1)
	go func() {
		_, writeErr := client.WriteBatch(ctx, []Mutation{{Key: []byte("before-drain"), Value: []byte("value")}}, nil, true, "")
		writes <- writeErr
	}()
	mutationEntered := <-accept()
	if mutationEntered.err != nil || mutationEntered.name != "VAULTICDB_TEST_MUTATION_BARRIER" {
		t.Fatalf("mutation barrier = %+v", mutationEntered)
	}
	drains := make(chan error, 1)
	go func() {
		_, drainErr := client.RPC().Drain(ctx, &vaulticdbv1.Empty{Context: requestContext(ctx)})
		drains <- drainErr
	}()
	drainWaiting := <-accept()
	if drainWaiting.err != nil || drainWaiting.name != "VAULTICDB_TEST_DRAIN_BARRIER" {
		t.Fatalf("drain waiting barrier = %+v", drainWaiting)
	}
	if _, err := drainWaiting.conn.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if _, err := mutationEntered.conn.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	mutationComplete := <-accept()
	if mutationComplete.err != nil || mutationComplete.name != "VAULTICDB_TEST_MUTATION_COMPLETE_BARRIER" {
		t.Fatalf("next barrier = %+v, want mutation completion before drain acquisition", mutationComplete)
	}
	if _, err := mutationComplete.conn.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	drainAcquired := <-accept()
	if drainAcquired.err != nil || drainAcquired.name != "VAULTICDB_TEST_DRAIN_ACQUIRED_BARRIER" {
		t.Fatalf("drain acquired barrier = %+v", drainAcquired)
	}
	if _, err := drainAcquired.conn.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := <-writes; err != nil {
		t.Fatalf("admitted mutation: %v", err)
	}
	if err := <-drains; err != nil {
		t.Fatal(err)
	}
	drained := daemonHealth(t, client)
	if drained.GetReady() || drained.GetState() != "draining" || drained.GetStateDetail() != "drain requested" {
		t.Fatalf("drained lifecycle = %+v", drained)
	}
	checks := map[string]func() error{
		"write-batch": func() error {
			_, err := client.WriteBatch(ctx, []Mutation{{Key: []byte("drained"), Value: []byte("value")}}, nil, true, "")
			return err
		},
		"begin-transaction": func() error {
			_, err := client.Begin(ctx)
			return err
		},
		"writer-transition": func() error {
			_, err := client.DemoteWriter(ctx, "drained", true, time.Second)
			return err
		},
		"generation": func() error {
			_, err := client.QuarantineGeneration(ctx, 1, strings.Repeat("dd", 32))
			return err
		},
		"key-management": func() error {
			_, err := client.RotateDEK(ctx)
			return err
		},
		"capsule": func() error {
			_, err := client.PrepareCapsuleMigration(ctx, t.TempDir(), 1, "operators", 1, []byte("public-key"), nil, []byte("topology"))
			return err
		},
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			err := check()
			requireRPCDetail(t, err, codes.Unavailable, "storage_unavailable", true)
			if !errors.Is(err, ErrStorageUnavailable) {
				t.Fatalf("drained mutation error = %v, want ErrStorageUnavailable", err)
			}
		})
	}
	after := daemonHealth(t, client)
	if after.GetStateSinceUnixMs() != drained.GetStateSinceUnixMs() || after.GetState() != drained.GetState() {
		t.Fatalf("drain lifecycle changed while rejecting mutations: before=%+v after=%+v", drained, after)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	options.Socket = testSocket(t)
	options.testEnvironment = nil
	restarted, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	writerAfter, err := restarted.WriterStatus(ctx)
	if err != nil || writerAfter.Role != "read-write" || writerAfter.CurrentEpoch <= writerBefore.CurrentEpoch {
		t.Fatalf("restarted writer = %+v, before=%+v, err=%v", writerAfter, writerBefore, err)
	}
	if durable, err := restarted.WriteBatch(ctx, []Mutation{{Key: []byte("after-drain"), Value: []byte("ok")}}, nil, true, ""); err != nil || !durable {
		t.Fatalf("mutation after restart = durable %t, err=%v", durable, err)
	}
}

func TestProcessStorageProviderOutageRecoversAndPersists(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	options := Options{
		Socket: testSocket(t), RepositoryID: "provider-recovery", DaemonPath: failureDaemonBinary(t),
		DataDir: dataDir, ObjectStore: "local",
		testEnvironment: []string{"VAULTICDB_TEST_FAILPOINTS=provider-unavailable"},
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	before := daemonHealth(t, client)
	_, err = client.WriteBatchWithIdempotency(ctx, []Mutation{{Key: []byte("provider-key"), Value: []byte("provider-value")}}, nil, true, "", "provider-request")
	requireRPCDetail(t, err, codes.Unavailable, "storage_unavailable", true)
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("provider error = %v, want ErrStorageUnavailable", err)
	}
	after := daemonHealth(t, client)
	if !after.GetReady() || after.GetState() != "read_write" || after.GetStateSinceUnixMs() != before.GetStateSinceUnixMs() {
		t.Fatalf("provider outage changed lifecycle: before=%+v after=%+v", before, after)
	}
	if durable, err := client.WriteBatchWithIdempotency(ctx, []Mutation{{Key: []byte("provider-key"), Value: []byte("provider-value")}}, nil, true, "", "provider-request"); err != nil || !durable {
		t.Fatalf("provider retry = durable %t, err=%v", durable, err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}

	options.Socket = testSocket(t)
	options.testEnvironment = nil
	restarted, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	value, found, err := restarted.Get(ctx, []byte("provider-key"), "")
	if err != nil || !found || string(value) != "provider-value" {
		t.Fatalf("restarted provider value = %q, found=%t, err=%v", value, found, err)
	}
	recordBytes, found, err := restarted.Get(ctx, []byte("meta:idempotency:provider-request"), "")
	if err != nil || !found {
		t.Fatalf("restarted provider idempotency record: found=%t, err=%v", found, err)
	}
	var record durableIdempotencyRecord
	if err := json.Unmarshal(recordBytes, &record); err != nil || record.Operation != "write-batch" || !record.Durable {
		t.Fatalf("restarted provider idempotency record = %+v, err=%v", record, err)
	}
}
