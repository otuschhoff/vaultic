package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type packCancelKey struct{}

type canceledBeginRPC struct {
	vaulticdbv1.VaulticDBClient
	cancel          context.CancelFunc
	caller          context.Context
	hideResponse    bool
	response        *vaulticdbv1.BeginResponse
	rollbackErr     error
	begins          atomic.Uint64
	rpcDeadline     time.Time
	logicalDeadline int64
}

func (rpc *canceledBeginRPC) Begin(ctx context.Context, request *vaulticdbv1.Empty, options ...grpc.CallOption) (*vaulticdbv1.BeginResponse, error) {
	rpc.begins.Add(1)
	rpc.rpcDeadline, _ = ctx.Deadline()
	rpc.logicalDeadline = request.GetContext().GetDeadlineUnixMs()
	response, err := rpc.VaulticDBClient.Begin(ctx, request, options...)
	if err != nil {
		return response, err
	}
	return rpc.finish(ctx, response)
}

func (rpc *canceledBeginRPC) BeginPublication(ctx context.Context, request *vaulticdbv1.BeginPublicationRequest, options ...grpc.CallOption) (*vaulticdbv1.BeginResponse, error) {
	rpc.begins.Add(1)
	rpc.rpcDeadline, _ = ctx.Deadline()
	rpc.logicalDeadline = request.GetContext().GetDeadlineUnixMs()
	response, err := rpc.VaulticDBClient.BeginPublication(ctx, request, options...)
	if err != nil {
		return response, err
	}
	return rpc.finish(ctx, response)
}

func (rpc *canceledBeginRPC) BeginOwned(ctx context.Context, request *vaulticdbv1.BeginOwnedRequest, options ...grpc.CallOption) (*vaulticdbv1.BeginResponse, error) {
	rpc.begins.Add(1)
	rpc.rpcDeadline, _ = ctx.Deadline()
	rpc.logicalDeadline = request.GetContext().GetDeadlineUnixMs()
	response, err := rpc.VaulticDBClient.BeginOwned(ctx, request, options...)
	if err != nil {
		return response, err
	}
	return rpc.finish(ctx, response)
}

func (rpc *canceledBeginRPC) finish(ctx context.Context, response *vaulticdbv1.BeginResponse) (*vaulticdbv1.BeginResponse, error) {
	if rpc.caller != nil {
		ctx = rpc.caller
	}
	rpc.response = response
	if rpc.cancel != nil {
		rpc.cancel()
	} else {
		<-ctx.Done()
	}
	if rpc.hideResponse {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return response, nil
}

func (rpc *canceledBeginRPC) Rollback(ctx context.Context, request *vaulticdbv1.TransactionRequest, options ...grpc.CallOption) (*vaulticdbv1.Empty, error) {
	if rpc.rollbackErr != nil {
		return nil, rpc.rollbackErr
	}
	return rpc.VaulticDBClient.Rollback(ctx, request, options...)
}

func TestClientCanceledBeginCleansOwnedTransaction(t *testing.T) {
	for _, scenario := range []struct{ publication, deadline, rollbackFail bool }{
		{}, {publication: true}, {deadline: true}, {publication: true, deadline: true},
		{rollbackFail: true}, {publication: true, rollbackFail: true},
		{deadline: true, rollbackFail: true}, {publication: true, deadline: true, rollbackFail: true},
	} {
		t.Run(fmt.Sprintf("publication=%t/deadline=%t/rollbackFail=%t", scenario.publication, scenario.deadline, scenario.rollbackFail), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "canceled-begin-cleanup", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close(context.Background())
			workCtx, stop := context.WithCancel(ctx)
			if scenario.deadline {
				stop()
				workCtx, stop = context.WithTimeout(ctx, 2*time.Second)
			}
			defer stop()
			rpc := &canceledBeginRPC{VaulticDBClient: client.rpc, cancel: stop, caller: workCtx}
			if scenario.deadline {
				rpc.cancel = nil
			}
			if scenario.rollbackFail {
				rpc.rollbackErr = status.Error(codes.Unavailable, "test rollback unavailable")
			}
			client.rpc = rpc
			client.limits.BeginReconciliation = false
			var transaction *Transaction
			var beginErr error
			started := time.Now()
			if scenario.publication {
				transaction, beginErr = client.beginPublication(workCtx, []schema.ID{daemonTestID(90)})
			} else {
				transaction, beginErr = client.Begin(workCtx)
			}
			logicalDeadline, _ := workCtx.Deadline()
			if rpc.rpcDeadline.Before(started.Add(defaultRPCDeadline)) || rpc.rpcDeadline.After(time.Now().Add(defaultRPCDeadline)) ||
				rpc.logicalDeadline != logicalDeadline.UnixMilli() {
				t.Fatalf("Begin handoff bound or logical deadline changed: rpc=%v logical=%d want=%v", rpc.rpcDeadline, rpc.logicalDeadline, logicalDeadline)
			}
			if transaction != nil {
				rpc.rollbackErr = nil
				_ = rollbackTransaction(workCtx, transaction)
				t.Fatal("canceled Begin returned transaction ownership instead of cleaning it up")
			}
			expectedErr, expectedCode := context.Canceled, codes.Canceled
			if scenario.deadline {
				expectedErr, expectedCode = context.DeadlineExceeded, codes.DeadlineExceeded
			}
			if !errors.Is(beginErr, expectedErr) || status.Code(beginErr) != expectedCode || rpc.begins.Load() != 1 {
				t.Fatalf("caller cancellation identity or attempts changed: err=%v begins=%d", beginErr, rpc.begins.Load())
			}
			var expectedTransactions uint64
			if scenario.rollbackFail {
				expectedTransactions = 1
				if !errors.Is(beginErr, rpc.rollbackErr) || !strings.Contains(beginErr.Error(), "rollback canceled Begin") {
					t.Fatalf("rollback failure hidden: %v", beginErr)
				}
			}
			after, err := client.WriterStatus(ctx)
			if err != nil || after.ActiveTransactions != expectedTransactions || after.ActiveWriteIntents != 0 || client.CommitRPCStats().Attempts != 0 {
				t.Fatalf("canceled Begin outcome: tx=%d intents=%d err=%v", after.ActiveTransactions, after.ActiveWriteIntents, err)
			}
			if scenario.rollbackFail {
				rpc.rollbackErr = nil
				owned, err := client.transactionFromBegin(rpc.response)
				if err != nil {
					t.Fatal(err)
				}
				if err := rollbackTransaction(workCtx, owned); err != nil {
					t.Fatal(err)
				}
			}
			settled, err := client.WriterStatus(ctx)
			if err != nil || settled.ActiveTransactions != 0 || settled.ActiveWriteIntents != 0 {
				t.Fatalf("fixture cleanup did not settle: tx=%d intents=%d err=%v", settled.ActiveTransactions, settled.ActiveWriteIntents, err)
			}
		})
	}
}

func TestClientCanceledBeginSkipsAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "canceled-begin-no-admission", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	for _, deadline := range []bool{false, true} {
		workCtx, stop := context.WithCancel(ctx)
		if deadline {
			stop()
			workCtx, stop = context.WithDeadline(ctx, time.Now().Add(-time.Second))
		}
		stop()
		rpc := &canceledBeginRPC{VaulticDBClient: client.rpc, cancel: stop}
		client.rpc = rpc
		for _, publication := range []bool{false, true} {
			var transaction *Transaction
			var beginErr error
			if publication {
				transaction, beginErr = client.beginPublication(workCtx, []schema.ID{daemonTestID(90)})
			} else {
				transaction, beginErr = client.Begin(workCtx)
			}
			if transaction != nil || !errors.Is(beginErr, workCtx.Err()) || rpc.begins.Load() != 0 {
				t.Fatalf("canceled caller admitted: deadline=%t publication=%t err=%v calls=%d", deadline, publication, beginErr, rpc.begins.Load())
			}
		}
		client.rpc = rpc.VaulticDBClient
	}
}

func TestSchemaStoreCanceledBeginOwnership(t *testing.T) {
	testSchemaStoreCanceledBeginOwnership(t, false)
}

func TestSchemaStoreOwnedBeginOwnership(t *testing.T) {
	testSchemaStoreCanceledBeginOwnership(t, true)
}

func testSchemaStoreCanceledBeginOwnership(t *testing.T, owned bool) {
	for _, scenario := range []struct {
		allocation, deadline, hideResponse bool
	}{
		{}, {hideResponse: true}, {deadline: true}, {deadline: true, hideResponse: true},
		{allocation: true}, {allocation: true, hideResponse: true},
		{allocation: true, deadline: true}, {allocation: true, deadline: true, hideResponse: true},
	} {
		t.Run(fmt.Sprintf("allocation=%t/deadline=%t/hideResponse=%t", scenario.allocation, scenario.deadline, scenario.hideResponse), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "canceled-begin-ownership", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close(context.Background())
			workCtx, stop := context.WithCancel(ctx)
			if scenario.deadline {
				stop()
				workCtx, stop = context.WithTimeout(ctx, 2*time.Second)
			}
			defer stop()
			rpc := &canceledBeginRPC{VaulticDBClient: client.rpc, cancel: stop, caller: workCtx, hideResponse: scenario.hideResponse}
			if scenario.deadline {
				rpc.cancel = nil
			}
			client.rpc = rpc
			if owned && !client.Limits().BeginReconciliation {
				t.Fatal("fixture did not negotiate owned Begin")
			}
			client.limits.BeginReconciliation = owned
			store := NewSchemaStore(client)
			published := readSessionTestPack(daemonTestID(20), daemonTestID(90))
			var expectedCalls, canceled, expired uint64 = 1, 1, 0
			expectedCode := codes.Canceled
			if scenario.deadline {
				expectedCode = codes.DeadlineExceeded
				canceled, expired = 0, 1
			}
			if scenario.allocation {
				first, aborted, allocationErr := store.AllocateRevisionBlockWithRetryCount(workCtx, 4)
				err = allocationErr
				if first != 0 || aborted != 0 {
					t.Fatalf("canceled allocation reserved revisions: first=%d aborted=%d", first, aborted)
				}
				expectedCalls, canceled, expired = 0, 0, 0
			} else {
				err = store.PublishPack(workCtx, published)
			}
			if status.Code(err) != expectedCode || rpc.response.GetTransactionId() == "" {
				t.Fatalf("Begin boundary not reached: response=%+v err=%v", rpc.response, err)
			}
			after, err := client.WriterStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			stats := store.PackPublicationStats()
			var expectedTransactions uint64
			if scenario.hideResponse && !owned {
				expectedTransactions = 1
			}
			if after.ActiveTransactions != expectedTransactions || after.ActiveWriteIntents != 0 ||
				client.CommitRPCStats().Attempts != 0 || stats.Calls != expectedCalls || stats.Failures != expectedCalls ||
				stats.FailedCanceled != canceled || stats.FailedTimedOut != expired || stats.Active != 0 ||
				stats.RecoveredAborts != 0 || stats.TerminalAborts != 0 {
				t.Fatalf("canceled Begin ownership: stats=%+v status=%+v", stats, after)
			}
			for _, key := range [][]byte{schema.PackKey(published.PackID), schema.BlobKey(daemonTestID(90)),
				schema.PackAggregateKey(schema.AggregateAll), schema.NextRevisionKey()} {
				if _, found, err := store.Get(ctx, key); err != nil || found {
					t.Fatalf("canceled Begin changed metadata: key=%x found=%t err=%v", key, found, err)
				}
			}
			if scenario.hideResponse && !owned {
				transaction, err := client.transactionFromBegin(rpc.response)
				if err != nil {
					t.Fatal(err)
				}
				if err := rollbackTransaction(workCtx, transaction); err != nil {
					t.Fatalf("known-ID rollback inherited cancellation: %v", err)
				}
				settled, err := client.WriterStatus(ctx)
				if err != nil || settled.ActiveTransactions != 0 || settled.ActiveWriteIntents != 0 {
					t.Fatalf("fixture cleanup did not settle: status=%+v err=%v", settled, err)
				}
			}
			t.Logf("owned=%t hidden_response=%t residual_transactions=%d cleanup=settled", owned, scenario.hideResponse, after.ActiveTransactions)
		})
	}
}

type withheldBeginService struct {
	vaulticdbv1.UnimplementedVaulticDBServer
	upstream        vaulticdbv1.VaulticDBClient
	responses       chan *vaulticdbv1.BeginResponse
	finished        chan struct{}
	release         <-chan struct{}
	dropReply       bool
	owned           *vaulticdbv1.BeginOwnedRequest
	beginCalls      atomic.Uint64
	cancelCalls     atomic.Uint64
	cancelReplyLoss uint64
	cancelFailure   bool
	emptyReply      bool
}

func (service *withheldBeginService) BeginOwned(ctx context.Context, request *vaulticdbv1.BeginOwnedRequest) (*vaulticdbv1.BeginResponse, error) {
	defer close(service.finished)
	service.beginCalls.Add(1)
	service.owned = request
	response, err := service.upstream.BeginOwned(ctx, request)
	if err != nil {
		return nil, err
	}
	service.responses <- response
	select {
	case <-service.release:
		if service.dropReply {
			return nil, status.Error(codes.Unavailable, "test independent response loss")
		}
		if service.emptyReply {
			return &vaulticdbv1.BeginResponse{}, nil
		}
		return response, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (service *withheldBeginService) CancelBegin(ctx context.Context, request *vaulticdbv1.CancelBeginRequest) (*vaulticdbv1.Empty, error) {
	calls := service.cancelCalls.Add(1)
	if request.GetBeginRequestId() != service.owned.GetBeginRequestId() || request.GetBeginDeadlineUnixMs() != service.owned.GetBeginDeadlineUnixMs() {
		return nil, status.Error(codes.InvalidArgument, "cleanup identity changed")
	}
	if service.cancelFailure {
		return nil, status.Error(codes.PermissionDenied, "test cleanup rejected")
	}
	response, err := service.upstream.CancelBegin(ctx, request)
	if err == nil && calls <= service.cancelReplyLoss {
		return nil, status.Error(codes.Unavailable, "test cleanup response loss")
	}
	return response, err
}

func (service *withheldBeginService) Begin(ctx context.Context, request *vaulticdbv1.Empty) (*vaulticdbv1.BeginResponse, error) {
	defer close(service.finished)
	response, err := service.upstream.Begin(ctx, request)
	if err != nil {
		return nil, err
	}
	service.responses <- response
	select {
	case <-service.release:
		if service.dropReply {
			return nil, status.Error(codes.Unavailable, "test independent response loss")
		}
		return response, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (service *withheldBeginService) Rollback(ctx context.Context, request *vaulticdbv1.TransactionRequest) (*vaulticdbv1.Empty, error) {
	return service.upstream.Rollback(ctx, request)
}

func TestClientOwnedBeginReconciliationOverGRPC(t *testing.T) {
	for _, publication := range []bool{false, true} {
		for _, scenario := range []struct {
			name                                                              string
			cancel, deadline, dropReply, emptyReply, cancelFailure, rpcExpiry bool
			cancelReplyLoss                                                   uint64
		}{
			{name: "lost-begin", dropReply: true},
			{name: "canceled", cancel: true},
			{name: "expired", deadline: true},
			{name: "canceled-lost-begin", cancel: true, dropReply: true},
			{name: "expired-lost-begin", deadline: true, dropReply: true},
			{name: "lost-cleanup-reply", dropReply: true, cancelReplyLoss: 1},
			{name: "both-cleanup-replies-lost", dropReply: true, cancelReplyLoss: 2},
			{name: "cleanup-rejected", dropReply: true, cancelFailure: true},
			{name: "malformed-reply", emptyReply: true},
			{name: "rpc-deadline", rpcExpiry: true},
		} {
			t.Run(fmt.Sprintf("publication=%t/%s", publication, scenario.name), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "owned-begin-grpc", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close(context.Background())
				if !client.Limits().BeginReconciliation {
					t.Fatal("fixture did not negotiate Begin reconciliation")
				}
				originalRPC := client.rpc
				workCtx, stop := context.WithCancel(ctx)
				if scenario.deadline {
					stop()
					workCtx, stop = context.WithTimeout(ctx, 2*time.Second)
				}
				defer stop()
				release := make(chan struct{})
				service := &withheldBeginService{upstream: originalRPC, responses: make(chan *vaulticdbv1.BeginResponse, 1), finished: make(chan struct{}), release: release, dropReply: scenario.dropReply, emptyReply: scenario.emptyReply, cancelFailure: scenario.cancelFailure, cancelReplyLoss: scenario.cancelReplyLoss}
				if scenario.cancel || scenario.deadline {
					service.release = workCtx.Done()
				} else if !scenario.rpcExpiry {
					close(release)
				}
				socket := testSocket(t)
				listener, err := net.Listen("unix", socket)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				if err := os.Chmod(socket, 0o600); err != nil {
					t.Fatal(err)
				}
				server := grpc.NewServer()
				vaulticdbv1.RegisterVaulticDBServer(server, service)
				go func() { _ = server.Serve(listener) }()
				defer server.Stop()
				connection, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatal(err)
				}
				defer connection.Close()
				client.rpc = vaulticdbv1.NewVaulticDBClient(connection)
				defer func() { client.rpc = originalRPC }()
				result := make(chan error, 1)
				started := time.Now()
				go func() {
					var transaction *Transaction
					var beginErr error
					if publication {
						transaction, beginErr = client.beginPublication(workCtx, []schema.ID{daemonTestID(90)})
					} else {
						transaction, beginErr = client.Begin(workCtx)
					}
					if transaction != nil {
						result <- fmt.Errorf("uncertain Begin exposed a transaction")
						return
					}
					result <- beginErr
				}()
				select {
				case <-service.responses:
				case <-ctx.Done():
					t.Fatal("owned Begin did not allocate")
				}
				logicalDeadline, _ := workCtx.Deadline()
				handoff := time.UnixMilli(service.owned.GetBeginDeadlineUnixMs())
				if service.owned.GetContext().GetDeadlineUnixMs() != logicalDeadline.UnixMilli() ||
					handoff.Before(started.Add(defaultRPCDeadline-time.Millisecond)) || handoff.After(time.Now().Add(defaultRPCDeadline)) || len(service.owned.GetBeginRequestId()) != 32 {
					t.Fatal("owned Begin identity, handoff bound or caller logical deadline changed")
				}
				if scenario.cancel {
					stop()
				}
				var beginErr error
				select {
				case beginErr = <-result:
				case <-ctx.Done():
					t.Fatal("owned Begin did not settle")
				}
				if beginErr == nil {
					t.Fatal("uncertain Begin succeeded")
				}
				uncertainCleanup := scenario.cancelFailure || scenario.cancelReplyLoss == 2
				if scenario.dropReply && !uncertainCleanup && status.Code(beginErr) != codes.Unavailable {
					t.Fatalf("lost Begin status changed: %v", beginErr)
				}
				if uncertainCleanup && status.Code(beginErr) != codes.FailedPrecondition {
					t.Fatalf("uncertain cleanup was not terminal: %v", beginErr)
				}
				if scenario.rpcExpiry && status.Code(beginErr) != codes.DeadlineExceeded {
					t.Fatalf("physical Begin timeout identity changed: %v", beginErr)
				}
				if (scenario.cancel || scenario.deadline) && !errors.Is(beginErr, workCtx.Err()) {
					t.Fatalf("caller cancellation identity lost: %v", beginErr)
				}
				if strings.Contains(beginErr.Error(), "reconcile uncertain Begin") != uncertainCleanup {
					t.Fatalf("cleanup uncertainty not preserved: %v", beginErr)
				}
				expectedCleanup := uint64(1)
				if scenario.cancelReplyLoss > 0 {
					expectedCleanup = 2
				}
				if service.beginCalls.Load() != 1 || service.cancelCalls.Load() != expectedCleanup {
					t.Fatalf("unexpected RPC attempts: Begin=%d Cancel=%d", service.beginCalls.Load(), service.cancelCalls.Load())
				}
				client.rpc = originalRPC
				after, err := client.WriterStatus(ctx)
				var expectedTransactions uint64
				if scenario.cancelFailure {
					expectedTransactions = 1
				}
				if err != nil || after.ActiveTransactions != expectedTransactions || after.ActiveWriteIntents != 0 || client.CommitRPCStats().Attempts != 0 {
					t.Fatalf("owned Begin ownership: transactions=%d intents=%d err=%v", after.ActiveTransactions, after.ActiveWriteIntents, err)
				}
				if scenario.cancelFailure {
					if err := client.cancelBegin(ctx, service.owned); err != nil {
						t.Fatal(err)
					}
				}
				settled, err := client.WriterStatus(ctx)
				if err != nil || settled.ActiveTransactions != 0 || settled.ActiveWriteIntents != 0 {
					t.Fatalf("owned fixture did not settle: transactions=%d err=%v", settled.ActiveTransactions, err)
				}
			})
		}
	}
}

func TestClientOwnedBeginIdentityGuards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "owned-begin-identity", DaemonPath: daemonBinary(t), DataDir: t.TempDir(), AuthToken: "owned-begin-fixture-token", testEnvironment: []string{"VAULTICDB_WRITER_MINIMUM_TENURE=1ms"}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	if !client.Limits().BeginReconciliation {
		t.Fatal("Begin reconciliation not negotiated")
	}
	deadline := time.Now().Add(10 * time.Second).UnixMilli()
	begin := &vaulticdbv1.BeginOwnedRequest{Context: requestContext(ctx), BeginRequestId: strings.Repeat("a", 32), BeginDeadlineUnixMs: deadline}
	cleanup := &vaulticdbv1.CancelBeginRequest{Context: requestContext(ctx), BeginRequestId: begin.BeginRequestId, BeginDeadlineUnixMs: deadline}
	unauthenticated := vaulticdbv1.NewVaulticDBClient(client.conn)
	if _, err := unauthenticated.BeginOwned(ctx, begin); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated owned Begin accepted: %v", err)
	}
	if _, err := unauthenticated.CancelBegin(ctx, cleanup); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated cleanup accepted: %v", err)
	}
	if _, err := client.rpc.CancelBegin(ctx, cleanup); err != nil {
		t.Fatal(err)
	}
	if _, err := client.rpc.BeginOwned(ctx, begin); status.Code(err) != codes.Canceled {
		t.Fatalf("late Begin after cancellation: %v", err)
	}
	cleanup.BeginDeadlineUnixMs--
	if _, err := client.rpc.CancelBegin(ctx, cleanup); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("identity/deadline reuse accepted: %v", err)
	}
	cleanup.BeginDeadlineUnixMs = deadline
	begin.BeginRequestId, cleanup.BeginRequestId = strings.Repeat("b", 32), strings.Repeat("b", 32)
	response, err := client.rpc.BeginOwned(ctx, begin)
	if err != nil || response.GetTransactionId() == "" {
		t.Fatalf("owned Begin: %v", err)
	}
	if _, err := client.rpc.BeginOwned(ctx, begin); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate Begin allocated again: %v", err)
	}
	before, err := client.WriterStatus(ctx)
	if err != nil || before.ActiveTransactions != 1 {
		t.Fatalf("duplicate ownership: transactions=%d err=%v", before.ActiveTransactions, err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := client.rpc.CancelBegin(ctx, cleanup); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.rpc.BeginOwned(ctx, begin); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled identity reopened: %v", err)
	}
	for _, scenario := range []struct {
		id       string
		deadline int64
		code     codes.Code
	}{
		{strings.Repeat("c", 32), time.Now().Add(-time.Millisecond).UnixMilli(), codes.DeadlineExceeded},
		{strings.Repeat("d", 32), time.Now().Add(-61 * time.Second).UnixMilli(), codes.FailedPrecondition},
		{strings.Repeat("e", 32), time.Now().Add(12 * time.Second).UnixMilli(), codes.InvalidArgument},
		{"bad", deadline, codes.InvalidArgument},
	} {
		begin.BeginRequestId, begin.BeginDeadlineUnixMs = scenario.id, scenario.deadline
		if _, err := client.rpc.BeginOwned(ctx, begin); status.Code(err) != scenario.code {
			t.Fatalf("identity guard %q: %v", scenario.id, err)
		}
	}
	settled, err := client.WriterStatus(ctx)
	if err != nil || settled.ActiveTransactions != 0 || settled.ActiveWriteIntents != 0 {
		t.Fatalf("identity guards did not settle: transactions=%d err=%v", settled.ActiveTransactions, err)
	}
	roleCtx := withAuth(ctx, client.options.AuthToken)
	if _, err := client.demoteWriter(roleCtx, "owned-begin-epoch-check", false, time.Second); err != nil {
		t.Fatal(err)
	}
	promoted, err := client.promoteWriterWithTakeover(roleCtx, "owned-begin-epoch-check", false, 0)
	if err != nil || promoted.CurrentEpoch <= before.CurrentEpoch {
		t.Fatalf("writer epoch did not advance: epoch=%d err=%v", promoted.CurrentEpoch, err)
	}
	if _, err := client.rpc.CancelBegin(ctx, cleanup); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale Begin authority accepted: %v", err)
	}
	transaction, err := client.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := rollbackTransaction(ctx, transaction); err != nil {
		t.Fatal(err)
	}
}

func TestClientOwnedBeginGenerationGuard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "owned-begin-generation", DaemonPath: daemonBinary(t), DataDir: t.TempDir(), ObjectStore: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	initial, err := client.GenerationStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	begin := &vaulticdbv1.BeginOwnedRequest{Context: requestContext(ctx), BeginRequestId: strings.Repeat("a", 32), BeginDeadlineUnixMs: time.Now().Add(10 * time.Second).UnixMilli()}
	if _, err := client.rpc.BeginOwned(ctx, begin); err != nil {
		t.Fatal(err)
	}
	if err := client.cancelBegin(ctx, begin); err != nil {
		t.Fatal(err)
	}
	quarantined, err := client.QuarantineGeneration(ctx, initial.ActiveGeneration, strings.Repeat("aa", 32))
	if err != nil {
		t.Fatal(err)
	}
	activated, err := client.ActivateGeneration(ctx, quarantined.ActiveGeneration, quarantined.ActiveGeneration+1, "candidate", strings.Repeat("bb", 32), time.Minute)
	if err != nil || activated.ActiveGeneration <= initial.ActiveGeneration {
		t.Fatalf("generation did not advance: generation=%d err=%v", activated.ActiveGeneration, err)
	}
	if err := client.cancelBegin(ctx, begin); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale generation cleanup accepted: %v", err)
	}
	settled, err := client.WriterStatus(ctx)
	if err != nil || settled.ActiveTransactions != 0 || settled.ActiveWriteIntents != 0 {
		t.Fatalf("generation guard changed ownership: transactions=%d err=%v", settled.ActiveTransactions, err)
	}
}

func TestClientOwnedBeginBlockedAdmissionExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "owned-begin-blocked", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	id := daemonTestID(90)
	holder, err := client.beginPublication(ctx, []schema.ID{id})
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTransaction(ctx, holder)
	deadline := time.Now().Add(150 * time.Millisecond).UnixMilli()
	begin := &vaulticdbv1.BeginOwnedRequest{Context: requestContext(ctx), BeginRequestId: strings.Repeat("a", 32), BeginDeadlineUnixMs: deadline, ContentIds: [][]byte{id[:]}}
	started := time.Now()
	if _, err := client.rpc.BeginOwned(ctx, begin); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("blocked Begin admitted: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("blocked Begin exceeded its admission budget")
	}
	cleanup := &vaulticdbv1.CancelBeginRequest{Context: requestContext(ctx), BeginRequestId: begin.BeginRequestId, BeginDeadlineUnixMs: deadline}
	if _, err := client.rpc.CancelBegin(ctx, cleanup); err != nil {
		t.Fatal(err)
	}
	if err := rollbackTransaction(ctx, holder); err != nil {
		t.Fatal(err)
	}
	if _, err := client.rpc.BeginOwned(ctx, begin); status.Code(err) != codes.Canceled {
		t.Fatalf("expired Begin reopened after permits released: %v", err)
	}
	settled, err := client.WriterStatus(ctx)
	if err != nil || settled.ActiveTransactions != 0 || settled.ActiveWriteIntents != 0 {
		t.Fatalf("blocked admission leaked ownership: transactions=%d err=%v", settled.ActiveTransactions, err)
	}
}

func TestClientBeginCancellationOverGRPC(t *testing.T) {
	for _, scenario := range []struct{ deadline, dropReply bool }{{}, {deadline: true}, {dropReply: true}, {deadline: true, dropReply: true}} {
		t.Run(fmt.Sprintf("deadline=%t/dropReply=%t", scenario.deadline, scenario.dropReply), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "lost-begin-response-grpc", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close(context.Background())
			originalRPC := client.rpc
			service := &withheldBeginService{upstream: originalRPC, responses: make(chan *vaulticdbv1.BeginResponse, 1), finished: make(chan struct{})}
			socket := testSocket(t)
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := os.Chmod(socket, 0o600); err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			vaulticdbv1.RegisterVaulticDBServer(server, service)
			go func() { _ = server.Serve(listener) }()
			defer server.Stop()
			connection, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			client.rpc = vaulticdbv1.NewVaulticDBClient(connection)
			client.limits.BeginReconciliation = false
			defer func() { client.rpc = originalRPC }()
			workCtx, stop := context.WithCancel(ctx)
			if scenario.deadline {
				stop()
				workCtx, stop = context.WithTimeout(ctx, 2*time.Second)
			}
			defer stop()
			service.release = workCtx.Done()
			service.dropReply = scenario.dropReply
			result := make(chan error, 1)
			go func() {
				transaction, beginErr := client.Begin(workCtx)
				if transaction != nil {
					result <- fmt.Errorf("lost response exposed a transaction: %s", transaction.ID())
					return
				}
				result <- beginErr
			}()
			var response *vaulticdbv1.BeginResponse
			select {
			case response = <-service.responses:
			case <-ctx.Done():
				t.Fatal("relay did not create a daemon transaction")
			}
			expectedCode := codes.DeadlineExceeded
			if !scenario.deadline {
				expectedCode = codes.Canceled
				stop()
			}
			if scenario.dropReply {
				expectedCode = codes.Unavailable
			}
			select {
			case beginErr := <-result:
				if status.Code(beginErr) != expectedCode {
					t.Fatalf("lost response error: %v", beginErr)
				}
			case <-ctx.Done():
				t.Fatal("canceled Begin did not return")
			}
			select {
			case <-service.finished:
			case <-ctx.Done():
				t.Fatal("relay handler did not settle")
			}
			client.rpc = originalRPC
			after, err := client.WriterStatus(ctx)
			var expectedTransactions uint64
			if scenario.dropReply {
				expectedTransactions = 1
			}
			if err != nil || after.ActiveTransactions != expectedTransactions || after.ActiveWriteIntents != 0 || client.CommitRPCStats().Attempts != 0 {
				t.Fatalf("lost gRPC response ownership: status=%+v err=%v", after, err)
			}
			if scenario.dropReply {
				transaction, err := client.transactionFromBegin(response)
				if err != nil {
					t.Fatal(err)
				}
				if err := rollbackTransaction(workCtx, transaction); err != nil {
					t.Fatal(err)
				}
			}
			settled, err := client.WriterStatus(ctx)
			if err != nil || settled.ActiveTransactions != 0 || settled.ActiveWriteIntents != 0 {
				t.Fatalf("gRPC fixture cleanup did not settle: status=%+v err=%v", settled, err)
			}
			t.Logf("status=%s residual_transactions=%d fixture_cleanup=settled", expectedCode, after.ActiveTransactions)
		})
	}
}

type packConflictRPC struct {
	vaulticdbv1.VaulticDBClient
	width           uint64
	commits         atomic.Uint64
	release         chan struct{}
	cancelOnAbort   bool
	beginAborts     atomic.Int64
	beginDeadline   atomic.Int64
	logicalDeadline atomic.Int64
	cancelErr       error
}

func (rpc *packConflictRPC) Begin(ctx context.Context, request *vaulticdbv1.Empty, options ...grpc.CallOption) (*vaulticdbv1.BeginResponse, error) {
	rpc.logicalDeadline.CompareAndSwap(0, request.GetContext().GetDeadlineUnixMs())
	if deadline, ok := ctx.Deadline(); ok {
		rpc.beginDeadline.CompareAndSwap(0, deadline.UnixNano())
	}
	remaining := rpc.beginAborts.Load()
	if remaining > 0 && rpc.beginAborts.CompareAndSwap(remaining, remaining-1) {
		return nil, status.Error(codes.Aborted, "test pre-Commit abort")
	}
	return rpc.VaulticDBClient.Begin(ctx, request, options...)
}

func (rpc *packConflictRPC) BeginOwned(ctx context.Context, request *vaulticdbv1.BeginOwnedRequest, options ...grpc.CallOption) (*vaulticdbv1.BeginResponse, error) {
	rpc.logicalDeadline.CompareAndSwap(0, request.GetContext().GetDeadlineUnixMs())
	if deadline, ok := ctx.Deadline(); ok {
		rpc.beginDeadline.CompareAndSwap(0, deadline.UnixNano())
	}
	remaining := rpc.beginAborts.Load()
	if remaining > 0 && rpc.beginAborts.CompareAndSwap(remaining, remaining-1) {
		return nil, status.Error(codes.Aborted, "test pre-Commit abort")
	}
	return rpc.VaulticDBClient.BeginOwned(ctx, request, options...)
}

func (rpc *packConflictRPC) CancelBegin(ctx context.Context, request *vaulticdbv1.CancelBeginRequest, options ...grpc.CallOption) (*vaulticdbv1.Empty, error) {
	if rpc.cancelErr != nil {
		return nil, rpc.cancelErr
	}
	return rpc.VaulticDBClient.CancelBegin(ctx, request, options...)
}

func TestSchemaStorePackUncertainBeginCleanupIsTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "uncertain-begin-terminal", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	rpc := &packConflictRPC{VaulticDBClient: client.rpc, cancelErr: status.Error(codes.Unavailable, "test cleanup unavailable")}
	rpc.beginAborts.Store(128)
	client.rpc = rpc
	store := NewSchemaStore(client)
	err = store.PublishPack(ctx, readSessionTestPack(daemonTestID(20), daemonTestID(90)))
	if status.Code(err) != codes.FailedPrecondition || !errors.Is(err, rpc.cancelErr) || rpc.beginAborts.Load() != 127 {
		t.Fatalf("uncertain Begin was retried or error lost: remaining=%d err=%v", rpc.beginAborts.Load(), err)
	}
	stats := store.PackPublicationStats()
	if stats.Calls != 1 || stats.Failures != 1 || stats.RecoveredAborts != 0 || stats.TerminalAborts != 0 || stats.RecoveredRetryCalls != 0 || stats.TerminalRetryCalls != 0 || client.CommitRPCStats().Attempts != 0 {
		t.Fatalf("uncertain ownership counted as retry recovery: %+v", stats)
	}
}

func (rpc *packConflictRPC) Commit(ctx context.Context, request *vaulticdbv1.TransactionRequest, options ...grpc.CallOption) (*vaulticdbv1.CommitResponse, error) {
	ordinal := rpc.commits.Add(1)
	if ordinal <= rpc.width {
		if ordinal == rpc.width {
			close(rpc.release)
		}
		select {
		case <-rpc.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	response, err := rpc.VaulticDBClient.Commit(ctx, request, options...)
	if rpc.cancelOnAbort && status.Code(err) == codes.Aborted {
		ctx.Value(packCancelKey{}).(context.CancelFunc)()
	}
	return response, err
}

func TestSchemaStorePackPublicationConflictOutcomes(t *testing.T) {
	for _, cancelOnAbort := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelOnAbort=%t", cancelOnAbort), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "pack-conflict-outcomes", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close(context.Background())
			const workers = 4
			client.rpc = &packConflictRPC{VaulticDBClient: client.rpc, width: workers, release: make(chan struct{}), cancelOnAbort: cancelOnAbort}
			store := NewSchemaStore(client)
			before, err := client.WriterStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			type outcome struct {
				aborted, commitAborted uint64
				err                    error
			}
			results := make(chan outcome, workers)
			for index := range workers {
				published := readSessionTestPack(daemonTestID(byte(index+20)), daemonTestID(90))
				go func() {
					workCtx, stop := context.WithCancel(ctx)
					defer stop()
					workCtx = context.WithValue(workCtx, packCancelKey{}, stop)
					aborted, commitAborted, err := store.PublishPackWithRetryDetails(workCtx, published)
					results <- outcome{aborted: aborted, commitAborted: commitAborted, err: err}
				}()
			}
			var recovered, terminal, canceled uint64
			for range workers {
				result := <-results
				if result.err == nil {
					recovered += result.commitAborted
				} else if cancelOnAbort && errors.Is(result.err, context.Canceled) {
					terminal += result.commitAborted
					canceled++
				} else {
					t.Fatalf("publication result: %+v", result)
				}
				if result.aborted < result.commitAborted {
					t.Fatalf("Commit aborts exceed all aborts: %+v", result)
				}
			}
			after, err := client.WriterStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			stats := store.PackPublicationStats()
			rpcs := client.CommitRPCStats()
			commits := after.Attribution.CommitRequest
			attempts := commits.Attempts - before.Attribution.CommitRequest.Attempts
			successes := commits.Successes - before.Attribution.CommitRequest.Successes
			failures := commits.Failures - before.Attribution.CommitRequest.Failures
			if failures == 0 || recovered+terminal != failures || stats.Calls != workers || stats.Failures != canceled ||
				stats.FailedCanceled != canceled || stats.FailedTimedOut != 0 || stats.Active != 0 || stats.NS == 0 ||
				stats.RecoveredCommitAborts != recovered || stats.TerminalCommitAborts != terminal ||
				successes != workers-canceled || attempts != successes+failures ||
				commits.Completed-before.Attribution.CommitRequest.Completed != attempts || commits.Active != 0 ||
				rpcs.Attempts != attempts || rpcs.Successes != successes || rpcs.Aborted != failures ||
				rpcs.Cancellations != 0 || rpcs.Timeouts != 0 || rpcs.OtherFailures != 0 || rpcs.Active != 0 ||
				after.ActiveTransactions != 0 || after.ActiveWriteIntents != 0 {
				t.Fatalf("pack Commit parity: stats=%+v rpc=%+v before=%+v after=%+v", stats, rpcs, before, after)
			}
			if cancelOnAbort && (terminal == 0 || stats.TerminalRetryCalls != canceled) {
				t.Fatalf("missing terminal retry attribution: %+v", stats)
			}
			if !cancelOnAbort && (stats.TerminalAborts != 0 || stats.RecoveredRetryCalls == 0) {
				t.Fatalf("missing recovered retry attribution: %+v", stats)
			}
			value, found, err := store.Get(ctx, schema.BlobKey(daemonTestID(90)))
			if err != nil || !found {
				t.Fatalf("published blob missing: %v", err)
			}
			record, err := schema.UnmarshalBlobRecord(value)
			if err != nil || uint64(len(record.Locations)) != successes {
				t.Fatalf("partial or lost pack locations: %+v err=%v", record, err)
			}
			expectedSize := readSessionTestPack(daemonTestID(20), daemonTestID(90)).Record.PayloadSize * successes
			checkAggregate := func() {
				t.Helper()
				aggregate, found := readAggregate(t, store, ctx, schema.PackAggregateKey(schema.AggregateAll))
				if !found || aggregate.PackCount != successes || aggregate.BlobCount != successes || aggregate.PayloadSize != expectedSize {
					t.Fatalf("partial or duplicate pack aggregate: %+v", aggregate)
				}
			}
			checkAggregate()
			for index := range workers {
				packID := daemonTestID(byte(index + 20))
				_, found, err := store.Get(ctx, schema.PackKey(packID))
				present := slices.ContainsFunc(record.Locations, func(location schema.BlobLocation) bool { return location.PackID == packID })
				if err != nil || found != present {
					t.Fatalf("pack catalog/location parity: pack=%x found=%t present=%t err=%v", packID, found, present, err)
				}
			}
			for _, location := range record.Locations {
				if err := store.PublishPack(ctx, readSessionTestPack(location.PackID, daemonTestID(90))); err != nil {
					t.Fatal(err)
				}
			}
			checkAggregate()
			t.Logf("calls=%d successful_commits=%d recovered_commit_aborts=%d terminal_commit_aborts=%d", stats.Calls, successes, recovered, terminal)
		})
	}
}

func TestSchemaStorePackPreCommitRetryOutcome(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "pack-pre-commit-retry", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	rpc := &packConflictRPC{VaulticDBClient: client.rpc}
	rpc.beginAborts.Store(1)
	client.rpc = rpc
	store := NewSchemaStore(client)
	aborted, commitAborted, err := store.PublishPackWithRetryDetails(ctx, readSessionTestPack(daemonTestID(20), daemonTestID(90)))
	stats := store.PackPublicationStats()
	deadline, _ := ctx.Deadline()
	if err != nil || aborted != 1 || commitAborted != 0 || stats.Calls != 1 || stats.Failures != 0 ||
		stats.RecoveredAborts != 1 || stats.RecoveredCommitAborts != 0 || stats.RecoveredRetryCalls != 1 ||
		rpc.logicalDeadline.Load() != deadline.UnixMilli() || client.CommitRPCStats().Aborted != 0 {
		t.Fatalf("pre-Commit retry/deadline changed: aborted=%d commitAborted=%d stats=%+v err=%v", aborted, commitAborted, stats, err)
	}
}

func TestSchemaStorePackPublicationTerminalOutcomes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "pack-terminal-outcomes", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	originalRPC := client.rpc
	for _, scenario := range []string{"default-budget", "canceled", "expired", "invalid", "retry-limit"} {
		t.Run(scenario, func(t *testing.T) {
			rpc := &packConflictRPC{VaulticDBClient: originalRPC}
			client.rpc = rpc
			store := NewSchemaStore(client)
			workCtx := ctx
			published := readSessionTestPack(daemonTestID(20), daemonTestID(90))
			var expectedAborts, canceled, timedOut uint64
			switch scenario {
			case "default-budget":
				workCtx = context.Background()
			case "canceled":
				var stop context.CancelFunc
				workCtx, stop = context.WithCancel(ctx)
				stop()
				canceled = 1
			case "expired":
				var stop context.CancelFunc
				workCtx, stop = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer stop()
				timedOut = 1
			case "invalid":
				published = PublishedPack{}
			case "retry-limit":
				expectedAborts = revisionAllocationAttempts
				rpc.beginAborts.Store(revisionAllocationAttempts)
			}
			before := time.Now()
			beforeRPC := client.CommitRPCStats()
			aborted, commitAborted, err := store.PublishPackWithRetryDetails(workCtx, published)
			stats := store.PackPublicationStats()
			if scenario == "default-budget" {
				deadline := time.UnixMilli(rpc.logicalDeadline.Load())
				if err != nil || deadline.Before(before.Truncate(time.Millisecond).Add(10*time.Minute)) || deadline.After(time.Now().Add(10*time.Minute)) ||
					stats.Failures != 0 || stats.RecoveredAborts != 0 {
					t.Fatalf("default pack budget/stats changed: deadline=%v stats=%+v err=%v", deadline, stats, err)
				}
			} else if err == nil || stats.Failures != 1 || stats.TerminalAborts != expectedAborts ||
				stats.TerminalRetryCalls != min(expectedAborts, 1) || stats.RecoveredAborts != 0 ||
				client.CommitRPCStats() != beforeRPC {
				t.Fatalf("terminal pack result: stats=%+v err=%v", stats, err)
			}
			if scenario == "retry-limit" && !strings.Contains(err.Error(), "transaction conflict retry limit exceeded") {
				t.Fatalf("retry-limit error changed: %v", err)
			}
			if aborted != expectedAborts || commitAborted != 0 || stats.Calls != 1 || stats.Active != 0 ||
				stats.FailedCanceled != canceled || stats.FailedTimedOut != timedOut || stats.NS == 0 ||
				stats.RecoveredCommitAborts != 0 || stats.TerminalCommitAborts != 0 {
				t.Fatalf("pack terminal classification: aborted=%d commitAborted=%d stats=%+v err=%v", aborted, commitAborted, stats, err)
			}
		})
	}
}

func TestSchemaStoreImportsHistoricalSnapshot(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{Socket: testSocket(t), RepositoryID: "historical-snapshots", DaemonPath: daemonBinary(t), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	snapshotID, treeID := daemonTestID(91), daemonTestID(92)
	record := schema.SnapshotRecord{LegacyTree: treeID, OriginalJSON: []byte(fmt.Sprintf(`{"tree":"%x","unknown":{"preserved":true}}`, treeID))}
	if err := store.ImportLegacySnapshot(ctx, snapshotID, record); err == nil {
		t.Fatal("missing root accepted")
	}
	packID := daemonTestID(93)
	if err := store.PublishPack(ctx, PublishedPack{
		PackID: packID, Record: schema.PackRecord{Type: schema.PackTree, BlobCount: 1, PayloadSize: 10, Lifecycle: schema.PackExportPending},
		Blobs: map[schema.ID]schema.BlobRecord{treeID: {Locations: []schema.BlobLocation{{PackID: packID, Length: 10, Type: schema.BlobTree}}}},
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := store.ImportLegacySnapshot(ctx, snapshotID, record); err != nil {
			t.Fatal(err)
		}
	}
	value, found, err := store.Get(ctx, schema.SnapshotKey(snapshotID))
	if err != nil || !found {
		t.Fatalf("snapshot missing: %v", err)
	}
	decoded, err := schema.UnmarshalSnapshotRecord(value)
	if err != nil || decoded.LegacyTree != treeID || !bytes.Equal(decoded.OriginalJSON, record.OriginalJSON) || decoded.CommitSequence != 0 {
		t.Fatalf("historical snapshot changed: %#v %v", decoded, err)
	}
	record.OriginalJSON = []byte(fmt.Sprintf(`{"tree":"%x"}`, treeID))
	if err := store.ImportLegacySnapshot(ctx, snapshotID, record); err == nil {
		t.Fatal("conflicting snapshot accepted")
	}
	batch := []LegacySnapshotImport{
		{SnapshotID: daemonTestID(94), Record: record},
		{SnapshotID: snapshotID, Record: record},
	}
	if err := store.ImportLegacySnapshots(ctx, batch); err == nil {
		t.Fatal("batch containing a conflict accepted")
	}
	if _, found, err := store.Get(ctx, schema.SnapshotKey(batch[0].SnapshotID)); err != nil || found {
		t.Fatalf("failed batch published a partial snapshot: found=%t err=%v", found, err)
	}
	batch[1].SnapshotID = daemonTestID(95)
	batch[1].Record.LegacyTree = daemonTestID(96)
	batch[1].Record.OriginalJSON = fmt.Appendf(nil, `{"tree":"%x"}`, batch[1].Record.LegacyTree)
	if err := store.ImportLegacySnapshots(ctx, batch); err == nil {
		t.Fatal("batch containing a missing root accepted")
	}
	if _, found, err := store.Get(ctx, schema.SnapshotKey(batch[0].SnapshotID)); err != nil || found {
		t.Fatalf("missing-root batch published a partial snapshot: found=%t err=%v", found, err)
	}
	batch[1].Record = record
	for index := len(batch); index < MaxLegacySnapshotsPerTransaction; index++ {
		batch = append(batch, LegacySnapshotImport{SnapshotID: daemonTestID(byte(100 + index)), Record: record})
	}
	before, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := store.ImportLegacySnapshots(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	after, err := client.WriterStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Attribution.DurableWait.Attempts != before.Attribution.DurableWait.Attempts+1 {
		t.Fatalf("batch and idempotent replay must use one durable wait: before=%d after=%d", before.Attribution.DurableWait.Attempts, after.Attribution.DurableWait.Attempts)
	}
	for _, snapshot := range batch {
		value, found, err := store.Get(ctx, schema.SnapshotKey(snapshot.SnapshotID))
		if err != nil || !found {
			t.Fatalf("batched snapshot missing: %v", err)
		}
		stored, err := schema.UnmarshalSnapshotRecord(value)
		if err != nil || !bytes.Equal(stored.OriginalJSON, snapshot.Record.OriginalJSON) {
			t.Fatalf("batched snapshot changed: %v", err)
		}
	}
}

func TestLegacySnapshotBatchLimits(t *testing.T) {
	store := &SchemaStore{}
	ctx := context.Background()
	if err := store.ImportLegacySnapshots(ctx, nil); err != nil {
		t.Fatal(err)
	}
	treeID := daemonTestID(1)
	record := schema.SnapshotRecord{LegacyTree: treeID, OriginalJSON: fmt.Appendf(nil, `{"tree":"%x","padding":"%s"}`, treeID, strings.Repeat("a", MaxLegacySnapshotTransactionBytes/2))}
	for _, scenario := range []struct {
		name  string
		batch []LegacySnapshotImport
	}{
		{name: "count", batch: make([]LegacySnapshotImport, MaxLegacySnapshotsPerTransaction+1)},
		{name: "bytes", batch: []LegacySnapshotImport{{SnapshotID: daemonTestID(2), Record: record}, {SnapshotID: daemonTestID(3), Record: record}}},
		{name: "duplicate", batch: []LegacySnapshotImport{{SnapshotID: daemonTestID(2), Record: record}, {SnapshotID: daemonTestID(2), Record: record}}},
		{name: "zero-id", batch: []LegacySnapshotImport{{Record: record}}},
		{name: "invalid-record", batch: []LegacySnapshotImport{{SnapshotID: daemonTestID(2), Record: schema.SnapshotRecord{LegacyTree: treeID}}}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			if err := store.ImportLegacySnapshots(ctx, scenario.batch); err == nil {
				t.Fatal("invalid batch accepted before transaction start")
			}
		})
	}
}

func TestSchemaStorePublishesAuthoritativePacksAndDuplicateLocations(t *testing.T) {
	client, err := Ensure(
		context.Background(),
		Options{Socket: testSocket(t), RepositoryID: "phase6-packs", DaemonPath: daemonBinary(t), DataDir: t.TempDir()},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	blobID, packOne, packTwo := daemonTestID(31), daemonTestID(32), daemonTestID(33)
	for _, published := range []PublishedPack{
		{PackID: packOne,
			Record: schema.PackRecord{Type: schema.PackData,
				PayloadSize: 10,
				BlobCount:   1,
				Lifecycle:   schema.PackExportPending},
			Blobs: map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packOne,
				Length: 10,
				Type:   schema.BlobData}}}}},

		{PackID: packTwo,
			Record: schema.PackRecord{Type: schema.PackData,
				PayloadSize: 10,
				BlobCount:   1,
				Lifecycle:   schema.PackExportPending},
			Blobs: map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{PackID: packTwo,
				Length: 10,
				Type:   schema.BlobData}}}}},
	} {
		if err := store.PublishPack(context.Background(), published); err != nil {
			t.Fatal(err)
		}
	}
	stats := store.PackPublicationStats()
	if stats.Calls != 2 || stats.Failures != 0 || stats.Active != 0 || stats.NS == 0 ||
		stats.RecoveredAborts != 0 || stats.TerminalAborts != 0 || stats.RecoveredCommitAborts != 0 {
		t.Fatalf("serial pack publication stats: %+v", stats)
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

func TestLegacyImportAdvancesUntouchedAggregateSequence(t *testing.T) {
	client, err := Ensure(
		context.Background(),
		Options{Socket: testSocket(t), RepositoryID: "phase32-aggregate-sequence", DaemonPath: daemonBinary(t), DataDir: t.TempDir()},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	treePack, treeBlob := daemonTestID(201), daemonTestID(202)
	if err := store.PublishPack(ctx, PublishedPack{
		PackID: treePack,
		Record: schema.PackRecord{Type: schema.PackTree, PayloadSize: 5, BlobCount: 1, Lifecycle: schema.PackExportPending},
		Blobs: map[schema.ID]schema.BlobRecord{treeBlob: {
			Locations: []schema.BlobLocation{{PackID: treePack, Length: 5, Type: schema.BlobTree}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	treeBefore, found := readAggregate(t, store, ctx, schema.PackAggregateKey(schema.AggregateTree))
	if !found {
		t.Fatal("tree aggregate missing before legacy import")
	}
	dataPack, dataBlob := daemonTestID(203), daemonTestID(204)
	if err := store.ImportLegacyPack(ctx, LegacyPackImport{
		SourceIndex: daemonTestID(205), PackID: dataPack,
		Record: schema.PackRecord{Type: schema.PackData, PayloadSize: 7, BlobCount: 1, Lifecycle: schema.PackImported},
		Blobs: map[schema.ID]schema.BlobRecord{dataBlob: {
			Locations: []schema.BlobLocation{{PackID: dataPack, Length: 7, Type: schema.BlobData}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	treeAfter, found := readAggregate(t, store, ctx, schema.PackAggregateKey(schema.AggregateTree))
	if !found || treeAfter.UpdateSequence != treeBefore.UpdateSequence+1 ||
		treeAfter.PackCount != treeBefore.PackCount || treeAfter.PayloadSize != treeBefore.PayloadSize {
		t.Fatalf("untouched tree aggregate before=%+v after=%+v found=%t", treeBefore, treeAfter, found)
	}
}

func TestSchemaStoreTwoPhasePackDeletion(t *testing.T) {
	client, err := Ensure(
		context.Background(),
		Options{
			Socket:       testSocket(t),
			RepositoryID: "phase8-gc-delete",
			DaemonPath:   daemonBinary(t),
			DataDir:      t.TempDir(),
		},
	)
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

func TestTransactionPublicationFence(t *testing.T) {
	store, ctx := historyTestStore(t, t.Name())
	for ordinal, name := range []string{"valid", "generation", "decision", "closed", "unsupported"} {
		t.Run(name, func(t *testing.T) {
			session, err := store.BeginReadSession(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close(context.Background())
			fence := &ReadSession{SchemaStore: store, transaction: session.transaction, Identity: session.Identity, decision: session.decision}
			switch name {
			case "generation":
				fence.Identity.Generation++
			case "decision":
				fence.decision++
			case "closed":
				if err := session.Close(ctx); err != nil {
					t.Fatal(err)
				}
			}
			transaction, err := store.client.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			key := schema.BlobKey(daemonTestID(byte(ordinal + 180)))
			if err := transaction.WriteBatch(ctx, []Mutation{{Key: key, Value: []byte("fenced value")}}, nil); err != nil {
				t.Fatal(err)
			}
			capability := store.client.limits.PublicationFence
			if name == "unsupported" {
				store.client.limits.PublicationFence = false
			}
			err = transaction.CommitForSession(ctx, fence)
			store.client.limits.PublicationFence = capability
			if name == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil {
					t.Fatal("invalid publication fence committed")
				}
				if err := transaction.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
			}
			_, found, err := store.Get(ctx, key)
			if err != nil || found != (name == "valid") {
				t.Fatalf("fenced write visible=%t err=%v", found, err)
			}
		})
	}
	status, err := store.client.WriterStatus(ctx)
	if err != nil || status.ActiveTransactions != 0 || status.ActiveWriteIntents != 0 {
		t.Fatalf("fence cleanup: transactions=%d intents=%d err=%v", status.ActiveTransactions, status.ActiveWriteIntents, err)
	}
}

func TestSchemaStoreCompletesSnapshotExportAtomically(t *testing.T) {
	client, err := Ensure(
		context.Background(),
		Options{
			Socket:       testSocket(t),
			RepositoryID: "phase6-snapshot",
			DaemonPath:   daemonBinary(t),
			DataDir:      t.TempDir(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	session, err := store.BeginReadSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	revision, err := store.AllocateRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rootKey := schema.DirectoryRevisionKey(0, 0, revision)
	rootValue := encodeSchemaRecord(
		t,
		schema.DirectoryRevision{
			Children: []schema.DirectoryChild{
				{Name: "file", Inode: 2, Type: schema.NodeFile, MetadataKey: schema.InodeRevisionKey(1, 2, 1)},
			},
			SourcePath: "/",
			Known:      schema.KnownPath,
			Freshness:  schema.FreshnessVerified,
		},
	)
	if err := store.PublishRevision(context.Background(), schema.CurrentDirectoryKey(0, 0), rootKey, rootValue, revision); err != nil {
		t.Fatal(err)
	}
	snapshotID := daemonTestID(41)
	if err := store.MarkExportPending(context.Background(), snapshotID, rootKey); err != nil {
		t.Fatal(err)
	}
	scope := SnapshotScope{SnapshotID: snapshotID, RootKey: rootKey,
		OriginalJSON: []byte("{\"time\":\"2026-08-29T12:34:56Z\",\"tree\":\"test\"}")}
	failure := errors.New("read session lost before commit")
	validations := 0
	err = store.publishSnapshotScope(context.Background(), scope, func(ctx context.Context) error {
		validations++
		if validations == 2 {
			return failure
		}
		return session.Validate(ctx)
	}, session)
	if !errors.Is(err, failure) || validations != 2 {
		t.Fatalf("late publication validation: calls=%d err=%v", validations, err)
	}
	if _, found, err := store.Get(context.Background(), schema.SnapshotKey(snapshotID)); err != nil || found {
		t.Fatalf("failed validation published snapshot: found=%t err=%v", found, err)
	}
	pendingValue, found, err := store.Get(context.Background(), schema.ExportCheckpointKey(snapshotID))
	if err != nil || !found {
		t.Fatalf("pending checkpoint: found=%t err=%v", found, err)
	}
	pending, err := schema.UnmarshalExportCheckpointRecord(pendingValue)
	if err != nil || pending.State != schema.ExportPending || pending.CommitSequence != 0 {
		t.Fatalf("failed validation completed checkpoint: %+v err=%v", pending, err)
	}
	if err := session.PublishSnapshotScope(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	checkpointValue, found, err := store.Get(context.Background(), schema.ExportCheckpointKey(snapshotID))
	if err != nil || !found {
		t.Fatalf("checkpoint: found=%t err=%v", found, err)
	}
	checkpoint, err := schema.UnmarshalExportCheckpointRecord(checkpointValue)
	if err != nil || checkpoint.State != schema.ExportComplete || checkpoint.CommitSequence == 0 ||
		!bytes.Equal(checkpoint.RootKey, rootKey) {
		t.Fatalf("checkpoint = %#v, err=%v", checkpoint, err)
	}
	if _, found, err := store.Get(context.Background(), schema.SnapshotKey(snapshotID)); err != nil || !found {
		t.Fatalf("snapshot scope: found=%t err=%v", found, err)
	}
	commitValue, found, err := store.Get(
		context.Background(),
		schema.SnapshotCommitKey(checkpoint.CommitSequence, snapshotID),
	)
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
	session.cancel(failure)
	if err := session.PublishSnapshotScope(context.Background(), scope); !errors.Is(err, failure) {
		t.Fatalf("failed session allowed publication replay: %v", err)
	}
}

func TestSchemaStoreSnapshotMembershipDeltasPublishAndForget(t *testing.T) {
	client, err := Ensure(
		context.Background(),
		Options{
			Socket:       testSocket(t),
			RepositoryID: "phase16-snapshot-membership",
			DaemonPath:   daemonBinary(t),
			DataDir:      t.TempDir(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	metadata := schema.AnalyticsMetadataRecord{
		Enabled:    true,
		Generation: 1,
		BuiltAt:    time.Now().UnixNano(),
		ConfigJSON: "{}",
	}
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
	root := schema.DirectoryRevision{
		Known:      schema.KnownPath,
		SourcePath: "/",
		Freshness:  schema.FreshnessVerified,
		Children:   []schema.DirectoryChild{{Name: "file", Inode: 2, Type: schema.NodeFile, MetadataKey: inodeKey}},
	}
	if err := store.PublishRevision(ctx, schema.CurrentDirectoryKey(1, 1), rootKey, encodeSchemaRecord(t, root), rootRevision); err != nil {
		t.Fatal(err)
	}
	snapshotID := daemonTestID(42)
	if err := store.MarkExportPending(ctx, snapshotID, rootKey); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishSnapshotScope(ctx,
		SnapshotScope{SnapshotID: snapshotID,
			RootKey:      rootKey,
			OriginalJSON: []byte(("{\"time\":\"2026-08-30T12:00:00Z\"}"))}); err != nil {
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
	if err != nil || delta.Kind != schema.AnalyticsDeltaRetainedReferences ||
		delta.IdentityGeneration != inodeRevision ||
		delta.RetainedSnapshotRefs != 1 {
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
	client, err := Ensure(
		context.Background(),
		Options{
			Socket:       testSocket(t),
			RepositoryID: "phase16-crawl-proof",
			DaemonPath:   daemonBinary(t),
			DataDir:      t.TempDir(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	ctx := context.Background()
	metadata := schema.AnalyticsMetadataRecord{
		Enabled:    true,
		Generation: 1,
		BuiltAt:    time.Now().UnixNano(),
		ConfigJSON: "{}",
	}
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
		if err := store.PublishRevision(ctx,
			schema.CurrentInodeKey(fsid,
				inode),
			schema.InodeRevisionKey(fsid,
				inode,
				revision),
			encodeSchemaRecord(t,
				record),
			revision); err != nil {
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
			root.Children = []schema.DirectoryChild{
				{
					Name:        "file",
					Inode:       inode,
					Type:        schema.NodeFile,
					MetadataKey: schema.InodeRevisionKey(1, inode, inodeRevision),
				},
			}
		}
		if err := store.PublishRevision(ctx,
			schema.CurrentDirectoryKey(1,
				uint64(snapshotByte)+100),
			rootKey,
			encodeSchemaRecord(t,
				root),
			rootRevision); err != nil {
			t.Fatal(err)
		}
		snapshotID := daemonTestID(snapshotByte)
		if err := store.MarkExportPending(ctx, snapshotID, rootKey); err != nil {
			t.Fatal(err)
		}
		claim := &AuthoritativeCrawlClaim{
			ScopeID:    scope,
			RootFSID:   1,
			RootInode:  uint64(snapshotByte) + 100,
			StartFence: startFence,
			Complete:   complete,
			DebtKeys:   debtKeys,
		}
		if err := store.PublishSnapshotScope(ctx,
			SnapshotScope{SnapshotID: snapshotID,
				RootKey:      rootKey,
				OriginalJSON: []byte(("{\"time\":\"2026-08-30T12:00:00Z\"}")),
				Crawl:        claim}); err != nil {
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
	if current := readBinding(scopeA, 2, reappeared); current.State != schema.AuthoritativeSourceLive ||
		current.Continuity != schema.AnalyticsContinuityProven ||
		current.Generation == first {
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
	if uncertain := readBinding(scopeGap, 3, gapFirst); uncertain.State != schema.AuthoritativeSourceUnknown ||
		uncertain.Continuity != schema.AnalyticsContinuityUnknown {
		t.Fatalf("incomplete absence binding = %#v", uncertain)
	}
	gapReappeared := publishInode(1, 3, "/gap-replacement")
	publishCrawl(107, scopeGap, 3, gapReappeared, gapObserved, false, nil)
	if uncertain := readBinding(scopeGap, 3, gapReappeared); uncertain.State != schema.AuthoritativeSourceLive ||
		uncertain.Continuity != schema.AnalyticsContinuityUnknown ||
		uncertain.Generation == gapFirst {
		t.Fatalf("gap reappearance binding = %#v", uncertain)
	}
	if old := readBinding(scopeGap, 3, gapFirst); old.State != schema.AuthoritativeSourceUnknown {
		t.Fatalf("gap generation was merged: %#v", old)
	}

	scopeDebt := daemonTestID(204)
	debtFirst := publishInode(1, 4, "/debt")
	debtObserved := publishCrawl(108, scopeDebt, 4, debtFirst, 1, true, nil)
	debtKey := schema.CrawlDebtKey(schema.ID{}, daemonTestID(205))
	debt := schema.CrawlDebtRecord{
		PathOrTree: []byte("debt"),
		Reason:     schema.DebtMissingInode,
		Status:     schema.DebtPending,
	}
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
	options := Options{
		Socket:       testSocket(t),
		RepositoryID: "phase4-pack-import",
		DaemonPath:   daemonBinary(t),
		DataDir:      t.TempDir(),
		RebuildReset: true,
	}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	ctx := context.Background()
	source1, source2 := daemonTestID(1), daemonTestID(2)
	pack1, pack2, blobID := daemonTestID(3), daemonTestID(4), daemonTestID(5)
	location := func(packID schema.ID, offset uint64) map[schema.ID]schema.BlobRecord {
		return map[schema.ID]schema.BlobRecord{
			blobID: {
				Locations: []schema.BlobLocation{
					{PackID: packID, Offset: offset, Length: 8, UncompressedSize: 7, Type: schema.BlobData},
				},
			},
		}
	}
	debtKey := schema.CrawlDebtKey(schema.ID{}, pack1)
	debt := schema.CrawlDebtRecord{
		SourceIndexOrPack: pack1,
		SourceKnown:       true,
		Reason:            schema.DebtUnavailablePack,
		Status:            schema.DebtPending,
		ErrorClass:        "offline",
	}
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
		Record: schema.PackRecord{
			Type:              schema.PackData,
			PhysicalSize:      12,
			PhysicalSizeKnown: true,
			PayloadSize:       8,
			HeaderSize:        4,
			BlobCount:         1,
			Lifecycle:         schema.PackImported,
		},
		Blobs: location(pack2, 2),
	}
	if err := store.ImportLegacyPack(ctx, second); err != nil {
		t.Fatal(err)
	}

	blobValue, found, err := store.Get(ctx, schema.BlobKey(blobID))
	if err != nil || !found {
		t.Fatalf("read imported blob: found=%t err=%v", found, err)
	}
	blob, err := schema.UnmarshalBlobRecord(blobValue)
	if err != nil || len(blob.Locations) != 2 || blob.Locations[0].PackID != pack1 ||
		blob.Locations[1].PackID != pack2 {
		t.Fatalf("imported blob locations = %#v, err=%v", blob.Locations, err)
	}
	packValue, found, err := store.Get(ctx, schema.PackKey(pack1))
	if err != nil || !found {
		t.Fatalf("read imported pack: found=%t err=%v", found, err)
	}
	packRecord, err := schema.UnmarshalPackRecord(packValue)
	if err != nil || len(packRecord.SourceIndexIDs) != 2 || packRecord.SourceIndexIDs[0] != source1 ||
		packRecord.SourceIndexIDs[1] != source2 ||
		packRecord.PhysicalSize != 10 {
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
	if err != nil || packRecord.Lifecycle != schema.PackPublished || len(packRecord.SourceIndexIDs) != 2 ||
		packRecord.PhysicalSize != 10 {
		t.Fatalf("published imported pack = %#v, err=%v", packRecord, err)
	}
	aggregateValue, found, err := store.Get(ctx, schema.PackAggregateKey(schema.AggregateAll))
	if err != nil || !found {
		t.Fatalf("read aggregate: found=%t err=%v", found, err)
	}
	aggregate, err := schema.UnmarshalPackAggregate(aggregateValue)
	if err != nil || aggregate.PackCount != 2 || aggregate.PhysicalSize != 22 || aggregate.PayloadSize != 16 ||
		aggregate.HeaderSize != 6 ||
		aggregate.BlobCount != 2 {
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

func TestSchemaStoreImportsLegacyPackBatchWithCheckpoint(t *testing.T) {
	options := Options{
		Socket:       testSocket(t),
		RepositoryID: "phase32-pack-batch-import",
		DaemonPath:   daemonBinary(t),
		DataDir:      t.TempDir(),
		RebuildReset: true,
	}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	store := NewSchemaStore(client)
	store.EnableFreshLegacyImport()
	ctx := context.Background()
	source, pack1, pack2, blobID := daemonTestID(31), daemonTestID(32), daemonTestID(33), daemonTestID(34)
	packImport := func(packID schema.ID, offset uint64, size uint64) LegacyPackImport {
		return LegacyPackImport{
			SourceIndex: source,
			PackID:      packID,
			BatchSize:   2,
			Record: schema.PackRecord{
				Type:              schema.PackData,
				SourceIndexIDs:    []schema.ID{daemonTestID(99), daemonTestID(1)},
				PhysicalSize:      size + 2,
				PhysicalSizeKnown: true,
				PayloadSize:       size,
				HeaderSize:        2,
				BlobCount:         1,
				Lifecycle:         schema.PackImported,
			},
			Blobs: map[schema.ID]schema.BlobRecord{
				blobID: {Locations: []schema.BlobLocation{{
					PackID: packID, Offset: offset, Length: uint32(size), UncompressedSize: uint32(size),
					Type: schema.BlobData,
				}}},
			},
		}
	}
	checkpointRecord := schema.ImportCheckpointRecord{PacksImported: 2, BlobsImported: 2}
	checkpoint := Mutation{
		Key:   schema.ImportCheckpointKey(source),
		Value: encodeSchemaRecord(t, checkpointRecord),
	}
	if err := store.ImportLegacyPacks(
		ctx,
		[]LegacyPackImport{packImport(pack1, 1, 8), packImport(pack2, 2, 9)},
		&checkpoint,
	); err != nil {
		t.Fatal(err)
	}
	packValue, found, err := store.Get(ctx, schema.PackKey(pack1))
	if err != nil || !found {
		t.Fatalf("read batched pack: found=%t err=%v", found, err)
	}
	packRecord, err := schema.UnmarshalPackRecord(packValue)
	expectedSources := []schema.ID{daemonTestID(1), source, daemonTestID(99)}
	if err != nil || !reflect.DeepEqual(packRecord.SourceIndexIDs, expectedSources) {
		t.Fatalf("canonical source indexes = %#v, want %#v, err=%v", packRecord.SourceIndexIDs, expectedSources, err)
	}

	blobValue, found, err := store.Get(ctx, schema.BlobKey(blobID))
	if err != nil || !found {
		t.Fatalf("read batched blob: found=%t err=%v", found, err)
	}
	blob, err := schema.UnmarshalBlobRecord(blobValue)
	if err != nil || len(blob.Locations) != 2 || blob.Locations[0].PackID != pack1 || blob.Locations[1].PackID != pack2 {
		t.Fatalf("batched blob locations = %#v, err=%v", blob.Locations, err)
	}
	aggregateValue, found, err := store.Get(ctx, schema.PackAggregateKey(schema.AggregateAll))
	if err != nil || !found {
		t.Fatalf("read batched aggregate: found=%t err=%v", found, err)
	}
	aggregate, err := schema.UnmarshalPackAggregate(aggregateValue)
	if err != nil || aggregate.PackCount != 2 || aggregate.PhysicalSize != 21 || aggregate.PayloadSize != 17 ||
		aggregate.HeaderSize != 4 || aggregate.BlobCount != 2 {
		t.Fatalf("batched aggregate = %#v, err=%v", aggregate, err)
	}
	checkpointValue, found, err := store.Get(ctx, checkpoint.Key)
	if err != nil || !found {
		t.Fatalf("read batch checkpoint: found=%t err=%v", found, err)
	}
	storedCheckpoint, err := schema.UnmarshalImportCheckpointRecord(checkpointValue)
	if err != nil || storedCheckpoint != checkpointRecord {
		t.Fatalf("batch checkpoint = %#v, err=%v", storedCheckpoint, err)
	}
	stats := store.LegacyImportStats()
	if stats.Batches != 1 || stats.Attempts != 1 || stats.Commits != 1 || stats.PacksCommitted != 2 ||
		stats.BlobsCommitted != 1 || stats.MutationsCommitted == 0 || stats.EncodedBytesCommitted == 0 ||
		stats.MutationRPCs <= 1 || stats.PlanningReads == 0 || stats.SourceIndexesCommitted != 1 ||
		stats.DefinitelyAbsentLookups != 3 || stats.FilterLayers == 0 || stats.FilterInserts != 3 ||
		len(stats.FilterLayerOccupancy) != int(stats.FilterLayers) || stats.FilterLayerOccupancy[0] <= 0 {
		t.Fatalf("legacy import stats = %#v", stats)
	}
}

func TestLegacyImportCheckpointOnlySkipsAggregatesAndNonFreshFilterMetrics(t *testing.T) {
	ctx := context.Background()
	client, err := Ensure(ctx, Options{
		Socket: testSocket(t), RepositoryID: "phase32-checkpoint-only", DaemonPath: daemonBinary(t),
		DataDir: t.TempDir(), RebuildReset: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	store := NewSchemaStore(client)
	source, packID, blobID := daemonTestID(38), daemonTestID(39), daemonTestID(40)
	checkpoint := Mutation{
		Key: schema.ImportCheckpointKey(source),
		Value: encodeSchemaRecord(t, schema.ImportCheckpointRecord{
			PacksImported: 0,
		}),
	}
	if err := store.ImportLegacyPacks(ctx, nil, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(ctx, schema.PackAggregateKey(schema.AggregateAll)); err != nil || found {
		t.Fatalf("checkpoint-only aggregate: found=%t err=%v", found, err)
	}
	imported := LegacyPackImport{
		SourceIndex: source,
		PackID:      packID,
		Record: schema.PackRecord{
			Type: schema.PackData, PhysicalSize: 2, PhysicalSizeKnown: true,
			PayloadSize: 1, HeaderSize: 1, BlobCount: 1, Lifecycle: schema.PackImported,
		},
		Blobs: map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{
			PackID: packID, Length: 1, UncompressedSize: 1, Type: schema.BlobData,
		}}}},
	}
	if err := store.ImportLegacyPack(ctx, imported); err != nil {
		t.Fatal(err)
	}
	stats := store.LegacyImportStats()
	if stats.PossiblyPresentLookups != 0 || stats.FalsePositiveEquivalentLookups != 0 || stats.FilterLayers != 0 {
		t.Fatalf("non-fresh filter metrics = %#v", stats)
	}
}

func TestPrepareLegacyImportBatchRejectsDebtWithoutKey(t *testing.T) {
	packID, blobID := daemonTestID(35), daemonTestID(37)
	_, err := prepareLegacyImportBatch([]LegacyPackImport{{
		SourceIndex: daemonTestID(36),
		PackID:      packID,
		Record: schema.PackRecord{
			Type: schema.PackData, PhysicalSize: 2, PhysicalSizeKnown: true,
			PayloadSize: 1, HeaderSize: 1, BlobCount: 1, Lifecycle: schema.PackImported,
		},
		Blobs: map[schema.ID]schema.BlobRecord{blobID: {Locations: []schema.BlobLocation{{
			PackID: packID, Length: 1, UncompressedSize: 1, Type: schema.BlobData,
		}}}},
		Debt: &schema.CrawlDebtRecord{
			SourceIndexOrPack: packID, SourceKnown: true, Reason: schema.DebtUnavailablePack,
			Status: schema.DebtPending,
		},
	}}, nil)
	if err == nil || err.Error() != "prepare legacy pack 0: legacy pack debt requires a key" {
		t.Fatalf("missing debt key error = %v", err)
	}
}

func TestSchemaStoreLegacyBatchMatchesOrderedOnePackImports(t *testing.T) {
	originalClock := historyClock
	historyClock = func() time.Time { return time.Unix(1_700_000_000, 0) }
	t.Cleanup(func() { historyClock = originalClock })
	ctx := context.Background()
	newStore := func(repositoryID string) (*SchemaStore, *Client) {
		client, err := Ensure(ctx, Options{
			Socket: testSocket(t), RepositoryID: repositoryID, DaemonPath: daemonBinary(t), DataDir: t.TempDir(),
			RebuildReset: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return NewSchemaStore(client), client
	}
	oneAtATime, sequentialClient := newStore("phase32-equivalence-sequential")
	defer sequentialClient.Close(ctx)
	batched, batchClient := newStore("phase32-equivalence-batch")
	defer batchClient.Close(ctx)
	batched.freshImportSeen = newIDSeenFilter(1, 1, freshImportFalsePositive)

	source1, source2 := daemonTestID(61), daemonTestID(62)
	packID, blob1, blob2 := daemonTestID(63), daemonTestID(64), daemonTestID(65)
	debtKey := schema.CrawlDebtKey(schema.ID{}, packID)
	debt := schema.CrawlDebtRecord{
		SourceIndexOrPack: packID, SourceKnown: true, Reason: schema.DebtUnavailablePack,
		Status: schema.DebtPending, ErrorClass: "offline",
	}
	makeImport := func(source, blob schema.ID, offset uint64) LegacyPackImport {
		return LegacyPackImport{
			SourceIndex: source,
			PackID:      packID,
			Record: schema.PackRecord{
				Type: schema.PackData, PhysicalSize: 24, PhysicalSizeKnown: true,
				PayloadSize: 8, HeaderSize: 16, BlobCount: 1, Lifecycle: schema.PackImported,
			},
			Blobs: map[schema.ID]schema.BlobRecord{blob: {Locations: []schema.BlobLocation{{
				PackID: packID, Offset: offset, Length: 8, UncompressedSize: 9, Type: schema.BlobData,
			}}}},
			Placements: map[uint64]schema.PlacementRecord{7: {
				State: schema.PlacementLive, StorageClass: "hot", Bytes: 24,
			}},
			DebtKey: debtKey,
			Debt:    &debt,
		}
	}
	imports := []LegacyPackImport{makeImport(source1, blob1, 1), makeImport(source2, blob2, 9)}
	imports[1].Debt = nil
	checkpointRecord := schema.ImportCheckpointRecord{PacksImported: 2, BlobsImported: 2, ErrorsSeen: 2}
	checkpoint := Mutation{Key: schema.ImportCheckpointKey(source2), Value: encodeSchemaRecord(t, checkpointRecord)}
	for _, imported := range imports {
		if err := oneAtATime.ImportLegacyPack(ctx, imported); err != nil {
			t.Fatal(err)
		}
	}
	if err := oneAtATime.Put(ctx, checkpoint.Key, checkpoint.Value, true); err != nil {
		t.Fatal(err)
	}
	if err := batched.ImportLegacyPacks(ctx, imports, &checkpoint); err != nil {
		t.Fatal(err)
	}

	prefixes := [][]byte{[]byte("b:"), []byte("p:"), []byte("pl:"), []byte("q:"), []byte("a:"), []byte("ph:"), []byte("meta:")}
	sequentialRecords := readSchemaPrefixes(t, oneAtATime, prefixes)
	batchRecords := readSchemaPrefixes(t, batched, prefixes)
	normalizeAggregateSequences(t, sequentialRecords)
	normalizeAggregateSequences(t, batchRecords)
	normalizeDebtAttemptTime(t, sequentialRecords, debtKey)
	normalizeDebtAttemptTime(t, batchRecords, debtKey)
	if !reflect.DeepEqual(batchRecords, sequentialRecords) {
		t.Fatalf("batched authoritative metadata differs:\nsequential=%#v\nbatched=%#v", sequentialRecords, batchRecords)
	}
	if stats := batched.LegacyImportStats(); !stats.FilterFallbackToDatabase || stats.DefinitelyAbsentLookups != 0 {
		t.Fatalf("fallback filter stats = %#v", stats)
	}
	resolvedDebt, err := schema.UnmarshalCrawlDebtRecord(batchRecords[string(debtKey)])
	if err != nil || resolvedDebt.Status != schema.DebtResolved || resolvedDebt.ErrorClass != "" {
		t.Fatalf("mixed batch debt = %#v, err=%v", resolvedDebt, err)
	}
}

func normalizeDebtAttemptTime(t *testing.T, records map[string][]byte, key []byte) {
	t.Helper()
	value, found := records[string(key)]
	if !found {
		return
	}
	debt, err := schema.UnmarshalCrawlDebtRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	debt.LastAttemptUnixNano = 0
	records[string(key)] = encodeSchemaRecord(t, debt)
}

func readSchemaPrefixes(t *testing.T, store *SchemaStore, prefixes [][]byte) map[string][]byte {
	t.Helper()
	result := make(map[string][]byte)
	for _, prefix := range prefixes {
		var after []byte
		for {
			entries, done, err := store.ScanPrefix(context.Background(), prefix, after, 1000)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				result[string(entry.Key)] = append([]byte(nil), entry.Value...)
				after = entry.Key
			}
			if done {
				break
			}
		}
	}
	return result
}

func normalizeAggregateSequences(t *testing.T, records map[string][]byte) {
	t.Helper()
	for key, value := range records {
		if !bytes.HasPrefix([]byte(key), []byte("a:")) {
			continue
		}
		aggregate, err := schema.UnmarshalPackAggregate(value)
		if err != nil {
			t.Fatal(err)
		}
		aggregate.UpdateSequence = 0
		records[key] = encodeSchemaRecord(t, aggregate)
	}
}
