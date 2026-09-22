package daemon

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc"
)

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
	if stats := session.ScanStats(); stats.Records != 1 || stats.Chunks != 1 || stats.Bytes == 0 || stats.IteratorSetupNS == 0 {
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
}

type scanStreamRPC struct {
	vaulticdbv1.VaulticDBClient
	responses []*vaulticdbv1.ScanResponse
	context   context.Context
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

func readSessionTestPack(packID, blobID schema.ID) PublishedPack {
	return PublishedPack{
		PackID: packID,
		Record: schema.PackRecord{
			Type: schema.PackData, PayloadSize: 7, BlobCount: 1, Lifecycle: schema.PackExportPending,
		},
		Blobs: map[schema.ID]schema.BlobRecord{blobID: {
			Locations: []schema.BlobLocation{{PackID: packID, Length: 7, Type: schema.BlobData}},
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
