package daemon

import (
	"context"
	"errors"
	"testing"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"google.golang.org/grpc"
)

func TestResponseDeliveryInterceptorRunsOnlyAfterSuccess(t *testing.T) {
	delivered := 0
	interceptor := responseDeliveryInterceptor(func(_ context.Context, method string) error {
		delivered++
		if method != "/vaulticdb.v1.VaulticDB/Commit" {
			t.Fatalf("method = %q", method)
		}
		return context.DeadlineExceeded
	})
	invoked := 0
	err := interceptor(context.Background(), "/vaulticdb.v1.VaulticDB/Commit", &vaulticdbv1.TransactionRequest{}, &vaulticdbv1.CommitResponse{}, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			invoked++
			return nil
		})
	if !errors.Is(err, context.DeadlineExceeded) || invoked != 1 || delivered != 1 {
		t.Fatalf("success path err=%v invoked=%d delivered=%d", err, invoked, delivered)
	}
	want := errors.New("server failed")
	err = interceptor(context.Background(), "/vaulticdb.v1.VaulticDB/Commit", nil, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return want })
	if !errors.Is(err, want) || delivered != 1 {
		t.Fatalf("failure path err=%v delivered=%d", err, delivered)
	}
}
