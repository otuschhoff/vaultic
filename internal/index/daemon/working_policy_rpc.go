package daemon

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/otuschhoff/vaultic/internal/workingstate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func reserveWorkingRPC(ctx context.Context, request any) (func(), int, error) {
	available, selected := workingstate.Available(ctx)
	if !selected {
		return func() {}, 0, nil
	}
	requestSize := uint64(0)
	if message, ok := request.(proto.Message); ok {
		requestSize = uint64(proto.Size(message))
	}
	receive := min(uint64(4<<20), available/128)
	if receive < 1024 {
		return nil, 0, workingstate.ErrMemoryLimitExceeded
	}
	release, err := workingstate.Reserve(ctx, receive*64+requestSize*3+4096)
	if err != nil {
		return nil, 0, err
	}
	return release, int(receive), nil
}

func workingUnaryInterceptor(ctx context.Context, method string, request, response any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
	if strings.HasSuffix(method, "/Rollback") || strings.HasSuffix(method, "/CancelBegin") {
		if message, ok := request.(proto.Message); ok && proto.Size(message) > 64<<10 {
			return workingstate.ErrMemoryLimitExceeded
		}
		return invoke(ctx, method, request, response, connection, append(options, grpc.MaxCallSendMsgSize(64<<10), grpc.MaxCallRecvMsgSize(4096))...)
	}
	release, limit, err := reserveWorkingRPC(ctx, request)
	if err != nil {
		return err
	}
	defer release()
	if limit != 0 {
		options = append(options, grpc.MaxCallRecvMsgSize(limit))
	}
	err = invoke(ctx, method, request, response, connection, options...)
	if limit != 0 && status.Code(err) == codes.ResourceExhausted && strings.HasPrefix(status.Convert(err).Message(), "grpc: received message larger than max") {
		return errors.Join(workingstate.ErrMemoryLimitExceeded, err)
	}
	return err
}

func workingStreamInterceptor(ctx context.Context, description *grpc.StreamDesc, connection *grpc.ClientConn, method string, open grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
	release, limit, err := reserveWorkingRPC(ctx, nil)
	if err != nil {
		return nil, err
	}
	if limit == 0 {
		release()
		return open(ctx, description, connection, method, options...)
	}
	stream, err := open(ctx, description, connection, method, append(options, grpc.MaxCallRecvMsgSize(limit))...)
	if err != nil {
		release()
		return nil, err
	}
	wrapped := &workingClientStream{ClientStream: stream, release: release}
	wrapped.stop = context.AfterFunc(stream.Context(), wrapped.finish)
	return wrapped, nil
}

type workingClientStream struct {
	grpc.ClientStream
	once    sync.Once
	release func()
	stop    func() bool
}

func (stream *workingClientStream) finish() { stream.once.Do(stream.release) }
func (stream *workingClientStream) RecvMsg(message any) error {
	err := stream.ClientStream.RecvMsg(message)
	if err != nil {
		if stream.stop != nil {
			stream.stop()
		}
		stream.finish()
		if status.Code(err) == codes.ResourceExhausted && strings.HasPrefix(status.Convert(err).Message(), "grpc: received message larger than max") {
			return errors.Join(workingstate.ErrMemoryLimitExceeded, err)
		}
	}
	return err
}
