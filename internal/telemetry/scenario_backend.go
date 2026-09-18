package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync"

	"github.com/otuschhoff/vaultic/internal/backend"
)

type experimentBackend struct {
	backend.Backend
	controller *ExperimentController
}

func WrapExperimentBackend(inner backend.Backend, controller *ExperimentController) backend.Backend {
	if inner == nil || controller == nil || !controller.Enabled() {
		return inner
	}
	return &experimentBackend{Backend: inner, controller: controller}
}

func (wrapped *experimentBackend) Save(ctx context.Context, handle backend.Handle, reader backend.RewindReader) error {
	if !wrapped.controller.Matches("repository", "put") && !wrapped.controller.Matches("cache", "put") {
		return wrapped.Backend.Save(ctx, handle, reader)
	}
	return wrapped.controller.Run(ctx, backendEvent("put", handle, 0), func(ctx context.Context) error {
		return wrapped.Backend.Save(ctx, handle, reader)
	})
}

func (wrapped *experimentBackend) Load(ctx context.Context, handle backend.Handle, length int, offset int64, consume func(io.Reader) error) error {
	method := "get"
	if length > 0 || offset != 0 {
		method = "range_get"
	}
	if !wrapped.controller.Matches("repository", method) && !wrapped.controller.Matches("cache", method) {
		return wrapped.Backend.Load(ctx, handle, length, offset, consume)
	}
	bytes := uint64(0)
	if length > 0 {
		bytes = uint64(length)
	}
	event := backendEvent(method, handle, bytes)
	if wrapped.controller.profile.Mode == DelayService {
		return wrapped.Backend.Load(ctx, handle, length, offset, func(reader io.Reader) error {
			return consume(WrapExperimentReader(ctx, wrapped.controller, event, reader))
		})
	}
	return wrapped.controller.Run(ctx, event, func(ctx context.Context) error {
		return wrapped.Backend.Load(ctx, handle, length, offset, consume)
	})
}

func (wrapped *experimentBackend) Stat(ctx context.Context, handle backend.Handle) (info backend.FileInfo, err error) {
	if !wrapped.controller.Matches("repository", "head") && !wrapped.controller.Matches("cache", "head") {
		return wrapped.Backend.Stat(ctx, handle)
	}
	err = wrapped.controller.Run(ctx, backendEvent("head", handle, 0), func(ctx context.Context) error {
		info, err = wrapped.Backend.Stat(ctx, handle)
		return err
	})
	return info, err
}

func (wrapped *experimentBackend) List(ctx context.Context, fileType backend.FileType, consume func(backend.FileInfo) error) error {
	if !wrapped.controller.Matches("repository", "list") && !wrapped.controller.Matches("cache", "list") {
		return wrapped.Backend.List(ctx, fileType, consume)
	}
	event := namedExperimentEvent("list", fileType.String(), 0)
	return wrapped.controller.Run(ctx, event, func(ctx context.Context) error {
		return wrapped.Backend.List(ctx, fileType, consume)
	})
}

func (wrapped *experimentBackend) Remove(ctx context.Context, handle backend.Handle) error {
	if !wrapped.controller.Matches("repository", "delete") && !wrapped.controller.Matches("cache", "delete") {
		return wrapped.Backend.Remove(ctx, handle)
	}
	return wrapped.controller.Run(ctx, backendEvent("delete", handle, 0), func(ctx context.Context) error {
		return wrapped.Backend.Remove(ctx, handle)
	})
}

func (wrapped *experimentBackend) CompareAndSwap(ctx context.Context, handle backend.Handle, expected, replacement []byte) (current []byte, swapped bool, err error) {
	conditional := backend.AsCapability[backend.ConditionalWriter](wrapped.Backend)
	if conditional == nil {
		return nil, false, backend.ErrConditionalWriteUnsupported
	}
	if !wrapped.controller.Matches("coordination", "conditional_write") && !wrapped.controller.Matches("repository", "conditional_write") {
		return conditional.CompareAndSwap(ctx, handle, expected, replacement)
	}
	err = wrapped.controller.Run(ctx, backendEvent("conditional_write", handle, uint64(len(replacement))), func(ctx context.Context) error {
		current, swapped, err = conditional.CompareAndSwap(ctx, handle, expected, replacement)
		return err
	})
	return current, swapped, err
}

func (wrapped *experimentBackend) Unwrap() backend.Backend { return wrapped.Backend }

func backendEvent(method string, handle backend.Handle, bytes uint64) ExperimentEvent {
	return namedExperimentEvent(method, handle.Type.String()+"\x00"+handle.Name, bytes)
}

func namedExperimentEvent(method, identity string, bytes uint64) ExperimentEvent {
	digest := sha256.Sum256([]byte(method + "\x00" + identity))
	return ExperimentEvent{Identity: method + "-" + hex.EncodeToString(digest[:12]), Bytes: bytes}
}

type experimentReader struct {
	ctx        context.Context
	controller *ExperimentController
	event      ExperimentEvent
	reader     io.Reader
	once       sync.Once
	err        error
}

func WrapExperimentReader(ctx context.Context, controller *ExperimentController, event ExperimentEvent, reader io.Reader) io.Reader {
	if controller == nil || !controller.Enabled() || reader == nil {
		return reader
	}
	return &experimentReader{ctx: ctx, controller: controller, event: event, reader: reader}
}

func (reader *experimentReader) Read(buffer []byte) (read int, err error) {
	reader.once.Do(func() {
		reader.err = reader.controller.Run(reader.ctx, reader.event, func(context.Context) error { return nil })
	})
	if reader.err != nil {
		return 0, reader.err
	}
	select {
	case <-reader.ctx.Done():
		return 0, reader.ctx.Err()
	default:
		return reader.reader.Read(buffer)
	}
}

var _ backend.Backend = (*experimentBackend)(nil)
var _ backend.ConditionalWriter = (*experimentBackend)(nil)
