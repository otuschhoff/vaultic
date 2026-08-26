package warmupcmd

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/warmup"
)

// WarmupCommandBackend wraps a backend.Backend and routes Warmup/WarmupWait to
// a user-supplied warm-up command (internal/warmup) instead of the backend's
// own warm-up logic. This is used when a --warm-up-command is configured for a
// (cold) repository.
type WarmupCommandBackend struct {
	backend.Backend
	runner *warmup.Runner
	// pathFor maps a handle to the path the warm-up command expects. It
	// defaults to the backend layout path.
	pathFor func(backend.Handle) string
}

// NewWarmupCommandBackend wraps be so that warm-up requests run the given
// command. pathFor may be nil to use the default (layout) path.
func NewWarmupCommandBackend(be backend.Backend, runner *warmup.Runner, pathFor func(backend.Handle) string) *WarmupCommandBackend {
	if pathFor == nil {
		pathFor = defaultWarmupPath(be)
	}
	return &WarmupCommandBackend{Backend: be, runner: runner, pathFor: pathFor}
}

// defaultWarmupPath derives the backend path for a handle. For local-style
// backends the layout path is used; otherwise the pack ID (handle name) is
// passed (the warm-up command is expected to map it onto the backend path).
func defaultWarmupPath(be backend.Backend) func(backend.Handle) string {
	type filenamer interface {
		Filename(backend.Handle) string
	}
	// unwrap to find a backend exposing Filename
	cur := be
	for cur != nil {
		if f, ok := cur.(filenamer); ok {
			return f.Filename
		}
		u, ok := cur.(backend.Unwrapper)
		if !ok {
			break
		}
		cur = u.Unwrap()
	}
	return func(h backend.Handle) string { return h.Name }
}

// Warmup starts warming up the handles via the warm-up command. It returns the
// handles for which a warm-up was requested; the command already ran
// synchronously (batch) so WarmupWait is a no-op beyond the command.
func (b *WarmupCommandBackend) Warmup(ctx context.Context, handles []backend.Handle) ([]backend.Handle, error) {
	if b.runner == nil || !b.runner.Enabled() {
		return b.Backend.Warmup(ctx, handles)
	}
	debug.Log("warm-up command: warming up %d files", len(handles))
	if err := b.runner.Warmup(ctx, handles, b.pathFor); err != nil {
		return nil, err
	}
	// the command ran synchronously; nothing left warming up in the background
	return nil, nil
}

// WarmupWait is a no-op for command-based warm-up (the command is synchronous).
func (b *WarmupCommandBackend) WarmupWait(ctx context.Context, handles []backend.Handle) error {
	if b.runner == nil || !b.runner.Enabled() {
		return b.Backend.WarmupWait(ctx, handles)
	}
	return nil
}

// make sure the wrapper is transparent for backend.AsBackend.
var _ backend.Unwrapper = &WarmupCommandBackend{}

// Unwrap returns the underlying backend.
func (b *WarmupCommandBackend) Unwrap() backend.Backend {
	return b.Backend
}

// ensure interface compliance.
var _ backend.Backend = &WarmupCommandBackend{}
