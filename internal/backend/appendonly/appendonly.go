package appendonly

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/errors"
)

// ErrAppendOnly is returned when a write or delete is attempted on an
// append-only repository.
var ErrAppendOnly = errors.Fatal("repository is in append-only mode")

// Backend wraps a backend.Backend and rejects all operations that modify or
// delete existing repository data. Saving new files and reading is allowed;
// Remove, Delete and overwriting existing files are rejected.
//
// This is used for repositories whose config sets append_only (see
// doc/vaultic/05-rustic-parity/02-workstreams.md, workstream WS-A / feature F5).
type Backend struct {
	backend.Backend
}

// New wraps be so that it behaves as append-only.
func New(be backend.Backend) *Backend {
	return &Backend{Backend: be}
}

// Remove deletes lock files (so that locking and unlock keep working) but
// rejects removal of any repository data in append-only mode.
func (b *Backend) Remove(ctx context.Context, h backend.Handle) error {
	if h.Type == backend.LockFile {
		return b.Backend.Remove(ctx, h)
	}
	return ErrAppendOnly
}

// Delete is rejected in append-only mode.
func (b *Backend) Delete(_ context.Context) error {
	return ErrAppendOnly
}

// Save stores new files but refuses to overwrite an existing file or to
// replace the repository config. Appending new snapshots, indexes, keys and
// packs is allowed.
func (b *Backend) Save(ctx context.Context, h backend.Handle, rd backend.RewindReader) error {
	// the config file may never be replaced in append-only mode
	if h.Type == backend.ConfigFile {
		return ErrAppendOnly
	}

	// refuse to overwrite an existing file
	_, err := b.Backend.Stat(ctx, h)
	if err == nil {
		return errors.Fatalf("%v: refusing to overwrite %v", ErrAppendOnly, h)
	}
	if !b.Backend.IsNotExist(err) {
		return err
	}

	return b.Backend.Save(ctx, h, rd)
}

// make sure that *Backend implements backend.Backend and backend.Unwrapper.
var _ backend.Backend = &Backend{}
var _ backend.Unwrapper = &Backend{}

// Unwrap returns the underlying backend.
func (b *Backend) Unwrap() backend.Backend {
	return b.Backend
}
