// Package hotcold implements a hot/cold composite backend.
//
// A hot/cold repository splits a repository over two storage locations
// (rustic's hot/cold support, see doc/rustic-parity-roadmap.md WS-D):
//
//   - the "cold" backend holds everything and is a complete repository on its
//     own (when fully warmed up). Data packs live only here.
//   - the "hot" backend additionally holds all metadata (config, keys,
//     snapshots, indexes) and tree packs. Metadata written by vaultic is
//     mirrored to both, so reads that only need metadata never touch the cold
//     (slow/expensive) storage.
//
// Writes go to the hot backend and are mirrored to the cold backend (except
// data packs, which are written to the cold backend only). Reads of metadata
// and tree packs are served from the hot backend; data packs are read from the
// cold backend. Removals (only used by prune/forget) are applied to both.
package hotcold

import (
	"context"
	"hash"
	"io"

	"github.com/vaultic/vaultic/internal/backend"
	"github.com/vaultic/vaultic/internal/debug"
	"github.com/vaultic/vaultic/internal/errors"
)

// Backend routes operations to a hot and a cold backend.
type Backend struct {
	hot  backend.Backend
	cold backend.Backend
}

// New returns a composite hot/cold backend. hot must hold the metadata and
// tree packs, cold the complete repository (data packs).
func New(hot, cold backend.Backend) *Backend {
	return &Backend{hot: hot, cold: cold}
}

// Hot returns the hot (metadata) backend.
func (b *Backend) Hot() backend.Backend { return b.hot }

// Cold returns the cold (data) backend.
func (b *Backend) Cold() backend.Backend { return b.cold }

// isHotFile reports whether a file is served from the hot backend: all
// metadata (config, keys, snapshots, indexes) and tree packs. Locks are
// neither metadata nor mirrored; they live on the cold (authoritative) part.
func isHotFile(h backend.Handle) bool {
	switch h.Type {
	case backend.PackFile:
		return h.IsMetadata // tree packs live in the hot part
	case backend.LockFile:
		return false // locks are not mirrored; they live on the cold repo
	default:
		return true // config, keys, snapshots, indexes
	}
}

// Properties returns the combined properties (connections of the cold backend,
// which serves the bulk data).
func (b *Backend) Properties() backend.Properties {
	p := b.cold.Properties()
	return p
}

// Hasher returns the cold backend's hash function.
func (b *Backend) Hasher() hash.Hash {
	return b.cold.Hasher()
}

// Save stores h. Metadata and tree packs are written to the hot backend and
// mirrored to the cold backend; data packs are written to the cold backend
// only.
func (b *Backend) Save(ctx context.Context, h backend.Handle, rd backend.RewindReader) error {
	if !isHotFile(h) {
		return b.cold.Save(ctx, h, rd)
	}

	// write to hot, then mirror to cold
	if err := b.hot.Save(ctx, h, rd); err != nil {
		return err
	}
	if err := rd.Rewind(); err != nil {
		return err
	}
	if err := b.cold.Save(ctx, h, rd); err != nil {
		return errors.Errorf("unable to mirror %v to the cold backend: %w", h, err)
	}
	return nil
}

// Load reads h. Metadata/tree packs are read from the hot backend; if a file is
// missing there (e.g. written before the hot/cold split was set up), it is
// read from the cold backend as a fallback. Data packs are read from cold.
func (b *Backend) Load(ctx context.Context, h backend.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	if isHotFile(h) {
		err := b.hot.Load(ctx, h, length, offset, fn)
		if err != nil && b.hot.IsNotExist(err) {
			debug.Log("%v not in hot backend, reading from cold", h)
			return b.cold.Load(ctx, h, length, offset, fn)
		}
		return err
	}
	return b.cold.Load(ctx, h, length, offset, fn)
}

// Stat returns file info, falling back to the cold backend when the file is
// not present in the hot backend.
func (b *Backend) Stat(ctx context.Context, h backend.Handle) (backend.FileInfo, error) {
	if isHotFile(h) {
		fi, err := b.hot.Stat(ctx, h)
		if err != nil && b.hot.IsNotExist(err) {
			return b.cold.Stat(ctx, h)
		}
		return fi, err
	}
	return b.cold.Stat(ctx, h)
}

// List lists files of type t from the appropriate backend. Data packs are
// listed from the cold backend (their only location), everything else from the
// hot backend.
func (b *Backend) List(ctx context.Context, t backend.FileType, fn func(backend.FileInfo) error) error {
	if t == backend.PackFile {
		// list data packs from cold and tree packs (metadata) from hot.
		// The backend List for packs does not distinguish tree/data packs, so
		// we list both and let the caller deduplicate; to avoid reporting data
		// packs twice we list packs from cold and additionally tree packs from
		// hot.
		var err error
		err = b.cold.List(ctx, t, fn)
		if err != nil {
			return err
		}
		// tree packs are mirrored to cold too (they are written to both), so
		// nothing extra is listed from hot here.
		return nil
	}
	return b.hot.List(ctx, t, fn)
}

// Remove removes h. Data packs and locks live only on the cold backend;
// metadata/tree packs live on both. Removing from a side that does not hold
// the file (not-exist) is not an error.
func (b *Backend) Remove(ctx context.Context, h backend.Handle) error {
	if !isHotFile(h) {
		// locks and data packs only exist on the cold backend
		return b.cold.Remove(ctx, h)
	}

	// metadata/tree packs: remove from both; tolerate not-exist on either side
	var firstErr error
	for _, be := range []backend.Backend{b.hot, b.cold} {
		err := be.Remove(ctx, h)
		if err != nil && firstErr == nil && !be.IsNotExist(err) {
			firstErr = err
		}
	}
	return firstErr
}

// Delete deletes both backends.
func (b *Backend) Delete(ctx context.Context) error {
	if err := b.cold.Delete(ctx); err != nil {
		return err
	}
	return b.hot.Delete(ctx)
}

// Close closes both backends.
func (b *Backend) Close() error {
	errHot := b.hot.Close()
	errCold := b.cold.Close()
	if errHot != nil {
		return errHot
	}
	return errCold
}

// IsNotExist delegates to the cold backend.
func (b *Backend) IsNotExist(err error) bool {
	return b.cold.IsNotExist(err)
}

// IsPermanentError delegates to the cold backend.
func (b *Backend) IsPermanentError(err error) bool {
	return b.cold.IsPermanentError(err)
}

// Warmup warms up handles on the cold backend (the only place where data can
// be cold). Metadata/tree packs are always warm (they live on the hot part),
// so only data packs are passed through.
func (b *Backend) Warmup(ctx context.Context, handles []backend.Handle) ([]backend.Handle, error) {
	cold := make([]backend.Handle, 0, len(handles))
	for _, h := range handles {
		if !isHotFile(h) {
			cold = append(cold, h)
		}
	}
	if len(cold) == 0 {
		return nil, nil
	}
	debug.Log("warming up %d cold files", len(cold))
	return b.cold.Warmup(ctx, cold)
}

// WarmupWait waits for warm-up on the cold backend.
func (b *Backend) WarmupWait(ctx context.Context, handles []backend.Handle) error {
	return b.cold.WarmupWait(ctx, handles)
}

// make sure *Backend implements backend.Backend.
var _ backend.Backend = &Backend{}
