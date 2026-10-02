package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/workingstate"
)

// LoadRaw reads all data stored in the backend for the file with id and filetype t.
// If the backend returns data that does not match the id, then the buffer is returned
// along with an error that is a vaultic.ErrInvalidData error.
func (r *Repository) LoadRaw(ctx context.Context, t vaultic.FileType, id vaultic.ID) (buf []byte, err error) {
	done := r.accounting.StartBlocking(ctx, "read", "backend_io")
	defer done.Done()
	h := backend.Handle{Type: backend.FileType(t), Name: id.String()}

	load := func() ([]byte, error) {
		dependency := r.accounting.StartDependency(ctx, "repository")
		loaded, loadErr := loadRaw(ctx, r.be, h)
		dependency.AddBytes(uint64(len(loaded)))
		valid := h.Type == backend.ConfigFile || id == vaultic.Hash(loaded)
		outcomeErr := loadErr
		if outcomeErr == nil && !valid {
			outcomeErr = vaultic.ErrInvalidData
		}
		dependency.Finish(outcomeErr)
		if outcomeErr == nil {
			r.accounting.AddProcessed(ctx, "repository", uint64(len(loaded)))
		}
		return loaded, loadErr
	}

	buf, err = load()
	if errors.Is(err, workingstate.ErrMemoryLimitExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}

	// retry loading damaged data only once. If a file fails to download correctly
	// the second time, then it is likely corrupted at the backend.
	if h.Type != backend.ConfigFile && id != vaultic.Hash(buf) {
		if r.cache != nil {
			// Cleanup cache to make sure it's not the cached copy that is broken.
			// Ignore error as there's not much we can do in that case.
			_ = r.cache.Forget(h) // Cache eviction cannot change the authoritative backend result.
		}

		buf, err = load()

		if err == nil && id != vaultic.Hash(buf) {
			// Return corrupted data to the caller if it is still broken the second time to
			// let the caller decide what to do with the data.
			return buf, fmt.Errorf("LoadRaw(%v): %w", h, vaultic.ErrInvalidData)
		}
	}

	if err != nil {
		return nil, err
	}
	return buf, nil
}

func loadRaw(ctx context.Context, be backend.Backend, h backend.Handle) (buf []byte, err error) {
	if _, selected := workingstate.Available(ctx); selected && (h.Type == backend.IndexFile || h.Type == backend.SnapshotFile) {
		info, err := be.Stat(ctx, h)
		if err != nil {
			return nil, err
		}
		if info.Size < 0 || uint64(info.Size) > uint64(^uint(0)>>1)-1 || uint64(info.Size) > (^uint64(0)-4096)/3 {
			return nil, workingstate.ErrMemoryLimitExceeded
		}
		release, err := workingstate.Reserve(ctx, uint64(info.Size)*3+4096)
		if err != nil {
			return nil, err
		}
		defer release()
		err = be.Load(ctx, h, 0, 0, func(reader io.Reader) error {
			loaded, err := io.ReadAll(io.LimitReader(reader, info.Size+1))
			if err != nil {
				return err
			}
			if int64(len(loaded)) > info.Size {
				return workingstate.ErrMemoryLimitExceeded
			}
			buf = loaded
			return ctx.Err()
		})
		return buf, err
	}
	err = be.Load(ctx, h, 0, 0, func(rd io.Reader) error {
		wr := new(bytes.Buffer)
		_, cerr := io.Copy(wr, rd)
		if cerr != nil {
			return cerr
		}
		buf = wr.Bytes()
		return cerr
	})
	return buf, err
}
