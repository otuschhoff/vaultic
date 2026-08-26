package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// LoadRaw reads all data stored in the backend for the file with id and filetype t.
// If the backend returns data that does not match the id, then the buffer is returned
// along with an error that is a vaultic.ErrInvalidData error.
func (r *Repository) LoadRaw(ctx context.Context, t vaultic.FileType, id vaultic.ID) (buf []byte, err error) {
	h := backend.Handle{Type: backend.FileType(t), Name: id.String()}

	buf, err = loadRaw(ctx, r.be, h)

	// retry loading damaged data only once. If a file fails to download correctly
	// the second time, then it is likely corrupted at the backend.
	if h.Type != backend.ConfigFile && id != vaultic.Hash(buf) {
		if r.cache != nil {
			// Cleanup cache to make sure it's not the cached copy that is broken.
			// Ignore error as there's not much we can do in that case.
			_ = r.cache.Forget(h)
		}

		buf, err = loadRaw(ctx, r.be, h)

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
