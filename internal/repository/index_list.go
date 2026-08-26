package repository

import (
	"context"
	"iter"

	"github.com/vaultic/vaultic/internal/errors"
	"github.com/vaultic/vaultic/internal/repository/index"
	"github.com/vaultic/vaultic/internal/vaultic"
)

// IndexBlob is one blob handle from an on-disk index file, or an error from loading/decoding
// that file.
type IndexBlob struct {
	Handle vaultic.BlobHandle
	Error  error
}

// AllIndexBlobs streams blob handles from each index file without building a master index.
func AllIndexBlobs(ctx context.Context, lister vaultic.Lister, loader vaultic.LoaderUnpacked) iter.Seq[IndexBlob] {
	return func(yield func(IndexBlob) bool) {
		stopIteration := errors.New("stop index blob iteration")
		err := index.ForAllIndexes(ctx, lister, loader, func(_ vaultic.ID, idx *index.Index, err error) error {
			if err != nil {
				return err
			}
			for blob := range idx.Values() {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if !yield(IndexBlob{Handle: blob.Handle()}) {
					return stopIteration
				}
			}
			return nil
		})
		if err != nil && !errors.Is(err, stopIteration) {
			yield(IndexBlob{Error: err})
		}
	}
}
