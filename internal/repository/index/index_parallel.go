package index

import (
	"context"
	"runtime"
	"sync"

	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// ForAllIndexes loads all index files in parallel and calls the given callback.
// It is guaranteed that the function is not run concurrently. If the callback
// returns an error, this function is cancelled and also returns that error.
func ForAllIndexes(ctx context.Context, lister vaultic.Lister, repo vaultic.LoaderUnpacked,
	fn func(id vaultic.ID, index *Index, err error) error) error {

	// decoding an index can take quite some time such that this can be both CPU- or IO-bound
	// as the whole index is kept in memory anyways, a few workers too much don't matter
	workerCount := repo.Connections() + uint(runtime.GOMAXPROCS(0))

	var m sync.Mutex
	return vaultic.ParallelList(ctx, lister, vaultic.IndexFile, workerCount, func(ctx context.Context, id vaultic.ID, _ int64) error {
		var err error
		var idx *Index

		buf, err := repo.LoadUnpacked(ctx, vaultic.IndexFile, id)
		if err == nil {
			idx, err = DecodeIndex(buf, id)
		}

		m.Lock()
		defer m.Unlock()
		return fn(id, idx, err)
	})
}
