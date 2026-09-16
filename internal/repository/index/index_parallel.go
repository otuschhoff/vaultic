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

// ForAllIndexesInOrder loads index files in parallel and calls fn in list order.
// Decoded indexes retained while an earlier index is loading are bounded by the
// same worker count used by ForAllIndexes.
func ForAllIndexesInOrder(ctx context.Context, lister vaultic.Lister, repo vaultic.LoaderUnpacked,
	fn func(id vaultic.ID, index *Index, err error) error) error {
	type listedIndex struct {
		id   vaultic.ID
		size int64
	}
	type loadedIndex struct {
		index *Index
		err   error
	}

	var listed []listedIndex
	if err := lister.List(ctx, vaultic.IndexFile, func(id vaultic.ID, size int64) error {
		listed = append(listed, listedIndex{id: id, size: size})
		return nil
	}); err != nil {
		return err
	}
	if len(listed) == 0 {
		return nil
	}

	workerCount := min(int(repo.Connections()+uint(runtime.GOMAXPROCS(0))), len(listed))
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	permits := make(chan struct{}, workerCount)
	for range workerCount {
		permits <- struct{}{}
	}
	results := make([]chan loadedIndex, len(listed))
	for sequence := range results {
		results[sequence] = make(chan loadedIndex, 1)
	}

	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for sequence := range jobs {
				select {
				case <-permits:
				case <-workerCtx.Done():
					return
				}
				entry := listed[sequence]
				buf, err := repo.LoadUnpacked(workerCtx, vaultic.IndexFile, entry.id)
				var idx *Index
				if err == nil {
					idx, err = DecodeIndex(buf, entry.id)
				}
				select {
				case results[sequence] <- loadedIndex{index: idx, err: err}:
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(jobs)
		for sequence := range listed {
			select {
			case jobs <- sequence:
			case <-workerCtx.Done():
				return
			}
		}
	}()

	for sequence, entry := range listed {
		select {
		case loaded := <-results[sequence]:
			if err := fn(entry.id, loaded.index, loaded.err); err != nil {
				cancel()
				workers.Wait()
				return err
			}
			permits <- struct{}{}
		case <-workerCtx.Done():
			workers.Wait()
			return context.Cause(workerCtx)
		}
	}
	workers.Wait()
	return nil
}
