package index

import (
	"context"
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const blobLookupCacheEntryBytes = 192

type blobLookupSession interface {
	Context() context.Context
	LookupBlobSizesContext(context.Context, []vaultic.BlobHandle) ([]vaultic.BlobSize, error)
}

type blobLookupResult struct {
	batch   *blobLookupBatch
	ordinal int
}

type blobLookupBatch struct {
	done  chan struct{}
	sizes []vaultic.BlobSize
	err   error
}

type CachedBlobLookup struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	session blobLookupSession
	local   *LegacyEngine
	cache   *lru.Cache[vaultic.BlobHandle, vaultic.BlobSize]
	slots   chan struct{}
	mutex   sync.Mutex
	pending map[vaultic.BlobHandle]blobLookupResult
	workers sync.WaitGroup
}

func NewCachedBlobLookup(session blobLookupSession, local *LegacyEngine, budgetBytes, concurrency int) (*CachedBlobLookup, error) {
	if session == nil || local == nil || budgetBytes < blobLookupCacheEntryBytes || concurrency < 1 {
		return nil, fmt.Errorf("blob lookup cache requires a session, write index, positive budget and concurrency")
	}
	if err := context.Cause(session.Context()); err != nil {
		return nil, err
	}
	cache, err := lru.New[vaultic.BlobHandle, vaultic.BlobSize](budgetBytes / blobLookupCacheEntryBytes)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(session.Context())
	return &CachedBlobLookup{ctx: ctx, cancel: cancel, session: session, local: local, cache: cache,
		slots: make(chan struct{}, concurrency), pending: make(map[vaultic.BlobHandle]blobLookupResult)}, nil
}

func (lookup *CachedBlobLookup) LookupSizesContext(ctx context.Context, handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
	if err := vaultic.ValidateBlobLookupBatch(handles); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lookup.mutex.Lock()
	if err := context.Cause(lookup.ctx); err != nil {
		lookup.mutex.Unlock()
		return nil, err
	}
	results := lookup.local.master.LookupSizes(handles)
	cached := true
	for ordinal, handle := range handles {
		if results[ordinal].Found {
			continue
		}
		size, found := lookup.cache.Get(handle)
		if !found {
			cached = false
			break
		}
		results[ordinal] = size
	}
	lookup.mutex.Unlock()
	if cached {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := context.Cause(lookup.ctx); err != nil {
			return nil, err
		}
		return results, nil
	}
	select {
	case lookup.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lookup.ctx.Done():
		return nil, context.Cause(lookup.ctx)
	}
	results = lookup.local.master.LookupSizes(handles)
	waiting := make([]blobLookupResult, len(handles))
	var missing []vaultic.BlobHandle
	var batch *blobLookupBatch
	lookup.mutex.Lock()
	if err := context.Cause(lookup.ctx); err != nil {
		lookup.mutex.Unlock()
		<-lookup.slots
		return nil, err
	}
	for ordinal, handle := range handles {
		if results[ordinal].Found {
			continue
		}
		if size, found := lookup.cache.Get(handle); found {
			results[ordinal] = size
			continue
		}
		result, found := lookup.pending[handle]
		if !found {
			if batch == nil {
				batch = &blobLookupBatch{done: make(chan struct{})}
			}
			result = blobLookupResult{batch: batch, ordinal: len(missing)}
			lookup.pending[handle] = result
			missing = append(missing, handle)
		}
		waiting[ordinal] = result
	}
	if len(missing) > 0 {
		lookup.workers.Add(1)
		go lookup.fetch(missing, batch)
	} else {
		<-lookup.slots
	}
	lookup.mutex.Unlock()
	for ordinal, result := range waiting {
		if result.batch == nil {
			continue
		}
		select {
		case <-result.batch.done:
			if result.batch.err != nil {
				return nil, result.batch.err
			}
			results[ordinal] = result.batch.sizes[result.ordinal]
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-lookup.ctx.Done():
			return nil, context.Cause(lookup.ctx)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := context.Cause(lookup.ctx); err != nil {
		return nil, err
	}
	for ordinal, current := range lookup.local.master.LookupSizes(handles) {
		if current.Found {
			results[ordinal] = current
		}
	}
	return results, nil
}

func (lookup *CachedBlobLookup) fetch(handles []vaultic.BlobHandle, batch *blobLookupBatch) {
	defer lookup.workers.Done()
	defer func() { <-lookup.slots }()
	sizes, err := lookup.session.LookupBlobSizesContext(lookup.ctx, handles)
	if err == nil && len(sizes) != len(handles) {
		err = fmt.Errorf("blob lookup returned %d results for %d handles", len(sizes), len(handles))
	}
	lookup.mutex.Lock()
	defer lookup.mutex.Unlock()
	if err == nil {
		err = context.Cause(lookup.ctx)
	}
	if err != nil {
		lookup.cancel(err)
	}
	batch.err = err
	if err == nil {
		batch.sizes = sizes
	}
	for ordinal, handle := range handles {
		if err == nil {
			lookup.cache.Add(handle, sizes[ordinal])
		}
		delete(lookup.pending, handle)
	}
	close(batch.done)
}

func (lookup *CachedBlobLookup) AddPendingContext(ctx context.Context, handle vaultic.BlobHandle, size uint) (bool, error) {
	sizes, err := lookup.LookupSizesContext(ctx, []vaultic.BlobHandle{handle})
	if err != nil {
		return false, err
	}
	if sizes[0].Found {
		return false, nil
	}
	lookup.mutex.Lock()
	defer lookup.mutex.Unlock()
	if err := context.Cause(lookup.ctx); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return lookup.local.AddPending(handle, size), nil
}

func (lookup *CachedBlobLookup) Close() error {
	lookup.mutex.Lock()
	lookup.cancel(context.Canceled)
	lookup.mutex.Unlock()
	lookup.workers.Wait()
	lookup.cache.Purge()
	return nil
}
