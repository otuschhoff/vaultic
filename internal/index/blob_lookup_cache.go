package index

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
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
	ctx      context.Context
	cancel   context.CancelCauseFunc
	session  blobLookupSession
	local    *LegacyEngine
	cache    *lru.Cache[vaultic.BlobHandle, vaultic.BlobSize]
	slots    chan struct{}
	mutex    sync.Mutex
	pending  map[vaultic.BlobHandle]blobLookupResult
	workers  sync.WaitGroup
	written  *writtenBlobStore
	stats    blobLookupCounters
	capacity int
}

type blobLookupCounters struct {
	hits                   atomic.Uint64
	negativeHits           atomic.Uint64
	misses                 atomic.Uint64
	evictions              atomic.Uint64
	peakEntries            atomic.Uint64
	sizeRPCs               atomic.Uint64
	sizeHandles            atomic.Uint64
	sizeRPCNanoseconds     atomic.Uint64
	locationRPCs           atomic.Uint64
	locationRPCNanoseconds atomic.Uint64
}

type BlobLookupStats struct {
	CacheHits              uint64 `json:"cache_hits"`
	NegativeHits           uint64 `json:"negative_hits"`
	CacheMisses            uint64 `json:"cache_misses"`
	Evictions              uint64 `json:"evictions"`
	PeakEntries            uint64 `json:"peak_entries"`
	Capacity               int    `json:"capacity"`
	AccountedCapacityBytes int    `json:"accounted_capacity_bytes"`
	SizeRPCs               uint64 `json:"size_rpcs"`
	SizeHandles            uint64 `json:"size_handles"`
	SizeRPCNanoseconds     uint64 `json:"size_rpc_nanoseconds"`
	LocationRPCs           uint64 `json:"location_rpcs"`
	LocationRPCNanoseconds uint64 `json:"location_rpc_nanoseconds"`
}

func (lookup *CachedBlobLookup) Stats() BlobLookupStats {
	return BlobLookupStats{
		CacheHits: lookup.stats.hits.Load(), NegativeHits: lookup.stats.negativeHits.Load(), CacheMisses: lookup.stats.misses.Load(),
		Evictions: lookup.stats.evictions.Load(), PeakEntries: lookup.stats.peakEntries.Load(), Capacity: lookup.capacity,
		AccountedCapacityBytes: lookup.capacity * blobLookupCacheEntryBytes,
		SizeRPCs:               lookup.stats.sizeRPCs.Load(), SizeHandles: lookup.stats.sizeHandles.Load(),
		SizeRPCNanoseconds: lookup.stats.sizeRPCNanoseconds.Load(), LocationRPCs: lookup.stats.locationRPCs.Load(),
		LocationRPCNanoseconds: lookup.stats.locationRPCNanoseconds.Load(),
	}
}

func (lookup *CachedBlobLookup) cachedSize(handle vaultic.BlobHandle) (vaultic.BlobSize, bool) {
	size, found := lookup.cache.Get(handle)
	if found {
		lookup.stats.hits.Add(1)
		if !size.Found {
			lookup.stats.negativeHits.Add(1)
		}
	} else {
		lookup.stats.misses.Add(1)
	}
	return size, found
}

func (lookup *CachedBlobLookup) cacheSize(handle vaultic.BlobHandle, size vaultic.BlobSize) {
	if lookup.cache.Add(handle, size) {
		lookup.stats.evictions.Add(1)
	}
	entries := uint64(lookup.cache.Len())
	for peak := lookup.stats.peakEntries.Load(); entries > peak; peak = lookup.stats.peakEntries.Load() {
		if lookup.stats.peakEntries.CompareAndSwap(peak, entries) {
			break
		}
	}
}

func (lookup *CachedBlobLookup) Error() error {
	if err := context.Cause(lookup.ctx); err != nil {
		return err
	}
	if lookup.written != nil {
		if err := lookup.written.Error(); err != nil {
			lookup.cancel(err)
			return err
		}
	}
	return nil
}

func NewSpillingBlobLookup(session blobLookupSession, directory string, budgetBytes, concurrency int) (*CachedBlobLookup, *LegacyEngine, error) {
	written, err := newWrittenBlobStore(directory)
	if err != nil {
		return nil, nil, err
	}
	local := NewLegacyEngine(legacyindex.NewSpillingMasterIndex(written.StoreIndex))
	lookup, err := NewCachedBlobLookup(session, local, budgetBytes, concurrency)
	if err != nil {
		return nil, nil, errors.Join(err, written.Close())
	}
	lookup.written = written
	return lookup, local, nil
}

func (lookup *CachedBlobLookup) localSizes(ctx context.Context, handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
	if lookup.written != nil {
		if err := lookup.written.Error(); err != nil {
			lookup.cancel(err)
			return nil, err
		}
	}
	sizes := lookup.local.master.LookupSizes(handles)
	if lookup.written == nil {
		return sizes, nil
	}
	for ordinal, handle := range handles {
		if sizes[ordinal].Found {
			continue
		}
		if size, cached := lookup.cachedSize(handle); cached && size.Found {
			sizes[ordinal] = size
			continue
		}
		blobs, err := lookup.written.lookup(ctx, handle, true)
		if err != nil {
			if ctx.Err() == nil {
				lookup.cancel(err)
			}
			return nil, err
		}
		if len(blobs) != 0 {
			sizes[ordinal] = vaultic.BlobSize{Size: blobs[0].PlaintextLength(), Found: true}
			lookup.cacheSize(handle, sizes[ordinal])
		}
	}
	return sizes, nil
}

func (lookup *CachedBlobLookup) localLocations(ctx context.Context, handle vaultic.BlobHandle) ([]*pack.PackedBlob, error) {
	if lookup.written != nil {
		if err := lookup.written.Error(); err != nil {
			lookup.cancel(err)
			return nil, err
		}
	}
	blobs := lookup.local.Lookup(handle)
	if len(blobs) != 0 || lookup.written == nil {
		return blobs, nil
	}
	blobs, err := lookup.written.lookup(ctx, handle, false)
	if err != nil && ctx.Err() == nil {
		lookup.cancel(err)
	}
	return blobs, err
}

var _ ContextReadEngine = (*CachedBlobLookup)(nil)
var _ ContextBatchReadEngine = (*CachedBlobLookup)(nil)
var _ ContextWriteEngine = (*CachedBlobLookup)(nil)

func (lookup *CachedBlobLookup) LookupSizeContext(ctx context.Context, handle vaultic.BlobHandle) (uint, bool, error) {
	sizes, err := lookup.LookupSizesContext(ctx, []vaultic.BlobHandle{handle})
	if err != nil {
		return 0, false, err
	}
	return sizes[0].Size, sizes[0].Found, nil
}

func (lookup *CachedBlobLookup) LookupContext(ctx context.Context, handle vaultic.BlobHandle) ([]*pack.PackedBlob, error) {
	if err := vaultic.ValidateBlobLookupBatch([]vaultic.BlobHandle{handle}); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := context.Cause(lookup.ctx); err != nil {
		return nil, err
	}
	local, err := lookup.localLocations(ctx, handle)
	if err != nil {
		return nil, err
	}
	if len(local) != 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := context.Cause(lookup.ctx); err != nil {
			return nil, err
		}
		return local, nil
	}
	select {
	case lookup.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lookup.ctx.Done():
		return nil, context.Cause(lookup.ctx)
	}
	lookup.mutex.Lock()
	if err := context.Cause(lookup.ctx); err != nil {
		lookup.mutex.Unlock()
		<-lookup.slots
		return nil, err
	}
	lookup.workers.Add(1)
	lookup.mutex.Unlock()
	defer func() {
		<-lookup.slots
		lookup.workers.Done()
	}()
	provider, ok := lookup.session.(interface {
		LookupContext(context.Context, vaultic.BlobHandle) ([]*pack.PackedBlob, error)
	})
	if !ok {
		err := fmt.Errorf("blob lookup session does not support location reads")
		lookup.cancel(err)
		return nil, err
	}
	rpcCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(lookup.ctx, func() { cancel(context.Cause(lookup.ctx)) })
	defer stop()
	defer cancel(nil)
	started := time.Now()
	lookup.stats.locationRPCs.Add(1)
	blobs, err := provider.LookupContext(rpcCtx, handle)
	lookup.stats.locationRPCNanoseconds.Add(uint64(time.Since(started)))
	if cause := context.Cause(lookup.ctx); cause != nil {
		return nil, cause
	}
	if cause := ctx.Err(); cause != nil {
		return nil, cause
	}
	if err != nil {
		lookup.cancel(err)
		return nil, err
	}
	current, err := lookup.localLocations(ctx, handle)
	if err != nil {
		return nil, err
	}
	if len(current) != 0 {
		blobs = current
	}
	return blobs, nil
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
		slots: make(chan struct{}, concurrency), pending: make(map[vaultic.BlobHandle]blobLookupResult),
		capacity: budgetBytes / blobLookupCacheEntryBytes}, nil
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
	results, err := lookup.localSizes(ctx, handles)
	if err != nil {
		lookup.mutex.Unlock()
		return nil, err
	}
	cached := true
	for ordinal, handle := range handles {
		if results[ordinal].Found {
			continue
		}
		size, found := lookup.cachedSize(handle)
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
	results, err = lookup.localSizes(ctx, handles)
	if err != nil {
		<-lookup.slots
		return nil, err
	}
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
		if size, found := lookup.cachedSize(handle); found {
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
	currentSizes, err := lookup.localSizes(ctx, handles)
	if err != nil {
		return nil, err
	}
	for ordinal, current := range currentSizes {
		if current.Found {
			results[ordinal] = current
		}
	}
	return results, nil
}

func (lookup *CachedBlobLookup) fetch(handles []vaultic.BlobHandle, batch *blobLookupBatch) {
	defer lookup.workers.Done()
	defer func() { <-lookup.slots }()
	started := time.Now()
	lookup.stats.sizeRPCs.Add(1)
	lookup.stats.sizeHandles.Add(uint64(len(handles)))
	sizes, err := lookup.session.LookupBlobSizesContext(lookup.ctx, handles)
	lookup.stats.sizeRPCNanoseconds.Add(uint64(time.Since(started)))
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
			lookup.cacheSize(handle, sizes[ordinal])
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
	current, err := lookup.localSizes(ctx, []vaultic.BlobHandle{handle})
	if err != nil {
		return false, err
	}
	if current[0].Found {
		return false, nil
	}
	return lookup.local.AddPending(handle, size), nil
}

func (lookup *CachedBlobLookup) Close() error {
	lookup.mutex.Lock()
	lookup.cancel(context.Canceled)
	lookup.mutex.Unlock()
	lookup.workers.Wait()
	lookup.cache.Purge()
	if lookup.written != nil {
		return lookup.written.Close()
	}
	return nil
}
