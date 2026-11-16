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
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/workingkv"
	"github.com/otuschhoff/vaultic/internal/workingstate"
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
	done    chan struct{}
	sizes   []vaultic.BlobSize
	err     error
	release func()
}

type CachedBlobLookup struct {
	catalog                *writtenBlobStore
	catalogEntries         uint64
	catalogLoadNanoseconds uint64
	coalesceDelay          time.Duration
	collecting             *coalescedBlobLookup
	ctx                    context.Context
	cancel                 context.CancelCauseFunc
	session                blobLookupSession
	local                  *LegacyEngine
	cache                  *lru.Cache[vaultic.BlobHandle, vaultic.BlobSize]
	cacheMutex             sync.RWMutex
	cacheCharge            uint64
	slots                  chan struct{}
	mutex                  sync.Mutex
	pending                map[vaultic.BlobHandle]blobLookupResult
	workers                sync.WaitGroup
	written                *writtenBlobStore
	stats                  blobLookupCounters
	capacity               int
	policy                 *workingkv.Policy
	charge                 uint64
	closeOnce              sync.Once
	closeErr               error
	unregisterEvictor      func()
}

type coalescedBlobLookup struct {
	users   int
	handles []vaultic.BlobHandle
	sizes   []vaultic.BlobSize
	err     error
	flush   chan struct{}
	done    chan struct{}
	release func()
}

type blobLookupCounters struct {
	hits                        atomic.Uint64
	negativeHits                atomic.Uint64
	misses                      atomic.Uint64
	evictions                   atomic.Uint64
	peakEntries                 atomic.Uint64
	sizeRPCs                    atomic.Uint64
	sizeHandles                 atomic.Uint64
	sizeRPCNanoseconds          atomic.Uint64
	sizeRPCActive               atomic.Uint64
	sizeRPCPeakActive           atomic.Uint64
	sizeRPCMaxNanoseconds       atomic.Uint64
	sizeRPCDurationBuckets      [8]atomic.Uint64
	lookupCalls                 atomic.Uint64
	lookupNanoseconds           atomic.Uint64
	slotWaitNanoseconds         atomic.Uint64
	resultWaitNanoseconds       atomic.Uint64
	locationRPCs                atomic.Uint64
	locationRPCNanoseconds      atomic.Uint64
	locationSlotWaitNanoseconds atomic.Uint64
	coalesceGroups              atomic.Uint64
	coalesceFollowers           atomic.Uint64
	coalesceCollectNanoseconds  atomic.Uint64
	coalesceFollowerNanoseconds atomic.Uint64
}

type BlobLookupStats struct {
	CatalogEntries                        uint64                          `json:"catalog_entries"`
	CatalogLoadNanoseconds                uint64                          `json:"catalog_load_nanoseconds"`
	ExactCatalog                          bool                            `json:"exact_catalog"`
	CatalogWorkingState                   *telemetry.WorkingStateSnapshot `json:"catalog_working_state,omitempty"`
	WorkingState                          *telemetry.WorkingStateSnapshot `json:"working_state,omitempty"`
	CacheHits                             uint64                          `json:"cache_hits"`
	NegativeHits                          uint64                          `json:"negative_hits"`
	CacheMisses                           uint64                          `json:"cache_misses"`
	Evictions                             uint64                          `json:"evictions"`
	PeakEntries                           uint64                          `json:"peak_entries"`
	Capacity                              int                             `json:"capacity"`
	AccountedCapacityBytes                int                             `json:"accounted_capacity_bytes"`
	SizeRPCs                              uint64                          `json:"size_rpcs"`
	SizeHandles                           uint64                          `json:"size_handles"`
	SizeRPCNanoseconds                    uint64                          `json:"size_rpc_nanoseconds"`
	SizeRPCActive                         uint64                          `json:"size_rpc_active"`
	SizeRPCPeakActive                     uint64                          `json:"size_rpc_peak_active"`
	SizeRPCMaxNanoseconds                 uint64                          `json:"size_rpc_max_nanoseconds"`
	SizeRPCDurationBuckets                [8]uint64                       `json:"size_rpc_duration_buckets"`
	SizeRPCDurationBucketUpperNanoseconds [8]uint64                       `json:"size_rpc_duration_bucket_upper_nanoseconds"`
	LookupCalls                           uint64                          `json:"lookup_calls"`
	LookupNanoseconds                     uint64                          `json:"lookup_nanoseconds"`
	SlotWaitNanoseconds                   uint64                          `json:"slot_wait_nanoseconds"`
	ResultWaitNanoseconds                 uint64                          `json:"result_wait_nanoseconds"`
	RPCConcurrencyLimit                   int                             `json:"rpc_concurrency_limit"`
	LocationRPCs                          uint64                          `json:"location_rpcs"`
	LocationRPCNanoseconds                uint64                          `json:"location_rpc_nanoseconds"`
	LocationSlotWaitNanoseconds           uint64                          `json:"location_slot_wait_nanoseconds"`
	CoalesceDelayNanoseconds              uint64                          `json:"coalesce_delay_nanoseconds"`
	CoalesceGroups                        uint64                          `json:"coalesce_groups"`
	CoalesceFollowers                     uint64                          `json:"coalesce_followers"`
	CoalesceCollectNanoseconds            uint64                          `json:"coalesce_collect_nanoseconds"`
	CoalesceFollowerNanoseconds           uint64                          `json:"coalesce_follower_nanoseconds"`
}

func (stats BlobLookupStats) Delta(baseline BlobLookupStats) BlobLookupStats {
	stats.CacheHits -= baseline.CacheHits
	stats.NegativeHits -= baseline.NegativeHits
	stats.CacheMisses -= baseline.CacheMisses
	stats.Evictions -= baseline.Evictions
	stats.SizeRPCs -= baseline.SizeRPCs
	stats.SizeHandles -= baseline.SizeHandles
	stats.SizeRPCNanoseconds -= baseline.SizeRPCNanoseconds
	stats.LookupCalls -= baseline.LookupCalls
	stats.LookupNanoseconds -= baseline.LookupNanoseconds
	stats.SlotWaitNanoseconds -= baseline.SlotWaitNanoseconds
	stats.ResultWaitNanoseconds -= baseline.ResultWaitNanoseconds
	stats.LocationRPCs -= baseline.LocationRPCs
	stats.LocationRPCNanoseconds -= baseline.LocationRPCNanoseconds
	stats.LocationSlotWaitNanoseconds -= baseline.LocationSlotWaitNanoseconds
	stats.CoalesceGroups -= baseline.CoalesceGroups
	stats.CoalesceFollowers -= baseline.CoalesceFollowers
	stats.CoalesceCollectNanoseconds -= baseline.CoalesceCollectNanoseconds
	stats.CoalesceFollowerNanoseconds -= baseline.CoalesceFollowerNanoseconds
	for ordinal := range stats.SizeRPCDurationBuckets {
		stats.SizeRPCDurationBuckets[ordinal] -= baseline.SizeRPCDurationBuckets[ordinal]
	}
	return stats
}

func (lookup *CachedBlobLookup) Stats() BlobLookupStats {
	stats := BlobLookupStats{
		CatalogEntries: lookup.catalogEntries, CatalogLoadNanoseconds: lookup.catalogLoadNanoseconds, ExactCatalog: lookup.catalog != nil,
		CacheHits: lookup.stats.hits.Load(), NegativeHits: lookup.stats.negativeHits.Load(), CacheMisses: lookup.stats.misses.Load(),
		Evictions: lookup.stats.evictions.Load(), PeakEntries: lookup.stats.peakEntries.Load(), Capacity: lookup.capacity,
		AccountedCapacityBytes: lookup.capacity * blobLookupCacheEntryBytes,
		SizeRPCs:               lookup.stats.sizeRPCs.Load(), SizeHandles: lookup.stats.sizeHandles.Load(),
		SizeRPCNanoseconds: lookup.stats.sizeRPCNanoseconds.Load(), LocationRPCs: lookup.stats.locationRPCs.Load(),
		LocationRPCNanoseconds:      lookup.stats.locationRPCNanoseconds.Load(),
		LocationSlotWaitNanoseconds: lookup.stats.locationSlotWaitNanoseconds.Load(),
		CoalesceDelayNanoseconds:    uint64(lookup.coalesceDelay),
		CoalesceGroups:              lookup.stats.coalesceGroups.Load(),
		CoalesceFollowers:           lookup.stats.coalesceFollowers.Load(),
		CoalesceCollectNanoseconds:  lookup.stats.coalesceCollectNanoseconds.Load(),
		CoalesceFollowerNanoseconds: lookup.stats.coalesceFollowerNanoseconds.Load(),
		SizeRPCActive:               lookup.stats.sizeRPCActive.Load(), SizeRPCPeakActive: lookup.stats.sizeRPCPeakActive.Load(),
		SizeRPCMaxNanoseconds: lookup.stats.sizeRPCMaxNanoseconds.Load(),
		LookupCalls:           lookup.stats.lookupCalls.Load(), LookupNanoseconds: lookup.stats.lookupNanoseconds.Load(),
		SlotWaitNanoseconds: lookup.stats.slotWaitNanoseconds.Load(), ResultWaitNanoseconds: lookup.stats.resultWaitNanoseconds.Load(),
		RPCConcurrencyLimit:                   cap(lookup.slots),
		SizeRPCDurationBucketUpperNanoseconds: [8]uint64{1000000, 5000000, 10000000, 25000000, 50000000, 100000000, 500000000, ^uint64(0)},
	}
	for ordinal := range stats.SizeRPCDurationBuckets {
		stats.SizeRPCDurationBuckets[ordinal] = lookup.stats.sizeRPCDurationBuckets[ordinal].Load()
	}
	if lookup.written != nil {
		state := lookup.written.WorkingState()
		stats.WorkingState = &state
	}
	if lookup.catalog != nil {
		state := lookup.catalog.WorkingState()
		stats.CatalogWorkingState = &state
	}
	return stats
}

func (lookup *CachedBlobLookup) loadExactCatalog(ctx context.Context, directory string) error {
	source, ok := lookup.session.(interface {
		ScanBlobSizes(context.Context, func([]vaultic.BlobHandle, []vaultic.BlobSize) error) error
	})
	if !ok {
		return fmt.Errorf("read session does not support an exact blob catalog")
	}
	started := time.Now()
	catalog, err := newWrittenBlobStoreContext(ctx, directory)
	if err != nil {
		return err
	}
	var entries uint64
	err = source.ScanBlobSizes(ctx, func(handles []vaultic.BlobHandle, sizes []vaultic.BlobSize) error {
		if err := catalog.storeSizes(ctx, handles, sizes); err != nil {
			return err
		}
		entries += uint64(len(handles))
		return nil
	})
	if err == nil {
		err = context.Cause(lookup.ctx)
	}
	if err != nil {
		return errors.Join(err, catalog.Close())
	}
	lookup.catalog, lookup.catalogEntries = catalog, entries
	lookup.catalogLoadNanoseconds = uint64(time.Since(started))
	return nil
}

func (lookup *CachedBlobLookup) cachedSize(handle vaultic.BlobHandle) (vaultic.BlobSize, bool) {
	lookup.cacheMutex.RLock()
	defer lookup.cacheMutex.RUnlock()
	if lookup.cache == nil {
		lookup.stats.misses.Add(1)
		return vaultic.BlobSize{}, false
	}
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
	lookup.cacheMutex.RLock()
	defer lookup.cacheMutex.RUnlock()
	if lookup.cache == nil {
		return
	}
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
	if session == nil {
		return nil, nil, workingkv.ErrInvalid
	}
	return NewSpillingBlobLookupContext(session.Context(), session, directory, budgetBytes, concurrency)
}

func NewSpillingBlobLookupContext(ctx context.Context, session blobLookupSession, directory string, budgetBytes, concurrency int, coalesceDelay ...time.Duration) (*CachedBlobLookup, *LegacyEngine, error) {
	written, err := newWrittenBlobStoreContext(ctx, directory)
	if err != nil {
		return nil, nil, err
	}
	local := NewLegacyEngine(legacyindex.NewSpillingMasterIndex(written.StoreIndex))
	lookup, err := NewCachedBlobLookupContext(ctx, session, local, budgetBytes, concurrency, coalesceDelay...)
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
	slotStarted := time.Now()
	select {
	case lookup.slots <- struct{}{}:
	case <-ctx.Done():
		lookup.stats.locationSlotWaitNanoseconds.Add(uint64(time.Since(slotStarted)))
		return nil, ctx.Err()
	case <-lookup.ctx.Done():
		lookup.stats.locationSlotWaitNanoseconds.Add(uint64(time.Since(slotStarted)))
		return nil, context.Cause(lookup.ctx)
	}
	lookup.stats.locationSlotWaitNanoseconds.Add(uint64(time.Since(slotStarted)))
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
	if session == nil {
		return nil, workingkv.ErrInvalid
	}
	return NewCachedBlobLookupContext(session.Context(), session, local, budgetBytes, concurrency)
}

func NewCachedBlobLookupContext(parent context.Context, session blobLookupSession, local *LegacyEngine, budgetBytes, concurrency int, coalesceDelay ...time.Duration) (*CachedBlobLookup, error) {
	var delay time.Duration
	if len(coalesceDelay) > 1 {
		return nil, fmt.Errorf("at most one coalescing delay is allowed")
	}
	if len(coalesceDelay) == 1 {
		delay = coalesceDelay[0]
	}
	if delay < 0 || delay > 5*time.Millisecond {
		return nil, fmt.Errorf("blob coalescing delay must be between zero and 5ms")
	}
	if session == nil || local == nil || budgetBytes < blobLookupCacheEntryBytes || concurrency < 1 {
		return nil, fmt.Errorf("blob lookup cache requires a session, write index, positive budget and concurrency")
	}
	if err := context.Cause(session.Context()); err != nil {
		return nil, err
	}
	policy := workingkv.PolicyFrom(parent)
	var charge uint64
	if policy != nil {
		budgetBytes = int(min(uint64(budgetBytes), policy.Budget().Limit/8))
		if budgetBytes < blobLookupCacheEntryBytes {
			state := policy.Budget()
			return nil, &workingkv.MemoryLimitError{Limit: state.Limit, Used: state.Used, Requested: blobLookupCacheEntryBytes}
		}
		charge = uint64(budgetBytes/blobLookupCacheEntryBytes)*blobLookupCacheEntryBytes + 512 + uint64(concurrency)*128
		if err := policy.Reserve(charge); err != nil {
			return nil, err
		}
	}
	cache, err := lru.New[vaultic.BlobHandle, vaultic.BlobSize](budgetBytes / blobLookupCacheEntryBytes)
	if err != nil {
		if policy != nil {
			policy.Release(charge)
		}
		return nil, err
	}
	owner := session.Context()
	if policy != nil {
		owner = workingkv.WithPolicy(owner, policy)
	}
	ctx, cancel := context.WithCancelCause(owner)
	lookup := &CachedBlobLookup{ctx: ctx, cancel: cancel, session: session, local: local, cache: cache,
		coalesceDelay: delay,
		slots:         make(chan struct{}, concurrency), pending: make(map[vaultic.BlobHandle]blobLookupResult),
		capacity: budgetBytes / blobLookupCacheEntryBytes, policy: policy, charge: charge}
	if policy != nil {
		lookup.cacheCharge = uint64(lookup.capacity) * blobLookupCacheEntryBytes
		lookup.unregisterEvictor = policy.RegisterEvictor(lookup.evictWorkingCache)
	}
	return lookup, nil
}

func (lookup *CachedBlobLookup) evictWorkingCache() {
	lookup.cacheMutex.Lock()
	defer lookup.cacheMutex.Unlock()
	lookup.cache = nil
	if lookup.policy != nil && lookup.cacheCharge != 0 {
		lookup.policy.Release(lookup.cacheCharge)
		lookup.charge -= lookup.cacheCharge
		lookup.cacheCharge = 0
	}
}

func (lookup *CachedBlobLookup) LookupSizesContext(ctx context.Context, handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
	if err := vaultic.ValidateBlobLookupBatch(handles); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	started := time.Now()
	lookup.stats.lookupCalls.Add(1)
	defer func() { lookup.stats.lookupNanoseconds.Add(uint64(time.Since(started))) }()
	release, err := workingstate.Reserve(lookup.ctx, uint64(len(handles))*512+1024)
	if err != nil {
		return nil, err
	}
	defer release()
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
	if lookup.catalog != nil {
		defer lookup.mutex.Unlock()
		for ordinal, handle := range handles {
			if results[ordinal].Found {
				continue
			}
			results[ordinal], err = lookup.catalog.lookupSize(ctx, handle)
			if err != nil {
				if ctx.Err() == nil {
					lookup.cancel(err)
				}
				return nil, err
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := context.Cause(lookup.ctx); err != nil {
			return nil, err
		}
		return results, nil
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
	slotStarted := time.Now()
	select {
	case lookup.slots <- struct{}{}:
	case <-ctx.Done():
		lookup.stats.slotWaitNanoseconds.Add(uint64(time.Since(slotStarted)))
		return nil, ctx.Err()
	case <-lookup.ctx.Done():
		lookup.stats.slotWaitNanoseconds.Add(uint64(time.Since(slotStarted)))
		return nil, context.Cause(lookup.ctx)
	}
	lookup.stats.slotWaitNanoseconds.Add(uint64(time.Since(slotStarted)))
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
	missingRelease, reserveErr := workingstate.Reserve(lookup.ctx, uint64(len(handles))*512+1024)
	if reserveErr != nil {
		lookup.mutex.Unlock()
		<-lookup.slots
		return nil, reserveErr
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
				batch = &blobLookupBatch{done: make(chan struct{}), release: missingRelease}
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
		missingRelease()
		<-lookup.slots
	}
	lookup.mutex.Unlock()
	waitStarted := time.Now()
	waitFinished := false
	defer func() {
		if !waitFinished {
			lookup.stats.resultWaitNanoseconds.Add(uint64(time.Since(waitStarted)))
		}
	}()
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
	lookup.stats.resultWaitNanoseconds.Add(uint64(time.Since(waitStarted)))
	waitFinished = true
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
	if batch.release != nil {
		defer batch.release()
	}
	slotOwned := true
	defer func() {
		if slotOwned {
			<-lookup.slots
		}
	}()
	var sizes []vaultic.BlobSize
	var err error
	if lookup.coalesceDelay > 0 {
		sizes, err = lookup.coalescedSizes(handles, &slotOwned)
	} else {
		sizes, err = lookup.querySizes(handles)
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

func (lookup *CachedBlobLookup) coalescedSizes(handles []vaultic.BlobHandle, slotOwned *bool) ([]vaultic.BlobSize, error) {
	lookup.mutex.Lock()
	group := lookup.collecting
	if group != nil && len(group.handles)+len(handles) > vaultic.BlobLookupBatchSize {
		close(group.flush)
		lookup.collecting = nil
		group = nil
	}
	leader := group == nil
	if leader {
		release, err := workingstate.Reserve(lookup.ctx, uint64(vaultic.BlobLookupBatchSize)*128+4096)
		if err != nil {
			lookup.mutex.Unlock()
			return nil, err
		}
		group = &coalescedBlobLookup{handles: make([]vaultic.BlobHandle, 0, vaultic.BlobLookupBatchSize), flush: make(chan struct{}), done: make(chan struct{}), release: release}
		lookup.collecting = group
	}
	start := len(group.handles)
	group.users++
	group.handles = append(group.handles, handles...)
	if len(group.handles) == vaultic.BlobLookupBatchSize {
		close(group.flush)
		lookup.collecting = nil
	}
	lookup.mutex.Unlock()
	if !leader {
		<-lookup.slots
		*slotOwned = false
		lookup.stats.coalesceFollowers.Add(1)
	}
	defer func() {
		lookup.mutex.Lock()
		group.users--
		if group.users == 0 {
			group.release()
		}
		lookup.mutex.Unlock()
	}()
	if leader {
		lookup.stats.coalesceGroups.Add(1)
		collectStarted := time.Now()
		timer := time.NewTimer(lookup.coalesceDelay)
		select {
		case <-timer.C:
		case <-group.flush:
		case <-lookup.ctx.Done():
		}
		timer.Stop()
		lookup.mutex.Lock()
		if lookup.collecting == group {
			lookup.collecting = nil
		}
		lookup.mutex.Unlock()
		lookup.stats.coalesceCollectNanoseconds.Add(uint64(time.Since(collectStarted)))
		group.sizes, group.err = lookup.querySizes(group.handles)
		close(group.done)
	} else {
		waitStarted := time.Now()
		<-group.done
		lookup.stats.coalesceFollowerNanoseconds.Add(uint64(time.Since(waitStarted)))
	}
	if group.err != nil {
		return nil, group.err
	}
	return append([]vaultic.BlobSize(nil), group.sizes[start:start+len(handles)]...), nil
}

func (lookup *CachedBlobLookup) querySizes(handles []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
	started := time.Now()
	lookup.stats.sizeRPCs.Add(1)
	lookup.stats.sizeHandles.Add(uint64(len(handles)))
	active := lookup.stats.sizeRPCActive.Add(1)
	for peak := lookup.stats.sizeRPCPeakActive.Load(); active > peak; peak = lookup.stats.sizeRPCPeakActive.Load() {
		if lookup.stats.sizeRPCPeakActive.CompareAndSwap(peak, active) {
			break
		}
	}
	sizes, err := lookup.session.LookupBlobSizesContext(lookup.ctx, handles)
	elapsed := time.Since(started)
	lookup.stats.sizeRPCActive.Add(^uint64(0))
	lookup.stats.sizeRPCNanoseconds.Add(uint64(elapsed))
	for maximum := lookup.stats.sizeRPCMaxNanoseconds.Load(); uint64(elapsed) > maximum; maximum = lookup.stats.sizeRPCMaxNanoseconds.Load() {
		if lookup.stats.sizeRPCMaxNanoseconds.CompareAndSwap(maximum, uint64(elapsed)) {
			break
		}
	}
	bounds := [...]time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond}
	bucket := len(bounds)
	for ordinal, bound := range bounds {
		if elapsed <= bound {
			bucket = ordinal
			break
		}
	}
	lookup.stats.sizeRPCDurationBuckets[bucket].Add(1)
	if err != nil {
		err = fmt.Errorf("blob size lookup RPC for %d handles after %s: %w", len(handles), elapsed, err)
	}
	if err == nil && len(sizes) != len(handles) {
		err = fmt.Errorf("blob lookup returned %d results for %d handles", len(sizes), len(handles))
	}
	return sizes, err
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
	lookup.closeOnce.Do(func() {
		lookup.mutex.Lock()
		lookup.cancel(context.Canceled)
		lookup.mutex.Unlock()
		lookup.workers.Wait()
		if lookup.unregisterEvictor != nil {
			lookup.unregisterEvictor()
		}
		lookup.cacheMutex.Lock()
		if lookup.cache != nil {
			lookup.cache.Purge()
		}
		if lookup.written != nil {
			lookup.closeErr = errors.Join(lookup.written.Close(), lookup.local.Close())
		}
		if lookup.catalog != nil {
			lookup.closeErr = errors.Join(lookup.closeErr, lookup.catalog.Close())
		}
		if lookup.policy != nil {
			lookup.policy.Release(lookup.charge)
			lookup.charge = 0
			lookup.cacheCharge = 0
		}
		lookup.cacheMutex.Unlock()
	})
	return lookup.closeErr
}
