package index

import (
	"context"
	"fmt"
	"iter"
	"runtime"
	"sync"

	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/sync/errgroup"
)

// MasterIndex is a collection of indexes and IDs of chunks that are in the process of being saved.
type MasterIndex struct {
	idx          []*Index
	pendingBlobs map[vaultic.BlobHandle]uint
	idxMutex     sync.RWMutex
}

// NewMasterIndex creates a new master index.
func NewMasterIndex() *MasterIndex {
	mi := &MasterIndex{}
	mi.clear()
	return mi
}

func (mi *MasterIndex) clear() {
	// Always add an empty final index, such that MergeFinalIndexes can merge into this.
	mi.idx = []*Index{NewIndex()}
	mi.idx[0].Finalize()
	mi.clearPendingBlobs()
}

func (mi *MasterIndex) clearPendingBlobs() {
	mi.pendingBlobs = make(map[vaultic.BlobHandle]uint)
}

// Lookup queries all known Indexes for the ID and returns all matches.
func (mi *MasterIndex) Lookup(bh vaultic.BlobHandle) []*pack.PackedBlob {
	mi.idxMutex.RLock()
	defer mi.idxMutex.RUnlock()

	var pbs []*pack.PackedBlob
	for _, idx := range mi.idx {
		pbs = idx.Lookup(bh, pbs)
	}

	return pbs
}

// LookupSize queries all known Indexes for the ID and returns the first match.
// Also returns true if the ID is pending.
func (mi *MasterIndex) LookupSize(bh vaultic.BlobHandle) (uint, bool) {
	mi.idxMutex.RLock()
	defer mi.idxMutex.RUnlock()

	// also return true if blob is pending
	if size, ok := mi.pendingBlobs[bh]; ok {
		return size, true
	}

	for _, idx := range mi.idx {
		if size, found := idx.LookupSize(bh); found {
			return size, found
		}
	}

	return 0, false
}

// AddPending adds a given blob to list of pending Blobs
// Before doing so it checks if this blob is already known.
// Returns true if adding was successful and false if the blob
// was already known
func (mi *MasterIndex) AddPending(bh vaultic.BlobHandle, size uint) bool {

	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	// Check if blob is pending or in index
	if _, ok := mi.pendingBlobs[bh]; ok {
		return false
	}

	for _, idx := range mi.idx {
		if idx.Has(bh) {
			return false
		}
	}

	// really not known -> insert
	mi.pendingBlobs[bh] = size
	return true
}

// IDs returns the IDs of all indexes contained in the index.
func (mi *MasterIndex) IDs() vaultic.IDSet {
	mi.idxMutex.RLock()
	defer mi.idxMutex.RUnlock()

	ids := vaultic.NewIDSet()
	for _, idx := range mi.idx {
		if !idx.Final() {
			continue
		}
		indexIDs, err := idx.IDs()
		if err != nil {
			debug.Log("not using index, ID() returned error %v", err)
			continue
		}
		for _, id := range indexIDs {
			ids.Insert(id)
		}
	}
	return ids
}

// Packs returns all packs that are covered by the index.
// If packBlacklist is given, those packs are only contained in the
// resulting IDSet if they are contained in a non-final (newly written) index.
func (mi *MasterIndex) Packs(packBlacklist vaultic.IDSet) vaultic.IDSet {
	mi.idxMutex.RLock()
	defer mi.idxMutex.RUnlock()

	packs := vaultic.NewIDSet()
	for _, idx := range mi.idx {
		idxPacks := idx.Packs()
		if idx.final && len(packBlacklist) > 0 {
			idxPacks = idxPacks.Sub(packBlacklist)
		}
		packs.Merge(idxPacks)
	}

	return packs
}

// Insert adds a new index to the MasterIndex.
func (mi *MasterIndex) Insert(idx *Index) {
	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	mi.idx = append(mi.idx, idx)
}

// StorePack remembers the id and pack in the index.
func (mi *MasterIndex) StorePack(ctx context.Context, id vaultic.ID, blobs pack.Blobs, r vaultic.SaverUnpacked[vaultic.FileType]) error {
	mi.storePack(id, blobs)
	return mi.saveFullIndex(ctx, r)
}

func (mi *MasterIndex) storePack(id vaultic.ID, blobs pack.Blobs) {
	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	// delete blobs from pending
	for _, blob := range blobs {
		delete(mi.pendingBlobs, vaultic.BlobHandle{Type: blob.Type, ID: blob.ID})
	}

	for _, idx := range mi.idx {
		if !idx.Final() {
			idx.StorePack(id, blobs)
			return
		}
	}

	newIdx := NewIndex()
	newIdx.StorePack(id, blobs)
	mi.idx = append(mi.idx, newIdx)
}

// finalizeNotFinalIndexes finalizes all indexes that
// have not yet been saved and returns that list
func (mi *MasterIndex) finalizeNotFinalIndexes() []*Index {
	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	var list []*Index

	for _, idx := range mi.idx {
		if !idx.Final() {
			idx.Finalize()
			list = append(list, idx)
		}
	}

	debug.Log("return %d indexes", len(list))
	return list
}

// finalizeFullIndexes finalizes all indexes that are full and returns that list.
func (mi *MasterIndex) finalizeFullIndexes() []*Index {
	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	var list []*Index

	debug.Log("checking %d indexes", len(mi.idx))
	for _, idx := range mi.idx {
		if idx.Final() {
			continue
		}

		if Full(idx) {
			debug.Log("index %p is full", idx)
			idx.Finalize()
			list = append(list, idx)
		} else {
			debug.Log("index %p not full", idx)
		}
	}

	debug.Log("return %d indexes", len(list))
	return list
}

// Values returns an iterator over all blobs known to the index. This blocks any
// modification of the index.
func (mi *MasterIndex) Values() iter.Seq[*pack.PackedBlob] {
	return func(yield func(*pack.PackedBlob) bool) {
		mi.idxMutex.RLock()
		defer mi.idxMutex.RUnlock()

		for _, idx := range mi.idx {
			for pb := range idx.Values() {
				if !yield(pb) {
					return
				}
			}
		}
	}
}

// MergeFinalIndexes merges all final indexes together.
// After calling, there will be only one big final index in MasterIndex
// containing all final index contents.
// Indexes that are not final are left untouched.
// This merging can only be called after all index files are loaded - as
// removing of superseded index contents is only possible for unmerged indexes.
func (mi *MasterIndex) MergeFinalIndexes() error {
	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	if len(mi.idx) == 0 {
		return nil
	}

	// preallocate space for all blob types
	for typ := range vaultic.NumBlobTypes {
		size := 0
		for _, idx := range mi.idx {
			size += int(idx.Len(typ))
		}

		mi.idx[0].Preallocate(typ, size)
	}

	// The first index is always final and the one to merge into
	newIdx := mi.idx[:1]
	for i := 1; i < len(mi.idx); i++ {
		idx := mi.idx[i]
		// clear reference in masterindex as it may become stale
		mi.idx[i] = nil
		// do not merge indexes that have no id set
		ids, _ := idx.IDs()
		if !idx.Final() || len(ids) == 0 {
			newIdx = append(newIdx, idx)
		} else {
			err := mi.idx[0].merge(idx)
			if err != nil {
				return fmt.Errorf("MergeFinalIndexes: %w", err)
			}
		}
	}
	mi.idx = newIdx

	return nil
}

func (mi *MasterIndex) Load(ctx context.Context, r vaultic.ListerLoaderUnpacked, p vaultic.Counter, cb func(id vaultic.ID, idx *Index, err error) error) error {
	defer p.Done()
	indexList, err := vaultic.MemorizeList(ctx, r, vaultic.IndexFile)
	if err != nil {
		return err
	}
	loadedIDs, err := mi.prepareIncrementalLoad(ctx, indexList)
	if err != nil {
		return err
	}
	var numIndexFiles uint64
	err = indexList.List(ctx, vaultic.IndexFile, func(id vaultic.ID, _ int64) error {
		if loadedIDs.Has(id) {
			// skip already loaded indexes
			return nil
		}
		numIndexFiles++
		return nil
	})
	if err != nil {
		return err
	}
	p.SetMax(numIndexFiles)

	err = ForAllIndexes(ctx, indexList, r, func(id vaultic.ID, idx *Index, err error) error {
		if loadedIDs.Has(id) {
			// skip already loaded indexes
			return nil
		}
		p.Add(1)
		if cb != nil {
			err = cb(id, idx, err)
		}
		if err != nil {
			return err
		}
		// special case to allow check to ignore index loading errors
		if idx == nil {
			return nil
		}
		mi.Insert(idx)
		return nil
	})

	if err != nil {
		return err
	}

	return mi.MergeFinalIndexes()
}

func (mi *MasterIndex) prepareIncrementalLoad(ctx context.Context, indexList vaultic.Lister) (vaultic.IDSet, error) {
	mi.idxMutex.Lock()
	// support incremental loading, while also ensuring that the result is identical to the result of a full load into a new MasterIndex
	mi.clearPendingBlobs()
	defer mi.idxMutex.Unlock()

	// the first index is always final so this can't actually fail
	loadedIDList, err := mi.idx[0].IDs()
	if err != nil {
		//nolint:forbidigo // This existing panic enforces an internal invariant; new panic paths remain forbidden.
		panic("internal error - failed to get index IDs")
	}
	loadedIDs := vaultic.NewIDSet(loadedIDList...)

	indexFiles := vaultic.NewIDSet()
	err = indexList.List(ctx, vaultic.IndexFile, func(id vaultic.ID, _ int64) error {
		indexFiles.Insert(id)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(loadedIDs.Sub(indexFiles)) > 0 {
		// indexes can only be removed by prune, which shouldn't happen concurrently, but behave correctly anyways
		mi.clear()
		loadedIDs = nil
	}

	return loadedIDs, nil
}

type MasterIndexRewriteOpts struct {
	SaveProgress   vaultic.Counter
	DeleteProgress func() vaultic.Counter
	DeleteReport   func(id vaultic.ID, err error)
	// SavedIndexFunc receives each newly saved replacement index ID. It is used
	// by prune to persist the exact indexes required before old ones may be
	// deleted, without issuing another eventually-consistent backend List.
	SavedIndexFunc func(id vaultic.ID)

	// SkipObsoleteDelete, when set, does NOT delete the superseded index files
	// at the end of the rewrite. Instead the obsolete index IDs are reported via
	// ObsoleteIndexFunc. The caller is responsible for deleting them (e.g.
	// prune's --early-delete-index deletes them right after the new index is in
	// place, but before the now-unreferenced packs are removed, to free space
	// earlier). When SkipObsoleteDelete is false (the default), the superseded
	// index files are deleted at the end of Rewrite as before.
	SkipObsoleteDelete bool
	// ObsoleteIndexFunc is called (once) with the set of obsolete index IDs when
	// SkipObsoleteDelete is set.
	ObsoleteIndexFunc func(obsolete vaultic.IDSet)
}

type rewriteTask struct {
	idx *Index
}

type rewritePipeline struct {
	master       *MasterIndex
	repo         vaultic.Unpacked[vaultic.FileType]
	excludePacks vaultic.IDSet
	progress     vaultic.Counter
	opts         MasterIndexRewriteOpts
	group        *errgroup.Group
	ctx          context.Context
	rewriteCh    chan rewriteTask
	saveCh       chan *Index
	obsolete     vaultic.IDSet
}

func rewriteIndexIDs(mi *MasterIndex, oldIndexes vaultic.IDSet) vaultic.IDSet {
	if oldIndexes != nil {
		return oldIndexes
	}
	return mi.IDs()
}

func cloneRewriteExcludes(excludePacks vaultic.IDSet) vaultic.IDSet {
	excludePacks = excludePacks.Clone()
	if excludePacks == nil {
		return vaultic.NewIDSet()
	}
	return excludePacks
}

// Rewrite removes packs whose ID is in excludePacks from all known indexes.
// It also removes the rewritten index files and those listed in extraObsolete.
// If oldIndexes is not nil, then only the indexes in this set are processed.
// This is used by repair index to only rewrite and delete the old indexes.
//
// Must not be called concurrently to any other MasterIndex operation.
func (mi *MasterIndex) Rewrite(
	ctx context.Context,
	repo vaultic.Unpacked[vaultic.FileType],
	excludePacks vaultic.IDSet,
	oldIndexes vaultic.IDSet,
	extraObsolete vaultic.IDs,
	opts MasterIndexRewriteOpts,
) error {
	for _, idx := range mi.idx {
		if !idx.Final() {
			//nolint:forbidigo // This existing panic enforces an internal invariant; new panic paths remain forbidden.
			panic("internal error - index must be saved before calling MasterIndex.Rewrite")
		}
	}

	indexes := rewriteIndexIDs(mi, oldIndexes)

	p := opts.SaveProgress
	if p == nil {
		p = vaultic.NoopCounter
	}
	p.SetMax(uint64(len(indexes)))

	// reset state which is not necessary for Rewrite and just consumes a lot of memory
	// the index state would be invalid after Rewrite completes anyways
	mi.clear()
	runtime.GC()

	excludePacks = cloneRewriteExcludes(excludePacks)
	debug.Log("start rebuilding index of %d indexes, excludePacks: %v", len(indexes), excludePacks)
	group, groupCtx := errgroup.WithContext(ctx)
	pipeline := rewritePipeline{
		master:       mi,
		repo:         repo,
		excludePacks: excludePacks,
		progress:     p,
		opts:         opts,
		group:        group,
		ctx:          groupCtx,
		rewriteCh:    make(chan rewriteTask),
		saveCh:       make(chan *Index),
		obsolete:     vaultic.NewIDSet(extraObsolete...),
	}
	pipeline.startLoaders(indexes)
	pipeline.startRewriter()
	pipeline.startSavers()

	err := group.Wait()
	p.Done()
	if err != nil {
		return fmt.Errorf("failed to rewrite indexes: %w", err)
	}

	return deleteObsoleteIndexes(ctx, repo, pipeline.obsolete, opts)
}

func (pipeline *rewritePipeline) startLoaders(indexes vaultic.IDSet) {
	idxCh := make(chan vaultic.ID)
	pipeline.group.Go(func() error {
		defer close(idxCh)
		for id := range indexes {
			select {
			case idxCh <- id:
			case <-pipeline.ctx.Done():
				return pipeline.ctx.Err()
			}
		}
		return nil
	})

	var loaders sync.WaitGroup
	loader := func() error {
		defer loaders.Done()
		for id := range idxCh {
			buf, err := pipeline.repo.LoadUnpacked(pipeline.ctx, vaultic.IndexFile, id)
			if err != nil {
				return fmt.Errorf("LoadUnpacked(%v): %w", id.Str(), err)
			}
			idx, err := DecodeIndex(buf, id)
			if err != nil {
				return err
			}
			select {
			case pipeline.rewriteCh <- rewriteTask{idx}:
			case <-pipeline.ctx.Done():
				return pipeline.ctx.Err()
			}
		}
		return nil
	}
	for range runtime.GOMAXPROCS(0) {
		loaders.Add(1)
		pipeline.group.Go(loader)
	}
	pipeline.group.Go(func() error {
		loaders.Wait()
		close(pipeline.rewriteCh)
		return nil
	})
}

func (pipeline *rewritePipeline) startRewriter() {
	pipeline.group.Go(pipeline.rewriteIndexes)
}

func (pipeline *rewritePipeline) rewriteIndexes() error {
	defer close(pipeline.saveCh)
	// Track pack contents separately to handle indexes written by vaultic < 0.10.0,
	// which could split one pack's blobs over multiple indexes.
	packBlobsIDSet := vaultic.NewIDSet()
	newIndex := NewIndex()
	for task := range pipeline.rewriteCh {
		if pipeline.keepCurrentIndex(task.idx, packBlobsIDSet) {
			pipeline.progress.Add(1)
			continue
		}

		pipeline.markObsolete(task.idx)
		var err error
		newIndex, err = pipeline.copyIndexPacks(task.idx, packBlobsIDSet, newIndex)
		if err != nil {
			return err
		}
		pipeline.progress.Add(1)
	}

	select {
	case pipeline.saveCh <- newIndex:
	case <-pipeline.ctx.Done():
	}
	return nil
}

func (pipeline *rewritePipeline) keepCurrentIndex(idx *Index, packBlobsIDSet vaultic.IDSet) bool {
	if len(idx.Packs().Intersect(pipeline.excludePacks)) != 0 || !Full(idx) || Oversized(idx) {
		return false
	}

	idxPackBlobsIDSet := vaultic.NewIDSet()
	for pbs := range idx.EachByPack(pipeline.ctx, pipeline.excludePacks) {
		idxPackBlobsIDSet.Insert(PackBlobsHash(pbs))
	}
	if len(idxPackBlobsIDSet.Intersect(packBlobsIDSet)) != 0 {
		return false
	}

	packBlobsIDSet.Merge(idxPackBlobsIDSet)
	return true
}

func (pipeline *rewritePipeline) markObsolete(idx *Index) {
	ids, err := idx.IDs()
	if err != nil || len(ids) != 1 {
		// A finalized rewrite input must have exactly one ID.
		//nolint:forbidigo // This existing panic enforces an internal invariant; new panic paths remain forbidden.
		panic("internal error, index has no ID")
	}
	pipeline.obsolete.Merge(vaultic.NewIDSet(ids...))
}

func (pipeline *rewritePipeline) copyIndexPacks(idx *Index, packBlobsIDSet vaultic.IDSet, newIndex *Index) (*Index, error) {
	for pbs := range idx.EachByPack(pipeline.ctx, pipeline.excludePacks) {
		packBlobsID := PackBlobsHash(pbs)
		if packBlobsIDSet.Has(packBlobsID) {
			continue
		}
		packBlobsIDSet.Insert(packBlobsID)

		newIndex.StorePack(pbs.PackID, pbs.Blobs)
		if Full(newIndex) {
			select {
			case pipeline.saveCh <- newIndex:
			case <-pipeline.ctx.Done():
				return nil, pipeline.ctx.Err()
			}
			newIndex = NewIndex()
		}
	}
	if pipeline.ctx.Err() != nil {
		return nil, pipeline.ctx.Err()
	}
	return newIndex, nil
}

func (pipeline *rewritePipeline) startSavers() {
	var savers errgroup.Group
	// encoding an index can take quite some time such that this can be CPU- or IO-bound
	// do not add repo.Connections() here as there are already the loader goroutines.
	savers.SetLimit(runtime.GOMAXPROCS(0))

	pipeline.group.Go(func() error {
		for idx := range pipeline.saveCh {
			savers.Go(func() error {
				idx.Finalize()
				if len(idx.packs) == 0 {
					return nil
				}
				id, err := idx.SaveIndex(pipeline.ctx, pipeline.repo)
				if err != nil {
					return err
				}
				// Retain the current rewritten view for callers that immediately
				// revalidate a prune plan under the same exclusive lock.
				pipeline.master.Insert(idx)
				if pipeline.opts.SavedIndexFunc != nil {
					pipeline.opts.SavedIndexFunc(id)
				}
				return nil
			})
		}
		return savers.Wait()
	})
}

func deleteObsoleteIndexes(ctx context.Context, repo vaultic.Unpacked[vaultic.FileType], obsolete vaultic.IDSet, opts MasterIndexRewriteOpts) error {
	p := vaultic.NoopCounter
	if opts.DeleteProgress != nil {
		p = opts.DeleteProgress()
	}
	defer p.Done()
	if opts.SkipObsoleteDelete {
		if opts.ObsoleteIndexFunc != nil {
			opts.ObsoleteIndexFunc(obsolete.Clone())
		}
		return nil
	}
	return vaultic.ParallelRemove(ctx, repo, obsolete, vaultic.IndexFile, func(id vaultic.ID, err error) error {
		if opts.DeleteReport != nil {
			opts.DeleteReport(id, err)
		}
		return err
	}, p)
}

// SaveFallback saves all known indexes to index files, leaving out any
// packs whose ID is contained in packBlacklist from finalized indexes.
// It is only intended for use by prune with the UnsafeRecovery option.
//
// Must not be called concurrently to any other MasterIndex operation.
func (mi *MasterIndex) SaveFallback(
	ctx context.Context,
	repo vaultic.SaverRemoverUnpacked[vaultic.FileType],
	excludePacks vaultic.IDSet,
	p vaultic.Counter,
) error {
	p.SetMax(uint64(len(mi.Packs(excludePacks))))

	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	debug.Log("start rebuilding index of %d indexes, excludePacks: %v", len(mi.idx), excludePacks)

	obsolete := vaultic.NewIDSet()
	wg, wgCtx := errgroup.WithContext(ctx)
	// keep concurrency bounded as we're on a fallback path
	wg.SetLimit(1 + int(repo.Connections()))

	ch := make(chan *Index)
	wg.Go(func() error {
		defer close(ch)
		newIndex := NewIndex()
		for _, idx := range mi.idx {
			if idx.Final() {
				ids, err := idx.IDs()
				if err != nil {
					//nolint:forbidigo // This existing panic enforces an internal invariant; new panic paths remain forbidden.
					panic("internal error - finalized index without ID")
				}
				debug.Log("adding index ids %v to supersedes field", ids)
				obsolete.Merge(vaultic.NewIDSet(ids...))
			}

			for pbs := range idx.EachByPack(wgCtx, excludePacks) {
				newIndex.StorePack(pbs.PackID, pbs.Blobs)
				p.Add(1)
				if Full(newIndex) {
					select {
					case ch <- newIndex:
					case <-wgCtx.Done():
						return wgCtx.Err()
					}
					newIndex = NewIndex()
				}
			}
			if wgCtx.Err() != nil {
				return wgCtx.Err()
			}
		}

		select {
		case ch <- newIndex:
		case <-wgCtx.Done():
		}
		return nil
	})

	for idx := range ch {
		wg.Go(func() error {
			idx.Finalize()
			_, err := idx.SaveIndex(wgCtx, repo)
			return err
		})
	}

	err := wg.Wait()
	p.Done()
	// the index no longer matches to stored state
	mi.clear()

	return err
}

// saveIndex saves all indexes in the backend.
func (mi *MasterIndex) saveIndex(ctx context.Context, r vaultic.SaverUnpacked[vaultic.FileType], indexes ...*Index) error {
	for i, idx := range indexes {
		debug.Log("Saving index %d", i)

		sid, err := idx.SaveIndex(ctx, r)
		if err != nil {
			return err
		}

		debug.Log("Saved index %d as %v", i, sid)
	}

	return mi.MergeFinalIndexes()
}

// Flush saves all new indexes in the backend.
func (mi *MasterIndex) Flush(ctx context.Context, r vaultic.SaverUnpacked[vaultic.FileType]) error {
	return mi.saveIndex(ctx, r, mi.finalizeNotFinalIndexes()...)
}

// saveFullIndex saves all full indexes in the backend.
func (mi *MasterIndex) saveFullIndex(ctx context.Context, r vaultic.SaverUnpacked[vaultic.FileType]) error {
	return mi.saveIndex(ctx, r, mi.finalizeFullIndexes()...)
}

// ListPacks returns the blobs of the specified pack files grouped by pack file.
//
//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func (mi *MasterIndex) ListPacks(ctx context.Context, packs vaultic.IDSet) <-chan PackBlobs {
	out := make(chan PackBlobs)
	go func() {
		defer close(out)
		// only resort a part of the index to keep the memory overhead bounded
		for i := range byte(16) {
			packBlob := make(map[vaultic.ID]pack.Blobs)
			for pack := range packs {
				if pack[0]&0xf == i {
					packBlob[pack] = nil
				}
			}
			if len(packBlob) == 0 {
				continue
			}
			for pb := range mi.Values() {
				if ctx.Err() != nil {
					return
				}
				packID := pb.PackID()
				if packs.Has(packID) && packID[0]&0xf == i {
					packBlob[packID] = append(packBlob[packID], pb.Blob)
				}
			}

			// pass on packs
			for packID, pbs := range packBlob {
				// allow GC
				packBlob[packID] = nil
				select {
				case out <- PackBlobs{PackID: packID, Blobs: pbs}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// Only for use by AssociatedSet
func (mi *MasterIndex) blobIndex(h vaultic.BlobHandle) int {
	mi.idxMutex.RLock()
	defer mi.idxMutex.RUnlock()

	// other indexes are ignored as their ids can change when merged into the main index
	return mi.idx[0].BlobIndex(h)
}

// Only for use by AssociatedSet
func (mi *MasterIndex) stableLen(t vaultic.BlobType) uint {
	mi.idxMutex.RLock()
	defer mi.idxMutex.RUnlock()

	// other indexes are ignored as their ids can change when merged into the main index
	return mi.idx[0].Len(t)
}
