package repository

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	"github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/vaultic"

	"golang.org/x/sync/errgroup"
)

func (r *Repository) WithBlobUploader(ctx context.Context, fn func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	// pack uploader + wg.Go below + blob saver (CPU bound)
	wg.SetLimit(2 + runtime.GOMAXPROCS(0))
	r.mainWg = wg
	if err := r.startPackUploader(ctx, wg); err != nil {
		return err
	}
	// blob saver are spawned on demand, use wait group to keep track of them
	r.blobSaver = &sync.WaitGroup{}
	wg.Go(func() error {
		inCallback := true
		defer func() {
			// when the defer is called while inCallback is true, this means
			// that runtime.Goexit was called within `fn`. This should only happen
			// if a test uses t.Fatal within `fn`.
			if inCallback {
				cancel()
			}
		}()
		err := fn(ctx, &blobSaverRepo{repo: r})
		inCallback = false
		if err != nil {
			return err
		}
		if err := r.flush(ctx); err != nil {
			return fmt.Errorf("error flushing repository: %w", err)
		}
		return nil
	})
	return wg.Wait()
}

func (r *Repository) startPackUploader(ctx context.Context, wg *errgroup.Group) error {
	if r.packerWg != nil {
		return ErrUploaderAlreadyStarted
	}

	innerWg, ctx := errgroup.WithContext(ctx)
	r.packerWg = innerWg
	r.uploader = newPackerUploader(ctx, innerWg, r, r.Connections())
	treeSize, treeLimit, treeGrow := r.packSizing(vaultic.TreeBlob)
	dataSize, dataLimit, dataGrow := r.packSizing(vaultic.DataBlob)
	r.treePM = newConfiguredPackerManager(
		r.key,
		vaultic.TreeBlob,
		treeSize,
		treeLimit,
		r.currentBlobSize(vaultic.TreeBlob),
		treeGrow,
		r.packerCount,
		r.uploader.QueuePacker,
	)
	r.dataPM = newConfiguredPackerManager(
		r.key,
		vaultic.DataBlob,
		dataSize,
		dataLimit,
		r.currentBlobSize(vaultic.DataBlob),
		dataGrow,
		r.packerCount,
		r.uploader.QueuePacker,
	)

	wg.Go(func() error {
		return innerWg.Wait()
	})
	return nil
}

type blobSaverRepo struct {
	repo *Repository
}

func (r *blobSaverRepo) SaveBlob(
	ctx context.Context,
	t vaultic.BlobType,
	buf []byte,
	id vaultic.ID,
	storeDuplicate bool,
) (newID vaultic.ID, known bool, size int, err error) {
	return r.repo.saveBlob(ctx, t, buf, id, storeDuplicate)
}

func (r *blobSaverRepo) SaveBlobAsync(
	ctx context.Context,
	t vaultic.BlobType,
	buf []byte,
	id vaultic.ID,
	storeDuplicate bool,
	cb func(newID vaultic.ID, known bool, size int, err error),
) {
	r.repo.saveBlobAsync(ctx, t, buf, id, storeDuplicate, cb)
}

// Flush saves all remaining packs and the index
func (r *Repository) flush(ctx context.Context) error {
	r.flushBlobSaver()
	r.mainWg = nil

	if err := r.flushPackUploader(ctx); err != nil {
		return err
	}

	engine, err := r.legacyIndexEngine()
	if err != nil {
		return err
	}
	return engine.Flush(ctx, &internalRepository{r})
}

func (r *Repository) flushBlobSaver() {
	if r.blobSaver == nil {
		return
	}
	r.blobSaver.Wait()
	r.blobSaver = nil
}

// FlushPacks saves all remaining packs.
func (r *Repository) flushPackUploader(ctx context.Context) error {
	if r.packerWg == nil {
		return nil
	}

	err := r.treePM.Flush(ctx)
	if err != nil {
		return err
	}
	err = r.dataPM.Flush(ctx)
	if err != nil {
		return err
	}
	r.uploader.TriggerShutdown()
	err = r.packerWg.Wait()

	r.treePM = nil
	r.dataPM = nil
	r.uploader = nil
	r.packerWg = nil

	return err
}

func (r *Repository) Connections() uint {
	return r.be.Properties().Connections
}

func (r *Repository) LookupBlob(bh vaultic.BlobHandle) []vaultic.PackBlob {
	engine, err := r.legacyIndexEngine()
	if err != nil {
		return nil
	}
	entries := engine.Lookup(bh)
	out := make([]vaultic.PackBlob, len(entries))
	for i, e := range entries {
		out[i] = e
	}
	return out
}

// LookupBlobSize returns the size of blob id. Also returns pending blobs.
func (r *Repository) LookupBlobSize(bh vaultic.BlobHandle) (uint, bool) {
	engine, err := r.legacyIndexEngine()
	if err != nil {
		return 0, false
	}
	return engine.LookupSize(bh)
}

// ListBlobs runs fn on all blobs known to the index. When the context is cancelled,
// the index iteration returns immediately with ctx.Err(). This blocks any modification of the index.
func (r *Repository) ListBlobs(ctx context.Context, fn func(vaultic.PackBlob)) error {
	engine, err := r.legacyIndexEngine()
	if err != nil {
		return err
	}
	for blob := range engine.Values() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fn(blob)
	}
	return nil
}

// listPacksFromIndex returns index entries for the given packs, grouped by pack file.
func (r *Repository) listPacksFromIndex(ctx context.Context, packs vaultic.IDSet) <-chan index.PackBlobs {
	engine, err := r.legacyIndexEngine()
	if err != nil {
		result := make(chan index.PackBlobs)
		close(result)
		return result
	}
	return engine.ListPacks(ctx, packs)
}

func (r *Repository) clearIndex() {
	r.idx = index.NewMasterIndex()
	r.engine = enginepkg.NewLegacyEngine(r.idx)
}

// LoadIndex loads all index files from the backend in parallel and stores them
func (r *Repository) LoadIndex(ctx context.Context, p vaultic.TerminalCounterFactory) error {
	return r.loadIndexWithCallback(ctx, p, nil)
}

// SaveLegacyIndex writes one canonical JSON index for metadata export tools.
func (r *Repository) SaveLegacyIndex(ctx context.Context, index *index.Index) (vaultic.ID, error) {
	return index.SaveIndex(ctx, &internalRepository{Repository: r})
}

// loadIndexWithCallback loads all index files from the backend in parallel and stores them
func (r *Repository) loadIndexWithCallback(
	ctx context.Context,
	p vaultic.TerminalCounterFactory,
	cb func(id vaultic.ID, idx *index.Index, err error) error,
) error {
	debug.Log("Loading index")

	bar := p.NewCounterTerminalOnly("index files loaded")

	engine, err := r.legacyIndexEngine()
	if err != nil {
		return err
	}
	err = engine.Load(ctx, r, bar, cb)
	if err != nil {
		return err
	}

	// Trigger GC to reset garbage collection threshold
	runtime.GC()

	if r.cfg.Version < 2 {
		// sanity check
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		invalidIndex := false
		for blob := range engine.Values() {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if blob.IsCompressed() {
				invalidIndex = true
			}
		}
		if invalidIndex {
			return errors.New("index uses feature not supported by repository version 1")
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// remove index files from the cache which have been removed in the repo
	return r.prepareCache()
}

// createIndexFromPacks creates a new index by reading all given pack files (with sizes).
// The index is added to the MasterIndex but not marked as finalized.
// Returned is the list of pack files which could not be read.
func (r *Repository) createIndexFromPacks(ctx context.Context, packsize map[vaultic.ID]int64, p vaultic.Counter) (invalid vaultic.IDs, err error) {
	var m sync.Mutex

	debug.Log("Loading index from pack files")

	// track spawned goroutines using wg, create a new context which is
	// cancelled as soon as an error occurs.
	wg, wgCtx := errgroup.WithContext(ctx)
	engine, err := r.legacyIndexEngine()
	if err != nil {
		return nil, err
	}

	type FileInfo struct {
		vaultic.ID
		Size int64
	}
	ch := make(chan FileInfo)

	// send list of pack files through ch, which is closed afterwards
	wg.Go(func() error {
		defer close(ch)
		for id, size := range packsize {
			select {
			case <-wgCtx.Done():
				return wgCtx.Err()
			case ch <- FileInfo{id, size}:
			}
		}
		return nil
	})

	// a worker receives an pack ID from ch, reads the pack contents, and adds them to idx
	worker := func() error {
		for fi := range ch {
			entries, err := r.listPack(wgCtx, fi.ID, fi.Size)
			if err != nil {
				debug.Log("unable to list pack file %v", fi.ID.Str())
				m.Lock()
				invalid = append(invalid, fi.ID)
				m.Unlock()
			} else if err := engine.StorePack(wgCtx, fi.ID, entries, &internalRepository{r}); err != nil {
				return err
			}
			p.Add(1)
		}

		return nil
	}

	// decoding the pack header is usually quite fast, thus we are primarily IO-bound
	workerCount := int(r.Connections())
	// run workers on ch
	for range workerCount {
		wg.Go(worker)
	}

	err = wg.Wait()
	if err != nil {
		return invalid, err
	}

	// flush the index to the repository
	err = r.flush(ctx)
	if err != nil {
		return invalid, err
	}

	return invalid, nil
}

func (r *Repository) NewAssociatedBlobSet() vaultic.AssociatedBlobSet {
	return &associatedBlobSet{*index.NewAssociatedSet[struct{}](r.idx)}
}

// associatedBlobSet is a wrapper around index.AssociatedSet to implement the vaultic.AssociatedBlobSet interface.
type associatedBlobSet struct {
	index.AssociatedSet[struct{}]
}

func (s *associatedBlobSet) Intersect(other vaultic.AssociatedBlobSet) vaultic.AssociatedBlobSet {
	return &associatedBlobSet{*s.AssociatedSet.Intersect(other)}
}

func (s *associatedBlobSet) Sub(other vaultic.AssociatedBlobSet) vaultic.AssociatedBlobSet {
	return &associatedBlobSet{*s.AssociatedSet.Sub(other)}
}

// prepareCache initializes the local cache. indexIDs is the list of IDs of
// index files still present in the repo.
func (r *Repository) prepareCache() error {
	if r.cache == nil {
		return nil
	}

	packs := r.idx.Packs(vaultic.NewIDSet())

	ids := make(map[string]struct{})
	for id := range packs {
		ids[id.String()] = struct{}{}
	}

	// clear old packs
	return r.cache.Clear(backend.PackFile, ids)
}

// SearchKey finds a key with the supplied password, afterwards the config is
// read and parsed. It tries at most maxKeys key files in the repo.
func (r *Repository) SearchKey(ctx context.Context, password string, maxKeys int, keyHint string) error {
	key, err := searchKey(ctx, r, password, maxKeys, keyHint)
	if err != nil {
		return err
	}

	oldKey := r.key
	oldKeyID := r.keyID

	r.key = key.master
	r.keyID = key.ID()
	cfg, err := vaultic.LoadConfig(ctx, r)
	if err != nil {
		r.key = oldKey
		r.keyID = oldKeyID

		if err == crypto.ErrUnauthenticated {
			return fmt.Errorf("config or key %v is damaged: %w", key.ID(), err)
		}
		return fmt.Errorf("config cannot be loaded: %w", err)
	}

	r.setConfig(cfg)
	r.ApplyRepoSettings()
	return nil
}
