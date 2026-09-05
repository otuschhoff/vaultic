package repository

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/repository/hashing"
	"github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/sync/errgroup"
)

const maxStreamBufferSize = 4 * 1024 * 1024

// ErrIncompletePackEntry is returned when indexes contain different data for a pack.
type ErrIncompletePackEntry struct {
	PackID  vaultic.ID
	Indexes vaultic.IDSet
}

func (e *ErrIncompletePackEntry) Error() string {
	return fmt.Sprintf("pack %v has different data in indexes: %v", e.PackID, e.Indexes)
}

// ErrDuplicatePacks is returned when a pack is found in more than one index.
type ErrDuplicatePacks struct {
	PackID  vaultic.ID
	Indexes vaultic.IDSet
}

func (e *ErrDuplicatePacks) Error() string {
	return fmt.Sprintf("pack %v contained in several indexes: %v", e.PackID, e.Indexes)
}

// ErrMixedPack is returned when a pack is found that contains both tree and data blobs.
type ErrMixedPack struct {
	PackID vaultic.ID
}

func (e *ErrMixedPack) Error() string {
	return fmt.Sprintf("pack %v contains a mix of tree and data blobs", e.PackID.Str())
}

// ErrPackMetadata describes an error with a specific pack. It is used for missing, truncated or orphaned packs.
// Errors of the actual pack data are returned as ErrPackData.
type ErrPackMetadata struct {
	ID        vaultic.ID
	Orphaned  bool
	Truncated bool
	Missing   bool
	Err       error
}

func (e *ErrPackMetadata) Error() string {
	return "pack " + e.ID.String() + ": " + e.Err.Error()
}

// ErrPackData is returned if errors are discovered while verifying a packfile
type ErrPackData struct {
	PackID vaultic.ID
	errs   []error
}

func (e *ErrPackData) Error() string {
	return fmt.Sprintf("pack %v contains %v errors: %v", e.PackID, len(e.errs), e.errs)
}

func (e *ErrPackData) Unwrap() []error { return e.errs }

// Checker handles index-related operations for repository checking.
type Checker struct {
	repo *Repository
}

// newChecker creates a new Checker.
func newChecker(repo *Repository) *Checker {
	return &Checker{
		repo: repo,
	}
}
func computePackTypes(ctx context.Context, idx vaultic.ListBlobser) (map[vaultic.ID]vaultic.BlobType, error) {
	packs := make(map[vaultic.ID]vaultic.BlobType)
	err := idx.ListBlobs(ctx, func(pb vaultic.PackBlob) {
		packID := pb.PackID()
		h := pb.Handle()
		tpe, exists := packs[packID]
		if exists {
			if h.Type != tpe {
				tpe = vaultic.InvalidBlob
			}
		} else {
			tpe = h.Type
		}
		packs[packID] = tpe
	})
	return packs, err
}

// LoadIndex loads all index files.
func (c *Checker) LoadIndex(ctx context.Context, p vaultic.TerminalCounterFactory) (hints []error, errs []error) {
	debug.Log("Start")
	packToIndex := make(map[vaultic.ID]vaultic.IDSet)
	// in vaultic < 0.10.0, the blobs of a pack could be split over multiple indexes.
	// by now this is considered as repository damage.
	packToPackBlobHash := make(map[vaultic.ID]vaultic.IDSet)

	// Use the repository's internal loadIndexWithCallback to handle per-index errors
	err := c.repo.loadIndexWithCallback(ctx, p, func(id vaultic.ID, idx *index.Index, err error) error {
		debug.Log("process index %v, err %v", id, err)
		err = errors.Wrapf(err, "error loading index %v", id)

		if err != nil {
			errs = append(errs, err)
			return nil
		}

		debug.Log("process blobs")
		cnt := 0
		for blob := range idx.Values() {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			cnt++

			packID := blob.PackID()
			if _, ok := packToIndex[packID]; !ok {
				packToIndex[packID] = vaultic.NewIDSet()
			}
			packToIndex[packID].Insert(id)
		}

		for pbs := range idx.EachByPack(ctx, vaultic.NewIDSet()) {
			packBlobHash := index.PackBlobsHash(pbs)
			if _, ok := packToPackBlobHash[pbs.PackID]; !ok {
				packToPackBlobHash[pbs.PackID] = vaultic.NewIDSet()
			}
			packToPackBlobHash[pbs.PackID].Insert(packBlobHash)
		}

		debug.Log("%d blobs processed", cnt)
		return nil
	})
	if err != nil {
		// failed to load the index
		return hints, append(errs, err)
	}

	packTypes, err := computePackTypes(ctx, c.repo)
	if err != nil {
		return hints, append(errs, err)
	}

	debug.Log("checking for duplicate packs")
	for packID := range packTypes {
		debug.Log("  check pack %v: contained in %d indexes", packID, len(packToIndex[packID]))
		if len(packToPackBlobHash[packID]) > 1 {
			hints = append(hints, &ErrIncompletePackEntry{
				PackID:  packID,
				Indexes: packToIndex[packID],
			})
		} else if len(packToIndex[packID]) > 1 {
			hints = append(hints, &ErrDuplicatePacks{
				PackID:  packID,
				Indexes: packToIndex[packID],
			})
		}
		if packTypes[packID] == vaultic.InvalidBlob {
			hints = append(hints, &ErrMixedPack{
				PackID: packID,
			})
		}
	}

	return hints, errs
}

// Packs checks that all packs referenced in the index are still available and
// there are no packs that aren't in an index. errChan is closed after all
// packs have been checked.
func (c *Checker) Packs(ctx context.Context, errChan chan<- error) {
	defer close(errChan)

	// compute pack size using index entries
	packs, err := pack.Size(ctx, c.repo, false)
	if err != nil {
		errChan <- err
		return
	}

	debug.Log("checking for %d packs", len(packs))

	debug.Log("listing repository packs")
	repoPacks := make(map[vaultic.ID]int64)

	err = c.repo.List(ctx, vaultic.PackFile, func(id vaultic.ID, size int64) error {
		repoPacks[id] = size
		return nil
	})

	if err != nil {
		errChan <- err
	}

	for id, size := range packs {
		reposize, ok := repoPacks[id]
		// remove from repoPacks so we can find orphaned packs
		delete(repoPacks, id)

		// missing: present in c.packs but not in the repo
		if !ok {
			select {
			case <-ctx.Done():
				return
			case errChan <- &ErrPackMetadata{ID: id, Missing: true, Err: errors.New("does not exist")}:
			}
			continue
		}

		// size not matching: present in c.packs and in the repo, but sizes do not match
		if size != reposize {
			select {
			case <-ctx.Done():
				return
			case errChan <- &ErrPackMetadata{ID: id, Truncated: true, Err: errors.Errorf("unexpected file size: got %d, expected %d", reposize, size)}:
			}
		}
	}

	// orphaned: present in the repo but not in c.packs
	for orphanID := range repoPacks {
		select {
		case <-ctx.Done():
			return
		case errChan <- &ErrPackMetadata{ID: orphanID, Orphaned: true, Err: errors.New("not referenced in any index")}:
		}
	}
}

// ReadPacks loads data from specified packs and checks the integrity.
//
//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func (c *Checker) ReadPacks(ctx context.Context, filter func(packs map[vaultic.ID]int64) map[vaultic.ID]int64, printer vaultic.Printer, errChan chan<- error) {
	defer close(errChan)

	// compute pack size using index entries
	packs, err := pack.Size(ctx, c.repo, false)
	if err != nil {
		errChan <- err
		return
	}
	packs = filter(packs)

	p := printer.NewCounter("packs")
	p.SetMax(uint64(len(packs)))
	defer p.Done()

	packSet := vaultic.NewIDSet()
	for pack := range packs {
		packSet.Insert(pack)
	}

	if feature.Flag.Enabled(feature.S3Restore) || feature.Flag.Enabled(feature.WarmupCommand) {
		job, err := c.repo.StartWarmup(ctx, packSet)
		if err != nil {
			errChan <- err
			return
		}
		if job.HandleCount() != 0 {
			printer.P("warming up %d packs from cold storage, this may take a while...", job.HandleCount())
			if err := job.Wait(ctx); err != nil {
				errChan <- err
				return
			}
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	type checkTask struct {
		id    vaultic.ID
		size  int64
		blobs pack.Blobs
	}
	ch := make(chan checkTask)

	// as packs are streamed the concurrency is limited by IO
	workerCount := int(c.repo.Connections())
	// run workers
	for range workerCount {
		g.Go(func() error {
			bufRd := bufio.NewReaderSize(nil, maxStreamBufferSize)
			dec, err := zstd.NewReader(nil)
			if err != nil {
				//nolint:forbidigo // This existing panic enforces an internal invariant; new panic paths remain forbidden.
				panic(err)
			}
			defer dec.Close()
			for {
				var ps checkTask
				var ok bool

				select {
				case <-ctx.Done():
					return nil
				case ps, ok = <-ch:
					if !ok {
						return nil
					}
				}

				err := checkPack(ctx, c.repo, ps.id, ps.blobs, ps.size, bufRd, dec)
				p.Add(1)
				if err == nil {
					continue
				}

				select {
				case <-ctx.Done():
					return nil
				case errChan <- err:
				}
			}
		})
	}

	// push packs to ch
	for pbs := range c.repo.listPacksFromIndex(ctx, packSet) {
		size := packs[pbs.PackID]
		debug.Log("listed %v", pbs.PackID)
		select {
		case ch <- checkTask{id: pbs.PackID, size: size, blobs: pbs.Blobs}:
		case <-ctx.Done():
		}
	}
	close(ch)

	err = g.Wait()
	if err != nil {
		select {
		case <-ctx.Done():
			return
		case errChan <- err:
		}
	}
}

// checkPack reads a pack and checks the integrity of all blobs.
func checkPack(ctx context.Context, r *Repository, id vaultic.ID, blobs pack.Blobs, size int64, bufRd *bufio.Reader, dec *zstd.Decoder) error {
	err := checkPackInner(ctx, r, id, blobs, size, bufRd, dec)
	if err != nil {
		if r.cache != nil {
			// ignore error as there's not much we can do here
			_ = r.cache.Forget(backend.Handle{Type: backend.PackFile, Name: id.String()}) // Cache eviction cannot change repository integrity results.
		}

		// retry pack verification to detect transient errors
		err2 := checkPackInner(ctx, r, id, blobs, size, bufRd, dec)
		if err2 != nil {
			err = err2
		} else {
			err = fmt.Errorf("check successful on second attempt, original error %w", err)
		}
	}
	return err
}

func checkPackInner(ctx context.Context, r *Repository, id vaultic.ID, blobs pack.Blobs, size int64, bufRd *bufio.Reader, dec *zstd.Decoder) error {
	return checkPackInnerBackend(ctx, r, r.be, id, blobs, size, bufRd, dec)
}

type partialReadError struct {
	error
}

type streamedPack struct {
	hash       vaultic.ID
	header     []byte
	blobErrors []error
}

func checkPackInnerBackend(
	ctx context.Context,
	r *Repository,
	source backend.Backend,
	id vaultic.ID,
	blobs pack.Blobs,
	size int64,
	bufRd *bufio.Reader,
	dec *zstd.Decoder,
) error {
	debug.Log("checking pack %v", id.String())

	if len(blobs) == 0 {
		return &ErrPackData{PackID: id, errs: []error{errors.New("pack is empty or not indexed")}}
	}

	// sanity check blobs in index
	blobs.Sort()
	idxHdrSize := pack.CalculateHeaderSize(blobs)
	lastBlobEnd := 0
	nonContinuousPack := false
	for _, blob := range blobs {
		if lastBlobEnd != int(blob.Offset) {
			nonContinuousPack = true
		}
		lastBlobEnd = int(blob.Offset + blob.Length)
	}
	// size was calculated by masterindex.PackSize, thus there's no need to recalculate it here

	var errs []error
	if nonContinuousPack {
		debug.Log("Index for pack contains gaps / overlaps, blobs: %v", blobs)
		errs = append(errs, errors.New("index for pack contains gaps / overlapping blobs"))
	}

	streamed, err := streamPackContents(ctx, r, source, id, blobs, size, lastBlobEnd, bufRd, dec)
	errs = append(errs, streamed.blobErrors...)
	if err != nil {
		var e *partialReadError
		isPartialReadError := errors.As(err, &e)
		// failed to load the pack file, return as further checks cannot succeed anyways
		debug.Log("  error streaming pack (partial %v): %v", isPartialReadError, err)
		if isPartialReadError {
			return &ErrPackData{PackID: id, errs: append(errs, fmt.Errorf("partial download error: %w", err))}
		}

		// The check command suggests to repair files for which a `ErrPackData` is returned. However, this file
		// completely failed to download such that there's no point in repairing anything.
		return fmt.Errorf("download error: %w", err)
	}
	if !streamed.hash.Equal(id) {
		debug.Log("pack ID does not match, want %v, got %v", id, streamed.hash)
		return &ErrPackData{PackID: id, errs: append(errs, errors.Errorf("unexpected pack id %v", streamed.hash))}
	}

	blobs, hdrSize, err := pack.List(r.Key(), bytes.NewReader(streamed.header), int64(len(streamed.header)))
	if err != nil {
		return &ErrPackData{PackID: id, errs: append(errs, err)}
	}

	if uint32(idxHdrSize) != hdrSize {
		debug.Log("Pack header size does not match, want %v, got %v", idxHdrSize, hdrSize)
		errs = append(errs, errors.Errorf("pack header size does not match, want %v, got %v", idxHdrSize, hdrSize))
	}

	engine, err := r.legacyIndexEngine()
	if err != nil {
		return err
	}
	for _, blob := range blobs {
		// Check if blob is contained in index and position is correct
		idxHas := false
		for _, pb := range engine.Lookup(blob.BlobHandle) {
			if pb.PackID().Equal(id) && pb.Blob == blob {
				idxHas = true
				break
			}
		}
		if !idxHas {
			errs = append(errs, errors.Errorf("blob %v is not contained in index or position is incorrect", blob.ID))
			continue
		}
	}

	if len(errs) > 0 {
		return &ErrPackData{PackID: id, errs: errs}
	}

	return nil
}

func streamPackContents(
	ctx context.Context,
	repository *Repository,
	source backend.Backend,
	id vaultic.ID,
	blobs pack.Blobs,
	size int64,
	lastBlobEnd int,
	buffer *bufio.Reader,
	decoder *zstd.Decoder,
) (streamed streamedPack, err error) {
	handle := backend.Handle{Type: backend.PackFile, Name: id.String()}
	err = source.Load(ctx, handle, int(size), 0, func(reader io.Reader) error {
		hashingReader := hashing.NewReader(reader, sha256.New())
		buffer.Reset(hashingReader)
		streamed.blobErrors = nil
		iterator := newPackBlobIterator(id, newBufReader(buffer), 0, blobs, repository.Key(), decoder)
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			value, nextErr := iterator.Next()
			if errors.Is(nextErr, errPackEOF) {
				break
			}
			if nextErr != nil {
				return &partialReadError{nextErr}
			}
			debug.Log("  check blob %v: %v", value.Handle.ID, value.Handle)
			if value.Err != nil {
				debug.Log("  error verifying blob %v: %v", value.Handle.ID, value.Err)
				streamed.blobErrors = append(streamed.blobErrors, fmt.Errorf("blob %v: %w", value.Handle.ID, value.Err))
			}
		}
		position := lastBlobEnd
		minimumHeaderStart := int(size) - pack.MaxHeaderSize
		if minimumHeaderStart > position {
			if _, err := buffer.Discard(minimumHeaderStart - position); err != nil {
				return &partialReadError{err}
			}
			position = minimumHeaderStart
		}
		streamed.header = make([]byte, int(size-int64(position)))
		if _, err := io.ReadFull(buffer, streamed.header); err != nil {
			return &partialReadError{err}
		}
		streamed.hash = vaultic.IDFromHash(hashingReader.Sum(nil))
		return nil
	})
	return streamed, err
}

type bufReader struct {
	rd  *bufio.Reader
	buf []byte
}

func newBufReader(rd *bufio.Reader) *bufReader {
	return &bufReader{
		rd: rd,
	}
}

func (b *bufReader) Discard(n int) (discarded int, err error) {
	return b.rd.Discard(n)
}

func (b *bufReader) ReadFull(n int) (buf []byte, err error) {
	if cap(b.buf) < n {
		b.buf = make([]byte, n)
	}
	b.buf = b.buf[:n]

	_, err = io.ReadFull(b.rd, b.buf)
	if err != nil {
		return nil, err
	}
	return b.buf, nil
}
