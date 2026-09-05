package repository

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/klauspost/compress/zstd"
	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/appendonly"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/restic/chunker"
)

// ApplyRepoSettings applies repository-config behavior that must be enforced
// by the repository itself (as opposed to the CLI layer). Currently this
// enforces append-only mode by wrapping the backend.
func (r *Repository) ApplyRepoSettings() {
	if r.cfg.AppendOnly() {
		r.be = appendonly.New(r.be)
	}
}

// UpdateConfig applies fn to the repository config, validates the result and
// writes it back. The write is rejected on an append-only repository.
func (r *Repository) UpdateConfig(ctx context.Context, fn func(*vaultic.Config) error) error {
	if r.cfg.AppendOnly() {
		return errors.Fatal("cannot modify config: repository is in append-only mode")
	}

	cfg := r.cfg
	if err := fn(&cfg); err != nil {
		return err
	}
	if err := cfg.ValidateExtensions(); err != nil {
		return err
	}

	if err := vaultic.SaveConfig(ctx, &internalRepository{r}, cfg); err != nil {
		return err
	}
	r.setConfig(cfg)
	return nil
}

// UpdateConfigAtomically is UpdateConfig for state that must survive a crash
// without a window where the singleton config is absent. It is used by durable
// prune plans and intentionally refuses backends without atomic replacement.
func (r *Repository) UpdateConfigAtomically(ctx context.Context, fn func(*vaultic.Config) error) error {
	if !r.be.Properties().HasAtomicReplace {
		return errors.Fatal("durable prune plans require a backend with atomic config replacement")
	}
	return r.UpdateConfig(ctx, fn)
}

// Init creates a new master key with the supplied password, initializes and
// saves the repository config.
func (r *Repository) Init(ctx context.Context, version uint, password string, chunkerPolynomial *chunker.Pol) error {
	if version > vaultic.MaxRepoVersion {
		return fmt.Errorf("repository version %v too high", version)
	}

	if version < vaultic.MinRepoVersion {
		return fmt.Errorf("repository version %v too low", version)
	}

	_, err := r.be.Stat(ctx, backend.Handle{Type: backend.ConfigFile})
	if err != nil && !r.be.IsNotExist(err) {
		return err
	}
	if err == nil {
		return errors.New("repository master key and config already initialized")
	}
	// double check to make sure that a repository is not accidentally reinitialized
	// if the backend somehow fails to stat the config file. An initialized repository
	// must always contain at least one key file.
	if err := r.List(ctx, vaultic.KeyFile, func(_ vaultic.ID, _ int64) error {
		return errors.New("repository already contains keys")
	}); err != nil {
		return err
	}
	// Also check for snapshots to detect repositories with a misconfigured retention
	// policy that deletes files older than x days. For such repositories usually the
	// config and key files are removed first and therefore the check would not detect
	// the old repository.
	if err := r.List(ctx, vaultic.SnapshotFile, func(_ vaultic.ID, _ int64) error {
		return errors.New("repository already contains snapshots")
	}); err != nil {
		return err
	}

	cfg, err := vaultic.CreateConfig(version, chunkerPolynomial)
	if err != nil {
		return err
	}

	return r.init(ctx, password, cfg)
}

// InitWithConfig initializes the repository with the given config. Unlike Init
// it does not generate a new repository ID; this is used to create the hot
// part of a hot/cold repository, which shares the cold part's identity (and
// chunker parameters) so that keys/snapshots/indexes are interchangeable.
func (r *Repository) InitWithConfig(ctx context.Context, password string, cfg vaultic.Config) error {
	if cfg.Version > vaultic.MaxRepoVersion || cfg.Version < vaultic.MinRepoVersion {
		return fmt.Errorf("repository version %v out of range", cfg.Version)
	}
	return r.init(ctx, password, cfg)
}

// InitWithConfigAndKey initializes the repository with the given config and an
// existing master key (instead of generating a new one). This is used for the
// hot part of a hot/cold repository so that data written by either part can be
// read with the same master key.
func (r *Repository) InitWithConfigAndKey(ctx context.Context, password string, cfg vaultic.Config, masterKey *crypto.Key) error {
	if cfg.Version > vaultic.MaxRepoVersion || cfg.Version < vaultic.MinRepoVersion {
		return fmt.Errorf("repository version %v out of range", cfg.Version)
	}
	if masterKey == nil {
		return r.init(ctx, password, cfg)
	}

	key, err := AddKey(ctx, r, password, "", "", masterKey)
	if err != nil {
		return err
	}
	r.key = key.master
	r.keyID = key.ID()
	r.setConfig(cfg)
	return vaultic.SaveConfig(ctx, &internalRepository{r}, cfg)
}

// init creates a new master key with the supplied password and uses it to save
// the config into the repo.
func (r *Repository) init(ctx context.Context, password string, cfg vaultic.Config) error {
	key, err := createMasterKey(ctx, r, password)
	if err != nil {
		return err
	}

	r.key = key.master
	r.keyID = key.ID()
	r.setConfig(cfg)
	return vaultic.SaveConfig(ctx, &internalRepository{r}, cfg)
}

// Key returns the current master key.
func (r *Repository) Key() *crypto.Key {
	return r.key
}

// KeyID returns the id of the current key in the backend.
func (r *Repository) KeyID() vaultic.ID {
	return r.keyID
}

// List runs fn for all files of type t in the repo.
func (r *Repository) List(ctx context.Context, t vaultic.FileType, fn func(vaultic.ID, int64) error) error {
	return r.be.List(ctx, backend.FileType(t), func(fi backend.FileInfo) error {
		id, err := vaultic.ParseID(fi.Name)
		if err != nil {
			debug.Log("unable to parse %v as an ID", fi.Name)
			return nil
		}
		return fn(id, fi.Size)
	})
}

// listPack returns blob entries from the pack file header including offsets.
func (r *Repository) listPack(ctx context.Context, id vaultic.ID, size int64) (pack.Blobs, error) {
	h := backend.Handle{Type: backend.PackFile, Name: id.String()}

	entries, _, err := pack.List(r.Key(), backend.ReaderAt(ctx, r.be, h), size)
	if err != nil {
		if r.cache != nil {
			// ignore error as there is not much we can do here
			_ = r.cache.Forget(h) // Cache eviction cannot change the authoritative metadata-engine result.
		}

		// retry on error
		entries, _, err = pack.List(r.Key(), backend.ReaderAt(ctx, r.be, h), size)
	}
	return pack.Blobs(entries), err
}

// ListPackHandles returns the blob handles stored in the pack file header.
func (r *Repository) ListPackHandles(ctx context.Context, id vaultic.ID, size int64) ([]vaultic.BlobHandle, error) {
	blobs, err := r.listPack(ctx, id, size)
	if err != nil {
		return nil, err
	}
	handles := make([]vaultic.BlobHandle, len(blobs))
	for i, blob := range blobs {
		handles[i] = blob.BlobHandle
	}
	return handles, nil
}

// Delete calls backend.Delete() if implemented, and returns an error
// otherwise.
func (r *Repository) Delete(ctx context.Context) error {
	return r.be.Delete(ctx)
}

// Close closes the repository by closing the backend.
func (r *Repository) Close() error {
	errs := []error{r.Engine().Close(), r.be.Close()}
	for _, closer := range r.ownedClosers {
		errs = append(errs, closer.Close())
	}
	for _, placement := range r.ownedPlacementBackends {
		errs = append(errs, placement.Close())
	}
	return errors.Join(errs...)
}

// saveBlob saves a blob of type t into the repository.
// It takes care that no duplicates are saved; this can be overwritten
// by setting storeDuplicate to true.
// If id is the null id, it will be computed and returned.
// Also returns if the blob was already known before.
// If the blob was not known before, it returns the number of bytes the blob
// occupies in the repo (compressed or not, including encryption overhead).
func (r *Repository) saveBlob(
	ctx context.Context,
	t vaultic.BlobType,
	buf []byte,
	id vaultic.ID,
	storeDuplicate bool,
) (newID vaultic.ID, known bool, size int, err error) {

	if int64(len(buf)) > math.MaxUint32 {
		return vaultic.ID{}, false, 0, fmt.Errorf("blob is larger than 4GB")
	}

	// compute plaintext hash if not already set
	if id.IsNull() {
		// Special case the hash calculation for all zero chunks. This is especially
		// useful for sparse files containing large all zero regions. For these we can
		// process chunks as fast as we can read the from disk.
		if len(buf) == chunker.MinSize && vaultic.ZeroPrefixLen(buf) == chunker.MinSize {
			newID = r.zeroChunk()
		} else {
			newID = vaultic.Hash(buf)
		}
	} else {
		newID = id
	}

	// first try to add to pending blobs; if not successful, this blob is already known
	engine, err := r.legacyIndexEngine()
	if err != nil {
		return vaultic.ID{}, false, 0, err
	}
	known = !engine.AddPending(vaultic.BlobHandle{ID: newID, Type: t}, uint(len(buf)))

	// only save when needed or explicitly told
	if !known || storeDuplicate {
		size, err = r.saveAndEncrypt(ctx, t, buf, newID)
	}

	return newID, known, size, err
}

func (r *Repository) saveBlobAsync(
	ctx context.Context,
	t vaultic.BlobType,
	buf []byte,
	id vaultic.ID,
	storeDuplicate bool,
	cb func(newID vaultic.ID, known bool, size int, err error),
) {
	r.mainWg.Go(func() error {
		if ctx.Err() != nil {
			// fail fast if the context is cancelled
			cb(vaultic.ID{}, false, 0, ctx.Err())
			return ctx.Err()
		}
		newID, known, size, err := r.saveBlob(ctx, t, buf, id, storeDuplicate)
		cb(newID, known, size, err)
		return err
	})
}

type backendLoadFn func(ctx context.Context, h backend.Handle, length int, offset int64, fn func(rd io.Reader) error) error

type loadBlobFn func(ctx context.Context, bh vaultic.BlobHandle, buf []byte) ([]byte, error)

// Skip sections with more than 1MB unused blobs
const maxUnusedRange = 1 * 1024 * 1024

var (
	ErrLegacyEngineRequired   = errors.New("repository operation requires the legacy index engine")
	ErrInvalidBlobType        = errors.New("invalid blob type")
	ErrUploaderAlreadyStarted = errors.New("repository uploader already started")
)

// LoadBlobsFromPack loads the listed blobs from the specified pack file. The plaintext blob is passed to
// the handleBlobFn callback or an error if decryption failed or the blob hash does not match.
// handleBlobFn is called at most once for each blob. If the callback returns an error,
// then LoadBlobsFromPack will abort and not retry it. The buf passed to the callback is only valid within
// this specific call. The callback must not keep a reference to buf.
func (r *Repository) LoadBlobsFromPack(
	ctx context.Context,
	packID vaultic.ID,
	handles []vaultic.BlobHandle,
	handleBlobFn func(blob vaultic.BlobHandle, buf []byte, err error) error,
) error {
	blobs, err := r.blobsInPack(packID, handles)
	if err != nil {
		return err
	}
	return r.loadBlobsFromPack(ctx, packID, blobs, handleBlobFn)
}

func (r *Repository) blobsInPack(packID vaultic.ID, handles []vaultic.BlobHandle) (pack.Blobs, error) {
	engine, err := r.legacyIndexEngine()
	if err != nil {
		return nil, err
	}
	blobs := make(pack.Blobs, 0, len(handles))
	for _, h := range handles {
		found := false
		for _, pb := range engine.Lookup(h) {
			if pb.PackID().Equal(packID) {
				blobs = append(blobs, pb.Blob)
				found = true
				break
			}
		}
		if !found {
			return nil, errors.Errorf("blob %v not found in pack %v", h, packID)
		}
	}
	return blobs, nil
}

func (r *Repository) loadBlobsFromPack(
	ctx context.Context,
	packID vaultic.ID,
	blobs pack.Blobs,
	handleBlobFn func(blob vaultic.BlobHandle, buf []byte, err error) error,
) error {
	decoder, err := r.getZstdDecoder()
	if err != nil {
		return err
	}
	return streamPack(ctx, r.loadPackFromPlacements, r.LoadBlob, decoder, r.key, packID, blobs, handleBlobFn)
}

func streamPack(
	ctx context.Context,
	beLoad backendLoadFn,
	loadBlobFn loadBlobFn,
	dec *zstd.Decoder,
	key *crypto.Key,
	packID vaultic.ID,
	blobs pack.Blobs,
	handleBlobFn func(blob vaultic.BlobHandle, buf []byte, err error) error,
) error {
	if len(blobs) == 0 {
		// nothing to do
		return nil
	}
	blobs.Sort()

	lowerIdx := 0
	lastPos := blobs[0].Offset
	const maxChunkSize = 2 * DefaultPackSize

	for i := range blobs {
		if blobs[i].Offset < lastPos {
			// don't wait for streamPackPart to fail
			return errors.Errorf("overlapping blobs in pack %v", packID)
		}

		chunkSizeAfter := (blobs[i].Offset + blobs[i].Length) - blobs[lowerIdx].Offset
		split := false
		// split if the chunk would become larger than maxChunkSize. Oversized chunks are
		// handled by the requirement that the chunk contains at least one blob (i > lowerIdx)
		if i > lowerIdx && chunkSizeAfter >= maxChunkSize {
			split = true
		}
		// skip too large gaps as a new request is typically much cheaper than data transfers
		if blobs[i].Offset-lastPos > maxUnusedRange {
			split = true
		}

		if split {
			// load everything up to the skipped file section
			err := streamPackPart(ctx, beLoad, loadBlobFn, dec, key, packID, blobs[lowerIdx:i], handleBlobFn)
			if err != nil {
				return err
			}
			lowerIdx = i
		}
		lastPos = blobs[i].Offset + blobs[i].Length
	}
	// load remainder
	return streamPackPart(ctx, beLoad, loadBlobFn, dec, key, packID, blobs[lowerIdx:], handleBlobFn)
}

func streamPackPart(
	ctx context.Context,
	beLoad backendLoadFn,
	loadBlobFn loadBlobFn,
	dec *zstd.Decoder,
	key *crypto.Key,
	packID vaultic.ID,
	blobs pack.Blobs,
	handleBlobFn func(blob vaultic.BlobHandle, buf []byte, err error) error,
) error {
	h := backend.Handle{Type: backend.PackFile, Name: packID.String(), IsMetadata: blobs[0].Type.IsMetadata()}

	dataStart := blobs[0].Offset
	dataEnd := blobs[len(blobs)-1].Offset + blobs[len(blobs)-1].Length

	debug.Log("streaming pack %v (%d to %d bytes), blobs: %v", packID, dataStart, dataEnd, len(blobs))

	data := make([]byte, int(dataEnd-dataStart))
	err := beLoad(ctx, h, int(dataEnd-dataStart), int64(dataStart), func(rd io.Reader) error {
		_, cerr := io.ReadFull(rd, data)
		return cerr
	})
	// prevent callbacks after cancellation
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		// the context is only still valid if handleBlobFn never returned an error
		if loadBlobFn != nil {
			// check whether we can get the remaining blobs somewhere else
			for _, entry := range blobs {
				buf, ierr := loadBlobFn(ctx, entry.BlobHandle, nil)
				err = handleBlobFn(entry.BlobHandle, buf, ierr)
				if err != nil {
					break
				}
			}
		}
		return errors.Wrap(err, "StreamPack")
	}

	it := newPackBlobIterator(packID, newByteReader(data), dataStart, blobs, key, dec)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		val, err := it.Next()
		if errors.Is(err, errPackEOF) {
			break
		} else if err != nil {
			return err
		}

		if val.Err != nil && loadBlobFn != nil {
			var ierr error
			// check whether we can get a valid copy somewhere else
			buf, ierr := loadBlobFn(ctx, val.Handle, nil)
			if ierr == nil {
				// success
				val.Plaintext = buf
				val.Err = nil
			}
		}

		err = handleBlobFn(val.Handle, val.Plaintext, val.Err)
		if err != nil {
			return err
		}
		// ensure that each blob is only passed once to handleBlobFn
		blobs = blobs[1:]
	}

	return errors.Wrap(err, "StreamPack")
}

// discardReader allows the PackBlobIterator to perform zero copy
// reads if the underlying data source is a byte slice.
type discardReader interface {
	Discard(n int) (discarded int, err error)
	// ReadFull reads the next n bytes into a byte slice. The caller must not
	// retain a reference to the byte. Modifications are only allowed within
	// the boundaries of the returned slice.
	ReadFull(n int) (buf []byte, err error)
}

type byteReader struct {
	buf []byte
}

func newByteReader(buf []byte) *byteReader {
	return &byteReader{
		buf: buf,
	}
}

func (b *byteReader) Discard(n int) (discarded int, err error) {
	if len(b.buf) < n {
		return 0, io.ErrUnexpectedEOF
	}
	b.buf = b.buf[n:]
	return n, nil
}

func (b *byteReader) ReadFull(n int) (buf []byte, err error) {
	if len(b.buf) < n {
		return nil, io.ErrUnexpectedEOF
	}
	buf = b.buf[:n]
	b.buf = b.buf[n:]
	return buf, nil
}

type packBlobIterator struct {
	packID        vaultic.ID
	rd            discardReader
	currentOffset uint

	blobs pack.Blobs
	key   *crypto.Key
	dec   *zstd.Decoder

	decode []byte
}

type packBlobValue struct {
	Handle    vaultic.BlobHandle
	Plaintext []byte
	Err       error
}

type blobVerificationError struct {
	stage string
	err   error
}

func (err *blobVerificationError) Error() string { return err.err.Error() }

func (err *blobVerificationError) Unwrap() error { return err.err }

var errPackEOF = errors.New("reached EOF of pack file")

func newPackBlobIterator(packID vaultic.ID, rd discardReader, currentOffset uint,
	blobs pack.Blobs, key *crypto.Key, dec *zstd.Decoder) *packBlobIterator {
	return &packBlobIterator{
		packID:        packID,
		rd:            rd,
		currentOffset: currentOffset,
		blobs:         blobs,
		key:           key,
		dec:           dec,
	}
}

// Next returns the next blob, an error or ErrPackEOF if all blobs were read
func (b *packBlobIterator) Next() (packBlobValue, error) {
	if len(b.blobs) == 0 {
		return packBlobValue{}, errPackEOF
	}

	entry := b.blobs[0]
	b.blobs = b.blobs[1:]

	skipBytes := int(entry.Offset - b.currentOffset)
	if skipBytes < 0 {
		return packBlobValue{}, fmt.Errorf("overlapping blobs in pack %v", b.packID)
	}

	_, err := b.rd.Discard(skipBytes)
	if err != nil {
		return packBlobValue{}, err
	}
	b.currentOffset = entry.Offset

	h := vaultic.BlobHandle{ID: entry.ID, Type: entry.Type}
	debug.Log("  process blob %v, skipped %d, %v", h, skipBytes, entry)

	buf, err := b.rd.ReadFull(int(entry.Length))
	if err != nil {
		debug.Log("    read error %v", err)
		return packBlobValue{}, fmt.Errorf("readFull: %w", err)
	}

	b.currentOffset = entry.Offset + entry.Length

	if int(entry.Length) <= b.key.NonceSize() {
		debug.Log("%v", b.blobs)
		return packBlobValue{}, fmt.Errorf("invalid blob length %v", entry)
	}

	// decryption errors are likely permanent, give the caller a chance to skip them
	nonce, ciphertext := buf[:b.key.NonceSize()], buf[b.key.NonceSize():]
	plaintext, err := b.key.Open(ciphertext[:0], nonce, ciphertext, nil)
	if err != nil {
		err = &blobVerificationError{stage: "decrypt", err: fmt.Errorf("decrypting blob %v from pack %v failed: %w", h, b.packID.String(), err)}
	}
	if err == nil && entry.IsCompressed() {
		// DecodeAll will allocate a slice if it is not large enough since it
		// knows the decompressed size (because we're using EncodeAll)
		b.decode, err = b.dec.DecodeAll(plaintext, b.decode[:0])
		plaintext = b.decode
		if err != nil {
			err = &blobVerificationError{stage: "decompress", err: fmt.Errorf("decompressing blob %v from pack %v failed: %w", h, b.packID.String(), err)}
		}
	}
	if err == nil {
		id := vaultic.Hash(plaintext)
		if !id.Equal(entry.ID) {
			debug.Log("read blob %v/%v from pack %v: wrong data returned, hash is %v",
				h.Type, h.ID, b.packID.String(), id)
			err = &blobVerificationError{stage: "hash", err: fmt.Errorf("read blob %v from pack %v: wrong data returned, hash is %v",
				h, b.packID.String(), id)}
		}
	}

	return packBlobValue{entry.BlobHandle, plaintext, err}, nil
}

func (r *Repository) zeroChunk() vaultic.ID {
	r.zeroChunkOnce.Do(func() {
		r.zeroChunkID = vaultic.Hash(make([]byte, chunker.MinSize))
	})
	return r.zeroChunkID
}
