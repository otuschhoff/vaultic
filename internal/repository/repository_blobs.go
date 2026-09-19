package repository

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type decodedPackedBlob struct {
	Plaintext        []byte
	CompressedSource []byte
}

//nolint:gocognit,nestif // Authenticated cache fallbacks remain ordered with origin reads in this security-sensitive path.
func (r *Repository) loadBlob(ctx context.Context, blobs []*pack.PackedBlob, buf []byte) ([]byte, error) {
	var lastError error
	//nolint:contextcheck // The shared cache worker is owned by Repository.Close, while ctx still bounds this blob read.
	manager := r.readCacheManager()
	if manager != nil && len(blobs) != 0 && r.readCacheAuthoritativeEligibleForBlob(ctx, blobs) && manager.refreshPolicyForAccess(ctx) {
		length := int(blobs[0].Blob.DataLength())
		if decoded, ok := manager.loadDecodedBlobRepresentation(ctx, blobs[0].Handle(), length); ok {
			if !vaultic.Hash(decoded).Equal(blobs[0].Blob.ID) {
				debug.Log("read-cache decoded blob hash mismatch for %v", blobs[0].Handle())
			} else {
				if len(decoded) > cap(buf) {
					return decoded, nil
				}
				buf = buf[:len(decoded)]
				copy(buf, decoded)
				return buf, nil
			}
		}
		compressedLength := crypto.PlaintextLength(int(blobs[0].Blob.Length))
		if compressed, ok := manager.loadCompressedBlobRepresentation(ctx, blobs[0].Handle(), compressedLength); ok {
			decoder, err := r.getZstdDecoder()
			if err != nil {
				return nil, err
			}
			decoded, err := decoder.DecodeAll(compressed, nil)
			if err == nil && vaultic.Hash(decoded).Equal(blobs[0].Blob.ID) {
				manager.admitBlobRepresentations(ctx, blobs[0].Handle(), decoded, compressed)
				if len(decoded) > cap(buf) {
					return decoded, nil
				}
				buf = buf[:len(decoded)]
				copy(buf, decoded)
				return buf, nil
			}
		}
	}
	for _, blob := range blobs {
		debug.Log("blob %v found: %v", blob.Handle(), blob)
		// load blob from pack
		h := backend.Handle{
			Type:       backend.PackFile,
			Name:       blob.PackID().String(),
			IsMetadata: blob.Blob.Type.IsMetadata(),
		}

		switch {
		case cap(buf) < int(blob.Blob.Length):
			buf = make([]byte, blob.Blob.Length)
		case len(buf) != int(blob.Blob.Length):
			buf = buf[:blob.Blob.Length]
		}

		_, err := r.readPackAtFromPlacements(ctx, h, int64(blob.Blob.Offset), buf)
		if err != nil {
			debug.Log("error loading blob %v: %v", blob, err)
			lastError = err
			continue
		}

		decoded, err := r.decodePackedBlob(blob, buf)
		if err != nil {
			debug.Log("error decoding blob %v: %v", blob, err)
			lastError = err
			continue
		}
		if manager != nil {
			manager.admitBlobRepresentations(ctx, blob.Handle(), decoded.Plaintext, decoded.CompressedSource)
		}
		plaintext := decoded.Plaintext
		if len(plaintext) > cap(buf) {
			return plaintext, nil
		}
		// move decrypted data to the start of the buffer
		buf = buf[:len(plaintext)]
		copy(buf, plaintext)
		return buf, nil
	}

	if lastError != nil {
		return nil, lastError
	}

	return nil, errors.Errorf("loading %v from %v packs failed", blobs[0].Handle(), len(blobs))
}

func (r *Repository) readCacheAuthoritativeEligibleForBlob(ctx context.Context, blobs []*pack.PackedBlob) bool {
	if !r.readCachePrimaryEligible() {
		return false
	}
	seenPacks := map[string]struct{}{}
	for _, blob := range blobs {
		packID := blob.PackID()
		packKey := packID.String()
		if _, exists := seenPacks[packKey]; exists {
			continue
		}
		seenPacks[packKey] = struct{}{}
		candidates, err := r.placementReadCandidates(ctx, packID)
		if err != nil {
			return false
		}
		if placementCandidatesAuthorizeCache(candidates) {
			return true
		}
	}
	return false
}

func (r *Repository) readCacheAuthoritativeEligibleForLogicalRange(
	ctx context.Context,
	content []vaultic.ID,
	cumSize []uint64,
	offset uint64,
	requestLen int,
) bool {
	if !r.readCachePrimaryEligible() {
		return false
	}
	engine, err := r.legacyIndexEngine()
	if err != nil {
		return false
	}
	indices := referencedLogicalBlobIndices(content, cumSize, offset, requestLen)
	if len(indices) == 0 {
		return false
	}
	for _, idx := range indices {
		if idx < 0 || idx >= len(content) {
			return false
		}
		blobID := content[idx]
		blobs := engine.Lookup(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: blobID})
		if len(blobs) == 0 {
			return false
		}
		authorizedForBlob := false
		seenPacks := map[string]struct{}{}
		for _, blob := range blobs {
			packID := blob.PackID()
			packKey := packID.String()
			if _, exists := seenPacks[packKey]; exists {
				continue
			}
			seenPacks[packKey] = struct{}{}
			candidates, err := r.placementReadCandidates(ctx, packID)
			if err != nil {
				return false
			}
			if placementCandidatesAuthorizeCache(candidates) {
				authorizedForBlob = true
				break
			}
		}
		if !authorizedForBlob {
			return false
		}
	}
	return true
}

func placementCandidatesAuthorizeCache(candidates []placementReadCandidate) bool {
	for _, candidate := range authorizedPlacementReadCandidates(candidates) {
		if candidate.metadataConfirmed {
			return true
		}
	}
	return false
}

func referencedLogicalBlobIndices(content []vaultic.ID, cumSize []uint64, offset uint64, requestLen int) []int {
	if len(content) == 0 || requestLen <= 0 {
		return nil
	}
	if len(cumSize) != len(content)+1 {
		all := make([]int, len(content))
		for i := range content {
			all[i] = i
		}
		return all
	}
	totalSize := cumSize[len(cumSize)-1]
	if offset >= totalSize {
		return nil
	}
	remaining := totalSize - offset
	requestBytes := uint64(requestLen)
	if requestBytes > remaining {
		requestBytes = remaining
	}
	if requestBytes == 0 {
		return nil
	}
	endExclusive := offset + requestBytes
	start := sort.Search(len(cumSize), func(i int) bool { return cumSize[i] > offset }) - 1
	if start < 0 || start >= len(content) {
		return nil
	}
	indices := make([]int, 0, len(content)-start)
	for i := start; i < len(content); i++ {
		blobStart := cumSize[i]
		if blobStart >= endExclusive {
			break
		}
		indices = append(indices, i)
	}
	return indices
}

func (r *Repository) readCachePrimaryEligible() bool {
	model, err := r.PlacementModel()
	if err != nil {
		return true
	}
	hasPrimary := false
	primaryReadable := false
	for _, placement := range model.Backends {
		if placement.Role != PlacementRolePrimary {
			continue
		}
		hasPrimary = true
		if placement.ReadAllowed() {
			primaryReadable = true
			break
		}
	}
	if hasPrimary && !primaryReadable {
		return false
	}
	return true
}

func (r *Repository) decodePackedBlob(blob *pack.PackedBlob, packed []byte) (decodedPackedBlob, error) {
	ciphertextLength := int(blob.Blob.Length)
	if ciphertextLength > len(packed) {
		return decodedPackedBlob{}, fmt.Errorf("readFull: short packed blob")
	}
	packed = packed[:ciphertextLength]
	if int(blob.Blob.Length) <= r.key.NonceSize() {
		return decodedPackedBlob{}, fmt.Errorf("invalid blob length %v", blob.Blob)
	}
	nonce, ciphertext := packed[:r.key.NonceSize()], packed[r.key.NonceSize():]
	decrypted, err := r.key.Open(ciphertext[:0], nonce, ciphertext, nil)
	if err != nil {
		return decodedPackedBlob{}, fmt.Errorf("decrypting blob %v from pack %v failed: %w", blob.Handle(), blob.PackID(), err)
	}
	result := decodedPackedBlob{Plaintext: decrypted}
	if blob.Blob.IsCompressed() {
		result.CompressedSource = append([]byte(nil), decrypted...)
		decoder, decErr := r.getZstdDecoder()
		if decErr != nil {
			return decodedPackedBlob{}, decErr
		}
		result.Plaintext, err = decoder.DecodeAll(decrypted, nil)
		if err != nil {
			return decodedPackedBlob{}, fmt.Errorf("decompressing blob %v from pack %v failed: %w", blob.Handle(), blob.PackID(), err)
		}
	}
	if !vaultic.Hash(result.Plaintext).Equal(blob.Blob.ID) {
		return decodedPackedBlob{}, fmt.Errorf("read blob %v from pack %v: wrong data returned", blob.Handle(), blob.PackID())
	}
	return result, nil
}

func (r *Repository) getZstdEncoder() (*zstd.Encoder, error) {
	r.allocEnc.Do(func() {

		var level zstd.EncoderLevel
		switch r.opts.Compression {
		case CompressionFastest:
			level = zstd.SpeedFastest
		case CompressionBetter:
			level = zstd.SpeedBetterCompression
		case CompressionMax:
			level = zstd.SpeedBestCompression
		default:
			level = zstd.SpeedDefault
		}

		opts := []zstd.EOption{
			// Set the compression level configured.
			zstd.WithEncoderLevel(level),
			// Disable CRC, we have enough checks in place, makes the
			// compressed data four bytes shorter.
			zstd.WithEncoderCRC(false),
			// Set a window of 512kbyte, so we have good lookbehind for usual
			// blob sizes.
			zstd.WithWindowSize(512 * 1024),
		}

		enc, err := zstd.NewWriter(nil, opts...)
		if err != nil {
			r.encErr = fmt.Errorf("initialize zstd encoder: %w", err)
			return
		}
		r.enc = enc
	})
	return r.enc, r.encErr
}

func (r *Repository) getZstdDecoder() (*zstd.Decoder, error) {
	r.allocDec.Do(func() {
		opts := []zstd.DOption{
			// Use all available cores.
			zstd.WithDecoderConcurrency(0),
			// Limit the maximum decompressed memory. Set to a very high,
			// conservative value.
			zstd.WithDecoderMaxMemory(16 * 1024 * 1024 * 1024),
		}

		dec, err := zstd.NewReader(nil, opts...)
		if err != nil {
			r.decErr = fmt.Errorf("initialize zstd decoder: %w", err)
			return
		}
		r.dec = dec
	})
	return r.dec, r.decErr
}

// saveAndEncrypt encrypts data and stores it to the backend as type t. If data
// is small enough, it will be packed together with other small blobs. The
// caller must ensure that the id matches the data. Returned is the size data
// occupies in the repo (compressed or not, including the encryption overhead).
func (r *Repository) saveAndEncrypt(
	ctx context.Context,
	t vaultic.BlobType,
	data []byte,
	id vaultic.ID,
) (size int, err error) {
	debug.Log("save id %v (%v, %d bytes)", id, t, len(data))

	uncompressedLength := 0
	if r.cfg.Version > 1 {
		// we have a repo v2, so compression is available. if the user opts to
		// not compress, we won't compress any data, but everything else is
		// compressed.
		// uncompressedLength != 0 is used to indicate compressed data. Thus, a zero-sized blob
		// cannot be compressed. This special case is only relevant for tests, normal operation does not
		// generate zero-sized blobs.
		if len(data) > 0 && (r.opts.Compression != CompressionOff || t != vaultic.DataBlob) {
			uncompressedLength = len(data)
			encoder, err := r.getZstdEncoder()
			if err != nil {
				return 0, err
			}
			data = encoder.EncodeAll(data, nil)
		}
	}

	nonce := crypto.NewRandomNonce()

	ciphertext := make([]byte, 0, crypto.CiphertextLength(len(data)))
	ciphertext = append(ciphertext, nonce...)

	// encrypt blob
	ciphertext = r.key.Seal(ciphertext, nonce, data, nil)

	if err := r.verifyCiphertext(ciphertext, uncompressedLength, id); err != nil {
		return 0, fmt.Errorf(
			("detected data corruption while saving blob %v: %w\nCorrupted blobs are " +
				"either caused by hardware issues or software bugs. Please open an issue at " +
				"https://github.com/otuschhoff/vaultic/issues/new/choose for further " +
				"troubleshooting"),
			id,
			err,
		)
	}

	// find suitable packer and add blob
	var pm *packerManager

	switch t {
	case vaultic.TreeBlob:
		pm = r.treePM
	case vaultic.DataBlob:
		pm = r.dataPM
	default:
		return 0, fmt.Errorf("%w: %v", ErrInvalidBlobType, t)
	}

	return pm.SaveBlob(ctx, t, id, ciphertext, uncompressedLength)
}

func (r *Repository) verifyCiphertext(buf []byte, uncompressedLength int, id vaultic.ID) error {
	if r.opts.NoExtraVerify {
		return nil
	}

	nonce, ciphertext := buf[:r.key.NonceSize()], buf[r.key.NonceSize():]
	plaintext, err := r.key.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return fmt.Errorf("decryption failed: %w", err)
	}
	if uncompressedLength != 0 {
		// DecodeAll will allocate a slice if it is not large enough since it
		// knows the decompressed size (because we're using EncodeAll)
		decoder, decoderErr := r.getZstdDecoder()
		if decoderErr != nil {
			return decoderErr
		}
		plaintext, err = decoder.DecodeAll(plaintext, nil)
		if err != nil {
			return fmt.Errorf("decompression failed: %w", err)
		}
	}
	if !vaultic.Hash(plaintext).Equal(id) {
		return errors.New("hash mismatch")
	}

	return nil
}

func (r *Repository) compressUnpacked(p []byte) ([]byte, error) {
	// compression is only available starting from version 2
	if r.cfg.Version < 2 {
		return p, nil
	}

	// version byte
	out := []byte{2}
	encoder, err := r.getZstdEncoder()
	if err != nil {
		return nil, err
	}
	out = encoder.EncodeAll(p, out)
	return out, nil
}

func (r *Repository) decompressUnpacked(p []byte) ([]byte, error) {
	// compression is only available starting from version 2
	if r.cfg.Version < 2 {
		return p, nil
	}

	if len(p) == 0 {
		// too short for version header
		return p, nil
	}
	if p[0] == '[' || p[0] == '{' {
		// probably raw JSON
		return p, nil
	}
	// version
	if p[0] != 2 {
		return nil, errors.New("not supported encoding format")
	}

	decoder, err := r.getZstdDecoder()
	if err != nil {
		return nil, err
	}
	return decoder.DecodeAll(p[1:], nil)
}

// SaveUnpacked encrypts data and stores it in the backend. Returned is the
// storage hash.
func (r *Repository) SaveUnpacked(
	ctx context.Context,
	t vaultic.WriteableFileType,
	buf []byte,
) (id vaultic.ID, err error) {
	return r.saveUnpacked(ctx, t.ToFileType(), buf)
}

func (r *internalRepository) SaveUnpacked(
	ctx context.Context,
	t vaultic.FileType,
	buf []byte,
) (id vaultic.ID, err error) {
	return r.Repository.saveUnpacked(ctx, t, buf)
}

func (r *Repository) saveUnpacked(ctx context.Context, t vaultic.FileType, buf []byte) (id vaultic.ID, err error) {
	done := r.accounting.StartBlocking(ctx, "upload", "backend_io")
	defer done.Done()
	p := buf
	if t != vaultic.ConfigFile {
		p, err = r.compressUnpacked(p)
		if err != nil {
			return vaultic.ID{}, err
		}
	}

	ciphertext := crypto.NewBlobBuffer(len(p))
	ciphertext = ciphertext[:0]
	nonce := crypto.NewRandomNonce()
	ciphertext = append(ciphertext, nonce...)

	ciphertext = r.key.Seal(ciphertext, nonce, p, nil)

	if err := r.verifyUnpacked(ciphertext, t, buf); err != nil {
		return vaultic.ID{}, fmt.Errorf(
			("detected data corruption while saving file of type %v: %w\nCorrupted data is " +
				"either caused by hardware issues or software bugs. Please open an issue at " +
				"https://github.com/otuschhoff/vaultic/issues/new/choose for further " +
				"troubleshooting"),
			t,
			err,
		)
	}

	if t == vaultic.ConfigFile {
		id = vaultic.ID{}
	} else {
		id = vaultic.Hash(ciphertext)
	}
	h := backend.Handle{Type: backend.FileType(t), Name: id.String()}
	if t == vaultic.SnapshotFile {
		if authority, ok := r.Engine().(snapshotAuthority); ok {
			if err := authority.MarkSnapshotPending(ctx, id, buf); err != nil {
				return vaultic.ID{}, fmt.Errorf("mark snapshot compatibility export pending: %w", err)
			}
		}
	}

	dependency := r.accounting.StartDependency(ctx, "repository")
	dependency.AddBytes(uint64(len(ciphertext)))
	err = r.be.Save(ctx, h, backend.NewByteReader(ciphertext, r.be.Hasher()))
	dependency.Finish(err)
	if err == nil {
		r.accounting.AddProcessed(ctx, "repository", uint64(len(ciphertext)))
	}
	//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
	if err != nil {
		if t == vaultic.SnapshotFile {
			if authority, ok := r.Engine().(snapshotAuthority); ok {
				if checkpointErr := authority.MarkSnapshotFailed(ctx, id, err); checkpointErr != nil {
					err = errors.Join(err, fmt.Errorf("mark snapshot compatibility export failed: %w", checkpointErr))
				}
			}
		}
		debug.Log("error saving blob %v: %v", h, err)
		return vaultic.ID{}, err
	}

	debug.Log("blob %v saved", h)
	return id, nil
}

func (r *Repository) verifyUnpacked(buf []byte, t vaultic.FileType, expected []byte) error {
	if r.opts.NoExtraVerify {
		return nil
	}

	nonce, ciphertext := buf[:r.key.NonceSize()], buf[r.key.NonceSize():]
	plaintext, err := r.key.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return fmt.Errorf("decryption failed: %w", err)
	}
	if t != vaultic.ConfigFile {
		plaintext, err = r.decompressUnpacked(plaintext)
		if err != nil {
			return fmt.Errorf("decompression failed: %w", err)
		}
	}

	if !bytes.Equal(plaintext, expected) {
		return errors.New("data mismatch")
	}
	return nil
}

func (r *Repository) RemoveUnpacked(ctx context.Context, t vaultic.WriteableFileType, id vaultic.ID) error {
	return r.removeUnpacked(ctx, t.ToFileType(), id)
}

func (r *internalRepository) RemoveUnpacked(ctx context.Context, t vaultic.FileType, id vaultic.ID) error {
	return r.Repository.removeUnpacked(ctx, t, id)
}

func (r *Repository) removeUnpacked(ctx context.Context, t vaultic.FileType, id vaultic.ID) error {
	done := r.accounting.StartBlocking(ctx, "delete", "backend_io")
	defer done.Done()
	dependency := r.accounting.StartDependency(ctx, "repository")
	removeErr := r.be.Remove(ctx, backend.Handle{Type: backend.FileType(t), Name: id.String()})
	dependency.Finish(removeErr)
	if t == vaultic.SnapshotFile {
		if engine, ok := r.Engine().(*enginepkg.DaemonEngine); ok {
			if removeErr != nil && !r.be.IsNotExist(removeErr) {
				return removeErr
			}
			return engine.ForgetSnapshot(ctx, id)
		}
	}
	return removeErr
}
