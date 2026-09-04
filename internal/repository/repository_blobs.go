package repository

import (
	"bytes"
	"context"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func (r *Repository) loadBlob(ctx context.Context, blobs []*pack.PackedBlob, buf []byte) ([]byte, error) {
	var lastError error
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

		decoder, err := r.getZstdDecoder()
		if err != nil {
			return nil, err
		}
		it := newPackBlobIterator(
			blob.PackID(),
			newByteReader(buf),
			blob.Blob.Offset,
			pack.Blobs{blob.Blob},
			r.key,
			decoder,
		)
		pbv, err := it.Next()

		if err == nil {
			err = pbv.Err
		}
		if err != nil {
			debug.Log("error decoding blob %v: %v", blob, err)
			lastError = err
			continue
		}

		plaintext := pbv.Plaintext
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
		//nolint:revive,staticcheck // ignore linter warnings about error message spelling
		return 0, fmt.Errorf(
			("Detected data corruption while saving blob %v: %w\nCorrupted blobs are " +
				"either caused by hardware issues or software bugs. Please open an issue at " +
				"https://github.com/otuschhoff/vaultic/issues/new/choose for further " +
				"troubleshooting."),
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
		//nolint:revive,staticcheck // ignore linter warnings about error message spelling
		return vaultic.ID{}, fmt.Errorf(
			("Detected data corruption while saving file of type %v: %w\nCorrupted data is " +
				"either caused by hardware issues or software bugs. Please open an issue at " +
				"https://github.com/otuschhoff/vaultic/issues/new/choose for further " +
				"troubleshooting."),
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

	err = r.be.Save(ctx, h, backend.NewByteReader(ciphertext, r.be.Hasher()))
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
	removeErr := r.be.Remove(ctx, backend.Handle{Type: backend.FileType(t), Name: id.String()})
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
