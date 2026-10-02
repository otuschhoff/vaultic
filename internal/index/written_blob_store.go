package index

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/workingkv"
)

type writtenBlobStore struct {
	mutex         sync.Mutex
	database      *workingkv.WorkingStore
	path          string
	cipher        cipher.AEAD
	tokenKey      []byte
	err           error
	metrics       *telemetry.WorkingStateMetric
	cleanupFailed bool
}

func newWrittenBlobStore(directory string) (*writtenBlobStore, error) {
	return newWrittenBlobStoreContext(context.Background(), directory)
}

func newWrittenBlobStoreContext(ctx context.Context, directory string) (*writtenBlobStore, error) {
	if directory == "" && workingkv.PolicyFrom(ctx) == nil {
		return nil, fmt.Errorf("write overlay requires a scratch directory")
	}
	key := make([]byte, 64)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key[:32])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	database, err := workingkv.OpenWorking(ctx, directory, "vaultic-written-")
	if err != nil {
		return nil, err
	}
	metrics := telemetry.NewWorkingStateMetric(telemetry.WorkingWrittenBlobs, database.Backend())
	metrics.Activate()
	return &writtenBlobStore{database: database, path: database.Path(), cipher: aead, tokenKey: key[32:], metrics: metrics}, nil
}

func (store *writtenBlobStore) token(value []byte) []byte {
	mac := hmac.New(sha256.New, store.tokenKey)
	_, _ = mac.Write(value)
	return mac.Sum(nil)
}

func (store *writtenBlobStore) prefix(handle vaultic.BlobHandle) []byte {
	var value [33]byte
	value[0] = byte(handle.Type)
	copy(value[1:], handle.ID[:])
	return store.token(value[:])
}

func (store *writtenBlobStore) Error() error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.err
}

func (store *writtenBlobStore) StoreIndex(ctx context.Context, index *legacyindex.Index) (resultErr error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.err != nil {
		return store.err
	}
	defer func() {
		if resultErr != nil {
			store.err = fmt.Errorf("write overlay: %w", resultErr)
		}
	}()
	var batch []workingkv.Entry
	size := 0
	defer store.metrics.ObserveBuffer(0)
	var entries, encodedBytes uint64
	commit := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := store.database.Put(ctx, batch); err != nil {
			return err
		}
		store.metrics.Committed(entries, encodedBytes)
		batch = batch[:0]
		size = 0
		entries = 0
		encodedBytes = 0
		return nil
	}
	for blob := range index.Values() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var encoded [56]byte
		copy(encoded[:32], blob.Pack[:])
		binary.LittleEndian.PutUint64(encoded[32:40], uint64(blob.Blob.Offset))
		binary.LittleEndian.PutUint64(encoded[40:48], uint64(blob.Blob.Length))
		binary.LittleEndian.PutUint64(encoded[48:56], uint64(blob.Blob.UncompressedLength))
		key := append(store.prefix(blob.Handle()), store.token(encoded[:40])...)
		nonce := make([]byte, store.cipher.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return err
		}
		value := store.cipher.Seal(nonce, nonce, encoded[:], key)
		if size+len(key)+len(value)+16 > workingkv.MaxBatchBytes {
			if err := commit(); err != nil {
				return err
			}
		}
		batch = append(batch, workingkv.Entry{Key: key, Value: value})
		size += len(key) + len(value) + 16
		entries++
		encodedBytes += uint64(len(key) + len(value))
		store.metrics.ObserveBuffer(uint64(size))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return commit()
}

func (store *writtenBlobStore) WorkingState() telemetry.WorkingStateSnapshot {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.database == nil {
		return store.metrics.Snapshot(0, !store.cleanupFailed)
	}
	size, known := store.database.ScratchBytes()
	return store.metrics.Snapshot(size, known)
}

func (store *writtenBlobStore) lookup(ctx context.Context, handle vaultic.BlobHandle, firstOnly bool) (blobs []*pack.PackedBlob, resultErr error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.err != nil {
		return nil, store.err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil && ctx.Err() == nil {
			store.err = fmt.Errorf("read overlay: %w", resultErr)
		}
	}()
	prefix := store.prefix(handle)
	var after []byte
	for {
		limit := 128
		if firstOnly {
			limit = 1
		}
		rows, err := store.database.Scan(ctx, prefix, after, limit)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			value := row.Value
			nonceSize := store.cipher.NonceSize()
			if len(value) < nonceSize {
				return nil, fmt.Errorf("truncated encrypted overlay entry")
			}
			decoded, err := store.cipher.Open(nil, value[:nonceSize], value[nonceSize:], row.Key)
			if err != nil {
				return nil, err
			}
			if len(decoded) != 56 {
				return nil, fmt.Errorf("invalid overlay entry length")
			}
			blob := &pack.PackedBlob{Blob: pack.Blob{BlobHandle: handle}}
			copy(blob.Pack[:], decoded[:32])
			blob.Blob.Offset = uint(binary.LittleEndian.Uint64(decoded[32:40]))
			blob.Blob.Length = uint(binary.LittleEndian.Uint64(decoded[40:48]))
			blob.Blob.UncompressedLength = uint(binary.LittleEndian.Uint64(decoded[48:56]))
			blobs = append(blobs, blob)
			if firstOnly {
				return blobs, nil
			}
		}
		after = rows[len(rows)-1].Key
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return blobs, nil
}

func (store *writtenBlobStore) Close() error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.database == nil {
		return nil
	}
	err := store.database.Close()
	store.database = nil
	store.err = fmt.Errorf("write overlay is closed")
	store.cipher = nil
	clear(store.tokenKey)
	store.metrics.Close()
	removeErr := os.RemoveAll(store.path)
	store.cleanupFailed = removeErr != nil
	return errors.Join(err, removeErr)
}
