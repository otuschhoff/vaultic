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

	"github.com/cockroachdb/pebble"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type writtenBlobStore struct {
	mutex         sync.Mutex
	database      *pebble.DB
	path          string
	cipher        cipher.AEAD
	tokenKey      []byte
	err           error
	metrics       *telemetry.WorkingStateMetric
	cleanupFailed bool
}

func newWrittenBlobStore(directory string) (*writtenBlobStore, error) {
	if directory == "" {
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
	path, err := os.MkdirTemp(directory, "vaultic-written-")
	if err != nil {
		return nil, err
	}
	cache := pebble.NewCache(8 << 20)
	database, err := pebble.Open(path, &pebble.Options{Cache: cache, MemTableSize: 4 << 20, MemTableStopWritesThreshold: 2})
	cache.Unref()
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(path))
	}
	metrics := telemetry.NewWorkingStateMetric(telemetry.WorkingWrittenBlobs, "pebble")
	metrics.Activate()
	return &writtenBlobStore{database: database, path: path, cipher: aead, tokenKey: key[32:], metrics: metrics}, nil
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
	batch := store.database.NewBatch()
	defer batch.Close()
	defer store.metrics.ObserveBuffer(0)
	var entries, encodedBytes uint64
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
		if err := batch.Set(key, value, nil); err != nil {
			return err
		}
		entries++
		encodedBytes += uint64(len(key) + len(value))
		store.metrics.ObserveBuffer(uint64(len(batch.Repr())))
		if len(batch.Repr()) >= 1<<20 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				return err
			}
			store.metrics.Committed(entries, encodedBytes)
			entries, encodedBytes = 0, 0
			batch.Reset()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	store.metrics.Committed(entries, encodedBytes)
	return nil
}

func (store *writtenBlobStore) WorkingState() telemetry.WorkingStateSnapshot {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.database == nil {
		return store.metrics.Snapshot(0, !store.cleanupFailed)
	}
	return store.metrics.Snapshot(store.database.Metrics().DiskSpaceUsage(), true)
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
	upper := append([]byte(nil), prefix...)
	for ordinal := len(upper) - 1; ordinal >= 0; ordinal-- {
		upper[ordinal]++
		if upper[ordinal] != 0 {
			upper = upper[:ordinal+1]
			break
		}
		if ordinal == 0 {
			upper = nil
		}
	}
	iterator, err := store.database.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, iterator.Close())
		if resultErr != nil {
			blobs = nil
		}
	}()
	for valid := iterator.First(); valid; valid = iterator.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value := iterator.Value()
		nonceSize := store.cipher.NonceSize()
		if len(value) < nonceSize {
			return nil, fmt.Errorf("truncated encrypted overlay entry")
		}
		decoded, err := store.cipher.Open(nil, value[:nonceSize], value[nonceSize:], iterator.Key())
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
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return blobs, iterator.Error()
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
