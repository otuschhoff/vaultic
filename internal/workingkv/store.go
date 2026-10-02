package workingkv

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"

	"github.com/cockroachdb/pebble"
)

var (
	ErrClosed     = errors.New("working KV closed")
	ErrBatchLimit = errors.New("working KV batch exceeds limit")
	ErrInvalid    = errors.New("invalid working KV request")
	ErrBackendIO  = errors.New("working KV backend IO failure")
)

const MaxBatchBytes = 1 << 20
const MaxScanBytes = 8 << 20

type Entry struct{ Key, Value []byte }

type engine interface {
	put(context.Context, []Entry) error
	get([]byte) ([]byte, bool, error)
	scan(context.Context, []byte, []byte, int) ([]Entry, error)
	close() error
}

type Store struct {
	mutex    sync.Mutex
	active   sync.WaitGroup
	once     sync.Once
	done     chan struct{}
	writer   chan struct{}
	engine   engine
	closed   bool
	closeErr error
}

func Open(backend, path string) (*Store, error) {
	if backend != "pebble" && backend != "bbolt" && backend != "badger" {
		return nil, ErrInvalid
	}
	if path == "" {
		return nil, ErrInvalid
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, ErrInvalid
	}
	contents, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	if len(contents) != 0 {
		return nil, ErrInvalid
	}
	var impl engine
	switch backend {
	case "pebble":
		impl, err = openPebble(path)
	case "bbolt":
		impl, err = openBolt(path)
	case "badger":
		impl, err = openBadger(path)
	default:
		return nil, ErrInvalid
	}
	if err != nil {
		return nil, err
	}
	return &Store{engine: impl, writer: make(chan struct{}, 1), done: make(chan struct{})}, nil
}

func (store *Store) Put(ctx context.Context, entries []Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var size uint64
	for _, entry := range entries {
		if len(entry.Key) == 0 || len(entry.Key) > 32768 {
			return ErrInvalid
		}
		size += uint64(len(entry.Key)) + uint64(len(entry.Value)) + 16
		if size > MaxBatchBytes {
			return ErrBatchLimit
		}
	}
	select {
	case store.writer <- struct{}{}:
		defer func() { <-store.writer }()
	case <-ctx.Done():
		return ctx.Err()
	case <-store.done:
		return ErrClosed
	}
	if err := store.admit(ctx); err != nil {
		return err
	}
	defer store.active.Done()
	return backendError(store.engine.put(ctx, entries))
}

func (store *Store) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if len(key) == 0 || len(key) > 32768 {
		return nil, false, ErrInvalid
	}
	if err := store.admit(ctx); err != nil {
		return nil, false, err
	}
	defer store.active.Done()
	value, found, err := store.engine.get(key)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, false, cancelled
	}
	if err != nil {
		return nil, false, backendError(err)
	}
	return value, found, nil
}

func (store *Store) Scan(ctx context.Context, prefix, after []byte, limit int) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 1024 || len(prefix) > 32768 || len(after) > 32768 {
		return nil, ErrInvalid
	}
	if err := store.admit(ctx); err != nil {
		return nil, err
	}
	defer store.active.Done()
	entries, err := store.engine.scan(ctx, prefix, after, limit)
	if cancelled := ctx.Err(); cancelled != nil {
		return nil, cancelled
	}
	if err != nil {
		return nil, backendError(err)
	}
	return entries, nil
}

func (store *Store) Close() error {
	store.once.Do(func() {
		store.mutex.Lock()
		store.closed = true
		if store.done != nil {
			close(store.done)
		}
		store.mutex.Unlock()
		store.active.Wait()
		store.closeErr = backendError(store.engine.close())
	})
	return store.closeErr
}

func (store *Store) admit(ctx context.Context) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if store.closed {
		return ErrClosed
	}
	store.active.Add(1)
	return nil
}

func backendError(err error) error {
	if err == nil || errors.Is(err, ErrBatchLimit) || errors.Is(err, ErrWorkingMemoryLimitExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errors.Join(ErrBackendIO, err)
}

func appendScan(entries []Entry, key, value []byte, size *int) ([]Entry, error) {
	if len(key) > 32768 || len(value) > MaxBatchBytes {
		return nil, ErrBatchLimit
	}
	if len(key)+len(value)+16 > MaxScanBytes-*size {
		return nil, ErrBatchLimit
	}
	*size += len(key) + len(value) + 16
	return append(entries, Entry{Key: bytes.Clone(key), Value: bytes.Clone(value)}), nil
}

type pebbleEngine struct{ database *pebble.DB }

func openPebble(path string) (engine, error) {
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()
	db, err := pebble.Open(path, &pebble.Options{Cache: cache, MemTableSize: 4 << 20, MemTableStopWritesThreshold: 2})
	if err != nil {
		return nil, err
	}
	return &pebbleEngine{db}, nil
}

func (impl *pebbleEngine) put(ctx context.Context, entries []Entry) error {
	batch := impl.database.NewBatch()
	defer batch.Close()
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := batch.Set(entry.Key, entry.Value, nil); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func (impl *pebbleEngine) get(key []byte) ([]byte, bool, error) {
	value, closer, err := impl.database.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if len(value) > MaxBatchBytes {
		return nil, false, errors.Join(ErrBatchLimit, closer.Close())
	}
	return bytes.Clone(value), true, closer.Close()
}

func (impl *pebbleEngine) scan(ctx context.Context, prefix, after []byte, limit int) (entries []Entry, resultErr error) {
	iterator, err := impl.database.NewIter(&pebble.IterOptions{LowerBound: prefix})
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, iterator.Close()) }()
	start := prefix
	if bytes.Compare(after, start) > 0 {
		start = after
	}
	var size int
	for valid := iterator.SeekGE(start); valid && bytes.HasPrefix(iterator.Key(), prefix); valid = iterator.Next() {
		if len(after) > 0 && bytes.Compare(iterator.Key(), after) <= 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entries, err = appendScan(entries, iterator.Key(), iterator.Value(), &size)
		if err != nil {
			return nil, err
		}
		if len(entries) == limit {
			break
		}
	}
	return entries, iterator.Error()
}

func (impl *pebbleEngine) close() error { return impl.database.Close() }
