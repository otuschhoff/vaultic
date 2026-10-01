package workingkv

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	bolt "go.etcd.io/bbolt"
)

var bucket = []byte("working-state")

type boltEngine struct{ database *bolt.DB }

func openBolt(path string) (engine, error) {
	db, err := bolt.Open(filepath.Join(path, "state.db"), 0600, &bolt.Options{Timeout: time.Second, NoSync: true, NoFreelistSync: true})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error { _, err := tx.CreateBucketIfNotExists(bucket); return err }); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &boltEngine{db}, nil
}

func (impl *boltEngine) put(ctx context.Context, entries []Entry) error {
	return impl.database.Update(func(tx *bolt.Tx) error {
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := tx.Bucket(bucket).Put(entry.Key, entry.Value); err != nil {
				return err
			}
		}
		return ctx.Err()
	})
}

func (impl *boltEngine) get(key []byte) (value []byte, found bool, resultErr error) {
	resultErr = impl.database.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucket).Cursor()
		storedKey, storedValue := cursor.Seek(key)
		found = storedKey != nil && bytes.Equal(storedKey, key)
		if found {
			if len(storedValue) > MaxBatchBytes {
				return ErrBatchLimit
			}
			value = bytes.Clone(storedValue)
		}
		return nil
	})
	return
}

func (impl *boltEngine) scan(ctx context.Context, prefix, after []byte, limit int) (entries []Entry, resultErr error) {
	resultErr = impl.database.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucket).Cursor()
		start := prefix
		if bytes.Compare(after, start) > 0 {
			start = after
		}
		var size int
		for key, value := cursor.Seek(start); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
			if len(after) > 0 && bytes.Compare(key, after) <= 0 {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			var err error
			entries, err = appendScan(entries, key, value, &size)
			if err != nil {
				return err
			}
			if len(entries) == limit {
				break
			}
		}
		return nil
	})
	return
}

func (impl *boltEngine) close() error { return impl.database.Close() }

type badgerEngine struct{ database *badger.DB }

func openBadger(path string) (engine, error) {
	config := badger.DefaultOptions(path).WithLogger(nil).WithSyncWrites(false).
		WithMemTableSize(16 << 20).WithNumMemtables(2).WithNumLevelZeroTables(2).
		WithNumLevelZeroTablesStall(4).WithBaseTableSize(2 << 20).WithBaseLevelSize(8 << 20).
		WithBlockCacheSize(8 << 20).WithIndexCacheSize(8 << 20).
		WithValueLogFileSize(16 << 20).WithValueThreshold(1024).WithCompression(options.None)
	db, err := badger.Open(config)
	if err != nil {
		return nil, err
	}
	return &badgerEngine{db}, nil
}

func (impl *badgerEngine) put(ctx context.Context, entries []Entry) error {
	return impl.database.Update(func(tx *badger.Txn) error {
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := tx.Set(entry.Key, entry.Value); err != nil {
				return err
			}
		}
		return ctx.Err()
	})
}

func (impl *badgerEngine) get(key []byte) (value []byte, found bool, resultErr error) {
	resultErr = impl.database.View(func(tx *badger.Txn) error {
		item, err := tx.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if item.ValueSize() > MaxBatchBytes {
			return ErrBatchLimit
		}
		value, err = item.ValueCopy(nil)
		found = err == nil
		return err
	})
	return
}

func (impl *badgerEngine) scan(ctx context.Context, prefix, after []byte, limit int) (entries []Entry, resultErr error) {
	resultErr = impl.database.View(func(tx *badger.Txn) error {
		config := badger.DefaultIteratorOptions
		config.PrefetchValues = false
		config.Prefix = prefix
		iterator := tx.NewIterator(config)
		defer iterator.Close()
		start := prefix
		if bytes.Compare(after, start) > 0 {
			start = after
		}
		var size int
		for iterator.Seek(start); iterator.ValidForPrefix(prefix); iterator.Next() {
			item := iterator.Item()
			if len(after) > 0 && bytes.Compare(item.Key(), after) <= 0 {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if item.ValueSize() > MaxBatchBytes || item.ValueSize() > int64(MaxScanBytes-size-len(item.Key())-16) {
				return ErrBatchLimit
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			entries, err = appendScan(entries, item.Key(), value, &size)
			if err != nil {
				return err
			}
			if len(entries) == limit {
				break
			}
		}
		return nil
	})
	return
}

func (impl *badgerEngine) close() error { return impl.database.Close() }
