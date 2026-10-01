package archiver

import (
	"fmt"
	"os"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/otuschhoff/vaultic/internal/telemetry"
)

type MarkerCacheStore struct {
	mutex         sync.Mutex
	database      *pebble.DB
	path          string
	limit         int
	err           error
	metrics       *telemetry.WorkingStateMetric
	cleanupFailed bool
}

func NewMarkerCacheStore(limit int) *MarkerCacheStore {
	return &MarkerCacheStore{limit: max(1, limit), metrics: telemetry.NewWorkingStateMetric(telemetry.WorkingMarkers, "pebble")}
}

func (store *MarkerCacheStore) Error() error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.err
}

func (store *MarkerCacheStore) get(prefix, directory string) (bool, bool) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.err != nil {
		return true, true
	}
	if store.database == nil {
		return false, false
	}
	value, closer, err := store.database.Get([]byte(prefix + "\x00" + directory))
	if err == pebble.ErrNotFound {
		return false, false
	}
	if err != nil {
		store.err = fmt.Errorf("read marker cache: %w", err)
		return true, true
	}
	defer closer.Close()
	if len(value) != 1 || value[0] > 1 {
		store.err = fmt.Errorf("invalid marker cache decision")
		return true, true
	}
	return value[0] == 1, true
}

func (store *MarkerCacheStore) spill(prefix string, values map[string]bool) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.err != nil {
		return
	}
	if store.database == nil {
		store.path, store.err = os.MkdirTemp("", "vaultic-markers-")
		if store.err != nil {
			store.err = fmt.Errorf("create marker cache: %w", store.err)
			return
		}
		cache := pebble.NewCache(8 << 20)
		store.database, store.err = pebble.Open(store.path, &pebble.Options{Cache: cache, MemTableSize: 4 << 20, MemTableStopWritesThreshold: 2})
		cache.Unref()
		if store.err != nil {
			store.err = fmt.Errorf("open marker cache: %w", store.err)
			return
		}
		store.metrics.Activate()
	}
	batch := store.database.NewBatch()
	defer batch.Close()
	defer store.metrics.ObserveBuffer(0)
	var encodedBytes uint64
	for directory, rejected := range values {
		value := byte(0)
		if rejected {
			value = 1
		}
		if err := batch.Set([]byte(prefix+"\x00"+directory), []byte{value}, nil); err != nil {
			store.err = fmt.Errorf("encode marker cache: %w", err)
			return
		}
		encodedBytes += uint64(len(prefix) + 1 + len(directory) + 1)
	}
	store.metrics.ObserveBuffer(uint64(len(batch.Repr())))
	if err := batch.Commit(pebble.NoSync); err != nil {
		store.err = fmt.Errorf("write marker cache: %w", err)
		return
	}
	store.metrics.Committed(uint64(len(values)), encodedBytes)
}

func (store *MarkerCacheStore) WorkingState() telemetry.WorkingStateSnapshot {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.database == nil {
		return store.metrics.Snapshot(0, store.path == "" && !store.cleanupFailed)
	}
	return store.metrics.Snapshot(store.database.Metrics().DiskSpaceUsage(), true)
}

func (store *MarkerCacheStore) Close() error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	var err error
	if store.database != nil {
		err = store.database.Close()
		store.database = nil
	}
	if store.path != "" {
		removeErr := os.RemoveAll(store.path)
		store.cleanupFailed = removeErr != nil
		if err == nil {
			err = removeErr
		}
		store.path = ""
	}
	store.metrics.Close()
	return err
}
