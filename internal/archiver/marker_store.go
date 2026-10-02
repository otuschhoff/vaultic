package archiver

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/workingkv"
)

type MarkerCacheStore struct {
	mutex         sync.Mutex
	database      *workingkv.SecureMap
	ctx           context.Context
	path          string
	limit         int
	err           error
	metrics       *telemetry.WorkingStateMetric
	cleanupFailed bool
}

func NewMarkerCacheStore(limit int) *MarkerCacheStore {
	return NewMarkerCacheStoreContext(context.Background(), limit)
}

func NewMarkerCacheStoreContext(ctx context.Context, limit int) *MarkerCacheStore {
	backend := "pebble"
	if policy := workingkv.PolicyFrom(ctx); policy != nil {
		backend = "bbolt"
		if policy.Mode() == workingkv.ModeRAM {
			backend = "ram"
		}
	}
	return &MarkerCacheStore{ctx: ctx, limit: max(1, limit), metrics: telemetry.NewWorkingStateMetric(telemetry.WorkingMarkers, backend)}
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
	value, found, err := store.database.GetValue(store.ctx, []byte(prefix+"\x00"+directory))
	if err == nil && !found {
		return false, false
	}
	if err != nil {
		store.err = fmt.Errorf("read marker cache: %w", err)
		return true, true
	}
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
		store.database, store.err = workingkv.OpenSecureMap(store.ctx, "", "vaultic-markers-")
		if store.err != nil {
			store.err = fmt.Errorf("open marker cache: %w", store.err)
			return
		}
		store.metrics.Activate()
		store.path = store.database.Path()
	}
	defer store.metrics.ObserveBuffer(0)
	for directory, rejected := range values {
		value := byte(0)
		if rejected {
			value = 1
		}
		if err := store.database.PutValue(store.ctx, []byte(prefix+"\x00"+directory), []byte{value}); err != nil {
			store.err = fmt.Errorf("encode marker cache: %w", err)
			return
		}
		encodedBytes := uint64(len(prefix) + 1 + len(directory) + 1)
		store.metrics.ObserveBuffer(encodedBytes)
		store.metrics.Committed(1, encodedBytes)
	}
}

func (store *MarkerCacheStore) WorkingState() telemetry.WorkingStateSnapshot {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.database == nil {
		return store.metrics.Snapshot(0, store.path == "" && !store.cleanupFailed)
	}
	size, known := store.database.ScratchBytes()
	return store.metrics.Snapshot(size, known)
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
