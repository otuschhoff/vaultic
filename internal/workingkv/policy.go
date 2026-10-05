package workingkv

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/otuschhoff/vaultic/internal/workingstate"
	boltErrors "go.etcd.io/bbolt/errors"
)

type Mode = workingstate.Mode

const (
	ModeRAM  = workingstate.ModeRAM
	ModeKV   = workingstate.ModeKV
	ModeAuto = workingstate.ModeAuto
)

var (
	ErrWorkingScratchLimitExceeded = errors.New("working scratch limit exceeded")
	ErrWorkingPolicyConflict       = errors.New("working policy conflict")
	ErrWorkingStateCorrupt         = errors.New("working state corrupt")
	ErrWorkingMigrationFailed      = errors.New("working migration failed")
)

type Policy struct {
	mode            Mode
	budget          *Budget
	scratch         string
	mutex           sync.Mutex
	cacheMutex      sync.Mutex
	cacheFiles      map[*os.File]struct{}
	migration       contextMutex
	headroom        uint64
	stores          int
	fallback        bool
	disk            *Budget
	ramStores       int
	configured      uint64
	resolution      Resolution
	actions         uint32
	events          uint64
	pressure        bool
	ceiling         uint64
	recoveryLimit   uint64
	recoverySamples uint8
	evictors        [4]func()
	report          func(PolicyEvent)
}

func NewPolicy(mode Mode, limit uint64, scratch string) (*Policy, error) {
	if mode != ModeRAM && mode != ModeKV && mode != ModeAuto {
		return nil, ErrInvalid
	}
	if mode == ModeAuto && scratch == "" {
		return nil, ErrWorkingPolicyConflict
	}
	budget, err := NewBudget(limit)
	if err != nil {
		return nil, err
	}
	disk, _ := NewBudget(8 << 30)
	return &Policy{mode: mode, budget: budget, scratch: scratch, disk: disk, configured: limit, ceiling: limit}, nil
}

func (policy *Policy) Mode() Mode                { return policy.mode }
func (policy *Policy) Budget() BudgetSnapshot    { return policy.budget.Snapshot() }
func (policy *Policy) ScratchDirectory() string  { return policy.scratch }
func (policy *Policy) Reserve(size uint64) error { return policy.budget.reserve(size) }
func (policy *Policy) Release(size uint64)       { policy.budget.release(size) }

func WithPolicy(ctx context.Context, policy *Policy) context.Context {
	if policy == nil {
		return workingstate.WithPolicy(ctx, nil)
	}
	return workingstate.WithPolicy(ctx, policy)
}

func PolicyFrom(ctx context.Context) *Policy {
	policy, _ := workingstate.PolicyFrom(ctx).(*Policy)
	return policy
}

type WorkingStore struct {
	store          *Store
	mutex          contextMutex
	policy         *Policy
	parent, prefix string
	closed         bool
	registered     bool
	mapLimit       uint64
	memoryCharge   uint64
	diskCharge     uint64
	encoded        uint64
	fault          func(string) error
	verify         func([]byte, []byte) error
	path           string
	cacheFile      *os.File
	backend        string
	closeErr       error
	once           sync.Once
}

type contextMutex struct {
	once sync.Once
	gate chan struct{}
}

func (mutex *contextMutex) LockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mutex.once.Do(func() { mutex.gate = make(chan struct{}, 1) })
	select {
	case mutex.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-mutex.gate
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (mutex *contextMutex) Lock()   { _ = mutex.LockContext(context.Background()) }
func (mutex *contextMutex) Unlock() { <-mutex.gate }

func (store *WorkingStore) unlockOperation() {
	policy := store.policy
	store.mutex.Unlock()
	if policy != nil && policy.mode == ModeAuto {
		policy.mutex.Lock()
		fallback := policy.fallback
		policy.mutex.Unlock()
		if fallback {
			policy.emit("fallback", "verified_handoff", 1<<8)
		}
	}
}

func OpenWorking(ctx context.Context, parent, prefix string) (*WorkingStore, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	policy := PolicyFrom(ctx)
	if policy != nil && (policy.mode == ModeRAM || policy.mode == ModeAuto) {
		policy.mutex.Lock()
		defer policy.mutex.Unlock()
		if policy.mode == ModeAuto && policy.fallback {
			store, err := openDiskWorking(policy, parent, prefix)
			if err == nil {
				policy.stores++
				store.registered = true
			}
			return store, err
		}
		if policy.mode == ModeAuto && policy.stores == 0 {
			policy.headroom = policy.budget.Snapshot().Limit / 2
			if err := policy.budget.reserve(policy.headroom); err != nil {
				return nil, err
			}
		}
		store, err := OpenRAM(policy.budget)
		if err != nil {
			if policy.stores == 0 && policy.headroom != 0 {
				policy.budget.release(policy.headroom)
				policy.headroom = 0
			}
			return nil, err
		}
		policy.stores++
		if policy.mode == ModeAuto {
			policy.ramStores++
		}
		return &WorkingStore{store: store, policy: policy, parent: parent, prefix: prefix, registered: policy.mode == ModeAuto, backend: "ram"}, nil
	}
	return openDiskWorking(policy, parent, prefix)
}

func openDiskWorking(policy *Policy, parent, prefix string) (*WorkingStore, error) {
	backend := "pebble"
	if policy != nil {
		backend = "bbolt"
		if policy.scratch != "" {
			parent = policy.scratch
		}
	}
	var memoryCharge, diskCharge uint64
	if policy != nil {
		memoryCharge = ramCharge((64 << 10) + (32 << 10))
		diskCharge = 64 << 10
		if err := policy.budget.reserve(memoryCharge); err != nil {
			return nil, err
		}
		if err := policy.ReserveDisk(diskCharge); err != nil {
			policy.budget.release(memoryCharge)
			return nil, err
		}
	}
	rollback := func() {
		if policy != nil {
			policy.budget.release(memoryCharge)
			policy.disk.release(diskCharge)
		}
	}
	path, err := os.MkdirTemp(parent, prefix)
	if err != nil {
		rollback()
		return nil, err
	}
	mapLimit := 0
	if policy != nil {
		mapLimit = 64 << 10
	}
	store, err := openLimited(backend, path, mapLimit)
	if err != nil {
		removeErr := os.RemoveAll(path)
		if policy != nil {
			policy.budget.release(memoryCharge)
			if removeErr == nil {
				policy.ReleaseDisk(diskCharge)
			}
		}
		return nil, errors.Join(err, removeErr)
	}
	result := &WorkingStore{store: store, policy: policy, parent: parent, prefix: prefix, path: path, backend: backend, memoryCharge: memoryCharge, diskCharge: diskCharge}
	if policy != nil {
		result.mapLimit = 64 << 10
		engine := store.engine.(*boltEngine)
		engine.database.MaxSize = int(result.mapLimit)
		engine.database.AllocSize = 4096
		if file, err := os.Open(filepath.Join(path, "state.db")); err == nil {
			result.cacheFile = file
			policy.trackCacheFile(file)
		}
	}
	return result, nil
}

func (store *WorkingStore) Backend() string {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.backend
}
func (store *WorkingStore) Path() string {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.path
}

func (store *WorkingStore) SetVerifier(verify func([]byte, []byte) error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.verify = verify
}

func (store *WorkingStore) reserveCopies(ctx context.Context, charge uint64) (func(), error) {
	if err := store.mutex.LockContext(ctx); err != nil {
		return nil, err
	}
	defer store.unlockOperation()
	if store.closed {
		return nil, ErrClosed
	}
	if store.shouldMigrate(charge + ramCharge(ramChunkBytes)) {
		if err := store.migrate(ctx); err != nil {
			return nil, err
		}
	}
	if store.policy == nil {
		return func() {}, nil
	}
	if err := store.policy.Reserve(charge); err != nil {
		return nil, err
	}
	return func() { store.policy.Release(charge) }, nil
}

func (store *WorkingStore) migrate(ctx context.Context) error {
	policy := store.policy
	if err := policy.migration.LockContext(ctx); err != nil {
		return err
	}
	defer policy.migration.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	state := policy.Budget()
	if state.Used > state.Limit {
		return &MemoryLimitError{state.Limit, state.Used, 0}
	}
	if store.fault != nil {
		if err := store.fault("open"); err != nil {
			return errors.Join(ErrWorkingMigrationFailed, err)
		}
	}
	policy.mutex.Lock()
	headroom := policy.headroom
	policy.mutex.Unlock()
	if headroom == 0 {
		return errors.Join(ErrWorkingMigrationFailed, ErrWorkingMemoryLimitExceeded)
	}
	transferBudget, _ := NewBudget(headroom)
	transferPolicy := &Policy{mode: ModeKV, budget: transferBudget, disk: policy.disk, scratch: policy.scratch}
	destination, err := openDiskWorking(transferPolicy, store.parent, store.prefix)
	if err != nil {
		return errors.Join(ErrWorkingMigrationFailed, err)
	}
	destination.fault = store.fault
	err = store.store.engine.(*ramEngine).visit(ctx, func(key, value []byte) error {
		if ramCharge(uint64(len(key)+len(value))*2+4096) > policy.headroom {
			return &MemoryLimitError{policy.Budget().Limit, policy.Budget().Used, ramCharge(uint64(len(key)+len(value))*2 + 4096)}
		}
		if store.fault != nil {
			if err := store.fault("copy"); err != nil {
				return err
			}
		}
		if store.verify != nil {
			charge := ramCharge(uint64(len(key)+len(value)) + 64)
			if err := transferPolicy.Reserve(charge); err != nil {
				return err
			}
			err := store.verify(key, value)
			transferPolicy.Release(charge)
			if err != nil {
				return errors.Join(ErrWorkingStateCorrupt, err)
			}
		}
		if err := destination.Put(ctx, []Entry{{key, value}}); err != nil {
			return err
		}
		if store.fault != nil {
			if err := store.fault("verify"); err != nil {
				return err
			}
		}
		actual, found, err := destination.Get(ctx, key)
		if err != nil {
			return err
		}
		if !found || !bytes.Equal(actual, value) {
			return ErrWorkingStateCorrupt
		}
		return nil
	})
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return errors.Join(ErrWorkingMigrationFailed, err, destination.Close())
	}
	if store.fault != nil {
		if err := store.fault("handoff"); err != nil {
			return errors.Join(ErrWorkingMigrationFailed, err, destination.Close())
		}
	}
	old := store.store
	store.store, store.path, store.backend = destination.store, destination.path, destination.backend
	if destination.cacheFile != nil {
		transferPolicy.untrackCacheFile(destination.cacheFile)
		policy.trackCacheFile(destination.cacheFile)
		store.cacheFile = destination.cacheFile
	}
	policy.mutex.Lock()
	policy.headroom -= destination.memoryCharge
	store.mapLimit, store.memoryCharge, store.diskCharge = destination.mapLimit, destination.memoryCharge, destination.diskCharge
	transferBudget.release(destination.memoryCharge)
	policy.ramStores--
	policy.fallback = true
	err = old.Close()
	if policy.ramStores == 0 {
		policy.budget.release(policy.headroom)
		policy.headroom = 0
	} else if policy.budget.reserve(destination.memoryCharge) == nil {
		policy.headroom += destination.memoryCharge
	}
	policy.mutex.Unlock()
	return err
}

func (store *WorkingStore) shouldMigrate(growth uint64) bool {
	if store.policy == nil || store.policy.mode != ModeAuto || store.backend != "ram" {
		return false
	}
	policy := store.policy
	policy.mutex.Lock()
	defer policy.mutex.Unlock()
	state := policy.Budget()
	return policy.fallback || policy.pressure || store.encoded >= policy.headroom/8 || state.Used >= state.Limit-state.Limit*15/100 || state.Used > state.Limit || growth > state.Limit-state.Used
}

func (store *WorkingStore) Put(ctx context.Context, entries []Entry) error {
	if err := store.mutex.LockContext(ctx); err != nil {
		return err
	}
	defer store.unlockOperation()
	if store.closed {
		return ErrClosed
	}
	var growth uint64
	for _, entry := range entries {
		if len(entry.Key) == 0 || len(entry.Key) > 32768 {
			return ErrInvalid
		}
		growth += uint64(len(entry.Key) + len(entry.Value) + 16)
		if growth > MaxBatchBytes {
			return ErrBatchLimit
		}
	}
	if store.shouldMigrate(ramCharge(growth) + ramCharge(ramChunkBytes)) {
		if err := store.migrate(ctx); err != nil {
			return err
		}
	}
	err := store.putLocked(ctx, entries, growth)
	if errors.Is(err, ErrWorkingMemoryLimitExceeded) && store.policy != nil && store.policy.mode == ModeAuto && store.backend == "ram" {
		if migrationErr := store.migrate(ctx); migrationErr != nil {
			return migrationErr
		}
		return store.putLocked(ctx, entries, growth)
	}
	if err == nil && store.backend == "ram" {
		store.encoded += growth
	}
	return err
}

func (store *WorkingStore) putLocked(ctx context.Context, entries []Entry, size uint64) error {
	if store.policy == nil || store.backend != "bbolt" {
		return store.store.Put(ctx, entries)
	}
	charge := ramCharge(size*2 + 4096)
	if err := store.policy.Reserve(charge); err != nil {
		return err
	}
	defer store.policy.Release(charge)
	for {
		err := store.store.Put(ctx, entries)
		if !errors.Is(err, boltErrors.ErrMaxSizeReached) {
			return err
		}
		if err := store.growLocked(); err != nil {
			return err
		}
	}
}

func (store *WorkingStore) growLocked() error {
	next := store.mapLimit * 2
	if store.mapLimit >= 1<<30 {
		next = store.mapLimit + 1<<30
	}
	if next > uint64(^uint(0)>>1) {
		return ErrWorkingScratchLimitExceeded
	}
	memoryDelta := ramCharge(next+(32<<10)) - store.memoryCharge
	diskDelta := next - store.diskCharge
	if err := store.policy.budget.reserve(memoryDelta); err != nil {
		return err
	}
	if err := store.policy.ReserveDisk(diskDelta); err != nil {
		store.policy.budget.release(memoryDelta)
		return err
	}
	store.mapLimit = next
	store.memoryCharge += memoryDelta
	store.diskCharge += diskDelta
	store.store.engine.(*boltEngine).database.MaxSize = int(next)
	return nil
}

func (store *WorkingStore) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if err := store.mutex.LockContext(ctx); err != nil {
		return nil, false, err
	}
	defer store.unlockOperation()
	if store.closed {
		return nil, false, ErrClosed
	}
	if len(key) == 0 || len(key) > 32768 {
		return nil, false, ErrInvalid
	}
	if store.shouldMigrate(0) {
		if err := store.migrate(ctx); err != nil {
			return nil, false, err
		}
	}
	value, found, err := store.getLocked(ctx, key)
	if errors.Is(err, ErrWorkingMemoryLimitExceeded) && store.policy != nil && store.policy.mode == ModeAuto && store.backend == "ram" {
		if err := store.migrate(ctx); err != nil {
			return nil, false, err
		}
		return store.getLocked(ctx, key)
	}
	return value, found, err
}

func (store *WorkingStore) getLocked(ctx context.Context, key []byte) ([]byte, bool, error) {
	if store.policy == nil || store.backend != "bbolt" {
		return store.store.Get(ctx, key)
	}
	size, err := store.store.engine.(*boltEngine).copySize(ctx, key, nil, 1, true)
	if err != nil {
		return nil, false, err
	}
	charge := ramCharge(size)
	if err := store.policy.Reserve(charge); err != nil {
		return nil, false, err
	}
	defer store.policy.Release(charge)
	return store.store.Get(ctx, key)
}

func (store *WorkingStore) Scan(ctx context.Context, prefix, after []byte, limit int) ([]Entry, error) {
	if err := store.mutex.LockContext(ctx); err != nil {
		return nil, err
	}
	defer store.unlockOperation()
	if store.closed {
		return nil, ErrClosed
	}
	if limit < 1 || limit > 1024 || len(prefix) > 32768 || len(after) > 32768 {
		return nil, ErrInvalid
	}
	if store.shouldMigrate(0) {
		if err := store.migrate(ctx); err != nil {
			return nil, err
		}
	}
	rows, err := store.scanLocked(ctx, prefix, after, limit)
	if errors.Is(err, ErrWorkingMemoryLimitExceeded) && store.policy != nil && store.policy.mode == ModeAuto && store.backend == "ram" {
		if err := store.migrate(ctx); err != nil {
			return nil, err
		}
		return store.scanLocked(ctx, prefix, after, limit)
	}
	return rows, err
}

func (store *WorkingStore) scanLocked(ctx context.Context, prefix, after []byte, limit int) ([]Entry, error) {
	if store.policy == nil || store.backend != "bbolt" {
		return store.store.Scan(ctx, prefix, after, limit)
	}
	if limit < 1 || limit > 1024 || len(prefix) > 32768 || len(after) > 32768 {
		return nil, ErrInvalid
	}
	size, err := store.store.engine.(*boltEngine).copySize(ctx, prefix, after, limit, false)
	if err != nil {
		return nil, err
	}
	charge := ramCharge(size)
	if err := store.policy.Reserve(charge); err != nil {
		return nil, err
	}
	defer store.policy.Release(charge)
	return store.store.Scan(ctx, prefix, after, limit)
}

func (store *WorkingStore) ScratchBytes() (uint64, bool) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.path == "" {
		return 0, true
	}
	var size uint64
	err := filepath.WalkDir(store.path, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err == nil {
			size += uint64(info.Size())
		}
		return err
	})
	return size, err == nil
}

func (store *WorkingStore) Close() error {
	store.once.Do(func() {
		store.mutex.Lock()
		defer store.mutex.Unlock()
		store.closed = true
		store.closeErr = store.store.Close()
		if store.cacheFile != nil {
			store.policy.untrackCacheFile(store.cacheFile)
			store.closeErr = errors.Join(store.closeErr, store.cacheFile.Close())
			store.cacheFile = nil
		}
		var removeErr error
		if store.path != "" {
			if store.fault != nil {
				removeErr = store.fault("cleanup")
			}
			if removeErr == nil {
				removeErr = os.RemoveAll(store.path)
			}
			store.closeErr = errors.Join(store.closeErr, removeErr)
		}
		if store.memoryCharge != 0 {
			store.policy.budget.release(store.memoryCharge)
			if removeErr == nil {
				store.policy.disk.release(store.diskCharge)
			}
		}
		if store.registered {
			store.policy.mutex.Lock()
			if store.backend == "ram" {
				store.policy.ramStores--
			}
			if store.policy.stores > 0 {
				store.policy.stores--
				if store.policy.stores == 0 {
					store.policy.budget.release(store.policy.headroom)
					store.policy.headroom = 0
				}
			}
			store.policy.mutex.Unlock()
		}
	})
	return store.closeErr
}
