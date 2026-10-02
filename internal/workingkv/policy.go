package workingkv

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/otuschhoff/vaultic/internal/workingstate"
)

type Mode = workingstate.Mode

const (
	ModeRAM = workingstate.ModeRAM
	ModeKV  = workingstate.ModeKV
)

type Policy struct {
	mode    Mode
	budget  *Budget
	scratch string
}

func NewPolicy(mode Mode, limit uint64, scratch string) (*Policy, error) {
	if mode != ModeRAM && mode != ModeKV {
		return nil, ErrInvalid
	}
	budget, err := NewBudget(limit)
	if err != nil {
		return nil, err
	}
	return &Policy{mode: mode, budget: budget, scratch: scratch}, nil
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
	*Store
	path     string
	backend  string
	closeErr error
	once     sync.Once
}

func OpenWorking(ctx context.Context, parent, prefix string) (*WorkingStore, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	policy := PolicyFrom(ctx)
	if policy != nil && policy.mode == ModeRAM {
		store, err := OpenRAM(policy.budget)
		if err != nil {
			return nil, err
		}
		return &WorkingStore{Store: store, backend: "ram"}, nil
	}
	backend := "pebble"
	if policy != nil {
		backend = "bbolt"
		if policy.scratch != "" {
			parent = policy.scratch
		}
	}
	path, err := os.MkdirTemp(parent, prefix)
	if err != nil {
		return nil, err
	}
	store, err := Open(backend, path)
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(path))
	}
	return &WorkingStore{Store: store, path: path, backend: backend}, nil
}

func (store *WorkingStore) Backend() string { return store.backend }
func (store *WorkingStore) Path() string    { return store.path }

func (store *WorkingStore) ScratchBytes() (uint64, bool) {
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
		store.closeErr = store.Store.Close()
		if store.path != "" {
			store.closeErr = errors.Join(store.closeErr, os.RemoveAll(store.path))
		}
	})
	return store.closeErr
}
