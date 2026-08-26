package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

var processLock sync.RWMutex

// LockPolicy describes the backend lock requested by a command.
//
// LockNone is appropriate only for reads that are explicitly lock-free or for
// an operator's deliberate --no-lock override. LockShared is used by additive
// writers while prune still cannot revalidate concurrent writes. LockExclusive
// protects destructive operations and commands that require a coherent view.
type LockPolicy uint8

const (
	LockNone LockPolicy = iota
	LockShared
	LockExclusive
)

type lockOpenOptions struct {
	// DryRun makes the repository non-writing and intentionally avoids a lock.
	DryRun bool
	// AllowNoLock permits the explicit --no-lock override for non-exclusive
	// commands. It never bypasses LockExclusive.
	AllowNoLock bool
	// LockFreeRead permits the Alpha lock-free feature to choose LockNone.
	// It is deliberately false for append writers.
	LockFreeRead bool
}

func (p LockPolicy) String() string {
	switch p {
	case LockNone:
		return "none"
	case LockShared:
		return "shared"
	case LockExclusive:
		return "exclusive"
	default:
		return "invalid"
	}
}

func effectiveLockPolicy(policy LockPolicy, opts lockOpenOptions) LockPolicy {
	if opts.DryRun {
		return LockNone
	}
	if policy != LockExclusive && opts.AllowNoLock {
		return LockNone
	}
	if policy == LockShared && opts.LockFreeRead && feature.Flag.Enabled(feature.LockFree) {
		return LockNone
	}
	return policy
}

func openWithLockPolicy(ctx context.Context, gopts global.Options, policy LockPolicy, opts lockOpenOptions, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	repo, err := global.OpenRepository(ctx, gopts, printer)
	if err != nil {
		return nil, nil, nil, err
	}

	unlock := func() {}
	policy = effectiveLockPolicy(policy, opts)
	if opts.DryRun {
		// A dry-run must never write repository objects, independently of the
		// selected locking policy.
		repo.SetDryRun()
		return ctx, repo, unlock, nil
	}

	switch policy {
	case LockNone:
		return ctx, repo, unlock, nil
	case LockShared, LockExclusive:
		// The backend lock is the cross-process guard. This in-process guard
		// closes the race between concurrent goroutines sharing one backend
		// client: append operations share it, exclusive operations serialize.
		exclusive := policy == LockExclusive
		if exclusive {
			processLock.Lock()
		} else {
			processLock.RLock()
		}
		localUnlock := func() {
			if exclusive {
				processLock.Unlock()
			} else {
				processLock.RUnlock()
			}
		}
		unlock, ctx, err = repository.LockRepo(ctx, repo, exclusive, gopts.RetryLock, func(msg string) {
			if !gopts.JSON {
				printer.P("%s", msg)
			}
		}, printer.E)
		if err != nil {
			localUnlock()
			return nil, nil, nil, err
		}
		remoteUnlock := unlock
		unlock = func() {
			remoteUnlock()
			localUnlock()
		}
	default:
		return nil, nil, nil, fmt.Errorf("invalid lock policy %v", policy)
	}

	return ctx, repo, unlock, nil
}

func openWithReadLock(ctx context.Context, gopts global.Options, noLock bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	return openWithLockPolicy(ctx, gopts, LockShared, lockOpenOptions{
		AllowNoLock:  noLock,
		LockFreeRead: true,
	}, printer)
}

func openWithAppendLock(ctx context.Context, gopts global.Options, dryRun bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	return openWithLockPolicy(ctx, gopts, LockShared, lockOpenOptions{
		DryRun:      dryRun,
		AllowNoLock: gopts.NoLock,
	}, printer)
}

func openWithExclusiveLock(ctx context.Context, gopts global.Options, dryRun bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	return openWithLockPolicy(ctx, gopts, LockExclusive, lockOpenOptions{
		DryRun: dryRun,
	}, printer)
}
