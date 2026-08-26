package main

import (
	"context"
	"sync"

	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

var processLock sync.RWMutex

func internalOpenWithLocked(ctx context.Context, gopts global.Options, dryRun bool, exclusive bool, skipLock bool, lockFreeRead bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	repo, err := global.OpenRepository(ctx, gopts, printer)
	if err != nil {
		return nil, nil, nil, err
	}

	unlock := func() {}
	// Lock-free mode (opt-in via the lock-free feature flag): read-only and
	// append-only operations skip the lock file entirely (but remain writable).
	// Index writes are additive, so concurrent lock-free writers cannot corrupt
	// the repository; stale locks therefore no longer block routine backups or
	// reads. Exclusive operations (prune, forget, repair, ...) always take an
	// exclusive lock regardless of this flag. Lock-free is opt-in (Alpha)
	// because it reorders backend list operations, which requires coordinated
	// clients on eventually-consistent backends.
	// --no-lock is an explicit user opt-out for non-exclusive operations.
	// The Alpha lock-free feature currently applies to read-only commands only.
	// Append commands retain a non-exclusive lock: an exclusive prune planned
	// before an unlocked backup can otherwise remove packs the backup still
	// needs. True lock-free append requires a prune revalidation protocol that
	// this repository does not yet have. Exclusive operations never bypass
	// their lock here; callers only bypass it for a genuine dry-run after
	// validating the command-specific safety conditions.
	skipLock = skipLock || (lockFreeRead && feature.Flag.Enabled(feature.LockFree))
	if dryRun {
		// A dry-run must never write repository objects, independently of the
		// selected locking policy.
		repo.SetDryRun()
	} else if !skipLock {
		// The backend lock is the cross-process guard. This in-process guard
		// closes the race between concurrent goroutines sharing one backend
		// client: append operations share it, exclusive operations serialize.
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
	}

	return ctx, repo, unlock, nil
}

func openWithReadLock(ctx context.Context, gopts global.Options, noLock bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	// TODO enforce read-only operations once the locking code has moved to the repository
	// As in-depth hardening, put the repository into read-only mode if noLock is true
	// Not possible if the repository has to be locked.
	return internalOpenWithLocked(ctx, gopts, false, false, noLock, true, printer)
}

func openWithAppendLock(ctx context.Context, gopts global.Options, dryRun bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	// TODO enforce non-exclusive operations once the locking code has moved to the repository
	return internalOpenWithLocked(ctx, gopts, dryRun, false, gopts.NoLock, false, printer)
}

func openWithExclusiveLock(ctx context.Context, gopts global.Options, dryRun bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	return internalOpenWithLocked(ctx, gopts, dryRun, true, false, false, printer)
}
