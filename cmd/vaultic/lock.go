package main

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func internalOpenWithLocked(ctx context.Context, gopts global.Options, dryRun bool, exclusive bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	repo, err := global.OpenRepository(ctx, gopts, printer)
	if err != nil {
		return nil, nil, nil, err
	}

	unlock := func() {}
	// Lock-free mode (default): read-only and append-only operations skip the
	// lock file entirely (but remain writable). Index writes are additive, so
	// concurrent lock-free writers cannot corrupt the repository; stale locks
	// therefore no longer block routine backups or reads. Exclusive operations
	// (prune, forget, repair, ...) always take an exclusive lock regardless of
	// this flag.
	skipLock := !exclusive && feature.Flag.Enabled(feature.LockFree)
	if !dryRun && !skipLock {
		unlock, ctx, err = repository.LockRepo(ctx, repo, exclusive, gopts.RetryLock, func(msg string) {
			if !gopts.JSON {
				printer.P("%s", msg)
			}
		}, printer.E)
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		repo.SetDryRun()
	}

	return ctx, repo, unlock, nil
}

func openWithReadLock(ctx context.Context, gopts global.Options, noLock bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	// TODO enforce read-only operations once the locking code has moved to the repository
	// As in-depth hardening, put the repository into read-only mode if noLock is true
	// Not possible if the repository has to be locked.
	return internalOpenWithLocked(ctx, gopts, noLock, false, printer)
}

func openWithAppendLock(ctx context.Context, gopts global.Options, dryRun bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	// TODO enforce non-exclusive operations once the locking code has moved to the repository
	return internalOpenWithLocked(ctx, gopts, dryRun, false, printer)
}

func openWithExclusiveLock(ctx context.Context, gopts global.Options, dryRun bool, printer vaultic.Printer) (context.Context, *repository.Repository, func(), error) {
	return internalOpenWithLocked(ctx, gopts, dryRun, true, printer)
}
