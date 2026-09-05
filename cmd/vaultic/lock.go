package main

import (
	"context"

	internalcli "github.com/otuschhoff/vaultic/internal/cli"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

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

func effectiveLockPolicy(policy LockPolicy, options lockOpenOptions) LockPolicy {
	if options.DryRun {
		return LockNone
	}
	if policy != LockExclusive && options.AllowNoLock {
		return LockNone
	}
	if policy == LockShared && options.LockFreeRead && feature.Flag.Enabled(feature.LockFree) {
		return LockNone
	}
	return policy
}

func openWithLockPolicy(
	ctx context.Context,
	globalOptions global.Options,
	policy LockPolicy,
	options lockOpenOptions,
	printer vaultic.Printer,
) (context.Context, *repository.Repository, func(), error) {
	return internalcli.OpenRepository(ctx, globalOptions, internalcli.LockPolicy(policy), internalcli.OpenOptions{
		DryRun: options.DryRun, AllowNoLock: options.AllowNoLock, LockFreeRead: options.LockFreeRead,
	}, printer)
}

func openWithReadLock(
	ctx context.Context,
	globalOptions global.Options,
	noLock bool,
	printer vaultic.Printer,
) (context.Context, *repository.Repository, func(), error) {
	return openWithLockPolicy(ctx, globalOptions, LockShared, lockOpenOptions{
		AllowNoLock:  noLock,
		LockFreeRead: true,
	}, printer)
}

func openWithAppendLock(
	ctx context.Context,
	globalOptions global.Options,
	dryRun bool,
	printer vaultic.Printer,
) (context.Context, *repository.Repository, func(), error) {
	return openWithLockPolicy(ctx, globalOptions, LockShared, lockOpenOptions{
		DryRun:      dryRun,
		AllowNoLock: globalOptions.NoLock,
	}, printer)
}

func openWithExclusiveLock(
	ctx context.Context,
	globalOptions global.Options,
	dryRun bool,
	printer vaultic.Printer,
) (context.Context, *repository.Repository, func(), error) {
	return openWithLockPolicy(ctx, globalOptions, LockExclusive, lockOpenOptions{
		DryRun: dryRun,
	}, printer)
}
