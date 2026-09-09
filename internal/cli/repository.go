package cli

import (
	"context"
	"fmt"
	"sync"

	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/topology"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

var processLock sync.RWMutex

// LockPolicy controls repository locking for a CLI operation.
type LockPolicy uint8

// Repository lock policies.
const (
	LockNone LockPolicy = iota
	LockShared
	LockExclusive
)

type OpenOptions struct {
	DryRun       bool
	AllowNoLock  bool
	LockFreeRead bool
	ReadOnlyData bool
}

func OpenRepository(
	ctx context.Context,
	options global.Options,
	policy LockPolicy,
	openOptions OpenOptions,
	printer vaultic.Printer,
) (context.Context, *repository.Repository, func(), error) {
	if openOptions.DryRun {
		policy = LockNone
	}
	if policy != LockExclusive && openOptions.AllowNoLock {
		policy = LockNone
	}
	if policy == LockShared && openOptions.LockFreeRead && feature.Flag.Enabled(feature.LockFree) {
		policy = LockNone
	}
	options.StorageCredentialTier, options.StorageLockCredential = storageCredentialAccess(policy, openOptions)
	repo, err := global.OpenRepository(ctx, options, printer)
	if err != nil {
		return nil, nil, nil, err
	}

	if openOptions.DryRun {
		repo.SetDryRun()
		return ctx, repo, func() {}, nil
	}
	if policy == LockNone {
		return ctx, repo, func() {}, nil
	}
	if policy != LockShared && policy != LockExclusive {
		return nil, nil, nil, fmt.Errorf("invalid lock policy %v", policy)
	}

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
	remoteUnlock, lockedContext, err := repository.LockRepo(ctx, repo, exclusive, options.RetryLock, func(message string) {
		if !options.JSON {
			printer.P("%s", message)
		}
	}, printer.E)
	if err != nil {
		localUnlock()
		return nil, nil, nil, err
	}
	return lockedContext, repo, func() {
		remoteUnlock()
		localUnlock()
	}, nil
}

func storageCredentialAccess(policy LockPolicy, openOptions OpenOptions) (string, bool) {
	switch {
	case openOptions.ReadOnlyData:
		return string(topology.StorageRead), policy != LockNone
	case policy == LockExclusive:
		return string(topology.StorageMaintain), true
	case policy == LockShared && !openOptions.LockFreeRead:
		return string(topology.StorageAppend), true
	case policy == LockShared:
		return string(topology.StorageRead), true
	default:
		return string(topology.StorageRead), false
	}
}
