package cli

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
}

func OpenRepository(
	ctx context.Context,
	options global.Options,
	policy LockPolicy,
	openOptions OpenOptions,
	printer vaultic.Printer,
) (context.Context, *repository.Repository, func(), error) {
	repo, err := global.OpenRepository(ctx, options, printer)
	if err != nil {
		return nil, nil, nil, err
	}

	if openOptions.DryRun {
		repo.SetDryRun()
		return ctx, repo, func() {}, nil
	}
	if policy != LockExclusive && openOptions.AllowNoLock {
		policy = LockNone
	}
	if policy == LockShared && openOptions.LockFreeRead && feature.Flag.Enabled(feature.LockFree) {
		policy = LockNone
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
