package backupcmd

import (
	"context"

	internalcli "github.com/otuschhoff/vaultic/internal/cli"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func openWithAppendLock(
	ctx context.Context,
	options global.Options,
	dryRun bool,
	printer vaultic.Printer,
) (context.Context, *repository.Repository, func(), error) {
	return internalcli.OpenRepository(ctx, options, internalcli.LockShared, internalcli.OpenOptions{
		DryRun: dryRun, AllowNoLock: options.NoLock,
	}, printer)
}
