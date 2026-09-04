package querycmd

import (
	"context"

	internalcli "github.com/otuschhoff/vaultic/internal/cli"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func openWithReadLock(
	ctx context.Context,
	options global.Options,
	noLock bool,
	printer vaultic.Printer,
) (context.Context, *repository.Repository, func(), error) {
	return internalcli.OpenRepository(ctx, options, internalcli.LockShared, internalcli.OpenOptions{
		AllowNoLock:  noLock,
		LockFreeRead: true,
	}, printer)
}
