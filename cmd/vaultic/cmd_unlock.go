package main

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newUnlockCommand(globalOptions *global.Options) *cobra.Command {
	var options unlockOptions

	cmd := &cobra.Command{
		Use:   "unlock",
		Short: "Remove locks other processes created",
		Long: `
The "unlock" command removes stale locks that have been created by other vaultic processes.

Removing locks works even with repositories served in append-only mode from vaultic's rest-server.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runUnlock(cmd.Context(), options, *globalOptions, globalOptions.Term)
		},
	}
	options.AddFlags(cmd.Flags())
	return cmd
}

// unlockOptions collects all options for the unlock command.
type unlockOptions struct {
	RemoveAll bool
}

func (options *unlockOptions) AddFlags(f *pflag.FlagSet) {
	f.BoolVar(&options.RemoveAll, "remove-all", false, "remove all locks, even non-stale ones")
}

func runUnlock(ctx context.Context, options unlockOptions, globalOptions global.Options, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, term)
	repo, err := global.OpenRepository(ctx, globalOptions, printer)
	if err != nil {
		return err
	}

	fn := repository.RemoveStaleLocks
	if options.RemoveAll {
		fn = repository.RemoveAllLocks
	}

	processed, err := fn(ctx, repo)
	if err != nil {
		return err
	}

	if processed > 0 {
		printer.P("successfully removed %d locks", processed)
	}
	return nil
}
