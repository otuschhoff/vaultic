package main

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vaultic/vaultic/internal/errors"
	"github.com/vaultic/vaultic/internal/global"
	"github.com/vaultic/vaultic/internal/ui/progress"
	"github.com/vaultic/vaultic/internal/vaultic"
)

func newShowConfigCommand(globalOptions *global.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show-config",
		Short: "Print the repository configuration",
		Long: `
The "show-config" command prints the configuration stored in the repository,
including vaultic/rustic extension settings (compression, pack sizes,
append-only, extra verification, chunker).

Unlike "config" it never modifies the repository and does not need a lock.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			term := globalOptions.Term
			return runShowConfig(cmd.Context(), *globalOptions, args, progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, term))
		},
	}
	return cmd
}

func runShowConfig(ctx context.Context, gopts global.Options, args []string, printer vaultic.Printer) error {
	if len(args) > 0 {
		return errors.Fatal("the show-config command expects no arguments - please see `vaultic help show-config` for usage")
	}

	ctx, repo, unlock, err := openWithReadLock(ctx, gopts, gopts.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	return printConfig(repo.Config(), gopts, printer)
}
