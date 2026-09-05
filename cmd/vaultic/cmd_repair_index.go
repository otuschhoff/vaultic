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

func newRepairIndexCommand(globalOptions *global.Options) *cobra.Command {
	var options repairIndexOptions

	cmd := &cobra.Command{
		Use:   "index [flags]",
		Short: "Build a new index",
		Long: `
The "repair index" command creates a new index based on the pack files in the
repository.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRebuildIndex(cmd.Context(), options, *globalOptions, globalOptions.Term)
		},
	}

	options.AddFlags(cmd.Flags())
	return cmd
}

// repairIndexOptions collects all options for the repair index command.
type repairIndexOptions struct {
	ReadAllPacks bool
}

func (options *repairIndexOptions) AddFlags(f *pflag.FlagSet) {
	f.BoolVar(&options.ReadAllPacks, "read-all-packs", false, "read all pack files to generate new index from scratch")
}

func newRebuildIndexCommand(globalOptions *global.Options) *cobra.Command {
	var options repairIndexOptions

	replacement := newRepairIndexCommand(globalOptions)
	cmd := &cobra.Command{
		Use:               "rebuild-index [flags]",
		Short:             replacement.Short,
		Long:              replacement.Long,
		Deprecated:        `Use "repair index" instead`,
		DisableAutoGenTag: true,
		// must create a new instance of the run function as it captures options
		// by reference
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRebuildIndex(cmd.Context(), options, *globalOptions, globalOptions.Term)
		},
	}

	options.AddFlags(cmd.Flags())
	return cmd
}

func runRebuildIndex(ctx context.Context, options repairIndexOptions, globalOptions global.Options, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)

	ctx, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, false, printer)
	if err != nil {
		return err
	}
	defer unlock()
	if err := requireLegacyMetadataMutation(repo, "repair index"); err != nil {
		return err
	}

	err = repository.RepairIndex(ctx, repo, repository.RepairIndexOptions{
		ReadAllPacks: options.ReadAllPacks,
	}, printer)
	if err != nil {
		return err
	}

	printer.P("done\n")
	return nil
}
