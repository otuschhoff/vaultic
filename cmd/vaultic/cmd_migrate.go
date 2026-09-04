package main

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/migrations"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newMigrateCommand(globalOptions *global.Options) *cobra.Command {
	var opts MigrateOptions

	cmd := &cobra.Command{
		Use:   "migrate [flags] [migration name] [...]",
		Short: "Apply migrations",
		Long: `
The "migrate" command checks which migrations can be applied for a repository
and prints a list with available migration names. If one or more migration
names are specified, these migrations are applied.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		DisableAutoGenTag: true,
		GroupID:           cmdGroupDefault,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMigrate(cmd.Context(), opts, *globalOptions, args, globalOptions.Term)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// MigrateOptions bundles all options for the 'check' command.
type MigrateOptions struct {
	Force bool
}

func (opts *MigrateOptions) AddFlags(f *pflag.FlagSet) {
	f.BoolVarP(&opts.Force, "force", "f", false, `apply a migration a second time`)
}

func checkMigrations(ctx context.Context, repo vaultic.Repository, printer vaultic.Printer) error {
	printer.P("available migrations:\n")
	found := false

	for _, m := range migrations.All {
		ok, _, err := m.Check(ctx, repo)
		if err != nil {
			return err
		}

		if ok {
			printer.P("  %v\t%v\n", m.Name(), m.Desc())
			found = true
		}
	}

	if !found {
		printer.P("no migrations found\n")
	}

	return nil
}

func applyMigrations(ctx context.Context, opts MigrateOptions, gopts global.Options, repo vaultic.Repository,
	args []string, term ui.Terminal, printer vaultic.Printer) error {
	var firsterr error
	for _, name := range args {
		found, applyErr, err := applyNamedMigration(ctx, opts, gopts, repo, name, term, printer)
		if err != nil {
			return err
		}
		if firsterr == nil && applyErr != nil {
			firsterr = applyErr
		}
		if !found {
			printer.E("unknown migration %v", name)
		}
	}
	return firsterr
}

func applyNamedMigration(ctx context.Context, opts MigrateOptions, gopts global.Options, repo vaultic.Repository,
	name string, term ui.Terminal, printer vaultic.Printer) (bool, error, error) {
	for _, migration := range migrations.All {
		if migration.Name() != name {
			continue
		}
		ok, reason, err := migration.Check(ctx, repo)
		if err != nil {
			return true, nil, err
		}
		if !ok && !opts.Force {
			if reason == "" {
				reason = "check failed"
			}
			printer.E("migration %v cannot be applied: %v\n"+
				"If you want to apply this migration anyway, re-run with option --force\n", migration.Name(), reason)
			return true, nil, nil
		}
		if !ok {
			printer.E("check for migration %v failed, continuing anyway\n", migration.Name())
		}
		if migration.RepoCheck() {
			printer.P("checking repository integrity...\n")
			checkGopts := gopts
			checkGopts.NoLock = true
			if _, err := runCheck(ctx, CheckOptions{}, checkGopts, nil, term); err != nil {
				return true, nil, err
			}
		}
		printer.P("applying migration %v...\n", migration.Name())
		if err := migration.Apply(ctx, repo); err != nil {
			printer.E("migration %v failed: %v\n", migration.Name(), err)
			return true, err, nil
		}
		printer.P("migration %v: success\n", migration.Name())
		return true, nil, nil
	}
	return false, nil, nil
}

func runMigrate(ctx context.Context, opts MigrateOptions, gopts global.Options, args []string, term ui.Terminal) error {
	printer := progress.NewTerminalPrinter(false, gopts.Verbosity, term)
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, false, printer)
	if err != nil {
		return err
	}
	defer unlock()
	if len(args) == 0 {
		return checkMigrations(ctx, repo, printer)
	}
	return applyMigrations(ctx, opts, gopts, repo, args, term, printer)
}
