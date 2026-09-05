package keycmd

import (
	"context"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newKeyPasswdCommand(globalOptions *global.Options) *cobra.Command {
	var options keyPasswdOptions

	cmd := &cobra.Command{
		Use:   "passwd",
		Short: "Change key (password); creates a new key ID and removes the old key ID, returns new key ID",
		Long: `
The "key passwd" command creates a new key, validates the key and removes the old key ID.
Returns the new key ID.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
	`,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runKeyPasswd(cmd.Context(), *globalOptions, options, args, globalOptions.Term)
		},
	}

	options.AddFlags(cmd.Flags())
	return cmd
}

type keyPasswdOptions struct {
	keyAddOptions
}

func (options *keyPasswdOptions) AddFlags(flags *pflag.FlagSet) {
	options.keyAddOptions.Add(flags)
}

func runKeyPasswd(ctx context.Context, globalOptions global.Options, options keyPasswdOptions, args []string, term ui.Terminal) error {
	return runKeyPasswdWithPassword(ctx, globalOptions, options, args, term, func() (string, error) {
		return getNewPassword(ctx, globalOptions, options.NewPasswordFile, options.InsecureNoPassword)
	})
}

func RunPasswdWithPassword(
	ctx context.Context, globalOptions global.Options, options keyPasswdOptions, args []string,
	term ui.Terminal, readPassword func() (string, error),
) error {
	return runKeyPasswdWithPassword(ctx, globalOptions, options, args, term, readPassword)
}

func runKeyPasswdWithPassword(
	ctx context.Context, globalOptions global.Options, options keyPasswdOptions, args []string,
	term ui.Terminal, readPassword func() (string, error),
) error {
	if len(args) > 0 {
		return fmt.Errorf("the key passwd command expects no arguments, only options - please see `vaultic help key passwd` for usage and flags")
	}

	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, false, printer)
	if err != nil {
		return err
	}
	defer unlock()

	return changePassword(ctx, repo, options, printer, readPassword)
}

func changePassword(
	ctx context.Context, repo *repository.Repository, options keyPasswdOptions,
	printer vaultic.Printer, readPassword func() (string, error),
) error {
	password, err := readPassword()
	if err != nil {
		return err
	}

	id, err := repository.AddKey(ctx, repo, password, options.Username, options.Hostname, repo.Key())
	if err != nil {
		return errors.Fatalf("creating new key failed: %v", err)
	}
	oldID := repo.KeyID()

	err = switchToNewKeyAndRemoveIfBroken(ctx, repo, id, password)
	if err != nil {
		return err
	}

	err = repository.RemoveKey(ctx, repo, oldID)
	if err != nil {
		return err
	}

	printer.P("saved new key as %s", id)

	return nil
}
