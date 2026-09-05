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

func newKeyAddCommand(globalOptions *global.Options) *cobra.Command {
	var options keyAddOptions

	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add a new key (password) to the repository; returns the new key ID",
		Long: `
The "key add" command creates a new key and validates the key. Returns the new key ID.

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
			return runKeyAdd(cmd.Context(), *globalOptions, options, args, globalOptions.Term)
		},
	}

	options.Add(cmd.Flags())
	return cmd
}

type keyAddOptions struct {
	NewPasswordFile    string
	InsecureNoPassword bool
	Username           string
	Hostname           string
}

func (options *keyAddOptions) Add(flags *pflag.FlagSet) {
	flags.StringVarP(&options.NewPasswordFile, "new-password-file", "", "", "`file` from which to read the new password")
	flags.BoolVar(&options.InsecureNoPassword, "new-insecure-no-password", false, "add an empty password for the repository (insecure)")
	flags.StringVarP(&options.Username, "user", "", "", "the username for new key")
	flags.StringVarP(&options.Hostname, "host", "", "", "the hostname for new key")
}

func runKeyAdd(ctx context.Context, globalOptions global.Options, options keyAddOptions, args []string, term ui.Terminal) error {
	return runKeyAddWithPassword(ctx, globalOptions, options, args, term, func() (string, error) {
		return getNewPassword(ctx, globalOptions, options.NewPasswordFile, options.InsecureNoPassword)
	})
}

func RunAddWithPassword(
	ctx context.Context, globalOptions global.Options, options keyAddOptions, args []string,
	term ui.Terminal, readPassword func() (string, error),
) error {
	return runKeyAddWithPassword(ctx, globalOptions, options, args, term, readPassword)
}

func runKeyAddWithPassword(
	ctx context.Context, globalOptions global.Options, options keyAddOptions, args []string,
	term ui.Terminal, readPassword func() (string, error),
) error {
	if len(args) > 0 {
		return fmt.Errorf("the key add command expects no arguments, only options - please see `vaultic help key add` for usage and flags")
	}

	printer := progress.NewTerminalPrinter(false, globalOptions.Verbosity, term)
	// Key validation may remove a newly-created broken key. It is therefore not
	// an append-only transaction and must use the exclusive lock policy.
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, globalOptions, false, printer)
	if err != nil {
		return err
	}
	defer unlock()

	return addKey(ctx, repo, options, printer, readPassword)
}

func addKey(ctx context.Context, repo *repository.Repository, options keyAddOptions, printer vaultic.Printer, readPassword func() (string, error)) error {
	password, err := readPassword()
	if err != nil {
		return err
	}

	id, err := repository.AddKey(ctx, repo, password, options.Username, options.Hostname, repo.Key())
	if err != nil {
		return errors.Fatalf("creating new key failed: %v", err)
	}

	err = switchToNewKeyAndRemoveIfBroken(ctx, repo, id, password)
	if err != nil {
		return err
	}

	printer.P("saved new key with ID %s", id.ID())

	return nil
}

func getNewPassword(ctx context.Context, globalOptions global.Options, newPasswordFile string, insecureNoPassword bool) (string, error) {
	if insecureNoPassword {
		if newPasswordFile != "" {
			return "", fmt.Errorf("only either --new-password-file or --new-insecure-no-password may be specified")
		}
		return "", nil
	}

	if newPasswordFile != "" {
		password, err := global.LoadPasswordFromFile(newPasswordFile)
		if err != nil {
			return "", err
		}
		if password == "" {
			return "", fmt.Errorf("an empty password is not allowed by default. Pass the flag `--new-insecure-no-password` to vaultic to disable this check")
		}
		return password, nil
	}

	// Since we already have an open repository, temporary remove the password
	// to prompt the user for the passwd.
	passwordOptions := globalOptions
	passwordOptions.Password = ""
	// empty passwords are already handled above
	passwordOptions.InsecureNoPassword = false

	return global.ReadPasswordTwice(ctx, passwordOptions,
		"enter new password: ",
		"enter password again: ")
}

func switchToNewKeyAndRemoveIfBroken(ctx context.Context, repo *repository.Repository, key *repository.Key, pw string) error {
	// Verify new key to make sure it really works. A broken key can render the
	// whole repository inaccessible
	err := repo.SearchKey(ctx, pw, 0, key.ID().String())
	if err != nil {
		// the key is invalid, try to remove it
		_ = repository.RemoveKey(ctx, repo, key.ID())
		return errors.Fatalf("failed to access repository with new key: %v", err)
	}
	return nil
}
