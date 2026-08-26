package main

import (
	"github.com/spf13/cobra"
	"github.com/vaultic/vaultic/internal/global"
)

func newKeyCommand(globalOptions *global.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "key",
		Short: "Manage keys (passwords)",
		Long: `
The "key" command allows you to set multiple access keys or passwords
per repository.
	`,
		DisableAutoGenTag: true,
		GroupID:           cmdGroupDefault,
	}

	cmd.AddCommand(
		newKeyAddCommand(globalOptions),
		newKeyListCommand(globalOptions),
		newKeyPasswdCommand(globalOptions),
		newKeyRemoveCommand(globalOptions),
	)
	return cmd
}
