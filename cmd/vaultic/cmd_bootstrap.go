package main

import (
	"fmt"
	"os"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/repository/bootstrap"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/spf13/cobra"
)

func newBootstrapCommand(globalOptions *global.Options) *cobra.Command {
	var fromCapsule bool
	var capsuleDirectory, output string
	command := &cobra.Command{
		Use:               "bootstrap",
		Short:             "Create a credential-free runtime profile from a recovery capsule",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			if !fromCapsule || capsuleDirectory == "" || output == "" {
				return fmt.Errorf("--from-capsule, --capsule-directory, and --output are required")
			}
			if globalOptions.KeyBrokerSocket == "" || globalOptions.KeyBrokerReleaseManifest == "" {
				return fmt.Errorf("capsule bootstrap requires --key-broker-socket and --key-broker-release-manifest")
			}
			info, err := os.Stat(capsuleDirectory)
			if err != nil || !info.IsDir() {
				return fmt.Errorf("capsule directory is not accessible: %w", err)
			}
			bootstrapOptions := *globalOptions
			bootstrapOptions.TopologySource = "capsule"
			bootstrapOptions.Repo = ""
			bootstrapOptions.RepositoryFile = ""
			bootstrapOptions.BootstrapProfile = ""
			printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
			repository, err := global.OpenRepository(command.Context(), bootstrapOptions, printer)
			if err != nil {
				return err
			}
			defer repository.Close()
			profile := bootstrap.Profile{
				Format:           2,
				RepositoryID:     repository.Config().ID,
				CapsuleDirectory: capsuleDirectory,
				BrokerSocket:     globalOptions.KeyBrokerSocket,
			}
			if err := bootstrap.StoreProfile(output, profile); err != nil {
				return err
			}
			globalOptions.Term.Print(fmt.Sprintf("credential-free capsule profile written to %s\n", output))
			return nil
		},
	}
	command.Flags().BoolVar(&fromCapsule, "from-capsule", false, "recover repository topology from the unlocked broker capsule")
	command.Flags().StringVar(&capsuleDirectory, "capsule-directory", "", "immutable recovery capsule directory")
	command.Flags().StringVar(&output, "output", "", "credential-free runtime profile path")
	return command
}
