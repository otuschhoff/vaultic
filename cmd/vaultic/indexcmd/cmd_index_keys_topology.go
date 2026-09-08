package indexcmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	vaulticerrors "github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/topology"
	"github.com/spf13/cobra"
)

func newIndexKeysQuorumTopologyCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	command := &cobra.Command{Use: "topology", Short: "Inspect and mutate capsule-sealed topology", Args: cobra.NoArgs, DisableAutoGenTag: true}
	command.AddCommand(newTopologyShowCommand(globalOptions))
	for _, operation := range []string{"set-backend", "set-backend-credential", "set-replica", "set-credential", "rotate-credential", "remove-credential"} {
		command.AddCommand(newTopologyMutationCommand(globalOptions, options, operation))
	}
	return command
}

func newTopologyShowCommand(globalOptions *global.Options) *cobra.Command {
	return &cobra.Command{
		Use: "show", Short: "Show capsule topology without credential values", Args: cobra.NoArgs, DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			client, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
			if err != nil {
				return err
			}
			defer client.Close()
			document, lease, err := client.ReadTopology(command.Context(), globalOptions.KeyBrokerReleaseManifest, globalOptions.KeyBrokerLeaseDuration)
			if err != nil {
				return err
			}
			defer clear(lease.Key)
			encoded, err := document.RedactedJSON()
			if err != nil {
				return err
			}
			globalOptions.Term.Print(string(encoded) + "\n")
			return nil
		},
	}
}

func newTopologyMutationCommand(globalOptions *global.Options, options *indexKeysOptions, operation string) *cobra.Command {
	var capsulePath, capsuleDirectory string
	var memberSpecs, externalMemberFiles []string
	argumentCount := 2
	usage := operation + " ID FILE"
	if operation == "set-backend" {
		argumentCount, usage = 1, operation+" FILE"
	}
	if operation == "set-backend-credential" {
		argumentCount, usage = 3, operation+" BACKEND_FILE REFERENCE CREDENTIAL_FILE"
	}
	if operation == "remove-credential" {
		argumentCount, usage = 1, operation+" REFERENCE"
	}
	command := &cobra.Command{
		Use: usage, Short: "Publish a validated topology change as a new capsule generation", Args: cobra.ExactArgs(argumentCount), DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, args []string) error {
			capsule, err := indexbroker.LoadCapsule(capsulePath)
			if err != nil {
				return err
			}
			if capsule.RepositoryID() != options.RepositoryID {
				return fmt.Errorf("recovery capsule repository identity mismatch")
			}
			policy, err := capsule.PolicyDefinition()
			if err != nil {
				return err
			}
			members, credentials, err := parseOfflinePolicyMembers(memberSpecs)
			if err != nil {
				return err
			}
			externalMembers, tokens, err := parseExternalPolicyMembers(command.Context(), options.RepositoryID, externalMemberFiles)
			defer clearPolicyCredentials(credentials)
			defer clearPolicyCredentials(tokens)
			if err != nil {
				return err
			}
			if err := validatePolicyMemberCredentials(policy, members, externalMembers); err != nil {
				return err
			}
			mutation, err := readTopologyMutation(operation, args)
			if err != nil {
				return err
			}
			client, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
			if err != nil {
				return err
			}
			status, err := client.Status(command.Context())
			if err != nil {
				_ = client.Close() // Preserve the status error; connection cleanup is best effort.
				return err
			}
			if err := matchQuorumCapsule(capsule.RepositoryID(), capsule.Generation(), capsule.LogicalID(), capsule.PolicyHash(), status); err != nil {
				_ = client.Close() // Preserve the capsule mismatch; connection cleanup is best effort.
				return err
			}
			prepared, err := client.PrepareTopologyMutation(command.Context(), globalOptions.KeyBrokerReleaseManifest, mutation, members, externalMembers)
			vaulticerrors.LogClose(client, "close key broker client", func(string, ...any) {})
			if err != nil {
				return err
			}
			return publishAndActivatePolicyMutation(command.Context(), globalOptions, options, operation, capsuleDirectory, status, prepared)
		},
	}
	command.Flags().StringVar(&capsulePath, "capsule", "", "current immutable recovery capsule")
	command.Flags().StringVar(&capsuleDirectory, "capsule-directory", "", "deterministic local capsule generation directory")
	command.Flags().StringArrayVar(&memberSpecs, "member", nil, "existing member ID=PROVIDER:FILE (repeatable; all members required)")
	command.Flags().StringArrayVar(&externalMemberFiles, "external-member-file", nil, "existing protected external-member definition (repeatable)")
	mustMarkFlagRequired(command, "capsule")
	mustMarkFlagRequired(command, "capsule-directory")
	return command
}

func readTopologyMutation(operation string, args []string) (topology.Mutation, error) {
	mutation := topology.Mutation{Operation: operation}
	switch operation {
	case "set-backend":
		var backend topology.PackBackend
		if err := readStrictJSON(args[0], &backend); err != nil {
			return topology.Mutation{}, err
		}
		mutation.Backend = &backend
	case "set-backend-credential":
		var backend topology.PackBackend
		if err := readStrictJSON(args[0], &backend); err != nil {
			return topology.Mutation{}, err
		}
		var credential topology.Credential
		if err := readProtectedJSON(args[2], "topology credential", &credential); err != nil {
			return topology.Mutation{}, err
		}
		if err := credential.Validate(); err != nil {
			return topology.Mutation{}, err
		}
		mutation.Backend, mutation.Reference, mutation.Credential = &backend, args[1], &credential
	case "set-replica":
		var replica topology.MetadataReplica
		if err := readStrictJSON(args[1], &replica); err != nil {
			return topology.Mutation{}, err
		}
		mutation.ID, mutation.Replica = args[0], &replica
	case "set-credential", "rotate-credential":
		var credential topology.Credential
		if err := readProtectedJSON(args[1], "topology credential", &credential); err != nil {
			return topology.Mutation{}, err
		}
		if err := credential.Validate(); err != nil {
			return topology.Mutation{}, err
		}
		mutation.Reference, mutation.Credential = args[0], &credential
	case "remove-credential":
		mutation.Reference = args[0]
	default:
		return topology.Mutation{}, fmt.Errorf("unsupported topology mutation %q", operation)
	}
	return mutation, nil
}

func readStrictJSON(path string, destination any) error {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("JSON file contains trailing data")
	}
	return nil
}
