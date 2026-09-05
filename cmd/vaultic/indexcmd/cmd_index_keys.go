package indexcmd

import (
	"bytes"
	"context"
	"crypto/ecdh"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	vaulticerrors "github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/spf13/cobra"
)

type indexKeysOptions struct {
	Daemon       indexDaemonOptions
	RepositoryID string
}

type indexEncryptOptions struct {
	Daemon       indexDaemonOptions
	RepositoryID string
}

func (options *indexEncryptOptions) finalize() error {
	if options.Daemon.PassphraseFile == "" {
		return fmt.Errorf("--metadata-recovery-passphrase-file is required")
	}
	options.Daemon.EncryptionMode = "initialize"
	options.Daemon.Start = true
	return options.Daemon.Finalize()
}

type hardwareEnrollmentOptions struct {
	MemberID, RelyingPartyID, PINFile, OutputPath string
}

func (options hardwareEnrollmentOptions) finalizeMacOS() error {
	if runtime.GOOS != "darwin" {
		return fmt.Errorf("macOS Secure Enclave enrollment requires macOS")
	}
	if options.MemberID == "" || options.OutputPath == "" {
		return fmt.Errorf("--member and --output are required")
	}
	return nil
}

func (options hardwareEnrollmentOptions) finalizeFIDO2() error {
	if options.MemberID == "" || options.RelyingPartyID == "" || options.PINFile == "" || options.OutputPath == "" {
		return fmt.Errorf("--member, --relying-party-id, --pin-file, and --output are required")
	}
	return nil
}

type quorumPrepareOptions struct {
	CapsuleDirectory, GroupID, BrokerPublicKeyFile, StateFile string
	Generation                                                uint64
	Threshold                                                 uint32
}

func (options quorumPrepareOptions) finalize(memberCount int) error {
	if options.Generation == 0 || options.Threshold == 0 || options.GroupID == "" || options.CapsuleDirectory == "" ||
		options.BrokerPublicKeyFile == "" || options.StateFile == "" {
		return fmt.Errorf("capsule directory, generation, group, threshold, broker public key, and state file are required")
	}
	if memberCount == 0 || options.Threshold > uint32(memberCount) {
		return fmt.Errorf("threshold must be satisfiable by at least one --member")
	}
	return nil
}

func newIndexEncryptCommand(globalOptions *global.Options) *cobra.Command {
	var options indexEncryptOptions
	command := &cobra.Command{
		Use:               "encrypt",
		Short:             "Migrate SlateDB metadata to authenticated encryption",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return options.finalize()
		},
		RunE: func(command *cobra.Command, _ []string) error {
			printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
			session, err := openReadDaemonSession(command.Context(), *globalOptions, options.Daemon, options.RepositoryID, printer)
			if err != nil {
				return err
			}
			defer session.CloseAndLog()
			if options.RepositoryID != session.Repository.Config().ID {
				return fmt.Errorf(
					"repository identity mismatch: configured %q, authoritative %q",
					options.RepositoryID,
					session.Repository.Config().ID,
				)
			}
			if _, _, err := mirrorCurrentEnvelope(session.Context, session.Repository.Backend(), session.Client); err != nil {
				return err
			}
			info := session.Client.Encryption()
			result := map[string]any{
				"enabled":             info.Enabled,
				"algorithm":           info.Algorithm,
				"active_dek_version":  info.ActiveDEKVersion,
				"envelope_generation": info.EnvelopeGeneration,
			}
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			} else {
				globalOptions.Term.Print(fmt.Sprintf(
					"metadata encrypted with %s; DEK version %d; envelope generation %d\n",
					info.Algorithm,
					info.ActiveDEKVersion,
					info.EnvelopeGeneration,
				))
			}
			return nil
		},
	}
	options.Daemon.AddFlags(command.Flags())
	command.Flags().StringVar(&options.RepositoryID, "repository-id", "", "repository identity bound to encrypted metadata")
	mustMarkFlagRequired(command, "repository-id")
	return command
}

func newIndexKeysCommand(globalOptions *global.Options) *cobra.Command {
	var options indexKeysOptions
	command := &cobra.Command{
		Use:               "keys",
		Short:             "Manage metadata encryption keys",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
	}
	options.Daemon.AddFlags(command.PersistentFlags())
	command.PersistentFlags().StringVar(&options.RepositoryID, "repository-id", "", "repository identity bound to the metadata key envelope")
	command.AddCommand(
		newIndexKeysStatusCommand(globalOptions, &options),
		newIndexKeysAddSlotCommand(globalOptions, &options),
		newIndexKeysRemoveSlotCommand(globalOptions, &options),
		newIndexKeysRotateKEKCommand(globalOptions, &options),
		newIndexKeysRotateDEKCommand(globalOptions, &options),
		newIndexKeysStoreMasterKeyCommand(globalOptions, &options),
		newIndexKeysEscrowCommand(globalOptions, &options),
		newIndexKeysMirrorEnvelopeCommand(globalOptions, &options),
		newIndexKeysQuorumCommand(globalOptions, &options),
	)
	return command
}

func newIndexKeysQuorumCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	command := &cobra.Command{
		Use:               "quorum",
		Short:             "Migrate and manage recovery capsule policy",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
	}
	command.AddCommand(
		newIndexKeysQuorumPrepareCommand(globalOptions, options),
		newIndexKeysQuorumFinalizeCommand(globalOptions, options),
		newIndexKeysQuorumVerifyCommand(globalOptions),
		newIndexKeysQuorumEnrollFIDO2Command(),
		newIndexKeysQuorumEnrollMacosSecureEnclaveCommand(),
		newIndexKeysQuorumGenerateAttestationKeyCommand(globalOptions),
		newIndexKeysQuorumAttestBypassesCommand(globalOptions),
		newIndexKeysQuorumMutationCommand(globalOptions, options, "create-group"),
		newIndexKeysQuorumMutationCommand(globalOptions, options, "add-member"),
		newIndexKeysQuorumMutationCommand(globalOptions, options, "remove-member"),
		newIndexKeysQuorumMutationCommand(globalOptions, options, "set-threshold"),
		newIndexKeysQuorumMutationCommand(globalOptions, options, "replace-member"),
		newIndexKeysQuorumResumeMutationCommand(globalOptions, options),
		newIndexKeysQuorumCancelMutationCommand(globalOptions),
	)
	return command
}

func newIndexKeysQuorumEnrollMacosSecureEnclaveCommand() *cobra.Command {
	var options hardwareEnrollmentOptions
	var custodianPath string
	command := &cobra.Command{
		Use:               "enroll-macos-secure-enclave",
		Short:             "Create a Touch ID-gated macOS Secure Enclave credential",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return options.finalizeMacOS()
		},
		RunE: func(command *cobra.Command, _ []string) (runErr error) {
			applicationTag := make([]byte, 32)
			if _, err := cryptorand.Read(applicationTag); err != nil {
				return fmt.Errorf("generate Secure Enclave application tag: %w", err)
			}
			defer clear(applicationTag)
			encodedTag := base64.RawURLEncoding.EncodeToString(applicationTag)
			helper := exec.CommandContext(command.Context(), custodianPath, "macos-secure-enclave-enroll", encodedTag)
			helper.Env = []string{}
			output, err := helper.Output()
			if err != nil {
				return fmt.Errorf("enroll macOS Secure Enclave credential: %w", err)
			}
			var result struct {
				ApplicationTag       string `json:"application_tag"`
				PublicKey            string `json:"public_key"`
				PublicKeyData        string `json:"public_key_data"`
				AccessControl        string `json:"access_control"`
				UserPresenceRequired bool   `json:"user_presence_required"`
			}
			decoder := json.NewDecoder(bytes.NewReader(output))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&result); err != nil || result.ApplicationTag != encodedTag || result.AccessControl != "biometry-current-set" ||
				!result.UserPresenceRequired {
				return fmt.Errorf("custodian helper returned invalid macOS Secure Enclave enrollment metadata")
			}
			publicKey, err := base64.RawURLEncoding.DecodeString(result.PublicKeyData)
			if err == nil {
				_, err = ecdh.P256().NewPublicKey(publicKey)
			}
			if err != nil || result.PublicKey != fmt.Sprintf("sha256:%x", sha256.Sum256(publicKey)) {
				return fmt.Errorf("custodian helper returned invalid macOS Secure Enclave public key metadata")
			}
			definition := externalPolicyMemberFile{
				MemberID: options.MemberID,
				Provider: "macos-secure-enclave",
				KeyReference: fmt.Sprintf(
					"secure-enclave:application-tag=%s;public-key=%s;access-control=%s",
					result.ApplicationTag,
					result.PublicKeyData,
					result.AccessControl,
				),
				Hardware:      &indexbroker.PolicyHardwareBinding{CredentialID: result.ApplicationTag, PublicKey: result.PublicKey, UserPresenceRequired: true},
				CustodianPath: custodianPath,
			}
			defer func() {
				if runErr == nil {
					return
				}
				rollbackContext, cancelRollback := context.WithTimeout(context.WithoutCancel(command.Context()), 30*time.Second)
				defer cancelRollback()
				rollback := exec.CommandContext(rollbackContext, custodianPath, "macos-secure-enclave-delete", definition.KeyReference)
				rollback.Env = []string{}
				if output, err := rollback.CombinedOutput(); err != nil {
					runErr = errors.Join(runErr, fmt.Errorf("secure Enclave enrollment rollback failed: %w: %s", err, strings.TrimSpace(string(output))))
				}
			}()
			return writeNewProtectedJSON(options.OutputPath, definition)
		},
	}
	command.Flags().StringVar(&options.MemberID, "member", "", "new Secure Enclave member ID")
	command.Flags().StringVar(&custodianPath, "custodian-path", "vaultic-key-custodian", "path to the hardware custodian executable")
	command.Flags().StringVar(&options.OutputPath, "output", "", "new mode-0600 external-member definition")
	return command
}

func newIndexKeysQuorumEnrollFIDO2Command() *cobra.Command {
	var options hardwareEnrollmentOptions
	var custodianPath string
	command := &cobra.Command{
		Use:               "enroll-fido2",
		Short:             "Create a PIN-and-touch FIDO2 hmac-secret credential",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return options.finalizeFIDO2()
		},
		RunE: func(command *cobra.Command, _ []string) error {
			helper := exec.CommandContext(command.Context(), custodianPath, "fido2-enroll", options.PINFile, options.RelyingPartyID)
			helper.Env = []string{}
			output, err := helper.Output()
			if err != nil {
				return fmt.Errorf("enroll FIDO2 credential: %w", err)
			}
			var result struct {
				CredentialID           string  `json:"credential_id"`
				PublicKey              string  `json:"public_key"`
				PublicKeyDER           string  `json:"public_key_der"`
				RelyingPartyID         string  `json:"relying_party_id"`
				AttestationFingerprint *string `json:"attestation_fingerprint"`
				UserPresenceRequired   bool    `json:"user_presence_required"`
			}
			decoder := json.NewDecoder(bytes.NewReader(output))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&result); err != nil || result.CredentialID == "" || result.PublicKey == "" || result.PublicKeyDER == "" ||
				result.RelyingPartyID != options.RelyingPartyID ||
				!result.UserPresenceRequired {
				return fmt.Errorf("custodian helper returned invalid FIDO2 enrollment metadata")
			}
			definition := externalPolicyMemberFile{
				MemberID: options.MemberID,
				Provider: "fido2-hmac-secret",
				KeyReference: fmt.Sprintf(
					"fido2:rp-id=%s;credential-id=%s;public-key-der=%s",
					options.RelyingPartyID, result.CredentialID, result.PublicKeyDER,
				),
				Hardware: &indexbroker.PolicyHardwareBinding{
					CredentialID:           result.CredentialID,
					PublicKey:              result.PublicKey,
					AttestationFingerprint: result.AttestationFingerprint,
					UserPresenceRequired:   true,
				},
				PINFile:       options.PINFile,
				CustodianPath: custodianPath,
			}
			if err := writeNewProtectedJSON(options.OutputPath, definition); err != nil {
				return err
			}
			return nil
		},
	}
	command.Flags().StringVar(&options.MemberID, "member", "", "new hardware member ID")
	command.Flags().StringVar(&options.RelyingPartyID, "relying-party-id", "", "FIDO2 relying-party ID")
	command.Flags().StringVar(&options.PINFile, "pin-file", "", "mode-0600 FIDO2 authenticator PIN file")
	command.Flags().StringVar(&custodianPath, "custodian-path", "vaultic-key-custodian", "path to the hardware custodian executable")
	command.Flags().StringVar(&options.OutputPath, "output", "", "new mode-0600 external-member definition")
	return command
}

func newIndexKeysQuorumMutationCommand(globalOptions *global.Options, options *indexKeysOptions, operation string) *cobra.Command {
	var capsulePath, capsuleDirectory, groupID, policyFile string
	var memberSpecs, externalMemberFiles []string
	var threshold uint32
	var acknowledgeDowngrade bool
	argumentRule := cobra.ExactArgs(1)
	usage := operation + " MEMBER"
	switch operation {
	case "create-group":
		usage = "create-group GROUP"
	case "set-threshold":
		usage = "set-threshold THRESHOLD"
	case "replace-member":
		usage = "replace-member OLD NEW"
		argumentRule = cobra.ExactArgs(2)
	}
	command := &cobra.Command{
		Use:               usage,
		Short:             "Create and publish a fresh recovery capsule policy generation",
		Args:              argumentRule,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, args []string) error {
			capsule, err := indexbroker.LoadCapsule(capsulePath)
			if err != nil {
				return err
			}
			if capsule.RepositoryID() != options.RepositoryID {
				return fmt.Errorf("recovery capsule repository identity mismatch")
			}
			current, err := capsule.PolicyDefinition()
			if err != nil {
				return err
			}
			policy, members, externalMembers, cleanup, err := mutationPolicyInputs(
				command.Context(), options.RepositoryID, current, operation, args, groupID, policyFile, threshold, memberSpecs, externalMemberFiles,
			)
			if err != nil {
				return err
			}
			defer cleanup()
			if acknowledgeDowngrade {
				observability.EmitBestEffort(
					command.Context(),
					observability.Event{
						Severity:  observability.Critical,
						Category:  observability.CategoryAuth,
						Component: "index",
						Message:   "recovery capsule policy downgrade acknowledged",
						Fields:    map[string]any{"repository_id": options.RepositoryID, "operation": operation},
					},
				)
			}
			brokerClient, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
			if err != nil {
				return err
			}
			status, err := brokerClient.Status(command.Context())
			if err != nil {
				vaulticerrors.LogClose(brokerClient, "close key broker client", log.Printf)
				return err
			}
			if err := matchQuorumCapsule(capsule.RepositoryID(), capsule.Generation(), capsule.LogicalID(), capsule.PolicyHash(), status); err != nil {
				vaulticerrors.LogClose(brokerClient, "close key broker client", log.Printf)
				return err
			}
			prepared, err := brokerClient.PreparePolicyMutation(
				command.Context(),
				globalOptions.KeyBrokerReleaseManifest,
				policy,
				members,
				externalMembers,
				acknowledgeDowngrade,
			)
			vaulticerrors.LogClose(brokerClient, "close key broker client", log.Printf)
			if err != nil {
				return err
			}
			return publishAndActivatePolicyMutation(
				command.Context(), globalOptions, options, operation, capsuleDirectory, status, prepared,
			)
		},
	}
	command.Flags().StringVar(&capsulePath, "capsule", "", "current immutable recovery capsule")
	command.Flags().StringVar(&capsuleDirectory, "capsule-directory", "", "deterministic local capsule generation directory")
	command.Flags().StringVar(&groupID, "group", "", "threshold group ID for create-group")
	command.Flags().StringVar(&policyFile, "policy-file", "", "complete resulting member/any_of/all_of/threshold policy JSON")
	command.Flags().Uint32Var(&threshold, "threshold", 0, "required contributions for create-group")
	command.Flags().StringArrayVar(&memberSpecs, "member", nil, "resulting member ID=PROVIDER:FILE (repeatable; all members required)")
	command.Flags().
		StringArrayVar(
			&externalMemberFiles,
			"external-member",
			nil,
			"mode-0600 cloud member enrollment JSON file (repeatable; all resulting external members required)",
		)
	command.Flags().BoolVar(&acknowledgeDowngrade, "acknowledge-policy-downgrade", false, "acknowledge a reduction in effective quorum strength")
	mustMarkFlagRequired(command, "capsule")
	mustMarkFlagRequired(command, "capsule-directory")
	return command
}

func mutationPolicyInputs(ctx context.Context, repositoryID string, current indexbroker.UnlockPolicy, operation string, args []string,
	groupID, policyFile string, threshold uint32, memberSpecs, externalMemberFiles []string,
) (indexbroker.UnlockPolicy, []indexbroker.OfflinePolicyMember, []indexbroker.ExternalPolicyMember, func(), error) {
	policy, err := resultingMutationPolicy(current, operation, args, groupID, policyFile, threshold)
	if err != nil {
		return indexbroker.UnlockPolicy{}, nil, nil, func() {}, err
	}
	members, credentials, err := parseOfflinePolicyMembers(memberSpecs)
	if err != nil {
		return indexbroker.UnlockPolicy{}, nil, nil, func() {}, err
	}
	externalMembers, tokens, err := parseExternalPolicyMembers(ctx, repositoryID, externalMemberFiles)
	cleanup := func() {
		clearPolicyCredentials(credentials)
		clearPolicyCredentials(tokens)
	}
	if err != nil {
		cleanup()
		return indexbroker.UnlockPolicy{}, nil, nil, func() {}, err
	}
	if operation == "create-group" && policyFile == "" {
		policy.Members = make([]string, 0, len(members)+len(externalMembers))
		for _, member := range members {
			policy.Members = append(policy.Members, member.MemberID)
		}
		for _, member := range externalMembers {
			policy.Members = append(policy.Members, member.MemberID)
		}
		sort.Strings(policy.Members)
	}
	if err := validatePolicyMemberCredentials(policy, members, externalMembers); err != nil {
		cleanup()
		return indexbroker.UnlockPolicy{}, nil, nil, func() {}, err
	}
	return policy, members, externalMembers, cleanup, nil
}

func resultingMutationPolicy(current indexbroker.UnlockPolicy, operation string, args []string, groupID, policyFile string,
	threshold uint32,
) (indexbroker.UnlockPolicy, error) {
	if policyFile == "" {
		return mutateThresholdPolicy(current, operation, args, groupID, threshold)
	}
	if operation == "create-group" && (threshold != 0 || groupID != "") {
		return indexbroker.UnlockPolicy{}, fmt.Errorf("--policy-file cannot be combined with --threshold or --group")
	}
	var policy indexbroker.UnlockPolicy
	if err := readPolicyDefinition(policyFile, &policy); err != nil {
		return indexbroker.UnlockPolicy{}, err
	}
	return policy, nil
}

func publishAndActivatePolicyMutation(ctx context.Context, globalOptions *global.Options, options *indexKeysOptions, operation,
	capsuleDirectory string, status indexbroker.Status, prepared indexbroker.PreparedPolicyMutation,
) error {
	observability.EmitBestEffort(ctx, observability.Event{Severity: observability.Notice, Category: observability.CategoryLifecycle, Component: "index",
		Message: "recovery capsule policy mutation prepared",
		Fields:  map[string]any{"repository_id": options.RepositoryID, "operation": operation, "capsule_sha256": prepared.CapsuleSHA256}})
	published, err := publishPreparedPolicyMutation(ctx, options, status, capsuleDirectory, prepared)
	if err != nil {
		observability.EmitBestEffort(ctx, observability.Event{Severity: observability.Warning, Category: observability.CategoryLifecycle, Component: "index",
			Message: "recovery capsule policy mutation publication interrupted; candidate retained for resume",
			Fields:  map[string]any{"repository_id": options.RepositoryID, "operation": operation, "capsule_sha256": prepared.CapsuleSHA256}})
		return fmt.Errorf("publish policy mutation (candidate retained; run quorum resume-mutation): %w", err)
	}
	if published.CapsuleSHA256 != prepared.CapsuleSHA256 {
		return fmt.Errorf("published capsule digest does not match broker candidate")
	}
	observability.EmitBestEffort(ctx, observability.Event{Severity: observability.Notice, Category: observability.CategoryLifecycle, Component: "index",
		Message: "recovery capsule generation published to local and repository mirrors",
		Fields: map[string]any{"repository_id": options.RepositoryID, "generation": published.Generation,
			"capsule_sha256": published.CapsuleSHA256, "local_path": published.LocalPath, "mirror_path": published.MirrorPath}})
	activationClient, err := indexbroker.Dial(ctx, globalOptions.KeyBrokerSocket)
	if err != nil {
		return fmt.Errorf("capsule was published but broker activation connection failed: %w", err)
	}
	defer vaulticerrors.CloseQuietly(activationClient)
	if err := activationClient.ActivatePolicyMutation(ctx, globalOptions.KeyBrokerReleaseManifest, prepared.CapsuleSHA256); err != nil {
		return fmt.Errorf("capsule was published but broker activation failed: %w", err)
	}
	observability.EmitBestEffort(ctx, observability.Event{Severity: observability.Critical, Category: observability.CategoryLifecycle, Component: "index",
		Message: "recovery capsule policy generation activated",
		Fields: map[string]any{
			"repository_id":  options.RepositoryID,
			"operation":      operation,
			"generation":     published.Generation,
			"capsule_sha256": published.CapsuleSHA256,
		}})
	if status.IdentityRecovery {
		observability.EmitBestEffort(ctx, observability.Event{Severity: observability.Critical, Category: observability.CategoryAuth, Component: "index",
			Message: "broker identity recovery completed and fresh identity pinned",
			Fields:  map[string]any{"repository_id": options.RepositoryID, "generation": published.Generation, "capsule_sha256": published.CapsuleSHA256}})
	}
	result := map[string]any{"operation": operation, "generation": published.Generation, "capsule_sha256": published.CapsuleSHA256,
		"local_path": published.LocalPath, "mirror_path": published.MirrorPath, "broker_locked": true}
	if globalOptions.JSON {
		globalOptions.Term.Print(ui.ToJSONString(result))
	} else {
		globalOptions.Term.Print(fmt.Sprintf("capsule generation %d published and activated; broker relocked\nlocal: %s\nmirror: %s\n",
			published.Generation, published.LocalPath, published.MirrorPath))
	}
	return nil
}

func newIndexKeysQuorumResumeMutationCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	var capsuleDirectory string
	command := &cobra.Command{
		Use:               "resume-mutation",
		Short:             "Resume publication and activation of the exact pending policy mutation",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			client, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
			if err != nil {
				return err
			}
			status, err := client.Status(command.Context())
			if err != nil {
				vaulticerrors.LogClose(client, "close activation client", log.Printf)
				return err
			}
			if !status.PolicyMutationPending || status.PendingCapsuleSHA256 == nil || status.PendingCapsuleGeneration == nil {
				vaulticerrors.LogClose(client, "close activation client", log.Printf)
				return fmt.Errorf("broker has no pending policy mutation")
			}
			prepared, err := client.PendingPolicyMutation(command.Context(), globalOptions.KeyBrokerReleaseManifest)
			vaulticerrors.LogClose(client, "close activation client", log.Printf)
			if err != nil {
				return err
			}
			if prepared.CapsuleSHA256 != *status.PendingCapsuleSHA256 {
				return fmt.Errorf("pending capsule digest changed during resume")
			}
			published, err := publishPreparedPolicyMutation(command.Context(), options, status, capsuleDirectory, prepared)
			if err != nil {
				return fmt.Errorf("resume policy mutation publication: %w", err)
			}
			activationClient, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
			if err != nil {
				return fmt.Errorf("capsule was published but broker activation connection failed: %w", err)
			}
			defer vaulticerrors.CloseQuietly(activationClient)
			if err := activationClient.ActivatePolicyMutation(command.Context(), globalOptions.KeyBrokerReleaseManifest, prepared.CapsuleSHA256); err != nil {
				return fmt.Errorf("capsule was published but broker activation failed: %w", err)
			}
			observability.EmitBestEffort(
				command.Context(),
				observability.Event{
					Severity:  observability.Critical,
					Category:  observability.CategoryLifecycle,
					Component: "index",
					Message:   "interrupted recovery capsule policy mutation resumed and activated",
					Fields: map[string]any{
						"repository_id":  options.RepositoryID,
						"generation":     published.Generation,
						"capsule_sha256": published.CapsuleSHA256,
					},
				},
			)
			if globalOptions.JSON {
				globalOptions.Term.Print(
					ui.ToJSONString(
						map[string]any{
							"resumed":        true,
							"generation":     published.Generation,
							"capsule_sha256": published.CapsuleSHA256,
							"local_path":     published.LocalPath,
							"mirror_path":    published.MirrorPath,
							"broker_locked":  true,
						},
					),
				)
			} else {
				globalOptions.Term.Print(fmt.Sprintf("capsule generation %d publication resumed and activated; broker relocked\n", published.Generation))
			}
			return nil
		},
	}
	command.Flags().StringVar(&capsuleDirectory, "capsule-directory", "", "deterministic local capsule generation directory")
	mustMarkFlagRequired(command, "capsule-directory")
	return command
}

func newIndexKeysQuorumCancelMutationCommand(globalOptions *global.Options) *cobra.Command {
	confirmation := keyConfirmationOptions{Action: "cancel the pending policy mutation"}
	command := &cobra.Command{
		Use:               "cancel-mutation",
		Short:             "Cancel an unpublished pending policy mutation",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return confirmation.finalize()
		},
		RunE: func(command *cobra.Command, _ []string) error {
			client, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
			if err != nil {
				return err
			}
			defer vaulticerrors.CloseQuietly(client)
			if err := client.CancelPolicyMutation(command.Context(), globalOptions.KeyBrokerReleaseManifest); err != nil {
				return err
			}
			observability.EmitBestEffort(
				command.Context(),
				observability.Event{
					Severity:  observability.Warning,
					Category:  observability.CategoryLifecycle,
					Component: "index",
					Message:   "pending recovery capsule policy mutation explicitly cancelled",
				},
			)
			return nil
		},
	}
	command.Flags().BoolVar(&confirmation.Confirm, "confirm", false, "confirm cancellation after verifying no candidate mirror was published")
	return command
}

func publishPreparedPolicyMutation(
	ctx context.Context,
	options *indexKeysOptions,
	status indexbroker.Status,
	capsuleDirectory string,
	prepared indexbroker.PreparedPolicyMutation,
) (daemon.PublishedCapsuleMutation, error) {
	if status.IdentityRecovery {
		daemonOptions, err := options.Daemon.config(options.RepositoryID)
		if err != nil {
			return daemon.PublishedCapsuleMutation{}, err
		}
		return daemon.PublishCapsuleWithoutDatabase(ctx, daemonOptions, capsuleDirectory, prepared.Capsule, prepared.CapsuleSHA256)
	}
	return withDaemonSession(ctx, options.Daemon, options.RepositoryID, func(client *daemon.Client) (daemon.PublishedCapsuleMutation, error) {
		return client.PublishCapsuleMutation(ctx, capsuleDirectory, prepared.Capsule, prepared.CapsuleSHA256, false)
	})
}

func mutateThresholdPolicy(
	current indexbroker.UnlockPolicy,
	operation string,
	args []string,
	groupID string,
	threshold uint32,
) (indexbroker.UnlockPolicy, error) {
	if operation == "create-group" {
		return createThresholdPolicy(args[0], groupID, threshold)
	}
	policy := current
	if policy.Type != "threshold" || policy.GroupID == "" {
		return indexbroker.UnlockPolicy{}, fmt.Errorf("%s currently requires a top-level threshold policy", operation)
	}
	var err error
	switch operation {
	case "add-member":
		policy.Members, err = addPolicyMember(policy.Members, args[0])
	case "remove-member":
		policy.Members, err = removePolicyMember(policy.Members, args[0])
	case "set-threshold":
		value, err := strconv.ParseUint(args[0], 10, 8)
		if err != nil || value == 0 {
			return indexbroker.UnlockPolicy{}, fmt.Errorf("threshold must be between 1 and 255")
		}
		policy.Required = uint8(value)
	case "replace-member":
		policy.Members, err = replacePolicyMember(policy.Members, args[0], args[1])
	default:
		return indexbroker.UnlockPolicy{}, fmt.Errorf("unsupported policy mutation %q", operation)
	}
	if err != nil {
		return indexbroker.UnlockPolicy{}, err
	}
	if len(policy.Members) == 0 || int(policy.Required) > len(policy.Members) {
		return indexbroker.UnlockPolicy{}, fmt.Errorf("threshold %d is not satisfiable by %d members", policy.Required, len(policy.Members))
	}
	sort.Strings(policy.Members)
	return policy, nil
}

func createThresholdPolicy(group, groupFlag string, threshold uint32) (indexbroker.UnlockPolicy, error) {
	if threshold == 0 || threshold > 255 {
		return indexbroker.UnlockPolicy{}, fmt.Errorf("--threshold must be between 1 and 255")
	}
	if groupFlag != "" && groupFlag != group {
		return indexbroker.UnlockPolicy{}, fmt.Errorf("group argument and --group must match")
	}
	return indexbroker.UnlockPolicy{Type: "threshold", GroupID: group, Required: uint8(threshold)}, nil
}

func addPolicyMember(members []string, added string) ([]string, error) {
	for _, member := range members {
		if member == added {
			return nil, fmt.Errorf("member %q already exists", added)
		}
	}
	return append(members, added), nil
}

func removePolicyMember(members []string, removed string) ([]string, error) {
	result := members[:0]
	for _, member := range members {
		if member != removed {
			result = append(result, member)
		}
	}
	if len(result) == len(members) {
		return nil, fmt.Errorf("member %q does not exist", removed)
	}
	return result, nil
}

func replacePolicyMember(members []string, oldMember, newMember string) ([]string, error) {
	if oldMember == newMember {
		return nil, fmt.Errorf("replacement member must have a different ID")
	}
	found := false
	for index, member := range members {
		if member == newMember {
			return nil, fmt.Errorf("replacement member %q already exists", newMember)
		}
		if member == oldMember {
			members[index] = newMember
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("member %q does not exist", oldMember)
	}
	return members, nil
}

func parseOfflinePolicyMembers(specs []string) ([]indexbroker.OfflinePolicyMember, [][]byte, error) {
	members := make([]indexbroker.OfflinePolicyMember, 0, len(specs))
	credentials := make([][]byte, 0, len(specs))
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		parts := strings.SplitN(spec, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			clearPolicyCredentials(credentials)
			return nil, credentials, fmt.Errorf("invalid --member %q; expected ID=PROVIDER:FILE", spec)
		}
		if _, exists := seen[parts[0]]; exists {
			clearPolicyCredentials(credentials)
			return nil, credentials, fmt.Errorf("duplicate member ID %q", parts[0])
		}
		providerAndFile := strings.SplitN(parts[1], ":", 2)
		if len(providerAndFile) != 2 || (providerAndFile[0] != "offline-argon2id" && providerAndFile[0] != "offline-keyfile") {
			clearPolicyCredentials(credentials)
			return nil, credentials, fmt.Errorf("invalid offline member provider in %q", spec)
		}
		credential, err := readProtectedBinary(providerAndFile[1], "capsule member credential", providerAndFile[0] == "offline-argon2id")
		if err != nil {
			clearPolicyCredentials(credentials)
			return nil, credentials, err
		}
		seen[parts[0]] = struct{}{}
		credentials = append(credentials, credential)
		members = append(
			members,
			indexbroker.OfflinePolicyMember{MemberID: parts[0], Provider: providerAndFile[0], Credential: base64.StdEncoding.EncodeToString(credential)},
		)
	}
	return members, credentials, nil
}

func clearPolicyCredentials(credentials [][]byte) {
	for _, credential := range credentials {
		clear(credential)
	}
}

func readPolicyDefinition(path string, policy *indexbroker.UnlockPolicy) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open resulting policy %q: %w", path, err)
	}
	defer vaulticerrors.CloseQuietly(file)
	decoder := json.NewDecoder(io.LimitReader(file, 1024*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(policy); err != nil {
		return fmt.Errorf("decode resulting policy %q: %w", path, err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("resulting policy %q contains trailing JSON data", path)
	}
	return nil
}

type externalPolicyMemberFile struct {
	MemberID        string                              `json:"member_id"`
	Provider        string                              `json:"provider"`
	KeyReference    string                              `json:"key_reference"`
	Principal       *indexbroker.PolicyPrincipalBinding `json:"principal"`
	Hardware        *indexbroker.PolicyHardwareBinding  `json:"hardware,omitempty"`
	BearerTokenFile string                              `json:"bearer_token_file,omitempty"`
	PINFile         string                              `json:"pin_file,omitempty"`
	CustodianPath   string                              `json:"custodian_path,omitempty"`
}

func parseExternalPolicyMembers(ctx context.Context, repositoryID string, paths []string) ([]indexbroker.ExternalPolicyMember, [][]byte, error) {
	members := make([]indexbroker.ExternalPolicyMember, 0, len(paths))
	tokens := make([][]byte, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		member, token, err := parseExternalPolicyMember(ctx, repositoryID, path)
		if err != nil {
			clearPolicyCredentials(tokens)
			return nil, tokens, err
		}
		if _, exists := seen[member.MemberID]; exists {
			clearPolicyCredentials(tokens)
			clear(token)
			return nil, tokens, fmt.Errorf("duplicate member ID %q", member.MemberID)
		}
		seen[member.MemberID] = struct{}{}
		if token != nil {
			tokens = append(tokens, token)
		}
		members = append(members, member)
	}
	return members, tokens, nil
}

func parseExternalPolicyMember(ctx context.Context, repositoryID, path string) (indexbroker.ExternalPolicyMember, []byte, error) {
	var definition externalPolicyMemberFile
	if err := readProtectedJSON(path, "external capsule member", &definition); err != nil {
		return indexbroker.ExternalPolicyMember{}, nil, err
	}
	if err := validateExternalPolicyMember(path, definition); err != nil {
		return indexbroker.ExternalPolicyMember{}, nil, err
	}
	token, tokenText, err := externalPolicyMemberToken(ctx, repositoryID, definition)
	if err != nil {
		return indexbroker.ExternalPolicyMember{}, nil, err
	}
	return indexbroker.ExternalPolicyMember{MemberID: definition.MemberID, Provider: definition.Provider, KeyReference: definition.KeyReference,
		Principal: definition.Principal, Hardware: definition.Hardware, BearerToken: tokenText}, token, nil
}

func validateExternalPolicyMember(path string, definition externalPolicyMemberFile) error {
	if definition.MemberID == "" || definition.KeyReference == "" || (definition.Principal == nil) == (definition.Hardware == nil) {
		return fmt.Errorf("external member %q requires member_id, key_reference, and exactly one principal or hardware binding", path)
	}
	switch definition.Provider {
	case "azure-key-vault", "aws-kms", "aws-cloudhsm", "gcp-kms", "gcp-cloud-hsm", "yubikey-piv", "fido2-hmac-secret", "macos-secure-enclave":
		return nil
	default:
		return fmt.Errorf("unsupported external member provider %q", definition.Provider)
	}
}

func externalPolicyMemberToken(ctx context.Context, repositoryID string, definition externalPolicyMemberFile) ([]byte, *string, error) {
	switch definition.Provider {
	case "azure-key-vault", "gcp-kms", "gcp-cloud-hsm":
		return readExternalMemberToken(definition, "cloud enrollment bearer token", "external member %q requires bearer_token_file")
	case "yubikey-piv":
		return readExternalMemberToken(definition, "YubiKey PIV enrollment PIN",
			"external member %q requires bearer_token_file containing the PIV PIN for enrollment")
	case "fido2-hmac-secret":
		return deriveFIDO2MemberToken(ctx, repositoryID, definition)
	case "macos-secure-enclave":
		if definition.BearerTokenFile != "" || definition.PINFile != "" {
			return nil, nil, fmt.Errorf("macOS Secure Enclave member %q must not configure a bearer token or PIN file", definition.MemberID)
		}
	default:
		if definition.BearerTokenFile != "" {
			return nil, nil, fmt.Errorf("AWS members use the SDK credential chain, not bearer_token_file")
		}
	}
	return nil, nil, nil
}

func readExternalMemberToken(definition externalPolicyMemberFile, description, missingFormat string) ([]byte, *string, error) {
	if definition.BearerTokenFile == "" {
		return nil, nil, fmt.Errorf(missingFormat, definition.MemberID)
	}
	value, err := readProtectedBinary(definition.BearerTokenFile, description, true)
	if err != nil {
		return nil, nil, err
	}
	text := string(value)
	return value, &text, nil
}

func deriveFIDO2MemberToken(ctx context.Context, repositoryID string, definition externalPolicyMemberFile) ([]byte, *string, error) {
	if definition.PINFile == "" {
		return nil, nil, fmt.Errorf("external member %q requires pin_file", definition.MemberID)
	}
	helper := definition.CustodianPath
	if helper == "" {
		helper = "vaultic-key-custodian"
	}
	command := exec.CommandContext(ctx, helper, "fido2-hmac-secret-derive", definition.PINFile, repositoryID, definition.MemberID, definition.KeyReference)
	command.Env = []string{}
	output, err := command.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("derive FIDO2 enrollment secret: %w", err)
	}
	value, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(output)))
	if err != nil || len(value) != 32 {
		return nil, nil, fmt.Errorf("FIDO2 custodian helper returned an invalid secret")
	}
	text := base64.StdEncoding.EncodeToString(value)
	return value, &text, nil
}

func validatePolicyMemberCredentials(
	policy indexbroker.UnlockPolicy,
	members []indexbroker.OfflinePolicyMember,
	externalMembers []indexbroker.ExternalPolicyMember,
) error {
	expected, err := policyMemberIDs(policy)
	if err != nil {
		return err
	}
	actual := make([]string, 0, len(members)+len(externalMembers))
	for _, member := range members {
		actual = append(actual, member.MemberID)
	}
	for _, member := range externalMembers {
		actual = append(actual, member.MemberID)
	}
	sort.Strings(expected)
	sort.Strings(actual)
	if strings.Join(expected, "\x00") != strings.Join(actual, "\x00") {
		return fmt.Errorf("--member credentials must match every resulting policy member exactly")
	}
	return nil
}

func policyMemberIDs(policy indexbroker.UnlockPolicy) ([]string, error) {
	var members []string
	seen := make(map[string]struct{})
	if err := collectPolicyMemberIDs(policy, seen, &members); err != nil {
		return nil, err
	}
	return members, nil
}

func collectPolicyMemberIDs(node indexbroker.UnlockPolicy, seen map[string]struct{}, members *[]string) error {
	switch node.Type {
	case "member":
		if node.MemberID == "" || len(node.Policies) != 0 || node.GroupID != "" || node.Required != 0 || len(node.Members) != 0 {
			return fmt.Errorf("member policy must contain only a non-empty member_id")
		}
		return addUniquePolicyMember(node.MemberID, seen, members)
	case "threshold":
		if node.GroupID == "" || node.Required == 0 || int(node.Required) > len(node.Members) || len(node.Policies) != 0 || node.MemberID != "" {
			return fmt.Errorf("threshold policy requires a group_id, a satisfiable required count, and members")
		}
		for _, member := range node.Members {
			if member == "" {
				return fmt.Errorf("threshold policy contains an empty member ID")
			}
			if err := addUniquePolicyMember(member, seen, members); err != nil {
				return err
			}
		}
		return nil
	case "any_of", "all_of":
		if len(node.Policies) == 0 || node.MemberID != "" || node.GroupID != "" || node.Required != 0 || len(node.Members) != 0 {
			return fmt.Errorf("%s policy must contain only non-empty policies", node.Type)
		}
		for _, child := range node.Policies {
			if err := collectPolicyMemberIDs(child, seen, members); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported policy type %q", node.Type)
	}
}

func addUniquePolicyMember(member string, seen map[string]struct{}, members *[]string) error {
	if _, exists := seen[member]; exists {
		return fmt.Errorf("policy member %q appears more than once", member)
	}
	seen[member] = struct{}{}
	*members = append(*members, member)
	return nil
}

func newIndexKeysQuorumVerifyCommand(globalOptions *global.Options) *cobra.Command {
	var capsulePath, brokerSocket string
	command := &cobra.Command{
		Use:               "verify",
		Short:             "Verify the current capsule and effective quorum policy",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			capsule, err := indexbroker.LoadCapsule(capsulePath)
			if err != nil {
				return err
			}
			client, err := indexbroker.Dial(command.Context(), brokerSocket)
			if err != nil {
				return err
			}
			defer vaulticerrors.CloseQuietly(client)
			status, err := client.Status(command.Context())
			if err != nil {
				return err
			}
			if err := verifyQuorumStatus(capsule.RepositoryID(), capsule.Generation(), capsule.LogicalID(), capsule.PolicyHash(), status); err != nil {
				return err
			}
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(status))
			} else {
				globalOptions.Term.Print(fmt.Sprintf(
					"capsule generation %d verified; minimum custodians %d; principal verified %t; "+
						"hardware verified %t; custody assumed %t\n",
					status.CapsuleGeneration,
					status.MinimumCustodians,
					status.PrincipalVerified,
					status.HardwareVerified,
					status.CustodyAssumed,
				))
			}
			return nil
		},
	}
	command.Flags().StringVar(&capsulePath, "capsule", "", "local immutable recovery capsule")
	command.Flags().StringVar(&brokerSocket, "broker-socket", "", "local key-broker Unix socket")
	mustMarkFlagRequired(command, "capsule")
	mustMarkFlagRequired(command, "broker-socket")
	return command
}

func verifyQuorumStatus(repositoryID string, generation uint64, logicalID, policyHash string, status indexbroker.Status) error {
	if err := matchQuorumCapsule(repositoryID, generation, logicalID, policyHash, status); err != nil {
		return err
	}
	if !status.Compliant {
		return fmt.Errorf("capsule policy is not quorum-compliant: %s", strings.Join(status.Findings, "; "))
	}
	return nil
}

func matchQuorumCapsule(repositoryID string, generation uint64, logicalID, policyHash string, status indexbroker.Status) error {
	if status.RepositoryID != repositoryID || status.CapsuleGeneration != generation || status.CapsuleLogicalID != logicalID ||
		status.PolicyHash != policyHash {
		return fmt.Errorf("broker capsule does not match local capsule: broker repository %q generation %d", status.RepositoryID, status.CapsuleGeneration)
	}
	return nil
}

func newIndexKeysQuorumPrepareCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	commandOptions := quorumPrepareOptions{Generation: 1, GroupID: "operators"}
	var memberSpecs []string
	command := &cobra.Command{
		Use:               "migrate-prepare",
		Short:             "Publish a verified capsule while retaining the database master key",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return commandOptions.finalize(len(memberSpecs))
		},
		RunE: func(command *cobra.Command, _ []string) error {
			publicKey, err := readBrokerPublicKey(commandOptions.BrokerPublicKeyFile)
			if err != nil {
				return err
			}
			members, cleanup, err := readOfflineCapsuleMembers(memberSpecs)
			if err != nil {
				return err
			}
			defer cleanup()
			migration, err := withDaemonSession(command.Context(), options.Daemon, options.RepositoryID,
				func(client *daemon.Client) (daemon.CapsuleMigration, error) {
					return client.PrepareCapsuleMigration(
						command.Context(), commandOptions.CapsuleDirectory, commandOptions.Generation,
						commandOptions.GroupID, commandOptions.Threshold, publicKey, members,
					)
				})
			if err != nil {
				return err
			}
			state := map[string]any{
				"format":         1,
				"repository_id":  options.RepositoryID,
				"generation":     migration.Generation,
				"capsule_sha256": migration.CapsuleSHA256,
				"local_path":     migration.LocalPath,
				"mirror_path":    migration.MirrorPath,
			}
			if err := writeNewProtectedJSON(commandOptions.StateFile, state); err != nil {
				return err
			}
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(state))
			} else {
				globalOptions.Term.Print(fmt.Sprintf(
					"capsule generation %d prepared at %s and mirrored at %s\nstate: %s\n"+
						"master key remains in database until migrate-finalize proves capsule access\n",
					migration.Generation,
					migration.LocalPath,
					migration.MirrorPath,
					commandOptions.StateFile,
				))
			}
			return nil
		},
	}
	command.Flags().StringVar(&commandOptions.CapsuleDirectory, "capsule-directory", "", "deterministic local capsule generation directory")
	command.Flags().Uint64Var(&commandOptions.Generation, "generation", 1, "new immutable capsule generation")
	command.Flags().StringVar(&commandOptions.GroupID, "group", "operators", "threshold group ID")
	command.Flags().Uint32Var(&commandOptions.Threshold, "threshold", 0, "required member contributions")
	command.Flags().StringVar(&commandOptions.BrokerPublicKeyFile, "broker-public-key", "", "broker Ed25519 public-key file")
	command.Flags().StringArrayVar(&memberSpecs, "member", nil, "offline member ID=PROVIDER:FILE (repeatable)")
	command.Flags().StringVar(&commandOptions.StateFile, "state-file", "", "new mode-0600 migration state file")
	return command
}

func readBrokerPublicKey(path string) ([]byte, error) {
	publicKey, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read broker identity public key: %w", err)
	}
	publicKey = bytes.TrimSpace(publicKey)
	if decoded, decodeErr := base64.StdEncoding.DecodeString(string(publicKey)); decodeErr == nil && len(decoded) == 32 {
		publicKey = decoded
	}
	if len(publicKey) != 32 {
		return nil, fmt.Errorf("broker identity public key must be 32 raw bytes or base64")
	}
	return publicKey, nil
}

func readOfflineCapsuleMembers(specs []string) ([]daemon.OfflineCapsuleMember, func(), error) {
	members := make([]daemon.OfflineCapsuleMember, 0, len(specs))
	cleanup := func() {
		for _, member := range members {
			clear(member.Credential)
		}
	}
	for _, spec := range specs {
		parts := strings.SplitN(spec, "=", 2)
		if len(parts) != 2 {
			cleanup()
			return nil, func() {}, fmt.Errorf("invalid --member %q; expected ID=PROVIDER:FILE", spec)
		}
		providerAndFile := strings.SplitN(parts[1], ":", 2)
		if len(providerAndFile) != 2 || (providerAndFile[0] != "offline-argon2id" && providerAndFile[0] != "offline-keyfile") {
			cleanup()
			return nil, func() {}, fmt.Errorf("invalid offline member provider in %q", spec)
		}
		credential, err := readProtectedBinary(providerAndFile[1], "capsule member credential", providerAndFile[0] == "offline-argon2id")
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
		members = append(members, daemon.OfflineCapsuleMember{ID: parts[0], Provider: providerAndFile[0], Credential: credential})
	}
	return members, cleanup, nil
}
