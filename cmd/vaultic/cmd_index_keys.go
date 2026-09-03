package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/global"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/cobra"
)

type indexKeysOptions struct {
	Daemon       indexDaemonOptions
	RepositoryID string
}

func newIndexEncryptCommand(globalOptions *global.Options) *cobra.Command {
	var daemonOptions indexDaemonOptions
	var repositoryID string
	command := &cobra.Command{Use: "encrypt", Short: "Migrate SlateDB metadata to authenticated encryption", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if daemonOptions.PassphraseFile == "" {
			return fmt.Errorf("--metadata-recovery-passphrase-file is required")
		}
		daemonOptions.EncryptionMode = "initialize"
		daemonOptions.Start = true
		commandCtx, err := indexDaemonContext(command.Context(), daemonOptions, repositoryID)
		if err != nil {
			return err
		}
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		ctx, repo, unlock, err := openWithReadLock(commandCtx, *globalOptions, globalOptions.NoLock, printer)
		if err != nil {
			return err
		}
		defer unlock()
		if repositoryID != repo.Config().ID {
			return fmt.Errorf("repository identity mismatch: configured %q, authoritative %q", repositoryID, repo.Config().ID)
		}
		client, err := daemonOptions.connect(ctx, repositoryID)
		if err != nil {
			return err
		}
		defer client.Close(ctx)
		if _, _, err := mirrorCurrentEnvelope(ctx, repo.Backend(), client); err != nil {
			return err
		}
		info := client.Encryption()
		result := map[string]any{"enabled": info.Enabled, "algorithm": info.Algorithm, "active_dek_version": info.ActiveDEKVersion, "envelope_generation": info.EnvelopeGeneration}
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(result))
		} else {
			globalOptions.Term.Print(fmt.Sprintf("metadata encrypted with %s; DEK version %d; envelope generation %d\n", info.Algorithm, info.ActiveDEKVersion, info.EnvelopeGeneration))
		}
		return nil
	}}
	daemonOptions.AddFlags(command.Flags())
	command.Flags().StringVar(&repositoryID, "repository-id", "", "repository identity bound to encrypted metadata")
	_ = command.MarkFlagRequired("repository-id")
	return command
}

func newIndexKeysCommand(globalOptions *global.Options) *cobra.Command {
	var options indexKeysOptions
	command := &cobra.Command{Use: "keys", Short: "Manage metadata encryption keys", Args: cobra.NoArgs, DisableAutoGenTag: true}
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
	command := &cobra.Command{Use: "quorum", Short: "Migrate and manage recovery capsule policy", Args: cobra.NoArgs, DisableAutoGenTag: true}
	command.AddCommand(
		newIndexKeysQuorumPrepareCommand(globalOptions, options),
		newIndexKeysQuorumFinalizeCommand(globalOptions, options),
		newIndexKeysQuorumVerifyCommand(globalOptions),
		newIndexKeysQuorumEnrollFIDO2Command(),
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

func newIndexKeysQuorumEnrollFIDO2Command() *cobra.Command {
	var memberID, relyingPartyID, pinFile, custodianPath, outputPath string
	command := &cobra.Command{Use: "enroll-fido2", Short: "Create a PIN-and-touch FIDO2 hmac-secret credential", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if memberID == "" || relyingPartyID == "" || pinFile == "" || outputPath == "" {
			return fmt.Errorf("--member, --relying-party-id, --pin-file, and --output are required")
		}
		output, err := exec.CommandContext(command.Context(), custodianPath, "fido2-enroll", pinFile, relyingPartyID).Output()
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
		if err := decoder.Decode(&result); err != nil || result.CredentialID == "" || result.PublicKey == "" || result.PublicKeyDER == "" || result.RelyingPartyID != relyingPartyID || !result.UserPresenceRequired {
			return fmt.Errorf("custodian helper returned invalid FIDO2 enrollment metadata")
		}
		definition := externalPolicyMemberFile{
			MemberID:      memberID,
			Provider:      "fido2-hmac-secret",
			KeyReference:  fmt.Sprintf("fido2:rp-id=%s;credential-id=%s;public-key-der=%s", relyingPartyID, result.CredentialID, result.PublicKeyDER),
			Hardware:      &indexbroker.PolicyHardwareBinding{CredentialID: result.CredentialID, PublicKey: result.PublicKey, AttestationFingerprint: result.AttestationFingerprint, UserPresenceRequired: true},
			PINFile:       pinFile,
			CustodianPath: custodianPath,
		}
		if err := writeNewProtectedJSON(outputPath, definition); err != nil {
			return err
		}
		return nil
	}}
	command.Flags().StringVar(&memberID, "member", "", "new hardware member ID")
	command.Flags().StringVar(&relyingPartyID, "relying-party-id", "", "FIDO2 relying-party ID")
	command.Flags().StringVar(&pinFile, "pin-file", "", "mode-0600 FIDO2 authenticator PIN file")
	command.Flags().StringVar(&custodianPath, "custodian-path", "vaultic-key-custodian", "path to the hardware custodian executable")
	command.Flags().StringVar(&outputPath, "output", "", "new mode-0600 external-member definition")
	return command
}

func newIndexKeysQuorumMutationCommand(globalOptions *global.Options, options *indexKeysOptions, operation string) *cobra.Command {
	var capsulePath, capsuleDirectory, groupID, policyFile string
	var memberSpecs, externalMemberFiles []string
	var threshold uint32
	var acknowledgeDowngrade bool
	argumentRule := cobra.ExactArgs(1)
	usage := operation + " MEMBER"
	if operation == "create-group" {
		usage = "create-group GROUP"
	} else if operation == "set-threshold" {
		usage = "set-threshold THRESHOLD"
	} else if operation == "replace-member" {
		usage = "replace-member OLD NEW"
		argumentRule = cobra.ExactArgs(2)
	}
	command := &cobra.Command{Use: usage, Short: "Create and publish a fresh recovery capsule policy generation", Args: argumentRule, DisableAutoGenTag: true, RunE: func(command *cobra.Command, args []string) error {
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
		var policy indexbroker.UnlockPolicy
		if policyFile != "" {
			if operation == "create-group" && (threshold != 0 || groupID != "") {
				return fmt.Errorf("--policy-file cannot be combined with --threshold or --group")
			}
			if err := readPolicyDefinition(policyFile, &policy); err != nil {
				return err
			}
		} else {
			policy, err = mutateThresholdPolicy(current, operation, args, groupID, threshold)
			if err != nil {
				return err
			}
		}
		members, credentials, err := parseOfflinePolicyMembers(memberSpecs)
		if err != nil {
			return err
		}
		defer func() {
			for _, credential := range credentials {
				clear(credential)
			}
		}()
		externalMembers, tokens, err := parseExternalPolicyMembers(command.Context(), options.RepositoryID, externalMemberFiles)
		if err != nil {
			return err
		}
		defer clearPolicyCredentials(tokens)
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
			return err
		}
		if acknowledgeDowngrade {
			_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Critical, Category: observability.CategoryAuth, Component: "index", Message: "recovery capsule policy downgrade acknowledged", Fields: map[string]any{"repository_id": options.RepositoryID, "operation": operation}})
		}
		brokerClient, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
		if err != nil {
			return err
		}
		status, err := brokerClient.Status(command.Context())
		if err != nil {
			_ = brokerClient.Close()
			return err
		}
		if err := matchQuorumCapsule(capsule.RepositoryID(), capsule.Generation(), capsule.LogicalID(), capsule.PolicyHash(), status); err != nil {
			_ = brokerClient.Close()
			return err
		}
		prepared, err := brokerClient.PreparePolicyMutation(command.Context(), globalOptions.KeyBrokerReleaseManifest, policy, members, externalMembers, acknowledgeDowngrade)
		_ = brokerClient.Close()
		if err != nil {
			return err
		}
		_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Notice, Category: observability.CategoryLifecycle, Component: "index", Message: "recovery capsule policy mutation prepared", Fields: map[string]any{"repository_id": options.RepositoryID, "operation": operation, "capsule_sha256": prepared.CapsuleSHA256}})
		published, err := publishPreparedPolicyMutation(command.Context(), options, status, capsuleDirectory, prepared)
		if err != nil {
			_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Warning, Category: observability.CategoryLifecycle, Component: "index", Message: "recovery capsule policy mutation publication interrupted; candidate retained for resume", Fields: map[string]any{"repository_id": options.RepositoryID, "operation": operation, "capsule_sha256": prepared.CapsuleSHA256}})
			return fmt.Errorf("publish policy mutation (candidate retained; run quorum resume-mutation): %w", err)
		}
		if published.CapsuleSHA256 != prepared.CapsuleSHA256 {
			return fmt.Errorf("published capsule digest does not match broker candidate")
		}
		_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Notice, Category: observability.CategoryLifecycle, Component: "index", Message: "recovery capsule generation published to local and repository mirrors", Fields: map[string]any{"repository_id": options.RepositoryID, "generation": published.Generation, "capsule_sha256": published.CapsuleSHA256, "local_path": published.LocalPath, "mirror_path": published.MirrorPath}})
		activationClient, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
		if err != nil {
			return fmt.Errorf("capsule was published but broker activation connection failed: %w", err)
		}
		defer activationClient.Close()
		if err := activationClient.ActivatePolicyMutation(command.Context(), globalOptions.KeyBrokerReleaseManifest, prepared.CapsuleSHA256); err != nil {
			return fmt.Errorf("capsule was published but broker activation failed: %w", err)
		}
		_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Critical, Category: observability.CategoryLifecycle, Component: "index", Message: "recovery capsule policy generation activated", Fields: map[string]any{"repository_id": options.RepositoryID, "operation": operation, "generation": published.Generation, "capsule_sha256": published.CapsuleSHA256}})
		if status.IdentityRecovery {
			_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Critical, Category: observability.CategoryAuth, Component: "index", Message: "broker identity recovery completed and fresh identity pinned", Fields: map[string]any{"repository_id": options.RepositoryID, "generation": published.Generation, "capsule_sha256": published.CapsuleSHA256}})
		}
		result := map[string]any{"operation": operation, "generation": published.Generation, "capsule_sha256": published.CapsuleSHA256, "local_path": published.LocalPath, "mirror_path": published.MirrorPath, "broker_locked": true}
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(result))
		} else {
			globalOptions.Term.Print(fmt.Sprintf("capsule generation %d published and activated; broker relocked\nlocal: %s\nmirror: %s\n", published.Generation, published.LocalPath, published.MirrorPath))
		}
		return nil
	}}
	command.Flags().StringVar(&capsulePath, "capsule", "", "current immutable recovery capsule")
	command.Flags().StringVar(&capsuleDirectory, "capsule-directory", "", "deterministic local capsule generation directory")
	command.Flags().StringVar(&groupID, "group", "", "threshold group ID for create-group")
	command.Flags().StringVar(&policyFile, "policy-file", "", "complete resulting member/any_of/all_of/threshold policy JSON")
	command.Flags().Uint32Var(&threshold, "threshold", 0, "required contributions for create-group")
	command.Flags().StringArrayVar(&memberSpecs, "member", nil, "resulting member ID=PROVIDER:FILE (repeatable; all members required)")
	command.Flags().StringArrayVar(&externalMemberFiles, "external-member", nil, "mode-0600 cloud member enrollment JSON file (repeatable; all resulting external members required)")
	command.Flags().BoolVar(&acknowledgeDowngrade, "acknowledge-policy-downgrade", false, "acknowledge a reduction in effective quorum strength")
	_ = command.MarkFlagRequired("capsule")
	_ = command.MarkFlagRequired("capsule-directory")
	return command
}

func newIndexKeysQuorumResumeMutationCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	var capsuleDirectory string
	command := &cobra.Command{Use: "resume-mutation", Short: "Resume publication and activation of the exact pending policy mutation", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		client, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
		if err != nil {
			return err
		}
		status, err := client.Status(command.Context())
		if err != nil {
			_ = client.Close()
			return err
		}
		if !status.PolicyMutationPending || status.PendingCapsuleSHA256 == nil || status.PendingCapsuleGeneration == nil {
			_ = client.Close()
			return fmt.Errorf("broker has no pending policy mutation")
		}
		prepared, err := client.PendingPolicyMutation(command.Context(), globalOptions.KeyBrokerReleaseManifest)
		_ = client.Close()
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
		defer activationClient.Close()
		if err := activationClient.ActivatePolicyMutation(command.Context(), globalOptions.KeyBrokerReleaseManifest, prepared.CapsuleSHA256); err != nil {
			return fmt.Errorf("capsule was published but broker activation failed: %w", err)
		}
		_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Critical, Category: observability.CategoryLifecycle, Component: "index", Message: "interrupted recovery capsule policy mutation resumed and activated", Fields: map[string]any{"repository_id": options.RepositoryID, "generation": published.Generation, "capsule_sha256": published.CapsuleSHA256}})
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(map[string]any{"resumed": true, "generation": published.Generation, "capsule_sha256": published.CapsuleSHA256, "local_path": published.LocalPath, "mirror_path": published.MirrorPath, "broker_locked": true}))
		} else {
			globalOptions.Term.Print(fmt.Sprintf("capsule generation %d publication resumed and activated; broker relocked\n", published.Generation))
		}
		return nil
	}}
	command.Flags().StringVar(&capsuleDirectory, "capsule-directory", "", "deterministic local capsule generation directory")
	_ = command.MarkFlagRequired("capsule-directory")
	return command
}

func newIndexKeysQuorumCancelMutationCommand(globalOptions *global.Options) *cobra.Command {
	var confirm bool
	command := &cobra.Command{Use: "cancel-mutation", Short: "Cancel an unpublished pending policy mutation", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if !confirm {
			return fmt.Errorf("--confirm is required")
		}
		client, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
		if err != nil {
			return err
		}
		defer client.Close()
		if err := client.CancelPolicyMutation(command.Context(), globalOptions.KeyBrokerReleaseManifest); err != nil {
			return err
		}
		_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Warning, Category: observability.CategoryLifecycle, Component: "index", Message: "pending recovery capsule policy mutation explicitly cancelled"})
		return nil
	}}
	command.Flags().BoolVar(&confirm, "confirm", false, "confirm cancellation after verifying no candidate mirror was published")
	return command
}

func publishPreparedPolicyMutation(ctx context.Context, options *indexKeysOptions, status indexbroker.Status, capsuleDirectory string, prepared indexbroker.PreparedPolicyMutation) (daemon.PublishedCapsuleMutation, error) {
	if status.IdentityRecovery {
		daemonOptions, err := options.Daemon.config(options.RepositoryID)
		if err != nil {
			return daemon.PublishedCapsuleMutation{}, err
		}
		return daemon.PublishCapsuleWithoutDatabase(ctx, daemonOptions, capsuleDirectory, prepared.Capsule, prepared.CapsuleSHA256)
	}
	client, err := options.Daemon.connect(ctx, options.RepositoryID)
	if err != nil {
		return daemon.PublishedCapsuleMutation{}, err
	}
	defer client.Close(ctx)
	return client.PublishCapsuleMutation(ctx, capsuleDirectory, prepared.Capsule, prepared.CapsuleSHA256, false)
}

func mutateThresholdPolicy(current indexbroker.UnlockPolicy, operation string, args []string, groupID string, threshold uint32) (indexbroker.UnlockPolicy, error) {
	policy := current
	if operation == "create-group" {
		if threshold == 0 || threshold > 255 {
			return indexbroker.UnlockPolicy{}, fmt.Errorf("--threshold must be between 1 and 255")
		}
		if groupID != "" && groupID != args[0] {
			return indexbroker.UnlockPolicy{}, fmt.Errorf("group argument and --group must match")
		}
		return indexbroker.UnlockPolicy{Type: "threshold", GroupID: args[0], Required: uint8(threshold)}, nil
	}
	if policy.Type != "threshold" || policy.GroupID == "" {
		return indexbroker.UnlockPolicy{}, fmt.Errorf("%s currently requires a top-level threshold policy", operation)
	}
	switch operation {
	case "add-member":
		for _, member := range policy.Members {
			if member == args[0] {
				return indexbroker.UnlockPolicy{}, fmt.Errorf("member %q already exists", args[0])
			}
		}
		policy.Members = append(policy.Members, args[0])
	case "remove-member":
		members := policy.Members[:0]
		for _, member := range policy.Members {
			if member != args[0] {
				members = append(members, member)
			}
		}
		if len(members) == len(policy.Members) {
			return indexbroker.UnlockPolicy{}, fmt.Errorf("member %q does not exist", args[0])
		}
		policy.Members = members
	case "set-threshold":
		value, err := strconv.ParseUint(args[0], 10, 8)
		if err != nil || value == 0 {
			return indexbroker.UnlockPolicy{}, fmt.Errorf("threshold must be between 1 and 255")
		}
		policy.Required = uint8(value)
	case "replace-member":
		if args[0] == args[1] {
			return indexbroker.UnlockPolicy{}, fmt.Errorf("replacement member must have a different ID")
		}
		found := false
		for index, member := range policy.Members {
			if member == args[1] {
				return indexbroker.UnlockPolicy{}, fmt.Errorf("replacement member %q already exists", args[1])
			}
			if member == args[0] {
				policy.Members[index] = args[1]
				found = true
			}
		}
		if !found {
			return indexbroker.UnlockPolicy{}, fmt.Errorf("member %q does not exist", args[0])
		}
	default:
		return indexbroker.UnlockPolicy{}, fmt.Errorf("unsupported policy mutation %q", operation)
	}
	if len(policy.Members) == 0 || int(policy.Required) > len(policy.Members) {
		return indexbroker.UnlockPolicy{}, fmt.Errorf("threshold %d is not satisfiable by %d members", policy.Required, len(policy.Members))
	}
	sort.Strings(policy.Members)
	return policy, nil
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
		members = append(members, indexbroker.OfflinePolicyMember{MemberID: parts[0], Provider: providerAndFile[0], Credential: base64.StdEncoding.EncodeToString(credential)})
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
	defer file.Close()
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
		var definition externalPolicyMemberFile
		if err := readProtectedJSON(path, "external capsule member", &definition); err != nil {
			clearPolicyCredentials(tokens)
			return nil, tokens, err
		}
		if definition.MemberID == "" || definition.KeyReference == "" || (definition.Principal == nil) == (definition.Hardware == nil) {
			clearPolicyCredentials(tokens)
			return nil, tokens, fmt.Errorf("external member %q requires member_id, key_reference, and exactly one principal or hardware binding", path)
		}
		if _, exists := seen[definition.MemberID]; exists {
			clearPolicyCredentials(tokens)
			return nil, tokens, fmt.Errorf("duplicate member ID %q", definition.MemberID)
		}
		if definition.Provider != "azure-key-vault" && definition.Provider != "aws-kms" && definition.Provider != "aws-cloudhsm" && definition.Provider != "gcp-kms" && definition.Provider != "gcp-cloud-hsm" && definition.Provider != "yubikey-piv" && definition.Provider != "fido2-hmac-secret" {
			clearPolicyCredentials(tokens)
			return nil, tokens, fmt.Errorf("unsupported external member provider %q", definition.Provider)
		}
		var token *string
		if definition.Provider == "azure-key-vault" || definition.Provider == "gcp-kms" || definition.Provider == "gcp-cloud-hsm" {
			if definition.BearerTokenFile == "" {
				clearPolicyCredentials(tokens)
				return nil, tokens, fmt.Errorf("external member %q requires bearer_token_file", definition.MemberID)
			}
			value, err := readProtectedBinary(definition.BearerTokenFile, "cloud enrollment bearer token", true)
			if err != nil {
				clearPolicyCredentials(tokens)
				return nil, tokens, err
			}
			tokens = append(tokens, value)
			text := string(value)
			token = &text
		} else if definition.Provider == "yubikey-piv" {
			if definition.BearerTokenFile == "" {
				clearPolicyCredentials(tokens)
				return nil, tokens, fmt.Errorf("external member %q requires bearer_token_file containing the PIV PIN for enrollment", definition.MemberID)
			}
			value, err := readProtectedBinary(definition.BearerTokenFile, "YubiKey PIV enrollment PIN", true)
			if err != nil {
				clearPolicyCredentials(tokens)
				return nil, tokens, err
			}
			tokens = append(tokens, value)
			text := string(value)
			token = &text
		} else if definition.Provider == "fido2-hmac-secret" {
			if definition.PINFile == "" {
				clearPolicyCredentials(tokens)
				return nil, tokens, fmt.Errorf("external member %q requires pin_file", definition.MemberID)
			}
			helper := definition.CustodianPath
			if helper == "" {
				helper = "vaultic-key-custodian"
			}
			output, err := exec.CommandContext(ctx, helper, "fido2-hmac-secret-derive", definition.PINFile, repositoryID, definition.MemberID, definition.KeyReference).Output()
			if err != nil {
				clearPolicyCredentials(tokens)
				return nil, tokens, fmt.Errorf("derive FIDO2 enrollment secret: %w", err)
			}
			value, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(output)))
			if err != nil || len(value) != 32 {
				clearPolicyCredentials(tokens)
				return nil, tokens, fmt.Errorf("FIDO2 custodian helper returned an invalid secret")
			}
			tokens = append(tokens, value)
			text := base64.StdEncoding.EncodeToString(value)
			token = &text
		} else if definition.BearerTokenFile != "" {
			clearPolicyCredentials(tokens)
			return nil, tokens, fmt.Errorf("AWS members use the SDK credential chain, not bearer_token_file")
		}
		seen[definition.MemberID] = struct{}{}
		members = append(members, indexbroker.ExternalPolicyMember{MemberID: definition.MemberID, Provider: definition.Provider, KeyReference: definition.KeyReference, Principal: definition.Principal, Hardware: definition.Hardware, BearerToken: token})
	}
	return members, tokens, nil
}

func validatePolicyMemberCredentials(policy indexbroker.UnlockPolicy, members []indexbroker.OfflinePolicyMember, externalMembers []indexbroker.ExternalPolicyMember) error {
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
	var visit func(indexbroker.UnlockPolicy) error
	visit = func(node indexbroker.UnlockPolicy) error {
		switch node.Type {
		case "member":
			if node.MemberID == "" || len(node.Policies) != 0 || node.GroupID != "" || node.Required != 0 || len(node.Members) != 0 {
				return fmt.Errorf("member policy must contain only a non-empty member_id")
			}
			if _, exists := seen[node.MemberID]; exists {
				return fmt.Errorf("policy member %q appears more than once", node.MemberID)
			}
			seen[node.MemberID] = struct{}{}
			members = append(members, node.MemberID)
		case "threshold":
			if node.GroupID == "" || node.Required == 0 || int(node.Required) > len(node.Members) || len(node.Policies) != 0 || node.MemberID != "" {
				return fmt.Errorf("threshold policy requires a group_id, a satisfiable required count, and members")
			}
			for _, member := range node.Members {
				if member == "" {
					return fmt.Errorf("threshold policy contains an empty member ID")
				}
				if _, exists := seen[member]; exists {
					return fmt.Errorf("policy member %q appears more than once", member)
				}
				seen[member] = struct{}{}
				members = append(members, member)
			}
		case "any_of", "all_of":
			if len(node.Policies) == 0 || node.MemberID != "" || node.GroupID != "" || node.Required != 0 || len(node.Members) != 0 {
				return fmt.Errorf("%s policy must contain only non-empty policies", node.Type)
			}
			for _, child := range node.Policies {
				if err := visit(child); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unsupported policy type %q", node.Type)
		}
		return nil
	}
	if err := visit(policy); err != nil {
		return nil, err
	}
	return members, nil
}

func newIndexKeysQuorumVerifyCommand(globalOptions *global.Options) *cobra.Command {
	var capsulePath, brokerSocket string
	command := &cobra.Command{Use: "verify", Short: "Verify the current capsule and effective quorum policy", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		capsule, err := indexbroker.LoadCapsule(capsulePath)
		if err != nil {
			return err
		}
		client, err := indexbroker.Dial(command.Context(), brokerSocket)
		if err != nil {
			return err
		}
		defer client.Close()
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
			globalOptions.Term.Print(fmt.Sprintf("capsule generation %d verified; minimum custodians %d; principal verified %t; hardware verified %t; custody assumed %t\n", status.CapsuleGeneration, status.MinimumCustodians, status.PrincipalVerified, status.HardwareVerified, status.CustodyAssumed))
		}
		return nil
	}}
	command.Flags().StringVar(&capsulePath, "capsule", "", "local immutable recovery capsule")
	command.Flags().StringVar(&brokerSocket, "broker-socket", "", "local key-broker Unix socket")
	_ = command.MarkFlagRequired("capsule")
	_ = command.MarkFlagRequired("broker-socket")
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
	if status.RepositoryID != repositoryID || status.CapsuleGeneration != generation || status.CapsuleLogicalID != logicalID || status.PolicyHash != policyHash {
		return fmt.Errorf("broker capsule does not match local capsule: broker repository %q generation %d", status.RepositoryID, status.CapsuleGeneration)
	}
	return nil
}

func newIndexKeysQuorumPrepareCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	var capsuleDirectory, groupID, brokerPublicKeyFile, stateFile string
	var generation uint64
	var threshold uint32
	var memberSpecs []string
	command := &cobra.Command{Use: "migrate-prepare", Short: "Publish a verified capsule while retaining the database master key", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if generation == 0 || threshold == 0 || groupID == "" || capsuleDirectory == "" || brokerPublicKeyFile == "" || stateFile == "" {
			return fmt.Errorf("capsule directory, generation, group, threshold, broker public key, and state file are required")
		}
		publicKey, err := os.ReadFile(brokerPublicKeyFile)
		if err != nil {
			return fmt.Errorf("read broker identity public key: %w", err)
		}
		publicKey = bytes.TrimSpace(publicKey)
		if decoded, decodeErr := base64.StdEncoding.DecodeString(string(publicKey)); decodeErr == nil && len(decoded) == 32 {
			publicKey = decoded
		}
		if len(publicKey) != 32 {
			return fmt.Errorf("broker identity public key must be 32 raw bytes or base64")
		}
		members := make([]daemon.OfflineCapsuleMember, 0, len(memberSpecs))
		for _, spec := range memberSpecs {
			parts := strings.SplitN(spec, "=", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid --member %q; expected ID=PROVIDER:FILE", spec)
			}
			providerAndFile := strings.SplitN(parts[1], ":", 2)
			if len(providerAndFile) != 2 || (providerAndFile[0] != "offline-argon2id" && providerAndFile[0] != "offline-keyfile") {
				return fmt.Errorf("invalid offline member provider in %q", spec)
			}
			credential, readErr := readProtectedBinary(providerAndFile[1], "capsule member credential", providerAndFile[0] == "offline-argon2id")
			if readErr != nil {
				return readErr
			}
			defer clear(credential)
			members = append(members, daemon.OfflineCapsuleMember{ID: parts[0], Provider: providerAndFile[0], Credential: credential})
		}
		if len(members) == 0 || threshold > uint32(len(members)) {
			return fmt.Errorf("threshold must be satisfiable by at least one --member")
		}
		client, err := options.Daemon.connect(command.Context(), options.RepositoryID)
		if err != nil {
			return err
		}
		defer client.Close(command.Context())
		migration, err := client.PrepareCapsuleMigration(command.Context(), capsuleDirectory, generation, groupID, threshold, publicKey, members)
		if err != nil {
			return err
		}
		state := map[string]any{"format": 1, "repository_id": options.RepositoryID, "generation": migration.Generation, "capsule_sha256": migration.CapsuleSHA256, "local_path": migration.LocalPath, "mirror_path": migration.MirrorPath}
		if err := writeNewProtectedJSON(stateFile, state); err != nil {
			return err
		}
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(state))
		} else {
			globalOptions.Term.Print(fmt.Sprintf("capsule generation %d prepared at %s and mirrored at %s\nstate: %s\nmaster key remains in database until migrate-finalize proves capsule access\n", migration.Generation, migration.LocalPath, migration.MirrorPath, stateFile))
		}
		return nil
	}}
	command.Flags().StringVar(&capsuleDirectory, "capsule-directory", "", "deterministic local capsule generation directory")
	command.Flags().Uint64Var(&generation, "generation", 1, "new immutable capsule generation")
	command.Flags().StringVar(&groupID, "group", "operators", "threshold group ID")
	command.Flags().Uint32Var(&threshold, "threshold", 0, "required member contributions")
	command.Flags().StringVar(&brokerPublicKeyFile, "broker-public-key", "", "broker Ed25519 public-key file")
	command.Flags().StringArrayVar(&memberSpecs, "member", nil, "offline member ID=PROVIDER:FILE (repeatable)")
	command.Flags().StringVar(&stateFile, "state-file", "", "new mode-0600 migration state file")
	return command
}

func newIndexKeysQuorumFinalizeCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	var stateFile string
	var confirm, retireLegacyRoutes, standaloneEscrowDestroyed bool
	command := &cobra.Command{Use: "migrate-finalize", Short: "Prove capsule pack access and remove the database master key", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if !confirm || !retireLegacyRoutes || !standaloneEscrowDestroyed || stateFile == "" {
			return fmt.Errorf("--state-file, --confirm, --retire-legacy-routes, and --confirm-standalone-escrow-destroyed are required")
		}
		var state struct {
			Format        int    `json:"format"`
			RepositoryID  string `json:"repository_id"`
			Generation    uint64 `json:"generation"`
			CapsuleSHA256 string `json:"capsule_sha256"`
			LocalPath     string `json:"local_path"`
			MirrorPath    string `json:"mirror_path"`
		}
		if err := readProtectedJSON(stateFile, "capsule migration state", &state); err != nil {
			return err
		}
		if state.Format != 1 || state.RepositoryID != options.RepositoryID || len(state.CapsuleSHA256) != 64 {
			return fmt.Errorf("migration state does not match repository")
		}
		proofOptions := *globalOptions
		proofOptions.MetadataLossRecovery = true
		printer := progress.NewTerminalPrinter(proofOptions.JSON, proofOptions.Verbosity, proofOptions.Term)
		repo, err := global.OpenRepository(command.Context(), proofOptions, printer)
		if err != nil {
			return fmt.Errorf("prove capsule repository access: %w", err)
		}
		defer repo.Close()
		var packProof bool
		err = repo.List(command.Context(), vaultic.PackFile, func(id vaultic.ID, size int64) error {
			if _, readErr := repo.ListPackHandles(command.Context(), id, size); readErr != nil {
				return readErr
			}
			packProof = true
			return errCapsulePackProofComplete
		})
		if err != nil && err != errCapsulePackProofComplete {
			_ = repo.Close()
			return fmt.Errorf("authenticate pack through capsule lease: %w", err)
		}
		brokerClient, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
		if err != nil {
			return fmt.Errorf("acquire capsule proof lease: %w", err)
		}
		defer brokerClient.Close()
		lease, err := brokerClient.AcquireLease(command.Context(), globalOptions.KeyBrokerReleaseManifest, "metadata-loss-recovery", globalOptions.KeyBrokerLeaseDuration)
		if err != nil {
			return fmt.Errorf("acquire capsule proof lease: %w", err)
		}
		proof := hmac.New(sha256.New, lease.Key)
		_, _ = proof.Write([]byte("vaultic-capsule-migration-finalize-v1\x00" + options.RepositoryID + "\x00" + state.CapsuleSHA256))
		brokerKeyProof := proof.Sum(nil)
		clear(lease.Key)
		defer clear(brokerKeyProof)
		client, err := options.Daemon.connect(command.Context(), options.RepositoryID)
		if err != nil {
			return err
		}
		defer client.Close(command.Context())
		retiredKeys, retiredEscrows, err := retireLegacyQuorumBypasses(command.Context(), repo.Backend())
		if err != nil {
			return fmt.Errorf("legacy bypass retirement is incomplete; database master key retained: %w", err)
		}
		if err := client.FinalizeCapsuleMigration(command.Context(), state.CapsuleSHA256, brokerKeyProof); err != nil {
			return fmt.Errorf("legacy repository routes retired but database master key retained: %w", err)
		}
		_ = observability.Emit(command.Context(), observability.Event{Severity: observability.Critical, Category: observability.CategoryLifecycle, Component: "index", Message: "capsule migration finalized and legacy key routes retired", Fields: map[string]any{"repository_id": state.RepositoryID, "capsule_generation": state.Generation, "retired_password_keys": retiredKeys, "retired_escrows": retiredEscrows}})
		result := map[string]any{"finalized": true, "generation": state.Generation, "capsule_sha256": state.CapsuleSHA256, "pack_authenticated": packProof, "retired_password_keys": retiredKeys, "retired_escrows": retiredEscrows}
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(result))
		} else {
			globalOptions.Term.Print(fmt.Sprintf("capsule migration finalized; database master key removed; pack authenticated: %t\n", packProof))
		}
		return nil
	}}
	command.Flags().StringVar(&stateFile, "state-file", "", "mode-0600 migration state from migrate-prepare")
	command.Flags().BoolVar(&confirm, "confirm", false, "confirm irreversible removal after capsule proof")
	command.Flags().BoolVar(&retireLegacyRoutes, "retire-legacy-routes", false, "remove all repository password keys and mirrored standalone escrow records after capsule proof")
	command.Flags().BoolVar(&standaloneEscrowDestroyed, "confirm-standalone-escrow-destroyed", false, "confirm externally copied standalone escrow and plaintext key files have been destroyed")
	return command
}

func retireLegacyQuorumBypasses(ctx context.Context, destination backend.Backend) (int, int, error) {
	var handles []backend.Handle
	if err := destination.List(ctx, backend.KeyFile, func(info backend.FileInfo) error {
		handles = append(handles, backend.Handle{Type: backend.KeyFile, Name: info.Name})
		return nil
	}); err != nil && !destination.IsNotExist(err) {
		return 0, 0, fmt.Errorf("inventory repository password keys: %w", err)
	}
	passwordKeys := len(handles)
	if err := destination.List(ctx, backend.SlateDBFile, func(info backend.FileInfo) error {
		if strings.HasPrefix(info.Name, "escrow-") && strings.HasSuffix(info.Name, ".json") {
			handles = append(handles, backend.Handle{Type: backend.SlateDBFile, Name: info.Name, IsMetadata: true})
		}
		return nil
	}); err != nil && !destination.IsNotExist(err) {
		return 0, 0, fmt.Errorf("inventory mirrored escrow records: %w", err)
	}
	escrows := len(handles) - passwordKeys
	for _, handle := range handles {
		if err := destination.Remove(ctx, handle); err != nil && !destination.IsNotExist(err) {
			return passwordKeys, escrows, fmt.Errorf("remove %s: %w", handle.Name, err)
		}
	}
	return passwordKeys, escrows, nil
}

var errCapsulePackProofComplete = fmt.Errorf("capsule pack proof complete")

func newIndexKeysMirrorEnvelopeCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	return &cobra.Command{Use: "mirror-envelope", Short: "Mirror the current key envelope into the repository backend", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		commandCtx, err := indexDaemonContext(command.Context(), options.Daemon, options.RepositoryID)
		if err != nil {
			return err
		}
		ctx, repo, unlock, err := openWithReadLock(commandCtx, *globalOptions, globalOptions.NoLock, printer)
		if err != nil {
			return err
		}
		defer unlock()
		connection := *options
		connection.RepositoryID = repo.Config().ID
		client, err := connection.Daemon.connect(ctx, connection.RepositoryID)
		if err != nil {
			return err
		}
		defer client.Close(ctx)
		generation, name, err := mirrorCurrentEnvelope(ctx, repo.Backend(), client)
		if err != nil {
			return err
		}
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(map[string]any{"generation": generation, "repository_mirror": name}))
		} else {
			globalOptions.Term.Print(fmt.Sprintf("envelope generation %d mirrored as %s\n", generation, name))
		}
		return nil
	}}
}

func mirrorCurrentEnvelope(ctx context.Context, destination backend.Backend, client *daemon.Client) (uint64, string, error) {
	envelope, generation, err := client.ExportKeyEnvelope(ctx)
	if err != nil {
		return 0, "", err
	}
	name := fmt.Sprintf("key-envelope-%020d.json", generation)
	handle := backend.Handle{Type: backend.SlateDBFile, Name: name, IsMetadata: true}
	if err := saveImmutableBackendRecord(ctx, destination, handle, envelope); err != nil {
		return 0, "", fmt.Errorf("mirror metadata key envelope: %w", err)
	}
	return generation, name, nil
}

func saveImmutableBackendRecord(ctx context.Context, destination backend.Backend, handle backend.Handle, payload []byte) error {
	compareExisting := func() (bool, error) {
		var existing []byte
		err := destination.Load(ctx, handle, 0, 0, func(reader io.Reader) error {
			var readErr error
			existing, readErr = io.ReadAll(io.LimitReader(reader, int64(len(payload))+1))
			return readErr
		})
		if err != nil {
			if destination.IsNotExist(err) {
				return false, nil
			}
			return false, err
		}
		if !bytes.Equal(existing, payload) {
			return false, fmt.Errorf("immutable backend record already exists with different content")
		}
		return true, nil
	}
	if same, err := compareExisting(); err != nil || same {
		return err
	}
	if err := destination.Save(ctx, handle, backend.NewByteReader(payload, destination.Hasher())); err != nil {
		if same, compareErr := compareExisting(); compareErr == nil && same {
			return nil
		}
		return err
	}
	return nil
}

func newIndexKeysEscrowCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	command := &cobra.Command{Use: "escrow", Short: "Create or recover cloud-wrapped repository master-key escrow", Args: cobra.NoArgs, DisableAutoGenTag: true}
	command.AddCommand(newIndexKeysEscrowCreateCommand(globalOptions, options), newIndexKeysEscrowRecoverCommand(options))
	return command
}

func newIndexKeysEscrowCreateCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	var escrowID, provider, keyReference, bearerTokenFile, recordFile string
	command := &cobra.Command{Use: "create", Short: "Wrap and mirror the stored repository master key", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if !validEscrowID(escrowID) || keyReference == "" {
			return fmt.Errorf("a safe --escrow-id and --key-reference are required")
		}
		var token []byte
		var err error
		if bearerTokenFile != "" {
			token, err = readProtectedSecret(bearerTokenFile, "cloud bearer token")
		}
		if err != nil {
			return err
		}
		defer clear(token)
		openOptions := *globalOptions
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		commandCtx, err := indexDaemonContext(command.Context(), options.Daemon, options.RepositoryID)
		if err != nil {
			return err
		}
		ctx, repo, unlock, err := openWithReadLock(commandCtx, openOptions, openOptions.NoLock, printer)
		if err != nil {
			return err
		}
		defer unlock()
		connection := *options
		connection.RepositoryID = repo.Config().ID
		client, err := connection.Daemon.connect(ctx, connection.RepositoryID)
		if err != nil {
			return err
		}
		defer client.Close(ctx)
		record, err := client.EscrowMasterKey(ctx, escrowID, provider, keyReference, token)
		if err != nil {
			return err
		}
		handle := backend.Handle{Type: backend.SlateDBFile, Name: "escrow-" + escrowID + ".json", IsMetadata: true}
		if err := repo.Backend().Save(ctx, handle, backend.NewByteReader(record, repo.Backend().Hasher())); err != nil {
			return fmt.Errorf("mirror escrow record in repository backend: %w", err)
		}
		if recordFile != "" {
			if err := os.WriteFile(recordFile, append(record, '\n'), 0o600); err != nil {
				return fmt.Errorf("write escrow recovery record: %w", err)
			}
		}
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(map[string]any{"escrow_id": escrowID, "provider": provider, "repository_mirror": handle.Name}))
		} else {
			globalOptions.Term.Print(fmt.Sprintf("escrow %s mirrored as %s\n", escrowID, handle.Name))
		}
		return nil
	}}
	command.Flags().StringVar(&escrowID, "escrow-id", "", "unique escrow record ID")
	command.Flags().StringVar(&provider, "provider", "", "key provider: aws-kms, azure-key-vault, gcp-kms, vault-transit, or pkcs11")
	command.Flags().StringVar(&keyReference, "key-reference", "", "versioned cloud KMS key reference")
	command.Flags().StringVar(&bearerTokenFile, "bearer-token-file", "", "protected provider token or PKCS#11 PIN file")
	command.Flags().StringVar(&recordFile, "record-file", "", "also write a mode-0600 standalone recovery record")
	_ = command.MarkFlagRequired("escrow-id")
	_ = command.MarkFlagRequired("provider")
	_ = command.MarkFlagRequired("key-reference")
	return command
}

func newIndexKeysEscrowRecoverCommand(options *indexKeysOptions) *cobra.Command {
	var recordFile, bearerTokenFile, outputKeyFile string
	command := &cobra.Command{Use: "recover", Short: "Recover a direct-open key from standalone escrow", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		record, err := os.ReadFile(recordFile)
		if err != nil {
			return fmt.Errorf("read escrow record: %w", err)
		}
		var token []byte
		if bearerTokenFile != "" {
			token, err = readProtectedSecret(bearerTokenFile, "cloud bearer token")
		}
		if err != nil {
			return err
		}
		defer clear(token)
		client, err := options.Daemon.connect(command.Context(), options.RepositoryID)
		if err != nil {
			return err
		}
		defer client.Close(command.Context())
		masterKey, err := client.RecoverEscrow(command.Context(), record, token)
		if err != nil {
			return err
		}
		defer clear(masterKey)
		file, err := os.OpenFile(outputKeyFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("create recovered key file: %w", err)
		}
		if _, err = file.Write(append(masterKey, '\n')); err == nil {
			err = file.Close()
		} else {
			_ = file.Close()
		}
		if err != nil {
			_ = os.Remove(outputKeyFile)
			return fmt.Errorf("write recovered key file: %w", err)
		}
		return nil
	}}
	command.Flags().StringVar(&recordFile, "record-file", "", "standalone escrow JSON record")
	command.Flags().StringVar(&bearerTokenFile, "bearer-token-file", "", "protected provider token or PKCS#11 PIN file")
	command.Flags().StringVar(&outputKeyFile, "output-key-file", "", "new mode-0600 file for vaultic --key-file")
	_ = command.MarkFlagRequired("record-file")
	_ = command.MarkFlagRequired("output-key-file")
	return command
}

func validEscrowID(value string) bool {
	return value != "" && strings.IndexFunc(value, func(character rune) bool {
		return !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_' || character == '.')
	}) == -1
}

func newIndexKeysRotateDEKCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	var confirm bool
	var resume bool
	var batchSize uint32
	command := &cobra.Command{Use: "rotate-dek", Short: "Generate and activate a new metadata DEK", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if !confirm {
			return fmt.Errorf("--confirm is required to rotate the metadata DEK")
		}
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		commandCtx, err := indexDaemonContext(command.Context(), options.Daemon, options.RepositoryID)
		if err != nil {
			return err
		}
		ctx, repo, unlock, err := openWithReadLock(commandCtx, *globalOptions, globalOptions.NoLock, printer)
		if err != nil {
			return err
		}
		defer unlock()
		connection := *options
		connection.RepositoryID = repo.Config().ID
		client, err := connection.Daemon.connect(ctx, connection.RepositoryID)
		if err != nil {
			return err
		}
		defer client.Close(ctx)
		if !resume {
			status, rotateErr := client.RotateDEK(ctx)
			if rotateErr != nil {
				_, _, _ = mirrorCurrentEnvelope(ctx, repo.Backend(), client)
				return rotateErr
			}
			if _, _, mirrorErr := mirrorCurrentEnvelope(ctx, repo.Backend(), client); mirrorErr != nil {
				return mirrorErr
			}
			printKeyStatus(globalOptions, status)
		}
		var total uint64
		for {
			progress, rewriteErr := client.RewriteDEK(ctx, batchSize)
			if rewriteErr != nil {
				_, _, _ = mirrorCurrentEnvelope(ctx, repo.Backend(), client)
				return rewriteErr
			}
			total += progress.Rewritten
			if progress.Remaining == 0 {
				break
			}
		}
		if _, _, err := mirrorCurrentEnvelope(ctx, repo.Backend(), client); err != nil {
			return err
		}
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(map[string]any{"rewrite_complete": true, "rewritten": total}))
		} else {
			globalOptions.Term.Print(fmt.Sprintf("DEK rewrite complete; objects rewritten: %d\n", total))
		}
		return nil
	}}
	command.Flags().BoolVar(&confirm, "confirm", false, "confirm generation and activation of a new metadata DEK")
	command.Flags().BoolVar(&resume, "resume", false, "resume the active DEK rewrite without generating another key")
	command.Flags().Uint32Var(&batchSize, "batch-size", 128, "maximum objects rewritten per checkpointed batch")
	return command
}

func newIndexKeysStoreMasterKeyCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	var confirm bool
	command := &cobra.Command{Use: "store-master-key", Short: "Store the repository master key in encrypted metadata", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if !confirm {
			return fmt.Errorf("--confirm is required to store the repository master key")
		}
		openOptions := *globalOptions
		openOptions.MetadataKeyInDB = false
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		commandCtx, err := indexDaemonContext(command.Context(), options.Daemon, options.RepositoryID)
		if err != nil {
			return err
		}
		ctx, repo, unlock, err := openWithReadLock(commandCtx, openOptions, openOptions.NoLock, printer)
		if err != nil {
			return err
		}
		defer unlock()
		raw, err := json.Marshal(repo.Key())
		if err != nil {
			return fmt.Errorf("serialize repository master key: %w", err)
		}
		encoded := []byte(base64.StdEncoding.EncodeToString(raw))
		clear(raw)
		defer clear(encoded)
		connection := *options
		connection.RepositoryID = repo.Config().ID
		client, err := connection.Daemon.connect(ctx, connection.RepositoryID)
		if err != nil {
			return err
		}
		defer client.Close(ctx)
		if err := client.StoreMasterKey(ctx, encoded); err != nil {
			return err
		}
		if globalOptions.JSON {
			globalOptions.Term.Print("{\"stored\":true}")
		} else {
			globalOptions.Term.Print("repository master key stored in encrypted metadata\n")
		}
		return nil
	}}
	command.Flags().BoolVar(&confirm, "confirm", false, "confirm storage of the repository master key in encrypted metadata")
	return command
}

func newIndexKeysStatusCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	var capsulePath string
	command := &cobra.Command{Use: "status", Short: "Show redacted metadata and quorum key status", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		status, err := withKeyClient(command.Context(), *options, func(client *daemon.Client) (daemon.KeyStatus, error) { return client.KeyStatus(command.Context()) })
		if err != nil {
			return err
		}
		if capsulePath == "" {
			printKeyStatus(globalOptions, status)
			return nil
		}
		if options.Daemon.BrokerSocket == "" {
			return fmt.Errorf("--capsule requires --metadata-key-broker-socket")
		}
		capsule, err := indexbroker.LoadCapsule(capsulePath)
		if err != nil {
			return err
		}
		brokerClient, err := indexbroker.Dial(command.Context(), options.Daemon.BrokerSocket)
		if err != nil {
			return err
		}
		defer brokerClient.Close()
		quorum, err := brokerClient.Status(command.Context())
		if err != nil {
			return err
		}
		if err := matchQuorumCapsule(capsule.RepositoryID(), capsule.Generation(), capsule.LogicalID(), capsule.PolicyHash(), quorum); err != nil {
			return err
		}
		findings := quorumAccessRouteFindings(*globalOptions, status)
		findings = append(findings, quorum.Findings...)
		compliant := quorum.Compliant && len(findings) == 0
		result := struct {
			Metadata  daemon.KeyStatus   `json:"metadata"`
			Quorum    indexbroker.Status `json:"quorum"`
			Compliant bool               `json:"compliant"`
			Findings  []string           `json:"findings,omitempty"`
		}{Metadata: status, Quorum: quorum, Compliant: compliant, Findings: findings}
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(result))
		} else {
			printKeyStatus(globalOptions, status)
			globalOptions.Term.Print(fmt.Sprintf("quorum generation %d; minimum custodians %d; compliant %t\n", quorum.CapsuleGeneration, quorum.MinimumCustodians, compliant))
			for _, finding := range findings {
				globalOptions.Term.Print("non-compliant: " + finding + "\n")
			}
		}
		return nil
	}}
	command.Flags().StringVar(&capsulePath, "capsule", "", "local immutable recovery capsule to verify against the running broker")
	return command
}

func quorumAccessRouteFindings(options global.Options, metadata daemon.KeyStatus) []string {
	var findings []string
	configured := []struct {
		active bool
		name   string
	}{
		{options.Password != "" || options.PasswordFile != "" || options.PasswordCommand != "", "ordinary repository password route configured"},
		{options.MasterKey != "" || options.MasterKeyFile != "" || options.MasterKeyCommand != "", "direct repository master-key route configured"},
		{options.AzureKeyVaultURL != "", "legacy Azure secret route configured"},
		{options.InsecureNoPassword, "insecure no-password route configured"},
		{options.MetadataKeyInDB, "repository master-key-in-database route configured"},
		{options.MetadataPassphraseFile != "", "standalone metadata passphrase route configured"},
	}
	for _, route := range configured {
		if route.active {
			findings = append(findings, route.name)
		}
	}
	for _, slot := range metadata.Slots {
		findings = append(findings, fmt.Sprintf("standalone metadata DEK slot %q (%s) remains", slot.ID, slot.Provider))
	}
	if metadata.PendingCapsuleMigrationSHA256 != "" {
		findings = append(findings, "capsule migration is prepared but not finalized")
	}
	return findings
}

func newIndexKeysAddSlotCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	var slotID, provider, keyReference, passphraseFile, bearerTokenFile string
	var priority uint32
	var recovery bool
	command := &cobra.Command{Use: "add-slot", Short: "Add an Argon2id-wrapped metadata key slot", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		var secret []byte
		var err error
		if provider == "local-argon2id" {
			if passphraseFile == "" || bearerTokenFile != "" || keyReference != "" {
				return fmt.Errorf("local-argon2id requires --passphrase-file and does not accept cloud key options")
			}
			secret, err = readProtectedSecret(passphraseFile, "metadata passphrase")
		} else {
			if passphraseFile != "" || recovery || keyReference == "" {
				return fmt.Errorf("cloud slots require --key-reference and do not accept --passphrase-file or --recovery")
			}
			if bearerTokenFile != "" {
				secret, err = readProtectedSecret(bearerTokenFile, "cloud bearer token")
			}
			if provider != "aws-kms" && bearerTokenFile == "" {
				return fmt.Errorf("%s requires --bearer-token-file containing its token or PIN", provider)
			}
		}
		if err != nil {
			return err
		}
		defer clear(secret)
		status, err := withMirroredKeyMutation(command.Context(), globalOptions, *options, func(client *daemon.Client) (daemon.KeyStatus, error) {
			if provider == "local-argon2id" {
				return client.AddLocalKeySlot(command.Context(), slotID, secret, priority, recovery)
			}
			return client.AddCloudKeySlot(command.Context(), slotID, provider, keyReference, secret, priority)
		})
		if err == nil {
			printKeyStatus(globalOptions, status)
		}
		return err
	}}
	command.Flags().StringVar(&slotID, "slot", "", "unique metadata key slot ID")
	command.Flags().StringVar(&provider, "provider", "local-argon2id", "key provider: local-argon2id, aws-kms, azure-key-vault, gcp-kms, vault-transit, or pkcs11")
	command.Flags().StringVar(&keyReference, "key-reference", "", "versioned cloud KMS key reference")
	command.Flags().StringVar(&passphraseFile, "passphrase-file", "", "mode-0600 file containing the new slot passphrase")
	command.Flags().StringVar(&bearerTokenFile, "bearer-token-file", "", "mode-0600 Azure or GCP access-token file")
	command.Flags().Uint32Var(&priority, "priority", 100, "unlock priority (lower is preferred)")
	command.Flags().BoolVar(&recovery, "recovery", false, "mark this slot as an offline recovery mechanism")
	_ = command.MarkFlagRequired("slot")
	return command
}

func newIndexKeysRemoveSlotCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	var slotID string
	var confirm bool
	command := &cobra.Command{Use: "remove-slot", Short: "Remove a metadata key slot", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if !confirm {
			return fmt.Errorf("--confirm is required to remove a metadata key slot")
		}
		status, err := withMirroredKeyMutation(command.Context(), globalOptions, *options, func(client *daemon.Client) (daemon.KeyStatus, error) {
			return client.RemoveKeySlot(command.Context(), slotID)
		})
		if err == nil {
			printKeyStatus(globalOptions, status)
		}
		return err
	}}
	command.Flags().StringVar(&slotID, "slot", "", "metadata key slot ID")
	command.Flags().BoolVar(&confirm, "confirm", false, "confirm permanent removal of the wrapping slot")
	_ = command.MarkFlagRequired("slot")
	return command
}

func newIndexKeysRotateKEKCommand(globalOptions *global.Options, options *indexKeysOptions) *cobra.Command {
	var slotID, passphraseFile string
	command := &cobra.Command{Use: "rotate-kek", Short: "Rewrap the metadata key under a new local KEK", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		passphrase, err := readProtectedSecret(passphraseFile, "metadata passphrase")
		if err != nil {
			return err
		}
		defer clear(passphrase)
		status, err := withMirroredKeyMutation(command.Context(), globalOptions, *options, func(client *daemon.Client) (daemon.KeyStatus, error) {
			return client.RotateLocalKeySlot(command.Context(), slotID, passphrase)
		})
		if err == nil {
			printKeyStatus(globalOptions, status)
		}
		return err
	}}
	command.Flags().StringVar(&slotID, "slot", "", "local metadata key slot ID")
	command.Flags().StringVar(&passphraseFile, "passphrase-file", "", "mode-0600 file containing the replacement passphrase")
	_ = command.MarkFlagRequired("slot")
	_ = command.MarkFlagRequired("passphrase-file")
	return command
}

func withKeyClient(ctx context.Context, options indexKeysOptions, operation func(*daemon.Client) (daemon.KeyStatus, error)) (daemon.KeyStatus, error) {
	client, err := options.Daemon.connect(ctx, options.RepositoryID)
	if err != nil {
		return daemon.KeyStatus{}, err
	}
	defer client.Close(ctx)
	return operation(client)
}

func withMirroredKeyMutation(ctx context.Context, globalOptions *global.Options, options indexKeysOptions, operation func(*daemon.Client) (daemon.KeyStatus, error)) (daemon.KeyStatus, error) {
	printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
	ctx, err := indexDaemonContext(ctx, options.Daemon, options.RepositoryID)
	if err != nil {
		return daemon.KeyStatus{}, err
	}
	ctx, repo, unlock, err := openWithReadLock(ctx, *globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return daemon.KeyStatus{}, err
	}
	defer unlock()
	options.RepositoryID = repo.Config().ID
	client, err := options.Daemon.connect(ctx, options.RepositoryID)
	if err != nil {
		return daemon.KeyStatus{}, err
	}
	defer client.Close(ctx)
	status, err := operation(client)
	_, _, mirrorErr := mirrorCurrentEnvelope(ctx, repo.Backend(), client)
	if err != nil {
		if mirrorErr != nil {
			return daemon.KeyStatus{}, fmt.Errorf("key operation failed: %w (current envelope mirror also failed: %v)", err, mirrorErr)
		}
		return daemon.KeyStatus{}, err
	}
	if mirrorErr != nil {
		return daemon.KeyStatus{}, mirrorErr
	}
	return status, nil
}

func indexDaemonContext(ctx context.Context, options indexDaemonOptions, repositoryID string) (context.Context, error) {
	config, err := options.config(repositoryID)
	if err != nil {
		return nil, err
	}
	return repository.WithDaemonOptions(ctx, config), nil
}

func printKeyStatus(globalOptions *global.Options, status daemon.KeyStatus) {
	if globalOptions.JSON {
		globalOptions.Term.Print(ui.ToJSONString(status))
		return
	}
	globalOptions.Term.Print(fmt.Sprintf("envelope generation %d; active DEK version %d\n", status.EnvelopeGeneration, status.ActiveDEKVersion))
	for _, slot := range status.Slots {
		globalOptions.Term.Print(fmt.Sprintf("%s: %s (%s), priority %d, DEK %d, recovery %t\n", slot.ID, slot.Provider, slot.KeyReference, slot.Priority, slot.DEKVersion, slot.Recovery))
	}
	if status.PendingCapsuleMigrationSHA256 != "" {
		globalOptions.Term.Print("capsule migration pending: " + status.PendingCapsuleMigrationSHA256 + "\n")
	}
	if status.FinalizedCapsuleMigrationSHA256 != "" {
		globalOptions.Term.Print("capsule migration finalized: " + status.FinalizedCapsuleMigrationSHA256 + "\n")
	}
}

func readMetadataPassphrase(path string) ([]byte, error) {
	return readProtectedSecret(path, "metadata passphrase")
}

func readProtectedSecret(path, description string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect %s file: %w", description, err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s file must not be accessible by group or others", description)
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s file: %w", description, err)
	}
	value = bytes.TrimRight(value, " \t\r\n\v\f")
	if len(value) == 0 {
		return nil, fmt.Errorf("%s file is empty", description)
	}
	return value, nil
}
