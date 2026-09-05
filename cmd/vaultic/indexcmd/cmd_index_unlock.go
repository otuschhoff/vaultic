package indexcmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	vaulticerrors "github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/spf13/cobra"
)

type indexUnlockOptions struct {
	Socket  string
	Capsule string
}

type unlockCredentialOptions struct {
	passphraseFile     string
	keyFile            string
	azureTokenFile     string
	gcpTokenFile       string
	yubikeyPINFile     string
	fido2PINFile       string
	custodianPath      string
	awsKMS             bool
	macosSecureEnclave bool
}

type unlockContributeOptions struct {
	sessionFile, memberID, confirmedFingerprint string
	prepare, unverifiedSession                  bool
	credentials                                 unlockCredentialOptions
}

func (commandOptions unlockContributeOptions) finalize(parent indexUnlockOptions) error {
	if parent.Capsule == "" || commandOptions.sessionFile == "" {
		return fmt.Errorf("--capsule and --session-file are required")
	}
	if commandOptions.prepare {
		return nil
	}
	if commandOptions.memberID == "" || commandOptions.confirmedFingerprint == "" ||
		commandOptions.credentials.routeCount() != 1 {
		return fmt.Errorf("--member, --confirm-fingerprint, and exactly one custodian credential route are required")
	}
	return nil
}

type unlockLockOptions struct {
	confirm bool
}

func (options unlockLockOptions) finalize() error {
	if !options.confirm {
		return fmt.Errorf("--confirm is required to revoke all broker leases")
	}
	return nil
}

type unlockContributionCapsule interface {
	ContributeOfflineSession(
		indexbroker.SignedSession,
		string,
		string,
		[]byte,
		bool,
		uint64,
		time.Time,
		bool,
	) (indexbroker.EncryptedContribution, error)
	ContributeExternalSession(
		context.Context, indexbroker.SignedSession, string, string, indexbroker.ExternalMemberUnwrapper, uint64, time.Time, bool,
	) (indexbroker.EncryptedContribution, error)
}

func newIndexUnlockCommand(globalOptions *global.Options) *cobra.Command {
	var options indexUnlockOptions
	command := &cobra.Command{
		Use:               "unlock",
		Short:             "Operate the local quorum key broker",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
	}
	command.PersistentFlags().StringVar(&options.Socket, "broker-socket", "", "local vaultic-key-broker Unix socket")
	command.PersistentFlags().StringVar(&options.Capsule, "capsule", "", "local immutable recovery capsule")
	command.AddCommand(
		newIndexUnlockStatusCommand(globalOptions, &options),
		newIndexUnlockContributeCommand(globalOptions, &options),
		newIndexUnlockLockCommand(globalOptions, &options),
	)
	return command
}

func newIndexUnlockStatusCommand(globalOptions *global.Options, options *indexUnlockOptions) *cobra.Command {
	return &cobra.Command{
		Use:               "status",
		Short:             "Show the key broker lock and lease state",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			client, err := dialIndexBroker(command.Context(), options.Socket)
			if err != nil {
				return err
			}
			defer vaulticerrors.CloseQuietly(client)
			status, err := client.Status(command.Context())
			if err != nil {
				return err
			}
			printUnlockStatus(globalOptions, status)
			return nil
		},
	}
}

func printUnlockStatus(globalOptions *global.Options, status indexbroker.Status) {
	if globalOptions.JSON {
		globalOptions.Term.Print(ui.ToJSONString(status))
		return
	}
	state := "unlocked"
	if status.Locked {
		state = "locked"
	}
	globalOptions.Term.Print(fmt.Sprintf(
		"broker %s; repository %s; capsule generation %d; minimum custodians %d; compliant %t; sessions %d; leases %d\n",
		state,
		status.RepositoryID,
		status.CapsuleGeneration,
		status.MinimumCustodians,
		status.Compliant,
		status.ActiveSessions,
		status.ActiveLeases,
	))
	globalOptions.Term.Print(fmt.Sprintf("assurance: principal-verified=%t hardware-verified=%t custody-assumed=%t\n",
		status.PrincipalVerified, status.HardwareVerified, status.CustodyAssumed))
	if status.IdentityRecovery {
		globalOptions.Term.Print(
			"identity recovery: active; leases disabled until a fresh capsule generation repins this broker\n",
		)
	}
	if status.PolicyMutationPending {
		generation, digest := "unknown", "unknown"
		if status.PendingCapsuleGeneration != nil {
			generation = strconv.FormatUint(*status.PendingCapsuleGeneration, 10)
		}
		if status.PendingCapsuleSHA256 != nil {
			digest = *status.PendingCapsuleSHA256
		}
		globalOptions.Term.Print(fmt.Sprintf("policy mutation pending: generation %s; sha256 %s\n", generation, digest))
	}
	for _, finding := range status.Findings {
		globalOptions.Term.Print("finding: " + finding + "\n")
	}
}

func prepareUnlockSession(
	ctx context.Context,
	globalOptions *global.Options,
	client *indexbroker.Client,
	repositoryID string,
	capsuleGeneration uint64,
	sessionFile string,
	sessionTTL time.Duration,
) error {
	session, err := client.CreateSession(ctx, sessionTTL)
	if err != nil {
		emitUnlockEventBestEffort(ctx, observability.Warning, "unlock session creation rejected",
			map[string]any{"capsule_generation": capsuleGeneration})
		return err
	}
	if session.Transcript.RepositoryID != repositoryID || session.Transcript.CapsuleGeneration != capsuleGeneration {
		return fmt.Errorf("broker session does not match local capsule")
	}
	if err := writeNewProtectedJSON(sessionFile, session); err != nil {
		return err
	}
	emitUnlockEventBestEffort(ctx, observability.Notice, "unlock session created", map[string]any{
		"session_id": session.Transcript.SessionID, "capsule_generation": capsuleGeneration, "expires_unix_ms": session.Transcript.ExpiresUnixMS,
	})
	result := map[string]any{"session_file": sessionFile, "fingerprint": session.Fingerprint,
		"expires_unix_ms": session.Transcript.ExpiresUnixMS, "capsule_generation": capsuleGeneration}
	if globalOptions.JSON {
		globalOptions.Term.Print(ui.ToJSONString(result))
	} else {
		globalOptions.Term.Print(fmt.Sprintf("session fingerprint: %s\ncapsule generation: %d\n", session.Fingerprint, capsuleGeneration))
	}
	return nil
}

func (options unlockCredentialOptions) routeCount() int {
	count := 0
	for _, configured := range []bool{
		options.passphraseFile != "", options.keyFile != "", options.azureTokenFile != "", options.gcpTokenFile != "",
		options.yubikeyPINFile != "", options.fido2PINFile != "", options.awsKMS, options.macosSecureEnclave,
	} {
		if configured {
			count++
		}
	}
	return count
}

func buildUnlockContribution(
	ctx context.Context,
	capsule unlockContributionCapsule,
	session indexbroker.SignedSession,
	endpoint, memberID string,
	lastSeen uint64,
	unverifiedSession bool,
	options unlockCredentialOptions,
) (indexbroker.EncryptedContribution, error) {
	if options.awsKMS {
		unwrapper, err := indexbroker.NewAWSKMSUnwrapper(ctx)
		if err != nil {
			return indexbroker.EncryptedContribution{}, err
		}
		return capsule.ContributeExternalSession(
			ctx,
			session,
			endpoint,
			memberID,
			unwrapper,
			lastSeen,
			time.Now(),
			unverifiedSession,
		)
	}
	if options.yubikeyPINFile != "" {
		unwrapper := indexbroker.YubiKeyPIVUnwrapper{HelperPath: options.custodianPath, PINFile: options.yubikeyPINFile}
		return capsule.ContributeExternalSession(
			ctx,
			session,
			endpoint,
			memberID,
			unwrapper,
			lastSeen,
			time.Now(),
			unverifiedSession,
		)
	}
	if options.fido2PINFile != "" {
		unwrapper := indexbroker.FIDO2HMACSecretUnwrapper{
			HelperPath: options.custodianPath,
			PINFile:    options.fido2PINFile,
		}
		return capsule.ContributeExternalSession(
			ctx,
			session,
			endpoint,
			memberID,
			unwrapper,
			lastSeen,
			time.Now(),
			unverifiedSession,
		)
	}
	if options.macosSecureEnclave {
		if runtime.GOOS != "darwin" {
			return indexbroker.EncryptedContribution{}, fmt.Errorf("macOS Secure Enclave contribution requires macOS")
		}
		unwrapper := indexbroker.MacosSecureEnclaveUnwrapper{HelperPath: options.custodianPath}
		return capsule.ContributeExternalSession(
			ctx,
			session,
			endpoint,
			memberID,
			unwrapper,
			lastSeen,
			time.Now(),
			unverifiedSession,
		)
	}
	if options.azureTokenFile != "" || options.gcpTokenFile != "" {
		return buildCloudUnlockContribution(
			ctx,
			capsule,
			session,
			endpoint,
			memberID,
			lastSeen,
			unverifiedSession,
			options,
		)
	}
	credentialPath, description, keyfile := options.passphraseFile, "custodian passphrase", false
	if options.keyFile != "" {
		credentialPath, description, keyfile = options.keyFile, "custodian keyfile", true
	}
	credential, err := readProtectedBinary(credentialPath, description, !keyfile)
	if err != nil {
		return indexbroker.EncryptedContribution{}, err
	}
	defer clear(credential)
	return capsule.ContributeOfflineSession(
		session,
		endpoint,
		memberID,
		credential,
		keyfile,
		lastSeen,
		time.Now(),
		unverifiedSession,
	)
}

func buildCloudUnlockContribution(
	ctx context.Context,
	capsule unlockContributionCapsule,
	session indexbroker.SignedSession,
	endpoint, memberID string,
	lastSeen uint64,
	unverifiedSession bool,
	options unlockCredentialOptions,
) (indexbroker.EncryptedContribution, error) {
	tokenFile, description := options.azureTokenFile, "Azure custodian bearer token"
	if options.gcpTokenFile != "" {
		tokenFile, description = options.gcpTokenFile, "Google Cloud custodian bearer token"
	}
	token, err := readProtectedBinary(tokenFile, description, true)
	if err != nil {
		return indexbroker.EncryptedContribution{}, err
	}
	defer clear(token)
	var unwrapper indexbroker.ExternalMemberUnwrapper
	if options.azureTokenFile != "" {
		azure, createErr := indexbroker.NewAzureKeyVaultUnwrapper(token, &http.Client{Timeout: 30 * time.Second})
		if createErr != nil {
			return indexbroker.EncryptedContribution{}, createErr
		}
		defer azure.Clear()
		unwrapper = azure
	} else {
		google, createErr := indexbroker.NewGoogleCloudKMSUnwrapper(token, &http.Client{Timeout: 30 * time.Second})
		if createErr != nil {
			return indexbroker.EncryptedContribution{}, createErr
		}
		defer google.Clear()
		unwrapper = google
	}
	return capsule.ContributeExternalSession(
		ctx,
		session,
		endpoint,
		memberID,
		unwrapper,
		lastSeen,
		time.Now(),
		unverifiedSession,
	)
}

func readValidatedUnlockSession(
	ctx context.Context,
	sessionFile, confirmedFingerprint, memberID string,
	unverifiedSession bool,
	capsuleGeneration uint64,
) (indexbroker.SignedSession, error) {
	var session indexbroker.SignedSession
	if err := readProtectedJSON(sessionFile, "unlock session", &session); err != nil {
		return session, err
	}
	if confirmedFingerprint != session.Fingerprint {
		return session, fmt.Errorf("confirmed fingerprint does not match signed session")
	}
	if session.Transcript.IdentityRecovery != unverifiedSession {
		return session, fmt.Errorf(
			"identity-recovery sessions require --unverified-session, which normal sessions reject",
		)
	}
	if unverifiedSession {
		emitUnlockEventBestEffort(
			ctx,
			observability.Critical,
			"unverified broker identity recovery session acknowledged",
			map[string]any{
				"session_id": session.Transcript.SessionID, "member_id": memberID,
				"capsule_generation": capsuleGeneration, "fingerprint": session.Fingerprint,
			},
		)
	}
	return session, nil
}

func submitUnlockContribution(ctx context.Context, globalOptions *global.Options, client *indexbroker.Client,
	contribution indexbroker.EncryptedContribution, generationAnchor, memberID string, capsuleGeneration uint64,
) error {
	if generationAnchor != "" {
		if err := writeGenerationAnchor(generationAnchor, capsuleGeneration); err != nil {
			return fmt.Errorf("persist observed capsule generation before contribution: %w", err)
		}
	}
	unlocked, err := client.SubmitContribution(ctx, contribution)
	if err != nil {
		emitUnlockEventBestEffort(
			ctx,
			observability.Warning,
			"custodian contribution rejected",
			map[string]any{
				"member_id":          memberID,
				"capsule_generation": capsuleGeneration,
				"stage":              "broker_submission",
			},
		)
		return err
	}
	emitUnlockEventBestEffort(ctx, observability.Notice, "custodian contribution accepted",
		map[string]any{"member_id": memberID, "capsule_generation": capsuleGeneration, "quorum_complete": unlocked})
	if globalOptions.JSON {
		globalOptions.Term.Print(
			ui.ToJSONString(
				map[string]any{"accepted": true, "unlocked": unlocked, "capsule_generation": capsuleGeneration},
			),
		)
	} else if unlocked {
		globalOptions.Term.Print("contribution accepted; quorum complete and broker unlocked\n")
	} else {
		globalOptions.Term.Print("contribution accepted; broker remains locked pending quorum\n")
	}
	return nil
}

func newIndexUnlockContributeCommand(globalOptions *global.Options, options *indexUnlockOptions) *cobra.Command {
	var commandOptions unlockContributeOptions
	var generationAnchor string
	var sessionTTL time.Duration
	command := &cobra.Command{
		Use:               "contribute",
		Short:             "Prepare or submit a verified custodian contribution",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return commandOptions.finalize(*options)
		},
		RunE: func(command *cobra.Command, _ []string) error {
			capsule, err := indexbroker.LoadCapsule(options.Capsule)
			if err != nil {
				return err
			}
			client, err := dialIndexBroker(command.Context(), options.Socket)
			if err != nil {
				return err
			}
			defer vaulticerrors.CloseQuietly(client)
			if commandOptions.prepare {
				return prepareUnlockSession(
					command.Context(),
					globalOptions,
					client,
					capsule.RepositoryID(),
					capsule.Generation(),
					commandOptions.sessionFile,
					sessionTTL,
				)
			}
			session, err := readValidatedUnlockSession(
				command.Context(),
				commandOptions.sessionFile,
				commandOptions.confirmedFingerprint,
				commandOptions.memberID,
				commandOptions.unverifiedSession,
				capsule.Generation(),
			)
			if err != nil {
				return err
			}
			lastSeen, err := readGenerationAnchor(generationAnchor, capsule.Generation())
			if err != nil {
				emitUnlockEventBestEffort(
					command.Context(),
					observability.Warning,
					"custodian contribution rejected",
					map[string]any{
						"member_id":          commandOptions.memberID,
						"capsule_generation": capsule.Generation(),
						"stage":              "unwrap_or_verify",
					},
				)
				return err
			}
			contribution, err := buildUnlockContribution(
				command.Context(), capsule, session, "unix:"+options.Socket, commandOptions.memberID, lastSeen,
				commandOptions.unverifiedSession, commandOptions.credentials,
			)
			if err != nil {
				return err
			}
			return submitUnlockContribution(
				command.Context(),
				globalOptions,
				client,
				contribution,
				generationAnchor,
				commandOptions.memberID,
				capsule.Generation(),
			)
		},
	}
	command.Flags().
		BoolVar(&commandOptions.prepare, "prepare", false, "create and save a new signed contribution session")
	command.Flags().
		StringVar(&commandOptions.sessionFile, "session-file", "", "mode-0600 signed session file shared with custodians")
	command.Flags().DurationVar(&sessionTTL, "session-ttl", 10*time.Minute, "new contribution-session lifetime")
	command.Flags().StringVar(&commandOptions.memberID, "member", "", "capsule member ID")
	command.Flags().
		StringVar(&commandOptions.credentials.passphraseFile, "passphrase-file", "", "mode-0600 offline custodian passphrase file")
	command.Flags().
		StringVar(&commandOptions.credentials.keyFile, "key-file", "", "mode-0600 offline custodian keyfile")
	command.Flags().
		StringVar(&commandOptions.credentials.azureTokenFile, "azure-token-file", "", "mode-0600 Entra bearer-token file for an Azure Key Vault member")
	command.Flags().
		StringVar(&commandOptions.credentials.gcpTokenFile, "gcp-token-file", "", "mode-0600 Google Cloud bearer-token file for a Cloud KMS member")
	command.Flags().
		BoolVar(&commandOptions.credentials.awsKMS, "aws-kms", false, "use the AWS SDK credential chain for an AWS KMS or CloudHSM-backed member")
	command.Flags().
		StringVar(&commandOptions.credentials.yubikeyPINFile, "yubikey-piv-pin-file", "", "mode-0600 YubiKey PIV PIN file")
	command.Flags().
		StringVar(&commandOptions.credentials.fido2PINFile, "fido2-pin-file", "", "mode-0600 FIDO2 authenticator PIN file")
	command.Flags().
		BoolVar(&commandOptions.credentials.macosSecureEnclave, "macos-secure-enclave", false, ("authorize the contribution with Touch ID and the " +
			"enrolled Secure Enclave key"))
	command.Flags().
		StringVar(&commandOptions.credentials.custodianPath, "custodian-path", "vaultic-key-custodian", "path to the hardware custodian executable")
	command.Flags().
		BoolVar(
			&commandOptions.unverifiedSession,
			"unverified-session",
			false,
			"acknowledge a broker identity-loss recovery session after independently confirming its fingerprint",
		)
	command.Flags().
		StringVar(&generationAnchor, "generation-anchor", "", "mode-0600 custodian last-seen-generation file")
	command.Flags().
		StringVar(&commandOptions.confirmedFingerprint, "confirm-fingerprint", "", "out-of-band confirmed signed-session fingerprint")
	return command
}

func newIndexUnlockLockCommand(globalOptions *global.Options, options *indexUnlockOptions) *cobra.Command {
	var lockOptions unlockLockOptions
	command := &cobra.Command{
		Use:               "lock",
		Short:             "End the unlock epoch and revoke every lease",
		Args:              cobra.NoArgs,
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return lockOptions.finalize()
		},
		RunE: func(command *cobra.Command, _ []string) error {
			client, err := dialIndexBroker(command.Context(), options.Socket)
			if err != nil {
				return err
			}
			defer vaulticerrors.CloseQuietly(client)
			if err := client.Lock(command.Context()); err != nil {
				return err
			}
			emitUnlockEventBestEffort(command.Context(), observability.Notice, "key broker explicitly locked", nil)
			if globalOptions.JSON {
				globalOptions.Term.Print("{\"locked\":true}")
			} else {
				globalOptions.Term.Print("broker locked; all leases revoked\n")
			}
			return nil
		},
	}
	command.Flags().BoolVar(&lockOptions.confirm, "confirm", false, "confirm explicit broker lock and lease revocation")
	return command
}

func dialIndexBroker(ctx context.Context, socket string) (*indexbroker.Client, error) {
	if socket == "" {
		return nil, fmt.Errorf("--broker-socket is required")
	}
	return indexbroker.Dial(ctx, socket)
}

func readProtectedBinary(path, description string, trim bool) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect %s file: %w", description, err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%s file must be a non-symlink regular file", description)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s file must not be accessible by group or others", description)
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s file: %w", description, err)
	}
	if trim {
		value = bytes.TrimRight(value, " \t\r\n\v\f")
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("%s file is empty", description)
	}
	return value, nil
}

func readProtectedJSON(path, description string, target any) error {
	value, err := readProtectedBinary(path, description, false)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", description, err)
	}
	return nil
}

func writeNewProtectedJSON(path string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create protected file: %w", err)
	}
	if _, err = file.Write(append(encoded, '\n')); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path) // The protected file write failed, and a stale partial file is never loaded.
	}
	return err
}

func readGenerationAnchor(path string, current uint64) (uint64, error) {
	if path == "" {
		return current, nil
	}
	value, err := readProtectedBinary(path, "generation anchor", true)
	if errors.Is(err, fs.ErrNotExist) {
		return current, nil
	}
	if err != nil {
		return 0, err
	}
	generation, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("decode generation anchor: %w", err)
	}
	if generation > current {
		return generation, nil
	}
	return current, nil
}

func writeGenerationAnchor(path string, generation uint64) error {
	if existing, err := readGenerationAnchor(path, generation); err == nil && existing > generation {
		return fmt.Errorf("refusing to lower generation anchor from %d to %d", existing, generation)
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".vaultic-generation-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }() // Rename removes the temp path on success; failure leaves an unusable staging file.
	if err = temporary.Chmod(0o600); err != nil {
		_ = temporary.Close() // Preserve the permission failure; no data has been written.
		return err
	}
	_, err = fmt.Fprintf(temporary, "%d\n", generation)
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(temporaryName, path)
	}
	return err
}

func emitUnlockEventBestEffort(
	ctx context.Context,
	severity observability.Severity,
	message string,
	fields map[string]any,
) {
	observability.EmitBestEffort(
		ctx,
		observability.Event{
			Severity:  severity,
			Category:  observability.CategoryAuth,
			Component: "key-broker",
			Message:   message,
			Fields:    fields,
		},
	)
}
