package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

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

func newIndexUnlockCommand(globalOptions *global.Options) *cobra.Command {
	var options indexUnlockOptions
	command := &cobra.Command{Use: "unlock", Short: "Operate the local quorum key broker", Args: cobra.NoArgs, DisableAutoGenTag: true}
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
	return &cobra.Command{Use: "status", Short: "Show the key broker lock and lease state", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		client, err := dialIndexBroker(command.Context(), options.Socket)
		if err != nil {
			return err
		}
		defer client.Close()
		status, err := client.Status(command.Context())
		if err != nil {
			return err
		}
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(status))
		} else {
			state := "unlocked"
			if status.Locked {
				state = "locked"
			}
			globalOptions.Term.Print(fmt.Sprintf("broker %s; repository %s; capsule generation %d; minimum custodians %d; compliant %t; sessions %d; leases %d\n", state, status.RepositoryID, status.CapsuleGeneration, status.MinimumCustodians, status.Compliant, status.ActiveSessions, status.ActiveLeases))
			globalOptions.Term.Print(fmt.Sprintf("assurance: principal-verified=%t hardware-verified=%t custody-assumed=%t\n", status.PrincipalVerified, status.HardwareVerified, status.CustodyAssumed))
			for _, finding := range status.Findings {
				globalOptions.Term.Print("finding: " + finding + "\n")
			}
		}
		return nil
	}}
}

func newIndexUnlockContributeCommand(globalOptions *global.Options, options *indexUnlockOptions) *cobra.Command {
	var sessionFile, memberID, passphraseFile, keyFile, generationAnchor, confirmedFingerprint string
	var prepare bool
	var sessionTTL time.Duration
	command := &cobra.Command{Use: "contribute", Short: "Prepare or submit a verified custodian contribution", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if options.Capsule == "" || sessionFile == "" {
			return fmt.Errorf("--capsule and --session-file are required")
		}
		capsule, err := indexbroker.LoadCapsule(options.Capsule)
		if err != nil {
			return err
		}
		client, err := dialIndexBroker(command.Context(), options.Socket)
		if err != nil {
			return err
		}
		defer client.Close()
		if prepare {
			session, err := client.CreateSession(command.Context(), sessionTTL)
			if err != nil {
				return err
			}
			if session.Transcript.RepositoryID != capsule.RepositoryID() || session.Transcript.CapsuleGeneration != capsule.Generation() {
				return fmt.Errorf("broker session does not match local capsule")
			}
			if err := writeNewProtectedJSON(sessionFile, session); err != nil {
				return err
			}
			result := map[string]any{"session_file": sessionFile, "fingerprint": session.Fingerprint, "expires_unix_ms": session.Transcript.ExpiresUnixMS, "capsule_generation": capsule.Generation()}
			if globalOptions.JSON {
				globalOptions.Term.Print(ui.ToJSONString(result))
			} else {
				globalOptions.Term.Print(fmt.Sprintf("session fingerprint: %s\ncapsule generation: %d\n", session.Fingerprint, capsule.Generation()))
			}
			return nil
		}
		if memberID == "" || confirmedFingerprint == "" || (passphraseFile == "") == (keyFile == "") {
			return fmt.Errorf("--member, --confirm-fingerprint, and exactly one of --passphrase-file or --key-file are required")
		}
		var session indexbroker.SignedSession
		if err := readProtectedJSON(sessionFile, "unlock session", &session); err != nil {
			return err
		}
		if confirmedFingerprint != session.Fingerprint {
			return fmt.Errorf("confirmed fingerprint does not match signed session")
		}
		credentialPath, description, keyfile := passphraseFile, "custodian passphrase", false
		if keyFile != "" {
			credentialPath, description, keyfile = keyFile, "custodian keyfile", true
		}
		credential, err := readProtectedBinary(credentialPath, description, !keyfile)
		if err != nil {
			return err
		}
		defer clear(credential)
		lastSeen, err := readGenerationAnchor(generationAnchor, capsule.Generation())
		if err != nil {
			return err
		}
		contribution, err := capsule.ContributeOffline(session, "unix:"+options.Socket, memberID, credential, keyfile, lastSeen, time.Now())
		if err != nil {
			return err
		}
		unlocked, err := client.SubmitContribution(command.Context(), contribution)
		if err != nil {
			_ = emitUnlockEvent(command.Context(), observability.Warning, "custodian contribution rejected", map[string]any{"member_id": memberID, "capsule_generation": capsule.Generation()})
			return err
		}
		if generationAnchor != "" {
			if err := writeGenerationAnchor(generationAnchor, capsule.Generation()); err != nil {
				return err
			}
		}
		_ = emitUnlockEvent(command.Context(), observability.Notice, "custodian contribution accepted", map[string]any{"member_id": memberID, "capsule_generation": capsule.Generation(), "quorum_complete": unlocked})
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(map[string]any{"accepted": true, "unlocked": unlocked, "capsule_generation": capsule.Generation()}))
		} else if unlocked {
			globalOptions.Term.Print("contribution accepted; quorum complete and broker unlocked\n")
		} else {
			globalOptions.Term.Print("contribution accepted; broker remains locked pending quorum\n")
		}
		return nil
	}}
	command.Flags().BoolVar(&prepare, "prepare", false, "create and save a new signed contribution session")
	command.Flags().StringVar(&sessionFile, "session-file", "", "mode-0600 signed session file shared with custodians")
	command.Flags().DurationVar(&sessionTTL, "session-ttl", 10*time.Minute, "new contribution-session lifetime")
	command.Flags().StringVar(&memberID, "member", "", "capsule member ID")
	command.Flags().StringVar(&passphraseFile, "passphrase-file", "", "mode-0600 offline custodian passphrase file")
	command.Flags().StringVar(&keyFile, "key-file", "", "mode-0600 offline custodian keyfile")
	command.Flags().StringVar(&generationAnchor, "generation-anchor", "", "mode-0600 custodian last-seen-generation file")
	command.Flags().StringVar(&confirmedFingerprint, "confirm-fingerprint", "", "out-of-band confirmed signed-session fingerprint")
	return command
}

func newIndexUnlockLockCommand(globalOptions *global.Options, options *indexUnlockOptions) *cobra.Command {
	var confirm bool
	command := &cobra.Command{Use: "lock", Short: "End the unlock epoch and revoke every lease", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(command *cobra.Command, _ []string) error {
		if !confirm {
			return fmt.Errorf("--confirm is required to revoke all broker leases")
		}
		client, err := dialIndexBroker(command.Context(), options.Socket)
		if err != nil {
			return err
		}
		defer client.Close()
		if err := client.Lock(command.Context()); err != nil {
			return err
		}
		_ = emitUnlockEvent(command.Context(), observability.Notice, "key broker explicitly locked", nil)
		if globalOptions.JSON {
			globalOptions.Term.Print("{\"locked\":true}")
		} else {
			globalOptions.Term.Print("broker locked; all leases revoked\n")
		}
		return nil
	}}
	command.Flags().BoolVar(&confirm, "confirm", false, "confirm explicit broker lock and lease revocation")
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
		_ = os.Remove(path)
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
	defer os.Remove(temporaryName)
	if err = temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
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

func emitUnlockEvent(ctx context.Context, severity observability.Severity, message string, fields map[string]any) error {
	return observability.Emit(ctx, observability.Event{Severity: severity, Category: observability.CategoryAuth, Component: "key-broker", Message: message, Fields: fields})
}
