package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/spf13/cobra"
)

type quorumBypassStatements struct {
	NoPlaintextKeyExports      bool `json:"no_plaintext_key_exports"`
	NoExternalStandaloneEscrow bool `json:"no_external_standalone_escrow"`
	GenerationAnchorsCurrent   bool `json:"generation_anchors_current"`
	BrokerCredentialsProtected bool `json:"broker_credentials_protected"`
	NoWarmRestartMaterial      bool `json:"no_warm_restart_material"`
	OfflineSharesSeparate      bool `json:"offline_shares_separate_custodians"`
}

type quorumBypassAttestation struct {
	Version           uint32                 `json:"version"`
	RepositoryID      string                 `json:"repository_id"`
	CapsuleGeneration uint64                 `json:"capsule_generation"`
	CapsuleLogicalID  string                 `json:"capsule_logical_id"`
	PolicyHash        string                 `json:"policy_hash"`
	IssuedUnix        int64                  `json:"issued_unix"`
	ExpiresUnix       int64                  `json:"expires_unix"`
	Statements        quorumBypassStatements `json:"statements"`
	Signature         string                 `json:"signature"`
}

func (attestation quorumBypassAttestation) signedPayload() ([]byte, error) {
	attestation.Signature = ""
	payload, err := json.Marshal(attestation)
	if err != nil {
		return nil, err
	}
	return append([]byte("vaultic/quorum-bypass-attestation/v1\x00"), payload...), nil
}

func newIndexKeysQuorumGenerateAttestationKeyCommand(globalOptions *global.Options) *cobra.Command {
	var privateKeyPath, publicKeyPath string
	command := &cobra.Command{Use: "generate-attestation-key", Short: "Generate a bypass-attestation signing keypair", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(_ *cobra.Command, _ []string) error {
		publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return fmt.Errorf("generate attestation key: %w", err)
		}
		defer clear(privateKey)
		if err := writeNewProtectedValue(privateKeyPath, base64.StdEncoding.EncodeToString(privateKey)); err != nil {
			return fmt.Errorf("write attestation private key: %w", err)
		}
		if err := writeNewProtectedValue(publicKeyPath, base64.StdEncoding.EncodeToString(publicKey)); err != nil {
			_ = os.Remove(privateKeyPath)
			return fmt.Errorf("write attestation public key: %w", err)
		}
		result := map[string]any{"private_key": privateKeyPath, "public_key": publicKeyPath, "algorithm": "Ed25519"}
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(result))
		} else {
			globalOptions.Term.Print(fmt.Sprintf("generated Ed25519 bypass-attestation keypair at %s and %s\n", privateKeyPath, publicKeyPath))
		}
		return nil
	}}
	command.Flags().StringVar(&privateKeyPath, "private-key", "", "new mode-0600 attestation private-key file")
	command.Flags().StringVar(&publicKeyPath, "public-key", "", "new mode-0600 pinned attestation public-key file")
	_ = command.MarkFlagRequired("private-key")
	_ = command.MarkFlagRequired("public-key")
	return command
}

func newIndexKeysQuorumAttestBypassesCommand(globalOptions *global.Options) *cobra.Command {
	var capsulePath, privateKeyPath, outputPath string
	var validFor time.Duration
	statements := quorumBypassStatements{}
	command := &cobra.Command{Use: "attest-bypasses", Short: "Sign an inventory of non-discoverable complete-key bypasses", Args: cobra.NoArgs, DisableAutoGenTag: true, RunE: func(_ *cobra.Command, _ []string) error {
		if !statements.NoPlaintextKeyExports || !statements.NoExternalStandaloneEscrow || !statements.GenerationAnchorsCurrent || !statements.BrokerCredentialsProtected || !statements.NoWarmRestartMaterial || !statements.OfflineSharesSeparate {
			return fmt.Errorf("every bypass inventory confirmation is required")
		}
		if validFor <= 0 || validFor > 90*24*time.Hour {
			return fmt.Errorf("--valid-for must be greater than zero and no more than 2160h")
		}
		capsule, err := indexbroker.LoadCapsule(capsulePath)
		if err != nil {
			return err
		}
		encodedKey, err := readProtectedBinary(privateKeyPath, "quorum bypass attestation private key", true)
		if err != nil {
			return err
		}
		defer clear(encodedKey)
		privateKey, err := base64.StdEncoding.DecodeString(string(encodedKey))
		defer clear(privateKey)
		if err != nil || len(privateKey) != ed25519.PrivateKeySize {
			return fmt.Errorf("bypass attestation private key must be a base64 Ed25519 private key")
		}
		now := time.Now()
		attestation := quorumBypassAttestation{
			Version: 1, RepositoryID: capsule.RepositoryID(), CapsuleGeneration: capsule.Generation(),
			CapsuleLogicalID: capsule.LogicalID(), PolicyHash: capsule.PolicyHash(),
			IssuedUnix: now.Unix(), ExpiresUnix: now.Add(validFor).Unix(), Statements: statements,
		}
		payload, err := attestation.signedPayload()
		if err != nil {
			return fmt.Errorf("encode bypass attestation: %w", err)
		}
		attestation.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
		if err := writeNewProtectedJSON(outputPath, attestation); err != nil {
			return fmt.Errorf("write bypass attestation: %w", err)
		}
		result := map[string]any{"attestation": outputPath, "repository_id": capsule.RepositoryID(), "capsule_generation": capsule.Generation(), "expires_unix": attestation.ExpiresUnix}
		if globalOptions.JSON {
			globalOptions.Term.Print(ui.ToJSONString(result))
		} else {
			globalOptions.Term.Print(fmt.Sprintf("wrote signed bypass attestation for capsule generation %d to %s\n", capsule.Generation(), outputPath))
		}
		return nil
	}}
	command.Flags().StringVar(&capsulePath, "capsule", "", "immutable recovery capsule to attest")
	command.Flags().StringVar(&privateKeyPath, "private-key", "", "mode-0600 base64 Ed25519 private-key file")
	command.Flags().StringVar(&outputPath, "output", "", "new mode-0600 signed attestation file")
	command.Flags().DurationVar(&validFor, "valid-for", 30*24*time.Hour, "attestation validity duration (maximum 2160h)")
	command.Flags().BoolVar(&statements.NoPlaintextKeyExports, "confirm-no-plaintext-key-exports", false, "attest that no persistent plaintext complete-key export exists")
	command.Flags().BoolVar(&statements.NoExternalStandaloneEscrow, "confirm-no-external-standalone-escrow", false, "attest that no standalone escrow copy remains outside managed inventory")
	command.Flags().BoolVar(&statements.GenerationAnchorsCurrent, "confirm-generation-anchors-current", false, "attest that every custodian generation anchor is current")
	command.Flags().BoolVar(&statements.BrokerCredentialsProtected, "confirm-broker-credentials-protected", false, "attest that broker credentials cannot bypass quorum")
	command.Flags().BoolVar(&statements.NoWarmRestartMaterial, "confirm-no-warm-restart-material", false, "attest that no complete-key warm-restart material persists")
	command.Flags().BoolVar(&statements.OfflineSharesSeparate, "confirm-offline-custodian-separation", false, "attest that offline shares map to distinct human custodians")
	_ = command.MarkFlagRequired("capsule")
	_ = command.MarkFlagRequired("private-key")
	_ = command.MarkFlagRequired("output")
	return command
}

func writeNewProtectedValue(path, value string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.WriteString(value + "\n"); err == nil {
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

func quorumAttestationFindings(capsule interface {
	RepositoryID() string
	Generation() uint64
	LogicalID() string
	PolicyHash() string
}, attestationPath, publicKeyPath string, now time.Time) []string {
	if attestationPath == "" || publicKeyPath == "" {
		return []string{"signed bypass attestation and pinned verification key are required"}
	}
	var attestation quorumBypassAttestation
	if err := readProtectedJSON(attestationPath, "quorum bypass attestation", &attestation); err != nil {
		return []string{fmt.Sprintf("bypass attestation is invalid: %v", err)}
	}
	encodedKey, err := readProtectedBinary(publicKeyPath, "quorum bypass attestation public key", true)
	if err != nil {
		return []string{fmt.Sprintf("bypass attestation key is invalid: %v", err)}
	}
	defer clear(encodedKey)
	publicKey, err := base64.StdEncoding.DecodeString(string(encodedKey))
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return []string{"bypass attestation key must be a base64 Ed25519 public key"}
	}
	signature, err := base64.StdEncoding.DecodeString(attestation.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return []string{"bypass attestation signature is invalid"}
	}
	payload, err := attestation.signedPayload()
	if err != nil || !ed25519.Verify(publicKey, payload, signature) {
		return []string{"bypass attestation signature verification failed"}
	}
	if attestation.Version != 1 ||
		attestation.RepositoryID != capsule.RepositoryID() ||
		attestation.CapsuleGeneration != capsule.Generation() ||
		attestation.CapsuleLogicalID != capsule.LogicalID() ||
		attestation.PolicyHash != capsule.PolicyHash() {
		return []string{"bypass attestation does not match the current capsule"}
	}
	if attestation.IssuedUnix > now.Unix() || attestation.ExpiresUnix <= now.Unix() || attestation.ExpiresUnix-attestation.IssuedUnix > int64(90*24*time.Hour/time.Second) {
		return []string{"bypass attestation validity window is invalid or expired"}
	}
	statements := []struct {
		value bool
		name  string
	}{
		{attestation.Statements.NoPlaintextKeyExports, "persistent plaintext key exports"},
		{attestation.Statements.NoExternalStandaloneEscrow, "external standalone escrow copies"},
		{attestation.Statements.GenerationAnchorsCurrent, "stale custodian generation anchors"},
		{attestation.Statements.BrokerCredentialsProtected, "unprotected broker credentials"},
		{attestation.Statements.NoWarmRestartMaterial, "warm-restart key material"},
		{attestation.Statements.OfflineSharesSeparate, "offline shares held by overlapping custodians"},
	}
	var findings []string
	for _, statement := range statements {
		if !statement.value {
			findings = append(findings, "operator did not attest absence of "+statement.name)
		}
	}
	return findings
}
