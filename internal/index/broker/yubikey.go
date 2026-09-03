package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

type YubiKeyPIVUnwrapper struct {
	HelperPath string
	PINFile    string
}

type FIDO2HMACSecretUnwrapper struct {
	HelperPath string
	PINFile    string
}

type MacosSecureEnclaveUnwrapper struct {
	HelperPath string
}

func (unwrapper MacosSecureEnclaveUnwrapper) UnwrapMember(ctx context.Context, member ExternalMemberContext, ciphertext []byte) ([]byte, VerifiedPrincipal, error) {
	if member.Provider != "macos-secure-enclave" || member.HardwareCredentialID == "" {
		return nil, VerifiedPrincipal{}, fmt.Errorf("macOS Secure Enclave unwrapper requires a hardware-bound macos-secure-enclave member")
	}
	if unwrapper.HelperPath == "" {
		return nil, VerifiedPrincipal{}, fmt.Errorf("macOS Secure Enclave helper path is required")
	}
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(ciphertext)))
	base64.StdEncoding.Encode(encoded, ciphertext)
	defer clear(encoded)
	command := exec.CommandContext(ctx, unwrapper.HelperPath, "macos-secure-enclave-unwrap", member.RepositoryID, member.MemberID, member.KeyReference, strconv.FormatUint(uint64(member.RootKeyVersion), 10), member.Purpose)
	command.Env = []string{}
	command.Stdin = bytes.NewReader(encoded)
	output, err := command.Output()
	if err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("macOS Secure Enclave unwrap helper: %w", err)
	}
	plaintext, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(output)))
	if err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("decode macOS Secure Enclave helper output: %w", err)
	}
	return plaintext, VerifiedPrincipal{Authority: "macos-secure-enclave", ImmutablePrincipalID: member.HardwareCredentialID}, nil
}

func (unwrapper FIDO2HMACSecretUnwrapper) UnwrapMember(ctx context.Context, member ExternalMemberContext, ciphertext []byte) ([]byte, VerifiedPrincipal, error) {
	if member.Provider != "fido2-hmac-secret" || member.HardwareCredentialID == "" {
		return nil, VerifiedPrincipal{}, fmt.Errorf("FIDO2 unwrapper requires a hardware-bound fido2-hmac-secret member")
	}
	if unwrapper.HelperPath == "" || unwrapper.PINFile == "" {
		return nil, VerifiedPrincipal{}, fmt.Errorf("FIDO2 helper path and PIN file are required")
	}
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(ciphertext)))
	base64.StdEncoding.Encode(encoded, ciphertext)
	defer clear(encoded)
	command := exec.CommandContext(ctx, unwrapper.HelperPath, "fido2-hmac-secret-unwrap", unwrapper.PINFile, member.RepositoryID, member.MemberID, member.KeyReference, strconv.FormatUint(uint64(member.RootKeyVersion), 10), member.Purpose)
	command.Env = []string{}
	command.Stdin = bytes.NewReader(encoded)
	output, err := command.Output()
	if err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("FIDO2 unwrap helper: %w", err)
	}
	plaintext, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(output)))
	if err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("decode FIDO2 helper output: %w", err)
	}
	return plaintext, VerifiedPrincipal{Authority: "fido2-hmac-secret", ImmutablePrincipalID: member.HardwareCredentialID}, nil
}

func (unwrapper YubiKeyPIVUnwrapper) UnwrapMember(ctx context.Context, member ExternalMemberContext, ciphertext []byte) ([]byte, VerifiedPrincipal, error) {
	if member.Provider != "yubikey-piv" || member.HardwareCredentialID == "" {
		return nil, VerifiedPrincipal{}, fmt.Errorf("YubiKey PIV unwrapper requires a hardware-bound yubikey-piv member")
	}
	if unwrapper.HelperPath == "" || unwrapper.PINFile == "" {
		return nil, VerifiedPrincipal{}, fmt.Errorf("YubiKey PIV helper path and PIN file are required")
	}
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(ciphertext)))
	base64.StdEncoding.Encode(encoded, ciphertext)
	defer clear(encoded)
	command := exec.CommandContext(ctx, unwrapper.HelperPath, "yubikey-piv-unwrap", unwrapper.PINFile, member.RepositoryID, member.MemberID, member.KeyReference, strconv.FormatUint(uint64(member.RootKeyVersion), 10), member.Purpose)
	command.Env = []string{}
	command.Stdin = bytes.NewReader(encoded)
	output, err := command.Output()
	if err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("YubiKey PIV unwrap helper: %w", err)
	}
	plaintext, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(output)))
	if err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("decode YubiKey PIV helper output: %w", err)
	}
	return plaintext, VerifiedPrincipal{Authority: "yubikey-piv", ImmutablePrincipalID: member.HardwareCredentialID}, nil
}
