package broker

import (
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

func (unwrapper YubiKeyPIVUnwrapper) UnwrapMember(ctx context.Context, member ExternalMemberContext, ciphertext []byte) ([]byte, VerifiedPrincipal, error) {
	if member.Provider != "yubikey-piv" || member.HardwareCredentialID == "" {
		return nil, VerifiedPrincipal{}, fmt.Errorf("YubiKey PIV unwrapper requires a hardware-bound yubikey-piv member")
	}
	if unwrapper.HelperPath == "" || unwrapper.PINFile == "" {
		return nil, VerifiedPrincipal{}, fmt.Errorf("YubiKey PIV helper path and PIN file are required")
	}
	command := exec.CommandContext(ctx, unwrapper.HelperPath, "yubikey-piv-unwrap", unwrapper.PINFile, member.RepositoryID, member.MemberID, member.KeyReference, strconv.FormatUint(uint64(member.RootKeyVersion), 10), member.Purpose, base64.StdEncoding.EncodeToString(ciphertext))
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
