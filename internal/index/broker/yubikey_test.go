package broker

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestYubiKeyPIVUnwrapperUsesBoundContext(t *testing.T) {
	root := t.TempDir()
	argumentsPath := filepath.Join(root, "arguments")
	helperPath := filepath.Join(root, "helper")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" > " + argumentsPath + "\nprintf '" + base64.StdEncoding.EncodeToString([]byte("wrapped-share")) + "\\n'\n"
	if err := os.WriteFile(helperPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	unwrapper := YubiKeyPIVUnwrapper{HelperPath: helperPath, PINFile: "/protected/pin"}
	plaintext, identity, err := unwrapper.UnwrapMember(t.Context(), ExternalMemberContext{RepositoryID: "repo-a", RootKeyVersion: 3, MemberID: "piv-a", Provider: "yubikey-piv", KeyReference: "pkcs11:reference", Purpose: "bound-purpose", HardwareCredentialID: "credential-a"}, []byte("ciphertext"))
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "wrapped-share" || identity.Authority != "yubikey-piv" || identity.ImmutablePrincipalID != "credential-a" {
		t.Fatalf("unexpected unwrap result: %q, %#v", plaintext, identity)
	}
	arguments, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"yubikey-piv-unwrap", "/protected/pin", "repo-a", "piv-a", "pkcs11:reference", "3", "bound-purpose", base64.StdEncoding.EncodeToString([]byte("ciphertext"))} {
		if !strings.Contains(string(arguments), expected) {
			t.Fatalf("helper arguments %q do not contain %q", arguments, expected)
		}
	}
}

func TestYubiKeyPIVUnwrapperFailsClosed(t *testing.T) {
	unwrapper := YubiKeyPIVUnwrapper{HelperPath: "/missing", PINFile: "/protected/pin"}
	if _, _, err := unwrapper.UnwrapMember(t.Context(), ExternalMemberContext{Provider: "aws-kms", HardwareCredentialID: "credential-a"}, nil); err == nil {
		t.Fatal("wrong provider was accepted")
	}
}
