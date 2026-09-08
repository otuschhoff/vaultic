package broker

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	if filepath.Base(os.Args[0]) == "helper" || filepath.Base(os.Args[0]) == "helper.exe" {
		root := filepath.Dir(os.Args[0])
		input, err := io.ReadAll(os.Stdin)
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "arguments"), []byte(strings.Join(os.Args[1:], " ")+"\n"), 0o600)
		}
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "stdin"), input, 0o600)
		}
		if err == nil && len(os.Args) > 1 && os.Args[1] == "macos-secure-enclave-unwrap" {
			err = os.WriteFile(filepath.Join(root, "environment"), []byte(strings.Join(os.Environ(), "\n")), 0o600)
		}
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		outputs := map[string]string{
			"yubikey-piv-unwrap":          base64.StdEncoding.EncodeToString([]byte("wrapped-share")),
			"fido2-hmac-secret-unwrap":    base64.StdEncoding.EncodeToString([]byte("fido-share")),
			"macos-secure-enclave-unwrap": base64.StdEncoding.EncodeToString([]byte("enclave-share")),
		}
		fmt.Println(outputs[os.Args[1]])
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func testCustodianHelper(t *testing.T, root string) string {
	t.Helper()
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	helperPath := filepath.Join(root, "helper"+extension)
	executable, err := os.Open(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	defer executable.Close()
	helper, err := os.OpenFile(helperPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(helper, executable); err != nil {
		_ = helper.Close()
		t.Fatal(err)
	}
	if err := helper.Close(); err != nil {
		t.Fatal(err)
	}
	return helperPath
}

func TestYubiKeyPIVUnwrapperUsesBoundContext(t *testing.T) {
	root := t.TempDir()
	argumentsPath := filepath.Join(root, "arguments")
	stdinPath := filepath.Join(root, "stdin")
	helperPath := testCustodianHelper(t, root)
	unwrapper := YubiKeyPIVUnwrapper{HelperPath: helperPath, PINFile: "/protected/pin"}
	plaintext, identity, err := unwrapper.UnwrapMember(
		t.Context(),
		ExternalMemberContext{
			RepositoryID:         "repo-a",
			RootKeyVersion:       3,
			MemberID:             "piv-a",
			Provider:             "yubikey-piv",
			KeyReference:         "pkcs11:reference",
			Purpose:              "bound-purpose",
			HardwareCredentialID: "credential-a",
		},
		[]byte("ciphertext"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "wrapped-share" || identity.Authority != "yubikey-piv" ||
		identity.ImmutablePrincipalID != "credential-a" {
		t.Fatalf("unexpected unwrap result: %q, %#v", plaintext, identity)
	}
	arguments, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"yubikey-piv-unwrap", "/protected/pin", "repo-a", "piv-a", "pkcs11:reference", "3", "bound-purpose"} {
		if !strings.Contains(string(arguments), expected) {
			t.Fatalf("helper arguments %q do not contain %q", arguments, expected)
		}
	}
	if strings.Contains(string(arguments), base64.StdEncoding.EncodeToString([]byte("ciphertext"))) {
		t.Fatalf("wrapped share leaked into helper argv: %q", arguments)
	}
	stdin, err := os.ReadFile(stdinPath)
	if err != nil || string(stdin) != base64.StdEncoding.EncodeToString([]byte("ciphertext")) {
		t.Fatalf("helper stdin = %q, %v", stdin, err)
	}
}

func TestYubiKeyPIVUnwrapperFailsClosed(t *testing.T) {
	unwrapper := YubiKeyPIVUnwrapper{HelperPath: "/missing", PINFile: "/protected/pin"}
	if _, _, err := unwrapper.UnwrapMember(t.Context(), ExternalMemberContext{Provider: "aws-kms", HardwareCredentialID: "credential-a"}, nil); err == nil {
		t.Fatal("wrong provider was accepted")
	}
}

func TestFIDO2HMACSecretUnwrapperUsesBoundContext(t *testing.T) {
	root := t.TempDir()
	argumentsPath := filepath.Join(root, "arguments")
	stdinPath := filepath.Join(root, "stdin")
	helperPath := testCustodianHelper(t, root)
	unwrapper := FIDO2HMACSecretUnwrapper{HelperPath: helperPath, PINFile: "/protected/pin"}
	plaintext, identity, err := unwrapper.UnwrapMember(
		t.Context(),
		ExternalMemberContext{
			RepositoryID:         "repo-a",
			RootKeyVersion:       3,
			MemberID:             "fido-a",
			Provider:             "fido2-hmac-secret",
			KeyReference:         "fido2:reference",
			Purpose:              "bound-purpose",
			HardwareCredentialID: "credential-a",
		},
		[]byte("ciphertext"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "fido-share" || identity.Authority != "fido2-hmac-secret" ||
		identity.ImmutablePrincipalID != "credential-a" {
		t.Fatalf("unexpected unwrap result: %q, %#v", plaintext, identity)
	}
	arguments, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(arguments), base64.StdEncoding.EncodeToString([]byte("ciphertext"))) {
		t.Fatalf("wrapped share leaked into helper argv: %q", arguments)
	}
	stdin, err := os.ReadFile(stdinPath)
	if err != nil || string(stdin) != base64.StdEncoding.EncodeToString([]byte("ciphertext")) {
		t.Fatalf("helper stdin = %q, %v", stdin, err)
	}
}

func TestMacosSecureEnclaveUnwrapperUsesBoundContextAndEmptyEnvironment(t *testing.T) {
	t.Setenv("VAULTIC_TEST_AMBIENT_SECRET", "must-not-be-inherited")
	root := t.TempDir()
	argumentsPath := filepath.Join(root, "arguments")
	stdinPath := filepath.Join(root, "stdin")
	environmentPath := filepath.Join(root, "environment")
	helperPath := testCustodianHelper(t, root)
	unwrapper := MacosSecureEnclaveUnwrapper{HelperPath: helperPath}
	plaintext, identity, err := unwrapper.UnwrapMember(
		t.Context(),
		ExternalMemberContext{
			RepositoryID:         "repo-a",
			RootKeyVersion:       3,
			MemberID:             "enclave-a",
			Provider:             "macos-secure-enclave",
			KeyReference:         "secure-enclave:reference",
			Purpose:              "bound-purpose",
			HardwareCredentialID: "credential-a",
		},
		[]byte("ciphertext"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "enclave-share" || identity.Authority != "macos-secure-enclave" ||
		identity.ImmutablePrincipalID != "credential-a" {
		t.Fatalf("unexpected unwrap result: %q, %#v", plaintext, identity)
	}
	arguments, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"macos-secure-enclave-unwrap", "repo-a", "enclave-a", "secure-enclave:reference", "3", "bound-purpose"} {
		if !strings.Contains(string(arguments), expected) {
			t.Fatalf("helper arguments %q do not contain %q", arguments, expected)
		}
	}
	if strings.Contains(string(arguments), base64.StdEncoding.EncodeToString([]byte("ciphertext"))) {
		t.Fatalf("wrapped share leaked into helper argv: %q", arguments)
	}
	stdin, err := os.ReadFile(stdinPath)
	if err != nil || string(stdin) != base64.StdEncoding.EncodeToString([]byte("ciphertext")) {
		t.Fatalf("helper stdin = %q, %v", stdin, err)
	}
	environment, err := os.ReadFile(environmentPath)
	if err != nil {
		t.Fatalf("helper environment = %q, %v", environment, err)
	}
	for _, entry := range strings.Fields(string(environment)) {
		name, _, _ := strings.Cut(entry, "=")
		if name != "PWD" && name != "SHLVL" && name != "_" {
			t.Fatalf("helper inherited unexpected environment entry %q", entry)
		}
	}
}

func TestMacosSecureEnclaveUnwrapperFailsClosed(t *testing.T) {
	unwrapper := MacosSecureEnclaveUnwrapper{HelperPath: "/missing"}
	if _,
		_,
		err := unwrapper.UnwrapMember(t.Context(),
		ExternalMemberContext{Provider: "fido2-hmac-secret",
			HardwareCredentialID: "credential-a"},
		nil); err == nil {
		t.Fatal("wrong provider was accepted")
	}
}
