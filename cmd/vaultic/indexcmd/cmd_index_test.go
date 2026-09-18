package indexcmd

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/backend/mock"
	"github.com/otuschhoff/vaultic/internal/global"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/legacyimport"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/ui"
)

func TestParsePositiveCheckBytes(t *testing.T) {
	for _, test := range []struct {
		value string
		want  int64
	}{
		{value: "64M", want: 64 << 20},
		{value: "8G", want: 8 << 30},
	} {
		got, err := parsePositiveCheckBytes("--limit", test.value)
		if err != nil || got != test.want {
			t.Fatalf("parse %q = %d, %v; want %d", test.value, got, err, test.want)
		}
	}
	for _, value := range []string{"", "0", "64MiB", "-1"} {
		if _, err := parsePositiveCheckBytes("--limit", value); err == nil {
			t.Fatalf("accepted invalid limit %q", value)
		}
	}
}

func TestParseCheckMemoryBytes(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	for _, test := range []struct {
		value       string
		available   uint64
		availableOK bool
		want        int64
	}{
		{value: "256M", available: 32 * gib, want: 256 << 20},
		{value: "auto", available: 32 * gib, availableOK: true, want: int64(32*gib - 32*gib/10)},
		{value: " AUTO ", available: 256 * gib, availableOK: true, want: int64(256*gib - 256*gib/10)},
		{value: "auto", available: gib, availableOK: true, want: 512 << 20},
		{value: "auto", available: 128 << 20, availableOK: true, want: 64 << 20},
		{value: "auto", available: 32 << 20, availableOK: true, want: 16 << 20},
		{value: "auto", available: 1, availableOK: true, want: 1},
		{value: "auto", available: 0, availableOK: true, want: 1},
		{value: "auto", want: 64 << 20},
	} {
		got, err := parseCheckMemoryBytes(test.value, test.available, test.availableOK)
		if err != nil || got != test.want {
			t.Fatalf("parse %q with %d available bytes (detected=%t) = %d, %v; want %d", test.value, test.available, test.availableOK, got, err, test.want)
		}
	}
}

func TestMain(m *testing.M) {
	base := filepath.Base(os.Args[0])
	if base == "custodian" || base == "custodian.exe" {
		switch os.Args[1] {
		case "fido2-hmac-secret-derive":
			fmt.Println(base64.StdEncoding.EncodeToString(make([]byte, 32)))
		case "fido2-enroll":
			fmt.Println(`{"credential_id":"AQID",` +
				`"public_key":"sha256:787c798e39a5bc1910355bae6d0cd87a36b2e10fd0202a83e3bb6b005da83472",` +
				`"public_key_der":"BAUG","relying_party_id":"vaultic.example",` +
				`"attestation_fingerprint":null,"user_presence_required":true}`)
		default:
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestSaveImmutableBackendRecordDoesNotLoadMissingRecord(t *testing.T) {
	payload := []byte("envelope")
	handle := backend.Handle{Type: backend.SlateDBFile, Name: "key-envelope.json", IsMetadata: true}
	loadCalled := false
	saveCalls := 0
	destination := mock.NewBackend()
	destination.StatFn = func(context.Context, backend.Handle) (backend.FileInfo, error) {
		return backend.FileInfo{}, fs.ErrNotExist
	}
	destination.IsNotExistFn = func(err error) bool {
		return errors.Is(err, fs.ErrNotExist)
	}
	destination.OpenReaderFn = func(context.Context, backend.Handle, int, int64) (io.ReadCloser, error) {
		loadCalled = true
		return nil, errors.New("unexpected load")
	}
	destination.SaveFn = func(_ context.Context, gotHandle backend.Handle, reader backend.RewindReader) error {
		saveCalls++
		gotPayload, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		if gotHandle != handle || !bytes.Equal(gotPayload, payload) {
			t.Fatalf("saved (%v, %q), want (%v, %q)", gotHandle, gotPayload, handle, payload)
		}
		return nil
	}

	if err := saveImmutableBackendRecord(context.Background(), destination, handle, payload); err != nil {
		t.Fatal(err)
	}
	if loadCalled || saveCalls != 1 {
		t.Fatalf("load called %v, save calls %d; want false, 1", loadCalled, saveCalls)
	}
}

func testCustodianPath(t *testing.T, root string) string {
	t.Helper()
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	helperPath := filepath.Join(root, "custodian"+extension)
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

func testP256PublicKey(t *testing.T) []byte {
	t.Helper()
	privateKeyBytes := make([]byte, 32)
	privateKeyBytes[len(privateKeyBytes)-1] = 1
	privateKey, err := ecdh.P256().NewPrivateKey(privateKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	return privateKey.PublicKey().Bytes()
}

func TestMutateThresholdPolicyOperations(t *testing.T) {
	current := indexbroker.UnlockPolicy{Type: "threshold", GroupID: "operators", Required: 2, Members: []string{"alice", "bob", "carol"}}
	tests := []struct {
		name      string
		operation string
		args      []string
		threshold uint32
		want      indexbroker.UnlockPolicy
	}{
		{
			name:      "create",
			operation: "create-group",
			args:      []string{"new-operators"},
			threshold: 2,
			want:      indexbroker.UnlockPolicy{Type: "threshold", GroupID: "new-operators", Required: 2},
		},
		{
			name:      "add",
			operation: "add-member",
			args:      []string{"dana"},
			want:      indexbroker.UnlockPolicy{Type: "threshold", GroupID: "operators", Required: 2, Members: []string{"alice", "bob", "carol", "dana"}},
		},
		{
			name:      "remove",
			operation: "remove-member",
			args:      []string{"carol"},
			want:      indexbroker.UnlockPolicy{Type: "threshold", GroupID: "operators", Required: 2, Members: []string{"alice", "bob"}},
		},
		{
			name:      "threshold",
			operation: "set-threshold",
			args:      []string{"3"},
			want:      indexbroker.UnlockPolicy{Type: "threshold", GroupID: "operators", Required: 3, Members: []string{"alice", "bob", "carol"}},
		},
		{
			name:      "replace",
			operation: "replace-member",
			args:      []string{"alice", "dana"},
			want:      indexbroker.UnlockPolicy{Type: "threshold", GroupID: "operators", Required: 2, Members: []string{"bob", "carol", "dana"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := mutateThresholdPolicy(current, test.operation, test.args, "", test.threshold)
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(got) != fmt.Sprint(test.want) {
				t.Fatalf("policy = %#v, want %#v", got, test.want)
			}
		})
	}
	twoOfTwo := indexbroker.UnlockPolicy{Type: "threshold", GroupID: "operators", Required: 2, Members: []string{"alice", "bob"}}
	if _, err := mutateThresholdPolicy(twoOfTwo, "remove-member", []string{"bob"}, "", 0); err == nil {
		t.Fatal("removal below the active threshold was accepted")
	}
	if _, err := mutateThresholdPolicy(current, "replace-member", []string{"alice", "bob"}, "", 0); err == nil {
		t.Fatal("replacement with an existing member was accepted")
	}
	if err := validatePolicyMemberCredentials(current, []indexbroker.OfflinePolicyMember{{MemberID: "alice"}, {MemberID: "bob"}}, nil); err == nil {
		t.Fatal("incomplete resulting credential set was accepted")
	}
}

func TestParseExternalPolicyMembers(t *testing.T) {
	root := t.TempDir()
	tokenPath := filepath.Join(root, "token")
	if err := os.WriteFile(tokenPath, []byte("azure-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	azurePath := filepath.Join(root, "azure.json")
	azure := fmt.Sprintf(
		`{"member_id":"azure-a","provider":"azure-key-vault",`+
			`"key_reference":"https://example.vault.azure.net/keys/key/version",`+
			`"principal":{"authority":"entra","tenant_account_or_project":"tenant-a",`+
			`"immutable_principal_id":"object-a"},"bearer_token_file":%q}`,
		tokenPath,
	)
	if err := os.WriteFile(azurePath, []byte(azure), 0o600); err != nil {
		t.Fatal(err)
	}
	members, tokens, err := parseExternalPolicyMembers(t.Context(), "repo-a", []string{azurePath})
	if err != nil {
		t.Fatal(err)
	}
	defer clearPolicyCredentials(tokens)
	if len(members) != 1 || members[0].BearerToken == nil || *members[0].BearerToken != "azure-token" {
		t.Fatalf("external members = %#v", members)
	}
	policy := indexbroker.UnlockPolicy{Type: "threshold", GroupID: "operators", Required: 2, Members: []string{"offline-a", "azure-a"}}
	if err := validatePolicyMemberCredentials(policy, []indexbroker.OfflinePolicyMember{{MemberID: "offline-a"}}, members); err != nil {
		t.Fatal(err)
	}

	awsPath := filepath.Join(root, "aws.json")
	aws := fmt.Sprintf(
		`{"member_id":"aws-a","provider":"aws-kms",`+
			`"key_reference":"arn:aws:kms:us-east-1:123456789012:key/abc",`+
			`"principal":{"authority":"aws-iam","tenant_account_or_project":"123456789012",`+
			`"immutable_principal_id":"arn:aws:iam::123456789012:role/operator"},"bearer_token_file":%q}`,
		tokenPath,
	)
	if err := os.WriteFile(awsPath, []byte(aws), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseExternalPolicyMembers(t.Context(), "repo-a", []string{awsPath}); err == nil || !strings.Contains(err.Error(), "SDK credential chain") {
		t.Fatalf("AWS bearer token file was accepted: %v", err)
	}

	pivPath := filepath.Join(root, "piv.json")
	piv := fmt.Sprintf(
		`{"member_id":"piv-a","provider":"yubikey-piv",`+
			`"key_reference":"pkcs11:module-path=/usr/lib/libykcs11.so;slot-id=1;id=9a;`+
			`public-key-sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;type=rsa-key-pair",`+
			`"hardware":{"credential_id":"piv-9a",`+
			`"public_key":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",`+
			`"user_presence_required":true},"bearer_token_file":%q}`,
		tokenPath,
	)
	if err := os.WriteFile(pivPath, []byte(piv), 0o600); err != nil {
		t.Fatal(err)
	}
	pivMembers, pivTokens, err := parseExternalPolicyMembers(t.Context(), "repo-a", []string{pivPath})
	if err != nil {
		t.Fatal(err)
	}
	defer clearPolicyCredentials(pivTokens)
	if len(pivMembers) != 1 || pivMembers[0].Hardware == nil || pivMembers[0].Principal != nil || pivMembers[0].BearerToken == nil ||
		*pivMembers[0].BearerToken != "azure-token" {
		t.Fatalf("PIV members = %#v", pivMembers)
	}

	helperPath := testCustodianPath(t, root)
	fidoPath := filepath.Join(root, "fido.json")
	fido := fmt.Sprintf(
		`{"member_id":"fido-a","provider":"fido2-hmac-secret",`+
			`"key_reference":"fido2:rp-id=vaultic.example;credential-id=AQID;public-key-der=BAUG",`+
			`"hardware":{"credential_id":"AQID",`+
			`"public_key":"sha256:787c798e39a5bc1910355bae6d0cd87a36b2e10fd0202a83e3bb6b005da83472",`+
			`"user_presence_required":true},"pin_file":%q,"custodian_path":%q}`,
		tokenPath,
		helperPath,
	)
	if err := os.WriteFile(fidoPath, []byte(fido), 0o600); err != nil {
		t.Fatal(err)
	}
	fidoMembers, fidoSecrets, err := parseExternalPolicyMembers(t.Context(), "repo-a", []string{fidoPath})
	if err != nil {
		t.Fatal(err)
	}
	defer clearPolicyCredentials(fidoSecrets)
	if len(fidoMembers) != 1 || fidoMembers[0].BearerToken == nil || *fidoMembers[0].BearerToken != base64.StdEncoding.EncodeToString(make([]byte, 32)) {
		t.Fatalf("FIDO2 members = %#v", fidoMembers)
	}

	testSecureEnclaveExternalPolicyMember(t, root, tokenPath)
}

func testSecureEnclaveExternalPolicyMember(t *testing.T, root, tokenPath string) {
	t.Helper()
	publicKey := testP256PublicKey(t)
	encodedPublicKey := base64.RawURLEncoding.EncodeToString(publicKey)
	applicationTag := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	enclavePath := filepath.Join(root, "enclave.json")
	enclave := fmt.Sprintf(
		`{"member_id":"enclave-a","provider":"macos-secure-enclave",`+
			`"key_reference":"secure-enclave:application-tag=%s;public-key=%s;access-control=biometry-current-set",`+
			`"hardware":{"credential_id":"%s","public_key":"sha256:%x","user_presence_required":true}}`,
		applicationTag,
		encodedPublicKey,
		applicationTag,
		sha256.Sum256(publicKey),
	)
	if err := os.WriteFile(enclavePath, []byte(enclave), 0o600); err != nil {
		t.Fatal(err)
	}
	enclaveMembers, enclaveTokens, err := parseExternalPolicyMembers(t.Context(), "repo-a", []string{enclavePath})
	if err != nil {
		t.Fatal(err)
	}
	if len(enclaveMembers) != 1 || enclaveMembers[0].Hardware == nil || enclaveMembers[0].BearerToken != nil || len(enclaveTokens) != 0 {
		t.Fatalf("Secure Enclave members = %#v, tokens = %#v", enclaveMembers, enclaveTokens)
	}
	enclaveWithPIN := strings.TrimSuffix(enclave, "}") + fmt.Sprintf(`,"pin_file":%q}`, tokenPath)
	if err := os.WriteFile(enclavePath, []byte(enclaveWithPIN), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseExternalPolicyMembers(t.Context(), "repo-a", []string{enclavePath}); err == nil ||
		!strings.Contains(err.Error(), "must not configure") {
		t.Fatalf("Secure Enclave PIN file was accepted: %v", err)
	}
}

func TestEnrollFIDO2WritesProtectedExternalMember(t *testing.T) {
	root := t.TempDir()
	helperPath := testCustodianPath(t, root)
	pinPath := filepath.Join(root, "pin")
	if err := os.WriteFile(pinPath, []byte("123456"), 0o600); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(root, "member.json")
	command := newIndexKeysQuorumEnrollFIDO2Command()
	command.SetArgs(
		[]string{"--member", "fido-a", "--relying-party-id", "vaultic.example", "--pin-file", pinPath, "--custodian-path", helperPath, "--output", outputPath},
	)
	if err := command.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	var definition externalPolicyMemberFile
	if err := readProtectedJSON(outputPath, "FIDO2 member", &definition); err != nil {
		t.Fatal(err)
	}
	if definition.Provider != "fido2-hmac-secret" || definition.Hardware == nil || definition.Hardware.CredentialID != "AQID" ||
		definition.BearerTokenFile != "" {
		t.Fatalf("unexpected FIDO2 definition: %#v", definition)
	}
	metadata, err := os.Stat(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && metadata.Mode().Perm() != 0o600 {
		t.Fatalf("FIDO2 definition mode = %o", metadata.Mode().Perm())
	}
}

func TestEnrollMacosSecureEnclaveWritesProtectedExternalMember(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Secure Enclave enrollment is macOS-only")
	}
	root := t.TempDir()
	publicKey := testP256PublicKey(t)
	encodedPublicKey := base64.RawURLEncoding.EncodeToString(publicKey)
	publicKeyFingerprint := fmt.Sprintf("sha256:%x", sha256.Sum256(publicKey))
	helperPath := filepath.Join(root, "custodian")
	helper := fmt.Sprintf(
		"#!/bin/sh\nprintf '{\"application_tag\":\"%%s\",\"public_key\":\"%s\",\"public_key_data\":\"%s\","+
			"\"access_control\":\"biometry-current-set\",\"user_presence_required\":true}\\n' \"$2\"\n",
		publicKeyFingerprint,
		encodedPublicKey,
	)
	if err := os.WriteFile(helperPath, []byte(helper), 0o700); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(root, "member.json")
	command := newIndexKeysQuorumEnrollMacosSecureEnclaveCommand()
	command.SetArgs([]string{"--member", "enclave-a", "--custodian-path", helperPath, "--output", outputPath})
	if err := command.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	var definition externalPolicyMemberFile
	if err := readProtectedJSON(outputPath, "Secure Enclave member", &definition); err != nil {
		t.Fatal(err)
	}
	if definition.Provider != "macos-secure-enclave" || definition.Hardware == nil || definition.Hardware.CredentialID == "" ||
		definition.Hardware.PublicKey != publicKeyFingerprint ||
		definition.BearerTokenFile != "" ||
		definition.PINFile != "" ||
		!strings.Contains(definition.KeyReference, "access-control=biometry-current-set") {
		t.Fatalf("unexpected Secure Enclave definition: %#v", definition)
	}
	metadata, err := os.Stat(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Mode().Perm() != 0o600 {
		t.Fatalf("Secure Enclave definition mode = %o", metadata.Mode().Perm())
	}
}

func TestEnrollMacosSecureEnclaveRollsBackKeyWhenOutputExists(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Secure Enclave enrollment is macOS-only")
	}
	root := t.TempDir()
	publicKey := testP256PublicKey(t)
	encodedPublicKey := base64.RawURLEncoding.EncodeToString(publicKey)
	publicKeyFingerprint := fmt.Sprintf("sha256:%x", sha256.Sum256(publicKey))
	operationsPath := filepath.Join(root, "operations")
	helperPath := filepath.Join(root, "custodian")
	helper := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$1" >> %q
if [ "$1" = macos-secure-enclave-enroll ]; then
  printf '{"application_tag":"%%s","public_key":"%s","public_key_data":"%s","access_control":"biometry-current-set","user_presence_required":true}\n' "$2"
fi
`, operationsPath, publicKeyFingerprint, encodedPublicKey)
	if err := os.WriteFile(helperPath, []byte(helper), 0o700); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(root, "member.json")
	if err := os.WriteFile(outputPath, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := newIndexKeysQuorumEnrollMacosSecureEnclaveCommand()
	command.SetArgs([]string{"--member", "enclave-a", "--custodian-path", helperPath, "--output", outputPath})
	if err := command.ExecuteContext(t.Context()); err == nil {
		t.Fatal("enrollment unexpectedly replaced an existing output file")
	}
	operations, err := os.ReadFile(operationsPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(operations) != "macos-secure-enclave-enroll\nmacos-secure-enclave-delete\n" {
		t.Fatalf("unexpected helper operations: %q", operations)
	}
}

func TestValidateComposedPolicyMemberCredentials(t *testing.T) {
	policy := indexbroker.UnlockPolicy{Type: "all_of", Policies: []indexbroker.UnlockPolicy{
		{Type: "member", MemberID: "offline-a"},
		{Type: "any_of", Policies: []indexbroker.UnlockPolicy{
			{Type: "member", MemberID: "cloud-a"},
			{Type: "threshold", GroupID: "backup", Required: 1, Members: []string{"offline-b", "cloud-b"}},
		}},
	}}
	offline := []indexbroker.OfflinePolicyMember{{MemberID: "offline-a"}, {MemberID: "offline-b"}}
	external := []indexbroker.ExternalPolicyMember{{MemberID: "cloud-a"}, {MemberID: "cloud-b"}}
	if err := validatePolicyMemberCredentials(policy, offline, external); err != nil {
		t.Fatal(err)
	}
	duplicate := indexbroker.UnlockPolicy{Type: "all_of", Policies: []indexbroker.UnlockPolicy{
		{Type: "member", MemberID: "same"},
		{Type: "member", MemberID: "same"},
	}}
	if err := validatePolicyMemberCredentials(duplicate, []indexbroker.OfflinePolicyMember{{MemberID: "same"}}, nil); err == nil ||
		!strings.Contains(err.Error(), "appears more than once") {
		t.Fatalf("duplicate policy member was accepted: %v", err)
	}
}

func TestVerifyQuorumStatusFailsClosed(t *testing.T) {
	valid := indexbroker.Status{
		RepositoryID:      "repo-a",
		CapsuleGeneration: 3,
		CapsuleLogicalID:  "capsule-a",
		PolicyHash:        "hash-a",
		Compliant:         true,
		MinimumCustodians: 2,
	}
	if err := verifyQuorumStatus("repo-a", 3, "capsule-a", "hash-a", valid); err != nil {
		t.Fatal(err)
	}
	if err := verifyQuorumStatus("repo-a", 2, "capsule-a", "hash-a", valid); err == nil {
		t.Fatal("mismatched capsule generation accepted")
	}
	if err := verifyQuorumStatus("repo-a", 3, "capsule-a", "other-hash", valid); err == nil {
		t.Fatal("mismatched capsule policy accepted")
	}
	valid.Compliant = false
	valid.Findings = []string{"effective policy has a 1-of-1 path"}
	if err := verifyQuorumStatus("repo-a", 3, "capsule-a", "hash-a", valid); err == nil || !strings.Contains(err.Error(), "1-of-1") {
		t.Fatalf("non-compliant policy accepted: %v", err)
	}
}

func TestQuorumAccessRouteFindingsEnumeratesCompleteKeyBypasses(t *testing.T) {
	options := global.Options{
		PasswordFile:           "password",
		MasterKeyCommand:       "key-command",
		AzureKeyVaultURL:       "https://vault",
		MetadataKeyInDB:        true,
		MetadataPassphraseFile: "metadata-password",
	}
	status := daemon.KeyStatus{
		Slots:                         []daemon.KeySlotInfo{{ID: "recovery", Provider: "local-argon2id"}},
		PendingCapsuleMigrationSHA256: strings.Repeat("a", 64),
	}
	findings := quorumAccessRouteFindings(options, status)
	if len(findings) != 7 || !slices.Contains(findings, "capsule migration is prepared but not finalized") {
		t.Fatalf("bypass findings = %v", findings)
	}
	if clean := quorumAccessRouteFindings(global.Options{}, daemon.KeyStatus{}); len(clean) != 0 {
		t.Fatalf("clean broker route reported findings: %v", clean)
	}
}

type testCapsuleIdentity struct {
	repositoryID string
	generation   uint64
	logicalID    string
	policyHash   string
}

func (identity testCapsuleIdentity) RepositoryID() string { return identity.repositoryID }
func (identity testCapsuleIdentity) Generation() uint64   { return identity.generation }
func (identity testCapsuleIdentity) LogicalID() string    { return identity.logicalID }
func (identity testCapsuleIdentity) PolicyHash() string   { return identity.policyHash }

func TestQuorumAttestationFindingsVerifiesSignedCapsuleBinding(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	identity := testCapsuleIdentity{"repo-a", 7, "capsule-a", "policy-a"}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "attestation.pub")
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(publicKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	valid := quorumBypassAttestation{
		Version: 1, RepositoryID: identity.repositoryID, CapsuleGeneration: identity.generation,
		CapsuleLogicalID: identity.logicalID, PolicyHash: identity.policyHash,
		IssuedUnix: now.Add(-time.Hour).Unix(), ExpiresUnix: now.Add(24 * time.Hour).Unix(),
		Statements: quorumBypassStatements{true, true, true, true, true, true},
	}
	writeAttestation := func(t *testing.T, attestation quorumBypassAttestation, mode os.FileMode) string {
		t.Helper()
		payload, err := attestation.signedPayload()
		if err != nil {
			t.Fatal(err)
		}
		attestation.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
		encoded, err := json.Marshal(attestation)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "attestation.json")
		if err := os.WriteFile(path, encoded, mode); err != nil {
			t.Fatal(err)
		}
		return path
	}

	if findings := quorumAttestationFindings(identity, writeAttestation(t, valid, 0o600), keyPath, now); len(findings) != 0 {
		t.Fatalf("valid attestation findings = %v", findings)
	}

	tests := []struct {
		name        string
		change      func(*quorumBypassAttestation)
		wantFinding string
	}{
		{"capsule mismatch", func(value *quorumBypassAttestation) { value.CapsuleGeneration++ }, "does not match"},
		{"expired", func(value *quorumBypassAttestation) { value.ExpiresUnix = now.Unix() }, "expired"},
		{"overlong validity", func(value *quorumBypassAttestation) {
			value.ExpiresUnix = value.IssuedUnix + int64(91*24*time.Hour/time.Second)
		}, "validity window"},
		{"missing statement", func(value *quorumBypassAttestation) { value.Statements.NoWarmRestartMaterial = false }, "warm-restart"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attestation := valid
			test.change(&attestation)
			findings := quorumAttestationFindings(identity, writeAttestation(t, attestation, 0o600), keyPath, now)
			if len(findings) == 0 || !strings.Contains(findings[0], test.wantFinding) {
				t.Fatalf("findings = %v, want %q", findings, test.wantFinding)
			}
		})
	}

	tamperedPath := writeAttestation(t, valid, 0o600)
	tampered, err := os.ReadFile(tamperedPath)
	if err != nil {
		t.Fatal(err)
	}
	tampered = bytes.Replace(tampered, []byte(`"repository_id":"repo-a"`), []byte(`"repository_id":"repo-b"`), 1)
	if err := os.WriteFile(tamperedPath, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	if findings := quorumAttestationFindings(identity, tamperedPath, keyPath, now); len(findings) == 0 || !strings.Contains(findings[0], "signature") {
		t.Fatalf("tampered attestation findings = %v", findings)
	}
	undomained := valid
	undomained.Signature = ""
	undomainedPayload, err := json.Marshal(undomained)
	if err != nil {
		t.Fatal(err)
	}
	undomained.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, undomainedPayload))
	undomainedPath := filepath.Join(t.TempDir(), "undomained.json")
	undomainedJSON, err := json.Marshal(undomained)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(undomainedPath, undomainedJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	if findings := quorumAttestationFindings(identity, undomainedPath, keyPath, now); len(findings) == 0 || !strings.Contains(findings[0], "signature") {
		t.Fatalf("undomained signature findings = %v", findings)
	}

	if runtime.GOOS != "windows" {
		unprotectedPath := writeAttestation(t, valid, 0o644)
		if findings := quorumAttestationFindings(identity, unprotectedPath, keyPath, now); len(findings) == 0 ||
			!strings.Contains(findings[0], "group or others") {
			t.Fatalf("unprotected attestation findings = %v", findings)
		}
	}
}

func TestGenerateQuorumAttestationKeyCommand(t *testing.T) {
	directory := t.TempDir()
	privateKeyPath := filepath.Join(directory, "attestation.key")
	publicKeyPath := filepath.Join(directory, "attestation.pub")
	options := &global.Options{Term: &ui.MockTerminal{}}
	command := newIndexKeysQuorumGenerateAttestationKeyCommand(options)
	command.SetArgs([]string{"--private-key", privateKeyPath, "--public-key", publicKeyPath})
	if err := command.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	privateEncoded, err := readProtectedBinary(privateKeyPath, "private key", true)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := base64.StdEncoding.DecodeString(string(privateEncoded))
	if err != nil || len(privateKey) != ed25519.PrivateKeySize {
		t.Fatalf("generated private key is invalid: %v", err)
	}
	publicEncoded, err := readProtectedBinary(publicKeyPath, "public key", true)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := base64.StdEncoding.DecodeString(string(publicEncoded))
	if err != nil || !bytes.Equal(publicKey, privateKey[ed25519.SeedSize:]) {
		t.Fatalf("generated public key does not match private key: %v", err)
	}
	originalPrivate := append([]byte(nil), privateEncoded...)
	second := newIndexKeysQuorumGenerateAttestationKeyCommand(options)
	second.SetArgs([]string{"--private-key", privateKeyPath, "--public-key", publicKeyPath})
	if err := second.ExecuteContext(t.Context()); err == nil {
		t.Fatal("existing attestation key was overwritten")
	}
	unchangedPrivate, err := os.ReadFile(privateKeyPath)
	if err != nil || !bytes.Equal(bytes.TrimSpace(unchangedPrivate), originalPrivate) {
		t.Fatalf("private key changed after overwrite attempt: %v", err)
	}
}

func TestRetireLegacyQuorumBypassesPreservesCapsuleMetadata(t *testing.T) {
	ctx := context.Background()
	store := mem.New()
	handles := []backend.Handle{
		{Type: backend.KeyFile, Name: "password-a"},
		{Type: backend.KeyFile, Name: "password-b"},
		{Type: backend.SlateDBFile, Name: "escrow-old.json", IsMetadata: true},
		{Type: backend.SlateDBFile, Name: "recovery-capsule-0001.json", IsMetadata: true},
	}
	for _, handle := range handles {
		if err := store.Save(ctx, handle, backend.NewByteReader([]byte("record"), store.Hasher())); err != nil {
			t.Fatal(err)
		}
	}
	keys, escrows, err := retireLegacyQuorumBypasses(ctx, store)
	if err != nil || keys != 2 || escrows != 1 {
		t.Fatalf("retirement = keys %d, escrows %d, err %v", keys, escrows, err)
	}
	for _, handle := range handles[:3] {
		if _, err := store.Stat(ctx, handle); err == nil {
			t.Fatalf("legacy bypass %s remains", handle.Name)
		}
	}
	if _, err := store.Stat(ctx, handles[3]); err != nil {
		t.Fatalf("capsule metadata was removed: %v", err)
	}
}

type failRemoveOnceBackend struct {
	backend.Backend
	target string
	failed bool
}

func (destination *failRemoveOnceBackend) Remove(ctx context.Context, handle backend.Handle) error {
	if handle.Name == destination.target && !destination.failed {
		destination.failed = true
		return errors.New("injected removal failure")
	}
	return destination.Backend.Remove(ctx, handle)
}

func TestRetireLegacyQuorumBypassesResumesAfterPartialFailure(t *testing.T) {
	ctx := context.Background()
	store := mem.New()
	handles := []backend.Handle{
		{Type: backend.KeyFile, Name: "password-a"},
		{Type: backend.KeyFile, Name: "password-b"},
		{Type: backend.SlateDBFile, Name: "escrow-old.json", IsMetadata: true},
		{Type: backend.SlateDBFile, Name: "recovery-capsule-0001.json", IsMetadata: true},
	}
	for _, handle := range handles {
		if err := store.Save(ctx, handle, backend.NewByteReader([]byte(handle.Name), store.Hasher())); err != nil {
			t.Fatal(err)
		}
	}
	failing := &failRemoveOnceBackend{Backend: store, target: "password-b"}
	if _, _, err := retireLegacyQuorumBypasses(ctx, failing); err == nil || !strings.Contains(err.Error(), "injected removal failure") {
		t.Fatalf("partial retirement error = %v", err)
	}
	keys, escrows, err := retireLegacyQuorumBypasses(ctx, failing)
	if err != nil || keys != 1 || escrows != 1 {
		t.Fatalf("resumed retirement = keys %d, escrows %d, err %v", keys, escrows, err)
	}
	for _, handle := range handles[:3] {
		if _, err := store.Stat(ctx, handle); err == nil {
			t.Fatalf("legacy bypass %s remains", handle.Name)
		}
	}
	if _, err := store.Stat(ctx, handles[3]); err != nil {
		t.Fatalf("capsule metadata was removed: %v", err)
	}
}

func TestMetadataRebuildImportRequiresRecoveryGuards(t *testing.T) {
	base := indexImportOptions{
		Activate:                   true,
		FromLegacy:                 true,
		ConfirmMetadataLossRebuild: true,
		Daemon: indexDaemonOptions{
			Start: true, RebuildInitialize: true, EncryptionMode: "required",
			DataDir: filepath.Join(t.TempDir(), "candidate"), BrokerSocket: "/tmp/broker.sock",
		},
	}
	globalOptions := global.Options{MetadataLossRecovery: true, KeyBrokerSocket: "/tmp/broker.sock"}
	tests := []struct {
		name   string
		mutate func(*indexImportOptions, *global.Options)
		want   string
	}{
		{
			name:   "acknowledgement",
			mutate: func(options *indexImportOptions, _ *global.Options) { options.ConfirmMetadataLossRebuild = false },
			want:   "requires --confirm-metadata-loss-rebuild",
		},
		{
			name:   "new candidate",
			mutate: func(options *indexImportOptions, _ *global.Options) { options.Daemon.DataDir = t.TempDir() },
			want:   "candidate directory already exists",
		},
		{name: "shared broker", mutate: func(_ *indexImportOptions, globalOptions *global.Options) {
			globalOptions.KeyBrokerSocket = "/tmp/other.sock"
		}, want: "use the same key broker"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := base
			globals := globalOptions
			test.mutate(&options, &globals)
			_, err := runIndexImport(context.Background(), options, globals, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("metadata rebuild guard = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateMetadataRebuildTarget(t *testing.T) {
	local := filepath.Join(t.TempDir(), "new-candidate")
	if target, err := validateMetadataRebuildTarget(indexDaemonOptions{DataDir: local}, false); err != nil || target != local {
		t.Fatalf("local candidate = %q, %v", target, err)
	}
	remote := indexDaemonOptions{ObjectStore: "s3", S3Bucket: "metadata-bucket", S3Prefix: "/repo-a/rebuild-2026/"}
	if target, err := validateMetadataRebuildTarget(remote, false); err != nil || target != "s3://metadata-bucket/repo-a/rebuild-2026" {
		t.Fatalf("remote candidate = %q, %v", target, err)
	}
	rados := indexDaemonOptions{ObjectStore: "rados", RadosPool: "db-sst", RadosNamespace: "vaultic-perf", RadosPrefix: "/repo-a/rebuild-2026/"}
	if target, err := validateMetadataRebuildTarget(rados, false); err != nil || target != "rados://db-sst/vaultic-perf/repo-a/rebuild-2026" {
		t.Fatalf("RADOS candidate = %q, %v", target, err)
	}
	if candidate := rebuildCandidateName(rados); candidate != "rados://db-sst/vaultic-perf/repo-a/rebuild-2026" {
		t.Fatalf("RADOS candidate name = %q", candidate)
	}
	existing := t.TempDir()
	if target, err := validateMetadataRebuildTarget(indexDaemonOptions{DataDir: existing}, true); err != nil || target != existing {
		t.Fatalf("reset local candidate = %q, %v", target, err)
	}
	for _, invalid := range []indexDaemonOptions{
		{ObjectStore: "s3", S3Bucket: "metadata-bucket"},
		{ObjectStore: "s3", S3Bucket: "metadata-bucket", S3Prefix: "candidate", DataDir: local},
		{ObjectStore: "rados", RadosPool: "db-sst", RadosNamespace: "vaultic-perf"},
		{ObjectStore: "rados", RadosPool: "db-sst", RadosNamespace: "vaultic-perf", RadosPrefix: "candidate", DataDir: local},
		{ObjectStore: "memory"},
	} {
		if _, err := validateMetadataRebuildTarget(invalid, false); err == nil {
			t.Fatalf("invalid rebuild target accepted: %+v", invalid)
		}
	}
}

func TestForceResetOldIndexRequiresStartedPersistentTarget(t *testing.T) {
	base := indexImportOptions{ForceResetOldIndex: true, FromLegacy: true}
	tests := []struct {
		name   string
		mutate func(*indexImportOptions)
		want   string
	}{
		{name: "started daemon", want: "requires --start-daemon"},
		{name: "not dry run", mutate: func(options *indexImportOptions) {
			options.Daemon.Start = true
			options.DryRun = true
		}, want: "cannot be combined with --dry-run"},
		{name: "temporary daemon", mutate: func(options *indexImportOptions) {
			options.Daemon.Start = true
			options.Daemon.Persistent = true
		}, want: "cannot be combined with --persistent-daemon"},
		{name: "persistent target", mutate: func(options *indexImportOptions) {
			options.Daemon.Start = true
		}, want: "local metadata rebuild candidate requires a new --daemon-data-dir"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := base
			if test.mutate != nil {
				test.mutate(&options)
			}
			_, err := runIndexImport(context.Background(), options, global.Options{}, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("reset guard = %v, want %q", err, test.want)
			}
		})
	}
}

func TestIndexImportRejectsUnboundedPackFloorBeforeReset(t *testing.T) {
	_, err := validateIndexImportOptions(indexImportOptions{
		FromLegacy: true, ForceResetOldIndex: true,
		PacksPerTransaction: legacyimport.MaxPacksPerTransaction + 1,
	})
	if err == nil || !strings.Contains(err.Error(), "--packs-per-transaction must not exceed 256") {
		t.Fatalf("unbounded pack floor error = %v", err)
	}
}

func TestIndexImportRejectsExcessPublicationLanes(t *testing.T) {
	_, err := validateIndexImportOptions(indexImportOptions{FromLegacy: true, ImportPublicationLanes: 9})
	if err == nil || !strings.Contains(err.Error(), "--import-publication-lanes must not exceed 8") {
		t.Fatalf("publication lanes validation error = %v", err)
	}
}

func TestIndexImportRejectsSplitLanesWithoutFreshReset(t *testing.T) {
	_, err := validateIndexImportOptions(indexImportOptions{FromLegacy: true, ImportPublicationLanes: 2})
	if err == nil || !strings.Contains(err.Error(), "requires --force-reset-old-idx") {
		t.Fatalf("fresh split-lane validation error = %v", err)
	}
}

func TestIndexImportDefaultsPublicationLanesForNonFresh(t *testing.T) {
	options, err := validateIndexImportOptions(indexImportOptions{FromLegacy: true})
	if err != nil {
		t.Fatal(err)
	}
	if options.ImportPublicationLanes != 1 {
		t.Fatalf("non-fresh publication lanes default = %d, want 1", options.ImportPublicationLanes)
	}
}

func TestBulkImportMemoryProfileScalesAndCaps(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)
	for _, test := range []struct {
		physical, cache, unflushed uint64
	}{
		{physical: 0, cache: 0, unflushed: 4 * gib},
		{physical: 16 * gib, cache: 4 * gib, unflushed: gib},
		{physical: 64 * gib, cache: 24 * gib, unflushed: 6 * gib},
		{physical: 256 * gib, cache: 64 * gib, unflushed: 16 * gib},
	} {
		profile := newBulkImportMemoryProfile(test.physical)
		if profile.readCacheBytes != test.cache || profile.maxUnflushedBytes != test.unflushed {
			t.Errorf("profile(%d) = cache %d, unflushed %d; want %d, %d",
				test.physical, profile.readCacheBytes, profile.maxUnflushedBytes, test.cache, test.unflushed)
		}
	}
}

func TestFreshBulkImportDefaultsPreserveExplicitTuning(t *testing.T) {
	options := applyFreshBulkImportDefaults(indexImportOptions{
		Daemon: indexDaemonOptions{
			WALStore: "local", WALFlushInterval: time.Second,
			MaxUnflushedBytes: 2, L0SSTSizeBytes: 3,
		},
		PackWorkers: 7, ImportPublicationLanes: 5, PacksPerTransaction: 4, ImportTransactionBytes: 1024,
		PreparedImportBytes: 2048, ImportBatchTimeout: time.Minute,
	})
	if options.Daemon.WALStore != "local" || options.Daemon.WALFlushInterval != time.Second ||
		options.Daemon.MaxUnflushedBytes != 2 || options.Daemon.L0SSTSizeBytes != 3 || options.PackWorkers != 7 ||
		options.ImportPublicationLanes != 5 ||
		options.PacksPerTransaction != 4 || options.ImportTransactionBytes != 1024 ||
		options.PreparedImportBytes != 2048 || options.ImportBatchTimeout != time.Minute {
		t.Fatalf("explicit bulk-import tuning was replaced: %+v", options)
	}
	if !options.Daemon.FreshBulkImport {
		t.Fatal("fresh bulk-import profile was not marked active")
	}
}

func TestImportDeferredCleanupValidation(t *testing.T) {
	for _, test := range []struct {
		name  string
		fresh bool
		wal   string
		lanes uint
		valid bool
	}{
		{"nonfresh", false, "memory", 2, false},
		{"persistent", true, "local", 2, false},
		{"serial", true, "memory", 1, false},
		{"fresh", true, "memory", 2, true},
		{"rados", true, "rados", 2, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateIndexImportOptions(indexImportOptions{
				FromLegacy: true, ForceResetOldIndex: test.fresh, ImportDeferCleanup: true, ImportPublicationLanes: test.lanes,
				Daemon: indexDaemonOptions{Start: true, DataDir: filepath.Join(t.TempDir(), "candidate"), WALStore: test.wal},
			})
			if (err == nil) != test.valid {
				t.Fatalf("valid=%t error=%v", test.valid, err)
			}
		})
	}
}

func TestImportStatsReportsWithoutProgressAndStops(t *testing.T) {
	reported := make(chan struct{}, 1)
	stop := startLegacyImportStats(context.Background(), &daemon.SchemaStore{}, time.Millisecond, func(daemon.LegacyImportStats) {
		select {
		case reported <- struct{}{}:
		default:
		}
	})
	defer stop()
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("no telemetry without progress")
	}
	stop()
	stop()
}

func TestFormatLegacyOperationStatsIsStableAndSkipsEmpty(t *testing.T) {
	formatted := formatLegacyOperationStats(map[string]daemon.DurationDistribution{
		"zeta":  {Count: 2, Sum: 3 * time.Millisecond, P50: time.Millisecond, P95: 2 * time.Millisecond, P99: 2 * time.Millisecond},
		"empty": {},
		"alpha": {Count: 1, Sum: time.Microsecond, P50: time.Microsecond, P95: time.Microsecond, P99: time.Microsecond},
	})
	want := "alpha=count:1,sum:1µs,p50<=1µs,p95<=1µs,p99<=1µs zeta=count:2,sum:3ms,p50<=1ms,p95<=2ms,p99<=2ms"
	if formatted != want {
		t.Fatalf("formatted operation stats = %q, want %q", formatted, want)
	}
}

func TestFormatLegacySchedulerStatsIsStableAndSkipsEmptyOperations(t *testing.T) {
	formatted := formatLegacySchedulerStats(legacyimport.SchedulerSnapshot{
		Phase: "reduce", PhaseTime: map[string]time.Duration{"reduce": 2 * time.Millisecond, "source": time.Millisecond},
		LaneTime: [9]time.Duration{time.Millisecond, 2 * time.Millisecond}, ActiveLanes: 1,
		ReadyBatches: 2, PendingReductionBatches: 3, RetainedPreparedBytes: 4, UnreducedPreparedBytes: 5,
		OldestUnreducedAge: 6 * time.Millisecond,
		Operations: map[string]legacyimport.DurationDistribution{
			"zeta":  {Count: 2, Sum: 3 * time.Millisecond, P50: time.Millisecond, P95: 2 * time.Millisecond, P99: 2 * time.Millisecond},
			"empty": {},
			"alpha": {Count: 1, Sum: time.Microsecond, P50: time.Microsecond, P95: time.Microsecond, P99: time.Microsecond},
		},
	})
	want := "phase=reduce phase_time=[reduce:2ms,source:1ms] lane_time=[0:1ms,1:2ms] active_lanes=1 ready=2 " +
		"pending_reduction=3 retained_bytes=4 unreduced_bytes=5 oldest_unreduced=6ms operations=[" +
		"alpha=count:1,sum:1µs,p50<=1µs,p95<=1µs,p99<=1µs zeta=count:2,sum:3ms,p50<=1ms,p95<=2ms,p99<=2ms]"
	if formatted != want {
		t.Fatalf("formatted scheduler stats = %q, want %q", formatted, want)
	}
}

func TestCompletedBulkImportReopensPersistentWALWithoutReset(t *testing.T) {
	options := completedBulkImportDaemonOptions(indexDaemonOptions{
		Start: true, RebuildInitialize: true, RebuildReset: true, FreshBulkImport: true,
		WALStore: "memory", WALDataDir: "/var/lib/vaultic/wal", DataDir: "/var/lib/vaultic/candidate",
	})
	if options.RebuildInitialize || options.RebuildReset || options.FreshBulkImport || options.WALStore != "" {
		t.Fatalf("completed bulk-import options retain destructive or memory-WAL state: %+v", options)
	}
	if !options.Start || options.WALDataDir != "/var/lib/vaultic/wal" || options.DataDir != "/var/lib/vaultic/candidate" {
		t.Fatalf("completed bulk-import options lost restart identity: %+v", options)
	}
}

func TestForceResetEnablesFreshBulkImportProfile(t *testing.T) {
	options, err := validateIndexImportOptions(indexImportOptions{
		ForceResetOldIndex: true,
		FromLegacy:         true,
		Daemon: indexDaemonOptions{
			Start:   true,
			DataDir: filepath.Join(t.TempDir(), "candidate"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !options.Daemon.FreshBulkImport || options.Daemon.WALStore != "memory" ||
		options.Daemon.WALFlushInterval != 500*time.Millisecond ||
		options.Daemon.L0SSTSizeBytes != bulkImportL0SSTSizeBytes ||
		options.Daemon.MaxUnflushedBytes == 0 || options.PackWorkers == 0 || options.ImportPublicationLanes != 2 ||
		options.PacksPerTransaction != 8 ||
		options.ImportTransactionBytes != 8<<20 || options.PreparedImportBytes != 256<<20 ||
		options.ImportBatchTimeout != 4*time.Minute {
		t.Fatalf("fresh bulk-import profile is incomplete: %+v", options)
	}
}

func TestImportProgressReporterFormatsAndThrottles(t *testing.T) {
	started := time.Date(2026, time.July, 7, 12, 0, 0, 0, time.UTC)
	var logged []string
	reporter := &importProgressReporter{
		started: started,
		log:     func(message string) { logged = append(logged, message) },
	}
	reporter.update(started, legacyimport.Progress{IndexesTotal: 4, SnapshotsTotal: 2})
	reporter.update(started.Add(time.Second), legacyimport.Progress{IndexesTotal: 4, SnapshotsTotal: 2})
	reporter.update(started.Add(4*time.Second), legacyimport.Progress{
		IndexesCompleted: 2, IndexesTotal: 4, IndexesImported: 1, IndexesResumed: 1,
		SnapshotsTotal: 2, PacksPrepared: 8, PacksImported: 6, BlobsImported: 17,
		PreparedBytes: 4096, BatchesCommitted: 2, CheckpointPending: true,
	})
	reporter.update(started.Add(8*time.Second), legacyimport.Progress{
		IndexesCompleted: 4, IndexesTotal: 4, IndexesImported: 3, IndexesResumed: 1,
		SnapshotsTotal: 2, PacksPrepared: 12, PacksImported: 12, BlobsImported: 34,
		PreparedBytes: 8192, BatchesCommitted: 4,
	})
	reporter.update(started.Add(12*time.Second), legacyimport.Progress{
		IndexesCompleted: 4, IndexesTotal: 4, IndexesImported: 3, IndexesResumed: 1,
		SnapshotsCompleted: 2, SnapshotsTotal: 2, SnapshotsImported: 2,
		PacksPrepared: 12, PacksImported: 12, BlobsImported: 34, PreparedBytes: 8192,
		BatchesCommitted: 4, NodesImported: 8,
	})
	if len(logged) != 4 {
		t.Fatalf("progress messages = %q", logged)
	}
	wantInitial := "legacy import progress: 0.0%; indexes 0/4 (imported 0, resumed 0); " +
		"snapshots 0/2 (imported 0, resumed 0); packs prepared/committed 0/0; blobs 0; " +
		"batches 0 (adaptive splits 0); prepared bytes total 0; queue 0 packs/0 bytes (peak 0 packs/0 bytes); " +
		"stage time prepare aggregate=0s publish aggregate-lane=0s checkpoint batch=0s; checkpoint pending false; speed last interval unknown; " +
		"speed since start unknown; elapsed 0s; est. remaining unknown; ETA unknown"
	if logged[0] != wantInitial {
		t.Fatalf("initial progress = %q", logged[0])
	}
	wantPartial := "legacy import progress: 33.3%; indexes 2/4 (imported 1, resumed 1); " +
		"snapshots 0/2 (imported 0, resumed 0); packs prepared/committed 8/6; blobs 17; " +
		"batches 2 (adaptive splits 0); prepared bytes total 4096; queue 0 packs/0 bytes (peak 0 packs/0 bytes); " +
		"stage time prepare aggregate=0s publish aggregate-lane=0s checkpoint batch=0s; checkpoint pending true; " +
		"speed last interval 1.5 packs/s, 4.2 blobs/s, 0.0 nodes/s; " +
		"speed since start 1.5 packs/s, 4.2 blobs/s, 0.0 nodes/s; elapsed 4s; " +
		"est. remaining 8s; ETA 2026-07-07T12:00:12Z"
	if logged[1] != wantPartial {
		t.Fatalf("partial progress = %q", logged[1])
	}
	wantIndexesComplete := "legacy import progress: 66.7%; indexes 4/4 (imported 3, resumed 1); " +
		"snapshots 0/2 (imported 0, resumed 0); packs prepared/committed 12/12; blobs 34; " +
		"batches 4 (adaptive splits 0); prepared bytes total 8192; queue 0 packs/0 bytes (peak 0 packs/0 bytes); " +
		"stage time prepare aggregate=0s publish aggregate-lane=0s checkpoint batch=0s; checkpoint pending false; " +
		"speed last interval 1.5 packs/s, 4.2 blobs/s, 0.0 nodes/s; " +
		"speed since start 1.5 packs/s, 4.2 blobs/s, 0.0 nodes/s; elapsed 8s; " +
		"est. remaining 4s; ETA 2026-07-07T12:00:12Z"
	if logged[2] != wantIndexesComplete {
		t.Fatalf("index completion progress = %q", logged[2])
	}
	wantFinal := "legacy import progress: 100.0%; indexes 4/4 (imported 3, resumed 1); " +
		"snapshots 2/2 (imported 2, resumed 0); packs prepared/committed 12/12; blobs 34; " +
		"batches 4 (adaptive splits 0); prepared bytes total 8192; queue 0 packs/0 bytes (peak 0 packs/0 bytes); " +
		"stage time prepare aggregate=0s publish aggregate-lane=0s checkpoint batch=0s; checkpoint pending false; " +
		"speed last interval 0.0 packs/s, 0.0 blobs/s, 2.0 nodes/s; " +
		"speed since start 1.0 packs/s, 2.8 blobs/s, 0.7 nodes/s; elapsed 12s; " +
		"est. remaining 0s; ETA 2026-07-07T12:00:12Z"
	if logged[3] != wantFinal {
		t.Fatalf("final progress = %q", logged[3])
	}
}

func TestIndexImportRegistersBulkTransactionFlags(t *testing.T) {
	command := newIndexImportCommand(&global.Options{})
	for _, name := range []string{
		"packs-per-transaction", "import-transaction-bytes", "prepared-import-bytes", "import-batch-timeout",
		"import-publication-lanes",
	} {
		if command.Flags().Lookup(name) == nil {
			t.Errorf("import flag --%s is not registered", name)
		}
	}
}

func TestGenerationAnchorNeverMovesBackward(t *testing.T) {
	path := t.TempDir() + "/generation"
	if generation, err := readGenerationAnchor(path, 4); err != nil || generation != 4 {
		t.Fatalf("initial anchor = %d, %v", generation, err)
	}
	if err := writeGenerationAnchor(path, 7); err != nil {
		t.Fatal(err)
	}
	if generation, err := readGenerationAnchor(path, 4); err != nil || generation != 7 {
		t.Fatalf("persisted anchor = %d, %v", generation, err)
	}
	if err := writeGenerationAnchor(path, 6); err == nil {
		t.Fatal("generation anchor downgrade was accepted")
	}
}

func TestReadMetadataPassphrase(t *testing.T) {
	path := t.TempDir() + "/passphrase"
	if err := os.WriteFile(path, []byte("secret\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err := readMetadataPassphrase(path)
	if err != nil || string(value) != "secret" {
		t.Fatalf("passphrase = %q, err=%v", value, err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := readMetadataPassphrase(path); err == nil {
			t.Fatal("insecure passphrase permissions were accepted")
		}
	}
}

func TestVerifyGDPRCertificateRequiresTrustedIdentity(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := schema.DeletionCertificateRecord{UID: 42, ExecutedAt: 100, RunID: schema.ID{1}, SigningAlgorithm: "Ed25519", PublicKey: publicKey}
	signingBytes, err := certificate.SigningBytes()
	if err != nil {
		t.Fatal(err)
	}
	certificate.Signature = ed25519.Sign(privateKey, signingBytes)
	if err := verifyGDPRCertificate(certificate, publicKey); err != nil {
		t.Fatalf("trusted certificate rejected: %v", err)
	}
	otherPublicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyGDPRCertificate(certificate, otherPublicKey); err == nil {
		t.Fatal("certificate signed by an untrusted identity accepted")
	}
}

func TestAnalyticsIDConversionRejectsOverflow(t *testing.T) {
	if values, err := toUint32([]uint{0, math.MaxUint32}); err != nil || len(values) != 2 || values[1] != math.MaxUint32 {
		t.Fatalf("valid IDs rejected: %v, %v", values, err)
	}
	if ^uint(0) > math.MaxUint32 {
		if _, err := toUint32([]uint{uint(math.MaxUint32) + 1}); err == nil {
			t.Fatal("overflowing ID accepted")
		}
	}
}

func TestAnalyticsQueryOptionValidation(t *testing.T) {
	for name, options := range map[string]indexAnalyticsOptions{
		"equal size bounds":    {HasSizeMin: true, HasSizeMax: true, SizeMin: 10, SizeMax: 10},
		"reversed size bounds": {HasSizeMin: true, HasSizeMax: true, SizeMin: 11, SizeMax: 10},
		"current and stale":    {RequireCurrent: true, AllowStale: true},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateAnalyticsQueryOptions(options); err == nil {
				t.Fatal("invalid analytics options accepted")
			}
		})
	}
	if err := validateAnalyticsQueryOptions(indexAnalyticsOptions{HasSizeMin: true, HasSizeMax: true, SizeMin: 10, SizeMax: 11}); err != nil {
		t.Fatalf("valid half-open size range rejected: %v", err)
	}
}

func TestAnalyticsPhase16CommandSurface(t *testing.T) {
	command := newIndexAnalyticsCommand(&global.Options{})
	flagNames := []string{
		"uid", "gid", "year", "month", "iso-year", "workweek", "svm", "volume", "path-group", "size-min", "size-max",
		"size-log10", "residency", "creation-basis", "identity-continuity", "group-by", "include-incomplete", "require-current",
		"allow-stale", "explain", "async", "query-id", "resume", "cancel", "wait",
	}
	for _, name := range flagNames {
		if command.Flags().Lookup(name) == nil {
			t.Errorf("analytics flag --%s is not registered", name)
		}
	}
	for _, path := range [][]string{{"status"}, {"catch-up"}, {"cache"}, {"cache", "purge"}} {
		child, args, err := command.Find(path)
		if err != nil || child == nil || len(args) != 0 || child.Name() != path[len(path)-1] {
			t.Errorf("analytics command %v = %v, %v, %v", path, child, args, err)
		}
	}
}

func TestAnalyticsParsingAndQueryConstruction(t *testing.T) {
	id, err := parseAnalyticsID(strings.Repeat("01", 32))
	if err != nil || id[0] != 1 || id[31] != 1 {
		t.Fatalf("valid query ID rejected: %x, %v", id, err)
	}
	if _, err := parseAnalyticsID("01"); err == nil {
		t.Fatal("short query ID accepted")
	}
	since, until, err := parseTimeRange("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z")
	if err != nil || *since != time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() || *until <= *since {
		t.Fatalf("valid time range rejected: %v, %v, %v", since, until, err)
	}
	if _, _, err := parseTimeRange("2024-02-01T00:00:00Z", "2024-01-01T00:00:00Z"); err == nil {
		t.Fatal("reversed time range accepted")
	}
	groups := []string{"uid", "year"}
	query, err := analyticsQuery(indexAnalyticsOptions{GroupBy: groups, CreationBases: []string{"first-seen"}, IncludeIncomplete: true})
	if err != nil || len(query.GroupBy) != 2 || query.CreationBases[0] != "first-seen" {
		t.Fatalf("query construction failed: %+v, %v", query, err)
	}
	query.GroupBy[0] = "gid"
	if groups[0] != "uid" {
		t.Fatal("analytics query construction mutated caller-owned slices")
	}
}

func TestAnalyticsJobOptionValidation(t *testing.T) {
	invalidOptions := []indexAnalyticsOptions{
		{Async: true, QueryID: strings.Repeat("0", 64)},
		{Resume: true},
		{Cancel: true},
		{Resume: true, Cancel: true, QueryID: strings.Repeat("0", 64)},
		{Wait: true},
	}
	for _, options := range invalidOptions {
		if err := validateAnalyticsJobOptions(options); err == nil {
			t.Fatalf("invalid job options accepted: %+v", options)
		}
	}
}

func TestIndexDaemonOptionsRequireExplicitTCPConfiguration(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "daemon.token")
	if err := os.WriteFile(tokenFile, []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, options := range map[string]indexDaemonOptions{
		"token without TCP":        {AuthTokenFile: tokenFile},
		"allowlist without TCP":    {TCPAllowlist: []string{"127.0.0.0/8"}},
		"socket and TCP":           {Socket: "/tmp/vaulticdb.sock", TCPAddress: "127.0.0.1:9876"},
		"persistent without start": {Persistent: true},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := options.Config("repository"); err == nil {
				t.Fatal("invalid daemon options accepted")
			}
		})
	}
	validOptions := indexDaemonOptions{
		TCPAddress: "127.0.0.1:9876", TCPAllowlist: []string{"127.0.0.0/8"}, AuthTokenFile: tokenFile, Start: true, DaemonPath: "vaulticdb",
	}
	config, err := validOptions.Config(
		"repository",
	)
	if err != nil || config.RepositoryID != "repository" || config.DaemonPath != "vaulticdb" || config.Socket != "" || config.AuthToken != "token" {
		t.Fatalf("valid TCP config = %#v, %v", config, err)
	}
	s3, err := (indexDaemonOptions{Start: true, DaemonPath: "vaulticdb", ObjectStore: "s3", S3Bucket: "bucket", S3Prefix: "repo/index"}).Config("repository")
	if err != nil || s3.ObjectStore != "s3" || s3.S3Bucket != "bucket" || s3.S3Prefix != "repo/index" {
		t.Fatalf("S3 config = %#v, %v", s3, err)
	}
	keyringFile := filepath.Join(t.TempDir(), "ceph.keyring")
	if err := os.WriteFile(keyringFile, []byte("[client.amakura]\n\tkey = AQIDBA==\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	walKeyringFile := filepath.Join(t.TempDir(), "ceph-wal.keyring")
	if err := os.WriteFile(walKeyringFile, []byte("[client.amakura]\n\tkey = BQYHCA==\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rados, err := (indexDaemonOptions{
		Start: true, DaemonPath: "vaulticdb", ObjectStore: "rados", RadosKeyFile: keyringFile,
		RadosMonitors: "mon-a:3300", RadosFSID: "2f525d6a-8f31-4f79-b731-82a6acb235f5",
		RadosPool: "db-sst", RadosNamespace: "repository", RadosPrefix: "main",
		RadosClient: "client.amakura", WALStore: "rados", WALRadosKeyFile: walKeyringFile,
		WALRadosMonitors: "mon-a:3300", WALRadosFSID: "2f525d6a-8f31-4f79-b731-82a6acb235f5",
		WALRadosPool: "db-wal", WALRadosNamespace: "repository", WALRadosPrefix: "wal",
		WALRadosClient: "client.amakura",
	}).Config("repository")
	if err != nil || rados.RadosPool != "db-sst" || rados.WALRadosPool != "db-wal" ||
		rados.RadosKey != "AQIDBA==" || rados.WALRadosKey != "BQYHCA==" {
		t.Fatalf("dual-pool RADOS keyring config loaded = %t, error = %v", rados.RadosKey != "" && rados.WALRadosKey != "", err)
	}
}

func TestParseCephXKey(t *testing.T) {
	for _, test := range []struct {
		name    string
		encoded string
		client  string
		want    string
		wantErr bool
	}{
		{name: "raw", encoded: "AQIDBA==\n", client: "client.amakura", want: "AQIDBA=="},
		{name: "keyring", encoded: "# generated by Ceph\n\n[client.other]\nkey = ignored==\n[client.amakura]\n\tkey = AQIDBA==\n", client: "client.amakura", want: "AQIDBA=="},
		{name: "missing client", encoded: "[client.other]\nkey = ignored==\n", client: "client.amakura", wantErr: true},
		{name: "duplicate", encoded: "[client.amakura]\nkey = first==\nkey = second==\n", client: "client.amakura", wantErr: true},
		{name: "malformed assignment", encoded: "key = AQIDBA==\n", client: "client.amakura", wantErr: true},
		{name: "multiline raw", encoded: "AQID\nBA==\n", client: "client.amakura", wantErr: true},
		{name: "invalid raw base64", encoded: "not-a-cephx-key", client: "client.amakura", wantErr: true},
		{name: "invalid keyring base64", encoded: "[client.amakura]\nkey = not-a-cephx-key\n", client: "client.amakura", wantErr: true},
		{name: "empty", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseCephXKey([]byte(test.encoded), test.client)
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("parseCephXKey() = %q, %v; want %q, error=%v", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestIndexCheckTreatsAnalyticsMismatchAsDirty(t *testing.T) {
	result := maintenance.CheckResult{AnalyticsMismatch: 1}
	if result.Clean() {
		t.Fatal("analytics consistency mismatch did not make index check dirty")
	}
}

func TestIndexCheckResourceFlags(t *testing.T) {
	command := newIndexCheckCommand(&global.Options{})
	for name, defaultValue := range map[string]string{
		"check-memory": "auto", "check-temp-max-bytes": "8G",
		"check-workers": "0", "check-rpc-concurrency": "0",
		"check-progress-interval": "30s",
	} {
		flag := command.Flags().Lookup(name)
		if flag == nil || flag.DefValue != defaultValue {
			t.Fatalf("--%s default = %v, want %q", name, flag, defaultValue)
		}
	}
}
