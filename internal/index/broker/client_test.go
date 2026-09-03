package broker

import (
	"bufio"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExternalShareBindingCrossLanguageFixture(t *testing.T) {
	value := capsule{Header: capsuleHeader{RepositoryID: "repo-a", Generation: 8, RootKeyVersion: 1, PolicyHash: "policy-hash"}}
	member := memberShare{
		MemberID:     "alice",
		GroupID:      "operators",
		ShareIndex:   1,
		Threshold:    2,
		ShareCount:   2,
		Provider:     "azure-key-vault",
		KeyReference: "https://example.vault.azure.net/keys/alice/version",
		Principal: &principalBinding{
			Authority:              "entra",
			TenantAccountOrProject: "tenant-a",
			ImmutablePrincipalID:   "object-alice",
		},
	}
	purpose, err := value.externalSharePurpose(member)
	if err != nil {
		t.Fatal(err)
	}
	const expected = "recovery-capsule-share:98436d46b6026a26669db00967c0c1c744f1095700a3c5b73abeddcbf8302306"
	if purpose != expected {
		t.Fatalf("external purpose mismatch: got %q, want %q", purpose, expected)
	}
	digest := sha256.Sum256([]byte(purpose))
	payload := append(append([]byte("VLTCAPSH1"), digest[:]...), []byte("share")...)
	share, err := decodeExternalShare(purpose, payload)
	if err != nil || string(share) != "share" {
		t.Fatalf("decode external share: %q, %v", share, err)
	}
	payload[len("VLTCAPSH1")] ^= 1
	if _, err := decodeExternalShare(purpose, payload); err == nil {
		t.Fatal("tampered external context digest was accepted")
	}
}

func TestPreparePolicyMutationUsesSignedChallengeAndBase64Credential(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executableData, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(executableData)
	manifestPath := filepath.Join(t.TempDir(), "release.json")
	manifest, err := json.Marshal(ReleaseManifest{Component: "vaultic", Version: 20, ReleaseIdentity: "release-a", ExecutableSHA256: hex.EncodeToString(digest[:]), Signature: "signature"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, manifest, 0o600); err != nil {
		t.Fatal(err)
	}

	clientConnection, serverConnection := net.Pipe()
	defer clientConnection.Close()
	defer serverConnection.Close()
	client := &Client{connection: clientConnection, reader: bufio.NewReader(clientConnection), protocol: protocolVersion, challenge: "challenge-a"}
	requestResult := make(chan map[string]any, 1)
	go func() {
		line, _ := bufio.NewReader(serverConnection).ReadBytes('\n')
		var request map[string]any
		_ = json.Unmarshal(line, &request)
		requestResult <- request
		_, _ = serverConnection.Write([]byte(`{"result":"policy_mutation_prepared","capsule":{"format":2},"capsule_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}` + "\n"))
	}()

	prepared, err := client.PreparePolicyMutation(
		t.Context(),
		manifestPath,
		UnlockPolicy{Type: "threshold", GroupID: "operators", Required: 2, Members: []string{"alice", "bob"}},
		[]OfflinePolicyMember{{MemberID: "alice", Provider: "offline-keyfile", Credential: "AQID"}},
		nil,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Capsule) == 0 || len(prepared.CapsuleSHA256) != 64 {
		t.Fatalf("invalid prepared mutation: %+v", prepared)
	}
	request := <-requestResult
	if request["operation"] != "prepare_policy_mutation" {
		t.Fatalf("unexpected operation: %#v", request["operation"])
	}
	authorization := request["authorization"].(map[string]any)
	if authorization["challenge_response"] == "" || authorization["release_identity"] != "release-a" {
		t.Fatalf("missing signed authorization: %#v", authorization)
	}
	members := request["members"].([]any)
	if members[0].(map[string]any)["credential"] != "AQID" {
		t.Fatalf("credential is not explicitly base64 framed: %#v", members[0])
	}
	if client.challenge != "" {
		t.Fatal("authorization challenge was not consumed")
	}
}

func TestPendingPolicyMutationUsesSignedChallenge(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executableData, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(executableData)
	manifestPath := filepath.Join(t.TempDir(), "release.json")
	manifest, err := json.Marshal(ReleaseManifest{Component: "vaultic", Version: 20, ReleaseIdentity: "release-a", ExecutableSHA256: hex.EncodeToString(digest[:]), Signature: "signature"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	clientConnection, serverConnection := net.Pipe()
	defer clientConnection.Close()
	defer serverConnection.Close()
	client := &Client{connection: clientConnection, reader: bufio.NewReader(clientConnection), protocol: protocolVersion, challenge: "challenge-a"}
	requestResult := make(chan map[string]any, 1)
	go func() {
		line, _ := bufio.NewReader(serverConnection).ReadBytes('\n')
		var request map[string]any
		_ = json.Unmarshal(line, &request)
		requestResult <- request
		_, _ = serverConnection.Write([]byte(`{"result":"policy_mutation_prepared","capsule":{"format":2},"capsule_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}` + "\n"))
	}()
	prepared, err := client.PendingPolicyMutation(t.Context(), manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Capsule) == 0 || len(prepared.CapsuleSHA256) != 64 {
		t.Fatalf("invalid pending mutation: %+v", prepared)
	}
	request := <-requestResult
	if request["operation"] != "pending_policy_mutation" {
		t.Fatalf("unexpected operation: %#v", request["operation"])
	}
	if client.challenge != "" {
		t.Fatal("authorization challenge was not consumed")
	}
}

func TestIdentityRecoverySessionBypassesOnlyLostIdentitySignature(t *testing.T) {
	oldPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, replacementPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	value := capsule{Header: capsuleHeader{Format: 2, RepositoryID: "repo-a", Generation: 4, BrokerIdentityPublicKey: base64.StdEncoding.EncodeToString(oldPublic)}}
	transcript := SessionTranscript{Protocol: protocolVersion, SessionID: "session-a", RepositoryID: "repo-a", CapsuleGeneration: 4, EndpointBinding: "unix:/broker.sock", Nonce: "nonce", ExpiresUnixMS: uint64(time.Now().Add(time.Minute).UnixMilli()), HPKEPublicKey: base64.StdEncoding.EncodeToString(make([]byte, 32)), IdentityRecovery: true}
	encoded, err := json.Marshal(transcript)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	parts := make([]string, 0, 8)
	for offset := 0; offset < 16; offset += 2 {
		parts = append(parts, strings.ToUpper(hex.EncodeToString(digest[offset:offset+2])))
	}
	session := SignedSession{Transcript: transcript, Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(replacementPrivate, encoded)), Fingerprint: strings.Join(parts, "-")}
	if err := value.verifySession(session, "unix:/broker.sock", time.Now(), true); err != nil {
		t.Fatalf("acknowledged identity recovery session rejected: %v", err)
	}
	if err := value.verifySession(session, "unix:/broker.sock", time.Now(), false); err == nil {
		t.Fatal("identity recovery session accepted without acknowledgement")
	}
	tampered := session
	tampered.Transcript.EndpointBinding = "unix:/other.sock"
	if err := value.verifySession(tampered, "unix:/broker.sock", time.Now(), true); err == nil {
		t.Fatal("tampered recovery transcript was accepted")
	}
	tampered = session
	tampered.Fingerprint = "0000-0000-0000-0000-0000-0000-0000-0000"
	if err := value.verifySession(tampered, "unix:/broker.sock", time.Now(), true); err == nil {
		t.Fatal("recovery session with wrong fingerprint was accepted")
	}
}
