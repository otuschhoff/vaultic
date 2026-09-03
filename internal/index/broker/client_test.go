package broker

import (
	"crypto/sha256"
	"testing"
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
