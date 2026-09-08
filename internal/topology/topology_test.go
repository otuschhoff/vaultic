package topology

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sharedFixture(t *testing.T) []byte {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join("..", "..", "testdata", "topology-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestSharedFixtureIsCanonicalAndValid(t *testing.T) {
	encoded := sharedFixture(t)
	document, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := document.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != string(encoded) {
		t.Fatalf("canonical topology differs from shared fixture")
	}
	digest, err := document.SHA256()
	if err != nil || len(digest) != 64 {
		t.Fatalf("digest = %q, %v", digest, err)
	}
}

func TestReferencesAndPolicyFailClosed(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	document.Credentials["cred:unused"] = document.Credentials["cred:archive"]
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "unused") {
		t.Fatalf("unused credential error = %v", err)
	}
	delete(document.Credentials, "cred:unused")
	document.PackBackends[0].CredentialRef = "cred:missing"
	if err := document.Validate(); err == nil || !(strings.Contains(err.Error(), "dangling") || strings.Contains(err.Error(), "unused")) {
		t.Fatalf("dangling credential error = %v", err)
	}
	document.PackBackends[0].CredentialRef = "cred:archive"
	document.PlacementPolicy.MinDomains = 3
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "placement policy") {
		t.Fatalf("placement policy error = %v", err)
	}
}

func TestRedactedJSONContainsNoCredentialValues(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	redacted, err := document.RedactedJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"archive-secret", "client-secret", "refresh-secret", "metadata-secret"} {
		if strings.Contains(string(redacted), secret) {
			t.Fatalf("redacted topology contains %q", secret)
		}
	}
}

func TestCredentialKindMustMatchProvider(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	credential := document.Credentials["cred:drive"]
	credential.Kind = CredentialS3Static
	credential.AccessKeyID = "access"
	credential.SecretAccessKey = "secret"
	document.Credentials["cred:drive"] = credential
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "cannot authenticate provider") {
		t.Fatalf("provider mismatch error = %v", err)
	}
}

func TestEndpointSchemaRejectsEmbeddedCredential(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	document.PackBackends[0].Endpoint["refresh_token"] = "must-not-appear-here"
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported field") {
		t.Fatalf("endpoint schema error = %v", err)
	}
}

func TestDecodeRedactedRetainsReferencesWithoutCredentials(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := document.RedactedJSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRedacted(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Credentials) != 0 || decoded.PackBackends[0].CredentialRef == "" {
		t.Fatal("redacted topology did not preserve only credential references")
	}
	if _, err := Decode(encoded); err == nil {
		t.Fatal("secret-bearing decoder accepted a redacted topology")
	}
}
