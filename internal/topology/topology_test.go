package topology

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sharedFixture(t *testing.T) []byte {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join("..", "..", "testdata", "topology-v2.json"))
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
	document.PackBackends[0].CredentialPolicy.Static.Bindings.StorageMaintain = "cred:missing"
	if err := document.Validate(); err == nil ||
		!strings.Contains(err.Error(), "dangling") && !strings.Contains(err.Error(), "unused") {
		t.Fatalf("dangling credential error = %v", err)
	}
	document.PackBackends[0].CredentialPolicy.Static.Bindings.StorageMaintain = "cred:archive"
	document.PlacementPolicy.MinDomains = 3
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "placement policy") {
		t.Fatalf("placement policy error = %v", err)
	}
}

func TestStaticCredentialBindingsAreExplicitAndFailClosed(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	backend := &document.PackBackends[0]
	legacyReference := backend.CredentialPolicy.Static.Bindings.StorageMaintain
	backend.CredentialPolicy.Static.Bindings = CredentialBindings{
		StorageRead:     "cred:archive-read",
		StorageAppend:   "cred:archive-append",
		StorageMaintain: legacyReference,
		StorageLock:     "cred:archive-lock",
	}
	for _, reference := range []string{"cred:archive-read", "cred:archive-append", "cred:archive-lock"} {
		document.Credentials[reference] = document.Credentials[legacyReference]
	}
	if err := document.Validate(); err != nil {
		t.Fatal(err)
	}
	for tier, expected := range map[StorageCredentialTier]string{
		StorageRead: "cred:archive-read", StorageAppend: "cred:archive-append",
		StorageMaintain: legacyReference, StorageLock: "cred:archive-lock",
	} {
		actual, selectErr := SelectStaticCredentialReference(backend.CredentialPolicy, tier)
		if selectErr != nil || actual != expected {
			t.Fatalf("binding %q = %q, %v; want %q", tier, actual, selectErr, expected)
		}
	}
	backend.CredentialPolicy.Static.Bindings.StorageAppend = ""
	if _, err := SelectStaticCredentialReference(backend.CredentialPolicy, StorageAppend); err == nil {
		t.Fatal("missing append binding selected a stronger credential")
	}
}

func TestSTSPolicyRequiresRoleAndStaticGeneration(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	document.Credentials["cred:issuer"] = document.Credentials["cred:archive"]
	policy := document.PackBackends[0].CredentialPolicy
	policy.STS = &S3STSPolicy{
		IssuerRef: "cred:issuer", Endpoint: "https://sts.example.com", Region: "us-east-1",
		SessionName: "vaultic", Roles: CredentialBindings{StorageRead: "not-an-arn"},
		Fallback: STSStaticOnUnavailable,
	}
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "role ARN") {
		t.Fatalf("invalid STS role error = %v", err)
	}
	policy.STS.Roles.StorageRead = "arn:aws:iam::123456789012:role/vaultic-read"
	policy.Static.Bindings.StorageRead = "cred:archive"
	policy.Static.Generation = 0
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "non-zero generation") {
		t.Fatalf("static generation error = %v", err)
	}
}

func TestAzureUserDelegationPolicyIsExactAndFailClosed(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	backend := &document.PackBackends[0]
	backend.Provider = ProviderAzure
	backend.Endpoint = map[string]any{
		"url": "https://account-a.blob.core.windows.net", "account": "account-a",
		"container": "repo-a", "prefix": "",
	}
	document.Credentials["cred:archive"] = Credential{
		Kind: CredentialAzureSAS, AccountName: "account-a", SASToken: "sp=rl&sig=static",
	}
	document.Credentials["cred:azure-issuer"] = Credential{
		Kind: CredentialAzureEntraClientSecret, TenantID: "tenant-a", ClientID: "client-a",
		ClientSecret: "issuer-secret", TokenURI: "https://login.microsoftonline.com/tenant-a/oauth2/v2.0/token",
		Scopes: []string{"https://storage.azure.com/.default"},
	}
	backend.CredentialPolicy = &CredentialPolicy{
		AzureUserDelegation: &AzureUserDelegationPolicy{
			IssuerRef: "cred:azure-issuer", ServiceVersion: "2026-04-06",
			Tiers: []StorageCredentialTier{StorageRead, StorageMaintain}, Fallback: STSStaticOnUnavailable,
		},
		Static: &StaticCredentialPolicy{Generation: 1, Bindings: CredentialBindings{
			StorageRead: "cred:archive", StorageMaintain: "cred:archive",
		}},
	}
	if err := document.Validate(); err != nil {
		t.Fatal(err)
	}
	backend.CredentialPolicy.AzureUserDelegation.Tiers = append(
		backend.CredentialPolicy.AzureUserDelegation.Tiers, StorageLock,
	)
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "hierarchical namespace") {
		t.Fatalf("lock scope error = %v", err)
	}
	backend.CredentialPolicy.AzureUserDelegation.Tiers = []StorageCredentialTier{StorageRead, StorageMaintain}
	document.Credentials["cred:consumer-issuer"] = document.Credentials["cred:azure-issuer"]
	backend.CredentialPolicy.Static.Bindings.StorageRead = "cred:consumer-issuer"
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "cannot authenticate") {
		t.Fatalf("consumer Entra credential error = %v", err)
	}
	backend.CredentialPolicy.Static.Bindings.StorageRead = "cred:archive"
	delete(document.Credentials, "cred:consumer-issuer")
	backend.Endpoint["prefix"] = "shared-prefix"
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "dedicated container") {
		t.Fatalf("prefix isolation error = %v", err)
	}
	backend.Endpoint["prefix"] = ""
	backend.CredentialPolicy.Static.Bindings.StorageRead = ""
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "exact static binding") {
		t.Fatalf("fallback coverage error = %v", err)
	}
	backend.CredentialPolicy.Static.Bindings.StorageRead = "cred:archive"
	document.Credentials["cred:azure-issuer"] = document.Credentials["cred:archive"]
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "Entra client secret") {
		t.Fatalf("issuer kind error = %v", err)
	}
}

func TestGCPDownscopePolicyIsExactAndFailClosed(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	backend := &document.PackBackends[0]
	backend.Provider = ProviderGCS
	backend.Endpoint = map[string]any{"bucket": "repo-bucket", "prefix": "repo"}
	document.Credentials["cred:archive"] = Credential{
		Kind: CredentialGCPServiceAccountJSON, ServiceAccountJSON: `{"type":"service_account","client_email":"fallback@example.test"}`,
	}
	document.Credentials["cred:gcp-issuer"] = Credential{
		Kind: CredentialGCPServiceAccountJSON, ServiceAccountJSON: `{"type":"service_account","client_email":"issuer@example.test"}`,
	}
	backend.CredentialPolicy = &CredentialPolicy{
		GCPDownscope: &GCPDownscopePolicy{
			IssuerRef: "cred:gcp-issuer", ServiceAccount: "target@example.iam.gserviceaccount.com",
			IAMCredentialsEndpoint: "https://iamcredentials.googleapis.com",
			TokenExchangeEndpoint:  "https://sts.googleapis.com/v1/token",
			Tiers:                  []StorageCredentialTier{StorageRead, StorageMaintain}, Fallback: STSStaticOnUnavailable,
		},
		Static: &StaticCredentialPolicy{Generation: 1, Bindings: CredentialBindings{
			StorageRead: "cred:archive", StorageMaintain: "cred:archive",
		}},
	}
	if err := document.Validate(); err != nil {
		t.Fatal(err)
	}
	backend.CredentialPolicy.GCPDownscope.TokenExchangeEndpoint = "https://attacker.example/token"
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "token exchange endpoint is invalid") {
		t.Fatalf("token endpoint pinning error = %v", err)
	}
	backend.CredentialPolicy.GCPDownscope.TokenExchangeEndpoint = "https://sts.googleapis.com/v1/token"
	backend.CredentialPolicy.GCPDownscope.Tiers = []StorageCredentialTier{StorageMaintain, StorageRead}
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "canonically ordered") {
		t.Fatalf("tier order error = %v", err)
	}
	backend.CredentialPolicy.GCPDownscope.Tiers = []StorageCredentialTier{StorageRead, StorageMaintain}
	backend.CredentialPolicy.Static.Bindings.StorageRead = "cred:gcp-issuer"
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "issuer credential") {
		t.Fatalf("issuer consumer binding error = %v", err)
	}
	backend.CredentialPolicy.Static.Bindings.StorageRead = "cred:archive"
	backend.CredentialPolicy.Static.Bindings.StorageMaintain = ""
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "exact static binding") {
		t.Fatalf("fallback coverage error = %v", err)
	}
	backend.CredentialPolicy.Static.Bindings.StorageMaintain = "cred:archive"
	backend.Provider = ProviderS3
	backend.Endpoint = map[string]any{"url": "https://s3.example.test", "bucket": "repo", "region": "us-east-1"}
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "requires a GCS endpoint") {
		t.Fatalf("provider mismatch error = %v", err)
	}
}

func TestProviderSpecificSTSPolicyAndRevocationValidation(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	document.Credentials["cred:issuer"] = document.Credentials["cred:archive"]
	backend := &document.PackBackends[0]
	backend.CredentialPolicy.STS = &S3STSPolicy{
		IssuerRef: "cred:issuer", Endpoint: "https://sts.wasabisys.com", Region: "us-east-1",
		SessionName: "vaultic", Roles: CredentialBindings{StorageRead: "arn:aws:iam::123456789012:role/read"},
		Fallback: STSStaticOnUnavailable,
	}
	backend.Endpoint["url"] = "https://s3.us-west-004.backblazeb2.com"
	backend.Endpoint["region"] = "us-west-004"
	backend.Endpoint["provider"] = "backblaze"
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "does not support STS") {
		t.Fatalf("Backblaze STS error = %v", err)
	}

	backend.Endpoint["url"] = "https://s3.eu-central-2.wasabisys.com"
	backend.Endpoint["region"] = "eu-central-2"
	backend.Endpoint["provider"] = "wasabi"
	backend.CredentialPolicy.STS.Region = "eu-central-2"
	backend.CredentialPolicy.STS.Endpoint = "https://sts.example.com"
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "sts.wasabisys.com") {
		t.Fatalf("Wasabi STS endpoint error = %v", err)
	}
	backend.CredentialPolicy.STS.Endpoint = "https://sts.wasabisys.com"
	backend.CredentialPolicy.Static.Bindings.StorageRead = "cred:archive"
	backend.CredentialPolicy.Static.Generation = 3
	backend.CredentialPolicy.Static.RevokedGenerations = []uint64{2, 1}
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "unique, and ordered") {
		t.Fatalf("revoked generation ordering error = %v", err)
	}
	backend.CredentialPolicy.Static.RevokedGenerations = []uint64{1, 2}
	if err := document.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestStaticFallbackRequiresExactBindingAndSeparateIssuer(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	document.Credentials["cred:issuer"] = document.Credentials["cred:archive"]
	policy := document.PackBackends[0].CredentialPolicy
	policy.STS = &S3STSPolicy{
		IssuerRef: "cred:issuer", Endpoint: "https://sts.example.com", Region: "us-east-1",
		SessionName: "vaultic", Roles: CredentialBindings{StorageRead: "arn:aws:iam::123456789012:role/read"},
		Fallback: STSStaticOnUnavailable,
	}
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "exact static binding") {
		t.Fatalf("missing exact fallback error = %v", err)
	}
	policy.Static.Bindings.StorageRead = "cred:issuer"
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "issuer credential") {
		t.Fatalf("issuer binding error = %v", err)
	}
	policy.Static.Bindings.StorageRead = "cred:archive"
	if err := document.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCredentialBindingsCanonicalJSON(t *testing.T) {
	encoded, err := json.Marshal(CredentialBindings{
		StorageRead: "cred:read", StorageAppend: "cred:append",
		StorageMaintain: "cred:maintain", StorageLock: "cred:lock",
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := `{"storage-read":"cred:read","storage-append":"cred:append","storage-maintain":"cred:maintain","storage-lock":"cred:lock"}`
	if string(encoded) != expected {
		t.Fatalf("credential bindings JSON = %s, want %s", encoded, expected)
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

func TestRADOSEndpointAndCredentialValidation(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	backend := &document.PackBackends[0]
	backend.Provider = ProviderRADOS
	backend.Endpoint = map[string]any{
		"monitors":     "ceph-mon-a.example:3300,[2001:db8::1]:3300",
		"cluster_fsid": "2f525d6a-8f31-4f79-b731-82a6acb235f5",
		"pool":         "vaultic-data",
		"namespace":    "repo-2",
		"prefix":       "packs/",
	}
	document.Credentials["cred:archive"] = Credential{
		Kind: CredentialCephXStatic, ClientID: "client.vaultic-repo-2", ClientSecret: "AQB-secret",
	}
	if err := document.Validate(); err != nil {
		t.Fatal(err)
	}
	for field, value := range map[string]string{
		"monitors":     "https://ceph-mon-a.example:3300",
		"cluster_fsid": "ceph",
		"pool":         "../other", "namespace": "other/repo", "prefix": "/packs",
	} {
		original := backend.Endpoint[field]
		backend.Endpoint[field] = value
		if err := document.Validate(); err == nil {
			t.Fatalf("invalid RADOS %s accepted", field)
		}
		backend.Endpoint[field] = original
	}
	document.Credentials["cred:archive"] = Credential{
		Kind: CredentialCephXStatic, ClientID: "vaultic-repo-2", ClientSecret: "AQB-secret",
	}
	if err := document.Validate(); err == nil {
		t.Fatal("CephX client without client. prefix accepted")
	}
}

func TestS3ProviderEndpointValidation(t *testing.T) {
	document, err := Decode(sharedFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := document.PackBackends[0].Endpoint
	endpoint["url"] = "https://s3.us-west-004.backblazeb2.com"
	endpoint["region"] = "us-west-004"
	endpoint["provider"] = "backblaze"
	endpoint["bucket_lookup"] = "dns"
	if err := document.Validate(); err != nil {
		t.Fatal(err)
	}
	canonical, err := document.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.PackBackends[0].Endpoint["provider"] != "backblaze" || decoded.PackBackends[0].Endpoint["bucket_lookup"] != "dns" {
		t.Fatalf("canonical provider endpoint = %#v", decoded.PackBackends[0].Endpoint)
	}

	endpoint["region"] = "eu-central-2"
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "does not match configured region") {
		t.Fatalf("region mismatch error = %v", err)
	}
	endpoint["region"] = "us-west-004"
	endpoint["provider"] = "wasabi"
	if err := document.Validate(); err == nil || !strings.Contains(err.Error(), "does not match endpoint") {
		t.Fatalf("provider mismatch error = %v", err)
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
	if len(decoded.Credentials) != 0 || decoded.PackBackends[0].CredentialPolicy == nil {
		t.Fatal("redacted topology did not preserve only credential references")
	}
	if _, err := Decode(encoded); err == nil {
		t.Fatal("secret-bearing decoder accepted a redacted topology")
	}
}
