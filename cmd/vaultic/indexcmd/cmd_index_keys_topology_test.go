package indexcmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/otuschhoff/vaultic/internal/topology"
)

func TestReadAtomicBackendCredentialMutation(t *testing.T) {
	directory := t.TempDir()
	backendPath := filepath.Join(directory, "backend.json")
	credentialPath := filepath.Join(directory, "credential.json")
	writeProtectedTestFile(t, backendPath, `{"id":"drive","provider":"google-drive",`+
		`"endpoint":{"drive_id":"drive-id","root_folder_id":"root","path":"Backup"},`+
		`"role":"primary","offsite":true,"failure_domain":"google-drive",`+
		`"credential_policy":{"static":{"generation":1,"bindings":{"storage-maintain":"cred:drive"}}}}`)
	writeProtectedTestFile(t, credentialPath, `{"cred:drive":{"kind":"oauth2-refresh-token","client_id":"client",`+
		`"client_secret":"secret","refresh_token":"refresh","scopes":["https://www.googleapis.com/auth/drive"],`+
		`"token_uri":"https://oauth2.googleapis.com/token"}}`)

	mutation, err := readTopologyMutation("set-backend-policy", []string{backendPath, credentialPath})
	if err != nil {
		t.Fatal(err)
	}
	if mutation.Operation != "set-backend-policy" ||
		mutation.Backend == nil || mutation.Backend.Provider != topology.ProviderGoogleDrive ||
		mutation.Credentials["cred:drive"].RefreshToken != "refresh" {
		t.Fatalf("unexpected mutation: %#v", mutation)
	}
}

func TestReadAtomicBackendBindingsMutation(t *testing.T) {
	directory := t.TempDir()
	backendPath := filepath.Join(directory, "backend.json")
	credentialsPath := filepath.Join(directory, "credentials.json")
	writeProtectedTestFile(t, backendPath, `{"id":"archive","provider":"s3",`+
		`"endpoint":{"url":"https://s3.us-west-004.backblazeb2.com","bucket":"bucket","region":"us-west-004","provider":"backblaze"},`+
		`"role":"archival","offsite":true,"failure_domain":"backblaze",`+
		`"credential_policy":{"static":{"generation":1,"bindings":{"storage-read":"cred:b2-read",`+
		`"storage-append":"cred:b2-append","storage-maintain":"cred:b2-maintain","storage-lock":"cred:b2-lock"}}}}`)
	writeProtectedTestFile(t, credentialsPath, `{"cred:b2-read":{"kind":"s3-static","access_key_id":"read-id","secret_access_key":"read-secret"},`+
		`"cred:b2-append":{"kind":"s3-static","access_key_id":"append-id","secret_access_key":"append-secret"},`+
		`"cred:b2-maintain":{"kind":"s3-static","access_key_id":"maintain-id","secret_access_key":"maintain-secret"},`+
		`"cred:b2-lock":{"kind":"s3-static","access_key_id":"lock-id","secret_access_key":"lock-secret"}}`)

	mutation, err := readTopologyMutation("set-backend-policy", []string{backendPath, credentialsPath})
	if err != nil {
		t.Fatal(err)
	}
	if mutation.Operation != "set-backend-policy" || mutation.Backend == nil ||
		mutation.Backend.CredentialPolicy == nil || len(mutation.Credentials) != 4 {
		t.Fatalf("unexpected mutation: %#v", mutation)
	}
}

func TestReadAtomicReplicaBindingsMutation(t *testing.T) {
	directory := t.TempDir()
	replicaPath := filepath.Join(directory, "replica.json")
	credentialsPath := filepath.Join(directory, "credentials.json")
	writeProtectedTestFile(t, replicaPath, `{"provider":"s3",`+
		`"endpoint":{"url":"https://s3.us-west-004.backblazeb2.com","bucket":"bucket","region":"us-west-004","provider":"backblaze"},`+
		`"credential_policy":{"static":{"generation":1,"bindings":{"storage-read":"cred:metadata-read",`+
		`"storage-maintain":"cred:metadata-maintain"}}},"read_only":false}`)
	writeProtectedTestFile(t, credentialsPath, `{"cred:metadata-read":{"kind":"s3-static","access_key_id":"read-id","secret_access_key":"read-secret"},`+
		`"cred:metadata-maintain":{"kind":"s3-static","access_key_id":"maintain-id","secret_access_key":"maintain-secret"}}`)

	mutation, err := readTopologyMutation("set-replica-policy", []string{"remote", replicaPath, credentialsPath})
	if err != nil {
		t.Fatal(err)
	}
	if mutation.Operation != "set-replica-policy" || mutation.ID != "remote" || mutation.Replica == nil ||
		mutation.Replica.CredentialPolicy == nil || len(mutation.Credentials) != 2 {
		t.Fatalf("unexpected mutation: %#v", mutation)
	}
}

func writeProtectedTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}
