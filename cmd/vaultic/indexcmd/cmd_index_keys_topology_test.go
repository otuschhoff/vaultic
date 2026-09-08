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
		`"role":"primary","offsite":true,"failure_domain":"google-drive","credential_ref":"cred:drive"}`)
	writeProtectedTestFile(t, credentialPath, `{"kind":"oauth2-refresh-token","client_id":"client",`+
		`"client_secret":"secret","refresh_token":"refresh","scopes":["https://www.googleapis.com/auth/drive"],`+
		`"token_uri":"https://oauth2.googleapis.com/token"}`)

	mutation, err := readTopologyMutation("set-backend-credential", []string{backendPath, "cred:drive", credentialPath})
	if err != nil {
		t.Fatal(err)
	}
	if mutation.Operation != "set-backend-credential" || mutation.Reference != "cred:drive" ||
		mutation.Backend == nil || mutation.Backend.Provider != topology.ProviderGoogleDrive ||
		mutation.Credential == nil || mutation.Credential.RefreshToken != "refresh" {
		t.Fatalf("unexpected mutation: %#v", mutation)
	}
}

func writeProtectedTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}
