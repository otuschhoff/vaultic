package bootstrap

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProfileIsStrictCanonicalAndCredentialFree(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "bootstrap.toml")
	valid := `format = 1
repository_id = "repo-a"
anchor_file = "/protected/anchor.json"
[[seed]]
id = "a"
location = "s3:https://storage.example/repository"
[[seed]]
id = "b"
location = "azure:https://storage.example/repository"
`
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	profile, err := LoadProfile(path)
	if err != nil || len(profile.Seeds) != 2 {
		t.Fatalf("profile = %#v, %v", profile, err)
	}
	for _, invalid := range []string{
		strings.Replace(valid, "https://storage.example/repository", "https://user:secret@storage.example/repository", 1),
		valid + "unknown = true\n",
		strings.Replace(valid, "id = \"b\"", "id = \"a\"", 1),
	} {
		if err := os.WriteFile(path, []byte(invalid), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadProfile(path); err == nil {
			t.Fatalf("invalid profile was accepted: %s", invalid)
		}
	}
}

func TestAnchorAndOfflineExportRejectRollbackAndOverwrite(t *testing.T) {
	directory := t.TempDir()
	anchorPath := filepath.Join(directory, "state", "anchor.json")
	anchor := Anchor{RepositoryID: "repo-a", Generation: 2, SHA256: strings.Repeat("ab", 32)}
	if err := StoreAnchor(anchorPath, anchor); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadAnchor(anchorPath)
	if err != nil || loaded != anchor {
		t.Fatalf("anchor = %#v, %v", loaded, err)
	}
	if err := StoreAnchor(anchorPath, Anchor{RepositoryID: "repo-a", Generation: 1, SHA256: strings.Repeat("cd", 32)}); err == nil {
		t.Fatal("anchor rollback was accepted")
	}
	export := filepath.Join(directory, "offline")
	if err := ExportOffline(export, anchor, []byte("encrypted topology")); err != nil {
		t.Fatal(err)
	}
	if err := ExportOffline(export, anchor, []byte("different")); err == nil {
		t.Fatal("offline manifest was overwritten")
	}
	info, err := os.Stat(filepath.Join(export, "topology-00000000000000000002.enc"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("offline manifest mode = %v, %v", info.Mode(), err)
	}
}
