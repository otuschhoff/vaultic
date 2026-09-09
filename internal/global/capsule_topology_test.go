package global

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/topology"
)

func TestCredentialRoutingBackendIsolatesLocks(t *testing.T) {
	ctx := context.Background()
	dataBackend := mem.New()
	lockBackend := mem.New()
	routed := &credentialRoutingBackend{data: dataBackend, lock: lockBackend}
	pack := backend.Handle{Type: backend.PackFile, Name: "pack"}
	lock := backend.Handle{Type: backend.LockFile, Name: "lock"}
	if err := routed.Save(ctx, pack, backend.NewByteReader([]byte("pack"), routed.Hasher())); err != nil {
		t.Fatal(err)
	}
	if err := routed.Save(ctx, lock, backend.NewByteReader([]byte("lock"), routed.Hasher())); err != nil {
		t.Fatal(err)
	}
	assertBackendNames(t, ctx, dataBackend, backend.PackFile, []string{"pack"})
	assertBackendNames(t, ctx, dataBackend, backend.LockFile, nil)
	assertBackendNames(t, ctx, lockBackend, backend.PackFile, nil)
	assertBackendNames(t, ctx, lockBackend, backend.LockFile, []string{"lock"})
	if err := routed.Remove(ctx, lock); err != nil {
		t.Fatal(err)
	}
	assertBackendNames(t, ctx, lockBackend, backend.LockFile, nil)
}

func assertBackendNames(
	t *testing.T,
	ctx context.Context,
	storage backend.Backend,
	fileType backend.FileType,
	expected []string,
) {
	t.Helper()
	var actual []string
	if err := storage.List(ctx, fileType, func(info backend.FileInfo) error {
		actual = append(actual, info.Name)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(actual) != len(expected) {
		t.Fatalf("names for %v = %v, want %v", fileType, actual, expected)
	}
	for index := range actual {
		if actual[index] != expected[index] {
			t.Fatalf("names for %v = %v, want %v", fileType, actual, expected)
		}
	}
}

func TestApplyTopologyOverridesRestrictsChangesToLocalDataDir(t *testing.T) {
	document := topology.Document{
		PackBackends: []topology.PackBackend{
			{ID: "local", Provider: topology.ProviderLocal, Endpoint: map[string]any{"data_dir": "/old"}},
			{ID: "remote", Provider: topology.ProviderS3, Endpoint: map[string]any{"url": "https://example.invalid", "bucket": "bucket", "region": "region"}},
		},
	}
	if err := applyTopologyOverrides(&document, []string{"local.data_dir=/new"}); err != nil {
		t.Fatal(err)
	}
	if document.PackBackends[0].Endpoint["data_dir"] != "/new" {
		t.Fatalf("local override was not applied: %#v", document.PackBackends[0].Endpoint)
	}
	for _, invalid := range []string{"remote.data_dir=/new", "local.url=https://other.invalid", "local.data_dir="} {
		if err := applyTopologyOverrides(&document, []string{invalid}); err == nil {
			t.Fatalf("override %q was accepted", invalid)
		}
	}
}

func TestS3BackendConfigValidatesProviderBeforeCredentials(t *testing.T) {
	declared := topology.PackBackend{
		ID: "remote", Provider: topology.ProviderS3,
		Endpoint: map[string]any{
			"url": "https://s3.us-west-004.backblazeb2.com", "bucket": "bucket",
			"region": "us-west-004", "provider": "wasabi",
		},
	}
	credential := topology.Credential{
		Kind: topology.CredentialS3Static, AccessKeyID: "must-not-leak", SecretAccessKey: "must-not-leak",
	}
	_, _, err := s3BackendConfig(declared, credential)
	if err == nil || !strings.Contains(err.Error(), "does not match endpoint") {
		t.Fatalf("s3BackendConfig() error = %v", err)
	}
	if strings.Contains(err.Error(), credential.AccessKeyID) || strings.Contains(err.Error(), credential.SecretAccessKey) {
		t.Fatalf("credential leaked in error: %v", err)
	}
}

func TestS3BackendConfigUsesOnlyCapsuleCredentialsAndRedactsDescription(t *testing.T) {
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_ASSUME_ROLE_ARN"} {
		original, existed := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if existed {
				_ = os.Setenv(name, original)
			} else {
				_ = os.Unsetenv(name)
			}
		}()
	}
	declared := topology.PackBackend{
		ID: "offsite", Provider: topology.ProviderS3,
		Endpoint: map[string]any{
			"url": "https://s3.us-west-004.backblazeb2.com", "bucket": "capsule-bucket",
			"prefix": "private/repository", "region": "us-west-004", "provider": "backblaze",
		},
	}
	credential := topology.Credential{
		Kind: topology.CredentialS3Static, AccessKeyID: "capsule-key", SecretAccessKey: "capsule-secret",
	}
	config, description, err := s3BackendConfig(declared, credential)
	if err != nil {
		t.Fatal(err)
	}
	if config.Provider != "backblaze" || config.KeyID != credential.AccessKeyID || config.Secret.Unwrap() != credential.SecretAccessKey {
		t.Fatalf("capsule S3 config = %#v", config)
	}
	for _, sensitive := range []string{credential.AccessKeyID, credential.SecretAccessKey, config.Prefix} {
		if strings.Contains(description, sensitive) {
			t.Fatalf("description leaked %q: %s", sensitive, description)
		}
	}
}
