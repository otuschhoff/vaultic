package global

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/topology"
)

func TestStorageCredentialLifetime(t *testing.T) {
	tests := []struct {
		name       string
		options    Options
		wantTTL    time.Duration
		wantMargin time.Duration
		wantGrace  time.Duration
		wantError  bool
	}{
		{name: "defaults", wantTTL: time.Hour, wantMargin: 20 * time.Minute, wantGrace: time.Hour},
		{name: "computed margin", options: Options{StorageTokenTTL: 45 * time.Minute}, wantTTL: 45 * time.Minute, wantMargin: 20 * time.Minute, wantGrace: 45 * time.Minute},
		{name: "explicit", options: Options{StorageTokenTTL: 30 * time.Minute, StorageTokenRenewMargin: 10 * time.Minute, BrokerOutageGrace: 25 * time.Minute}, wantTTL: 30 * time.Minute, wantMargin: 10 * time.Minute, wantGrace: 25 * time.Minute},
		{name: "margin exceeds ttl", options: Options{StorageTokenTTL: 15 * time.Minute}, wantError: true},
		{name: "ttl exceeds broker maximum", options: Options{StorageTokenTTL: 2 * time.Hour}, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ttl, margin, grace, err := storageCredentialLifetime(test.options)
			if (err != nil) != test.wantError {
				t.Fatalf("storageCredentialLifetime() error = %v, want error %v", err, test.wantError)
			}
			if err == nil && (ttl != test.wantTTL || margin != test.wantMargin || grace != test.wantGrace) {
				t.Fatalf("storageCredentialLifetime() = (%v, %v, %v), want (%v, %v, %v)", ttl, margin, grace, test.wantTTL, test.wantMargin, test.wantGrace)
			}
		})
	}
}

func TestRenewableBackendFailsClosedAtExpiry(t *testing.T) {
	ctx := context.Background()
	storage := mem.New()
	renewable := newRenewableBackend(storage, time.Now().Add(-time.Second), time.Hour)
	handle := backend.Handle{Type: backend.PackFile, Name: "expired"}
	if err := renewable.Save(ctx, handle, backend.NewByteReader([]byte("data"), renewable.Hasher())); !errors.Is(err, errStorageCredentialExpired) {
		t.Fatalf("Save() error = %v, want credential expiry", err)
	}
	if _, err := renewable.Stat(ctx, handle); !errors.Is(err, errStorageCredentialExpired) {
		t.Fatalf("Stat() error = %v, want credential expiry", err)
	}
}

func TestRenewableBackendStopsWritesBeforeReads(t *testing.T) {
	ctx := context.Background()
	storage := mem.New()
	handle := backend.Handle{Type: backend.PackFile, Name: "existing"}
	if err := storage.Save(ctx, handle, backend.NewByteReader([]byte("data"), storage.Hasher())); err != nil {
		t.Fatal(err)
	}
	renewable := newRenewableBackend(storage, time.Now().Add(4*time.Minute), time.Hour)
	if err := renewable.Save(ctx, backend.Handle{Type: backend.PackFile, Name: "new"}, backend.NewByteReader([]byte("data"), renewable.Hasher())); !errors.Is(err, errStorageCredentialExpired) {
		t.Fatalf("Save() error = %v, want credential safety-margin expiry", err)
	}
	if _, err := renewable.Stat(ctx, handle); err != nil {
		t.Fatalf("Stat() error = %v, want read to remain admitted", err)
	}
}

func TestRenewableBackendSwapDrainsInflightOperation(t *testing.T) {
	ctx := context.Background()
	oldStorage := mem.New()
	newStorage := mem.New()
	handle := backend.Handle{Type: backend.PackFile, Name: "pack"}
	if err := oldStorage.Save(ctx, handle, backend.NewByteReader([]byte("old"), oldStorage.Hasher())); err != nil {
		t.Fatal(err)
	}
	renewable := newRenewableBackend(oldStorage, time.Now().Add(time.Hour), time.Hour)
	entered := make(chan struct{})
	release := make(chan struct{})
	loadDone := make(chan error, 1)
	go func() {
		loadDone <- renewable.Load(ctx, handle, 0, 0, func(io.Reader) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	var swapped atomic.Bool
	swapDone := make(chan error, 1)
	go func() {
		_, err := renewable.swap(newStorage, time.Now().Add(time.Hour), time.Hour)
		swapped.Store(true)
		swapDone <- err
	}()
	time.Sleep(10 * time.Millisecond)
	if swapped.Load() {
		t.Fatal("credential swap completed before the in-flight operation drained")
	}
	close(release)
	if err := <-loadDone; err != nil {
		t.Fatal(err)
	}
	if err := <-swapDone; err != nil {
		t.Fatal(err)
	}
}

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
