//go:build rados

package rados_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend/rados"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/options"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestLiveEncryptedRepositoryLifecycle(t *testing.T) {
	monitors, key := os.Getenv("VAULTIC_RADOS_TEST_MONITORS"), os.Getenv("VAULTIC_RADOS_TEST_KEY")
	if monitors == "" || key == "" {
		t.Skip("set VAULTIC_RADOS_TEST_MONITORS and VAULTIC_RADOS_TEST_KEY")
	}
	config := rados.Config{
		Monitors: monitors, ClusterFSID: "2f525d6a-8f31-4f79-b731-82a6acb235f5",
		Pool: "vaultic", Namespace: "repo", Prefix: fmt.Sprintf("repository-%d/", time.Now().UnixNano()),
		Client: "client.vaultic", Key: options.NewSecretString(key), OperationTTL: 5 * time.Second,
	}
	store, err := rados.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	repo, _ := repository.TestRepositoryWithBackend(t, store, 0, repository.Options{})
	plaintext := bytes.Repeat([]byte("encrypted repository payload"), 1024)
	var blobID vaultic.ID
	err = repo.WithBlobUploader(t.Context(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var saveErr error
		blobID, _, _, saveErr = uploader.SaveBlob(ctx, vaultic.DataBlob, plaintext, vaultic.ID{}, false)
		return saveErr
	})
	if err != nil {
		t.Fatal(err)
	}
	data.TestCreateSnapshot(t, repo, time.Unix(1_700_000_000, 0), 2)
	loaded, err := repo.LoadBlob(t.Context(), vaultic.BlobHandle{Type: vaultic.DataBlob, ID: blobID}, nil)
	if err != nil || !bytes.Equal(loaded, plaintext) {
		t.Fatalf("restored blob = %d bytes, %v", len(loaded), err)
	}
	repository.TestCheckRepo(t, repo)
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := rados.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	reopened := repository.TestOpenBackend(t, reopenedStore)
	defer reopened.Close()
	if err := reopened.LoadIndex(t.Context(), vaultic.NoopTerminalCounterFactory); err != nil {
		t.Fatal(err)
	}
	loaded, err = reopened.LoadBlob(t.Context(), vaultic.BlobHandle{Type: vaultic.DataBlob, ID: blobID}, nil)
	if err != nil || !bytes.Equal(loaded, plaintext) {
		t.Fatalf("reopened restored blob = %d bytes, %v", len(loaded), err)
	}
	repository.TestCheckRepo(t, reopened)
}
