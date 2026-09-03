package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/ui/termstatus"
)

func TestRepositoryOpensWithEncryptedMasterKeyInDB(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	environment, cleanup := withTestEnvironment(t)
	defer cleanup()
	environment.gopts.BackendTestHook = nil
	testRunInit(t, environment.gopts)
	term, cancelTerm := termstatus.Setup(os.Stdin, io.Discard, io.Discard, true)
	defer cancelTerm()
	environment.gopts.Term = term

	ctx := context.Background()
	printer := progress.NewTerminalPrinter(true, 0, environment.gopts.Term)
	ctx, repo, unlock, err := openWithReadLock(ctx, environment.gopts, true, printer)
	if err != nil {
		t.Fatal(err)
	}
	repositoryID := repo.Config().ID
	if err := metadataindex.Activate(ctx, repo.Backend(), repositoryID); err != nil {
		unlock()
		t.Fatal(err)
	}
	rawMasterKey, err := json.Marshal(repo.Key())
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	masterKey := []byte(base64.StdEncoding.EncodeToString(rawMasterKey))
	clear(rawMasterKey)
	defer clear(masterKey)

	passphraseFile := filepath.Join(t.TempDir(), "metadata-passphrase")
	if err := os.WriteFile(passphraseFile, []byte("metadata recovery passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	daemonOptions := daemon.Options{
		Socket: testMetadataSocket(t), RepositoryID: repositoryID,
		DaemonPath: testVaulticDBBinary(t), DataDir: t.TempDir(),
		EncryptionMode: "initialize", PassphraseFile: passphraseFile,
	}
	client, err := daemon.Ensure(ctx, daemonOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	if err := client.StoreMasterKey(ctx, masterKey); err != nil {
		t.Fatal(err)
	}
	secondPassphrase := filepath.Join(t.TempDir(), "second-metadata-passphrase")
	if err := os.WriteFile(secondPassphrase, []byte("second recovery passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	keyOptions := indexKeysOptions{
		RepositoryID: repositoryID,
		Daemon: indexDaemonOptions{
			Socket: daemonOptions.Socket, DataDir: daemonOptions.DataDir,
			EncryptionMode: "required", PassphraseFile: passphraseFile,
		},
	}
	addSlot := newIndexKeysAddSlotCommand(&environment.gopts, &keyOptions)
	addSlot.SetArgs([]string{"--slot", "second-recovery", "--passphrase-file", secondPassphrase, "--priority", "10", "--recovery"})
	if err := addSlot.ExecuteContext(ctx); err != nil {
		t.Fatal(err)
	}
	ctx, repo, unlock, err = openWithReadLock(metadataRepositoryContext(t, ctx, keyOptions.Daemon, repositoryID), environment.gopts, true, printer)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Backend().Stat(ctx, backend.Handle{Type: backend.SlateDBFile, Name: "key-envelope-00000000000000000002.json", IsMetadata: true})
	unlock()
	if err != nil {
		t.Fatalf("automatic envelope mirror missing: %v", err)
	}

	keyInDBOptions := environment.gopts
	keyInDBOptions.Password = ""
	keyInDBOptions.PasswordFile = ""
	keyInDBOptions.PasswordCommand = ""
	keyInDBOptions.MetadataKeyInDB = true
	keyInDBOptions.MetadataDaemonSocket = daemonOptions.Socket
	keyInDBOptions.MetadataDaemonDataDir = daemonOptions.DataDir
	keyInDBOptions.MetadataEncryptionMode = "required"
	keyInDBOptions.MetadataPassphraseFile = passphraseFile
	opened, err := global.OpenRepository(ctx, keyInDBOptions, printer)
	if err != nil {
		t.Fatal(err)
	}
	if opened.Config().ID != repositoryID {
		t.Fatalf("opened repository %q, want %q", opened.Config().ID, repositoryID)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(daemonOptions.DataDir); err != nil {
		t.Fatal(err)
	}
	recoveredKeyFile := filepath.Join(t.TempDir(), "recovered-master-key")
	if err := os.WriteFile(recoveredKeyFile, append(masterKey, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	recoveryOptions := environment.gopts
	recoveryOptions.Password = ""
	recoveryOptions.PasswordFile = ""
	recoveryOptions.PasswordCommand = ""
	recoveryOptions.MasterKeyFile = recoveredKeyFile
	recoveryOptions.MetadataLossRecovery = true
	recovered, err := global.OpenRepository(ctx, recoveryOptions, printer)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if recovered.Config().ID != repositoryID {
		t.Fatalf("recovered repository %q, want %q", recovered.Config().ID, repositoryID)
	}
}

func TestCapsuleMigrationRetiresDatabaseKeyAndManagedBypasses(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	environment, cleanup := withTestEnvironment(t)
	defer cleanup()
	environment.gopts.BackendTestHook = nil
	testRunInit(t, environment.gopts)
	term, cancelTerm := termstatus.Setup(os.Stdin, io.Discard, io.Discard, true)
	defer cancelTerm()
	environment.gopts.Term = term

	ctx := context.Background()
	printer := progress.NewTerminalPrinter(true, 0, environment.gopts.Term)
	ctx, repo, unlock, err := openWithReadLock(ctx, environment.gopts, true, printer)
	if err != nil {
		t.Fatal(err)
	}
	repositoryID := repo.Config().ID
	if err := metadataindex.Activate(ctx, repo.Backend(), repositoryID); err != nil {
		unlock()
		t.Fatal(err)
	}
	rawMasterKey, err := json.Marshal(repo.Key())
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	masterKey := []byte(base64.StdEncoding.EncodeToString(rawMasterKey))
	clear(rawMasterKey)
	backendStore := repo.Backend()
	unlock()
	defer clear(masterKey)

	passphraseFile := filepath.Join(t.TempDir(), "metadata-passphrase")
	if err := os.WriteFile(passphraseFile, []byte("metadata recovery passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	daemonOptions := daemon.Options{
		Socket: testMetadataSocket(t), RepositoryID: repositoryID,
		DaemonPath: testVaulticDBBinary(t), DataDir: t.TempDir(),
		EncryptionMode: "initialize", PassphraseFile: passphraseFile,
	}
	client, err := daemon.Ensure(ctx, daemonOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	if err := client.StoreMasterKey(ctx, masterKey); err != nil {
		t.Fatal(err)
	}
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	migration, err := client.PrepareCapsuleMigration(ctx, t.TempDir(), 1, "operators", 2, publicKey, []daemon.OfflineCapsuleMember{
		{ID: "alice", Provider: "offline-keyfile", Credential: bytes.Repeat([]byte{1}, 32)},
		{ID: "bob", Provider: "offline-keyfile", Credential: bytes.Repeat([]byte{2}, 32)},
		{ID: "carol", Provider: "offline-keyfile", Credential: bytes.Repeat([]byte{3}, 32)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := client.GetMasterKey(ctx); err != nil || !found {
		t.Fatalf("prepare did not retain database master key: found %t, err %v", found, err)
	}
	if err := backendStore.Save(ctx, backend.Handle{Type: backend.SlateDBFile, Name: "escrow-retire.json", IsMetadata: true}, backend.NewByteReader([]byte("wrapped escrow"), backendStore.Hasher())); err != nil {
		t.Fatal(err)
	}

	proof := hmac.New(sha256.New, masterKey)
	_, _ = proof.Write([]byte("vaultic-capsule-migration-finalize-v1\x00" + repositoryID + "\x00" + migration.CapsuleSHA256))
	if err := client.FinalizeCapsuleMigration(ctx, migration.CapsuleSHA256, proof.Sum(nil)); err != nil {
		t.Fatal(err)
	}
	if err := client.FinalizeCapsuleMigration(ctx, migration.CapsuleSHA256, proof.Sum(nil)); err != nil {
		t.Fatalf("same migration finalize retry failed: %v", err)
	}
	if err := client.FinalizeCapsuleMigration(ctx, strings.Repeat("f", 64), proof.Sum(nil)); err == nil {
		t.Fatal("different migration digest accepted after finalization")
	}
	if key, found, err := client.GetMasterKey(ctx); err != nil || found || len(key) != 0 {
		t.Fatalf("database master key remains after finalization: found %t, bytes %d, err %v", found, len(key), err)
	}
	if _, _, err := retireLegacyQuorumBypasses(ctx, backendStore); err != nil {
		t.Fatal(err)
	}
	for _, fileType := range []backend.FileType{backend.KeyFile} {
		if err := backendStore.List(ctx, fileType, func(info backend.FileInfo) error {
			return fmt.Errorf("managed bypass remains: %s", info.Name)
		}); err != nil && !backendStore.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	if _, err := backendStore.Stat(ctx, backend.Handle{Type: backend.SlateDBFile, Name: "escrow-retire.json", IsMetadata: true}); err == nil {
		t.Fatal("managed escrow remains after retirement")
	}
	status, err := client.KeyStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.PendingCapsuleMigrationSHA256 != "" || status.FinalizedCapsuleMigrationSHA256 != migration.CapsuleSHA256 {
		t.Fatalf("migration status = %+v", status)
	}
}

func metadataRepositoryContext(t *testing.T, ctx context.Context, options indexDaemonOptions, repositoryID string) context.Context {
	t.Helper()
	ctx, err := indexDaemonContext(ctx, options, repositoryID)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func testVaulticDBBinary(t *testing.T) string {
	t.Helper()
	if binary := os.Getenv("VAULTICDB_TEST_BINARY"); binary != "" {
		return binary
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test")
	}
	return filepath.Join(filepath.Dir(source), "..", "..", "vaulticdb", "target", "debug", "vaulticdb")
}

func testMetadataSocket(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "vdb-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return filepath.Join(directory, "d.sock")
}
