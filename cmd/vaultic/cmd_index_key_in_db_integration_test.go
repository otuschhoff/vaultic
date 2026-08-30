package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
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
