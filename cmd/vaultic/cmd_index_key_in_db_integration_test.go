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
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/global"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
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

func TestGoContributionsUnlockRustBroker(t *testing.T) {
	brokerBinary := filepath.Join(filepath.Dir(testVaulticDBBinary(t)), "vaultic-key-broker")
	if _, err := os.Stat(brokerBinary); err != nil {
		t.Skipf("compiled vaultic-key-broker unavailable: %v", err)
	}
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
	daemonClient, err := daemon.Ensure(ctx, daemon.Options{
		Socket: testMetadataSocket(t), RepositoryID: repositoryID,
		DaemonPath: testVaulticDBBinary(t), DataDir: t.TempDir(),
		EncryptionMode: "initialize", PassphraseFile: passphraseFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer daemonClient.Close(ctx)
	if err := daemonClient.StoreMasterKey(ctx, masterKey); err != nil {
		t.Fatal(err)
	}
	publicIdentity, privateIdentity, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	capsuleDirectory := t.TempDir()
	credentials := [][]byte{bytes.Repeat([]byte{1}, 32), []byte("bob's offline recovery passphrase")}
	migration, err := daemonClient.PrepareCapsuleMigration(ctx, capsuleDirectory, 1, "operators", 2, publicIdentity, []daemon.OfflineCapsuleMember{
		{ID: "alice", Provider: "offline-keyfile", Credential: credentials[0]},
		{ID: "bob", Provider: "offline-argon2id", Credential: credentials[1]},
	})
	if err != nil {
		t.Fatal(err)
	}
	capsule, err := indexbroker.LoadCapsule(migration.LocalPath)
	if err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	identityPath := filepath.Join(root, "identity.key")
	if err := os.WriteFile(identityPath, privateIdentity.Seed(), 0o600); err != nil {
		t.Fatal(err)
	}
	_, releasePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	socketDirectory, err := os.MkdirTemp("/tmp", "vkb-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
	socket := filepath.Join(socketDirectory, "broker.sock")
	configPath := filepath.Join(root, "broker.json")
	config := map[string]any{
		"format": 1, "capsule_directory": capsuleDirectory, "repository_id": repositoryID,
		"identity_key_path": identityPath, "socket_path": socket,
		"authorizations": []map[string]any{{
			"component": "unused", "minimum_version": 1, "maximum_version": 1,
			"release_identity": "test", "release_public_key": base64.StdEncoding.EncodeToString(releasePrivate[ed25519.SeedSize:]),
			"peer_uid": uint32(os.Geteuid()), "capabilities": []string{"metadata-dek"},
		}},
	}
	if err := writeNewProtectedJSON(configPath, config); err != nil {
		t.Fatal(err)
	}
	process := exec.CommandContext(ctx, brokerBinary, configPath)
	process.Env = []string{}
	var processErrors bytes.Buffer
	process.Stderr = &processErrors
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	processDone := make(chan error, 1)
	go func() { processDone <- process.Wait() }()
	processExited := false
	t.Cleanup(func() {
		if processExited {
			return
		}
		_ = process.Process.Kill()
		<-processDone
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		connection, dialErr := net.DialTimeout("unix", socket, 50*time.Millisecond)
		if dialErr == nil {
			_ = connection.Close()
			break
		}
		select {
		case processErr := <-processDone:
			processExited = true
			t.Fatalf("broker exited before creating socket: %v: %s", processErr, processErrors.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker did not create socket: %v: %s", dialErr, processErrors.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	brokerClient, err := indexbroker.Dial(ctx, socket)
	if err != nil {
		t.Fatal(err)
	}
	defer brokerClient.Close()
	session, err := brokerClient.CreateSession(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for index, member := range []string{"alice", "bob"} {
		contribution, err := capsule.ContributeOffline(session, "unix:"+socket, member, credentials[index], index == 0, capsule.Generation(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		unlocked, err := brokerClient.SubmitContribution(ctx, contribution)
		if err != nil {
			t.Fatal(err)
		}
		if unlocked != (index == 1) {
			t.Fatalf("contribution %d unlocked=%t", index, unlocked)
		}
	}
	status, err := brokerClient.Status(ctx)
	if err != nil || status.Locked {
		t.Fatalf("Rust broker status locked=%t, err=%v", status.Locked, err)
	}
	if err := brokerClient.Close(); err != nil {
		t.Fatal(err)
	}
	if err := process.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-processDone
	processExited = true

	restarted := exec.CommandContext(ctx, brokerBinary, configPath)
	restarted.Env = []string{}
	var restartErrors bytes.Buffer
	restarted.Stderr = &restartErrors
	if err := restarted.Start(); err != nil {
		t.Fatal(err)
	}
	restartDone := make(chan error, 1)
	go func() { restartDone <- restarted.Wait() }()
	restartExited := false
	t.Cleanup(func() {
		if restartExited {
			return
		}
		_ = restarted.Process.Kill()
		<-restartDone
	})
	deadline = time.Now().Add(5 * time.Second)
	var restartedClient *indexbroker.Client
	for {
		restartedClient, err = indexbroker.Dial(ctx, socket)
		if err == nil {
			break
		}
		select {
		case processErr := <-restartDone:
			restartExited = true
			t.Fatalf("restarted broker exited before accepting connections: %v: %s", processErr, restartErrors.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted broker did not accept connections: %v: %s", err, restartErrors.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer restartedClient.Close()
	status, err = restartedClient.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Locked || status.ActiveSessions != 0 || status.ActiveLeases != 0 {
		t.Fatalf("restarted broker retained epoch state: %+v", status)
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
