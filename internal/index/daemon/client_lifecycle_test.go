package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEnsureRejectsInsecureTCPConfiguration(t *testing.T) {
	address := freeTCPAddress(t)
	base := Options{TCPAddress: address, RepositoryID: "tcp-repo", DaemonPath: daemonBinary(t), StartTimeout: time.Second}
	if _, err := Ensure(context.Background(), base); err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("expected missing allowlist error, got %v", err)
	}
	base.TCPAllowlist = []string{"127.0.0.1/32"}
	if _, err := Ensure(context.Background(), base); err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("expected missing token error, got %v", err)
	}
	base.TCPAllowlist = []string{"not-a-network"}
	base.AuthToken = "token"
	if _, err := Ensure(context.Background(), base); err == nil || !strings.Contains(err.Error(), "invalid TCP allowlist") {
		t.Fatalf("expected invalid allowlist error, got %v", err)
	}
	if _, err := Ensure(context.Background(), Options{ObjectStore: "s3", DaemonPath: daemonBinary(t)}); err == nil || !strings.Contains(err.Error(), "bucket") {
		t.Fatalf("expected missing S3 bucket error, got %v", err)
	}

}

func TestValidateEnsureOptions(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		want    string
	}{
		{name: "encryption mode", options: Options{EncryptionMode: "unknown"}, want: "unsupported metadata encryption mode"},
		{name: "rebuild", options: Options{RebuildInitialize: true}, want: "requires brokered required encryption"},
		{name: "passphrase", options: Options{EncryptionMode: "required"}, want: "passphrase file is required"},
		{name: "broker manifest", options: Options{BrokerSocket: "broker"}, want: "broker release manifest is required"},
		{name: "recovery unlock", options: Options{RecoveryUnlock: true}, want: "recovery unlock requires a passphrase file"},
		{name: "S3 bucket", options: Options{ObjectStore: "s3"}, want: "S3 bucket is not configured"},
		{name: "object store", options: Options{ObjectStore: "unknown"}, want: "unsupported object store"},
		{name: "allowlist", options: Options{TCPAllowlist: []string{"invalid"}}, want: "invalid TCP allowlist entry"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateEnsureOptions(test.options)
			if err == nil || !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateEnsureOptions() = %v, want ErrUnavailable containing %q", err, test.want)
			}
		})
	}
	if err := validateEnsureOptions(Options{ObjectStore: "memory", TCPAllowlist: []string{"127.0.0.1/32"}}); err != nil {
		t.Fatalf("validateEnsureOptions(valid) = %v", err)
	}
}

func TestValidateDaemonStartOptions(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		want    string
	}{
		{name: "daemon path", options: Options{}, want: "daemon path is not configured"},
		{name: "TCP allowlist", options: Options{DaemonPath: "daemon", TCPAddress: "127.0.0.1:1"}, want: "TCP allowlist is not configured"},
		{
			name:    "TCP token",
			options: Options{DaemonPath: "daemon", TCPAddress: "127.0.0.1:1", TCPAllowlist: []string{"127.0.0.1/32"}},
			want:    "TCP authentication token is not configured",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateDaemonStartOptions(test.options)
			if err == nil || !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateDaemonStartOptions() = %v, want ErrUnavailable containing %q", err, test.want)
			}
		})
	}
	valid := Options{DaemonPath: "daemon", TCPAddress: "127.0.0.1:1", TCPAllowlist: []string{"127.0.0.1/32"}, AuthToken: "token"}
	if err := validateDaemonStartOptions(valid); err != nil {
		t.Fatalf("validateDaemonStartOptions(valid) = %v", err)
	}
}

func TestPrepareDaemonCommand(t *testing.T) {
	options := Options{
		Socket: "/tmp/vaulticdb/test.sock", TCPAddress: "127.0.0.1:1234", TCPAllowlist: []string{"127.0.0.1/32"},
		AuthToken: "secret", RepositoryID: "repo", DaemonPath: "/path/to/vaulticdb", ObjectStore: "memory",
		RecoveryUnlock: true, BrokerLease: 3 * time.Second, RebuildInitialize: true,
	}
	cmd, authRead, authWrite, err := prepareDaemonCommand(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = authRead.Close()
		_ = authWrite.Close()
	})
	if cmd.Path != options.DaemonPath || len(cmd.ExtraFiles) != 1 || cmd.ExtraFiles[0] != authRead {
		t.Fatalf("unexpected daemon command: path=%q extra-files=%v", cmd.Path, cmd.ExtraFiles)
	}
	environment := strings.Join(cmd.Env, "\n")
	for _, entry := range []string{
		"VAULTICDB_SOCKET=" + options.Socket,
		"VAULTICDB_REPOSITORY_ID=" + options.RepositoryID,
		"VAULTICDB_OBJECT_STORE=memory",
		"VAULTICDB_TCP_AUTH_TOKEN_FD=3",
		"VAULTICDB_TRANSPORT=tcp",
		"VAULTICDB_ENCRYPTION_RECOVERY_ACK=true",
		"VAULTICDB_BROKER_LEASE_SECONDS=3",
		"VAULTICDB_METADATA_REBUILD_INITIALIZE=true",
	} {
		if !strings.Contains(environment, entry) {
			t.Errorf("daemon environment missing %q: %s", entry, environment)
		}
	}
}

func TestDaemonEnvironmentFiltersAmbientSecrets(t *testing.T) {
	t.Setenv("VAULTIC_UNRELATED_SECRET", "must-not-be-inherited")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s3-credential")
	t.Setenv("PATH", "/test/bin")

	local := strings.Join(daemonEnvironment(Options{ObjectStore: "local"}), "\n")
	if strings.Contains(local, "VAULTIC_UNRELATED_SECRET") || strings.Contains(local, "AWS_SECRET_ACCESS_KEY") {
		t.Fatalf("local daemon inherited a secret-bearing environment: %s", local)
	}
	if !strings.Contains(local, "PATH=/test/bin") {
		t.Fatalf("local daemon lost required runtime environment: %s", local)
	}

	s3 := strings.Join(daemonEnvironment(Options{ObjectStore: "s3"}), "\n")
	if strings.Contains(s3, "VAULTIC_UNRELATED_SECRET") || !strings.Contains(s3, "AWS_SECRET_ACCESS_KEY=s3-credential") {
		t.Fatalf("S3 daemon environment is not credential-chain scoped: %s", s3)
	}
}

func TestEncryptedDaemonPersistsOnlyCiphertextAndReopens(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	passphraseFile := filepath.Join(t.TempDir(), "recovery-passphrase")
	if err := os.WriteFile(passphraseFile, []byte("correct horse battery staple\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := Options{
		Socket: testSocket(t), RepositoryID: "encrypted-daemon", DaemonPath: daemonBinary(t),
		DataDir: dataDir, EncryptionMode: "initialize", PassphraseFile: passphraseFile,
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	if info := client.Encryption(); !info.Enabled || info.Algorithm != "AES-256-GCM" || info.ActiveDEKVersion != 1 || info.UnlockSlot != "local-recovery" {
		t.Fatalf("unexpected encryption status: %+v", info)
	}
	secret := []byte("alice/private/metadata-value")
	masterKey := []byte("base64-encoded-repository-master-key-fixture")
	if _, err := client.WriteBatch(ctx, []Mutation{{Key: []byte("phase18/secret"), Value: secret}}, nil, true, ""); err != nil {
		t.Fatal(err)
	}
	if err := client.StoreMasterKey(ctx, masterKey); err != nil {
		t.Fatal(err)
	}
	if err := client.StoreMasterKey(ctx, []byte("different-key")); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("master key replacement was not rejected: %v", err)
	}
	keyStatus, err := client.AddLocalKeySlot(ctx, "replacement-recovery", []byte("temporary passphrase"), 10, true)
	if err != nil || keyStatus.EnvelopeGeneration != 2 || len(keyStatus.Slots) != 2 {
		t.Fatalf("add key slot = %+v, err=%v", keyStatus, err)
	}
	keyStatus, err = client.RotateLocalKeySlot(ctx, "replacement-recovery", []byte("replacement passphrase"))
	if err != nil || keyStatus.EnvelopeGeneration != 3 {
		t.Fatalf("rotate key slot = %+v, err=%v", keyStatus, err)
	}
	keyStatus, err = client.RemoveKeySlot(ctx, "local-recovery")
	if err != nil || keyStatus.EnvelopeGeneration != 4 || len(keyStatus.Slots) != 1 {
		t.Fatalf("remove key slot = %+v, err=%v", keyStatus, err)
	}
	if _, err := client.RemoveKeySlot(ctx, "replacement-recovery"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("final slot removal was not rejected: %v", err)
	}
	keyStatus, err = client.RotateDEK(ctx)
	if err != nil || keyStatus.EnvelopeGeneration != 5 || keyStatus.ActiveDEKVersion != 2 {
		t.Fatalf("rotate DEK = %+v, err=%v", keyStatus, err)
	}
	rotatedSecret := []byte("metadata-written-under-DEK-version-2")
	if _, err := client.WriteBatch(ctx, []Mutation{{Key: []byte("phase18/rotated"), Value: rotatedSecret}}, nil, true, ""); err != nil {
		t.Fatal(err)
	}
	var rewritten uint64
	for {
		progress, rewriteErr := client.RewriteDEK(ctx, 1)
		if rewriteErr != nil {
			t.Fatal(rewriteErr)
		}
		rewritten += progress.Rewritten
		if progress.Remaining == 0 {
			break
		}
	}
	if rewritten == 0 {
		t.Fatal("DEK rotation did not rewrite any old-version objects")
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := filepath.WalkDir(dataDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if bytes.Contains(data, secret) || bytes.Contains(data, masterKey) {
			return fmt.Errorf("plaintext metadata found in %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	options.Socket = testSocket(t)
	options.EncryptionMode = "required"
	if err := os.WriteFile(passphraseFile, []byte("replacement passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err = Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)
	if info := client.Encryption(); info.ActiveDEKVersion != 2 || info.EnvelopeGeneration != 6 {
		t.Fatalf("reopened rotated encryption status: %+v", info)
	}
	value, found, err := client.Get(ctx, []byte("phase18/secret"), "")
	if err != nil || !found || !bytes.Equal(value, secret) {
		t.Fatalf("reopened encrypted value = %q, found=%t, err=%v", value, found, err)
	}
	value, found, err = client.GetMasterKey(ctx)
	if err != nil || !found || !bytes.Equal(value, masterKey) {
		t.Fatalf("reopened master key = %q, found=%t, err=%v", value, found, err)
	}
	value, found, err = client.Get(ctx, []byte("phase18/rotated"), "")
	if err != nil || !found || !bytes.Equal(value, rotatedSecret) {
		t.Fatalf("reopened rotated value = %q, found=%t, err=%v", value, found, err)
	}
}

func TestEncryptedDaemonRefusesMissingPersistentPolicy(t *testing.T) {
	ctx := context.Background()
	passphraseFile := filepath.Join(t.TempDir(), "recovery-passphrase")
	if err := os.WriteFile(passphraseFile, []byte("policy test passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := Options{
		Socket: testSocket(t), RepositoryID: "encrypted-policy", DaemonPath: daemonBinary(t),
		DataDir: t.TempDir(), EncryptionMode: "initialize", PassphraseFile: passphraseFile,
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.WriteBatch(ctx, nil, [][]byte{[]byte("meta:encryption")}, true, ""); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	options.Socket = testSocket(t)
	options.EncryptionMode = "required"
	if client, err := Ensure(ctx, options); err == nil {
		_ = client.Close(ctx)
		t.Fatal("required encryption recreated a missing persistent policy")
	}
}

func TestFailedMetadataKeyUnwrapEmitsAuthEvent(t *testing.T) {
	ctx := context.Background()
	passphraseFile := filepath.Join(t.TempDir(), "recovery-passphrase")
	if err := os.WriteFile(passphraseFile, []byte("correct passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	options := Options{
		Socket: testSocket(t), RepositoryID: "failed-unwrap", DaemonPath: daemonBinary(t),
		DataDir: t.TempDir(), EncryptionMode: "initialize", PassphraseFile: passphraseFile,
	}
	client, err := Ensure(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(passphraseFile, []byte("wrong passphrase\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	target, err := observability.ParseSyslogTarget("udp://" + listener.LocalAddr().String() + "?categories=auth&min-severity=warning")
	if err != nil {
		t.Fatal(err)
	}
	observability.SetDefaultSyslog(observability.NewSyslogExporter([]observability.SyslogTarget{target}, "host", "vaultic"))
	defer observability.SetDefaultSyslog(nil)
	options.Socket = testSocket(t)
	options.EncryptionMode = "required"
	if client, err := Ensure(ctx, options); err == nil {
		_ = client.Close(ctx)
		t.Fatal("wrong metadata passphrase unexpectedly unlocked the daemon")
	} else if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unexpected failed-unlock error: %v", err)
	}
	buffer := make([]byte, 4096)
	if err := listener.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	count, _, err := listener.ReadFrom(buffer)
	if err != nil {
		t.Fatal(err)
	}
	message := string(buffer[:count])
	categoryFound := strings.Contains(message, `"category":"auth"`)
	eventFound := strings.Contains(message, `"message":"encrypted metadata daemon failed during startup"`)
	if !categoryFound || !eventFound || strings.Contains(message, "passphrase") {
		t.Fatalf("unexpected failed-unlock event: %s", message)
	}
}

func TestTCPLifecycleAuthenticationDrainDeadlineAndLimit(t *testing.T) {
	options := Options{
		TCPAddress:   freeTCPAddress(t),
		TCPAllowlist: []string{"127.0.0.1/32"},
		AuthToken:    "phase1-secret",
		RepositoryID: "tcp-repo",
		DaemonPath:   daemonBinary(t),
		StartTimeout: 5 * time.Second,
	}
	client, err := Ensure(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if client.process != nil {
			_ = client.Close(context.Background())
		}
	}()

	capabilities, err := client.RPC().Capabilities(context.Background(), &vaulticdbv1.CapabilitiesRequest{
		RepositoryId: options.RepositoryID,
		Context:      requestContext(context.Background()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.GetTcpEnabled() || capabilities.GetUnixSocket() {
		t.Fatalf("unexpected transport capabilities: %#v", capabilities)
	}
	if capabilities.GetMaxBatchItems() != 10_000 || capabilities.GetMaxPageItems() != 1_000 || capabilities.GetMaxMessageBytes() != 16*1024*1024 ||
		capabilities.GetMaxConcurrentRequests() != 128 {
		t.Fatalf("unexpected bounded-work capabilities: %#v", capabilities)
	}

	for tokenName, token := range map[string]string{"missing": "", "wrong": "wrong-secret"} {
		rpc := vaulticdbv1.NewVaulticDBClient(client.conn)
		if token != "" {
			rpc = &authenticatedClient{VaulticDBClient: rpc, token: token}
		}
		checks := map[string]func() error{
			"health": func() error {
				_, err := rpc.Health(
					context.Background(),
					&vaulticdbv1.HealthRequest{RepositoryId: options.RepositoryID, Context: requestContext(context.Background())},
				)
				return err
			},
			"capabilities": func() error {
				_, err := rpc.Capabilities(
					context.Background(),
					&vaulticdbv1.CapabilitiesRequest{RepositoryId: options.RepositoryID, Context: requestContext(context.Background())},
				)
				return err
			},
			"drain": func() error {
				_, err := rpc.Drain(context.Background(), &vaulticdbv1.Empty{Context: requestContext(context.Background())})
				return err
			},
			"shutdown": func() error {
				_, err := rpc.Shutdown(context.Background(), &vaulticdbv1.Empty{Context: requestContext(context.Background())})
				return err
			},
			"get": func() error {
				_, err := rpc.Get(context.Background(), &vaulticdbv1.GetRequest{Context: requestContext(context.Background()), Key: []byte("key")})
				return err
			},
			"multi-get": func() error {
				_, err := rpc.MultiGet(
					context.Background(),
					&vaulticdbv1.MultiGetRequest{Context: requestContext(context.Background()), Keys: [][]byte{[]byte("key")}},
				)
				return err
			},
			"scan": func() error {
				_, err := rpc.Scan(
					context.Background(),
					&vaulticdbv1.ScanRequest{Context: requestContext(context.Background()), Prefix: []byte("k"), PageSize: 1},
				)
				return err
			},
			"write-batch": func() error {
				_, err := rpc.WriteBatch(
					context.Background(),
					&vaulticdbv1.WriteBatchRequest{
						Context: requestContext(context.Background()),
						Puts:    []*vaulticdbv1.KeyValue{{Key: []byte("key"), Value: []byte("value")}},
					},
				)
				return err
			},
			"begin": func() error {
				_, err := rpc.Begin(context.Background(), &vaulticdbv1.Empty{Context: requestContext(context.Background())})
				return err
			},
			"commit": func() error {
				_, err := rpc.Commit(
					context.Background(),
					&vaulticdbv1.TransactionRequest{Context: requestContext(context.Background()), TransactionId: "unknown"},
				)
				return err
			},
			"rollback": func() error {
				_, err := rpc.Rollback(
					context.Background(),
					&vaulticdbv1.TransactionRequest{Context: requestContext(context.Background()), TransactionId: "unknown"},
				)
				return err
			},
		}
		for rpcName, check := range checks {
			if err := check(); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("expected %s token rejection from %s, got %v", tokenName, rpcName, err)
			}
		}
	}

	expired := &vaulticdbv1.RequestContext{RequestId: "expired", DeadlineUnixMs: time.Now().Add(-time.Second).UnixMilli()}
	_, err = client.RPC().Health(context.Background(), &vaulticdbv1.HealthRequest{RepositoryId: options.RepositoryID, Context: expired})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected expired deadline rejection, got %v", err)
	}
	_, err = client.RPC().Health(context.Background(), &vaulticdbv1.HealthRequest{
		RepositoryId: options.RepositoryID,
		Context:      &vaulticdbv1.RequestContext{DeadlineUnixMs: time.Now().Add(time.Minute).UnixMilli()},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected missing request ID rejection, got %v", err)
	}

	oversized := &vaulticdbv1.RequestContext{RequestId: strings.Repeat("x", 17*1024*1024), DeadlineUnixMs: time.Now().Add(time.Minute).UnixMilli()}
	_, err = client.RPC().Health(context.Background(), &vaulticdbv1.HealthRequest{RepositoryId: options.RepositoryID, Context: oversized})
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("expected oversized request rejection, got %v", err)
	}

	if _, err := client.RPC().Drain(context.Background(), &vaulticdbv1.Empty{Context: requestContext(context.Background())}); err != nil {
		t.Fatal(err)
	}
	health, err := client.RPC().Health(context.Background(), &vaulticdbv1.HealthRequest{
		RepositoryId: options.RepositoryID,
		Context:      requestContext(context.Background()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if health.GetReady() {
		t.Fatal("daemon remained ready after drain")
	}
	if _, _, err := client.Get(context.Background(), []byte("after-drain"), ""); status.Code(err) != codes.Unavailable {
		t.Fatalf("storage request after drain returned %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureTCPRejectsPeerOutsideAllowlist(t *testing.T) {
	options := Options{
		TCPAddress:   freeTCPAddress(t),
		TCPAllowlist: []string{"192.0.2.0/24"},
		AuthToken:    "allowlist-secret",
		RepositoryID: "tcp-allowlist-repo",
		DaemonPath:   daemonBinary(t),
		StartTimeout: 750 * time.Millisecond,
	}
	_, err := Ensure(context.Background(), options)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected disallowed peer startup timeout, got %v", err)
	}
	if _, err := os.Stat(strings.TrimSuffix(tcpMetadataPath(options.TCPAddress), filepath.Ext(tcpMetadataPath(options.TCPAddress))) + ".pid"); !errors.Is(
		err,
		os.ErrNotExist,
	) {
		t.Fatalf("TCP metadata remained after failed startup: %v", err)
	}
}

func TestEnsureTCPRaceHasOneOwner(t *testing.T) {
	options := Options{
		TCPAddress:   freeTCPAddress(t),
		TCPAllowlist: []string{"127.0.0.1/32"},
		AuthToken:    "race-secret",
		RepositoryID: "tcp-race-repo",
		DaemonPath:   daemonBinary(t),
		StartTimeout: 5 * time.Second,
	}
	clients := make([]*Client, 4)
	errs := make([]error, len(clients))
	var group sync.WaitGroup
	for index := range clients {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			clients[index], errs[index] = Ensure(context.Background(), options)
		}(index)
	}
	group.Wait()
	owners := 0
	for index, client := range clients {
		if errs[index] != nil {
			t.Fatal(errs[index])
		}
		if client.process != nil {
			owners++
			defer client.Close(context.Background())
		} else if err := client.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if owners != 1 {
		t.Fatalf("got %d TCP daemon owners, want 1", owners)
	}
}

func TestEnsureCancellationKillsUnreadyChild(t *testing.T) {
	directory := t.TempDir()
	script := filepath.Join(directory, "unready-daemon")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(directory, "daemon.sock")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := Ensure(ctx, Options{Socket: socket, RepositoryID: "cancel-repo", DaemonPath: script, StartTimeout: 5 * time.Second})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected cancellation error, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("cancellation cleanup took %v", elapsed)
	}
}

func TestCloseBoundsShutdownAndKillsOwnedChild(t *testing.T) {
	socket := testSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	vaulticdbv1.RegisterVaulticDBServer(server, testService{protocol: ProtocolVersion, schema: SchemaVersion, blockShutdown: true})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client, err := Connect(context.Background(), Options{Socket: socket, RepositoryID: "shutdown-repo"})
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command("sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	client.process = child
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := client.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected bounded shutdown deadline, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("bounded shutdown took %v", elapsed)
	}
}
