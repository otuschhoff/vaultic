package daemon

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testService struct {
	vaulticdbv1.UnimplementedVaulticDBServer
	protocol      string
	schema        string
	repo          string
	blockShutdown bool
}

func testSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "vd-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "d.sock")
}

func daemonBinary(t *testing.T) string {
	t.Helper()
	if binary := os.Getenv("VAULTICDB_TEST_BINARY"); binary != "" {
		return binary
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	binary := filepath.Join(filepath.Dir(source), "..", "..", "..", "vaulticdb", "target", "debug", "vaulticdb")
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	return binary
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func (s testService) Health(_ context.Context, request *vaulticdbv1.HealthRequest) (*vaulticdbv1.HealthResponse, error) {
	return &vaulticdbv1.HealthResponse{Ready: true, ProtocolVersion: s.protocol, SchemaVersion: s.schema, RepositoryId: request.GetRepositoryId()}, nil
}

func (s testService) Capabilities(_ context.Context, request *vaulticdbv1.CapabilitiesRequest) (*vaulticdbv1.CapabilitiesResponse, error) {
	return &vaulticdbv1.CapabilitiesResponse{ProtocolVersion: s.protocol, SchemaVersion: s.schema, RepositoryId: request.GetRepositoryId()}, nil
}

func (s testService) Shutdown(ctx context.Context, _ *vaulticdbv1.Empty) (*vaulticdbv1.Empty, error) {
	if s.blockShutdown {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &vaulticdbv1.Empty{}, nil
}

func TestOptionsDefaults(t *testing.T) {
	options := (Options{}).withDefaults()
	if options.Socket != DefaultSocket("") || options.StartTimeout != 10*time.Second || options.RetryInterval != 25*time.Millisecond {
		t.Fatalf("unexpected defaults: %#v", options)
	}
}

func TestDefaultSocketIsRepositoryScoped(t *testing.T) {
	if DefaultSocket("first") == DefaultSocket("second") {
		t.Fatal("repository-scoped socket paths must differ")
	}
}

func TestConnectValidatesDaemon(t *testing.T) {
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
	vaulticdbv1.RegisterVaulticDBServer(server, testService{protocol: ProtocolVersion, schema: SchemaVersion})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	client, err := Connect(context.Background(), Options{Socket: socket, RepositoryID: "test-repo"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConnectRejectsIncompatibleDaemon(t *testing.T) {
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
	vaulticdbv1.RegisterVaulticDBServer(server, testService{protocol: "vaulticdb.v0", schema: SchemaVersion})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	if _, err := Connect(context.Background(), Options{Socket: socket, RepositoryID: "test-repo"}); err == nil {
		t.Fatal("expected incompatible daemon error")
	}
}

func TestSocketDir(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "runtime", "vaulticdb.sock")
	if got, want := SocketDir(socket), filepath.Join(dir, "runtime"); got != want {
		t.Fatalf("SocketDir() = %q, want %q", got, want)
	}
}

func TestEnsureStartsDaemonRecoversStaleSocketAndCleansUp(t *testing.T) {
	socket := testSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	client, err := Ensure(context.Background(), Options{Socket: socket, RepositoryID: "test-repo", DaemonPath: daemonBinary(t)})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(socket)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket permissions: %v, %v", info.Mode(), err)
	}
	dir, err := os.Stat(filepath.Dir(socket))
	if err != nil || dir.Mode().Perm() != 0o700 {
		t.Fatalf("directory permissions: %v, %v", dir.Mode(), err)
	}
	capabilities, err := client.RPC().Capabilities(context.Background(), &vaulticdbv1.CapabilitiesRequest{RepositoryId: "test-repo", Context: requestContext(context.Background())})
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.GetUnixSocket() || capabilities.GetTcpEnabled() || capabilities.GetMaxConcurrentRequests() != 128 {
		t.Fatalf("unexpected Unix transport capabilities: %#v", capabilities)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("socket remains after shutdown: %v", err)
	}
}

func TestEnsureRacesToOneDaemon(t *testing.T) {
	socket := testSocket(t)
	options := Options{Socket: socket, RepositoryID: "race-repo", DaemonPath: daemonBinary(t)}
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
	var owner *Client
	for index, client := range clients {
		if errs[index] != nil {
			t.Fatal(errs[index])
		}
		if client.process != nil {
			owner = client
		} else if err := client.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if owner == nil {
		t.Fatal("no client owned the daemon")
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

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
	if capabilities.GetMaxBatchItems() != 10_000 || capabilities.GetMaxPageItems() != 1_000 || capabilities.GetMaxMessageBytes() != 16*1024*1024 || capabilities.GetMaxConcurrentRequests() != 128 {
		t.Fatalf("unexpected bounded-work capabilities: %#v", capabilities)
	}

	for tokenName, token := range map[string]string{"missing": "", "wrong": "wrong-secret"} {
		rpc := vaulticdbv1.NewVaulticDBClient(client.conn)
		if token != "" {
			rpc = &authenticatedClient{VaulticDBClient: rpc, token: token}
		}
		checks := map[string]func() error{
			"health": func() error {
				_, err := rpc.Health(context.Background(), &vaulticdbv1.HealthRequest{RepositoryId: options.RepositoryID, Context: requestContext(context.Background())})
				return err
			},
			"capabilities": func() error {
				_, err := rpc.Capabilities(context.Background(), &vaulticdbv1.CapabilitiesRequest{RepositoryId: options.RepositoryID, Context: requestContext(context.Background())})
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
	if _, err := os.Stat(strings.TrimSuffix(tcpMetadataPath(options.TCPAddress), filepath.Ext(tcpMetadataPath(options.TCPAddress))) + ".pid"); !errors.Is(err, os.ErrNotExist) {
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
