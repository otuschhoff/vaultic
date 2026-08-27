package daemon

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"google.golang.org/grpc"
)

type testService struct {
	vaulticdbv1.UnimplementedVaulticDBServer
	protocol string
	schema   string
	repo     string
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

func (s testService) Health(_ context.Context, request *vaulticdbv1.HealthRequest) (*vaulticdbv1.HealthResponse, error) {
	return &vaulticdbv1.HealthResponse{Ready: true, ProtocolVersion: s.protocol, SchemaVersion: s.schema, RepositoryId: request.GetRepositoryId()}, nil
}

func (s testService) Capabilities(_ context.Context, request *vaulticdbv1.CapabilitiesRequest) (*vaulticdbv1.CapabilitiesResponse, error) {
	return &vaulticdbv1.CapabilitiesResponse{ProtocolVersion: s.protocol, SchemaVersion: s.schema, RepositoryId: request.GetRepositoryId()}, nil
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
