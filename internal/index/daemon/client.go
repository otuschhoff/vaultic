package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	ProtocolVersion = "vaulticdb.v1"
	SchemaVersion   = "0"
)

var ErrUnavailable = errors.New("vaulticdb unavailable")

var requestSequence atomic.Uint64

// Options controls how a vaultic process connects to or starts vaulticdb.
type Options struct {
	Socket           string
	TCPAddress       string
	TCPAllowlist     []string
	AuthToken        string
	RepositoryID     string
	DaemonPath       string
	StartTimeout     time.Duration
	RetryInterval    time.Duration
	PersistentDaemon bool
}

func (o Options) withDefaults() Options {
	if o.Socket == "" {
		o.Socket = DefaultSocket(o.RepositoryID)
	}
	if o.StartTimeout == 0 {
		o.StartTimeout = 10 * time.Second
	}
	if o.RetryInterval == 0 {
		o.RetryInterval = 25 * time.Millisecond
	}
	return o
}

// DefaultSocket returns a repository-scoped Unix endpoint path.
func DefaultSocket(repositoryID string) string {
	if repositoryID == "" {
		repositoryID = "default"
	}
	digest := sha256.Sum256([]byte(repositoryID))
	return filepath.Join("/tmp/vaulticdb", hex.EncodeToString(digest[:])+".sock")
}

// Client is a validated connection to one vaulticdb endpoint.
type Client struct {
	conn    *grpc.ClientConn
	rpc     vaulticdbv1.VaulticDBClient
	process *exec.Cmd
	options Options
}

// Connect attaches to an already-running compatible daemon.
func Connect(ctx context.Context, options Options) (*Client, error) {
	options = options.withDefaults()
	client, err := dial(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return client, nil
}

// Ensure connects to a daemon or starts one when no compatible daemon exists.
// If multiple vaultic processes race, only the process that acquires the
// daemon's singleton lock successfully starts it; other callers retry attach.
func Ensure(ctx context.Context, options Options) (*Client, error) {
	options = options.withDefaults()
	if client, err := Connect(ctx, options); err == nil {
		return client, nil
	}
	if options.DaemonPath == "" {
		return nil, fmt.Errorf("%w: daemon path is not configured", ErrUnavailable)
	}
	if options.TCPAddress != "" && len(options.TCPAllowlist) == 0 {
		return nil, fmt.Errorf("%w: TCP allowlist is not configured", ErrUnavailable)
	}

	cmd := exec.Command(options.DaemonPath)
	cmd.Env = append(os.Environ(),
		"VAULTICDB_SOCKET="+options.Socket,
		"VAULTICDB_REPOSITORY_ID="+options.RepositoryID,
	)
	if options.AuthToken != "" {
		cmd.Env = append(cmd.Env, "VAULTICDB_TCP_AUTH_TOKEN="+options.AuthToken)
	}
	if options.TCPAddress != "" {
		cmd.Env = append(cmd.Env,
			"VAULTICDB_TRANSPORT=tcp",
			"VAULTICDB_TCP_ADDR="+options.TCPAddress,
			"VAULTICDB_TCP_ALLOWLIST="+strings.Join(options.TCPAllowlist, ","),
		)
	}
	if err := cmd.Start(); err != nil {
		// Another caller may have started the singleton between our first dial
		// and this start attempt. Retry attach before returning the start error.
		if client, dialErr := retryDial(ctx, options); dialErr == nil {
			return client, nil
		}
		return nil, fmt.Errorf("start vaulticdb: %w", err)
	}

	client, err := retryDial(ctx, options)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("%w: wait for daemon readiness: %v", ErrUnavailable, err)
	}
	if options.TCPAddress == "" && !daemonOwnsProcess(options.Socket, cmd.Process.Pid) {
		_ = cmd.Wait()
		cmd = nil
	}
	if options.PersistentDaemon {
		cmd = nil
	}
	client.process = cmd
	return client, nil
}

func daemonOwnsProcess(socket string, processID int) bool {
	pidPath := strings.TrimSuffix(filepath.Clean(socket), filepath.Ext(socket)) + ".pid"
	contents, err := os.ReadFile(pidPath)
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(contents)))
	return err == nil && pid == processID
}

func dial(ctx context.Context, options Options) (*Client, error) {
	target := "passthrough:///vaulticdb"
	dialer := unixDialer(options.Socket)
	if options.TCPAddress == "" {
		if err := validateUnixEndpoint(options.Socket); err != nil {
			return nil, err
		}
	}
	if options.TCPAddress != "" {
		target = options.TCPAddress
		dialer = func(ctx context.Context, address string) (net.Conn, error) {
			var netDialer net.Dialer
			return netDialer.DialContext(ctx, "tcp", address)
		}
	}
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}
	client := &Client{conn: conn, rpc: vaulticdbv1.NewVaulticDBClient(conn), options: options}
	if options.AuthToken != "" {
		client.rpc = &authenticatedClient{VaulticDBClient: client.rpc, token: options.AuthToken}
	}
	if err := client.validate(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return client, nil
}

func validateUnixEndpoint(socket string) error {
	info, err := os.Lstat(socket)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("unsafe vaulticdb socket permissions at %s", socket)
	}
	directory, err := os.Stat(filepath.Dir(socket))
	if err != nil {
		return err
	}
	if !directory.IsDir() || directory.Mode().Perm() != 0o700 {
		return fmt.Errorf("unsafe vaulticdb runtime directory permissions at %s", filepath.Dir(socket))
	}
	return nil
}

type authenticatedClient struct {
	vaulticdbv1.VaulticDBClient
	token string
}

func (c *authenticatedClient) Health(ctx context.Context, in *vaulticdbv1.HealthRequest, opts ...grpc.CallOption) (*vaulticdbv1.HealthResponse, error) {
	return c.VaulticDBClient.Health(withAuth(ctx, c.token), in, opts...)
}

func (c *authenticatedClient) Capabilities(ctx context.Context, in *vaulticdbv1.CapabilitiesRequest, opts ...grpc.CallOption) (*vaulticdbv1.CapabilitiesResponse, error) {
	return c.VaulticDBClient.Capabilities(withAuth(ctx, c.token), in, opts...)
}

func (c *authenticatedClient) Drain(ctx context.Context, in *vaulticdbv1.Empty, opts ...grpc.CallOption) (*vaulticdbv1.Empty, error) {
	return c.VaulticDBClient.Drain(withAuth(ctx, c.token), in, opts...)
}

func (c *authenticatedClient) Shutdown(ctx context.Context, in *vaulticdbv1.Empty, opts ...grpc.CallOption) (*vaulticdbv1.Empty, error) {
	return c.VaulticDBClient.Shutdown(withAuth(ctx, c.token), in, opts...)
}

func withAuth(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func retryDial(ctx context.Context, options Options) (*Client, error) {
	deadline := time.Now().Add(options.StartTimeout)
	for {
		client, err := dial(ctx, options)
		if err == nil {
			return client, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		timer := time.NewTimer(options.RetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func unixDialer(socket string) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", socket)
	}
}

func (c *Client) validate(ctx context.Context) error {
	health, err := c.rpc.Health(ctx, &vaulticdbv1.HealthRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx)})
	if err != nil {
		return err
	}
	if !health.GetReady() || health.GetProtocolVersion() != ProtocolVersion || health.GetSchemaVersion() != SchemaVersion {
		return fmt.Errorf("incompatible daemon: ready=%t protocol=%q schema=%q", health.GetReady(), health.GetProtocolVersion(), health.GetSchemaVersion())
	}
	if health.GetRepositoryId() != "" && health.GetRepositoryId() != c.options.RepositoryID {
		return fmt.Errorf("daemon repository identity %q does not match %q", health.GetRepositoryId(), c.options.RepositoryID)
	}
	capabilities, err := c.rpc.Capabilities(ctx, &vaulticdbv1.CapabilitiesRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx)})
	if err != nil {
		return err
	}
	if capabilities.GetProtocolVersion() != ProtocolVersion || capabilities.GetSchemaVersion() != SchemaVersion {
		return fmt.Errorf("incompatible daemon capabilities: protocol=%q schema=%q", capabilities.GetProtocolVersion(), capabilities.GetSchemaVersion())
	}
	if capabilities.GetTcpEnabled() != (c.options.TCPAddress != "") {
		return fmt.Errorf("daemon transport does not match the requested endpoint")
	}
	return nil
}

func (c *Client) RPC() vaulticdbv1.VaulticDBClient { return c.rpc }

// Close closes the RPC connection and shuts down only a daemon started by this client.
func (c *Client) Close(ctx context.Context) error {
	var result error
	if c.process != nil {
		_, err := c.rpc.Shutdown(ctx, &vaulticdbv1.Empty{Context: requestContext(ctx)})
		if err != nil {
			result = err
		}
	}
	if err := c.conn.Close(); err != nil && result == nil {
		result = err
	}
	if c.process != nil {
		if err := c.process.Wait(); err != nil && result == nil {
			result = err
		}
		c.process = nil
	}
	return result
}

func requestContext(ctx context.Context) *vaulticdbv1.RequestContext {
	deadline := time.Now().Add(10 * time.Second)
	if value, ok := ctx.Deadline(); ok {
		deadline = value
	}
	return &vaulticdbv1.RequestContext{
		RequestId:      fmt.Sprintf("vaultic-%d", requestSequence.Add(1)),
		DeadlineUnixMs: deadline.UnixMilli(),
	}
}

// SocketDir returns the private directory expected for an endpoint socket.
func SocketDir(socket string) string { return filepath.Dir(socket) }
