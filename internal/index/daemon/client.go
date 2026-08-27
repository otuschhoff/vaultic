package daemon

import (
	"bytes"
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

	vaulticfs "github.com/otuschhoff/vaultic/internal/fs"
	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	ProtocolVersion    = "vaulticdb.v1"
	SchemaVersion      = "0"
	defaultRPCDeadline = 10 * time.Second
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
	ObjectStore      string
	DataDir          string
	S3Bucket         string
	S3Prefix         string
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
	limits  Limits
}

// Limits are the bounded-work capabilities advertised by vaulticdb.
type Limits struct {
	MaxBatchItems   uint32
	MaxMessageBytes uint32
	MaxPageItems    uint32
}

// KeyValue is one binary metadata record returned by a scan.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// Mutation is one put operation in a write batch.
type Mutation struct {
	Key   []byte
	Value []byte
}

// Connect attaches to an already-running compatible daemon.
func Connect(ctx context.Context, options Options) (*Client, error) {
	options = options.withDefaults()
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
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
	if options.ObjectStore == "s3" && options.S3Bucket == "" {
		return nil, fmt.Errorf("%w: S3 bucket is not configured", ErrUnavailable)
	}
	if options.ObjectStore != "" && options.ObjectStore != "local" && options.ObjectStore != "memory" && options.ObjectStore != "s3" {
		return nil, fmt.Errorf("%w: unsupported object store %q", ErrUnavailable, options.ObjectStore)
	}
	for _, network := range options.TCPAllowlist {
		if _, _, err := net.ParseCIDR(network); err != nil {
			return nil, fmt.Errorf("%w: invalid TCP allowlist entry %q: %v", ErrUnavailable, network, err)
		}
	}
	probeCtx, cancelProbe := context.WithTimeout(ctx, dialAttemptTimeout(options))
	client, connectErr := Connect(probeCtx, options)
	cancelProbe()
	if connectErr == nil {
		return client, nil
	}
	if options.DaemonPath == "" {
		return nil, fmt.Errorf("%w: daemon path is not configured", ErrUnavailable)
	}
	if options.TCPAddress != "" && len(options.TCPAllowlist) == 0 {
		return nil, fmt.Errorf("%w: TCP allowlist is not configured", ErrUnavailable)
	}
	if options.TCPAddress != "" && options.AuthToken == "" {
		return nil, fmt.Errorf("%w: TCP authentication token is not configured", ErrUnavailable)
	}

	cmd := exec.Command(options.DaemonPath)
	cmd.Env = append(os.Environ(),
		"VAULTICDB_SOCKET="+options.Socket,
		"VAULTICDB_REPOSITORY_ID="+options.RepositoryID,
	)
	for name, value := range map[string]string{
		"VAULTICDB_OBJECT_STORE": options.ObjectStore,
		"VAULTICDB_DATA_DIR":     options.DataDir,
		"VAULTICDB_S3_BUCKET":    options.S3Bucket,
		"VAULTICDB_S3_PREFIX":    options.S3Prefix,
	} {
		if value != "" {
			cmd.Env = append(cmd.Env, name+"="+value)
		}
	}
	if options.AuthToken != "" {
		cmd.Env = append(cmd.Env, "VAULTICDB_TCP_AUTH_TOKEN="+options.AuthToken)
	}
	if options.TCPAddress != "" {
		cmd.Env = append(cmd.Env,
			"VAULTICDB_TRANSPORT=tcp",
			"VAULTICDB_TCP_ADDR="+options.TCPAddress,
			"VAULTICDB_TCP_ALLOWLIST="+strings.Join(options.TCPAllowlist, ","),
			"VAULTICDB_TCP_METADATA="+tcpMetadataPath(options.TCPAddress),
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
		cleanupOwnedArtifacts(options, cmd.Process.Pid)
		return nil, fmt.Errorf("%w: wait for daemon readiness: %w", ErrUnavailable, err)
	}
	metadataPath := options.Socket
	if options.TCPAddress != "" {
		metadataPath = tcpMetadataPath(options.TCPAddress)
	}
	if !daemonOwnsProcess(metadataPath, cmd.Process.Pid) {
		_ = cmd.Wait()
		cmd = nil
	}
	if options.PersistentDaemon {
		cmd = nil
	}
	client.process = cmd
	return client, nil
}

func tcpMetadataPath(address string) string {
	digest := sha256.Sum256([]byte(address))
	return filepath.Join("/tmp/vaulticdb", "tcp-"+hex.EncodeToString(digest[:]))
}

func daemonOwnsProcess(socket string, processID int) bool {
	pidPath := metadataPath(socket, ".pid")
	contents, err := os.ReadFile(pidPath)
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(contents)))
	return err == nil && pid == processID
}

func cleanupOwnedArtifacts(options Options, processID int) {
	base := options.Socket
	if options.TCPAddress != "" {
		base = tcpMetadataPath(options.TCPAddress)
	}
	if !daemonOwnsProcess(base, processID) {
		return
	}
	_ = os.Remove(metadataPath(base, ".pid"))
	_ = os.Remove(metadataPath(base, ".cap"))
	if options.TCPAddress == "" {
		if info, err := os.Lstat(options.Socket); err == nil && info.Mode()&os.ModeSocket != 0 {
			_ = os.Remove(options.Socket)
		}
	}
}

func metadataPath(base, extension string) string {
	return strings.TrimSuffix(filepath.Clean(base), filepath.Ext(base)) + extension
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
	if vaulticfs.ExtendedStat(info).UID != uint32(os.Geteuid()) {
		return fmt.Errorf("unsafe vaulticdb socket owner at %s", socket)
	}
	directory, err := os.Stat(filepath.Dir(socket))
	if err != nil {
		return err
	}
	if !directory.IsDir() || directory.Mode().Perm() != 0o700 {
		return fmt.Errorf("unsafe vaulticdb runtime directory permissions at %s", filepath.Dir(socket))
	}
	if vaulticfs.ExtendedStat(directory).UID != uint32(os.Geteuid()) {
		return fmt.Errorf("unsafe vaulticdb runtime directory owner at %s", filepath.Dir(socket))
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

func (c *authenticatedClient) Get(ctx context.Context, in *vaulticdbv1.GetRequest, opts ...grpc.CallOption) (*vaulticdbv1.GetResponse, error) {
	return c.VaulticDBClient.Get(withAuth(ctx, c.token), in, opts...)
}

func (c *authenticatedClient) MultiGet(ctx context.Context, in *vaulticdbv1.MultiGetRequest, opts ...grpc.CallOption) (*vaulticdbv1.MultiGetResponse, error) {
	return c.VaulticDBClient.MultiGet(withAuth(ctx, c.token), in, opts...)
}

func (c *authenticatedClient) Scan(ctx context.Context, in *vaulticdbv1.ScanRequest, opts ...grpc.CallOption) (*vaulticdbv1.ScanResponse, error) {
	return c.VaulticDBClient.Scan(withAuth(ctx, c.token), in, opts...)
}

func (c *authenticatedClient) WriteBatch(ctx context.Context, in *vaulticdbv1.WriteBatchRequest, opts ...grpc.CallOption) (*vaulticdbv1.WriteBatchResponse, error) {
	return c.VaulticDBClient.WriteBatch(withAuth(ctx, c.token), in, opts...)
}

func (c *authenticatedClient) Begin(ctx context.Context, in *vaulticdbv1.Empty, opts ...grpc.CallOption) (*vaulticdbv1.BeginResponse, error) {
	return c.VaulticDBClient.Begin(withAuth(ctx, c.token), in, opts...)
}

func (c *authenticatedClient) Commit(ctx context.Context, in *vaulticdbv1.TransactionRequest, opts ...grpc.CallOption) (*vaulticdbv1.CommitResponse, error) {
	return c.VaulticDBClient.Commit(withAuth(ctx, c.token), in, opts...)
}

func (c *authenticatedClient) Rollback(ctx context.Context, in *vaulticdbv1.TransactionRequest, opts ...grpc.CallOption) (*vaulticdbv1.Empty, error) {
	return c.VaulticDBClient.Rollback(withAuth(ctx, c.token), in, opts...)
}

func withAuth(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func retryDial(ctx context.Context, options Options) (*Client, error) {
	deadline := time.Now().Add(options.StartTimeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		attemptTimeout := dialAttemptTimeout(options)
		if remaining < attemptTimeout {
			attemptTimeout = remaining
		}
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, attemptTimeout)
		client, err := dial(attemptCtx, options)
		cancelAttempt()
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

func dialAttemptTimeout(options Options) time.Duration {
	if options.RetryInterval > 250*time.Millisecond {
		return options.RetryInterval
	}
	return 250 * time.Millisecond
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
	if capabilities.GetMaxBatchItems() == 0 || capabilities.GetMaxMessageBytes() == 0 || capabilities.GetMaxPageItems() == 0 {
		return fmt.Errorf("daemon advertised invalid storage limits")
	}
	c.limits = Limits{
		MaxBatchItems:   capabilities.GetMaxBatchItems(),
		MaxMessageBytes: capabilities.GetMaxMessageBytes(),
		MaxPageItems:    capabilities.GetMaxPageItems(),
	}
	return nil
}

func (c *Client) RPC() vaulticdbv1.VaulticDBClient { return c.rpc }

// Limits returns the storage limits validated during connection setup.
func (c *Client) Limits() Limits { return c.limits }

// Get reads one binary record, optionally through a transaction.
func (c *Client) Get(ctx context.Context, key []byte, transactionID string) ([]byte, bool, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.Get(ctx, &vaulticdbv1.GetRequest{
		Context: requestContext(ctx), TransactionId: transactionID, Key: key,
	})
	if err != nil {
		return nil, false, err
	}
	if !bytes.Equal(response.GetKey(), key) {
		return nil, false, fmt.Errorf("vaulticdb returned a point-read result for the wrong key")
	}
	return response.GetValue(), response.GetFound(), nil
}

// MultiGet reads records in request order, preserving missing entries.
func (c *Client) MultiGet(ctx context.Context, keys [][]byte, transactionID string) ([]KeyValue, []bool, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	if uint64(len(keys)) > uint64(c.limits.MaxBatchItems) {
		return nil, nil, fmt.Errorf("multi-get has %d items, limit is %d", len(keys), c.limits.MaxBatchItems)
	}
	request := &vaulticdbv1.MultiGetRequest{
		Context: requestContext(ctx), TransactionId: transactionID, Keys: keys,
	}
	if proto.Size(request) > int(c.limits.MaxMessageBytes) {
		return nil, nil, fmt.Errorf("multi-get request exceeds %d bytes", c.limits.MaxMessageBytes)
	}
	response, err := c.rpc.MultiGet(ctx, request)
	if err != nil {
		return nil, nil, err
	}
	if len(response.GetResults()) != len(keys) {
		return nil, nil, fmt.Errorf("vaulticdb returned %d multi-get results for %d keys", len(response.GetResults()), len(keys))
	}
	values := make([]KeyValue, len(keys))
	found := make([]bool, len(keys))
	for index, result := range response.GetResults() {
		if !bytes.Equal(result.GetKey(), keys[index]) {
			return nil, nil, fmt.Errorf("vaulticdb returned an out-of-order multi-get result at index %d", index)
		}
		values[index] = KeyValue{Key: result.GetKey(), Value: result.GetValue()}
		found[index] = result.GetFound()
	}
	return values, found, nil
}

// ScanPage reads one bounded page for a prefix. Pass the last returned key as
// afterKey to continue without repeating it.
func (c *Client) ScanPage(ctx context.Context, prefix, afterKey []byte, pageSize uint32, transactionID string) ([]KeyValue, bool, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	if pageSize == 0 || pageSize > c.limits.MaxPageItems {
		return nil, false, fmt.Errorf("scan page size %d is outside 1..%d", pageSize, c.limits.MaxPageItems)
	}
	response, err := c.rpc.Scan(ctx, &vaulticdbv1.ScanRequest{
		Context: requestContext(ctx), TransactionId: transactionID,
		Prefix: prefix, AfterKey: afterKey, PageSize: pageSize,
	})
	if err != nil {
		return nil, false, err
	}
	entries := make([]KeyValue, len(response.GetEntries()))
	previous := afterKey
	for index, entry := range response.GetEntries() {
		if !bytes.HasPrefix(entry.GetKey(), prefix) || (len(previous) > 0 && bytes.Compare(entry.GetKey(), previous) <= 0) {
			return nil, false, fmt.Errorf("vaulticdb returned an out-of-order scan page")
		}
		entries[index] = KeyValue{Key: entry.GetKey(), Value: entry.GetValue()}
		previous = entry.GetKey()
	}
	return entries, response.GetDone(), nil
}

// WriteBatch atomically applies a bounded set of puts and deletes.
func (c *Client) WriteBatch(ctx context.Context, puts []Mutation, deletes [][]byte, awaitDurable bool, transactionID string) (bool, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	if uint64(len(puts)+len(deletes)) > uint64(c.limits.MaxBatchItems) {
		return false, fmt.Errorf("write batch has %d items, limit is %d", len(puts)+len(deletes), c.limits.MaxBatchItems)
	}
	request := &vaulticdbv1.WriteBatchRequest{
		Context: requestContext(ctx), TransactionId: transactionID,
		Deletes: deletes, AwaitDurable: awaitDurable,
		Puts: make([]*vaulticdbv1.KeyValue, len(puts)),
	}
	for index, put := range puts {
		request.Puts[index] = &vaulticdbv1.KeyValue{Key: put.Key, Value: put.Value}
	}
	if proto.Size(request) > int(c.limits.MaxMessageBytes) {
		return false, fmt.Errorf("write batch exceeds %d bytes", c.limits.MaxMessageBytes)
	}
	response, err := c.rpc.WriteBatch(ctx, request)
	if err != nil {
		return false, err
	}
	return response.GetDurable(), nil
}

// Transaction is a serializable daemon transaction.
type Transaction struct {
	client *Client
	id     string
	state  atomic.Uint32
}

const (
	transactionOpen uint32 = iota
	transactionClosed
	transactionCommitUncertain
)

// Begin starts a serializable transaction.
func (c *Client) Begin(ctx context.Context) (*Transaction, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.Begin(ctx, &vaulticdbv1.Empty{Context: requestContext(ctx)})
	if err != nil {
		return nil, err
	}
	if response.GetTransactionId() == "" {
		return nil, fmt.Errorf("vaulticdb returned an empty transaction ID")
	}
	return &Transaction{client: c, id: response.GetTransactionId()}, nil
}

func (t *Transaction) ID() string { return t.id }

func (t *Transaction) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	return t.client.Get(ctx, key, t.id)
}

func (t *Transaction) MultiGet(ctx context.Context, keys [][]byte) ([]KeyValue, []bool, error) {
	return t.client.MultiGet(ctx, keys, t.id)
}

func (t *Transaction) ScanPage(ctx context.Context, prefix, afterKey []byte, pageSize uint32) ([]KeyValue, bool, error) {
	return t.client.ScanPage(ctx, prefix, afterKey, pageSize, t.id)
}

func (t *Transaction) WriteBatch(ctx context.Context, puts []Mutation, deletes [][]byte) error {
	_, err := t.client.WriteBatch(ctx, puts, deletes, false, t.id)
	return err
}

// Commit atomically publishes all mutations and waits for durability.
func (t *Transaction) Commit(ctx context.Context) error {
	if !t.state.CompareAndSwap(transactionOpen, transactionClosed) {
		return fmt.Errorf("transaction is already closed")
	}
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := t.client.rpc.Commit(ctx, &vaulticdbv1.TransactionRequest{
		Context: requestContext(ctx), TransactionId: t.id,
	})
	if err != nil {
		switch status.Code(err) {
		case codes.Aborted, codes.NotFound, codes.InvalidArgument, codes.FailedPrecondition:
		default:
			t.state.Store(transactionCommitUncertain)
		}
		return err
	}
	if !response.GetDurable() {
		return fmt.Errorf("vaulticdb committed transaction without durability acknowledgement")
	}
	return nil
}

// Rollback discards all transaction mutations.
func (t *Transaction) Rollback(ctx context.Context) error {
	previous := t.state.Load()
	for {
		if previous == transactionClosed {
			return fmt.Errorf("transaction is already closed")
		}
		if t.state.CompareAndSwap(previous, transactionClosed) {
			break
		}
		previous = t.state.Load()
	}
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	_, err := t.client.rpc.Rollback(ctx, &vaulticdbv1.TransactionRequest{
		Context: requestContext(ctx), TransactionId: t.id,
	})
	if err != nil {
		if previous == transactionCommitUncertain && status.Code(err) == codes.NotFound {
			return err
		}
		t.state.CompareAndSwap(transactionClosed, previous)
	}
	return err
}

// Close closes the RPC connection and shuts down only a daemon started by this client.
func (c *Client) Close(ctx context.Context) error {
	var result error
	shutdownCtx := ctx
	cancel := func() {}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		shutdownCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
	}
	defer cancel()
	if c.process != nil {
		_, err := c.rpc.Shutdown(shutdownCtx, &vaulticdbv1.Empty{Context: requestContext(shutdownCtx)})
		if err != nil {
			result = err
			if shutdownCtx.Err() != nil {
				result = shutdownCtx.Err()
			}
			_ = c.process.Process.Kill()
		}
	}
	if err := c.conn.Close(); err != nil && result == nil {
		result = err
	}
	if c.process != nil {
		wait := make(chan error, 1)
		go func() { wait <- c.process.Wait() }()
		select {
		case err := <-wait:
			if err != nil && result == nil {
				result = err
			}
		case <-shutdownCtx.Done():
			_ = c.process.Process.Kill()
			<-wait
			if result == nil {
				result = shutdownCtx.Err()
			}
		}
		c.process = nil
	}
	return result
}

func requestContext(ctx context.Context) *vaulticdbv1.RequestContext {
	deadline := time.Now().Add(defaultRPCDeadline)
	if value, ok := ctx.Deadline(); ok {
		deadline = value
	}
	return &vaulticdbv1.RequestContext{
		RequestId:      fmt.Sprintf("vaultic-%d", requestSequence.Add(1)),
		DeadlineUnixMs: deadline.UnixMilli(),
	}
}

func withDefaultRPCDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultRPCDeadline)
}

// SocketDir returns the private directory expected for an endpoint socket.
func SocketDir(socket string) string { return filepath.Dir(socket) }
