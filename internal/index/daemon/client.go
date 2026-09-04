package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	vaulticerrors "github.com/otuschhoff/vaultic/internal/errors"
	vaulticfs "github.com/otuschhoff/vaultic/internal/fs"
	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	ProtocolVersion    = "vaulticdb.v1"
	SchemaVersion      = "0"
	defaultRPCDeadline = 10 * time.Second
)

var requestSequence atomic.Uint64

// Options controls how a vaultic process connects to or starts vaulticdb.
type Options struct {
	Socket            string
	TCPAddress        string
	TCPAllowlist      []string
	AuthToken         string
	RepositoryID      string
	DaemonPath        string
	StartTimeout      time.Duration
	RetryInterval     time.Duration
	PersistentDaemon  bool
	ObjectStore       string
	DataDir           string
	S3Bucket          string
	S3Prefix          string
	EncryptionMode    string
	PassphraseFile    string
	AzureTokenFile    string
	GCPTokenFile      string
	VaultTokenFile    string
	PKCS11PINFile     string
	RecoveryUnlock    bool
	BrokerSocket      string
	BrokerManifest    string
	BrokerLease       time.Duration
	RebuildInitialize bool
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
	conn       *grpc.ClientConn
	rpc        vaulticdbv1.VaulticDBClient
	process    *exec.Cmd
	options    Options
	limits     Limits
	encryption EncryptionInfo
}

// EncryptionInfo describes the validated daemon metadata-encryption state.
type EncryptionInfo struct {
	Enabled            bool
	Algorithm          string
	ActiveDEKVersion   uint32
	EnvelopeGeneration uint64
	UnlockSlot         string
	RecoveryUnlock     bool
}

// KeySlotInfo is non-secret metadata describing one independent DEK wrapping.
type KeySlotInfo struct {
	ID           string `json:"id"`
	Provider     string `json:"provider"`
	Priority     uint32 `json:"priority"`
	Recovery     bool   `json:"recovery"`
	KeyReference string `json:"key_reference"`
	DEKVersion   uint32 `json:"dek_version"`
}

// KeyStatus is the redacted metadata-encryption key state.
type KeyStatus struct {
	EnvelopeGeneration              uint64        `json:"envelope_generation"`
	ActiveDEKVersion                uint32        `json:"active_dek_version"`
	Slots                           []KeySlotInfo `json:"slots"`
	PendingCapsuleMigrationSHA256   string        `json:"pending_capsule_migration_sha256,omitempty"`
	FinalizedCapsuleMigrationSHA256 string        `json:"finalized_capsule_migration_sha256,omitempty"`
}

// DEKRewriteProgress reports one bounded old-version rewrite batch.
type DEKRewriteProgress struct {
	Rewritten uint64 `json:"rewritten"`
	Remaining uint64 `json:"remaining"`
}

// EncryptionAudit reports raw metadata-object encryption consistency.
type EncryptionAudit struct {
	Enabled            bool   `json:"enabled"`
	Objects            uint64 `json:"objects"`
	InvalidObjects     uint64 `json:"invalid_objects"`
	PlaintextObjects   uint64 `json:"plaintext_objects"`
	OldVersionObjects  uint64 `json:"old_version_objects"`
	EnvelopeGeneration uint64 `json:"envelope_generation"`
	ActiveDEKVersion   uint32 `json:"active_dek_version"`
	Algorithm          string `json:"algorithm"`
}

// WriterStatus describes observable VaulticDB writer ownership without exposing protobuf types.
type WriterStatus struct {
	InstanceID          string `json:"instance_id"`
	Role                string `json:"role"`
	CurrentEpoch        uint64 `json:"current_epoch"`
	ObservedEpoch       uint64 `json:"observed_epoch"`
	TransitionReason    string `json:"transition_reason"`
	TransitionUnixMS    int64  `json:"transition_unix_ms"`
	ActiveWriteIntents  uint64 `json:"active_write_intents"`
	ActiveTransactions  uint64 `json:"active_transactions"`
	LastDurableSequence uint64 `json:"last_durable_sequence"`
	IdleDeadlineUnixMS  int64  `json:"idle_deadline_unix_ms"`
	PromotionSafe       bool   `json:"promotion_safe"`
}

type GenerationStatus struct {
	RepositoryID                  string `json:"repository_id"`
	Decision                      uint64 `json:"decision"`
	ActiveGeneration              uint64 `json:"active_generation"`
	Namespace                     string `json:"namespace"`
	PreviousGeneration            uint64 `json:"previous_generation"`
	PreviousNamespace             string `json:"previous_namespace"`
	State                         string `json:"state"`
	ReportSHA256                  string `json:"report_sha256"`
	DecidedAtUnixMS               int64  `json:"decided_at_unix_ms"`
	ObservationUntilUnixMS        int64  `json:"observation_until_unix_ms"`
	RetiredGeneration             uint64 `json:"retired_generation,omitempty"`
	DestructiveMaintenanceAllowed bool   `json:"destructive_maintenance_allowed"`
}

type durableIdempotencyRecord struct {
	Format        uint32 `json:"format"`
	Operation     string `json:"operation"`
	RequestSHA256 string `json:"request_sha256"`
	Durable       bool   `json:"durable"`
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
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return client, nil
}

// Ensure connects to a daemon or starts one when no compatible daemon exists.
// If multiple vaultic processes race, only the process that acquires the
// daemon's singleton lock successfully starts it; other callers retry attach.
func Ensure(ctx context.Context, options Options) (*Client, error) {
	options = options.withDefaults()
	if err := validateEnsureOptions(options); err != nil {
		return nil, err
	}
	client, err := connectExistingDaemon(ctx, options)
	if err != nil || client != nil {
		return client, err
	}
	if err := validateDaemonStartOptions(options); err != nil {
		return nil, err
	}

	cmd, authRead, authWrite, err := prepareDaemonCommand(options)
	if err != nil {
		return nil, err
	}
	client, err = startDaemon(ctx, options, cmd, authRead, authWrite)
	if err != nil || client != nil {
		return client, err
	}
	return awaitDaemonReady(ctx, options, cmd)
}

func validateEnsureOptions(options Options) error {
	if options.EncryptionMode != "" && options.EncryptionMode != "off" && options.EncryptionMode != "required" && options.EncryptionMode != "initialize" {
		return fmt.Errorf("%w: unsupported metadata encryption mode %q", ErrUnavailable, options.EncryptionMode)
	}
	if options.RebuildInitialize && (options.BrokerSocket == "" || options.EncryptionMode != "required") {
		return fmt.Errorf("%w: metadata rebuild initialization requires brokered required encryption", ErrUnavailable)
	}
	if (options.EncryptionMode == "required" || options.EncryptionMode == "initialize") && options.PassphraseFile == "" && options.BrokerSocket == "" {
		return fmt.Errorf("%w: metadata recovery passphrase file is required", ErrUnavailable)
	}
	if options.BrokerSocket != "" && options.BrokerManifest == "" {
		return fmt.Errorf("%w: broker release manifest is required", ErrUnavailable)
	}
	if options.RecoveryUnlock && options.PassphraseFile == "" {
		return fmt.Errorf("%w: recovery unlock requires a passphrase file", ErrUnavailable)
	}
	if err := validateProtectedFiles(options); err != nil {
		return err
	}
	if options.ObjectStore == "s3" && options.S3Bucket == "" {
		return fmt.Errorf("%w: S3 bucket is not configured", ErrUnavailable)
	}
	if options.ObjectStore != "" && options.ObjectStore != "local" && options.ObjectStore != "memory" && options.ObjectStore != "s3" {
		return fmt.Errorf("%w: unsupported object store %q", ErrUnavailable, options.ObjectStore)
	}
	for _, network := range options.TCPAllowlist {
		if _, _, err := net.ParseCIDR(network); err != nil {
			return fmt.Errorf("%w: invalid TCP allowlist entry %q: %w", ErrUnavailable, network, err)
		}
	}
	return nil
}

func validateProtectedFiles(options Options) error {
	for description, path := range map[string]string{
		"metadata recovery passphrase": options.PassphraseFile,
		"Azure KMS token":              options.AzureTokenFile,
		"Google Cloud KMS token":       options.GCPTokenFile,
		"Vault Transit token":          options.VaultTokenFile,
		"PKCS#11 PIN":                  options.PKCS11PINFile,
	} {
		if path != "" {
			if err := validateProtectedFile(path, description); err != nil {
				return fmt.Errorf("%w: %w", ErrUnavailable, err)
			}
		}
	}
	return nil
}

func connectExistingDaemon(ctx context.Context, options Options) (*Client, error) {
	probeCtx, cancelProbe := context.WithTimeout(ctx, dialAttemptTimeout(options))
	client, connectErr := Connect(probeCtx, options)
	cancelProbe()
	if connectErr == nil {
		if options.RebuildInitialize {
			vaulticerrors.LogCleanup("close existing vaulticdb client", func() error { return client.Close(ctx) }, log.Printf)
			return nil, fmt.Errorf("%w: metadata rebuild initialization refuses an existing daemon", ErrUnavailable)
		}
		return client, nil
	}
	return nil, nil
}

func validateDaemonStartOptions(options Options) error {
	if options.DaemonPath == "" {
		return fmt.Errorf("%w: daemon path is not configured", ErrUnavailable)
	}
	if options.TCPAddress != "" && len(options.TCPAllowlist) == 0 {
		return fmt.Errorf("%w: TCP allowlist is not configured", ErrUnavailable)
	}
	if options.TCPAddress != "" && options.AuthToken == "" {
		return fmt.Errorf("%w: TCP authentication token is not configured", ErrUnavailable)
	}
	return nil
}

func prepareDaemonCommand(options Options) (*exec.Cmd, *os.File, *os.File, error) {
	cmd := exec.Command(options.DaemonPath)
	var daemonErrors bytes.Buffer
	cmd.Stderr = &limitedWriter{writer: &daemonErrors, remaining: 64 * 1024}
	cmd.Env = append(daemonEnvironment(options),
		"VAULTICDB_SOCKET="+options.Socket,
		"VAULTICDB_REPOSITORY_ID="+options.RepositoryID,
	)
	for name, value := range map[string]string{
		"VAULTICDB_OBJECT_STORE":               options.ObjectStore,
		"VAULTICDB_DATA_DIR":                   options.DataDir,
		"VAULTICDB_S3_BUCKET":                  options.S3Bucket,
		"VAULTICDB_S3_PREFIX":                  options.S3Prefix,
		"VAULTICDB_ENCRYPTION":                 options.EncryptionMode,
		"VAULTICDB_ENCRYPTION_PASSPHRASE_FILE": options.PassphraseFile,
		"VAULTICDB_AZURE_TOKEN_FILE":           options.AzureTokenFile,
		"VAULTICDB_GCP_TOKEN_FILE":             options.GCPTokenFile,
		"VAULTICDB_VAULT_TOKEN_FILE":           options.VaultTokenFile,
		"VAULTICDB_PKCS11_PIN_FILE":            options.PKCS11PINFile,
		"VAULTICDB_BROKER_SOCKET":              options.BrokerSocket,
		"VAULTICDB_RELEASE_MANIFEST":           options.BrokerManifest,
	} {
		if value != "" {
			cmd.Env = append(cmd.Env, name+"="+value)
		}
	}
	if options.RecoveryUnlock {
		cmd.Env = append(cmd.Env, "VAULTICDB_ENCRYPTION_RECOVERY_ACK=true")
	}
	if options.BrokerLease > 0 {
		cmd.Env = append(cmd.Env, "VAULTICDB_BROKER_LEASE_SECONDS="+strconv.FormatUint(uint64(options.BrokerLease/time.Second), 10))
	}
	if options.RebuildInitialize {
		cmd.Env = append(cmd.Env, "VAULTICDB_METADATA_REBUILD_INITIALIZE=true")
	}
	var authRead, authWrite *os.File
	if options.AuthToken != "" {
		var pipeErr error
		authRead, authWrite, pipeErr = os.Pipe()
		if pipeErr != nil {
			return nil, nil, nil, fmt.Errorf("create vaulticdb authentication pipe: %w", pipeErr)
		}
		cmd.ExtraFiles = append(cmd.ExtraFiles, authRead)
		cmd.Env = append(cmd.Env, "VAULTICDB_TCP_AUTH_TOKEN_FD="+strconv.Itoa(3+len(cmd.ExtraFiles)-1))
	}
	if options.TCPAddress != "" {
		cmd.Env = append(cmd.Env,
			"VAULTICDB_TRANSPORT=tcp",
			"VAULTICDB_TCP_ADDR="+options.TCPAddress,
			"VAULTICDB_TCP_ALLOWLIST="+strings.Join(options.TCPAllowlist, ","),
			"VAULTICDB_TCP_METADATA="+tcpMetadataPath(options.TCPAddress),
		)
	}
	return cmd, authRead, authWrite, nil
}

func startDaemon(
	ctx context.Context,
	options Options,
	cmd *exec.Cmd,
	authRead, authWrite *os.File,
) (*Client, error) {
	if err := cmd.Start(); err != nil {
		if authRead != nil {
			vaulticerrors.LogClose(authRead, "close vaulticdb authentication reader", log.Printf)
			vaulticerrors.LogClose(authWrite, "close vaulticdb authentication writer", log.Printf)
		}
		// Another caller may have started the singleton between our first dial
		// and this start attempt. Retry attach before returning the start error.
		if client, dialErr := retryDial(ctx, options); dialErr == nil {
			return client, nil
		}
		return nil, fmt.Errorf("start vaulticdb: %w", err)
	}
	if authRead != nil {
		vaulticerrors.CloseQuietly(authRead)
		_, writeErr := io.WriteString(authWrite, options.AuthToken)
		closeErr := authWrite.Close()
		if writeErr != nil || closeErr != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return nil, fmt.Errorf("send vaulticdb authentication token: %w", errors.Join(writeErr, closeErr))
		}
	}
	return nil, nil
}

func awaitDaemonReady(ctx context.Context, options Options, cmd *exec.Cmd) (*Client, error) {
	client, err := retryDial(ctx, options)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		cleanupOwnedArtifacts(options, cmd.Process.Pid)
		if options.EncryptionMode == "required" || options.EncryptionMode == "initialize" {
			_ = observability.Emit(ctx, observability.Event{
				Severity: observability.Warning, Category: observability.CategoryAuth,
				Component: "vaulticdb", Message: "encrypted metadata daemon failed during startup",
				Fields: map[string]any{"repository_id": options.RepositoryID},
			})
		}
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

type limitedWriter struct {
	writer    io.Writer
	remaining int
}

func (writer *limitedWriter) Write(data []byte) (int, error) {
	length := len(data)
	if writer.remaining == 0 {
		return length, nil
	}
	if len(data) > writer.remaining {
		data = data[:writer.remaining]
	}
	written, err := writer.writer.Write(data)
	writer.remaining -= written
	if err != nil {
		return written, err
	}
	return length, nil
}

func validateProtectedFile(path, description string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect %s file: %w", description, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s file is not a regular file", description)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%s file must not be accessible by group or others", description)
	}
	return nil
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
	vaulticerrors.LogCleanup("remove vaulticdb PID file", func() error { return os.Remove(metadataPath(base, ".pid")) }, log.Printf)
	vaulticerrors.LogCleanup("remove vaulticdb capability file", func() error { return os.Remove(metadataPath(base, ".cap")) }, log.Printf)
	if options.TCPAddress == "" {
		if info, err := os.Lstat(options.Socket); err == nil && info.Mode()&os.ModeSocket != 0 {
			vaulticerrors.LogCleanup("remove vaulticdb socket", func() error { return os.Remove(options.Socket) }, log.Printf)
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
		grpc.WithUnaryInterceptor(classifyUnaryClientError),
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
		vaulticerrors.CloseQuietly(conn)
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
	c.encryption = EncryptionInfo{
		Enabled: capabilities.GetEncryptionEnabled(), Algorithm: capabilities.GetEncryptionAlgorithm(),
		ActiveDEKVersion: capabilities.GetActiveDekVersion(), EnvelopeGeneration: capabilities.GetEnvelopeGeneration(),
		UnlockSlot: capabilities.GetUnlockSlot(), RecoveryUnlock: capabilities.GetRecoveryUnlock(),
	}
	if (c.options.EncryptionMode == "required" || c.options.EncryptionMode == "initialize") && !c.encryption.Enabled {
		return fmt.Errorf("daemon did not enable required metadata encryption")
	}
	if c.encryption.Enabled {
		c.auditEncryptionUnlock(ctx)
	}
	return nil
}

func (c *Client) auditEncryptionUnlock(ctx context.Context) {
	severity := observability.Notice
	if c.encryption.RecoveryUnlock {
		severity = observability.Warning
	}
	_ = observability.Emit(ctx, observability.Event{Severity: severity, Category: observability.CategoryAuth, Component: "vaulticdb", Message: "metadata key slot unlocked", Fields: map[string]any{"repository_id": c.options.RepositoryID, "slot_id": c.encryption.UnlockSlot, "dek_version": c.encryption.ActiveDEKVersion, "envelope_generation": c.encryption.EnvelopeGeneration, "recovery": c.encryption.RecoveryUnlock}})
}

func (c *Client) RPC() vaulticdbv1.VaulticDBClient { return c.rpc }

// Limits returns the storage limits validated during connection setup.
func (c *Client) Limits() Limits { return c.limits }

// Encryption returns the daemon's validated metadata-encryption state.
func (c *Client) Encryption() EncryptionInfo { return c.encryption }

// WriterStatus returns the daemon's current writer ownership and quiescence state.
func (c *Client) WriterStatus(ctx context.Context) (WriterStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.WriterStatus(ctx, &vaulticdbv1.WriterStatusRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx)})
	if err != nil {
		return WriterStatus{}, err
	}
	return writerStatus(response), nil
}

func (c *Client) GenerationStatus(ctx context.Context) (GenerationStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.GenerationStatus(ctx, &vaulticdbv1.GenerationStatusRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx)})
	if err != nil {
		return GenerationStatus{}, err
	}
	return generationStatus(response), nil
}

func (c *Client) ActivateGeneration(ctx context.Context, expected, candidate uint64, namespace, reportSHA256 string, observationWindow time.Duration) (GenerationStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.ActivateGeneration(ctx, &vaulticdbv1.ActivateGenerationRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), ExpectedActiveGeneration: expected, CandidateGeneration: candidate, CandidateNamespace: namespace, ReportSha256: reportSHA256, ObservationWindowMs: uint64(observationWindow.Milliseconds()), Approve: true})
	if err != nil {
		return GenerationStatus{}, err
	}
	return generationStatus(response), nil
}

func (c *Client) QuarantineGeneration(ctx context.Context, expectedGeneration uint64, diagnosticSHA256 string) (GenerationStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.QuarantineGeneration(ctx, &vaulticdbv1.QuarantineGenerationRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), ExpectedActiveGeneration: expectedGeneration, DiagnosticSha256: diagnosticSHA256, HealingRequired: true})
	if err != nil {
		return GenerationStatus{}, err
	}
	return generationStatus(response), nil
}

func (c *Client) VerifyGeneration(ctx context.Context, expectedDecision uint64, reportSHA256 string) (GenerationStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.VerifyGeneration(ctx, &vaulticdbv1.VerifyGenerationRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), ExpectedDecision: expectedDecision, ReportSha256: reportSHA256, PostActivationCheckClean: true})
	if err != nil {
		return GenerationStatus{}, err
	}
	return generationStatus(response), nil
}

func (c *Client) RollbackGeneration(ctx context.Context, expectedDecision uint64, reportSHA256 string, observationWindow time.Duration) (GenerationStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.RollbackGeneration(ctx, &vaulticdbv1.RollbackGenerationRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), ExpectedDecision: expectedDecision, ReportSha256: reportSHA256, ObservationWindowMs: uint64(observationWindow.Milliseconds()), Acknowledge: true})
	if err != nil {
		return GenerationStatus{}, err
	}
	return generationStatus(response), nil
}

func (c *Client) RetireGeneration(ctx context.Context, expectedDecision, generation uint64, reportSHA256 string) (GenerationStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.RetireGeneration(ctx, &vaulticdbv1.RetireGenerationRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), ExpectedDecision: expectedDecision, Generation: generation, ReportSha256: reportSHA256, Acknowledge: true})
	if err != nil {
		return GenerationStatus{}, err
	}
	return generationStatus(response), nil
}

func generationStatus(response *vaulticdbv1.GenerationStatusResponse) GenerationStatus {
	return GenerationStatus{RepositoryID: response.GetRepositoryId(), Decision: response.GetDecision(), ActiveGeneration: response.GetActiveGeneration(), Namespace: response.GetNamespace(), PreviousGeneration: response.GetPreviousGeneration(), PreviousNamespace: response.GetPreviousNamespace(), State: response.GetState(), ReportSHA256: response.GetReportSha256(), DecidedAtUnixMS: response.GetDecidedAtUnixMs(), ObservationUntilUnixMS: response.GetObservationUntilUnixMs(), RetiredGeneration: response.GetRetiredGeneration(), DestructiveMaintenanceAllowed: response.GetDestructiveMaintenanceAllowed()}
}

// IdempotencyCommitted reports whether a durable transaction commit exists for key.
func (c *Client) IdempotencyCommitted(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("idempotency key is required")
	}
	value, found, err := c.Get(ctx, append([]byte("meta:idempotency:"), key...), "")
	if err != nil || !found {
		return false, err
	}
	var record durableIdempotencyRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return false, fmt.Errorf("decode durable idempotency record: %w", err)
	}
	if record.Format != 1 || record.Operation != "transaction-commit" || record.RequestSHA256 == "" || !record.Durable {
		return false, fmt.Errorf("invalid durable transaction idempotency record")
	}
	return true, nil
}

// DemoteWriter quiesces and relinquishes the daemon's writable SlateDB handle.
func (c *Client) DemoteWriter(ctx context.Context, reason string, force bool, timeout time.Duration) (WriterStatus, error) {
	if timeout <= 0 || timeout > 5*time.Minute {
		return WriterStatus{}, fmt.Errorf("writer demotion timeout must be between 1ns and 5m")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout+defaultRPCDeadline)
	defer cancel()
	response, err := c.rpc.DemoteWriter(ctx, &vaulticdbv1.DemoteWriterRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), Force: force, TimeoutMs: uint64(timeout.Milliseconds()), Reason: reason})
	if err != nil {
		return WriterStatus{}, err
	}
	return writerStatus(response), nil
}

// PromoteWriter requests a freshly fenced writable SlateDB handle.
func (c *Client) PromoteWriter(ctx context.Context, reason string) (WriterStatus, error) {
	return c.PromoteWriterWithTakeover(ctx, reason, false, 0)
}

// PromoteWriterWithTakeover conditionally replaces the exact crashed-writer epoch acknowledged by an operator.
func (c *Client) PromoteWriterWithTakeover(ctx context.Context, reason string, forceTakeover bool, expectedActiveEpoch uint64) (WriterStatus, error) {
	if forceTakeover && expectedActiveEpoch == 0 {
		return WriterStatus{}, fmt.Errorf("writer takeover requires the expected active epoch")
	}
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.PromoteWriter(ctx, &vaulticdbv1.PromoteWriterRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), Reason: reason, ForceTakeover: forceTakeover, ExpectedActiveEpoch: expectedActiveEpoch})
	if err != nil {
		return WriterStatus{}, err
	}
	return writerStatus(response), nil
}

func writerStatus(response *vaulticdbv1.WriterStatusResponse) WriterStatus {
	role := strings.ToLower(strings.TrimPrefix(response.GetRole().String(), "WRITER_ROLE_"))
	role = strings.ReplaceAll(role, "_", "-")
	return WriterStatus{
		InstanceID: response.GetInstanceId(), Role: role, CurrentEpoch: response.GetCurrentEpoch(), ObservedEpoch: response.GetObservedEpoch(),
		TransitionReason: response.GetTransitionReason(), TransitionUnixMS: response.GetTransitionUnixMs(), ActiveWriteIntents: response.GetActiveWriteIntents(),
		ActiveTransactions: response.GetActiveTransactions(), LastDurableSequence: response.GetLastDurableSequence(), IdleDeadlineUnixMS: response.GetIdleDeadlineUnixMs(), PromotionSafe: response.GetPromotionSafe(),
	}
}
