package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	vaulticfs "github.com/otuschhoff/vaultic/internal/fs"
	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/observability"
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
	EnvelopeGeneration uint64        `json:"envelope_generation"`
	ActiveDEKVersion   uint32        `json:"active_dek_version"`
	Slots              []KeySlotInfo `json:"slots"`
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
	if options.EncryptionMode != "" && options.EncryptionMode != "off" && options.EncryptionMode != "required" && options.EncryptionMode != "initialize" {
		return nil, fmt.Errorf("%w: unsupported metadata encryption mode %q", ErrUnavailable, options.EncryptionMode)
	}
	if options.RebuildInitialize && (options.BrokerSocket == "" || options.EncryptionMode != "required") {
		return nil, fmt.Errorf("%w: metadata rebuild initialization requires brokered required encryption", ErrUnavailable)
	}
	if (options.EncryptionMode == "required" || options.EncryptionMode == "initialize") && options.PassphraseFile == "" && options.BrokerSocket == "" {
		return nil, fmt.Errorf("%w: metadata recovery passphrase file is required", ErrUnavailable)
	}
	if options.BrokerSocket != "" && options.BrokerManifest == "" {
		return nil, fmt.Errorf("%w: broker release manifest is required", ErrUnavailable)
	}
	if options.RecoveryUnlock && options.PassphraseFile == "" {
		return nil, fmt.Errorf("%w: recovery unlock requires a passphrase file", ErrUnavailable)
	}
	for description, path := range map[string]string{
		"metadata recovery passphrase": options.PassphraseFile,
		"Azure KMS token":              options.AzureTokenFile,
		"Google Cloud KMS token":       options.GCPTokenFile,
		"Vault Transit token":          options.VaultTokenFile,
		"PKCS#11 PIN":                  options.PKCS11PINFile,
	} {
		if path != "" {
			if err := validateProtectedFile(path, description); err != nil {
				return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
			}
		}
	}
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
		if options.RebuildInitialize {
			_ = client.Close(ctx)
			return nil, fmt.Errorf("%w: metadata rebuild initialization refuses an existing daemon", ErrUnavailable)
		}
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
	var daemonErrors bytes.Buffer
	cmd.Stderr = &limitedWriter{writer: &daemonErrors, remaining: 64 * 1024}
	cmd.Env = append(os.Environ(),
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
		if strings.Contains(daemonErrors.String(), "no metadata key slot could be unwrapped") {
			_ = observability.Emit(ctx, observability.Event{Severity: observability.Warning, Category: observability.CategoryAuth, Component: "vaulticdb", Message: "metadata key unwrap failed", Fields: map[string]any{"repository_id": options.RepositoryID}})
			return nil, fmt.Errorf("%w: no metadata key slot could be unwrapped", ErrUnavailable)
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

// GetMasterKey returns the repository master key held by encrypted metadata.
func (c *Client) GetMasterKey(ctx context.Context) ([]byte, bool, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.GetMasterKey(ctx, &vaulticdbv1.MasterKeyRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx)})
	if err != nil {
		return nil, false, err
	}
	return response.GetMasterKey(), response.GetFound(), nil
}

// StoreMasterKey immutably stores a repository master key inside encrypted metadata.
func (c *Client) StoreMasterKey(ctx context.Context, masterKey []byte) error {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	_, err := c.rpc.StoreMasterKey(ctx, &vaulticdbv1.StoreMasterKeyRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), MasterKey: masterKey})
	return err
}

type OfflineCapsuleMember struct {
	ID         string
	Provider   string
	Credential []byte
}

type CapsuleMigration struct {
	Generation    uint64
	LocalPath     string
	MirrorPath    string
	CapsuleSHA256 string
	Capsule       []byte
}

func (c *Client) PrepareCapsuleMigration(ctx context.Context, capsuleDirectory string, generation uint64, groupID string, threshold uint32, brokerIdentityPublicKey []byte, members []OfflineCapsuleMember) (CapsuleMigration, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	requestMembers := make([]*vaulticdbv1.OfflineCapsuleMember, len(members))
	for index, member := range members {
		requestMembers[index] = &vaulticdbv1.OfflineCapsuleMember{MemberId: member.ID, Provider: member.Provider, Credential: append([]byte(nil), member.Credential...)}
		defer func(value []byte) {
			for index := range value {
				value[index] = 0
			}
		}(requestMembers[index].Credential)
	}
	response, err := c.rpc.PrepareCapsuleMigration(ctx, &vaulticdbv1.PrepareCapsuleMigrationRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), CapsuleDirectory: capsuleDirectory, Generation: generation, GroupId: groupID, Threshold: threshold, BrokerIdentityPublicKey: brokerIdentityPublicKey, Members: requestMembers})
	if err != nil {
		return CapsuleMigration{}, err
	}
	_ = observability.Emit(ctx, observability.Event{Severity: observability.Warning, Category: observability.CategoryLifecycle, Component: "vaulticdb", Message: "recovery capsule migration prepared", Fields: map[string]any{"repository_id": c.options.RepositoryID, "generation": response.GetGeneration(), "capsule_sha256": response.GetCapsuleSha256()}})
	return CapsuleMigration{Generation: response.GetGeneration(), LocalPath: response.GetLocalPath(), MirrorPath: response.GetMirrorPath(), CapsuleSHA256: response.GetCapsuleSha256(), Capsule: response.GetCapsule()}, nil
}

func (c *Client) FinalizeCapsuleMigration(ctx context.Context, capsuleSHA256 string, brokerKeyProof []byte) error {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	_, err := c.rpc.FinalizeCapsuleMigration(ctx, &vaulticdbv1.FinalizeCapsuleMigrationRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), CapsuleSha256: capsuleSHA256, BrokerKeyProof: brokerKeyProof})
	if err == nil {
		_ = observability.Emit(ctx, observability.Event{Severity: observability.Warning, Category: observability.CategoryLifecycle, Component: "vaulticdb", Message: "recovery capsule migration finalized and database master key removed", Fields: map[string]any{"repository_id": c.options.RepositoryID, "capsule_sha256": capsuleSHA256}})
	}
	return err
}

// KeyStatus returns redacted key-envelope metadata.
func (c *Client) KeyStatus(ctx context.Context) (KeyStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.KeyStatus(ctx, &vaulticdbv1.KeyStatusRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx)})
	if err != nil {
		return KeyStatus{}, err
	}
	return keyStatus(response), nil
}

// ExportKeyEnvelope returns the current wrapped-key envelope for repository mirroring.
func (c *Client) ExportKeyEnvelope(ctx context.Context) ([]byte, uint64, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.ExportKeyEnvelope(ctx, &vaulticdbv1.KeyStatusRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx)})
	if err != nil {
		return nil, 0, err
	}
	return response.GetEnvelope(), response.GetGeneration(), nil
}

// CheckEncryption validates every raw metadata object header and DEK version.
func (c *Client) CheckEncryption(ctx context.Context) (EncryptionAudit, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.CheckEncryption(ctx, &vaulticdbv1.KeyStatusRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx)})
	if err != nil {
		return EncryptionAudit{}, err
	}
	return EncryptionAudit{
		Enabled:            response.GetEnabled(),
		Objects:            response.GetObjects(),
		InvalidObjects:     response.GetInvalidObjects(),
		PlaintextObjects:   response.GetPlaintextObjects(),
		OldVersionObjects:  response.GetOldVersionObjects(),
		EnvelopeGeneration: response.GetEnvelopeGeneration(),
		ActiveDEKVersion:   response.GetActiveDekVersion(),
		Algorithm:          response.GetAlgorithm(),
	}, nil
}

// AddLocalKeySlot adds an independently wrapped Argon2id slot.
func (c *Client) AddLocalKeySlot(ctx context.Context, slotID string, passphrase []byte, priority uint32, recovery bool) (KeyStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.AddLocalKeySlot(ctx, &vaulticdbv1.AddLocalKeySlotRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), SlotId: slotID, Passphrase: passphrase, Priority: priority, Recovery: recovery})
	if err != nil {
		return KeyStatus{}, err
	}
	_ = observability.Emit(ctx, observability.Event{Severity: observability.Notice, Category: observability.CategoryLifecycle, Component: "vaulticdb", Message: "metadata key slot added", Fields: map[string]any{"slot_id": slotID, "provider": "local-argon2id", "envelope_generation": response.GetEnvelopeGeneration()}})
	return keyStatus(response), nil
}

// AddCloudKeySlot wraps the metadata DEK using a cloud KMS provider.
func (c *Client) AddCloudKeySlot(ctx context.Context, slotID, provider, keyReference string, bearerToken []byte, priority uint32) (KeyStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.AddCloudKeySlot(ctx, &vaulticdbv1.AddCloudKeySlotRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), SlotId: slotID, Provider: provider, KeyReference: keyReference, BearerToken: bearerToken, Priority: priority})
	if err != nil {
		return KeyStatus{}, err
	}
	_ = observability.Emit(ctx, observability.Event{Severity: observability.Notice, Category: observability.CategoryLifecycle, Component: "vaulticdb", Message: "metadata cloud key slot added", Fields: map[string]any{"slot_id": slotID, "provider": provider, "key_reference": keyReference, "envelope_generation": response.GetEnvelopeGeneration()}})
	return keyStatus(response), nil
}

// RemoveKeySlot removes a wrapping slot while retaining at least one slot.
func (c *Client) RemoveKeySlot(ctx context.Context, slotID string) (KeyStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.RemoveKeySlot(ctx, &vaulticdbv1.RemoveKeySlotRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), SlotId: slotID})
	if err != nil {
		return KeyStatus{}, err
	}
	_ = observability.Emit(ctx, observability.Event{Severity: observability.Warning, Category: observability.CategoryLifecycle, Component: "vaulticdb", Message: "metadata key slot removed", Fields: map[string]any{"slot_id": slotID, "envelope_generation": response.GetEnvelopeGeneration()}})
	return keyStatus(response), nil
}

// RotateLocalKeySlot rewraps the unchanged DEK under a new passphrase-derived KEK.
func (c *Client) RotateLocalKeySlot(ctx context.Context, slotID string, passphrase []byte) (KeyStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.RotateLocalKeySlot(ctx, &vaulticdbv1.RotateLocalKeySlotRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), SlotId: slotID, Passphrase: passphrase})
	if err != nil {
		return KeyStatus{}, err
	}
	_ = observability.Emit(ctx, observability.Event{Severity: observability.Notice, Category: observability.CategoryAuth, Component: "vaulticdb", Message: "metadata KEK rotated", Fields: map[string]any{"slot_id": slotID, "envelope_generation": response.GetEnvelopeGeneration()}})
	return keyStatus(response), nil
}

// RotateDEK publishes a new metadata DEK version and switches new writes to it.
func (c *Client) RotateDEK(ctx context.Context) (KeyStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.RotateDek(ctx, &vaulticdbv1.RotateDekRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx)})
	if err != nil {
		return KeyStatus{}, err
	}
	_ = observability.Emit(ctx, observability.Event{Severity: observability.Notice, Category: observability.CategoryLifecycle, Component: "vaulticdb", Message: "metadata DEK rotated", Fields: map[string]any{"dek_version": response.GetActiveDekVersion(), "envelope_generation": response.GetEnvelopeGeneration()}})
	return keyStatus(response), nil
}

// RewriteDEK rewrites at most maxObjects encrypted objects under the active DEK.
func (c *Client) RewriteDEK(ctx context.Context, maxObjects uint32) (DEKRewriteProgress, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.RewriteDek(ctx, &vaulticdbv1.RewriteDekRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), MaxObjects: maxObjects})
	if err != nil {
		return DEKRewriteProgress{}, err
	}
	return DEKRewriteProgress{Rewritten: response.GetRewritten(), Remaining: response.GetRemaining()}, nil
}

// EscrowMasterKey wraps the in-DB repository master key with a cloud provider.
func (c *Client) EscrowMasterKey(ctx context.Context, escrowID, provider, keyReference string, bearerToken []byte) ([]byte, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.EscrowMasterKey(ctx, &vaulticdbv1.EscrowMasterKeyRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), EscrowId: escrowID, Provider: provider, KeyReference: keyReference, BearerToken: bearerToken})
	if err != nil {
		return nil, err
	}
	_ = observability.Emit(ctx, observability.Event{Severity: observability.Notice, Category: observability.CategoryAuth, Component: "vaulticdb", Message: "repository master key escrowed", Fields: map[string]any{"escrow_id": escrowID, "provider": provider, "key_reference": keyReference}})
	return response.GetRecord(), nil
}

// RecoverEscrow unwraps a standalone escrow record without metadata DB access.
func (c *Client) RecoverEscrow(ctx context.Context, record, bearerToken []byte) ([]byte, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.RecoverEscrow(ctx, &vaulticdbv1.RecoverEscrowRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), Record: record, BearerToken: bearerToken})
	if err != nil {
		return nil, err
	}
	_ = observability.Emit(ctx, observability.Event{Severity: observability.Warning, Category: observability.CategoryAuth, Component: "vaulticdb", Message: "repository master key recovered from escrow", Fields: map[string]any{"repository_id": c.options.RepositoryID}})
	return response.GetMasterKey(), nil
}

func keyStatus(response *vaulticdbv1.KeyStatusResponse) KeyStatus {
	result := KeyStatus{EnvelopeGeneration: response.GetEnvelopeGeneration(), ActiveDEKVersion: response.GetActiveDekVersion(), Slots: make([]KeySlotInfo, len(response.GetSlots()))}
	for index, slot := range response.GetSlots() {
		result.Slots[index] = KeySlotInfo{ID: slot.GetId(), Provider: slot.GetProvider(), Priority: slot.GetPriority(), Recovery: slot.GetRecovery(), KeyReference: slot.GetKeyReference(), DEKVersion: slot.GetDekVersion()}
	}
	return result
}

// Get reads one binary record, optionally through a transaction.
func (c *Client) Get(ctx context.Context, key []byte, transactionID string) ([]byte, bool, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.Get(ctx, &vaulticdbv1.GetRequest{
		Context: requestContext(ctx), TransactionId: transactionID, Key: key,
	})
	if err != nil {
		c.auditRPCError(ctx, "get", err)
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
		c.auditRPCError(ctx, "multi_get", err)
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
		c.auditRPCError(ctx, "scan", err)
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
		c.auditRPCError(ctx, "write_batch", err)
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
		c.auditRPCError(ctx, "begin", err)
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
		t.client.auditRPCError(ctx, "commit", err)
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
		t.client.auditRPCError(ctx, "rollback", err)
		if previous == transactionCommitUncertain && status.Code(err) == codes.NotFound {
			return err
		}
		t.state.CompareAndSwap(transactionClosed, previous)
	}
	return err
}

func (c *Client) auditRPCError(ctx context.Context, operation string, err error) {
	if status.Code(err) != codes.DataLoss {
		return
	}
	_ = observability.Emit(ctx, observability.Event{Severity: observability.Critical, Category: observability.CategoryIntegrity, Component: "vaulticdb", Message: "encrypted metadata authentication failed", Fields: map[string]any{"repository_id": c.options.RepositoryID, "operation": operation}})
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
