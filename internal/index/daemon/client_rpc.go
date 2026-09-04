package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	vaulticerrors "github.com/otuschhoff/vaultic/internal/errors"
	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"github.com/otuschhoff/vaultic/internal/observability"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

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
	_, err := c.rpc.StoreMasterKey(
		ctx,
		&vaulticdbv1.StoreMasterKeyRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), MasterKey: masterKey},
	)
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

type PublishedCapsuleMutation struct {
	Generation    uint64 `json:"generation"`
	LocalPath     string `json:"local_path"`
	MirrorPath    string `json:"mirror_path"`
	CapsuleSHA256 string `json:"capsule_sha256"`
}

func (c *Client) PublishCapsuleMutation(
	ctx context.Context,
	capsuleDirectory string,
	capsule []byte,
	capsuleSHA256 string,
	identityRecovery bool,
) (PublishedCapsuleMutation, error) {
	if capsuleDirectory == "" || len(capsule) == 0 || len(capsuleSHA256) != 64 {
		return PublishedCapsuleMutation{}, errors.New("capsule directory, capsule, and SHA-256 digest are required")
	}
	response, err := c.rpc.PublishCapsuleMutation(
		ctx,
		&vaulticdbv1.PublishCapsuleMutationRequest{
			RepositoryId:     c.options.RepositoryID,
			Context:          requestContext(ctx),
			CapsuleDirectory: capsuleDirectory,
			Capsule:          capsule,
			CapsuleSha256:    capsuleSHA256,
			IdentityRecovery: identityRecovery,
		},
	)
	if err != nil {
		return PublishedCapsuleMutation{}, err
	}
	return PublishedCapsuleMutation{
		Generation:    response.GetGeneration(),
		LocalPath:     response.GetLocalPath(),
		MirrorPath:    response.GetMirrorPath(),
		CapsuleSHA256: response.GetCapsuleSha256(),
	}, nil
}

func PublishCapsuleWithoutDatabase(
	ctx context.Context,
	options Options,
	capsuleDirectory string,
	capsule []byte,
	capsuleSHA256 string,
) (PublishedCapsuleMutation, error) {
	if options.DaemonPath == "" || options.RepositoryID == "" || capsuleDirectory == "" || len(capsule) == 0 || len(capsuleSHA256) != 64 {
		return PublishedCapsuleMutation{}, errors.New("daemon path, repository ID, capsule directory, capsule, and SHA-256 digest are required")
	}
	temporary, err := os.CreateTemp("", "vaultic-capsule-mutation-*.json")
	if err != nil {
		return PublishedCapsuleMutation{}, fmt.Errorf("create temporary capsule: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		vaulticerrors.CloseQuietly(temporary)
		return PublishedCapsuleMutation{}, err
	}
	if _, err := temporary.Write(capsule); err != nil {
		vaulticerrors.CloseQuietly(temporary)
		return PublishedCapsuleMutation{}, fmt.Errorf("write temporary capsule: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		vaulticerrors.CloseQuietly(temporary)
		return PublishedCapsuleMutation{}, fmt.Errorf("sync temporary capsule: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return PublishedCapsuleMutation{}, err
	}
	command := exec.CommandContext(ctx, options.DaemonPath, "publish-capsule", capsuleDirectory, temporaryPath, capsuleSHA256, "true")
	command.Env = append(daemonEnvironment(options), "VAULTICDB_REPOSITORY_ID="+options.RepositoryID)
	for name, value := range map[string]string{
		"VAULTICDB_OBJECT_STORE": options.ObjectStore,
		"VAULTICDB_DATA_DIR":     options.DataDir,
		"VAULTICDB_S3_BUCKET":    options.S3Bucket,
		"VAULTICDB_S3_PREFIX":    options.S3Prefix,
	} {
		if value != "" {
			command.Env = append(command.Env, name+"="+value)
		}
	}
	var output, daemonErrors bytes.Buffer
	command.Stdout = &limitedWriter{writer: &output, remaining: 64 * 1024}
	command.Stderr = &limitedWriter{writer: &daemonErrors, remaining: 64 * 1024}
	if err := command.Run(); err != nil {
		return PublishedCapsuleMutation{}, fmt.Errorf("publish capsule without database: %w: %s", err, strings.TrimSpace(daemonErrors.String()))
	}
	var published PublishedCapsuleMutation
	decoder := json.NewDecoder(&output)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&published); err != nil {
		return PublishedCapsuleMutation{}, fmt.Errorf("decode capsule publisher result: %w", err)
	}
	if published.CapsuleSHA256 != capsuleSHA256 || published.Generation == 0 || published.LocalPath == "" || published.MirrorPath == "" {
		return PublishedCapsuleMutation{}, errors.New("capsule publisher returned inconsistent results")
	}
	return published, nil
}

func daemonEnvironment(options Options) []string {
	allowed := map[string]bool{
		"HOME": true, "PATH": true, "TMPDIR": true,
		"HTTP_PROXY": true, "HTTPS_PROXY": true, "NO_PROXY": true,
		"http_proxy": true, "https_proxy": true, "no_proxy": true,
		"SSL_CERT_FILE": true, "SSL_CERT_DIR": true,
	}
	result := make([]string, 0, len(allowed))
	for _, entry := range os.Environ() {
		name, _, found := strings.Cut(entry, "=")
		if found && (allowed[name] || (options.ObjectStore == "s3" && strings.HasPrefix(name, "AWS_"))) {
			result = append(result, entry)
		}
	}
	return result
}

func (c *Client) PrepareCapsuleMigration(
	ctx context.Context,
	capsuleDirectory string,
	generation uint64,
	groupID string,
	threshold uint32,
	brokerIdentityPublicKey []byte,
	members []OfflineCapsuleMember,
) (CapsuleMigration, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	requestMembers := make([]*vaulticdbv1.OfflineCapsuleMember, len(members))
	for index, member := range members {
		requestMembers[index] = &vaulticdbv1.OfflineCapsuleMember{
			MemberId:   member.ID,
			Provider:   member.Provider,
			Credential: append([]byte(nil), member.Credential...),
		}
		defer func(value []byte) {
			for index := range value {
				value[index] = 0
			}
		}(requestMembers[index].Credential)
	}
	response, err := c.rpc.PrepareCapsuleMigration(
		ctx,
		&vaulticdbv1.PrepareCapsuleMigrationRequest{
			RepositoryId:            c.options.RepositoryID,
			Context:                 requestContext(ctx),
			CapsuleDirectory:        capsuleDirectory,
			Generation:              generation,
			GroupId:                 groupID,
			Threshold:               threshold,
			BrokerIdentityPublicKey: brokerIdentityPublicKey,
			Members:                 requestMembers,
		},
	)
	if err != nil {
		return CapsuleMigration{}, err
	}
	_ = observability.Emit(
		ctx,
		observability.Event{
			Severity:  observability.Warning,
			Category:  observability.CategoryLifecycle,
			Component: "vaulticdb",
			Message:   "recovery capsule migration prepared",
			Fields: map[string]any{
				"repository_id":  c.options.RepositoryID,
				"generation":     response.GetGeneration(),
				"capsule_sha256": response.GetCapsuleSha256(),
			},
		},
	)
	return CapsuleMigration{
		Generation:    response.GetGeneration(),
		LocalPath:     response.GetLocalPath(),
		MirrorPath:    response.GetMirrorPath(),
		CapsuleSHA256: response.GetCapsuleSha256(),
		Capsule:       response.GetCapsule(),
	}, nil
}

func (c *Client) FinalizeCapsuleMigration(ctx context.Context, capsuleSHA256 string, brokerKeyProof []byte) error {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	_, err := c.rpc.FinalizeCapsuleMigration(
		ctx,
		&vaulticdbv1.FinalizeCapsuleMigrationRequest{
			RepositoryId:   c.options.RepositoryID,
			Context:        requestContext(ctx),
			CapsuleSha256:  capsuleSHA256,
			BrokerKeyProof: brokerKeyProof,
		},
	)
	if err == nil {
		_ = observability.Emit(
			ctx,
			observability.Event{
				Severity:  observability.Warning,
				Category:  observability.CategoryLifecycle,
				Component: "vaulticdb",
				Message:   "recovery capsule migration finalized and database master key removed",
				Fields:    map[string]any{"repository_id": c.options.RepositoryID, "capsule_sha256": capsuleSHA256},
			},
		)
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
	response, err := c.rpc.AddLocalKeySlot(
		ctx,
		&vaulticdbv1.AddLocalKeySlotRequest{
			RepositoryId: c.options.RepositoryID,
			Context:      requestContext(ctx),
			SlotId:       slotID,
			Passphrase:   passphrase,
			Priority:     priority,
			Recovery:     recovery,
		},
	)
	if err != nil {
		return KeyStatus{}, err
	}
	_ = observability.Emit(
		ctx,
		observability.Event{
			Severity:  observability.Notice,
			Category:  observability.CategoryLifecycle,
			Component: "vaulticdb",
			Message:   "metadata key slot added",
			Fields:    map[string]any{"slot_id": slotID, "provider": "local-argon2id", "envelope_generation": response.GetEnvelopeGeneration()},
		},
	)
	return keyStatus(response), nil
}

// AddCloudKeySlot wraps the metadata DEK using a cloud KMS provider.
func (c *Client) AddCloudKeySlot(ctx context.Context, slotID, provider, keyReference string, bearerToken []byte, priority uint32) (KeyStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.AddCloudKeySlot(
		ctx,
		&vaulticdbv1.AddCloudKeySlotRequest{
			RepositoryId: c.options.RepositoryID,
			Context:      requestContext(ctx),
			SlotId:       slotID,
			Provider:     provider,
			KeyReference: keyReference,
			BearerToken:  bearerToken,
			Priority:     priority,
		},
	)
	if err != nil {
		return KeyStatus{}, err
	}
	_ = observability.Emit(
		ctx,
		observability.Event{
			Severity:  observability.Notice,
			Category:  observability.CategoryLifecycle,
			Component: "vaulticdb",
			Message:   "metadata cloud key slot added",
			Fields: map[string]any{
				"slot_id":             slotID,
				"provider":            provider,
				"key_reference":       keyReference,
				"envelope_generation": response.GetEnvelopeGeneration(),
			},
		},
	)
	return keyStatus(response), nil
}

// RemoveKeySlot removes a wrapping slot while retaining at least one slot.
func (c *Client) RemoveKeySlot(ctx context.Context, slotID string) (KeyStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.RemoveKeySlot(
		ctx,
		&vaulticdbv1.RemoveKeySlotRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), SlotId: slotID},
	)
	if err != nil {
		return KeyStatus{}, err
	}
	_ = observability.Emit(
		ctx,
		observability.Event{
			Severity:  observability.Warning,
			Category:  observability.CategoryLifecycle,
			Component: "vaulticdb",
			Message:   "metadata key slot removed",
			Fields:    map[string]any{"slot_id": slotID, "envelope_generation": response.GetEnvelopeGeneration()},
		},
	)
	return keyStatus(response), nil
}

// RotateLocalKeySlot rewraps the unchanged DEK under a new passphrase-derived KEK.
func (c *Client) RotateLocalKeySlot(ctx context.Context, slotID string, passphrase []byte) (KeyStatus, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.RotateLocalKeySlot(
		ctx,
		&vaulticdbv1.RotateLocalKeySlotRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), SlotId: slotID, Passphrase: passphrase},
	)
	if err != nil {
		return KeyStatus{}, err
	}
	_ = observability.Emit(
		ctx,
		observability.Event{
			Severity:  observability.Notice,
			Category:  observability.CategoryAuth,
			Component: "vaulticdb",
			Message:   "metadata KEK rotated",
			Fields:    map[string]any{"slot_id": slotID, "envelope_generation": response.GetEnvelopeGeneration()},
		},
	)
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
	_ = observability.Emit(
		ctx,
		observability.Event{
			Severity:  observability.Notice,
			Category:  observability.CategoryLifecycle,
			Component: "vaulticdb",
			Message:   "metadata DEK rotated",
			Fields:    map[string]any{"dek_version": response.GetActiveDekVersion(), "envelope_generation": response.GetEnvelopeGeneration()},
		},
	)
	return keyStatus(response), nil
}

// RewriteDEK rewrites at most maxObjects encrypted objects under the active DEK.
func (c *Client) RewriteDEK(ctx context.Context, maxObjects uint32) (DEKRewriteProgress, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.RewriteDek(
		ctx,
		&vaulticdbv1.RewriteDekRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), MaxObjects: maxObjects},
	)
	if err != nil {
		return DEKRewriteProgress{}, err
	}
	return DEKRewriteProgress{Rewritten: response.GetRewritten(), Remaining: response.GetRemaining()}, nil
}

// EscrowMasterKey wraps the in-DB repository master key with a cloud provider.
func (c *Client) EscrowMasterKey(ctx context.Context, escrowID, provider, keyReference string, bearerToken []byte) ([]byte, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.EscrowMasterKey(
		ctx,
		&vaulticdbv1.EscrowMasterKeyRequest{
			RepositoryId: c.options.RepositoryID,
			Context:      requestContext(ctx),
			EscrowId:     escrowID,
			Provider:     provider,
			KeyReference: keyReference,
			BearerToken:  bearerToken,
		},
	)
	if err != nil {
		return nil, err
	}
	_ = observability.Emit(
		ctx,
		observability.Event{
			Severity:  observability.Notice,
			Category:  observability.CategoryAuth,
			Component: "vaulticdb",
			Message:   "repository master key escrowed",
			Fields:    map[string]any{"escrow_id": escrowID, "provider": provider, "key_reference": keyReference},
		},
	)
	return response.GetRecord(), nil
}

// RecoverEscrow unwraps a standalone escrow record without metadata DB access.
func (c *Client) RecoverEscrow(ctx context.Context, record, bearerToken []byte) ([]byte, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := c.rpc.RecoverEscrow(
		ctx,
		&vaulticdbv1.RecoverEscrowRequest{RepositoryId: c.options.RepositoryID, Context: requestContext(ctx), Record: record, BearerToken: bearerToken},
	)
	if err != nil {
		return nil, err
	}
	_ = observability.Emit(
		ctx,
		observability.Event{
			Severity:  observability.Warning,
			Category:  observability.CategoryAuth,
			Component: "vaulticdb",
			Message:   "repository master key recovered from escrow",
			Fields:    map[string]any{"repository_id": c.options.RepositoryID},
		},
	)
	return response.GetMasterKey(), nil
}

func keyStatus(response *vaulticdbv1.KeyStatusResponse) KeyStatus {
	result := KeyStatus{
		EnvelopeGeneration:              response.GetEnvelopeGeneration(),
		ActiveDEKVersion:                response.GetActiveDekVersion(),
		Slots:                           make([]KeySlotInfo, len(response.GetSlots())),
		PendingCapsuleMigrationSHA256:   response.GetPendingCapsuleMigrationSha256(),
		FinalizedCapsuleMigrationSHA256: response.GetFinalizedCapsuleMigrationSha256(),
	}
	for index, slot := range response.GetSlots() {
		result.Slots[index] = KeySlotInfo{
			ID:           slot.GetId(),
			Provider:     slot.GetProvider(),
			Priority:     slot.GetPriority(),
			Recovery:     slot.GetRecovery(),
			KeyReference: slot.GetKeyReference(),
			DEKVersion:   slot.GetDekVersion(),
		}
	}
	return result
}

func (c *Client) get(ctx context.Context, key []byte, transactionID string) ([]byte, bool, error) {
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

func (c *Client) multiGet(ctx context.Context, keys [][]byte, transactionID string) ([]KeyValue, []bool, error) {
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

func (c *Client) scanPage(ctx context.Context, prefix, afterKey []byte, pageSize uint32, transactionID string) ([]KeyValue, bool, error) {
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

func (c *Client) writeBatchWithIdempotency(
	ctx context.Context,
	puts []Mutation,
	deletes [][]byte,
	awaitDurable bool,
	transactionID, idempotencyKey string,
) (bool, error) {
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	if uint64(len(puts)+len(deletes)) > uint64(c.limits.MaxBatchItems) {
		return false, fmt.Errorf("write batch has %d items, limit is %d", len(puts)+len(deletes), c.limits.MaxBatchItems)
	}
	request := &vaulticdbv1.WriteBatchRequest{
		Context: requestContext(ctx), TransactionId: transactionID,
		Deletes: deletes, AwaitDurable: awaitDurable, IdempotencyKey: idempotencyKey,
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
	client         *Client
	id             string
	state          atomic.Uint32
	idempotencyKey string
}

const (
	transactionOpen uint32 = iota
	transactionClosed
	transactionCommitUncertain
)

func (c *Client) begin(ctx context.Context) (*Transaction, error) {
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
	return t.CommitWithIdempotency(ctx, "")
}

// CommitWithIdempotency commits once and permits recovery of an uncertain response using the same key.
func (t *Transaction) CommitWithIdempotency(ctx context.Context, idempotencyKey string) error {
	state := t.state.Load()
	if state == transactionCommitUncertain {
		if idempotencyKey == "" || idempotencyKey != t.idempotencyKey {
			return fmt.Errorf("transaction is already closed; uncertain commit requires its original idempotency key")
		}
	} else if !t.state.CompareAndSwap(transactionOpen, transactionClosed) {
		return fmt.Errorf("transaction is already closed")
	}
	t.idempotencyKey = idempotencyKey
	ctx, cancel := withDefaultRPCDeadline(ctx)
	defer cancel()
	response, err := t.client.rpc.Commit(ctx, &vaulticdbv1.TransactionRequest{
		Context: requestContext(ctx), TransactionId: t.id, IdempotencyKey: idempotencyKey,
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
	t.state.Store(transactionClosed)
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
	_ = observability.Emit(
		ctx,
		observability.Event{
			Severity:  observability.Critical,
			Category:  observability.CategoryIntegrity,
			Component: "vaulticdb",
			Message:   "encrypted metadata authentication failed",
			Fields:    map[string]any{"repository_id": c.options.RepositoryID, "operation": operation},
		},
	)
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
