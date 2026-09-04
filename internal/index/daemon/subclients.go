package daemon

import (
	"context"
	"time"
)

// KV groups point and range reads.
type KV struct{ client *Client }

// Txn groups atomic writes and transaction creation.
type Txn struct{ client *Client }

// Role groups writer-role inspection and transitions.
type Role struct{ client *Client }

// Generation groups metadata-generation authority operations.
type Generation struct{ client *Client }

func (c *Client) initializeSubclients() {
	c.KV.client = c
	c.Txn.client = c
	c.Role.client = c
	c.Generation.client = c
}

func (kv *KV) Get(ctx context.Context, key []byte, transactionID string) ([]byte, bool, error) {
	return kv.client.get(ctx, key, transactionID)
}

func (kv *KV) MultiGet(ctx context.Context, keys [][]byte, transactionID string) ([]KeyValue, []bool, error) {
	return kv.client.multiGet(ctx, keys, transactionID)
}

func (kv *KV) ScanPage(ctx context.Context, prefix, afterKey []byte, pageSize uint32, transactionID string) ([]KeyValue, bool, error) {
	return kv.client.scanPage(ctx, prefix, afterKey, pageSize, transactionID)
}

func (txn *Txn) WriteBatch(ctx context.Context, puts []Mutation, deletes [][]byte, awaitDurable bool, transactionID string) (bool, error) {
	return txn.client.writeBatchWithIdempotency(ctx, puts, deletes, awaitDurable, transactionID, "")
}

func (txn *Txn) WriteBatchWithIdempotency(
	ctx context.Context,
	puts []Mutation,
	deletes [][]byte,
	awaitDurable bool,
	transactionID, idempotencyKey string,
) (bool, error) {
	return txn.client.writeBatchWithIdempotency(ctx, puts, deletes, awaitDurable, transactionID, idempotencyKey)
}

func (txn *Txn) Begin(ctx context.Context) (*Transaction, error) {
	return txn.client.begin(ctx)
}

func (txn *Txn) IdempotencyCommitted(ctx context.Context, key string) (bool, error) {
	return txn.client.idempotencyCommitted(ctx, key)
}

func (role *Role) WriterStatus(ctx context.Context) (WriterStatus, error) {
	return role.client.writerStatus(ctx)
}

func (role *Role) DemoteWriter(ctx context.Context, reason string, force bool, timeout time.Duration) (WriterStatus, error) {
	return role.client.demoteWriter(ctx, reason, force, timeout)
}

func (role *Role) PromoteWriter(ctx context.Context, reason string) (WriterStatus, error) {
	return role.client.promoteWriterWithTakeover(ctx, reason, false, 0)
}

func (role *Role) PromoteWriterWithTakeover(ctx context.Context, reason string, forceTakeover bool, expectedActiveEpoch uint64) (WriterStatus, error) {
	return role.client.promoteWriterWithTakeover(ctx, reason, forceTakeover, expectedActiveEpoch)
}

func (generation *Generation) GenerationStatus(ctx context.Context) (GenerationStatus, error) {
	return generation.client.generationStatus(ctx)
}

func (generation *Generation) ActivateGeneration(
	ctx context.Context,
	expected, candidate uint64,
	namespace, reportSHA256 string,
	observationWindow time.Duration,
) (GenerationStatus, error) {
	return generation.client.activateGeneration(ctx, expected, candidate, namespace, reportSHA256, observationWindow)
}

func (generation *Generation) QuarantineGeneration(ctx context.Context, expectedGeneration uint64, diagnosticSHA256 string) (GenerationStatus, error) {
	return generation.client.quarantineGeneration(ctx, expectedGeneration, diagnosticSHA256)
}

func (generation *Generation) VerifyGeneration(ctx context.Context, expectedDecision uint64, reportSHA256 string) (GenerationStatus, error) {
	return generation.client.verifyGeneration(ctx, expectedDecision, reportSHA256)
}

func (generation *Generation) RollbackGeneration(
	ctx context.Context,
	expectedDecision uint64,
	reportSHA256 string,
	observationWindow time.Duration,
) (GenerationStatus, error) {
	return generation.client.rollbackGeneration(ctx, expectedDecision, reportSHA256, observationWindow)
}

func (generation *Generation) RetireGeneration(ctx context.Context, expectedDecision, retiredGeneration uint64, reportSHA256 string) (GenerationStatus, error) {
	return generation.client.retireGeneration(ctx, expectedDecision, retiredGeneration, reportSHA256)
}

// Get forwards a point lookup to the key-value subclient.
func (c *Client) Get(ctx context.Context, key []byte, transactionID string) ([]byte, bool, error) {
	return c.KV.Get(ctx, key, transactionID)
}

func (c *Client) MultiGet(ctx context.Context, keys [][]byte, transactionID string) ([]KeyValue, []bool, error) {
	return c.KV.MultiGet(ctx, keys, transactionID)
}

func (c *Client) ScanPage(ctx context.Context, prefix, afterKey []byte, pageSize uint32, transactionID string) ([]KeyValue, bool, error) {
	return c.KV.ScanPage(ctx, prefix, afterKey, pageSize, transactionID)
}

func (c *Client) WriteBatch(ctx context.Context, puts []Mutation, deletes [][]byte, awaitDurable bool, transactionID string) (bool, error) {
	return c.Txn.WriteBatch(ctx, puts, deletes, awaitDurable, transactionID)
}

func (c *Client) WriteBatchWithIdempotency(
	ctx context.Context,
	puts []Mutation,
	deletes [][]byte,
	awaitDurable bool,
	transactionID, idempotencyKey string,
) (bool, error) {
	return c.Txn.WriteBatchWithIdempotency(ctx, puts, deletes, awaitDurable, transactionID, idempotencyKey)
}

func (c *Client) Begin(ctx context.Context) (*Transaction, error) {
	return c.Txn.Begin(ctx)
}

func (c *Client) IdempotencyCommitted(ctx context.Context, key string) (bool, error) {
	return c.Txn.IdempotencyCommitted(ctx, key)
}

func (c *Client) WriterStatus(ctx context.Context) (WriterStatus, error) {
	return c.Role.WriterStatus(ctx)
}

func (c *Client) DemoteWriter(ctx context.Context, reason string, force bool, timeout time.Duration) (WriterStatus, error) {
	return c.Role.DemoteWriter(ctx, reason, force, timeout)
}

func (c *Client) PromoteWriter(ctx context.Context, reason string) (WriterStatus, error) {
	return c.Role.PromoteWriter(ctx, reason)
}

func (c *Client) PromoteWriterWithTakeover(ctx context.Context, reason string, forceTakeover bool, expectedActiveEpoch uint64) (WriterStatus, error) {
	return c.Role.PromoteWriterWithTakeover(ctx, reason, forceTakeover, expectedActiveEpoch)
}

func (c *Client) GenerationStatus(ctx context.Context) (GenerationStatus, error) {
	return c.Generation.GenerationStatus(ctx)
}

func (c *Client) ActivateGeneration(
	ctx context.Context,
	expected, candidate uint64,
	namespace, reportSHA256 string,
	observationWindow time.Duration,
) (GenerationStatus, error) {
	return c.Generation.ActivateGeneration(ctx, expected, candidate, namespace, reportSHA256, observationWindow)
}

func (c *Client) QuarantineGeneration(ctx context.Context, expectedGeneration uint64, diagnosticSHA256 string) (GenerationStatus, error) {
	return c.Generation.QuarantineGeneration(ctx, expectedGeneration, diagnosticSHA256)
}

func (c *Client) VerifyGeneration(ctx context.Context, expectedDecision uint64, reportSHA256 string) (GenerationStatus, error) {
	return c.Generation.VerifyGeneration(ctx, expectedDecision, reportSHA256)
}

func (c *Client) RollbackGeneration(
	ctx context.Context,
	expectedDecision uint64,
	reportSHA256 string,
	observationWindow time.Duration,
) (GenerationStatus, error) {
	return c.Generation.RollbackGeneration(ctx, expectedDecision, reportSHA256, observationWindow)
}

func (c *Client) RetireGeneration(ctx context.Context, expectedDecision, retiredGeneration uint64, reportSHA256 string) (GenerationStatus, error) {
	return c.Generation.RetireGeneration(ctx, expectedDecision, retiredGeneration, reportSHA256)
}
