package daemon

import (
	"context"
	"time"
)

// KVClient is the reusable read-only key-value client contract.
type KVClient interface {
	Get(context.Context, []byte, string) ([]byte, bool, error)
	MultiGet(context.Context, [][]byte, string) ([]KeyValue, []bool, error)
	ScanPage(context.Context, []byte, []byte, uint32, string) ([]KeyValue, bool, error)
}

// TxnClient is the reusable mutation and transaction client contract.
type TxnClient interface {
	WriteBatch(context.Context, []Mutation, [][]byte, bool, string) (bool, error)
	WriteBatchWithIdempotency(context.Context, []Mutation, [][]byte, bool, string, string) (bool, error)
	Begin(context.Context) (*Transaction, error)
	IdempotencyCommitted(context.Context, string) (bool, error)
}

// RoleClient is the reusable writer-role client contract.
type RoleClient interface {
	WriterStatus(context.Context) (WriterStatus, error)
	DemoteWriter(context.Context, string, bool, time.Duration) (WriterStatus, error)
	PromoteWriter(context.Context, string) (WriterStatus, error)
	PromoteWriterWithTakeover(context.Context, string, bool, uint64) (WriterStatus, error)
}

// GenerationClient is the reusable generation-authority client contract.
type GenerationClient interface {
	GenerationStatus(context.Context) (GenerationStatus, error)
	ActivateGeneration(context.Context, uint64, uint64, string, string, time.Duration) (GenerationStatus, error)
	QuarantineGeneration(context.Context, uint64, string) (GenerationStatus, error)
	VerifyGeneration(context.Context, uint64, string) (GenerationStatus, error)
	RollbackGeneration(context.Context, uint64, string, time.Duration) (GenerationStatus, error)
	RetireGeneration(context.Context, uint64, uint64, string) (GenerationStatus, error)
}

var (
	_ KVClient         = (*KV)(nil)
	_ KVClient         = (*Client)(nil)
	_ TxnClient        = (*Txn)(nil)
	_ TxnClient        = (*Client)(nil)
	_ RoleClient       = (*Role)(nil)
	_ RoleClient       = (*Client)(nil)
	_ GenerationClient = (*Generation)(nil)
	_ GenerationClient = (*Client)(nil)
)
