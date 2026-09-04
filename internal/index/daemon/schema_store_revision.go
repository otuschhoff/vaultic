package daemon

import (
	"context"
)

func rollbackTransaction(ctx context.Context, transaction *Transaction) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultRPCDeadline)
	defer cancel()
	return transaction.Rollback(cleanupCtx)
}
