package staging

import (
	"context"
	"errors"
	"fmt"
)

type ReconcileDisposition string

const (
	ReconcileCommitted       ReconcileDisposition = "committed"
	ReconcileRetryable       ReconcileDisposition = "retryable"
	ReconcileRejected        ReconcileDisposition = "rejected"
	ReconcileHealingRequired ReconcileDisposition = "healing-required"
)

type ReconcileResult struct {
	Disposition         ReconcileDisposition `json:"disposition"`
	JobID               string               `json:"job_id"`
	IdempotencyKey      string               `json:"idempotency_key"`
	MetadataTransaction string               `json:"metadata_transaction,omitempty"`
	SnapshotID          string               `json:"snapshot_id,omitempty"`
	Reason              string               `json:"reason,omitempty"`
}

type CommitResult struct {
	MetadataTransaction string
	SnapshotID          string
}

type Authority interface {
	IntegrityPreflight(context.Context, Header) error
	LookupIdempotency(context.Context, Job, []Segment, string) (CommitResult, bool, error)
	CommitMetadata(context.Context, Job, []Segment, string) (CommitResult, error)
	PublishSnapshot(context.Context, CommitResult) error
}

type PackVerifier interface {
	VerifyPack(context.Context, Pack) error
}

type classifiedError struct {
	disposition ReconcileDisposition
	err         error
}

func (error classifiedError) Error() string { return error.err.Error() }
func (error classifiedError) Unwrap() error { return error.err }

func Retryable(error error) error {
	return classifiedError{disposition: ReconcileRetryable, err: error}
}
func Reject(error error) error { return classifiedError{disposition: ReconcileRejected, err: error} }
func HealingRequired(error error) error {
	return classifiedError{disposition: ReconcileHealingRequired, err: error}
}

func (store Store) PublishCompletion(ctx context.Context, completion Completion) error {
	encoded, _, err := SealCompletion(completion, store.Key)
	if err != nil {
		return err
	}
	return store.publish(ctx, CompletionHandle(completion.Header.JobID), encoded)
}

func Reconcile(ctx context.Context, store Store, authority Authority, verifier PackVerifier, job Job) ReconcileResult {
	return reconcile(ctx, store, authority, verifier, job, false)
}

// ReconcileCandidate replays authenticated journals into an isolated Plan B
// generation without changing repository-visible snapshot or completion state.
func ReconcileCandidate(ctx context.Context, store Store, authority Authority, verifier PackVerifier, job Job) ReconcileResult {
	return reconcile(ctx, store, authority, verifier, job, true)
}

func reconcile(ctx context.Context, store Store, authority Authority, verifier PackVerifier, job Job, candidate bool) ReconcileResult {
	result := ReconcileResult{JobID: job.Header.JobID, IdempotencyKey: job.Header.IdempotencyKey}
	if !candidate && job.State == StateCommitted && job.Completion != nil {
		result.Disposition = ReconcileCommitted
		result.MetadataTransaction = job.Completion.MetadataTransaction
		result.SnapshotID = job.Completion.SnapshotID
		return result
	}
	if job.State != StateSealedPending && !(candidate && job.State == StateCommitted && job.Completion != nil) {
		return reconcileFailure(result, Reject(fmt.Errorf("journal state %q is not eligible for reconciliation", job.State)))
	}
	segments, err := store.VerifyJob(ctx, job)
	if err != nil {
		return reconcileFailure(result, Reject(fmt.Errorf("authenticate journal: %w", err)))
	}
	for _, segment := range segments {
		for _, pack := range segment.Packs {
			if err := verifier.VerifyPack(ctx, pack); err != nil {
				return reconcileFailure(result, err)
			}
		}
	}
	if err := authority.IntegrityPreflight(ctx, job.Header); err != nil {
		return reconcileFailure(result, HealingRequired(fmt.Errorf("metadata integrity preflight: %w", err)))
	}
	commit, found, err := authority.LookupIdempotency(ctx, job, segments, job.Header.IdempotencyKey)
	if err != nil {
		return reconcileFailure(result, Retryable(fmt.Errorf("lookup reconciliation idempotency: %w", err)))
	}
	if !found {
		commit, err = authority.CommitMetadata(ctx, job, segments, job.Header.IdempotencyKey)
		if err != nil {
			return reconcileFailure(result, err)
		}
	}
	if commit.MetadataTransaction == "" || commit.SnapshotID == "" {
		return reconcileFailure(result, HealingRequired(errors.New("metadata commit returned incomplete authority")))
	}
	if err := authority.PublishSnapshot(ctx, commit); err != nil {
		return reconcileFailure(result, Retryable(fmt.Errorf("publish normal snapshot after metadata commit: %w", err)))
	}
	if candidate {
		result.Disposition = ReconcileCommitted
		result.MetadataTransaction = commit.MetadataTransaction
		result.SnapshotID = commit.SnapshotID
		return result
	}
	completion := Completion{
		Header: job.Header, State: StateCommitted, SealSHA256: job.SealSHA256,
		MetadataTransaction: commit.MetadataTransaction, SnapshotID: commit.SnapshotID,
		CompletedAt: store.now(),
	}
	if err := store.PublishCompletion(ctx, completion); err != nil {
		return reconcileFailure(result, Retryable(fmt.Errorf("publish journal completion: %w", err)))
	}
	result.Disposition = ReconcileCommitted
	result.MetadataTransaction = commit.MetadataTransaction
	result.SnapshotID = commit.SnapshotID
	return result
}

func reconcileFailure(result ReconcileResult, err error) ReconcileResult {
	result.Disposition = ReconcileRetryable
	var classified classifiedError
	if errors.As(err, &classified) {
		result.Disposition = classified.disposition
	}
	result.Reason = err.Error()
	return result
}
