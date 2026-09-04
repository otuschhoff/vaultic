package repository

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/repository/staging"
)

// Staging command API types are aliases so command code does not depend on a
// package below the repository boundary.
type (
	// Store provides access to authenticated staging journals.
	Store = staging.Store
	// MirrorPlacement describes a staging mirror's durability placement.
	MirrorPlacement = staging.MirrorPlacement
	// Policy defines the required staging mirror durability.
	Policy = staging.Policy
	// Header identifies and constrains a staging journal.
	Header = staging.Header
	// Job is a discovered staging journal and its current state.
	Job = staging.Job
	// Segment is an authenticated portion of a staging journal.
	Segment = staging.Segment
	// DaemonAuthority commits staging journals through VaulticDB.
	DaemonAuthority = staging.DaemonAuthority
	// BackendPackVerifier verifies staged packs against configured mirrors.
	BackendPackVerifier = staging.BackendPackVerifier
	// ReconcileResult reports a staging reconciliation outcome.
	ReconcileResult = staging.ReconcileResult
)

// Staging state and reconciliation dispositions used by operator commands.
const (
	StateSealedPending       = staging.StateSealedPending
	StateCommitted           = staging.StateCommitted
	ReconcileCommitted       = staging.ReconcileCommitted
	ReconcileRetryable       = staging.ReconcileRetryable
	ReconcileRejected        = staging.ReconcileRejected
	ReconcileHealingRequired = staging.ReconcileHealingRequired
)

// DeriveJournalKey derives the authenticated staging journal key.
func DeriveJournalKey(repositoryKey []byte, repositoryID string) ([]byte, error) {
	return staging.DeriveJournalKey(repositoryKey, repositoryID)
}

// Reconcile applies an authenticated staging journal to authoritative metadata.
func Reconcile(ctx context.Context, store Store, authority staging.Authority, verifier staging.PackVerifier, job Job) ReconcileResult {
	return staging.Reconcile(ctx, store, authority, verifier, job)
}

// ReconcileCandidate replays a journal into an isolated healing candidate.
func ReconcileCandidate(ctx context.Context, store Store, authority staging.Authority, verifier staging.PackVerifier, job Job) ReconcileResult {
	return staging.ReconcileCandidate(ctx, store, authority, verifier, job)
}
