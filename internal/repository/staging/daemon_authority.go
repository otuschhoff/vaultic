package staging

import (
	"context"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
)

type DaemonCommitPlan struct {
	Puts         []daemon.Mutation
	Deletes      [][]byte
	SnapshotID   string
	SnapshotJSON []byte
}

type DaemonAuthority struct {
	Client            *daemon.Client
	Store             *daemon.SchemaStore
	Preflight         func(context.Context, Header) error
	SnapshotPublisher func(context.Context, string, []byte) error
	plan              DaemonCommitPlan
}

func (authority *DaemonAuthority) IntegrityPreflight(ctx context.Context, header Header) error {
	if authority.Client == nil || authority.Store == nil || authority.Preflight == nil || authority.SnapshotPublisher == nil {
		return fmt.Errorf("deferred reconciliation authority is incomplete")
	}
	return authority.Preflight(ctx, header)
}

func (authority *DaemonAuthority) LookupIdempotency(ctx context.Context, _ Job, segments []Segment, key string) (CommitResult, bool, error) {
	plan, err := BuildDaemonCommitPlan(ctx, authority.Store, segments)
	if err != nil {
		return CommitResult{}, false, err
	}
	authority.plan = plan
	committed, err := authority.Client.IdempotencyCommitted(ctx, key)
	if err != nil || !committed {
		return CommitResult{}, committed, err
	}
	return CommitResult{MetadataTransaction: key, SnapshotID: authority.plan.SnapshotID}, true, nil
}

func (authority *DaemonAuthority) CommitMetadata(ctx context.Context, _ Job, _ []Segment, key string) (CommitResult, error) {
	if authority.plan.SnapshotID == "" {
		return CommitResult{}, HealingRequired(fmt.Errorf("deferred reconciliation plan was not prepared"))
	}
	if err := authority.Store.PublishSchemaBatchWithIdempotency(ctx, authority.plan.Puts, authority.plan.Deletes, key); err != nil {
		return CommitResult{}, Retryable(err)
	}
	return CommitResult{MetadataTransaction: key, SnapshotID: authority.plan.SnapshotID}, nil
}

func (authority *DaemonAuthority) PublishSnapshot(ctx context.Context, commit CommitResult) error {
	if commit.SnapshotID != authority.plan.SnapshotID {
		return fmt.Errorf("idempotency result does not match prospective snapshot")
	}
	return authority.SnapshotPublisher(ctx, commit.SnapshotID, authority.plan.SnapshotJSON)
}
