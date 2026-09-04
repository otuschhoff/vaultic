package staging

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
)

type DaemonCommitPlan struct {
	Puts         []daemon.Mutation
	Deletes      [][]byte
	SnapshotID   string
	SnapshotJSON []byte
	Observations []json.RawMessage
	RootKey      []byte
}

type DaemonAuthority struct {
	Client             *daemon.Client
	Store              *daemon.SchemaStore
	Preflight          func(context.Context, Header) error
	SnapshotPublisher  func(context.Context, string, []byte) error
	ReplayObservations func(context.Context, []json.RawMessage) ([]byte, error)
	plan               DaemonCommitPlan
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
	committed, err := authority.Client.IdempotencyCommitted(ctx, key)
	if err != nil {
		return CommitResult{}, false, err
	}
	snapshotID, err := schemaID(plan.SnapshotID)
	if err != nil {
		return CommitResult{}, false, Reject(err)
	}
	if committed {
		plan.RootKey, err = authority.Store.ExportCheckpointRoot(ctx, snapshotID)
		if err != nil {
			return CommitResult{}, false, HealingRequired(fmt.Errorf("recover committed deferred snapshot root: %w", err))
		}
	} else {
		if authority.ReplayObservations == nil {
			return CommitResult{}, false, HealingRequired(fmt.Errorf("deferred semantic replay is not configured"))
		}
		plan.RootKey, err = authority.ReplayObservations(ctx, plan.Observations)
		if err != nil {
			return CommitResult{}, false, Reject(fmt.Errorf("replay deferred crawl observations: %w", err))
		}
		if err := authority.Store.MarkExportPending(ctx, snapshotID, plan.RootKey); err != nil {
			return CommitResult{}, false, Retryable(fmt.Errorf("persist deferred snapshot root: %w", err))
		}
	}
	authority.plan = plan
	if !committed {
		return CommitResult{}, false, nil
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
	if commit.SnapshotID != authority.plan.SnapshotID || len(authority.plan.RootKey) == 0 {
		return fmt.Errorf("idempotency result does not match prospective snapshot")
	}
	if err := authority.SnapshotPublisher(ctx, commit.SnapshotID, authority.plan.SnapshotJSON); err != nil {
		return err
	}
	snapshotID, err := schemaID(commit.SnapshotID)
	if err != nil {
		return err
	}
	return authority.Store.PublishSnapshotScope(
		ctx,
		daemon.SnapshotScope{SnapshotID: snapshotID, RootKey: authority.plan.RootKey, OriginalJSON: authority.plan.SnapshotJSON},
	)
}
