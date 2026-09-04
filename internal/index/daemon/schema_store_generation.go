package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (store *SchemaStore) ExportCheckpointRoot(ctx context.Context, snapshotID schema.ID) ([]byte, error) {
	value, found, err := store.Get(ctx, schema.ExportCheckpointKey(snapshotID))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("snapshot export checkpoint is missing")
	}
	record, err := schema.UnmarshalExportCheckpointRecord(value)
	if err != nil {
		return nil, err
	}
	if (record.State != schema.ExportPending && record.State != schema.ExportComplete) || len(record.RootKey) == 0 {
		return nil, fmt.Errorf("snapshot export checkpoint has no recoverable root")
	}
	return append([]byte(nil), record.RootKey...), nil
}

func (store *SchemaStore) MarkExportFailed(ctx context.Context, snapshotID schema.ID, failure error) error {
	key := schema.ExportCheckpointKey(snapshotID)
	value, found, err := store.Get(ctx, key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("snapshot export checkpoint is missing")
	}
	record, err := schema.UnmarshalExportCheckpointRecord(value)
	if err != nil {
		return err
	}
	if record.State != schema.ExportPending {
		return fmt.Errorf("snapshot export cannot fail from state %d", record.State)
	}
	record.State = schema.ExportFailed
	record.Attempts++
	record.LastError = failure.Error()
	encoded, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	return store.Put(ctx, key, encoded, true)
}

// PublishSnapshotScope verifies that the reconciled root is durable, allocates
// a commit sequence, creates the immutable snapshot record, and completes its
// export checkpoint in one serializable transaction.
func (store *SchemaStore) PublishSnapshotScope(ctx context.Context, scope SnapshotScope) error {
	root, err := schema.ParseKey(scope.RootKey)
	if err != nil || root.Kind != schema.KeyDirectoryRevision || root.Revision == 0 ||
		scope.SnapshotID == (schema.ID{}) {
		return fmt.Errorf("invalid snapshot scope")
	}
	if scope.Crawl != nil && (scope.Crawl.RootFSID != root.FSID || scope.Crawl.RootInode != root.Inode) {
		return fmt.Errorf("authoritative crawl root does not match snapshot root")
	}
	var identities []schema.ParsedKey
	if enabled, enabledErr := store.analyticsEnabled(ctx); enabledErr != nil {
		return enabledErr
	} else if enabled {
		identities, err = store.snapshotIdentities(ctx, scope.RootKey)
		if err != nil {
			return err
		}
	}
	bindings, err := store.authoritativeScopeBindings(ctx, scope.Crawl)
	if err != nil {
		return err
	}
	crawlIdentities := identities
	if scope.Crawl != nil {
		crawlIdentities, err = store.snapshotObservedIdentities(ctx, scope.RootKey)
		if err != nil {
			return err
		}
	}
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		err := store.publishSnapshotScopeOnce(ctx, scope, root, identities, crawlIdentities, bindings)
		if status.Code(err) != codes.Aborted {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
	return fmt.Errorf("publish snapshot scope: transaction conflict retry limit exceeded")
}

type snapshotPublishPlan struct {
	puts                []Mutation
	next                uint64
	analyticsGeneration uint64
	deltaOrdinal        uint32
	noop                bool
}

func (store *SchemaStore) publishSnapshotScopeOnce(
	ctx context.Context,
	scope SnapshotScope,
	root schema.ParsedKey,
	identities, crawlIdentities []schema.ParsedKey,
	bindings map[string]schema.AuthoritativeSourceBindingRecord,
) error {
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	fail := func(err error) error { rollbackTransaction(ctx, transaction); return err }
	plan, err := planSnapshotBase(ctx, transaction, scope, root)
	if err != nil {
		return fail(err)
	}
	if plan.noop {
		return transaction.Rollback(ctx)
	}
	if err := store.completeSnapshotPlan(ctx, transaction, scope, identities, crawlIdentities, bindings, &plan); err != nil {
		return fail(err)
	}
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), plan.puts, nil); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

func (store *SchemaStore) completeSnapshotPlan(
	ctx context.Context,
	transaction *Transaction,
	scope SnapshotScope,
	identities, crawlIdentities []schema.ParsedKey,
	bindings map[string]schema.AuthoritativeSourceBindingRecord,
	plan *snapshotPublishPlan,
) error {
	crawlGenerations := map[identityKey]uint64{}
	if scope.Crawl != nil {
		for _, identity := range crawlIdentities {
			generation := identity.Revision
			if _, prior, found := currentSourceBinding(bindings, identity.FSID, identity.Inode); found &&
				prior.State == schema.AuthoritativeSourceLive {
				generation = prior.Generation
			}
			crawlGenerations[identityKey{identity.FSID, identity.Inode}] = generation
		}
	}
	if err := planSnapshotAnalytics(ctx, transaction, identities, crawlGenerations, plan); err != nil {
		return err
	}
	if scope.Crawl == nil {
		return nil
	}
	crawlPuts, err := store.authoritativeCrawlMutations(
		ctx,
		transaction,
		*scope.Crawl,
		crawlIdentities,
		bindings,
		plan.next,
		plan.analyticsGeneration,
	)
	if err != nil {
		return err
	}
	for _, mutation := range crawlPuts {
		if parsed, parseErr := schema.ParseKey(mutation.Key); parseErr == nil &&
			parsed.Kind == schema.KeyAnalyticsDelta {
			mutation.Key = schema.AnalyticsDeltaKey(plan.next, plan.deltaOrdinal)
			plan.deltaOrdinal++
		}
		plan.puts = append(plan.puts, mutation)
	}
	return nil
}

func planSnapshotAnalytics(
	ctx context.Context,
	transaction *Transaction,
	identities []schema.ParsedKey,
	crawlGenerations map[identityKey]uint64,
	plan *snapshotPublishPlan,
) error {
	if metadataValue, found, getErr := transaction.Get(ctx, schema.AnalyticsMetadataKey()); getErr != nil {
		return getErr
	} else if found {
		metadata, decodeErr := schema.UnmarshalAnalyticsMetadataRecord(metadataValue)
		if decodeErr == nil && metadata.Enabled {
			plan.analyticsGeneration = metadata.Generation
			for _, identity := range identities {
				generation := identity.Revision
				if crawlGeneration := crawlGenerations[identityKey{identity.FSID, identity.Inode}]; crawlGeneration != 0 {
					generation = crawlGeneration
				}
				delta := schema.AnalyticsDeltaRecord{Kind: schema.AnalyticsDeltaRetainedReferences,
					FSID:                 identity.FSID,
					Inode:                identity.Inode,
					IdentityGeneration:   generation,
					Revision:             plan.next,
					State:                schema.AnalyticsUnknown,
					RetainedSnapshotRefs: 1,
					ReferenceOperation:   schema.AnalyticsReferencesIncrement,
					ClassificationEpoch:  metadata.Generation}
				encoded, encodeErr := delta.MarshalBinary()
				if encodeErr != nil {
					return encodeErr
				}
				plan.puts = append(plan.puts, Mutation{Key: schema.AnalyticsDeltaKey(plan.next, plan.deltaOrdinal), Value: encoded})
				plan.deltaOrdinal++
			}
		}
	}
	return nil
}

func planSnapshotBase(
	ctx context.Context,
	transaction *Transaction,
	scope SnapshotScope,
	root schema.ParsedKey,
) (snapshotPublishPlan, error) {
	if _, found, err := transaction.Get(ctx, scope.RootKey); err != nil {
		return snapshotPublishPlan{}, err
	} else if !found {
		return snapshotPublishPlan{}, fmt.Errorf("snapshot root revision is not durable")
	}
	checkpointKey := schema.ExportCheckpointKey(scope.SnapshotID)
	value, found, err := transaction.Get(ctx, checkpointKey)
	if err != nil || !found {
		if err == nil {
			err = fmt.Errorf("snapshot export checkpoint is missing")
		}
		return snapshotPublishPlan{}, err
	}
	checkpoint, err := schema.UnmarshalExportCheckpointRecord(value)
	if err != nil {
		return snapshotPublishPlan{}, err
	}
	if checkpoint.State != schema.ExportPending {
		return snapshotPublishPlan{noop: true}, nil
	}
	nextKey := schema.NextRevisionKey()
	next, err := loadNextSnapshotRevision(ctx, transaction, nextKey)
	if err != nil {
		return snapshotPublishPlan{}, err
	}
	values, err := encodeSnapshotPublication(scope, root, next)
	if err != nil {
		return snapshotPublishPlan{}, err
	}
	return snapshotPublishPlan{next: next, puts: []Mutation{
		{Key: schema.SnapshotKey(scope.SnapshotID), Value: values[0]},
		{Key: schema.SnapshotCommitKey(next, scope.SnapshotID), Value: values[1]},
		{Key: checkpointKey, Value: values[2]}, {Key: nextKey, Value: values[3]},
	}}, nil
}

func loadNextSnapshotRevision(ctx context.Context, transaction *Transaction, key []byte) (uint64, error) {
	next := uint64(1)
	value, found, err := transaction.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	if found {
		next, err = schema.UnmarshalNextRevision(value)
	}
	if err == nil && next == math.MaxUint64 {
		err = fmt.Errorf("repository revision sequence exhausted")
	}
	return next, err
}

func encodeSnapshotPublication(scope SnapshotScope, root schema.ParsedKey, next uint64) ([4][]byte, error) {
	var values [4][]byte
	record := schema.SnapshotRecord{
		CommitSequence: next,
		RootFSID:       root.FSID,
		RootInode:      root.Inode,
		RootRevision:   root.Revision,
		OriginalJSON:   append([]byte(nil), scope.OriginalJSON...),
	}
	var err error
	values[0], err = record.MarshalBinary()
	if err != nil {
		return values, err
	}
	values[1],
		err = (schema.SnapshotCommitRecord{SnapshotTimeUnixNano: snapshotTimeUnixNano(scope.OriginalJSON),
		RootKey: append([]byte(nil),
			scope.RootKey...)}).MarshalBinary()
	if err != nil {
		return values, err
	}
	values[2], err = (schema.ExportCheckpointRecord{State: schema.ExportComplete, CommitSequence: next, Attempts: 1, RootKey: scope.RootKey}).MarshalBinary()
	if err != nil {
		return values, err
	}
	values[3], err = schema.MarshalNextRevision(next + 1)
	return values, err
}

type identityKey struct {
	fsid  uint32
	inode uint64
}

type authoritativeCrawlPlan struct {
	proof            schema.AuthoritativeCrawlProofRecord
	mutations        []Mutation
	observed         map[string]schema.ParsedKey
	analyticsOrdinal uint32
}

func (store *SchemaStore) authoritativeScopeBindings(
	ctx context.Context,
	claim *AuthoritativeCrawlClaim,
) (map[string]schema.AuthoritativeSourceBindingRecord, error) {
	result := map[string]schema.AuthoritativeSourceBindingRecord{}
	if claim == nil {
		return result, nil
	}
	if claim.ScopeID == (schema.ID{}) || claim.RootFSID == 0 || claim.RootInode == 0 || claim.StartFence == 0 {
		return nil, fmt.Errorf("invalid authoritative crawl claim")
	}
	prefix := schema.AuthoritativeSourceBindingPrefix(claim.ScopeID)
	var after []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, prefix, after, 10_000)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			record, err := schema.UnmarshalAuthoritativeSourceBindingRecord(entry.Value)
			if err != nil {
				return nil, err
			}
			result[string(entry.Key)] = record
		}
		if done {
			return result, nil
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("authoritative source-binding scan made no progress")
		}
		after = append(after[:0], entries[len(entries)-1].Key...)
	}
}

func (store *SchemaStore) authoritativeCrawlMutations(
	ctx context.Context,
	transaction *Transaction,
	claim AuthoritativeCrawlClaim,
	identities []schema.ParsedKey,
	bindings map[string]schema.AuthoritativeSourceBindingRecord,
	commit, analyticsGeneration uint64,
) ([]Mutation, error) {
	plan, err := planAuthoritativeCrawlProof(ctx, transaction, claim, commit)
	if err != nil {
		return nil, err
	}
	if err := planObservedAuthoritativeIdentities(ctx, transaction, claim, identities, bindings, commit, analyticsGeneration, &plan); err != nil {
		return nil, err
	}
	if err := planMissingAuthoritativeIdentities(ctx, transaction, claim, bindings, commit, analyticsGeneration, &plan); err != nil {
		return nil, err
	}
	return plan.mutations, nil
}

func planAuthoritativeCrawlProof(
	ctx context.Context,
	transaction *Transaction,
	claim AuthoritativeCrawlClaim,
	commit uint64,
) (authoritativeCrawlPlan, error) {
	debtFree := true
	for _, key := range claim.DebtKeys {
		parsed, err := schema.ParseKey(key)
		if err != nil || parsed.Kind != schema.KeyCrawlDebt {
			return authoritativeCrawlPlan{}, fmt.Errorf("invalid authoritative crawl debt key")
		}
		value, found, err := transaction.Get(ctx, key)
		if err != nil {
			return authoritativeCrawlPlan{}, err
		}
		if found {
			debt, err := schema.UnmarshalCrawlDebtRecord(value)
			if err != nil {
				return authoritativeCrawlPlan{}, err
			}
			debtFree = debtFree && debt.Status == schema.DebtResolved
		}
	}
	proof := schema.AuthoritativeCrawlProofRecord{
		ScopeID:     claim.ScopeID,
		RootFSID:    claim.RootFSID,
		RootInode:   claim.RootInode,
		StartFence:  claim.StartFence,
		EndCommit:   commit,
		CompletedAt: time.Now().UnixNano(),
		Complete:    claim.Complete,
		DebtFree:    debtFree,
	}
	proofValue, err := proof.MarshalBinary()
	if err != nil {
		return authoritativeCrawlPlan{}, err
	}
	return authoritativeCrawlPlan{
		proof: proof, mutations: []Mutation{{Key: schema.AuthoritativeCrawlProofKey(claim.ScopeID, commit), Value: proofValue}},
		observed: make(map[string]schema.ParsedKey),
	}, nil
}

func planObservedAuthoritativeIdentities(
	ctx context.Context,
	transaction *Transaction,
	claim AuthoritativeCrawlClaim,
	identities []schema.ParsedKey,
	bindings map[string]schema.AuthoritativeSourceBindingRecord,
	commit, analyticsGeneration uint64,
	plan *authoritativeCrawlPlan,
) error {
	for _, identity := range identities {
		if err := planObservedAuthoritativeIdentity(ctx, transaction, claim, identity, bindings, commit, analyticsGeneration, plan); err != nil {
			return err
		}
	}
	return nil
}

func planObservedAuthoritativeIdentity(
	ctx context.Context,
	transaction *Transaction,
	claim AuthoritativeCrawlClaim,
	identity schema.ParsedKey,
	bindings map[string]schema.AuthoritativeSourceBindingRecord,
	commit, analyticsGeneration uint64,
	plan *authoritativeCrawlPlan,
) error {
	priorKey, prior, found := currentSourceBinding(bindings, identity.FSID, identity.Inode)
	if found {
		value, exists, err := transaction.Get(ctx, priorKey)
		if err != nil {
			return err
		}
		if exists {
			prior, err = schema.UnmarshalAuthoritativeSourceBindingRecord(value)
			if err != nil {
				return err
			}
		}
	}
	generation, continuity, newGeneration := observedIdentityGeneration(identity, prior, found)
	stateChanged := !found || prior.State != schema.AuthoritativeSourceLive
	key := schema.AuthoritativeSourceBindingKey(claim.ScopeID, identity.FSID, identity.Inode, generation)
	plan.observed[string(key)] = identity
	binding := schema.AuthoritativeSourceBindingRecord{
		Generation:         generation,
		Revision:           identity.Revision,
		State:              schema.AuthoritativeSourceLive,
		Continuity:         continuity,
		LastObservedCommit: commit,
	}
	encoded, err := binding.MarshalBinary()
	if err != nil {
		return err
	}
	plan.mutations = append(plan.mutations, Mutation{Key: key, Value: encoded})
	if analyticsGeneration == 0 || !stateChanged {
		return nil
	}
	delta := observedIdentityDelta(identity, generation, continuity, analyticsGeneration, newGeneration)
	value, err := delta.MarshalBinary()
	if err != nil {
		return err
	}
	plan.mutations = append(
		plan.mutations,
		Mutation{Key: schema.AnalyticsDeltaKey(commit, plan.analyticsOrdinal), Value: value},
	)
	plan.analyticsOrdinal++
	return nil
}

func observedIdentityGeneration(
	identity schema.ParsedKey,
	prior schema.AuthoritativeSourceBindingRecord,
	found bool,
) (uint64, schema.AnalyticsIdentityContinuity, bool) {
	if found && prior.State == schema.AuthoritativeSourceLive {
		return prior.Generation, prior.Continuity, false
	}
	if found && prior.State == schema.AuthoritativeSourceDeleted {
		return identity.Revision, schema.AnalyticsContinuityProven, true
	}
	return identity.Revision, schema.AnalyticsContinuityUnknown, true
}

func observedIdentityDelta(
	identity schema.ParsedKey,
	generation uint64,
	continuity schema.AnalyticsIdentityContinuity,
	analyticsGeneration uint64,
	newGeneration bool,
) schema.AnalyticsDeltaRecord {
	if newGeneration {
		return schema.AnalyticsDeltaRecord{
			Kind:                schema.AnalyticsDeltaCreation,
			FSID:                identity.FSID,
			Inode:               identity.Inode,
			IdentityGeneration:  generation,
			Revision:            identity.Revision,
			CreatedAt:           time.Now().UnixNano(),
			CreationBasis:       schema.AnalyticsFirstSeen,
			IdentityContinuity:  continuity,
			State:               schema.AnalyticsLive,
			ClassificationEpoch: analyticsGeneration,
		}
	}
	return schema.AnalyticsDeltaRecord{
		Kind:                schema.AnalyticsDeltaSourceState,
		FSID:                identity.FSID,
		Inode:               identity.Inode,
		IdentityGeneration:  generation,
		Revision:            identity.Revision,
		IdentityContinuity:  continuity,
		State:               schema.AnalyticsLive,
		ClassificationEpoch: analyticsGeneration,
	}
}

func planMissingAuthoritativeIdentities(
	ctx context.Context,
	transaction *Transaction,
	claim AuthoritativeCrawlClaim,
	bindings map[string]schema.AuthoritativeSourceBindingRecord,
	commit, analyticsGeneration uint64,
	plan *authoritativeCrawlPlan,
) error {
	for keyString, prior := range bindings {
		if _, found := plan.observed[keyString]; found || prior.State == schema.AuthoritativeSourceDeleted {
			continue
		}
		key := []byte(keyString)
		if value, found, err := transaction.Get(ctx, key); err != nil {
			return err
		} else if found {
			prior, err = schema.UnmarshalAuthoritativeSourceBindingRecord(value)
			if err != nil {
				return err
			}
		}
		state := schema.AuthoritativeSourceUnknown
		analyticsState := schema.AnalyticsUnknown
		if plan.proof.Complete && plan.proof.DebtFree && prior.LastObservedCommit <= claim.StartFence {
			state = schema.AuthoritativeSourceDeleted
			analyticsState = schema.AnalyticsDeleted
			if prior.State == schema.AuthoritativeSourceLive && !hasUnknownGeneration(bindings, keyString) {
				prior.Continuity = schema.AnalyticsContinuityProven
			}
		} else {
			prior.Continuity = schema.AnalyticsContinuityUnknown
		}
		prior.State, prior.LastObservedCommit = state, commit
		encoded, err := prior.MarshalBinary()
		if err != nil {
			return err
		}
		plan.mutations = append(plan.mutations, Mutation{Key: key, Value: encoded})
		if analyticsGeneration != 0 {
			parsed, _ := schema.ParseKey(key)
			delta := schema.AnalyticsDeltaRecord{
				Kind:                schema.AnalyticsDeltaSourceState,
				FSID:                parsed.FSID,
				Inode:               parsed.Inode,
				IdentityGeneration:  prior.Generation,
				Revision:            prior.Revision,
				IdentityContinuity:  prior.Continuity,
				State:               analyticsState,
				ClassificationEpoch: analyticsGeneration,
			}
			value, err := delta.MarshalBinary()
			if err != nil {
				return err
			}
			plan.mutations = append(
				plan.mutations,
				Mutation{Key: schema.AnalyticsDeltaKey(commit, plan.analyticsOrdinal), Value: value},
			)
			plan.analyticsOrdinal++
		}
	}
	return nil
}

func hasUnknownGeneration(bindings map[string]schema.AuthoritativeSourceBindingRecord, currentKey string) bool {
	current, err := schema.ParseKey([]byte(currentKey))
	if err != nil {
		return true
	}
	for keyString, binding := range bindings {
		if keyString == currentKey || binding.State != schema.AuthoritativeSourceUnknown {
			continue
		}
		parsed, err := schema.ParseKey([]byte(keyString))
		if err == nil && parsed.FSID == current.FSID && parsed.Inode == current.Inode {
			return true
		}
	}
	return false
}

func currentSourceBinding(
	bindings map[string]schema.AuthoritativeSourceBindingRecord,
	fsid uint32,
	inode uint64,
) ([]byte, schema.AuthoritativeSourceBindingRecord, bool) {
	var selectedKey []byte
	var selected schema.AuthoritativeSourceBindingRecord
	for keyString, record := range bindings {
		parsed, err := schema.ParseKey([]byte(keyString))
		if err != nil || parsed.FSID != fsid || parsed.Inode != inode {
			continue
		}
		if selectedKey == nil || sourceBindingPriority(record.State) > sourceBindingPriority(selected.State) ||
			sourceBindingPriority(record.State) == sourceBindingPriority(selected.State) &&
				record.LastObservedCommit > selected.LastObservedCommit {
			selectedKey, selected = []byte(keyString), record
		}
	}
	return selectedKey, selected, selectedKey != nil
}

func sourceBindingPriority(state schema.AuthoritativeSourceState) int {
	if state == schema.AuthoritativeSourceLive {
		return 2
	}
	if state == schema.AuthoritativeSourceUnknown {
		return 1
	}
	return 0
}

// ForgetSnapshot atomically removes logical snapshot membership and emits
// ordered analytics triggers. Physical pack lifecycle is deliberately absent.
func (store *SchemaStore) ForgetSnapshot(ctx context.Context, snapshotID schema.ID) error {
	value, found, err := store.Get(ctx, schema.SnapshotKey(snapshotID))
	if err != nil || !found {
		return err
	}
	record, err := schema.UnmarshalSnapshotRecord(value)
	if err != nil {
		return err
	}
	rootKey := schema.DirectoryRevisionKey(record.RootFSID, record.RootInode, record.RootRevision)
	var identities []schema.ParsedKey
	if enabled, enabledErr := store.analyticsEnabled(ctx); enabledErr != nil {
		return enabledErr
	} else if enabled {
		identities, err = store.snapshotIdentities(ctx, rootKey)
		if err != nil {
			return err
		}
	}
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		err = store.forgetSnapshotOnce(ctx, snapshotID, record, identities)
		if status.Code(err) != codes.Aborted {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
	return fmt.Errorf("forget snapshot: transaction conflict retry limit exceeded")
}

func (store *SchemaStore) forgetSnapshotOnce(
	ctx context.Context,
	snapshotID schema.ID,
	record schema.SnapshotRecord,
	identities []schema.ParsedKey,
) error {
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	fail := func(err error) error { rollbackTransaction(ctx, transaction); return err }
	if _, found, err := transaction.Get(ctx, schema.SnapshotKey(snapshotID)); err != nil {
		return fail(err)
	} else if !found {
		return transaction.Rollback(ctx)
	}
	next := uint64(1)
	if value, found, err := transaction.Get(ctx, schema.NextRevisionKey()); err != nil {
		return fail(err)
	} else if found {
		next, err = schema.UnmarshalNextRevision(value)
		if err != nil {
			return fail(err)
		}
	}
	if next == math.MaxUint64 {
		return fail(fmt.Errorf("repository revision sequence exhausted"))
	}
	nextValue, err := schema.MarshalNextRevision(next + 1)
	if err != nil {
		return fail(err)
	}
	puts := []Mutation{{Key: schema.NextRevisionKey(), Value: nextValue}}
	if metadataValue, found, getErr := transaction.Get(ctx, schema.AnalyticsMetadataKey()); getErr != nil {
		return fail(getErr)
	} else if found {
		metadata, decodeErr := schema.UnmarshalAnalyticsMetadataRecord(metadataValue)
		if decodeErr == nil && metadata.Enabled {
			for ordinal, identity := range identities {
				delta := schema.AnalyticsDeltaRecord{Kind: schema.AnalyticsDeltaRetainedReferences,
					FSID:                identity.FSID,
					Inode:               identity.Inode,
					IdentityGeneration:  identity.Revision,
					Revision:            next,
					State:               schema.AnalyticsUnknown,
					ReferenceOperation:  schema.AnalyticsReferencesDecrement,
					ClassificationEpoch: metadata.Generation}
				encoded, encodeErr := delta.MarshalBinary()
				if encodeErr != nil {
					return fail(encodeErr)
				}
				puts = append(puts, Mutation{Key: schema.AnalyticsDeltaKey(next, uint32(ordinal)), Value: encoded})
			}
		}
	}
	deletes := [][]byte{schema.SnapshotKey(snapshotID), schema.SnapshotCommitKey(record.CommitSequence, snapshotID)}
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), puts, deletes); err != nil {
		return fail(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return fail(err)
	}
	return nil
}

func (store *SchemaStore) snapshotIdentities(ctx context.Context, rootKey []byte) ([]schema.ParsedKey, error) {
	identities, err := store.snapshotIdentityRevisions(ctx, rootKey, false)
	if err != nil {
		return nil, err
	}
	generations := map[identityKey][]uint64{}
	var after []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, []byte("asb:"), after, 10_000)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			parsed, err := schema.ParseKey(entry.Key)
			if err != nil || parsed.Kind != schema.KeyAuthoritativeSourceBinding {
				return nil, fmt.Errorf("invalid authoritative source binding key %x", entry.Key)
			}
			generations[identityKey{parsed.FSID, parsed.Inode}] = append(
				generations[identityKey{parsed.FSID, parsed.Inode}],
				parsed.Generation,
			)
		}
		if done {
			break
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("authoritative source-binding scan made no progress")
		}
		after = append(after[:0], entries[len(entries)-1].Key...)
	}
	for index := range identities {
		generation := uint64(0)
		for _, candidate := range generations[identityKey{identities[index].FSID, identities[index].Inode}] {
			if candidate <= identities[index].Revision && candidate > generation {
				generation = candidate
			}
		}
		if generation == 0 {
			items, _, err := store.ScanPrefix(
				ctx,
				schema.InodeRevisionPrefix(identities[index].FSID, identities[index].Inode),
				nil,
				1,
			)
			if err != nil || len(items) == 0 {
				return nil, errors.Join(
					err,
					fmt.Errorf("snapshot inode %d:%d has no revision", identities[index].FSID, identities[index].Inode),
				)
			}
			parsed, err := schema.ParseKey(items[0].Key)
			if err != nil {
				return nil, err
			}
			generation = parsed.Revision
		}
		identities[index].Revision = generation
	}
	return identities, nil
}

func (store *SchemaStore) snapshotObservedIdentities(ctx context.Context, rootKey []byte) ([]schema.ParsedKey, error) {
	return store.snapshotIdentityRevisions(ctx, rootKey, false)
}

func (store *SchemaStore) snapshotIdentityRevisions(
	ctx context.Context,
	rootKey []byte,
	oldest bool,
) ([]schema.ParsedKey, error) {
	seenDirectories := map[string]struct{}{}
	identities := map[string]schema.ParsedKey{}
	var visit func([]byte) error
	visit = func(key []byte) error {
		if _, seen := seenDirectories[string(key)]; seen {
			return nil
		}
		seenDirectories[string(key)] = struct{}{}
		value, found, err := store.Get(ctx, key)
		if err != nil || !found {
			return errors.Join(err, fmt.Errorf("snapshot directory %x is missing", key))
		}
		directory, err := schema.UnmarshalDirectoryRevision(value)
		if err != nil {
			return err
		}
		for _, child := range directory.Children {
			parsed, err := schema.ParseKey(child.MetadataKey)
			if err != nil {
				return err
			}
			if parsed.Kind == schema.KeyDirectoryRevision {
				if err := visit(child.MetadataKey); err != nil {
					return err
				}
				continue
			}
			if parsed.Kind != schema.KeyInodeRevision {
				continue
			}
			generation := parsed
			if oldest {
				items, _, err := store.ScanPrefix(ctx, schema.InodeRevisionPrefix(parsed.FSID, parsed.Inode), nil, 1)
				if err != nil || len(items) == 0 {
					return errors.Join(
						err,
						fmt.Errorf("snapshot inode %d:%d has no revision", parsed.FSID, parsed.Inode),
					)
				}
				generation, err = schema.ParseKey(items[0].Key)
				if err != nil {
					return err
				}
			}
			identities[string(schema.AnalyticsResidencyKey(parsed.FSID, parsed.Inode, generation.Revision))] = generation
		}
		return nil
	}
	if err := visit(rootKey); err != nil {
		return nil, err
	}
	result := make([]schema.ParsedKey, 0, len(identities))
	for _, identity := range identities {
		result = append(result, identity)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].FSID != result[j].FSID {
			return result[i].FSID < result[j].FSID
		}
		if result[i].Inode != result[j].Inode {
			return result[i].Inode < result[j].Inode
		}
		return result[i].Revision < result[j].Revision
	})
	return result, nil
}

func (store *SchemaStore) analyticsEnabled(ctx context.Context) (bool, error) {
	value, found, err := store.Get(ctx, schema.AnalyticsMetadataKey())
	if err != nil || !found {
		return false, err
	}
	metadata, err := schema.UnmarshalAnalyticsMetadataRecord(value)
	if err != nil {
		return false, nil
	}
	return metadata.Enabled, nil
}

func snapshotTimeUnixNano(originalJSON []byte) int64 {
	var decoded struct {
		Time time.Time `json:"time"`
	}
	if len(originalJSON) == 0 || json.Unmarshal(originalJSON, &decoded) != nil || decoded.Time.IsZero() {
		return 0
	}
	return decoded.Time.UnixNano()
}

// CreateImmutable creates a historical record or verifies an identical retry.
// It never overwrites an existing key with different bytes.
func (store *SchemaStore) CreateImmutable(ctx context.Context, key, value []byte) error {
	parsed, err := schema.ParseKey(key)
	if err != nil {
		return err
	}
	switch parsed.Kind {
	case schema.KeyBlob, schema.KeyInodeRevision, schema.KeyDirectoryRevision, schema.KeySnapshot:
	case schema.KeyContentManifest:
		return fmt.Errorf("content manifests require CreateContentManifest")
	default:
		return fmt.Errorf("schema key is mutable and cannot use CreateImmutable")
	}
	if err := schema.ValidateValue(key, value); err != nil {
		return err
	}
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	existing, found, err := transaction.Get(ctx, key)
	if err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if found {
		if rollbackErr := rollbackTransaction(ctx, transaction); rollbackErr != nil {
			return rollbackErr
		}
		if !bytes.Equal(existing, value) {
			return fmt.Errorf("immutable schema record already exists with different data")
		}
		return nil
	}
	if err := transaction.WriteBatch(ctx, []Mutation{{Key: key, Value: value}}, nil); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

// CreateContentManifest atomically creates every immutable segment for one
// content-addressed ordered blob sequence, or verifies an identical retry.
func (store *SchemaStore) CreateContentManifest(ctx context.Context, ids []schema.ID) (schema.ID, error) {
	return store.PublishContentManifest(ctx, ids, nil, nil)
}

// PublishContentManifest atomically creates canonical immutable segments and
// applies their mutable reverse-reference updates.
func (store *SchemaStore) PublishContentManifest(
	ctx context.Context,
	ids []schema.ID,
	relatedPuts []Mutation,
	relatedDeletes [][]byte,
) (schema.ID, error) {
	if err := validateRelatedMutations(relatedPuts, relatedDeletes); err != nil {
		return schema.ID{}, err
	}
	manifestID, segments, err := schema.SegmentContent(ids, schema.DefaultContentSegmentIDs)
	if err != nil {
		return schema.ID{}, err
	}
	if _, err := schema.AssembleContent(manifestID, append([]schema.ContentManifest(nil), segments...)); err != nil {
		return schema.ID{}, err
	}
	keys := make([][]byte, len(segments))
	encoded := make([][]byte, len(segments))
	for index, segment := range segments {
		keys[index] = schema.ContentManifestKey(manifestID, uint32(index))
		encoded[index], err = segment.MarshalBinary()
		if err != nil {
			return schema.ID{}, err
		}
	}
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return schema.ID{}, err
	}
	for start := 0; start < len(keys); {
		end, err := manifestBatchEnd(store.client.Limits(), keys, encoded, start)
		if err != nil {
			rollbackTransaction(ctx, transaction)
			return schema.ID{}, err
		}
		values, found, err := transaction.MultiGet(ctx, keys[start:end])
		if err != nil {
			rollbackTransaction(ctx, transaction)
			return schema.ID{}, err
		}
		puts := make([]Mutation, 0, end-start)
		for offset := range keys[start:end] {
			index := start + offset
			if found[offset] {
				if !bytes.Equal(values[offset].Value, encoded[index]) {
					rollbackTransaction(ctx, transaction)
					return schema.ID{}, fmt.Errorf(
						"immutable content manifest segment already exists with different data",
					)
				}
				continue
			}
			puts = append(puts, Mutation{Key: keys[index], Value: encoded[index]})
		}
		if len(puts) > 0 {
			if err := transaction.WriteBatch(ctx, puts, nil); err != nil {
				rollbackTransaction(ctx, transaction)
				return schema.ID{}, err
			}
		}
		start = end
	}
	if len(relatedPuts) > 0 || len(relatedDeletes) > 0 {
		if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), relatedPuts, relatedDeletes); err != nil {
			rollbackTransaction(ctx, transaction)
			return schema.ID{}, err
		}
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return schema.ID{}, err
	}
	return manifestID, nil
}

func manifestBatchEnd(limits Limits, keys, values [][]byte, start int) (int, error) {
	if limits.MaxBatchItems == 0 || limits.MaxMessageBytes < 128 {
		return 0, fmt.Errorf("vaulticdb advertised insufficient manifest batch limits")
	}
	maxBytes := uint64(limits.MaxMessageBytes) / 2
	used := uint64(0)
	end := start
	for end < len(keys) && uint64(end-start) < uint64(limits.MaxBatchItems) {
		size := uint64(len(keys[end])) + uint64(len(values[end])) + 32
		if size > maxBytes {
			return 0, fmt.Errorf("content manifest segment exceeds daemon message limit")
		}
		if used+size > maxBytes {
			break
		}
		used += size
		end++
	}
	if end == start {
		return 0, fmt.Errorf("content manifest batch made no progress")
	}
	return end, nil
}

// PublishRevision atomically creates an immutable revision and advances its
// current pointer. A retry with identical revision bytes is idempotent.
func (store *SchemaStore) PublishRevision(
	ctx context.Context,
	currentKey, revisionKey, revisionValue []byte,
	revision uint64,
) error {
	return store.PublishRevisionBatch(ctx, currentKey, revisionKey, revisionValue, revision, nil, nil)
}

// PublishRevisionBatch atomically creates an immutable revision, advances its
// current pointer, and applies corresponding mutable reverse-reference data.
func (store *SchemaStore) PublishRevisionBatch(
	ctx context.Context,
	currentKey, revisionKey, revisionValue []byte,
	revision uint64,
	relatedPuts []Mutation,
	relatedDeletes [][]byte,
) error {
	if err := validateRelatedMutations(relatedPuts, relatedDeletes); err != nil {
		return err
	}
	current, err := schema.CurrentPointer{Revision: revision, RecordKey: revisionKey}.MarshalBinary()
	if err != nil {
		return err
	}
	currentParsed, err := schema.ParseKey(currentKey)
	if err != nil {
		return err
	}
	parsed, err := schema.ParseKey(revisionKey)
	validPair := currentParsed.FSID == parsed.FSID && currentParsed.Inode == parsed.Inode &&
		((currentParsed.Kind == schema.KeyCurrentInode && parsed.Kind == schema.KeyInodeRevision) ||
			(currentParsed.Kind == schema.KeyCurrentDirectory && parsed.Kind == schema.KeyDirectoryRevision))
	if err != nil || parsed.Revision != revision || !validPair {
		return fmt.Errorf("revision key does not match revision %d", revision)
	}
	if err := schema.ValidateValue(revisionKey, revisionValue); err != nil {
		return err
	}
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	existing, found, err := transaction.Get(ctx, revisionKey)
	if err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if found && !bytes.Equal(existing, revisionValue) {
		rollbackTransaction(ctx, transaction)
		return fmt.Errorf("immutable revision already exists with different data")
	}
	currentValue, currentFound, err := transaction.Get(ctx, currentKey)
	if err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if currentFound {
		pointer, decodeErr := schema.UnmarshalCurrentPointer(currentValue)
		if decodeErr != nil {
			rollbackTransaction(ctx, transaction)
			return decodeErr
		}
		if pointer.Revision > revision {
			rollbackTransaction(ctx, transaction)
			return fmt.Errorf("current revision %d is newer than %d", pointer.Revision, revision)
		}
	}
	puts := []Mutation{{Key: currentKey, Value: current}}
	if !found {
		puts = append(puts, Mutation{Key: revisionKey, Value: revisionValue})
	}
	puts = append(puts, relatedPuts...)
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), puts, relatedDeletes); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

// PublishReconciledRevision atomically publishes a verified metadata revision,
// content manifests, reverse references, reference counters, and debt
// resolution. Serializable conflicts retry the complete read/modify/write.
func (store *SchemaStore) PublishReconciledRevision(ctx context.Context, reconciled ReconciledRevision) error {
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		err := store.publishReconciledRevisionOnce(ctx, reconciled)
		if status.Code(err) != codes.Aborted {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
	return fmt.Errorf("publish reconciled revision: transaction conflict retry limit exceeded")
}
