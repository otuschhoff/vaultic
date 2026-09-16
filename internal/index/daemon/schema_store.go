package daemon

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const revisionAllocationAttempts = 128

const (
	freshImportInitialCapacity = 8_000_000
	freshImportSeenMaxBytes    = 1536 << 20
	freshImportFalsePositive   = 0.001
)

const bulkImportCompleteKey = "_vaultic/bulk-import-complete-v1"

const legacyDurationBuckets = 64

type DurationDistribution struct {
	Count uint64
	Sum   time.Duration
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
}

type lockedDurationHistogram struct {
	mu     sync.Mutex
	counts [legacyDurationBuckets]uint64
	count  uint64
	sum    uint64
}

func (histogram *lockedDurationHistogram) observe(elapsed time.Duration) {
	nanos := uint64(max(elapsed, 0))
	bucket := min(bits.Len64(nanos), legacyDurationBuckets-1)
	histogram.mu.Lock()
	defer histogram.mu.Unlock()
	histogram.counts[bucket]++
	histogram.count++
	histogram.sum += nanos
}

func (histogram *lockedDurationHistogram) snapshot() DurationDistribution {
	histogram.mu.Lock()
	defer histogram.mu.Unlock()
	return DurationDistribution{
		Count: histogram.count, Sum: time.Duration(histogram.sum),
		P50: durationQuantile(histogram.counts, histogram.count, 50),
		P95: durationQuantile(histogram.counts, histogram.count, 95),
		P99: durationQuantile(histogram.counts, histogram.count, 99),
	}
}

func durationQuantile(counts [legacyDurationBuckets]uint64, count, percent uint64) time.Duration {
	if count == 0 {
		return 0
	}
	target := (count*percent + 99) / 100
	var cumulative uint64
	for bucket, bucketCount := range counts {
		cumulative += bucketCount
		if cumulative < target {
			continue
		}
		if bucket == 0 {
			return 0
		}
		if bucket == legacyDurationBuckets-1 {
			return time.Duration(math.MaxInt64)
		}
		return time.Duration((uint64(1) << bucket) - 1)
	}
	return time.Duration(math.MaxInt64)
}

type legacyOperationMetrics struct {
	prepare, hash, hints, ingestBegin, receiptRead, packRead, blobRead lockedDurationHistogram
	planBuild, mutationRPC, commit, postCommit, ingestRecoveryRead     lockedDurationHistogram
	reduceCheckpointRead, ingestRetryBackoff                           lockedDurationHistogram
	reduceReceiptRead, reduceAggregateRead, reduceHistoryRead          lockedDurationHistogram
	reduceBegin, reduceEncode, reduceMutationRPC, reduceCommit         lockedDurationHistogram
	reduceRecoveryRead, reduceRetryBackoff                             lockedDurationHistogram
}

func (metrics *legacyOperationMetrics) snapshots() map[string]DurationDistribution {
	return map[string]DurationDistribution{
		"prepare": metrics.prepare.snapshot(), "hash": metrics.hash.snapshot(), "hints": metrics.hints.snapshot(),
		"ingest_begin": metrics.ingestBegin.snapshot(), "receipt_read": metrics.receiptRead.snapshot(),
		"pack_read": metrics.packRead.snapshot(), "blob_read": metrics.blobRead.snapshot(),
		"plan_build": metrics.planBuild.snapshot(), "mutation_rpc": metrics.mutationRPC.snapshot(),
		"commit": metrics.commit.snapshot(), "post_commit": metrics.postCommit.snapshot(),
		"ingest_recovery_read": metrics.ingestRecoveryRead.snapshot(), "reduce_receipt_read": metrics.reduceReceiptRead.snapshot(),
		"reduce_checkpoint_read": metrics.reduceCheckpointRead.snapshot(),
		"reduce_aggregate_plan":  metrics.reduceAggregateRead.snapshot(), "reduce_history_plan": metrics.reduceHistoryRead.snapshot(),
		"reduce_encode": metrics.reduceEncode.snapshot(), "reduce_mutation_rpc": metrics.reduceMutationRPC.snapshot(),
		"reduce_begin": metrics.reduceBegin.snapshot(), "reduce_commit": metrics.reduceCommit.snapshot(),
		"ingest_retry_backoff": metrics.ingestRetryBackoff.snapshot(),
		"reduce_recovery_read": metrics.reduceRecoveryRead.snapshot(), "reduce_retry_backoff": metrics.reduceRetryBackoff.snapshot(),
	}
}

// SchemaStore applies the Vaultic schema's immutability and revision rules over
// the bounded daemon client.
type SchemaStore struct {
	client             *Client
	publicationMu      sync.RWMutex
	legacyImportGate   chan struct{}
	freshImportSeen    *idSeenFilter
	legacySplitMu      sync.Mutex
	legacySplitAuth    map[schema.ID]struct{}
	legacyMetrics      legacyImportMetrics
	deferLegacyCleanup bool
}

type legacyImportMetrics struct {
	operations                   legacyOperationMetrics
	batches                      atomic.Uint64
	ingestedBatches              atomic.Uint64
	reducedBatches               atomic.Uint64
	attempts                     atomic.Uint64
	ingestAttempts               atomic.Uint64
	reduceAttempts               atomic.Uint64
	ingestFailures               atomic.Uint64
	reduceFailures               atomic.Uint64
	commits                      atomic.Uint64
	retries                      atomic.Uint64
	conflicts                    atomic.Uint64
	packsCommitted               atomic.Uint64
	blobsCommitted               atomic.Uint64
	mutationsCommitted           atomic.Uint64
	encodedBytesCommitted        atomic.Uint64
	mutationRPCs                 atomic.Uint64
	mutationRPCAttempts          atomic.Uint64
	reductionMutationRPCs        atomic.Uint64
	reductionMutationRPCAttempts atomic.Uint64
	reductionMutations           atomic.Uint64
	receiptReads                 atomic.Uint64
	reductionReceiptReads        atomic.Uint64
	recoveryReads                atomic.Uint64
	reduceCheckpointReads        atomic.Uint64
	catalogReadRPCs              atomic.Uint64
	catalogReadKeys              atomic.Uint64
	reductionPlanReadRPCs        atomic.Uint64
	reductionPlanReadKeys        atomic.Uint64
	mutationRPCNanos             atomic.Uint64
	planningReads                atomic.Uint64
	replannedBytes               atomic.Uint64
	sourceIndexes                atomic.Uint64
	definitelyAbsent             atomic.Uint64
	possiblyPresent              atomic.Uint64
	found                        atomic.Uint64
	gateWaitNanos                atomic.Uint64
	planningNanos                atomic.Uint64
	reductionNanos               atomic.Uint64
	commitNanos                  atomic.Uint64
	totalNanos                   atomic.Uint64
	cleanupCalls                 atomic.Uint64
	cleanupPages                 atomic.Uint64
	cleanupReceipts              atomic.Uint64
	cleanupNanos                 atomic.Uint64
	cleanupScanNanos             atomic.Uint64
	cleanupBeginNanos            atomic.Uint64
	cleanupWriteNanos            atomic.Uint64
	cleanupCommitNanos           atomic.Uint64
	cleanupDeferredCommits       atomic.Uint64
}

// LegacyImportStats is a process-local, low-cardinality snapshot of bulk
// import activity. Phase 34 can export these fields without changing import behavior.
type LegacyImportStats struct {
	Operations                     map[string]DurationDistribution
	Batches                        uint64
	IngestedBatches                uint64
	ReducedBatches                 uint64
	Attempts                       uint64
	IngestAttempts                 uint64
	ReduceAttempts                 uint64
	IngestFailures                 uint64
	ReduceFailures                 uint64
	Commits                        uint64
	Retries                        uint64
	Conflicts                      uint64
	PacksCommitted                 uint64
	BlobsCommitted                 uint64
	MutationsCommitted             uint64
	EncodedBytesCommitted          uint64
	MutationRPCs                   uint64
	MutationRPCAttempts            uint64
	ReductionMutationRPCs          uint64
	ReductionMutationRPCAttempts   uint64
	ReductionMutations             uint64
	ReceiptReads                   uint64
	ReductionReceiptReads          uint64
	RecoveryReads                  uint64
	ReduceCheckpointReads          uint64
	CatalogReadRPCs                uint64
	CatalogReadKeys                uint64
	ReductionPlanReadRPCs          uint64
	ReductionPlanReadKeys          uint64
	MutationRPCTime                time.Duration
	PlanningReads                  uint64
	ReplannedBytes                 uint64
	SourceIndexesCommitted         uint64
	DefinitelyAbsentLookups        uint64
	PossiblyPresentLookups         uint64
	FoundLookups                   uint64
	FalsePositiveEquivalentLookups uint64
	GateWait                       time.Duration
	PlanningTime                   time.Duration
	ReductionTime                  time.Duration
	CommitTime                     time.Duration
	CleanupCalls                   uint64
	CleanupPages                   uint64
	CleanupReceipts                uint64
	CleanupTime                    time.Duration
	CleanupScanTime                time.Duration
	CleanupBeginTime               time.Duration
	CleanupWriteTime               time.Duration
	CleanupCommitTime              time.Duration
	CleanupDeferredCommits         uint64
	// TotalTime is aggregate importer transaction time across attempts/lanes,
	// not wall-clock elapsed time.
	TotalTime                time.Duration
	FilterLayers             uint64
	FilterBytes              uint64
	FilterInserts            uint64
	FilterFalsePositive      float64
	FilterFallbackToDatabase bool
	FilterLayerOccupancy     []float64
}

func (store *SchemaStore) LegacyImportStats() LegacyImportStats {
	metrics := &store.legacyMetrics
	result := LegacyImportStats{
		Operations: metrics.operations.snapshots(),
		Batches:    metrics.batches.Load(), Attempts: metrics.attempts.Load(), Commits: metrics.commits.Load(),
		IngestAttempts: metrics.ingestAttempts.Load(), ReduceAttempts: metrics.reduceAttempts.Load(),
		IngestFailures: metrics.ingestFailures.Load(), ReduceFailures: metrics.reduceFailures.Load(),
		IngestedBatches: metrics.ingestedBatches.Load(), ReducedBatches: metrics.reducedBatches.Load(),
		Retries: metrics.retries.Load(), Conflicts: metrics.conflicts.Load(),
		PacksCommitted: metrics.packsCommitted.Load(), BlobsCommitted: metrics.blobsCommitted.Load(),
		MutationsCommitted: metrics.mutationsCommitted.Load(), EncodedBytesCommitted: metrics.encodedBytesCommitted.Load(),
		MutationRPCs: metrics.mutationRPCs.Load(), MutationRPCAttempts: metrics.mutationRPCAttempts.Load(),
		MutationRPCTime:       time.Duration(metrics.mutationRPCNanos.Load()),
		ReductionMutationRPCs: metrics.reductionMutationRPCs.Load(), ReductionMutations: metrics.reductionMutations.Load(),
		ReductionMutationRPCAttempts: metrics.reductionMutationRPCAttempts.Load(),
		ReceiptReads:                 metrics.receiptReads.Load(), ReductionReceiptReads: metrics.reductionReceiptReads.Load(),
		RecoveryReads: metrics.recoveryReads.Load(), ReduceCheckpointReads: metrics.reduceCheckpointReads.Load(),
		CatalogReadRPCs: metrics.catalogReadRPCs.Load(), CatalogReadKeys: metrics.catalogReadKeys.Load(),
		ReductionPlanReadRPCs: metrics.reductionPlanReadRPCs.Load(), ReductionPlanReadKeys: metrics.reductionPlanReadKeys.Load(),
		PlanningReads: metrics.planningReads.Load(), ReplannedBytes: metrics.replannedBytes.Load(),
		SourceIndexesCommitted:  metrics.sourceIndexes.Load(),
		DefinitelyAbsentLookups: metrics.definitelyAbsent.Load(), PossiblyPresentLookups: metrics.possiblyPresent.Load(),
		FoundLookups: metrics.found.Load(), GateWait: time.Duration(metrics.gateWaitNanos.Load()),
		PlanningTime: time.Duration(metrics.planningNanos.Load()), ReductionTime: time.Duration(metrics.reductionNanos.Load()),
		CommitTime:   time.Duration(metrics.commitNanos.Load()),
		TotalTime:    time.Duration(metrics.totalNanos.Load()),
		CleanupCalls: metrics.cleanupCalls.Load(), CleanupPages: metrics.cleanupPages.Load(),
		CleanupReceipts: metrics.cleanupReceipts.Load(), CleanupTime: time.Duration(metrics.cleanupNanos.Load()),
		CleanupScanTime: time.Duration(metrics.cleanupScanNanos.Load()), CleanupBeginTime: time.Duration(metrics.cleanupBeginNanos.Load()),
		CleanupWriteTime: time.Duration(metrics.cleanupWriteNanos.Load()), CleanupCommitTime: time.Duration(metrics.cleanupCommitNanos.Load()),
		CleanupDeferredCommits: metrics.cleanupDeferredCommits.Load(),
	}
	if result.PossiblyPresentLookups > result.FoundLookups {
		result.FalsePositiveEquivalentLookups = result.PossiblyPresentLookups - result.FoundLookups
	}
	if store.freshImportSeen != nil {
		filter := store.freshImportSeen.stats()
		result.FilterLayers, result.FilterBytes, result.FilterInserts = filter.Layers, filter.Bytes, filter.Inserts
		result.FilterFalsePositive, result.FilterFallbackToDatabase = filter.EstimatedFalsePositive, filter.FallbackToDatabase
		result.FilterLayerOccupancy = append([]float64(nil), filter.LayerOccupancy...)
	}
	return result
}

// MarkBulkImportComplete durably authorizes the temporary memory WAL to hand
// the completed metadata generation to its configured local WAL on shutdown.
func (store *SchemaStore) MarkBulkImportComplete(ctx context.Context) error {
	acknowledged, err := store.client.WriteBatch(
		ctx,
		[]Mutation{{Key: []byte(bulkImportCompleteKey), Value: []byte("complete")}},
		nil,
		true,
		"",
	)
	if err != nil {
		return err
	}
	if !acknowledged {
		return errors.New("vaulticdb did not durably acknowledge bulk-import completion")
	}
	return nil
}

// EnableFreshLegacyImport skips reads only for IDs not previously committed by
// this store. It is safe only for a candidate reset to empty immediately before import.
func (s *SchemaStore) EnableFreshLegacyImport() {
	s.freshImportSeen = newIDSeenFilter(freshImportInitialCapacity, freshImportSeenMaxBytes, freshImportFalsePositive)
	s.legacySplitMu.Lock()
	if s.legacySplitAuth == nil {
		s.legacySplitAuth = make(map[schema.ID]struct{})
	}
	s.legacySplitMu.Unlock()
}

func (s *SchemaStore) EnableDeferredLegacyImportCleanup() error {
	if s.freshImportSeen == nil {
		return ErrLegacyImportFreshRequired
	}
	s.deferLegacyCleanup = true
	return nil
}

// CheckEncryption validates the underlying metadata objects without exposing keys.
func (s *SchemaStore) CheckEncryption(ctx context.Context) (EncryptionAudit, error) {
	return s.client.CheckEncryption(ctx)
}

type LegacyPackImport struct {
	SourceIndex      schema.ID
	PackID           schema.ID
	Record           schema.PackRecord
	Blobs            map[schema.ID]schema.BlobRecord
	Placements       map[uint64]schema.PlacementRecord
	BatchSize        uint32
	TransactionBytes uint64
	DebtKey          []byte
	Debt             *schema.CrawlDebtRecord
	// RunID groups the history events emitted by one operator-visible run.
	RunID schema.ID
	// PredecessorPackIDs records repack lineage when this pack replaces others.
	PredecessorPackIDs []schema.ID
	LineageKind        schema.RepackLineageKind
}

// PublishedPack contains the catalog and blob locations generated by a new
// backup pack. Unlike legacy imports it has no source JSON index.
type PublishedPack struct {
	PackID schema.ID
	Record schema.PackRecord
	Blobs  map[schema.ID]schema.BlobRecord
	// Placements records where the pack bytes physically live, keyed by backend
	// hash. These are written atomically with the pack transition that produced
	// them.
	Placements map[uint64]schema.PlacementRecord
	// PredecessorPackIDs carries repack lineage when this pack replaces others,
	// so a reader can tell a rewrite from genuine new data.
	PredecessorPackIDs []schema.ID
	LineageKind        schema.RepackLineageKind
	RunID              schema.ID
}

type SnapshotScope struct {
	SnapshotID   schema.ID
	RootKey      []byte
	OriginalJSON []byte
	Crawl        *AuthoritativeCrawlClaim
}

type AuthoritativeCrawlClaim struct {
	ScopeID    schema.ID
	RootFSID   uint32
	RootInode  uint64
	StartFence uint64
	Complete   bool
	DebtKeys   [][]byte
}

type ReconciledRevision struct {
	CurrentKey         []byte
	RevisionKey        []byte
	RevisionValue      []byte
	Revision           uint64
	ContentIDs         []schema.ID
	DebtKeys           [][]byte
	RelatedPuts        []Mutation
	HasMultipleParents bool
	HardlinkParents    []schema.HardlinkParentRef
}

func NewSchemaStore(client *Client) *SchemaStore {
	return &SchemaStore{
		client:           client,
		legacyImportGate: make(chan struct{}, 1),
		legacySplitAuth:  make(map[schema.ID]struct{}),
	}
}

func (store *SchemaStore) LockAnalyticsPublication() {
	store.publicationMu.Lock()
}

func (store *SchemaStore) UnlockAnalyticsPublication() {
	store.publicationMu.Unlock()
}

func (store *SchemaStore) RLockAnalyticsPublication() {
	store.publicationMu.RLock()
}

func (store *SchemaStore) RUnlockAnalyticsPublication() {
	store.publicationMu.RUnlock()
}

func (store *SchemaStore) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if _, err := schema.ParseKey(key); err != nil {
		return nil, false, err
	}
	return store.client.Get(ctx, key, "")
}

// MultiGet reads a bounded set of schema records in request order.
func (store *SchemaStore) MultiGet(ctx context.Context, keys [][]byte) ([]KeyValue, []bool, error) {
	for _, key := range keys {
		if _, err := schema.ParseKey(key); err != nil {
			return nil, nil, err
		}
	}
	return store.client.MultiGet(ctx, keys, "")
}

// ScanPrefix reads one bounded schema page for an exact key prefix.
func (store *SchemaStore) ScanPrefix(ctx context.Context, prefix, afterKey []byte, pageSize uint32) ([]KeyValue, bool, error) {
	if pageSize > store.client.Limits().MaxPageItems {
		pageSize = store.client.Limits().MaxPageItems
	}
	return store.client.ScanPage(ctx, prefix, afterKey, pageSize, "")
}

// MetadataHead returns the highest globally allocated authoritative commit.
func (store *SchemaStore) MetadataHead(ctx context.Context) (uint64, error) {
	value, found, err := store.Get(ctx, schema.NextRevisionKey())
	if err != nil || !found {
		return 0, err
	}
	next, err := schema.UnmarshalNextRevision(value)
	if err != nil {
		return 0, err
	}
	if next == 0 {
		return 0, fmt.Errorf("invalid next metadata revision")
	}
	return next - 1, nil
}

// RecordCrawlDebtFailure leaves debt pending while atomically recording a
// failed reconciliation attempt for each existing debt key.
func (store *SchemaStore) RecordCrawlDebtFailure(ctx context.Context, keys [][]byte, errorClass string) error {
	if errorClass == "" {
		errorClass = "reconciliation-failed"
	}
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		err := store.recordCrawlDebtFailureOnce(ctx, keys, errorClass)
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
	return fmt.Errorf("record crawl-debt failure: transaction conflict retry limit exceeded")
}

// ResolveCrawlDebt atomically marks existing debt records resolved without
// creating an otherwise identical metadata revision.
func (store *SchemaStore) ResolveCrawlDebt(ctx context.Context, keys [][]byte) error {
	backoff := 100 * time.Microsecond
	for range revisionAllocationAttempts {
		err := store.resolveCrawlDebtOnce(ctx, keys)
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
	return fmt.Errorf("resolve crawl debt: transaction conflict retry limit exceeded")
}

func (store *SchemaStore) resolveCrawlDebtOnce(ctx context.Context, keys [][]byte) error {
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	puts := make([]Mutation, 0, len(keys))
	for _, key := range keys {
		parsed, parseErr := schema.ParseKey(key)
		if parseErr != nil || parsed.Kind != schema.KeyCrawlDebt {
			rollbackTransaction(ctx, transaction)
			return fmt.Errorf("invalid crawl-debt key")
		}
		value, found, getErr := transaction.Get(ctx, key)
		if getErr != nil {
			rollbackTransaction(ctx, transaction)
			return getErr
		}
		if !found {
			continue
		}
		debt, decodeErr := schema.UnmarshalCrawlDebtRecord(value)
		if decodeErr != nil {
			rollbackTransaction(ctx, transaction)
			return decodeErr
		}
		if debt.Status == schema.DebtResolved {
			continue
		}
		debt.Status = schema.DebtResolved
		debt.ErrorClass = ""
		debt.LastAttemptUnixNano = time.Now().UnixNano()
		encoded, encodeErr := debt.MarshalBinary()
		if encodeErr != nil {
			rollbackTransaction(ctx, transaction)
			return encodeErr
		}
		puts = append(puts, Mutation{Key: key, Value: encoded})
	}
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), puts, nil); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

func (store *SchemaStore) recordCrawlDebtFailureOnce(ctx context.Context, keys [][]byte, errorClass string) error {
	transaction, err := store.client.Begin(ctx)
	if err != nil {
		return err
	}
	puts := make([]Mutation, 0, len(keys))
	for _, key := range keys {
		parsed, parseErr := schema.ParseKey(key)
		if parseErr != nil || parsed.Kind != schema.KeyCrawlDebt {
			rollbackTransaction(ctx, transaction)
			return fmt.Errorf("invalid crawl-debt key")
		}
		value, found, getErr := transaction.Get(ctx, key)
		if getErr != nil {
			rollbackTransaction(ctx, transaction)
			return getErr
		}
		if !found {
			continue
		}
		debt, decodeErr := schema.UnmarshalCrawlDebtRecord(value)
		if decodeErr != nil {
			rollbackTransaction(ctx, transaction)
			return decodeErr
		}
		if debt.Status == schema.DebtResolved {
			continue
		}
		if debt.RetryCount < math.MaxUint32 {
			debt.RetryCount++
		}
		debt.Status = schema.DebtPending
		debt.LastAttemptUnixNano = time.Now().UnixNano()
		debt.ErrorClass = errorClass
		encoded, encodeErr := debt.MarshalBinary()
		if encodeErr != nil {
			rollbackTransaction(ctx, transaction)
			return encodeErr
		}
		puts = append(puts, Mutation{Key: key, Value: encoded})
	}
	if err := writeTransactionBatches(ctx, transaction, store.client.Limits(), puts, nil); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		rollbackTransaction(ctx, transaction)
		return err
	}
	return nil
}

// ImportLegacyPacks atomically merges a bounded ordered group of legacy packs
// and, when provided, the checkpoint for the completed source index.
func (store *SchemaStore) ImportLegacyPacks(
	ctx context.Context,
	imports []LegacyPackImport,
	finalCheckpoint *Mutation,
) error {
	started := time.Now()
	defer func() { store.legacyMetrics.totalNanos.Add(uint64(time.Since(started))) }()
	prepared, err := prepareLegacyImportBatch(imports, finalCheckpoint)
	if err != nil {
		return err
	}
	if len(prepared) == 0 && finalCheckpoint == nil {
		return nil
	}
	if err := store.acquireLegacyImportGate(ctx); err != nil {
		return err
	}
	defer func() { <-store.legacyImportGate }()
	store.legacyMetrics.batches.Add(1)
	return store.importLegacyPacksWithRetry(ctx, prepared, finalCheckpoint)
}

func prepareLegacyImportBatch(imports []LegacyPackImport, finalCheckpoint *Mutation) ([]LegacyPackImport, error) {
	prepared := append([]LegacyPackImport(nil), imports...)
	for index := range prepared {
		if err := preparePackImport(&prepared[index], true); err != nil {
			return nil, fmt.Errorf("prepare legacy pack %d: %w", index, err)
		}
		if err := validateLegacyImportDebt(prepared[index]); err != nil {
			return nil, fmt.Errorf("prepare legacy pack %d: %w", index, err)
		}
		if index > 0 && prepared[index].TransactionBytes != prepared[0].TransactionBytes {
			return nil, fmt.Errorf("legacy import packs have inconsistent transaction byte limits")
		}
	}
	if err := validateLegacyImportCheckpoint(finalCheckpoint); err != nil {
		return nil, err
	}
	return prepared, nil
}

func validateLegacyImportDebt(imported LegacyPackImport) error {
	if imported.Debt == nil {
		return nil
	}
	if len(imported.DebtKey) == 0 {
		return fmt.Errorf("legacy pack debt requires a key")
	}
	value, err := imported.Debt.MarshalBinary()
	if err != nil {
		return err
	}
	return schema.ValidateValue(imported.DebtKey, value)
}

func (store *SchemaStore) acquireLegacyImportGate(ctx context.Context) error {
	gateStarted := time.Now()
	select {
	case store.legacyImportGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	store.legacyMetrics.gateWaitNanos.Add(uint64(time.Since(gateStarted)))
	return nil
}

func (store *SchemaStore) importLegacyPacksWithRetry(
	ctx context.Context,
	prepared []LegacyPackImport,
	finalCheckpoint *Mutation,
) error {
	hints := store.freshImportLookupHintsForBatch(prepared)
	deferDurability := store.freshImportSeen != nil
	backoff := 100 * time.Microsecond
	for attempt := range revisionAllocationAttempts {
		store.legacyMetrics.attempts.Add(1)
		attemptHints := hints
		if attempt > 0 {
			// A conflicting writer may have committed an ID that the process-local
			// fresh-import filter has never seen. Retried plans therefore read all
			// relevant keys from their new transaction snapshot.
			attemptHints = legacyImportBatchHints{}
		}
		err := store.importLegacyPacksOnce(ctx, prepared, finalCheckpoint, attemptHints, deferDurability, attempt > 0)
		if status.Code(err) != codes.Aborted {
			if err == nil && store.freshImportSeen != nil {
				packIDs, blobIDs := uniqueLegacyImportIDs(prepared)
				for _, packID := range packIDs {
					store.freshImportSeen.insert(packID)
				}
				for _, blobID := range blobIDs {
					store.freshImportSeen.insert(blobID)
				}
			}
			if err == nil {
				store.legacyMetrics.commits.Add(1)
				packIDs, blobIDs := uniqueLegacyImportIDs(prepared)
				store.legacyMetrics.packsCommitted.Add(uint64(len(packIDs)))
				store.legacyMetrics.blobsCommitted.Add(uint64(len(blobIDs)))
				store.legacyMetrics.sourceIndexes.Add(uniqueLegacySourceIndexCount(prepared))
			}
			return err
		}
		store.legacyMetrics.conflicts.Add(1)
		store.legacyMetrics.retries.Add(1)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, 25*time.Millisecond)
	}
	return fmt.Errorf("import legacy packs: %w", errors.New("transaction conflict retry limit exceeded"))
}

func uniqueLegacySourceIndexCount(imports []LegacyPackImport) uint64 {
	ids := make(map[schema.ID]struct{}, len(imports))
	for _, imported := range imports {
		ids[imported.SourceIndex] = struct{}{}
	}
	return uint64(len(ids))
}

// ImportLegacyPack preserves the one-pack API for callers that do not use the
// bulk importer while sharing the same transaction planner.
func (store *SchemaStore) ImportLegacyPack(ctx context.Context, imported LegacyPackImport) error {
	return store.ImportLegacyPacks(ctx, []LegacyPackImport{imported}, nil)
}
