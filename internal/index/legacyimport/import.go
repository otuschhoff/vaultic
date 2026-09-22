package legacyimport

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

var (
	ErrLimitReached     = errors.New("legacy import limit reached")
	errPackTimeout      = errors.New("legacy pack import timeout")
	errPreparationEnded = errors.New("legacy pack preparation ended before completion")
)

const (
	maxDefaultPackWorkers         = 8
	maxPublicationLanes           = 8
	defaultPacksPerTransaction    = 8
	MaxPacksPerTransaction        = 256
	defaultImportTransactionBytes = 8 << 20
	defaultPreparedImportBytes    = 256 << 20
	defaultPackTimeout            = 5 * time.Minute
	defaultImportBatchTimeout     = 4 * time.Minute
	maxImportBatchTimeout         = 5 * time.Minute
)

type Options struct {
	Resume                  bool
	DryRun                  bool
	PreserveIndexOrder      bool
	PublicationLanes        uint
	BatchSize               uint32
	PackWorkers             uint
	PackTimeout             time.Duration
	PacksPerTransaction     uint
	ImportTransactionBytes  uint64
	PreparedImportBytes     uint64
	ImportBatchTimeout      time.Duration
	MaxErrors               uint64
	WorkBudget              uint64
	SnapshotDepth           uint
	SnapshotWorkBudget      uint64
	DeferSnapshotDurability bool
	Progress                func(Progress)
	Telemetry               *SchedulerTelemetry
}

type Progress struct {
	IndexesCompleted    uint64
	IndexesTotal        uint64
	IndexesImported     uint64
	IndexesResumed      uint64
	SnapshotsCompleted  uint64
	SnapshotsTotal      uint64
	SnapshotsImported   uint64
	SnapshotsResumed    uint64
	PacksImported       uint64
	BlobsImported       uint64
	PacksPrepared       uint64
	PreparedBytes       uint64
	BatchesIngested     uint64
	BatchesReduced      uint64
	QueuedPreparedPacks uint64
	QueuedPreparedBytes uint64
	BatchesCommitted    uint64
	InFlightLanes       uint64
	PeakLanes           uint64
	PeakPreparedPacks   uint64
	PeakPreparedBytes   uint64
	PreparationTime     time.Duration
	IngestTime          time.Duration
	ReductionTime       time.Duration
	PublicationTime     time.Duration
	CheckpointBatchTime time.Duration
	AdaptiveSplits      uint64
	CheckpointPending   bool
	NodesImported       uint64
}

type Finding struct {
	SourceID vaultic.ID `json:"source_id"`
	Stage    string     `json:"stage"`
	Error    string     `json:"error"`
}

type Result struct {
	IndexesTotal          uint64    `json:"indexes_total"`
	IndexesSeen           uint64    `json:"indexes_seen"`
	IndexesImported       uint64    `json:"indexes_imported"`
	IndexesResumed        uint64    `json:"indexes_resumed"`
	PacksImported         uint64    `json:"packs_imported"`
	BlobsImported         uint64    `json:"blobs_imported"`
	PacksPrepared         uint64    `json:"packs_prepared"`
	PreparedBytes         uint64    `json:"prepared_bytes"`
	BatchesIngested       uint64    `json:"batches_ingested"`
	BatchesReduced        uint64    `json:"batches_reduced"`
	BatchesCommitted      uint64    `json:"batches_committed"`
	PeakPublicationLanes  uint64    `json:"peak_publication_lanes"`
	PeakPreparedPacks     uint64    `json:"peak_prepared_packs"`
	PeakPreparedBytes     uint64    `json:"peak_prepared_bytes"`
	PreparationTimeMS     uint64    `json:"preparation_time_ms"`
	IngestTimeMS          uint64    `json:"ingest_time_ms"`
	ReductionTimeMS       uint64    `json:"reduction_time_ms"`
	PublicationTimeMS     uint64    `json:"publication_time_ms"`
	CheckpointBatchTimeMS uint64    `json:"checkpoint_batch_time_ms"`
	AdaptiveSplits        uint64    `json:"adaptive_splits"`
	RecordsSeen           uint64    `json:"records_seen"`
	RecordsImported       uint64    `json:"records_imported"`
	RecordsSkipped        uint64    `json:"records_skipped"`
	CrawlDebtCreated      uint64    `json:"crawl_debt_created"`
	SnapshotsTotal        uint64    `json:"snapshots_total"`
	SnapshotsSeen         uint64    `json:"snapshots_seen"`
	SnapshotsImported     uint64    `json:"snapshots_imported"`
	SnapshotsResumed      uint64    `json:"snapshots_resumed"`
	TreesVisited          uint64    `json:"trees_visited"`
	NodesVisited          uint64    `json:"nodes_visited"`
	NodesImported         uint64    `json:"nodes_imported"`
	WarningsSeen          uint64    `json:"warnings"`
	ErrorsSeen            uint64    `json:"errors"`
	Checkpoint            string    `json:"checkpoint,omitempty"`
	ResetElapsedMS        uint64    `json:"reset_elapsed_ms,omitempty"`
	Findings              []Finding `json:"findings,omitempty"`
}

type Source interface {
	vaultic.Lister
	vaultic.LoaderUnpacked
}

type PackStatter interface {
	Stat(context.Context, backend.Handle) (backend.FileInfo, error)
}

type Store interface {
	Get(context.Context, []byte) ([]byte, bool, error)
	ImportLegacyPacks(context.Context, []daemon.LegacyPackImport, *daemon.Mutation) error
	Put(context.Context, []byte, []byte, bool) error
}

type SplitStore interface {
	Store
	IngestLegacyPacks(context.Context, schema.ID, uint64, []daemon.LegacyPackImport) error
	IngestLegacyPacksCheckpoints(context.Context, schema.ID, uint64, []daemon.LegacyPackImport, []daemon.Mutation) error
	ReduceLegacyImportBatch(context.Context, schema.ID, uint64, *daemon.Mutation) error
	ReduceLegacyImportBatchCheckpoints(context.Context, schema.ID, uint64, []daemon.Mutation) error
	ReduceLegacyImportBatchesCheckpoints(context.Context, schema.ID, []uint64, []daemon.Mutation) error
	CompleteLegacyImportSession(context.Context, schema.ID) error
}

type packImportResult struct {
	imported  daemon.LegacyPackImport
	debt      *schema.CrawlDebtRecord
	bytes     uint64
	mutations uint64
	err       error
	complete  bool
}

type checkpointBatchError struct{ err error }

func (err *checkpointBatchError) Error() string { return err.err.Error() }
func (err *checkpointBatchError) Unwrap() error { return err.err }

type packPreparation struct {
	index         int
	reservedBytes uint64
	outcome       packImportResult
}

type packJob struct {
	index         int
	reservedBytes uint64
}

type packBatch struct {
	items     []packPreparation
	bytes     uint64
	mutations uint64
}

type packPipelineOptions struct {
	workers             uint
	packTimeout         time.Duration
	packsPerTransaction uint
	transactionBytes    uint64
	preparedBytes       uint64
}

type packPipelineCounters struct {
	preparedPacks        atomic.Uint64
	preparedBytes        atomic.Uint64
	totalPreparedPacks   atomic.Uint64
	totalPreparedBytes   atomic.Uint64
	committedPacks       atomic.Uint64
	committedBlobs       atomic.Uint64
	ingestedBatches      atomic.Uint64
	reducedBatches       atomic.Uint64
	committedBatches     atomic.Uint64
	activeLanes          atomic.Uint64
	peakLanes            atomic.Uint64
	checkpointPending    atomic.Bool
	peakPreparedPacks    atomic.Uint64
	peakPreparedBytes    atomic.Uint64
	preparationNanos     atomic.Uint64
	ingestNanos          atomic.Uint64
	reductionNanos       atomic.Uint64
	publicationNanos     atomic.Uint64
	checkpointBatchNanos atomic.Uint64
	adaptiveSplits       atomic.Uint64
	report               func(packPipelineStats)
}

type packPipelineStats struct {
	totalPreparedPacks  uint64
	totalPreparedBytes  uint64
	queuedPreparedPacks uint64
	queuedPreparedBytes uint64
	committedPacks      uint64
	committedBlobs      uint64
	ingestedBatches     uint64
	reducedBatches      uint64
	committedBatches    uint64
	inFlightLanes       uint64
	peakLanes           uint64
	checkpointPending   bool
	peakPreparedPacks   uint64
	peakPreparedBytes   uint64
	preparationTime     time.Duration
	ingestTime          time.Duration
	reductionTime       time.Duration
	publicationTime     time.Duration
	checkpointBatchTime time.Duration
	adaptiveSplits      uint64
}

//nolint:funlen,gocognit,gocyclo,nestif // Existing domain flow is an explicit complexity exception; Stage 3 remains gated.
func Import(ctx context.Context, source Source, statter PackStatter, store Store, options Options) (result Result, err error) {
	ownsAction := options.Telemetry.startAction()
	options.Telemetry.phase("source")
	defer func() {
		if ownsAction {
			options.Telemetry.FinishAction(result, err)
		} else {
			options.Telemetry.phase("finalize")
		}
	}()
	if options.PackTimeout < 0 {
		return result, fmt.Errorf("pack timeout must not be negative")
	}
	if options.ImportBatchTimeout < 0 || options.ImportBatchTimeout > maxImportBatchTimeout {
		return result, fmt.Errorf("import batch timeout must be between zero and %s", maxImportBatchTimeout)
	}
	if options.PublicationLanes > maxPublicationLanes {
		return result, fmt.Errorf("publication lanes must not exceed %d", maxPublicationLanes)
	}
	if options.PacksPerTransaction > MaxPacksPerTransaction {
		return result, fmt.Errorf("packs per transaction must not exceed %d", MaxPacksPerTransaction)
	}
	indexList, err := vaultic.MemorizeList(ctx, source, vaultic.IndexFile)
	if err != nil {
		return result, err
	}
	if err := indexList.List(ctx, vaultic.IndexFile, func(vaultic.ID, int64) error {
		result.IndexesTotal++
		return nil
	}); err != nil {
		return result, err
	}
	var snapshotList vaultic.Lister
	splitStore, splitCapable := store.(SplitStore)
	useStage3 := options.PublicationLanes > 1 && !options.DryRun && splitCapable
	if options.SnapshotDepth > 0 || options.SnapshotWorkBudget > 0 {
		snapshotList, err = vaultic.MemorizeList(ctx, source, vaultic.SnapshotFile)
		if err != nil {
			return result, err
		}
		if err := snapshotList.List(ctx, vaultic.SnapshotFile, func(vaultic.ID, int64) error {
			result.SnapshotsTotal++
			return nil
		}); err != nil {
			return result, err
		}
	}
	var preparationTime, ingestTime, reductionTime, publicationTime, checkpointBatchTime time.Duration
	var checkpointPending bool
	var inFlightLanes, peakLanes uint64
	reportProgress := func() {
		progress := Progress{
			IndexesCompleted: result.IndexesSeen, IndexesTotal: result.IndexesTotal,
			IndexesImported: result.IndexesImported, IndexesResumed: result.IndexesResumed,
			SnapshotsCompleted: result.SnapshotsSeen, SnapshotsTotal: result.SnapshotsTotal,
			SnapshotsImported: result.SnapshotsImported, SnapshotsResumed: result.SnapshotsResumed,
			PacksImported: result.PacksImported, BlobsImported: result.BlobsImported,
			PacksPrepared: result.PacksPrepared, PreparedBytes: result.PreparedBytes,
			BatchesIngested: result.BatchesIngested, BatchesReduced: result.BatchesReduced,
			BatchesCommitted: result.BatchesCommitted, InFlightLanes: inFlightLanes, PeakLanes: peakLanes,
			PeakPreparedPacks: result.PeakPreparedPacks, PeakPreparedBytes: result.PeakPreparedBytes,
			PreparationTime: preparationTime, IngestTime: ingestTime, ReductionTime: reductionTime,
			PublicationTime: publicationTime, CheckpointBatchTime: checkpointBatchTime,
			AdaptiveSplits: result.AdaptiveSplits, CheckpointPending: checkpointPending, NodesImported: result.NodesImported,
		}
		options.Telemetry.progress(progress)
		if options.Progress != nil {
			options.Progress(progress)
		}
	}
	reportProgress()
	if useStage3 && options.WorkBudget == 0 && options.SnapshotDepth == 0 && options.SnapshotWorkBudget == 0 {
		stats, groupedErr := importGroupedStage3Indexes(
			ctx, indexList, source, statter, splitStore, options, &result, reportProgress,
		)
		applyGroupedStage3Stats(&result, stats)
		preparationTime = stats.preparationTime
		ingestTime = stats.ingestTime
		reductionTime = stats.reductionTime
		publicationTime = stats.publicationTime
		checkpointBatchTime = stats.checkpointBatchTime
		checkpointPending = stats.checkpointPending
		inFlightLanes = stats.inFlightLanes
		peakLanes = stats.peakLanes
		reportProgress()
		return result, groupedErr
	}
	var workUsed uint64
	forAllIndexes := legacyindex.ForAllIndexes
	if options.PreserveIndexOrder {
		forAllIndexes = legacyindex.ForAllIndexesInOrder
	}
	err = forAllIndexes(ctx, indexList, source, func(indexID vaultic.ID, index *legacyindex.Index, loadErr error) error {
		result.IndexesSeen++
		defer reportProgress()
		if loadErr != nil {
			return recordFinding(&result, options, indexID, "decode-index", loadErr)
		}
		schemaIndexID := schema.ID(indexID)
		if options.Resume {
			value, found, err := store.Get(ctx, schema.ImportCheckpointKey(schemaIndexID))
			if err != nil {
				return fmt.Errorf("read import checkpoint for %s: %w", indexID.Str(), err)
			}
			if found {
				if _, err := schema.UnmarshalImportCheckpointRecord(value); err != nil {
					return fmt.Errorf("decode import checkpoint for %s: %w", indexID.Str(), err)
				}
				if useStage3 {
					if err := splitStore.CompleteLegacyImportSession(ctx, schemaIndexID); err != nil {
						return fmt.Errorf("cleanup split-session receipts for %s: %w", indexID.Str(), err)
					}
				}
				result.IndexesResumed++
				result.Checkpoint = indexID.String()
				return nil
			}
		}

		packs := collectPacks(ctx, index)
		if err := ctx.Err(); err != nil {
			return err
		}
		selected := packs

		limitReached := false
		for packIndex, indexedPack := range packs {
			work := uint64(len(indexedPack.Blobs))
			if options.WorkBudget > 0 && workUsed+work > options.WorkBudget {
				selected = packs[:packIndex]
				limitReached = true
				break
			}
			workUsed += work
			result.RecordsSeen += work
		}
		basePacks, baseBlobs := result.PacksImported, result.BlobsImported
		basePrepared, basePreparedBytes := result.PacksPrepared, result.PreparedBytes
		basePreparationTime, baseIngestTime := preparationTime, ingestTime
		baseReductionTime, basePublicationTime := reductionTime, publicationTime
		baseCheckpointBatchTime, baseAdaptiveSplits := checkpointBatchTime, result.AdaptiveSplits
		baseBatchesIngested, baseBatchesReduced := result.BatchesIngested, result.BatchesReduced
		basePeakLanes := result.PeakPublicationLanes
		liveProgress := func(stats packPipelineStats) {
			progress := Progress{
				IndexesCompleted: result.IndexesSeen - 1, IndexesTotal: result.IndexesTotal,
				IndexesImported: result.IndexesImported, IndexesResumed: result.IndexesResumed,
				SnapshotsCompleted: result.SnapshotsSeen, SnapshotsTotal: result.SnapshotsTotal,
				SnapshotsImported: result.SnapshotsImported, SnapshotsResumed: result.SnapshotsResumed,
				PacksPrepared:       basePrepared + stats.totalPreparedPacks,
				PreparedBytes:       basePreparedBytes + stats.totalPreparedBytes,
				BatchesIngested:     baseBatchesIngested + stats.ingestedBatches,
				BatchesReduced:      baseBatchesReduced + stats.reducedBatches,
				QueuedPreparedPacks: stats.queuedPreparedPacks, QueuedPreparedBytes: stats.queuedPreparedBytes,
				PacksImported: basePacks + stats.committedPacks, BlobsImported: baseBlobs + stats.committedBlobs,
				BatchesCommitted:    result.BatchesCommitted + stats.committedBatches,
				InFlightLanes:       stats.inFlightLanes,
				PeakLanes:           max(basePeakLanes, stats.peakLanes),
				PeakPreparedPacks:   max(result.PeakPreparedPacks, stats.peakPreparedPacks),
				PeakPreparedBytes:   max(result.PeakPreparedBytes, stats.peakPreparedBytes),
				PreparationTime:     basePreparationTime + stats.preparationTime,
				IngestTime:          baseIngestTime + stats.ingestTime,
				ReductionTime:       baseReductionTime + stats.reductionTime,
				PublicationTime:     basePublicationTime + stats.publicationTime,
				CheckpointBatchTime: baseCheckpointBatchTime + stats.checkpointBatchTime,
				AdaptiveSplits:      baseAdaptiveSplits + stats.adaptiveSplits, CheckpointPending: stats.checkpointPending,
				NodesImported: result.NodesImported,
			}
			options.Telemetry.progress(progress)
			if options.Progress != nil {
				options.Progress(progress)
			}
		}
		importPacksFn := importPacks
		if useStage3 {
			importPacksFn = func(
				ctx context.Context,
				statter PackStatter,
				_ Store,
				sourceIndex schema.ID,
				packs []legacyindex.PackBlobs,
				options Options,
				checkpointIndex bool,
				report func(packPipelineStats),
			) ([]packImportResult, uint64, packPipelineStats, int) {
				return importPacksStage3(ctx, statter, splitStore, sourceIndex, packs, options, checkpointIndex, report)
			}
		}
		outcomes, batchesCommitted, pipelineStats, failedPack := importPacksFn(
			ctx, statter, store, schemaIndexID, selected, options, !limitReached, liveProgress,
		)
		result.BatchesIngested += pipelineStats.ingestedBatches
		result.BatchesReduced += pipelineStats.reducedBatches
		result.BatchesCommitted += batchesCommitted
		result.PeakPublicationLanes = max(result.PeakPublicationLanes, pipelineStats.peakLanes)
		result.PeakPreparedPacks = max(result.PeakPreparedPacks, pipelineStats.peakPreparedPacks)
		result.PeakPreparedBytes = max(result.PeakPreparedBytes, pipelineStats.peakPreparedBytes)
		preparationTime += pipelineStats.preparationTime
		ingestTime += pipelineStats.ingestTime
		reductionTime += pipelineStats.reductionTime
		publicationTime += pipelineStats.publicationTime
		checkpointBatchTime += pipelineStats.checkpointBatchTime
		result.PreparationTimeMS = uint64(preparationTime / time.Millisecond)
		result.IngestTimeMS = uint64(ingestTime / time.Millisecond)
		result.ReductionTimeMS = uint64(reductionTime / time.Millisecond)
		result.PublicationTimeMS = uint64(publicationTime / time.Millisecond)
		result.CheckpointBatchTimeMS = uint64(checkpointBatchTime / time.Millisecond)
		checkpointPending = pipelineStats.checkpointPending
		inFlightLanes = pipelineStats.inFlightLanes
		peakLanes = max(peakLanes, pipelineStats.peakLanes)
		result.AdaptiveSplits += pipelineStats.adaptiveSplits
		for packIndex, outcome := range outcomes {
			if outcome.imported.PackID != (schema.ID{}) {
				result.PacksPrepared++
				result.PreparedBytes += outcome.bytes
			}
			if !outcome.complete || outcome.err != nil {
				continue
			}
			if outcome.debt != nil {
				result.CrawlDebtCreated++
			}
			result.PacksImported++
			result.BlobsImported += outcome.imported.Record.BlobCount
			result.RecordsImported += outcome.imported.Record.BlobCount
			result.RecordsSkipped += uint64(len(selected[packIndex].Blobs)) - outcome.imported.Record.BlobCount
			if outcome.debt != nil {
				result.WarningsSeen++
				result.Findings = append(result.Findings, Finding{SourceID: indexID, Stage: "stat-pack", Error: outcome.debt.ErrorClass})
			}
		}
		if failedPack >= 0 {
			if failedPack >= len(selected) {
				return fmt.Errorf("publish import checkpoint for index %s: %w", indexID.Str(), outcomes[failedPack].err)
			}
			var checkpointErr *checkpointBatchError
			if errors.As(outcomes[failedPack].err, &checkpointErr) {
				return fmt.Errorf("publish final import batch and checkpoint for index %s: %w", indexID.Str(), checkpointErr)
			}
			return fmt.Errorf("import pack %s from index %s: %w", selected[failedPack].PackID.Str(), indexID.Str(), outcomes[failedPack].err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if limitReached {
			return ErrLimitReached
		}
		if !options.DryRun {
			result.Checkpoint = indexID.String()
		}
		result.IndexesImported++
		return nil
	})
	if err != nil {
		return result, err
	}
	if options.SnapshotDepth > 0 || options.SnapshotWorkBudget > 0 {
		treeSource, ok := source.(SnapshotSource)
		if !ok {
			return result, fmt.Errorf("snapshot import requires blob loading support")
		}
		treeStore, ok := store.(TreeStore)
		if !ok {
			return result, fmt.Errorf("snapshot import requires revision storage support")
		}
		if err := importSnapshots(ctx, snapshotList, treeSource, treeStore, options, &result, reportProgress); err != nil {
			return result, err
		}
	}
	return result, nil
}

func importPacks(
	ctx context.Context,
	statter PackStatter,
	store Store,
	sourceIndex schema.ID,
	packs []legacyindex.PackBlobs,
	options Options,
	checkpointIndex bool,
	report func(packPipelineStats),
) ([]packImportResult, uint64, packPipelineStats, int) {
	outcomes := make([]packImportResult, len(packs))
	counters := &packPipelineCounters{report: report}
	if len(packs) == 0 {
		return importEmptyPackIndex(ctx, store, sourceIndex, options, checkpointIndex, counters)
	}
	pipelineOptions := resolvePackPipelineOptions(options, len(packs))
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan packJob)
	prepared := make(chan packPreparation, pipelineOptions.workers)
	released := make(chan uint64, len(packs))
	var group sync.WaitGroup
	for range pipelineOptions.workers {
		group.Add(1)
		go func() {
			defer group.Done()
			preparePackJobs(
				workerCtx, jobs, prepared, statter, sourceIndex, nil, packs, options, pipelineOptions.packTimeout, counters,
			)
		}()
	}
	go dispatchPackJobs(
		workerCtx, jobs, released, packs, pipelineOptions.preparedBytes, pipelineOptions.packsPerTransaction,
	)
	go func() {
		group.Wait()
		close(prepared)
	}()
	return consumePreparedPacks(
		ctx, cancel, store, sourceIndex, packs, options, checkpointIndex, pipelineOptions,
		prepared, released, outcomes, counters,
	)
}

func importEmptyPackIndex(
	ctx context.Context,
	store Store,
	sourceIndex schema.ID,
	options Options,
	checkpointIndex bool,
	counters *packPipelineCounters,
) ([]packImportResult, uint64, packPipelineStats, int) {
	outcomes := []packImportResult{}
	if !checkpointIndex || options.DryRun {
		return outcomes, 0, counters.snapshot(), -1
	}
	checkpoint, err := importCheckpointMutation(sourceIndex, schema.ImportCheckpointRecord{})
	if err == nil {
		err = publishLegacyImportBatch(ctx, store, nil, &checkpoint, options)
	}
	if err != nil {
		outcomes = append(outcomes, packImportResult{err: err, complete: true})
		return outcomes, 0, counters.snapshot(), 0
	}
	return outcomes, 1, counters.snapshot(), -1
}

func resolvePackPipelineOptions(options Options, packCount int) packPipelineOptions {
	resolved := packPipelineOptions{
		workers: options.PackWorkers, packTimeout: options.PackTimeout,
		packsPerTransaction: options.PacksPerTransaction,
		transactionBytes:    options.ImportTransactionBytes, preparedBytes: options.PreparedImportBytes,
	}
	if resolved.workers == 0 {
		resolved.workers = min(uint(runtime.GOMAXPROCS(0)), maxDefaultPackWorkers)
	}
	resolved.workers = min(resolved.workers, uint(packCount))
	if resolved.packTimeout == 0 {
		resolved.packTimeout = defaultPackTimeout
	}
	if resolved.packsPerTransaction == 0 {
		resolved.packsPerTransaction = defaultPacksPerTransaction
	}
	if resolved.transactionBytes == 0 {
		resolved.transactionBytes = defaultImportTransactionBytes
	}
	if resolved.preparedBytes == 0 {
		resolved.preparedBytes = defaultPreparedImportBytes
	}
	return resolved
}

func consumePreparedPacks(
	ctx context.Context,
	cancel context.CancelFunc,
	store Store,
	sourceIndex schema.ID,
	packs []legacyindex.PackBlobs,
	options Options,
	checkpointIndex bool,
	pipelineOptions packPipelineOptions,
	prepared <-chan packPreparation,
	released chan<- uint64,
	outcomes []packImportResult,
	counters *packPipelineCounters,
) ([]packImportResult, uint64, packPipelineStats, int) {
	pending := make(map[int]packPreparation, pipelineOptions.workers)
	batch := packBatch{items: make([]packPreparation, 0, pipelineOptions.packsPerTransaction)}
	var batchesCommitted uint64
	var checkpoint schema.ImportCheckpointRecord
	next := 0
	for next < len(packs) {
		item, ok := awaitPreparedPack(ctx, prepared, pending, next)
		if !ok {
			cancel()
			outcomes[next].err = preparationEndError(ctx)
			return outcomes, batchesCommitted, counters.snapshot(), next
		}
		outcomes[next] = item.outcome
		if item.outcome.err != nil {
			commits, failed, err := commitPreparedBatch(
				ctx, store, batch.items, nil, options, outcomes, released, counters,
			)
			batchesCommitted += commits
			if err != nil {
				outcomes[failed].err = err
				next = failed
			}
			cancel()
			return outcomes, batchesCommitted, counters.snapshot(), next
		}
		if batch.shouldFlushBefore(item, pipelineOptions) {
			commits, failed, err := commitPreparedBatch(
				ctx, store, batch.items, nil, options, outcomes, released, counters,
			)
			batchesCommitted += commits
			if err != nil {
				outcomes[failed].err = err
				cancel()
				return outcomes, batchesCommitted, counters.snapshot(), failed
			}
			batch.reset()
		}
		batch.add(item)
		checkpoint.PacksImported++
		checkpoint.BlobsImported += item.outcome.imported.Record.BlobCount
		if item.outcome.debt != nil {
			checkpoint.ErrorsSeen++
		}
		next++
		if next == len(packs) || batch.full(pipelineOptions) {
			var finalCheckpoint *daemon.Mutation
			if next == len(packs) && checkpointIndex && !options.DryRun {
				mutation, err := importCheckpointMutation(sourceIndex, checkpoint)
				if err != nil {
					outcomes[batch.items[0].index].err = err
					cancel()
					return outcomes, batchesCommitted, counters.snapshot(), batch.items[0].index
				}
				finalCheckpoint = &mutation
			}
			commits, failed, err := commitPreparedBatch(
				ctx, store, batch.items, finalCheckpoint, options, outcomes, released, counters,
			)
			batchesCommitted += commits
			if err != nil {
				outcomes[failed].err = err
				cancel()
				return outcomes, batchesCommitted, counters.snapshot(), failed
			}
			batch.reset()
		}
	}
	return outcomes, batchesCommitted, counters.snapshot(), -1
}

func preparationEndError(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return errPreparationEnded
}

func awaitPreparedPack(
	ctx context.Context,
	prepared <-chan packPreparation,
	pending map[int]packPreparation,
	next int,
) (packPreparation, bool) {
	for {
		if item, ready := pending[next]; ready {
			delete(pending, next)
			return item, true
		}
		select {
		case item, ok := <-prepared:
			if !ok {
				return packPreparation{}, false
			}
			pending[item.index] = item
		case <-ctx.Done():
			return packPreparation{}, false
		}
	}
}

func (batch *packBatch) shouldFlushBefore(item packPreparation, options packPipelineOptions) bool {
	return batch.flushReasonBefore(item, options) != ""
}

func (batch *packBatch) flushReasonBefore(item packPreparation, options packPipelineOptions) string {
	if len(batch.items) == 0 {
		return ""
	}
	if batch.full(options) {
		return "pack_count"
	}
	if batch.bytes+item.outcome.bytes > options.transactionBytes {
		return "byte_limit"
	}
	if batch.mutations+item.outcome.mutations > daemon.LegacyImportTransactionMutationLimit {
		return "mutation_limit"
	}
	return ""
}

func (batch *packBatch) full(options packPipelineOptions) bool {
	return uint(len(batch.items)) >= options.packsPerTransaction
}

func (batch *packBatch) add(item packPreparation) {
	batch.items = append(batch.items, item)
	batch.bytes += item.outcome.bytes
	batch.mutations += item.outcome.mutations
}

func (batch *packBatch) reset() {
	batch.items = batch.items[:0]
	batch.bytes = 0
	batch.mutations = 0
}

func preparePackJobs(
	ctx context.Context,
	jobs <-chan packJob,
	prepared chan<- packPreparation,
	statter PackStatter,
	sourceIndex schema.ID,
	sourceIndexes []schema.ID,
	packs []legacyindex.PackBlobs,
	options Options,
	packTimeout time.Duration,
	counters *packPipelineCounters,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			started := time.Now()
			packSourceIndex := sourceIndex
			if len(sourceIndexes) == len(packs) {
				packSourceIndex = sourceIndexes[job.index]
			}
			outcome := preparePack(ctx, statter, packSourceIndex, packs[job.index], options, packTimeout)
			counters.preparationNanos.Add(uint64(time.Since(started)))
			currentPacks := counters.preparedPacks.Add(1)
			currentBytes := counters.preparedBytes.Add(outcome.bytes)
			counters.totalPreparedPacks.Add(1)
			counters.totalPreparedBytes.Add(outcome.bytes)
			updateAtomicMaximum(&counters.peakPreparedPacks, currentPacks)
			updateAtomicMaximum(&counters.peakPreparedBytes, currentBytes)
			select {
			case prepared <- packPreparation{index: job.index, reservedBytes: job.reservedBytes, outcome: outcome}:
			case <-ctx.Done():
				counters.preparedPacks.Add(^uint64(0))
				counters.preparedBytes.Add(^uint64(outcome.bytes - 1))
				return
			}
		}
	}
}

func preparePack(
	ctx context.Context,
	statter PackStatter,
	sourceIndex schema.ID,
	indexedPack legacyindex.PackBlobs,
	options Options,
	packTimeout time.Duration,
) packImportResult {
	packCtx, cancel := context.WithTimeoutCause(ctx, packTimeout, errPackTimeout)
	defer cancel()
	imported, debt, err := buildPackImport(packCtx, statter, sourceIndex, indexedPack)
	if err == nil && packCtx.Err() != nil {
		err = packCtx.Err()
	}
	if err == nil {
		imported.BatchSize = options.BatchSize
		imported.TransactionBytes = options.ImportTransactionBytes
	}
	if err != nil && errors.Is(context.Cause(packCtx), errPackTimeout) {
		err = fmt.Errorf("pack import exceeded %s; increase --pack-timeout if the storage backend is healthy: %w", packTimeout, err)
	}
	return packImportResult{
		imported: imported, debt: debt, bytes: estimatePreparedImportBytes(imported),
		mutations: estimateImportMutations(imported), err: err,
	}
}

func dispatchPackJobs(
	ctx context.Context,
	jobs chan<- packJob,
	released <-chan uint64,
	packs []legacyindex.PackBlobs,
	limit uint64,
	minimumInFlight uint,
) {
	defer close(jobs)
	var queued uint64
	var inFlight uint
	for index, indexedPack := range packs {
		reserved := estimateIndexedPackBytes(indexedPack)
		for inFlight >= minimumInFlight && queued > 0 && (reserved > limit || queued > limit-reserved) {
			select {
			case amount := <-released:
				queued -= amount
				inFlight--
			case <-ctx.Done():
				return
			}
		}
		select {
		case jobs <- packJob{index: index, reservedBytes: reserved}:
			queued += reserved
			inFlight++
		case <-ctx.Done():
			return
		}
	}
}

func commitPreparedBatch(
	ctx context.Context,
	store Store,
	batch []packPreparation,
	checkpoint *daemon.Mutation,
	options Options,
	outcomes []packImportResult,
	released chan<- uint64,
	counters *packPipelineCounters,
) (uint64, int, error) {
	if len(batch) == 0 && checkpoint == nil {
		return 0, -1, nil
	}
	imports := make([]daemon.LegacyPackImport, len(batch))
	for index := range batch {
		imports[index] = batch[index].outcome.imported
	}
	if !options.DryRun {
		if err := publishPreparedBatch(ctx, store, imports, checkpoint, options, counters); err != nil {
			return recoverOversizedPreparedBatch(
				ctx, store, batch, checkpoint, options, outcomes, released, counters, err,
			)
		}
		var publishedBytes uint64
		for index := range batch {
			if math.MaxUint64-publishedBytes < batch[index].outcome.bytes {
				publishedBytes = math.MaxUint64
			} else {
				publishedBytes += batch[index].outcome.bytes
			}
		}
		options.Telemetry.processed("database", publishedBytes)
	}
	for _, item := range batch {
		outcome := outcomes[item.index]
		outcome.complete = true
		outcomes[item.index] = outcome
		counters.preparedPacks.Add(^uint64(0))
		counters.preparedBytes.Add(^uint64(item.outcome.bytes - 1))
		released <- item.reservedBytes
	}
	counters.committedPacks.Add(uint64(len(batch)))
	for _, item := range batch {
		counters.committedBlobs.Add(item.outcome.imported.Record.BlobCount)
	}
	if !options.DryRun {
		counters.committedBatches.Add(1)
	}
	counters.checkpointPending.Store(!options.DryRun && checkpoint == nil && len(batch) > 0)
	counters.reportSnapshot()
	if options.DryRun {
		return 0, -1, nil
	}
	return 1, -1, nil
}

func publishPreparedBatch(
	ctx context.Context,
	store Store,
	imports []daemon.LegacyPackImport,
	checkpoint *daemon.Mutation,
	options Options,
	counters *packPipelineCounters,
) error {
	started := time.Now()
	err := publishLegacyImportBatch(ctx, store, imports, checkpoint, options)
	elapsed := uint64(time.Since(started))
	counters.publicationNanos.Add(elapsed)
	if checkpoint != nil {
		counters.checkpointBatchNanos.Add(elapsed)
	}
	var inputErr *daemon.LegacyImportInputError
	if err != nil && checkpoint != nil && !errors.Is(err, daemon.ErrLegacyImportBatchTooLarge) &&
		!errors.As(err, &inputErr) {
		return &checkpointBatchError{err: err}
	}
	return err
}

func recoverOversizedPreparedBatch(
	ctx context.Context,
	store Store,
	batch []packPreparation,
	checkpoint *daemon.Mutation,
	options Options,
	outcomes []packImportResult,
	released chan<- uint64,
	counters *packPipelineCounters,
	publishErr error,
) (uint64, int, error) {
	if !errors.Is(publishErr, daemon.ErrLegacyImportBatchTooLarge) || len(batch) <= 1 {
		return 0, preparedBatchFailureIndex(batch, publishErr), publishErr
	}
	counters.adaptiveSplits.Add(1)
	middle := len(batch) / 2
	leftCommits, failed, err := commitPreparedBatch(
		ctx, store, batch[:middle], nil, options, outcomes, released, counters,
	)
	if err != nil {
		return leftCommits, failed, err
	}
	rightCommits, failed, err := commitPreparedBatch(
		ctx, store, batch[middle:], checkpoint, options, outcomes, released, counters,
	)
	return leftCommits + rightCommits, failed, err
}

func preparedBatchFailureIndex(batch []packPreparation, err error) int {
	var inputErr *daemon.LegacyImportInputError
	if errors.As(err, &inputErr) && inputErr.Index >= 0 && inputErr.Index < len(batch) {
		return batch[inputErr.Index].index
	}
	return batch[0].index
}

func (counters *packPipelineCounters) snapshot() packPipelineStats {
	return packPipelineStats{
		totalPreparedPacks: counters.totalPreparedPacks.Load(), totalPreparedBytes: counters.totalPreparedBytes.Load(),
		queuedPreparedPacks: counters.preparedPacks.Load(), queuedPreparedBytes: counters.preparedBytes.Load(),
		committedPacks: counters.committedPacks.Load(), committedBlobs: counters.committedBlobs.Load(),
		ingestedBatches: counters.ingestedBatches.Load(), reducedBatches: counters.reducedBatches.Load(),
		committedBatches: counters.committedBatches.Load(),
		inFlightLanes:    counters.activeLanes.Load(), peakLanes: counters.peakLanes.Load(),
		checkpointPending: counters.checkpointPending.Load(),
		peakPreparedPacks: counters.peakPreparedPacks.Load(), peakPreparedBytes: counters.peakPreparedBytes.Load(),
		preparationTime:     time.Duration(counters.preparationNanos.Load()),
		ingestTime:          time.Duration(counters.ingestNanos.Load()),
		reductionTime:       time.Duration(counters.reductionNanos.Load()),
		publicationTime:     time.Duration(counters.publicationNanos.Load()),
		checkpointBatchTime: time.Duration(counters.checkpointBatchNanos.Load()), adaptiveSplits: counters.adaptiveSplits.Load(),
	}
}

func (counters *packPipelineCounters) reportSnapshot() {
	if counters.report != nil {
		counters.report(counters.snapshot())
	}
}

func updateAtomicMaximum(value *atomic.Uint64, candidate uint64) {
	for current := value.Load(); candidate > current; current = value.Load() {
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func publishLegacyImportBatch(
	ctx context.Context,
	store Store,
	imports []daemon.LegacyPackImport,
	checkpoint *daemon.Mutation,
	options Options,
) error {
	timeout := options.ImportBatchTimeout
	if timeout == 0 {
		timeout = defaultImportBatchTimeout
	}
	batchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := store.ImportLegacyPacks(batchCtx, imports, checkpoint); err != nil {
		if errors.Is(batchCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			return fmt.Errorf("legacy import batch exceeded %s: %w", timeout, err)
		}
		return err
	}
	return nil
}

func importCheckpointMutation(sourceIndex schema.ID, record schema.ImportCheckpointRecord) (daemon.Mutation, error) {
	value, err := record.MarshalBinary()
	if err != nil {
		return daemon.Mutation{}, err
	}
	return daemon.Mutation{Key: schema.ImportCheckpointKey(sourceIndex), Value: value}, nil
}

func estimateIndexedPackBytes(indexed legacyindex.PackBlobs) uint64 {
	const packOverhead = uint64(1152)
	const blobOverhead = uint64(224)
	if uint64(len(indexed.Blobs)) > (math.MaxUint64-packOverhead)/blobOverhead {
		return math.MaxUint64
	}
	return packOverhead + uint64(len(indexed.Blobs))*blobOverhead
}

func estimatePreparedImportBytes(imported daemon.LegacyPackImport) uint64 {
	// Include receipt/update overhead so pre-admission splitting avoids
	// single-pack oversize commits when batch limits are tight.
	bytes := uint64(1280 + len(imported.DebtKey))
	for _, blob := range imported.Blobs {
		bytes += 128 + uint64(len(blob.Locations))*96
	}
	bytes += uint64(len(imported.Placements))*192 + uint64(len(imported.PredecessorPackIDs))*64
	return bytes
}

func estimateImportMutations(imported daemon.LegacyPackImport) uint64 {
	const sharedMutationAllowance = uint64(16)
	return uint64(len(imported.Blobs)+len(imported.Placements)+len(imported.PredecessorPackIDs)) +
		sharedMutationAllowance
}

func recordFinding(result *Result, options Options, sourceID vaultic.ID, stage string, err error) error {
	result.ErrorsSeen++
	result.Findings = append(result.Findings, Finding{SourceID: sourceID, Stage: stage, Error: err.Error()})
	if options.MaxErrors > 0 && result.ErrorsSeen >= options.MaxErrors {
		return ErrLimitReached
	}
	return nil
}

func collectPacks(ctx context.Context, index *legacyindex.Index) []legacyindex.PackBlobs {
	packs := make([]legacyindex.PackBlobs, 0)
	seen := make(vaultic.IDSet)
	for indexedPack := range index.EachByPack(ctx, nil) {
		packs = append(packs, indexedPack)
		seen.Insert(indexedPack.PackID)
	}
	for packID := range index.Packs() {
		if !seen.Has(packID) {
			packs = append(packs, legacyindex.PackBlobs{PackID: packID})
		}
	}
	sort.Slice(packs, func(left, right int) bool {
		return string(packs[left].PackID[:]) < string(packs[right].PackID[:])
	})
	return packs
}

func buildPackImport(
	ctx context.Context,
	statter PackStatter,
	sourceIndex schema.ID,
	indexedPack legacyindex.PackBlobs,
) (daemon.LegacyPackImport, *schema.CrawlDebtRecord, error) {
	packID := schema.ID(indexedPack.PackID)
	record := schema.PackRecord{Lifecycle: schema.PackImported, SourceIndexIDs: []schema.ID{sourceIndex}}
	blobs := make(map[schema.ID]schema.BlobRecord)
	types := make([]schema.BlobType, 0, len(indexedPack.Blobs))
	type physicalLocation struct {
		blobID schema.ID
		offset uint64
		length uint32
		typeID schema.BlobType
	}
	locations := make(map[physicalLocation]schema.BlobLocation, len(indexedPack.Blobs))
	for _, blob := range indexedPack.Blobs {
		if blob.Length > math.MaxUint32 || blob.UncompressedLength > math.MaxUint32 {
			return daemon.LegacyPackImport{}, nil, fmt.Errorf("pack %s contains an oversized blob location", indexedPack.PackID.Str())
		}
		blobType, err := convertBlobType(blob.Type)
		if err != nil {
			return daemon.LegacyPackImport{}, nil, err
		}
		location := schema.BlobLocation{
			PackID: packID, Offset: uint64(blob.Offset), Length: uint32(blob.Length),
			UncompressedSize: uint32(blob.UncompressedLength), Type: blobType,
		}
		id := schema.ID(blob.ID)
		locationKey := physicalLocation{blobID: id, offset: location.Offset, length: location.Length, typeID: location.Type}
		if existing, found := locations[locationKey]; found {
			if location.UncompressedSize > existing.UncompressedSize {
				locations[locationKey] = location
			}
			continue
		}
		locations[locationKey] = location
		if math.MaxUint64-record.PayloadSize < uint64(location.Length) {
			return daemon.LegacyPackImport{}, nil, fmt.Errorf("pack %s payload size overflows", indexedPack.PackID.Str())
		}
		record.PayloadSize += uint64(location.Length)
	}
	for locationKey, location := range locations {
		value := blobs[locationKey.blobID]
		value.Locations = append(value.Locations, location)
		blobs[locationKey.blobID] = value
		types = append(types, location.Type)
	}
	record.BlobCount = uint64(len(locations))
	record.Type = schema.ClassifyPack(types)

	var debt *schema.CrawlDebtRecord
	info, err := statter.Stat(ctx, backend.Handle{Type: backend.PackFile, Name: indexedPack.PackID.String()})
	if err != nil || info.Size < 0 || uint64(info.Size) < record.PayloadSize {
		errorClass := "pack-stat-unavailable"
		if err == nil {
			errorClass = "pack-size-smaller-than-index-payload"
			record.PhysicalSize = uint64(info.Size)
			record.PhysicalSizeKnown = true
		}
		debt = &schema.CrawlDebtRecord{
			SourceIndexOrPack: packID, SourceKnown: true, PathOrTree: indexedPack.PackID[:],
			Reason: schema.DebtUnavailablePack, Status: schema.DebtPending, ErrorClass: errorClass,
		}
	} else {
		record.PhysicalSize = uint64(info.Size)
		record.HeaderSize = record.PhysicalSize - record.PayloadSize
		record.PhysicalSizeKnown = true
	}
	imported := daemon.LegacyPackImport{SourceIndex: sourceIndex, PackID: packID, Record: record, Blobs: blobs}
	if debt != nil {
		imported.DebtKey = schema.CrawlDebtKey(schema.ID{}, packID)
		imported.Debt = debt
	}
	return imported, debt, nil
}

func convertBlobType(blobType vaultic.BlobType) (schema.BlobType, error) {
	switch blobType {
	case vaultic.DataBlob:
		return schema.BlobData, nil
	case vaultic.TreeBlob:
		return schema.BlobTree, nil
	default:
		return 0, fmt.Errorf("unsupported legacy blob type %d", blobType)
	}
}
