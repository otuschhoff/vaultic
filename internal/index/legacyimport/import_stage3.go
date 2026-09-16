package legacyimport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
)

const (
	stage3BatchPathBits  = 16
	stage3BatchPathRoot  = uint64(1)
	stage3BatchPathMask  = (uint64(1) << stage3BatchPathBits) - 1
	stage3BatchPathShift = stage3BatchPathBits
)

type stage3LogicalBatch struct {
	ordinal    uint64
	rootBatch  uint64
	items      []packPreparation
	imports    []daemon.LegacyPackImport
	checkpoint *daemon.Mutation
	deps       []string
	queuedAt   time.Time
}

type stage3IngestOutcome struct {
	ordinal     uint64
	parts       []uint64
	failedPack  int
	err         error
	completedAt time.Time
}

//nolint:funlen,gocognit,gocyclo,nestif // Ordered preparation, concurrent ingestion, and reduction form one scheduler state machine.
func importPacksStage3(
	ctx context.Context,
	statter PackStatter,
	store SplitStore,
	sourceIndex schema.ID,
	packs []legacyindex.PackBlobs,
	options Options,
	checkpointIndex bool,
	report func(packPipelineStats),
) ([]packImportResult, uint64, packPipelineStats, int) {
	options.Telemetry.phase("schedule")
	defer options.Telemetry.phase("source")
	defer options.Telemetry.queues(0, 0, 0, 0, time.Time{})
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
				workerCtx, jobs, prepared, statter, sourceIndex, packs, options, pipelineOptions.packTimeout, counters,
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

	lanes := options.PublicationLanes
	if lanes == 0 {
		lanes = 1
	}
	ingested := make(chan stage3IngestOutcome, lanes)

	pendingPrepared := make(map[int]packPreparation, pipelineOptions.workers)
	ready := make([]stage3LogicalBatch, 0)
	active := make(map[uint64]stage3LogicalBatch)
	depBusy := make(map[string]uint64)
	completed := make(map[uint64]stage3IngestOutcome)

	nextPack := 0
	nextOrdinal := uint64(1)
	nextReduce := uint64(1)
	var batch packBatch
	batch.items = make([]packPreparation, 0, pipelineOptions.packsPerTransaction)
	var checkpoint schema.ImportCheckpointRecord
	prepareClosed := false
	failedPack := -1
	failureSeen := false
	failures := make([]stage3IngestOutcome, 0, lanes)
	stopAdmission := false
	activeLanes := uint(0)

	queueBatch := func(final bool) bool {
		if len(batch.items) == 0 {
			return true
		}
		rootBatch, err := stage3RootBatchID(batch.items)
		if err != nil {
			failedPack = batch.items[0].index
			outcomes[failedPack].err = err
			stopAdmission = true
			cancel()
			return false
		}
		imports := make([]daemon.LegacyPackImport, len(batch.items))
		for index := range batch.items {
			imports[index] = batch.items[index].outcome.imported
		}
		var finalCheckpoint *daemon.Mutation
		if final && checkpointIndex && !options.DryRun {
			mutation, err := importCheckpointMutation(sourceIndex, checkpoint)
			if err != nil {
				failedPack = batch.items[0].index
				outcomes[failedPack].err = err
				stopAdmission = true
				cancel()
				return false
			}
			finalCheckpoint = &mutation
		}
		ready = append(ready, stage3LogicalBatch{
			ordinal: nextOrdinal, rootBatch: rootBatch, items: append([]packPreparation(nil), batch.items...),
			imports: imports, checkpoint: finalCheckpoint, deps: stage3DependencyKeys(imports), queuedAt: time.Now(),
		})
		nextOrdinal++
		batch.reset()
		return true
	}

	admitReady := func() {
		if stopAdmission {
			return
		}
		for activeLanes < lanes {
			selected := -1
			blockedDeps := make(map[string]struct{})
			for i := range ready {
				if failureSeen {
					continue
				}
				if !stage3DependenciesFree(depBusy, ready[i].deps) || stage3DependenciesOverlap(blockedDeps, ready[i].deps) {
					for _, dependency := range ready[i].deps {
						blockedDeps[dependency] = struct{}{}
					}
					continue
				}
				selected = i
				break
			}
			if selected < 0 {
				return
			}
			batchToIngest := ready[selected]
			ready = append(ready[:selected], ready[selected+1:]...)
			stage3ReserveDependencies(depBusy, batchToIngest)
			active[batchToIngest.ordinal] = batchToIngest
			activeLanes++
			current := counters.activeLanes.Add(1)
			updateAtomicMaximum(&counters.peakLanes, current)
			go func(logical stage3LogicalBatch) {
				options.Telemetry.lane(1)
				outcome := stage3IngestOutcome{ordinal: logical.ordinal}
				parts, failureOffset, err := stage3IngestSplitBatch(workerCtx, store, sourceIndex, logical, options, counters)
				options.Telemetry.lane(-1)
				if failureOffset >= 0 && failureOffset < len(logical.items) {
					outcome.failedPack = logical.items[failureOffset].index
				} else {
					outcome.failedPack = logical.items[0].index
				}
				outcome.parts, outcome.err, outcome.completedAt = parts, err, time.Now()
				options.Telemetry.pendingCompletion(1)
				ingested <- outcome
			}(batchToIngest)
		}
	}

	acceptIngested := func(outcome stage3IngestOutcome) {
		options.Telemetry.pendingCompletion(-1)
		receivedAt := time.Now()
		options.Telemetry.observe("completion_to_receive", receivedAt.Sub(outcome.completedAt))
		if activeLanes > 0 {
			activeLanes--
		}
		counters.activeLanes.Add(^uint64(0))
		if outcome.err != nil {
			if failedPack < 0 {
				failedPack = outcome.failedPack
			}
			stopAdmission = true
			failureSeen = true
			cancel()
		}
		completed[outcome.ordinal] = outcome
	}

	reduceReady := func() bool {
		progress := false
		for {
			outcome, found := completed[nextReduce]
			if !found {
				return progress
			}
			delete(completed, nextReduce)
			logical, known := active[outcome.ordinal]
			if !known {
				failedPack = max(failedPack, outcome.failedPack)
				return true
			}
			if outcome.err != nil {
				failures = append(failures, outcome)
				if failedPack < 0 {
					failedPack = outcome.failedPack
				}
				failureSeen = true
				stopAdmission = true
				cancel()
				delete(active, outcome.ordinal)
				stage3ReleaseDependencies(depBusy, logical)
				nextReduce++
				progress = true
				continue
			}
			if failureSeen {
				delete(active, outcome.ordinal)
				stage3ReleaseDependencies(depBusy, logical)
				nextReduce++
				progress = true
				continue
			}
			for index, part := range outcome.parts {
				var finalCheckpoint *daemon.Mutation
				if index == len(outcome.parts)-1 {
					finalCheckpoint = logical.checkpoint
				}
				options.Telemetry.phase("reduce")
				reductionStarted := time.Now()
				var cancellationMu sync.Mutex
				var cancellationTime time.Time
				cancellationObserved := make(chan struct{})
				stopCancellationWatch := context.AfterFunc(ctx, func() {
					cancellationMu.Lock()
					cancellationTime = time.Now()
					cancellationMu.Unlock()
					close(cancellationObserved)
				})
				err := reduceLegacyImportBatch(ctx, store, sourceIndex, part, finalCheckpoint, options, counters)
				reductionEnded := time.Now()
				if !stopCancellationWatch() {
					<-cancellationObserved
				}
				cancellationMu.Lock()
				observedCancellation := cancellationTime
				cancellationMu.Unlock()
				drained := make([]stage3IngestOutcome, 0, lanes)
				for {
					select {
					case completedOutcome := <-ingested:
						drained = append(drained, completedOutcome)
					default:
						goto ingestsDrained
					}
				}
			ingestsDrained:
				sort.Slice(drained, func(left, right int) bool {
					return drained[left].completedAt.Before(drained[right].completedAt)
				})
				eligibleDuration := stage3EligibleRefillDuration(
					reductionStarted, reductionEnded, activeLanes, lanes, stopAdmission,
					stage3ReadyAfterCompletion(ready, depBusy), observedCancellation, drained,
				)
				if eligibleDuration > 0 {
					options.Telemetry.observe("eligible_ready_during_reduce", eligibleDuration)
				}
				for _, completedOutcome := range drained {
					acceptIngested(completedOutcome)
				}
				options.Telemetry.phase("schedule")
				if err != nil {
					failures = append(failures, stage3IngestOutcome{ordinal: outcome.ordinal, failedPack: logical.items[0].index, err: err})
					if failedPack < 0 {
						failedPack = logical.items[0].index
					}
					failureSeen = true
					stopAdmission = true
					cancel()
					delete(active, outcome.ordinal)
					stage3ReleaseDependencies(depBusy, logical)
					return true
				}
				counters.reducedBatches.Add(1)
			}
			for _, item := range logical.items {
				current := outcomes[item.index]
				current.complete = true
				outcomes[item.index] = current
				counters.preparedPacks.Add(^uint64(0))
				counters.preparedBytes.Add(^uint64(item.outcome.bytes - 1))
				released <- item.reservedBytes
				counters.committedBlobs.Add(item.outcome.imported.Record.BlobCount)
			}
			counters.committedPacks.Add(uint64(len(logical.items)))
			counters.committedBatches.Add(1)
			counters.checkpointPending.Store(logical.checkpoint == nil)
			counters.reportSnapshot()
			delete(active, outcome.ordinal)
			stage3ReleaseDependencies(depBusy, logical)
			nextReduce++
			progress = true
		}
	}

	for {
		options.Telemetry.phase("schedule")
		progress := false
		for {
			item, found := pendingPrepared[nextPack]
			if !found || stopAdmission {
				break
			}
			delete(pendingPrepared, nextPack)
			outcomes[nextPack] = item.outcome
			if item.outcome.err != nil {
				failedPack = item.index
				outcomes[failedPack].err = item.outcome.err
				stopAdmission = true
				cancel()
				queueBatch(false)
				break
			}
			if batch.shouldFlushBefore(item, pipelineOptions) {
				if !queueBatch(false) {
					break
				}
			}
			batch.add(item)
			checkpoint.PacksImported++
			checkpoint.BlobsImported += item.outcome.imported.Record.BlobCount
			if item.outcome.debt != nil {
				checkpoint.ErrorsSeen++
			}
			nextPack++
			if nextPack == len(packs) || batch.full(pipelineOptions) {
				if !queueBatch(nextPack == len(packs)) {
					break
				}
			}
			progress = true
		}

		admitReady()
		unreducedBytes, oldestUnreduced := stage3UnreducedState(ready, active)
		options.Telemetry.queues(
			len(ready), len(completed), counters.preparedBytes.Load(), unreducedBytes, oldestUnreduced,
		)
		reduceStarted := time.Now()
		if reduceReady() {
			options.Telemetry.observe("reduce_blocking", time.Since(reduceStarted))
			progress = true
		}

		if prepareClosed && activeLanes == 0 {
			if failedPack >= 0 {
				break
			}
			if len(active) == 0 && len(ready) == 0 && len(completed) == 0 && nextPack >= len(packs) {
				break
			}
		}
		if failureSeen && activeLanes == 0 {
			break
		}
		if !progress {
			switch {
			case len(ready) > 0 && activeLanes < lanes:
				options.Telemetry.phase("dependency_wait")
			case activeLanes > 0:
				options.Telemetry.phase("ingest_wait")
			default:
				options.Telemetry.phase("prepare_wait")
			}
			select {
			case item, ok := <-prepared:
				if !ok {
					prepareClosed = true
					prepared = nil
					if failedPack < 0 && nextPack < len(packs) && !stopAdmission {
						failedPack = nextPack
						outcomes[failedPack].err = preparationEndError(ctx)
					}
					continue
				}
				pendingPrepared[item.index] = item
			case outcome := <-ingested:
				acceptIngested(outcome)
			case <-ctx.Done():
				if failedPack < 0 {
					failedPack = min(nextPack, len(outcomes)-1)
					outcomes[failedPack].err = ctx.Err()
				}
				stopAdmission = true
				cancel()
			}
		}
	}

	if failedPack >= 0 {
		if failureSeen {
			for activeLanes > 0 {
				outcome := <-ingested
				options.Telemetry.pendingCompletion(-1)
				receivedAt := time.Now()
				options.Telemetry.observe("completion_to_receive", receivedAt.Sub(outcome.completedAt))
				activeLanes--
				counters.activeLanes.Add(^uint64(0))
				if outcome.err != nil {
					failures = append(failures, outcome)
				}
				logical, found := active[outcome.ordinal]
				if found {
					delete(active, outcome.ordinal)
					stage3ReleaseDependencies(depBusy, logical)
				}
			}
		}
		if selected, ok := stage3SelectFailure(failures); ok {
			failedPack = selected.failedPack
			if failedPack < 0 {
				failedPack = 0
			}
			if failedPack >= len(outcomes) {
				failedPack = len(outcomes) - 1
			}
			if failedPack >= 0 {
				outcomes[failedPack].err = stage3WrapResumeConflict(selected.err)
			}
		}
		if failedPack < len(outcomes) && outcomes[failedPack].err == nil {
			outcomes[failedPack].err = preparationEndError(ctx)
		}
		return outcomes, counters.committedBatches.Load(), counters.snapshot(), failedPack
	}
	if len(completed) > 0 {
		remainingFailures := make([]stage3IngestOutcome, 0, len(completed))
		for _, outcome := range completed {
			if outcome.err != nil {
				remainingFailures = append(remainingFailures, outcome)
			}
		}
		if selected, ok := stage3SelectFailure(remainingFailures); ok {
			failedPack = selected.failedPack
			if failedPack < 0 {
				failedPack = 0
			}
			if failedPack >= len(outcomes) {
				failedPack = len(outcomes) - 1
			}
			if failedPack >= 0 {
				outcomes[failedPack].err = stage3WrapResumeConflict(selected.err)
				return outcomes, counters.committedBatches.Load(), counters.snapshot(), failedPack
			}
		}
	}

	if checkpointIndex && !options.DryRun {
		options.Telemetry.phase("cleanup")
		if err := store.CompleteLegacyImportSession(ctx, sourceIndex); err != nil {
			outcomes[len(outcomes)-1].err = err
			return outcomes, counters.committedBatches.Load(), counters.snapshot(), len(outcomes) - 1
		}
	}

	return outcomes, counters.committedBatches.Load(), counters.snapshot(), -1
}

func stage3UnreducedState(ready []stage3LogicalBatch, active map[uint64]stage3LogicalBatch) (uint64, time.Time) {
	var bytes uint64
	var oldest time.Time
	account := func(batch stage3LogicalBatch) {
		for _, item := range batch.items {
			bytes += item.outcome.bytes
		}
		if oldest.IsZero() || batch.queuedAt.Before(oldest) {
			oldest = batch.queuedAt
		}
	}
	for _, batch := range ready {
		account(batch)
	}
	for _, batch := range active {
		account(batch)
	}
	return bytes, oldest
}

func stage3ReadyAfterCompletion(ready []stage3LogicalBatch, busy map[string]uint64) bool {
	blockedDeps := make(map[string]struct{})
	for _, candidate := range ready {
		if !stage3DependenciesFree(busy, candidate.deps) || stage3DependenciesOverlap(blockedDeps, candidate.deps) {
			for _, dependency := range candidate.deps {
				blockedDeps[dependency] = struct{}{}
			}
			continue
		}
		return true
	}
	return false
}

func stage3EligibleRefillDuration(
	started, ended time.Time,
	activeLanes, lanes uint,
	stopAdmission, readyEligible bool,
	canceledAt time.Time,
	completions []stage3IngestOutcome,
) time.Duration {
	cutoff := ended
	if !canceledAt.IsZero() && canceledAt.Before(cutoff) {
		cutoff = maxTime(canceledAt, started)
	}
	eligible := !stopAdmission && readyEligible && activeLanes < lanes
	cursor := started
	var elapsed time.Duration
	for _, outcome := range completions {
		completedAt := outcome.completedAt
		if completedAt.Before(started) {
			completedAt = started
		}
		if completedAt.After(cutoff) {
			break
		}
		if eligible && completedAt.After(cursor) {
			elapsed += completedAt.Sub(cursor)
		}
		if activeLanes > 0 {
			activeLanes--
		}
		if outcome.err != nil {
			stopAdmission = true
		}
		eligible = !stopAdmission && readyEligible && activeLanes < lanes
		cursor = completedAt
	}
	if eligible && cutoff.After(cursor) {
		elapsed += cutoff.Sub(cursor)
	}
	return elapsed
}

func maxTime(left, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}

func stage3IngestSplitBatch(
	ctx context.Context,
	store SplitStore,
	session schema.ID,
	logical stage3LogicalBatch,
	options Options,
	counters *packPipelineCounters,
) ([]uint64, int, error) {
	return stage3IngestSplitParts(ctx, store, session, logical.rootBatch, logical.imports, options, counters)
}

func stage3IngestSplitParts(
	ctx context.Context,
	store SplitStore,
	session schema.ID,
	batchID uint64,
	imports []daemon.LegacyPackImport,
	options Options,
	counters *packPipelineCounters,
) ([]uint64, int, error) {
	err := ingestLegacyImportBatch(ctx, store, session, batchID, imports, options, counters)
	if err == nil {
		counters.ingestedBatches.Add(1)
		return []uint64{batchID}, -1, nil
	}
	if !errors.Is(err, daemon.ErrLegacyImportBatchTooLarge) || len(imports) <= 1 {
		return nil, preparedBatchFailureOffset(imports, err), err
	}
	counters.adaptiveSplits.Add(1)
	leftID, rightID, idErr := stage3SplitBatchIDs(batchID)
	if idErr != nil {
		return nil, 0, idErr
	}
	middle := len(imports) / 2
	leftParts, leftFailure, leftErr := stage3IngestSplitParts(ctx, store, session, leftID, imports[:middle], options, counters)
	if leftErr != nil {
		return nil, leftFailure, leftErr
	}
	rightParts, rightFailure, rightErr := stage3IngestSplitParts(ctx, store, session, rightID, imports[middle:], options, counters)
	if rightErr != nil {
		return nil, middle + rightFailure, rightErr
	}
	return append(leftParts, rightParts...), -1, nil
}

func ingestLegacyImportBatch(
	ctx context.Context,
	store SplitStore,
	session schema.ID,
	batchID uint64,
	imports []daemon.LegacyPackImport,
	options Options,
	counters *packPipelineCounters,
) error {
	timeout := options.ImportBatchTimeout
	if timeout == 0 {
		timeout = defaultImportBatchTimeout
	}
	started := time.Now()
	batchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := store.IngestLegacyPacks(batchCtx, session, batchID, imports)
	elapsed := uint64(time.Since(started))
	counters.ingestNanos.Add(elapsed)
	counters.publicationNanos.Add(elapsed)
	if err != nil {
		if errors.Is(batchCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			return fmt.Errorf("legacy import ingest batch exceeded %s: %w", timeout, err)
		}
		return err
	}
	return nil
}

func reduceLegacyImportBatch(
	ctx context.Context,
	store SplitStore,
	session schema.ID,
	batchID uint64,
	checkpoint *daemon.Mutation,
	options Options,
	counters *packPipelineCounters,
) error {
	timeout := options.ImportBatchTimeout
	if timeout == 0 {
		timeout = defaultImportBatchTimeout
	}
	started := time.Now()
	batchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := store.ReduceLegacyImportBatch(batchCtx, session, batchID, checkpoint)
	elapsed := uint64(time.Since(started))
	counters.reductionNanos.Add(elapsed)
	counters.publicationNanos.Add(elapsed)
	if checkpoint != nil {
		counters.checkpointBatchNanos.Add(elapsed)
	}
	if err != nil {
		if errors.Is(batchCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			return fmt.Errorf("legacy import reduction batch exceeded %s: %w", timeout, err)
		}
		if checkpoint != nil {
			return &checkpointBatchError{err: err}
		}
		return err
	}
	return nil
}

func stage3RootBatchID(items []packPreparation) (uint64, error) {
	high, err := stage3CanonicalBatchIDPrefix(items)
	if err != nil {
		return 0, err
	}
	return high<<stage3BatchPathShift | stage3BatchPathRoot, nil
}

//nolint:gocognit // Stable identity includes every canonical field that can change batch semantics.
func stage3CanonicalBatchIDPrefix(items []packPreparation) (uint64, error) {
	hasher := sha256.New()
	if err := stage3HashFramedU64(hasher, uint64(len(items))); err != nil {
		return 0, err
	}
	for _, item := range items {
		imported := item.outcome.imported
		if _, err := hasher.Write(imported.SourceIndex[:]); err != nil {
			return 0, err
		}
		if _, err := hasher.Write(imported.PackID[:]); err != nil {
			return 0, err
		}
		recordValue, err := imported.Record.MarshalBinary()
		if err != nil {
			return 0, err
		}
		if err := stage3HashFramedBytes(hasher, recordValue); err != nil {
			return 0, err
		}
		blobIDs := sortedSchemaBlobIDs(imported.Blobs)
		if err := stage3HashFramedU64(hasher, uint64(len(blobIDs))); err != nil {
			return 0, err
		}
		for _, blobID := range blobIDs {
			if _, err := hasher.Write(blobID[:]); err != nil {
				return 0, err
			}
			blobValue, err := imported.Blobs[blobID].MarshalBinary()
			if err != nil {
				return 0, err
			}
			if err := stage3HashFramedBytes(hasher, blobValue); err != nil {
				return 0, err
			}
		}
		if err := stage3HashFramedBytes(hasher, imported.DebtKey); err != nil {
			return 0, err
		}
		if imported.Debt == nil {
			if err := stage3HashFramedBytes(hasher, nil); err != nil {
				return 0, err
			}
		} else {
			debtValue, err := imported.Debt.MarshalBinary()
			if err != nil {
				return 0, err
			}
			if err := stage3HashFramedBytes(hasher, debtValue); err != nil {
				return 0, err
			}
		}
		placementBackends := sortedPlacementBackends(imported.Placements)
		if err := stage3HashFramedU64(hasher, uint64(len(placementBackends))); err != nil {
			return 0, err
		}
		for _, backend := range placementBackends {
			if err := stage3HashFramedU64(hasher, backend); err != nil {
				return 0, err
			}
			placementValue, err := imported.Placements[backend].MarshalBinary()
			if err != nil {
				return 0, err
			}
			if err := stage3HashFramedBytes(hasher, placementValue); err != nil {
				return 0, err
			}
		}
		if err := stage3HashFramedU64(hasher, uint64(len(imported.PredecessorPackIDs))); err != nil {
			return 0, err
		}
		for _, predecessor := range imported.PredecessorPackIDs {
			if _, err := hasher.Write(predecessor[:]); err != nil {
				return 0, err
			}
		}
		if err := stage3HashFramedU64(hasher, uint64(imported.LineageKind)); err != nil {
			return 0, err
		}
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return binary.BigEndian.Uint64(digest[:8]) >> stage3BatchPathBits, nil
}

func stage3SelectFailure(failures []stage3IngestOutcome) (stage3IngestOutcome, bool) {
	bestIndex := -1
	for i := range failures {
		if failures[i].err == nil {
			continue
		}
		if bestIndex < 0 {
			bestIndex = i
			continue
		}
		current := failures[i]
		best := failures[bestIndex]
		if stage3IsCancellation(current.err) {
			if stage3IsCancellation(best.err) && current.ordinal < best.ordinal {
				bestIndex = i
			}
			continue
		}
		if stage3IsCancellation(best.err) || current.ordinal < best.ordinal {
			bestIndex = i
		}
	}
	if bestIndex < 0 {
		return stage3IngestOutcome{}, false
	}
	return failures[bestIndex], true
}

func stage3IsCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func stage3HashFramedBytes(hasher interface{ Write([]byte) (int, error) }, value []byte) error {
	if err := stage3HashFramedU64(hasher, uint64(len(value))); err != nil {
		return err
	}
	if len(value) == 0 {
		return nil
	}
	_, err := hasher.Write(value)
	return err
}

func stage3HashFramedU64(hasher interface{ Write([]byte) (int, error) }, value uint64) error {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, err := hasher.Write(encoded[:])
	return err
}

func stage3SplitBatchIDs(parent uint64) (uint64, uint64, error) {
	prefix := parent &^ stage3BatchPathMask
	path := parent & stage3BatchPathMask
	if path == 0 {
		return 0, 0, fmt.Errorf("legacy import split batch id %d has an empty path", parent)
	}
	if path > (stage3BatchPathMask >> 1) {
		return 0, 0, fmt.Errorf("legacy import split batch id overflow under %d", parent)
	}
	left := path << 1
	right := left | 1
	if left > stage3BatchPathMask || right > stage3BatchPathMask {
		return 0, 0, fmt.Errorf("legacy import split batch id overflow under %d", parent)
	}
	return prefix | left, prefix | right, nil
}

func preparedBatchFailureOffset(imports []daemon.LegacyPackImport, err error) int {
	var inputErr *daemon.LegacyImportInputError
	if errors.As(err, &inputErr) && inputErr.Index >= 0 && inputErr.Index < len(imports) {
		return inputErr.Index
	}
	return 0
}

func stage3DependencyKeys(imports []daemon.LegacyPackImport) []string {
	keys := make(map[string]struct{}, len(imports)*4)
	for _, imported := range imports {
		keys[string(schema.PackKey(imported.PackID))] = struct{}{}
		for _, blobID := range sortedSchemaBlobIDs(imported.Blobs) {
			keys[string(schema.BlobKey(blobID))] = struct{}{}
		}
		debtKey := imported.DebtKey
		if len(debtKey) == 0 {
			debtKey = schema.CrawlDebtKey(schema.ID{}, imported.PackID)
		}
		keys[string(debtKey)] = struct{}{}
		for _, backend := range sortedPlacementBackends(imported.Placements) {
			keys[string(schema.PackPlacementKey(imported.PackID, backend))] = struct{}{}
			keys[string(schema.BackendPackKey(backend, imported.PackID))] = struct{}{}
		}
		for _, predecessor := range imported.PredecessorPackIDs {
			keys[string(schema.RepackLineageKey(predecessor, imported.PackID))] = struct{}{}
		}
	}
	result := make([]string, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func sortedSchemaBlobIDs(values map[schema.ID]schema.BlobRecord) []schema.ID {
	result := make([]schema.ID, 0, len(values))
	for blobID := range values {
		result = append(result, blobID)
	}
	sort.Slice(result, func(left, right int) bool { return bytes.Compare(result[left][:], result[right][:]) < 0 })
	return result
}

func sortedPlacementBackends(values map[uint64]schema.PlacementRecord) []uint64 {
	result := make([]uint64, 0, len(values))
	for backend := range values {
		result = append(result, backend)
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return result
}

func stage3DependenciesFree(inFlight map[string]uint64, deps []string) bool {
	for _, key := range deps {
		if _, busy := inFlight[key]; busy {
			return false
		}
	}
	return true
}

func stage3DependenciesOverlap(blocked map[string]struct{}, deps []string) bool {
	for _, key := range deps {
		if _, found := blocked[key]; found {
			return true
		}
	}
	return false
}

func stage3ReserveDependencies(inFlight map[string]uint64, batch stage3LogicalBatch) {
	for _, key := range batch.deps {
		inFlight[key] = batch.ordinal
	}
}

func stage3ReleaseDependencies(inFlight map[string]uint64, batch stage3LogicalBatch) {
	for _, key := range batch.deps {
		if owner, busy := inFlight[key]; busy && owner == batch.ordinal {
			delete(inFlight, key)
		}
	}
}

func stage3WrapResumeConflict(err error) error {
	if !errors.Is(err, daemon.ErrIdempotencyConflict) {
		return err
	}
	return fmt.Errorf(
		"%w: stage 3 derived batch ID conflict (changed canonical batch content at the same derived ID or a 48-bit ID prefix collision); "+
			"rerun with consistent --packs-per-transaction, --import-publication-lanes, --batch-size, and "+
			"--import-transaction-bytes or reset with --force-reset-old-idx",
		err,
	)
}
