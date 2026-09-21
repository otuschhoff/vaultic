package legacyimport

import (
	"context"
	"fmt"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const stage3MaxGroupedPacks = MaxPacksPerTransaction

type stage3SourceGroup struct {
	indexIDs      []vaultic.ID
	packs         []legacyindex.PackBlobs
	sourceIndexes []schema.ID
}

func importGroupedStage3Indexes(
	ctx context.Context,
	indexList vaultic.Lister,
	source Source,
	statter PackStatter,
	store SplitStore,
	options Options,
	result *Result,
	reportProgress func(),
) (packPipelineStats, error) {
	var total packPipelineStats
	group := stage3SourceGroup{}
	flush := func() error {
		if len(group.indexIDs) == 0 {
			return nil
		}
		if len(group.packs) == 0 {
			for _, indexID := range group.indexIDs {
				outcomes, committed, stats, failed := importPacksStage3(
					ctx, statter, store, schema.ID(indexID), nil, options, true, nil,
				)
				mergePackPipelineStats(&total, stats)
				result.BatchesCommitted += committed
				if failed >= 0 {
					return fmt.Errorf("publish import checkpoint for index %s: %w", indexID.Str(), outcomes[failed].err)
				}
				result.IndexesImported++
				result.Checkpoint = indexID.String()
			}
			reportProgress()
			group = stage3SourceGroup{}
			return nil
		}

		outcomes, committed, stats, failed := importPacksStage3Sources(
			ctx, statter, store, schema.ID(group.indexIDs[0]), group.packs, group.sourceIndexes,
			options, true, nil,
		)
		mergePackPipelineStats(&total, stats)
		result.BatchesCommitted += committed
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
				result.WarningsSeen++
				result.Findings = append(result.Findings, Finding{
					SourceID: vaultic.ID(group.sourceIndexes[packIndex]), Stage: "stat-pack", Error: outcome.debt.ErrorClass,
				})
			}
			result.PacksImported++
			result.BlobsImported += outcome.imported.Record.BlobCount
			result.RecordsImported += outcome.imported.Record.BlobCount
			result.RecordsSkipped += uint64(len(group.packs[packIndex].Blobs)) - outcome.imported.Record.BlobCount
		}
		if failed >= 0 {
			if failed >= len(group.packs) {
				return fmt.Errorf("publish grouped import checkpoints: %w", outcomes[failed].err)
			}
			failedSource := vaultic.ID(group.sourceIndexes[failed])
			return fmt.Errorf(
				"import pack %s from index %s: %w", group.packs[failed].PackID.Str(), failedSource.Str(), outcomes[failed].err,
			)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		result.IndexesImported += uint64(len(group.indexIDs))
		result.Checkpoint = group.indexIDs[len(group.indexIDs)-1].String()
		reportProgress()
		group = stage3SourceGroup{}
		return nil
	}

	err := legacyindex.ForAllIndexesInOrder(ctx, indexList, source, func(indexID vaultic.ID, index *legacyindex.Index, loadErr error) error {
		result.IndexesSeen++
		if loadErr != nil {
			if err := flush(); err != nil {
				return err
			}
			defer reportProgress()
			return recordFinding(result, options, indexID, "decode-index", loadErr)
		}
		if options.Resume {
			value, found, err := store.Get(ctx, schema.ImportCheckpointKey(schema.ID(indexID)))
			if err != nil {
				return fmt.Errorf("read import checkpoint for %s: %w", indexID.Str(), err)
			}
			if found {
				if err := flush(); err != nil {
					return err
				}
				if _, err := schema.UnmarshalImportCheckpointRecord(value); err != nil {
					return fmt.Errorf("decode import checkpoint for %s: %w", indexID.Str(), err)
				}
				if err := store.CompleteLegacyImportSession(ctx, schema.ID(indexID)); err != nil {
					return fmt.Errorf("cleanup split-session receipts for %s: %w", indexID.Str(), err)
				}
				result.IndexesResumed++
				result.Checkpoint = indexID.String()
				reportProgress()
				return nil
			}
		}
		packs := collectPacks(ctx, index)
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(packs) == 0 {
			if err := flush(); err != nil {
				return err
			}
			group.indexIDs = append(group.indexIDs, indexID)
			return flush()
		}
		if len(group.indexIDs) > 0 && (len(group.packs)+len(packs) > stage3MaxGroupedPacks || len(group.indexIDs) >= stage3MaxGroupedPacks) {
			if err := flush(); err != nil {
				return err
			}
		}
		group.indexIDs = append(group.indexIDs, indexID)
		group.packs = append(group.packs, packs...)
		for range packs {
			group.sourceIndexes = append(group.sourceIndexes, schema.ID(indexID))
		}
		result.RecordsSeen += indexBlobCount(packs)
		if len(group.packs) >= stage3MaxGroupedPacks {
			return flush()
		}
		return nil
	})
	if err != nil {
		return total, err
	}
	return total, flush()
}

func indexBlobCount(packs []legacyindex.PackBlobs) uint64 {
	var count uint64
	for _, indexedPack := range packs {
		count += uint64(len(indexedPack.Blobs))
	}
	return count
}

func mergePackPipelineStats(total *packPipelineStats, next packPipelineStats) {
	total.totalPreparedPacks += next.totalPreparedPacks
	total.totalPreparedBytes += next.totalPreparedBytes
	total.committedPacks += next.committedPacks
	total.committedBlobs += next.committedBlobs
	total.ingestedBatches += next.ingestedBatches
	total.reducedBatches += next.reducedBatches
	total.committedBatches += next.committedBatches
	total.peakLanes = max(total.peakLanes, next.peakLanes)
	total.peakPreparedPacks = max(total.peakPreparedPacks, next.peakPreparedPacks)
	total.peakPreparedBytes = max(total.peakPreparedBytes, next.peakPreparedBytes)
	total.preparationTime += next.preparationTime
	total.ingestTime += next.ingestTime
	total.reductionTime += next.reductionTime
	total.publicationTime += next.publicationTime
	total.checkpointBatchTime += next.checkpointBatchTime
	total.adaptiveSplits += next.adaptiveSplits
	total.checkpointPending = next.checkpointPending
	total.inFlightLanes = next.inFlightLanes
}

func applyGroupedStage3Stats(result *Result, stats packPipelineStats) {
	result.BatchesIngested += stats.ingestedBatches
	result.BatchesReduced += stats.reducedBatches
	result.PeakPublicationLanes = max(result.PeakPublicationLanes, stats.peakLanes)
	result.PeakPreparedPacks = max(result.PeakPreparedPacks, stats.peakPreparedPacks)
	result.PeakPreparedBytes = max(result.PeakPreparedBytes, stats.peakPreparedBytes)
	result.PreparationTimeMS = uint64(stats.preparationTime / time.Millisecond)
	result.IngestTimeMS = uint64(stats.ingestTime / time.Millisecond)
	result.ReductionTimeMS = uint64(stats.reductionTime / time.Millisecond)
	result.PublicationTimeMS = uint64(stats.publicationTime / time.Millisecond)
	result.CheckpointBatchTimeMS = uint64(stats.checkpointBatchTime / time.Millisecond)
	result.AdaptiveSplits += stats.adaptiveSplits
}
