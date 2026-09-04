package repository

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type SnapshotRetentionHorizon struct {
	Until      time.Time
	Indefinite bool
}

// RecordPromotionEligibility derives durable pack survival evidence from one
// completed forget-policy evaluation. Stale evidence from older policies is
// removed in the same metadata batch.
func RecordPromotionEligibility(
	ctx context.Context,
	repo *Repository,
	store *daemon.SchemaStore,
	snapshots map[vaultic.ID]SnapshotRetentionHorizon,
	evaluatedAt time.Time,
	printer vaultic.Printer,
) error {
	blobHorizons := make(map[vaultic.ID]int64)
	liveBlobs := make(map[vaultic.ID]struct{})
	err := data.ForAllSnapshots(ctx, repo, repo, nil, func(snapshotID vaultic.ID, snapshot *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		if snapshot.Tree == nil {
			return fmt.Errorf("retained snapshot %s has no tree", snapshotID.Str())
		}
		used := newGCBlobSet()
		counter := printer.NewCounter("promotion policy")
		err = data.FindUsedBlobs(ctx, repo, vaultic.IDs{*snapshot.Tree}, used, counter)
		counter.Done()
		if err != nil {
			return fmt.Errorf("walk retained snapshot %s: %w", snapshotID.Str(), err)
		}
		for handle := range used.handles {
			liveBlobs[handle.ID] = struct{}{}
			if horizon, known := snapshots[snapshotID]; known {
				until := horizon.Until.UnixNano()
				if horizon.Indefinite {
					until = math.MaxInt64
				}
				if until > blobHorizons[handle.ID] {
					blobHorizons[handle.ID] = until
				}
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk surviving snapshots: %w", err)
	}
	_, packMembers, _, err := scanBlobCatalog(ctx, store)
	if err != nil {
		return err
	}
	records := promotionEligibilityRecords(packMembers, liveBlobs, blobHorizons, evaluatedAt.UnixNano())
	puts := make([]daemon.Mutation, 0, len(records))
	for packID, record := range records {
		value, err := record.MarshalBinary()
		if err != nil {
			return err
		}
		puts = append(puts, daemon.Mutation{Key: schema.PromotionEligibilityKey(schema.ID(packID)), Value: value})
	}
	deletes := make([][]byte, 0)
	if err := gcScan(ctx, store, schema.PromotionEligibilityPrefix(), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyPromotionEligibility {
			return schema.ErrMalformed
		}
		if _, found := records[vaultic.ID(parsed.ID)]; !found {
			deletes = append(deletes, append([]byte(nil), entry.Key...))
		}
		return nil
	}); err != nil {
		return err
	}
	return store.WriteMutableBatch(ctx, puts, deletes, false)
}

func promotionEligibilityRecords(
	packMembers map[vaultic.ID][]vaultic.ID,
	liveBlobs map[vaultic.ID]struct{},
	blobHorizons map[vaultic.ID]int64,
	evaluatedAt int64,
) map[vaultic.ID]schema.PromotionEligibilityRecord {
	records := make(map[vaultic.ID]schema.PromotionEligibilityRecord)
	for packID, members := range packMembers {
		minimum := int64(math.MaxInt64)
		var retained bool
		for _, blobID := range members {
			if _, live := liveBlobs[blobID]; !live {
				continue
			}
			horizon, found := blobHorizons[blobID]
			if !found {
				retained = false
				minimum = 0
				break
			}
			retained = true
			if horizon < minimum {
				minimum = horizon
			}
		}
		if !retained || minimum == 0 {
			continue
		}
		records[packID] = schema.PromotionEligibilityRecord{
			SurvivalUntil: minimum, EvaluatedAt: evaluatedAt, Indefinite: minimum == math.MaxInt64,
		}
	}
	return records
}
