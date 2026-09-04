package repository

import (
	"context"
	"fmt"

	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// PromotePack rewrites the retained blobs of one source pack into newly
// published packs. It never copies the encrypted source object verbatim.
func PromotePack(ctx context.Context, repo *Repository, packID vaultic.ID, targetBackend uint64, printer vaultic.Printer) ([]vaultic.ID, error) {
	if repo.Engine().Mode() != metadataindex.ModeSlateDB {
		return nil, fmt.Errorf("promotion requires a SlateDB-authoritative repository")
	}
	engine, ok := repo.Engine().(*metadataindex.DaemonEngine)
	if !ok {
		return nil, fmt.Errorf("promotion requires the SlateDB daemon engine")
	}
	if repo.Connections() < 2 {
		return nil, fmt.Errorf("promotion requires a backend connection limit of at least two")
	}
	model, err := repo.PlacementModel()
	if err != nil {
		return nil, err
	}
	target, found := model.BackendByIDHash(targetBackend)
	if !found {
		return nil, fmt.Errorf("promotion target %016x is not registered", targetBackend)
	}
	if !promotionUsesRepositoryWriteRoute(repo, model, target) {
		return nil, fmt.Errorf("promotion target %q is not addressable by the repository write route", target.ID)
	}
	store := engine.SchemaStore()
	successors, err := findPublishedRepackSuccessors(ctx, store, packID, targetBackend)
	if err != nil {
		return nil, err
	}
	if len(successors) != 0 {
		if err := store.MarkPackDeletePending(ctx, schema.ID(packID)); err != nil {
			return nil, fmt.Errorf("finish resumed promotion: %w", err)
		}
		return successors, nil
	}
	blobTypes, packMembers, _, err := scanBlobCatalog(ctx, store)
	if err != nil {
		return nil, err
	}
	members := packMembers[packID]
	if len(members) == 0 {
		return nil, fmt.Errorf("source pack %s has no catalog members", packID.Str())
	}
	retained := newGCBlobSet()
	if err := walkRetainedSnapshots(ctx, repo, nil, retained, printer); err != nil {
		return nil, err
	}
	keep := newGCBlobSet()
	for _, blobID := range members {
		handle := vaultic.BlobHandle{ID: blobID, Type: legacyBlobType(blobTypes[blobID])}
		if retained.Has(handle) {
			keep.Insert(handle)
		}
	}
	if keep.Len() == 0 {
		return nil, schema.ErrPlacementObsolete
	}
	return promotePackBlobs(ctx, repo, engine, packID, targetBackend, keep, printer, nil)
}

func promotePackBlobs(
	ctx context.Context,
	repo *Repository,
	engine *metadataindex.DaemonEngine,
	packID vaultic.ID,
	targetBackend uint64,
	keep *gcBlobSet,
	printer vaultic.Printer,
	afterPublish func([]vaultic.ID) error,
) ([]vaultic.ID, error) {
	store := engine.SchemaStore()
	before, err := scanPacks(ctx, store)
	if err != nil {
		return nil, err
	}
	runID := vaultic.NewRandomID()
	engine.SetPromotionContext(schema.ID(runID), []schema.ID{schema.ID(packID)}, targetBackend)
	packSet := vaultic.NewIDSet()
	packSet.Insert(packID)
	destination, err := repo.placementWriteRepository(targetBackend)
	if err != nil {
		engine.ClearRepackContext()
		return nil, err
	}
	err = destination.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		return CopyBlobs(ctx, repo, destination, uploader, packSet, keep, vaultic.NoopCounter, printer.P)
	})
	engine.ClearRepackContext()
	if err != nil {
		return nil, fmt.Errorf("promote pack %s: %w", packID.Str(), err)
	}
	if keep.Len() != 0 {
		return nil, fmt.Errorf("promotion left %d retained blobs unwritten", keep.Len())
	}
	after, err := scanPacks(ctx, store)
	if err != nil {
		return nil, err
	}
	successors := make([]vaultic.ID, 0)
	for candidate, record := range after {
		if _, existed := before[candidate]; existed || record.Lifecycle != schema.PackPublished {
			continue
		}
		successors = append(successors, candidate)
	}
	if len(successors) == 0 {
		return nil, fmt.Errorf("promotion published no replacement pack")
	}
	if afterPublish != nil {
		if err := afterPublish(successors); err != nil {
			return nil, err
		}
	}
	if err := store.MarkPackDeletePending(ctx, schema.ID(packID)); err != nil {
		return nil, fmt.Errorf("mark promoted source delete-pending: %w", err)
	}
	return successors, nil
}

func findPublishedRepackSuccessors(ctx context.Context, store *daemon.SchemaStore, source vaultic.ID, targetBackend uint64) ([]vaultic.ID, error) {
	var successors []vaultic.ID
	var after []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, schema.RepackLineagePrefix(schema.ID(source)), after, 1_000)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			parsed, err := schema.ParseKey(entry.Key)
			if err != nil || parsed.Kind != schema.KeyRepackLineage {
				return nil, schema.ErrMalformed
			}
			packValue, found, err := store.Get(ctx, schema.PackKey(parsed.SecondID))
			if err != nil {
				return nil, err
			}
			if !found {
				continue
			}
			pack, err := schema.UnmarshalPackRecord(packValue)
			if err != nil {
				return nil, err
			}
			if pack.Lifecycle != schema.PackExportPending && pack.Lifecycle != schema.PackPublished {
				continue
			}
			placementValue, found, err := store.Get(ctx, schema.PackPlacementKey(parsed.SecondID, targetBackend))
			if err != nil {
				return nil, err
			}
			if !found {
				continue
			}
			placement, err := schema.UnmarshalPlacementRecord(placementValue)
			if err != nil {
				return nil, err
			}
			if placement.State == schema.PlacementLive {
				successors = append(successors, vaultic.ID(parsed.SecondID))
			}
		}
		if done {
			return successors, nil
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("scan repack lineage made no progress")
		}
		after = append(after[:0], entries[len(entries)-1].Key...)
	}
}

func promotionUsesRepositoryWriteRoute(repo *Repository, model PlacementModel, target PlacementBackend) bool {
	if _, found := repo.placementBackend(target.Hash); found {
		return true
	}
	if model.HotCold {
		return target.Role == PlacementRoleArchival
	}
	return len(model.Backends) == 1 && model.Backends[0].Hash == target.Hash
}

func (r *Repository) placementWriteRepository(targetBackend uint64) (*Repository, error) {
	model, err := r.PlacementModel()
	if err != nil {
		return nil, err
	}
	target, found := model.BackendByIDHash(targetBackend)
	if !found {
		return nil, fmt.Errorf("placement backend %016x is not registered", targetBackend)
	}
	physical, found := r.backendForPlacement(model, target)
	if !found {
		return nil, fmt.Errorf("placement backend %q is not addressable", target.ID)
	}
	options := r.opts
	if target.TargetPackSizeBytes != 0 {
		options.DataPackSize = target.TargetPackSizeBytes
	}
	destination, err := New(physical, options)
	if err != nil {
		return nil, err
	}
	destination.cfg = r.cfg
	destination.key = r.key
	destination.keyID = r.keyID
	destination.idx = r.idx
	destination.engine = r.engine
	return destination, nil
}
