package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const prunePlanVersion = 1

const (
	prunePlanPhaseA = "phase_a"
	prunePlanReady  = "ready"
)

// BeginPrunePlan claims the short exclusive phase before additive prune work.
// A pending marker prevents another prune from entering phase A concurrently
// while allowing ordinary shared append writers to proceed once the caller
// releases its exclusive lock.
func (r *Repository) BeginPrunePlan(ctx context.Context) (*vaultic.PrunePlan, error) {
	if r.Config().PrunePlan != nil {
		return nil, errors.Fatal("repository already contains a prune plan; finalize or clear it before starting another prune")
	}
	plan := &vaultic.PrunePlan{
		Version:   prunePlanVersion,
		ID:        vaultic.NewRandomID().String(),
		State:     prunePlanPhaseA,
		CreatedAt: time.Now().UTC(),
	}
	if err := r.UpdateConfigAtomically(ctx, func(cfg *vaultic.Config) error {
		cfg.PrunePlan = plan
		return nil
	}); err != nil {
		return nil, err
	}
	return plan, nil
}

// PersistPrunePlan stores the exact cleanup candidates after replacement index
// files have been uploaded. The marker is an additive encrypted config field,
// which restic and rustic tolerate as an unknown extension.
func (r *Repository) PersistPrunePlan(ctx context.Context, observedIndexes, requiredIndexes, indexIDs, packIDs vaultic.IDSet) (*vaultic.PrunePlan, error) {
	plan, err := r.BeginPrunePlan(ctx)
	if err != nil {
		return nil, err
	}
	return r.CompletePrunePlan(ctx, plan.ID, observedIndexes, requiredIndexes, indexIDs, packIDs)
}

// CompletePrunePlan atomically promotes a pending phase-A claim to a ready
// marker containing the exact deletion candidates.
func (r *Repository) CompletePrunePlan(ctx context.Context, id string, observedIndexes, requiredIndexes, indexIDs, packIDs vaultic.IDSet) (*vaultic.PrunePlan, error) {
	plan := &vaultic.PrunePlan{
		Version:         prunePlanVersion,
		ID:              id,
		State:           prunePlanReady,
		CreatedAt:       time.Now().UTC(),
		ObservedIndexes: observedIndexes.List(),
		RequiredIndexes: requiredIndexes.List(),
		IndexIDs:        indexIDs.List(),
		PackIDs:         packIDs.List(),
	}
	if err := r.UpdateConfigAtomically(ctx, func(cfg *vaultic.Config) error {
		if cfg.PrunePlan == nil || cfg.PrunePlan.ID != id || cfg.PrunePlan.State != prunePlanPhaseA {
			return errors.Fatal("prune plan changed while phase A was running")
		}
		cfg.PrunePlan = plan
		return nil
	}); err != nil {
		return nil, err
	}
	return plan, nil
}

// FinalizePrunePlan revalidates and deletes the files named by a persisted
// prune plan. It never removes a file that is not explicitly in the marker.
// Required replacement indexes must exist before old indexes are deleted; pack
// candidates are checked against a freshly loaded post-index-delete index.
func (r *Repository) FinalizePrunePlan(ctx context.Context, printer vaultic.Printer) error {
	plan := r.Config().PrunePlan
	if plan == nil {
		return nil
	}
	if plan.Version != prunePlanVersion || plan.ID == "" {
		return errors.Fatal("repository contains an unsupported deferred prune plan")
	}
	if plan.State == prunePlanPhaseA {
		// Phase A was interrupted before its replacement indexes and exact
		// candidates became durable. Nothing may be deleted; discard only the
		// claim. Uploaded packs, if any, are harmless unreferenced leftovers.
		return r.clearPendingPrunePlan(ctx, plan.ID)
	}
	if plan.State != prunePlanReady {
		return errors.Fatal("repository contains an invalid deferred prune plan state")
	}

	// Config is the durable phase-A commit. Start phase B from a fresh backend
	// view, never from the in-memory index that created the marker.
	r.clearIndex()
	if err := r.LoadIndex(ctx, printer); err != nil {
		return err
	}
	currentIndexes := r.idx.IDs()
	for _, id := range plan.RequiredIndexes {
		if !currentIndexes.Has(id) {
			return fmt.Errorf("deferred prune plan %s cannot be finalized: replacement index %s is missing", plan.ID, id.Str())
		}
	}

	indexIDs := vaultic.NewIDSet(plan.IndexIDs...)
	survivingIndexes := currentIndexes.Sub(indexIDs)
	indexedPacks, err := r.packsFromIndexes(ctx, survivingIndexes)
	if err != nil {
		return err
	}
	for _, id := range plan.PackIDs {
		if indexedPacks.Has(id) {
			return fmt.Errorf("deferred prune plan %s cannot delete pack %s: it is referenced by a current non-obsolete index", plan.ID, id.Str())
		}
	}

	indexIDs = indexIDs.Intersect(currentIndexes)
	if len(indexIDs) != 0 {
		printer.P("finalizing deferred prune plan %s: removing %d old index files\n", plan.ID, len(indexIDs))
		if err := deleteFiles(ctx, false, &internalRepository{r}, indexIDs, vaultic.IndexFile, printer); err != nil {
			return err
		}
	}

	packIDs := vaultic.NewIDSet()
	for _, id := range plan.PackIDs {
		packIDs.Insert(id)
	}
	if err := r.excludeStagedPackRoots(ctx, packIDs); err != nil {
		return err
	}
	if len(packIDs) != 0 {
		printer.P("finalizing deferred prune plan %s: removing %d old packs\n", plan.ID, len(packIDs))
		// Missing packs are safe here: retaining too many was the only safety
		// concern, and the plan has already proven these packs unreferenced.
		if err := deleteFiles(ctx, true, &internalRepository{r}, packIDs, vaultic.PackFile, printer); err != nil {
			return err
		}
	}

	if err := r.UpdateConfigAtomically(ctx, func(cfg *vaultic.Config) error {
		if cfg.PrunePlan == nil || cfg.PrunePlan.ID != plan.ID {
			return errors.Fatal("deferred prune plan changed while it was being finalized")
		}
		cfg.PrunePlan = nil
		return nil
	}); err != nil {
		return err
	}
	r.clearIndex()
	return nil
}

// packsFromIndexes loads the explicit current non-obsolete indexes observed in
// the one initial List. It avoids a second backend List after old index files
// are deleted, which is essential for eventually-consistent backends.
func (r *Repository) packsFromIndexes(ctx context.Context, ids vaultic.IDSet) (vaultic.IDSet, error) {
	packs := vaultic.NewIDSet()
	for id := range ids {
		buf, err := r.LoadUnpacked(ctx, vaultic.IndexFile, id)
		if err != nil {
			return nil, fmt.Errorf("load index %s for prune plan revalidation: %w", id.Str(), err)
		}
		idx, err := index.DecodeIndex(buf, id)
		if err != nil {
			return nil, fmt.Errorf("decode index %s for prune plan revalidation: %w", id.Str(), err)
		}
		packs.Merge(idx.Packs())
	}
	return packs, nil
}

// FinalizePrunePlanLoaded finalizes a marker immediately after index rewrite.
// MasterIndex.Rewrite retains the just-saved replacement indexes in memory, so
// this path can validate/delete without a second backend index listing. That
// preserves the one-list discipline required by eventually-consistent stores.
func (r *Repository) FinalizePrunePlanLoaded(ctx context.Context, printer vaultic.Printer) error {
	plan := r.Config().PrunePlan
	if plan == nil {
		return nil
	}
	if plan.Version != prunePlanVersion || plan.ID == "" || plan.State != prunePlanReady {
		return errors.Fatal("repository contains an unsupported deferred prune plan")
	}

	currentIndexes := r.idx.IDs()
	for _, id := range plan.RequiredIndexes {
		if !currentIndexes.Has(id) {
			return fmt.Errorf("deferred prune plan %s cannot be finalized: replacement index %s is missing", plan.ID, id.Str())
		}
	}
	indexIDs := vaultic.NewIDSet(plan.IndexIDs...)
	if len(indexIDs) != 0 {
		printer.P("finalizing prune plan %s: removing %d old index files\n", plan.ID, len(indexIDs))
		if err := deleteFiles(ctx, false, &internalRepository{r}, indexIDs, vaultic.IndexFile, printer); err != nil {
			return err
		}
	}

	indexedPacks, err := r.packsFromIndexes(ctx, currentIndexes.Sub(vaultic.NewIDSet(plan.IndexIDs...)))
	if err != nil {
		return err
	}
	packIDs := vaultic.NewIDSet()
	for _, id := range plan.PackIDs {
		if indexedPacks.Has(id) {
			return fmt.Errorf("prune plan %s cannot delete pack %s: it is still referenced by a rewritten index", plan.ID, id.Str())
		}
		packIDs.Insert(id)
	}
	if err := r.excludeStagedPackRoots(ctx, packIDs); err != nil {
		return err
	}
	if len(packIDs) != 0 {
		printer.P("finalizing prune plan %s: removing %d old packs\n", plan.ID, len(packIDs))
		if err := deleteFiles(ctx, true, &internalRepository{r}, packIDs, vaultic.PackFile, printer); err != nil {
			return err
		}
	}
	if err := r.UpdateConfigAtomically(ctx, func(cfg *vaultic.Config) error {
		if cfg.PrunePlan == nil || cfg.PrunePlan.ID != plan.ID {
			return errors.Fatal("deferred prune plan changed while it was being finalized")
		}
		cfg.PrunePlan = nil
		return nil
	}); err != nil {
		return err
	}
	r.clearIndex()
	return nil
}

func (r *Repository) excludeStagedPackRoots(ctx context.Context, candidates vaultic.IDSet) error {
	if r.stagedPackRoots == nil || len(candidates) == 0 {
		return nil
	}
	staged, err := r.stagedPackRoots.Current(ctx)
	if err != nil {
		return fmt.Errorf("revalidate staged journal roots before prune deletion: %w", err)
	}
	for id := range staged {
		candidates.Delete(id)
	}
	return nil
}

func (r *Repository) clearPendingPrunePlan(ctx context.Context, id string) error {
	return r.UpdateConfigAtomically(ctx, func(cfg *vaultic.Config) error {
		if cfg.PrunePlan == nil || cfg.PrunePlan.ID != id || cfg.PrunePlan.State != prunePlanPhaseA {
			return errors.Fatal("pending prune plan changed while it was being cleared")
		}
		cfg.PrunePlan = nil
		return nil
	})
}
