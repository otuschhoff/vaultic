package repository

import (
	"context"
	"sync"

	"github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type RepairIndexOptions struct {
	ReadAllPacks bool
}

func RepairIndex(ctx context.Context, repo *Repository, opts RepairIndexOptions, printer vaultic.Printer) error {
	var obsoleteIndexes vaultic.IDs
	packSizeFromList := make(map[vaultic.ID]int64)
	packSizeFromIndex := make(map[vaultic.ID]int64)
	removePacks := vaultic.NewIDSet()

	//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
	if opts.ReadAllPacks {
		// get list of old index files but start with empty index
		err := repo.List(ctx, vaultic.IndexFile, func(id vaultic.ID, _ int64) error {
			obsoleteIndexes = append(obsoleteIndexes, id)
			return nil
		})
		if err != nil {
			return err
		}
		repo.clearIndex()

	} else {
		printer.P("loading indexes...\n")
		err := repo.loadIndexWithCallback(ctx, vaultic.NoopTerminalCounterFactory, func(id vaultic.ID, _ *index.Index, err error) error {
			if err != nil {
				printer.E("removing invalid index %v: %v\n", id, err)
				obsoleteIndexes = append(obsoleteIndexes, id)
				return nil
			}
			return nil
		})
		if err != nil {
			return err
		}

		packSizeFromIndex, err = pack.Size(ctx, repo, false)
		if err != nil {
			return err
		}
	}

	oldIndexes := repo.idx.IDs()

	printer.P("getting pack files to read...\n")
	err := repo.List(ctx, vaultic.PackFile, func(id vaultic.ID, packSize int64) error {
		size, ok := packSizeFromIndex[id]
		if !ok || size != packSize {
			// Pack was not referenced in index or size does not match
			packSizeFromList[id] = packSize
			removePacks.Insert(id)
		}
		if !ok {
			printer.E("adding pack file to index %v\n", id)
		} else if size != packSize {
			printer.E("reindexing pack file %v with unexpected size %v instead of %v\n", id, packSize, size)
		}
		delete(packSizeFromIndex, id)
		return nil
	})
	if err != nil {
		return err
	}
	for id := range packSizeFromIndex {
		// forget pack files that are referenced in the index but do not exist
		// when rebuilding the index
		removePacks.Insert(id)
		printer.E("removing not found pack file %v\n", id)
	}

	if len(packSizeFromList) > 0 {
		printer.P("reading pack files\n")
		bar := printer.NewCounter("packs")
		bar.SetMax(uint64(len(packSizeFromList)))
		invalidFiles, err := repo.createIndexFromPacks(ctx, packSizeFromList, bar)
		bar.Done()
		if err != nil {
			return err
		}

		for _, id := range invalidFiles {
			printer.V("skipped incomplete pack file: %v\n", id)
		}
	}

	err = rewriteIndexFiles(ctx, repo, removePacks, oldIndexes, obsoleteIndexes, printer)
	if err != nil {
		return err
	}

	// drop outdated in-memory index
	repo.clearIndex()
	return nil
}

// rewriteIndexFiles rebuilds the index, excluding removePacks. When earlyDelete
// is non-nil the superseded index files are NOT deleted here; instead their IDs
// are returned via the *earlyDelete set so the caller can delete them earlier.
func rewriteIndexFiles(
	ctx context.Context,
	repo *Repository,
	removePacks vaultic.IDSet,
	oldIndexes vaultic.IDSet,
	extraObsolete vaultic.IDs,
	printer vaultic.Printer,
) error {
	return rewriteIndexFilesOpt(ctx, repo, removePacks, oldIndexes, extraObsolete, nil, nil, printer)
}

func rewriteIndexFilesOpt(
	ctx context.Context,
	repo *Repository,
	removePacks vaultic.IDSet,
	oldIndexes vaultic.IDSet,
	extraObsolete vaultic.IDs,
	earlyDelete *vaultic.IDSet,
	savedIndexes *vaultic.IDSet,
	printer vaultic.Printer,
) error {
	printer.P("rebuilding index\n")

	bar := printer.NewCounter("indexes processed")
	opts := index.MasterIndexRewriteOpts{
		SaveProgress: bar,
		DeleteProgress: func() vaultic.Counter {
			return printer.NewCounter("old indexes deleted")
		},
		DeleteReport: func(id vaultic.ID, err error) {
			if err != nil {
				printer.VV("failed to remove index %v: %v\n", id.String(), err)
			} else {
				printer.VV("removed index %v\n", id.String())
			}
		},
	}
	if earlyDelete != nil {
		opts.SkipObsoleteDelete = true
		opts.ObsoleteIndexFunc = func(obsolete vaultic.IDSet) {
			*earlyDelete = obsolete
		}
	}
	if savedIndexes != nil {
		var savedIndexesMutex sync.Mutex
		opts.SavedIndexFunc = func(id vaultic.ID) {
			savedIndexesMutex.Lock()
			defer savedIndexesMutex.Unlock()
			savedIndexes.Insert(id)
		}
	}
	return repo.idx.Rewrite(ctx, &internalRepository{repo}, removePacks, oldIndexes, extraObsolete, opts)
}
