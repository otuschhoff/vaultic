package index

import (
	"context"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type backupIndexRepository interface {
	LoadPackHeader(context.Context, vaultic.ID) (pack.Blobs, error)
	SaveLegacyIndex(context.Context, *legacyindex.Index) (vaultic.ID, error)
}

func (engine *DaemonEngine) loadBackupCatalog(ctx context.Context, repo vaultic.ListerLoaderUnpacked, progress vaultic.Counter) (resultErr error) {
	engine.writeMu.Lock()
	defer engine.writeMu.Unlock()
	defer progress.Done()
	if engine.backupLoaded {
		return fmt.Errorf("on-demand backup index cannot be loaded twice")
	}
	engine.backupLoaded = true
	defer func() {
		if resultErr != nil {
			engine.mu.Lock()
			engine.exportErr = resultErr
			engine.mu.Unlock()
		}
	}()
	recovery, ok := repo.(backupIndexRepository)
	if !ok {
		return fmt.Errorf("on-demand backup requires authenticated pack recovery")
	}
	if err := engine.lookupError(ctx); err != nil {
		return err
	}
	sizes, available, err := engine.session.ScanPackInventory(ctx, func(ctx context.Context, id schema.ID, _ schema.PackRecord) error {
		blobs, err := engine.session.ReadPendingPack(ctx, id, recovery.LoadPackHeader)
		if err != nil {
			return err
		}
		index := legacyindex.NewIndex()
		index.StorePack(vaultic.ID(id), blobs)
		index.Finalize()
		if _, err := recovery.SaveLegacyIndex(ctx, index); err != nil {
			return err
		}
		if err := engine.store.MarkPackPublished(ctx, id); err != nil {
			return err
		}
		progress.Add(1)
		return nil
	})
	if err != nil {
		return err
	}
	if !available {
		return fmt.Errorf("on-demand backup requires complete pack sizing metadata; use full index loading")
	}
	if err := engine.recoverPendingSnapshots(ctx, repo); err != nil {
		return err
	}
	if err := engine.session.Validate(ctx); err != nil {
		return err
	}
	engine.mu.Lock()
	engine.blobSizes, engine.blobSizesValid = sizes, true
	engine.mu.Unlock()
	return nil
}
