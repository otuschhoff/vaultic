package repository

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type noAtomicReplaceBackend struct{ backend.Backend }

func (b noAtomicReplaceBackend) Properties() backend.Properties {
	p := b.Backend.Properties()
	p.HasAtomicReplace = false
	return p
}

func TestPersistAndFinalizeEmptyPrunePlan(t *testing.T) {
	TestAllVersions(t, func(t *testing.T, version uint) {
		repo, _, _ := TestRepositoryWithVersion(t, version)
		marker, err := repo.PersistPrunePlan(context.Background(), vaultic.NewIDSet(), vaultic.NewIDSet(), vaultic.NewIDSet(), vaultic.NewIDSet())
		rtest.OK(t, err)
		rtest.Assert(t, marker.ID != "", "plan ID is empty")
		rtest.Assert(t, repo.Config().PrunePlan != nil, "plan was not persisted")

		rtest.OK(t, repo.FinalizePrunePlan(context.Background(), vaultic.NewNoopPrinter()))
		rtest.Assert(t, repo.Config().PrunePlan == nil, "plan was not cleared")
	})
}

func TestPrunePlanRequiresReplacementIndexes(t *testing.T) {
	TestAllVersions(t, func(t *testing.T, version uint) {
		repo, _, _ := TestRepositoryWithVersion(t, version)
		missing := vaultic.NewRandomID()
		_, err := repo.PersistPrunePlan(context.Background(), vaultic.NewIDSet(), vaultic.NewIDSet(missing), vaultic.NewIDSet(), vaultic.NewIDSet())
		rtest.OK(t, err)
		err = repo.FinalizePrunePlan(context.Background(), vaultic.NewNoopPrinter())
		rtest.Assert(t, err != nil, "finalized plan with missing replacement index")
		rtest.Assert(t, repo.Config().PrunePlan != nil, "failed plan was cleared")
	})
}

func TestPrunePlanDoesNotDeleteReferencedPack(t *testing.T) {
	TestAllVersions(t, func(t *testing.T, version uint) {
		repo, _, _ := TestRepositoryWithVersion(t, version)
		var blobID vaultic.ID
		rtest.OK(t, repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
			id, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, []byte("referenced"), vaultic.ID{}, false)
			blobID = id
			return err
		}))
		rtest.OK(t, repo.LoadIndex(context.Background(), vaultic.NoopTerminalCounterFactory))
		packs := repo.idx.Packs(nil)
		rtest.Assert(t, len(packs) == 1, "expected one pack for blob %s, got %d", blobID.Str(), len(packs))
		indexes := repo.idx.IDs()
		_, err := repo.PersistPrunePlan(context.Background(), indexes, indexes, vaultic.NewIDSet(), packs)
		rtest.OK(t, err)

		err = repo.FinalizePrunePlan(context.Background(), vaultic.NewNoopPrinter())
		rtest.Assert(t, err != nil, "finalized a plan that deletes a referenced pack")
		rtest.Assert(t, repo.Config().PrunePlan != nil, "referenced-pack plan was cleared")
	})
}

func TestPrunePlanRequiresAtomicConfigReplacement(t *testing.T) {
	TestAllVersions(t, func(t *testing.T, version uint) {
		repo, _, _ := TestRepositoryWithVersion(t, version)
		repo.be = noAtomicReplaceBackend{Backend: repo.be}
		_, err := repo.PersistPrunePlan(context.Background(), vaultic.NewIDSet(), vaultic.NewIDSet(), vaultic.NewIDSet(), vaultic.NewIDSet())
		rtest.Assert(t, err != nil, "persisted plan without atomic config replacement")
		rtest.Assert(t, repo.Config().PrunePlan == nil, "non-atomic backend stored a plan")
	})
}
