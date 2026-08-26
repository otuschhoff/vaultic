package repository

import (
	"context"
	"testing"

	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestAppendTransactionSavesSnapshot(t *testing.T) {
	TestAllVersions(t, func(t *testing.T, version uint) {
		repo, _, _ := TestRepositoryWithVersion(t, version)
		tx := repo.AppendTransaction()
		id, err := tx.SaveUnpacked(context.Background(), vaultic.WriteableSnapshotFile, []byte("append-only snapshot"))
		rtest.OK(t, err)
		data, err := repo.LoadUnpacked(context.Background(), vaultic.SnapshotFile, id)
		rtest.OK(t, err)
		rtest.Equals(t, "append-only snapshot", string(data))
	})
}

// AppendRepository intentionally has no RemoveUnpacked method. This helper is
// a compile-time assertion that append workflows cannot accept a remover.
func acceptsAppendRepository(_ vaultic.AppendRepository) {}

func TestAppendTransactionCapability(t *testing.T) {
	TestAllVersions(t, func(t *testing.T, version uint) {
		repo, _, _ := TestRepositoryWithVersion(t, version)
		acceptsAppendRepository(repo.AppendTransaction())
	})
}
