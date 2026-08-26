package index_test

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	"github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestRepositoryForAllIndexes(t *testing.T) {
	originalFull := index.Full
	defer func() {
		index.Full = originalFull
	}()
	index.Full = func(*index.Index) bool { return true }

	repo, unpacked, _ := repository.TestRepositoryWithVersion(t, vaultic.StableRepoVersion)

	mi := index.NewMasterIndex()
	for range 3 {
		packID := vaultic.NewRandomID()
		blob := pack.Blob{
			BlobHandle: vaultic.NewRandomBlobHandle(),
			Length:     uint(crypto.CiphertextLength(10)),
			Offset:     0,
		}
		rtest.OK(t, mi.StorePack(context.TODO(), packID, pack.Blobs{blob}, unpacked))
		rtest.OK(t, mi.Flush(context.TODO(), unpacked))
	}

	expectedIndexIDs := vaultic.NewIDSet()
	rtest.OK(t, repo.List(context.TODO(), vaultic.IndexFile, func(id vaultic.ID, size int64) error {
		expectedIndexIDs.Insert(id)
		return nil
	}))
	rtest.Assert(t, len(expectedIndexIDs) > 1, "test repo should have multiple indexes")

	// check that all expected indexes are loaded without errors
	indexIDs := vaultic.NewIDSet()
	var indexErr error
	rtest.OK(t, index.ForAllIndexes(context.TODO(), repo, repo, func(id vaultic.ID, index *index.Index, err error) error {
		if err != nil {
			indexErr = err
		}
		indexIDs.Insert(id)
		return nil
	}))
	rtest.OK(t, indexErr)
	rtest.Equals(t, expectedIndexIDs, indexIDs)

	// must failed with the returned error
	iterErr := errors.New("error to pass upwards")

	err := index.ForAllIndexes(context.TODO(), repo, repo, func(id vaultic.ID, index *index.Index, err error) error {
		return iterErr
	})

	rtest.Equals(t, iterErr, err)
}
