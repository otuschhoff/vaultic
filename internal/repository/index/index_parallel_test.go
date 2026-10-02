package index_test

import (
	"context"
	"sync"
	"testing"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	"github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/workingkv"
)

type delayedIndexLoader struct {
	vaultic.LoaderUnpacked
	delayed     vaultic.ID
	laterLoaded chan struct{}
	notifyOnce  *sync.Once
}

func TestM4IndexDecodeOwnership(t *testing.T) {
	repo, unpacked, _ := repository.TestRepositoryWithVersion(t, vaultic.StableRepoVersion)
	source := index.NewMasterIndex()
	for range 3 {
		rtest.OK(t, source.StorePack(t.Context(), vaultic.NewRandomID(), pack.Blobs{{BlobHandle: vaultic.NewRandomBlobHandle(), Length: uint(crypto.CiphertextLength(10))}}, unpacked))
		rtest.OK(t, source.Flush(t.Context(), unpacked))
	}
	policy, err := workingkv.NewPolicy(workingkv.ModeRAM, 32<<20, "")
	rtest.OK(t, err)
	ctx := workingkv.WithPolicy(t.Context(), policy)
	for _, ordered := range []bool{false, true} {
		master := index.NewMasterIndex()
		callback := func(_ vaultic.ID, idx *index.Index, err error) error {
			if err == nil {
				master.Insert(idx)
			}
			return err
		}
		if ordered {
			err = index.ForAllIndexesInOrder(ctx, repo, repo, callback)
		} else {
			err = index.ForAllIndexesWorkers(ctx, repo, repo, 2, callback)
		}
		rtest.OK(t, err)
		if policy.Budget().Used == 0 {
			t.Fatal("retained full index released early")
		}
		rtest.OK(t, master.MergeFinalIndexes())
		rtest.OK(t, master.Close())
		if policy.Budget().Used != 0 {
			t.Fatal("full index reservation leaked", policy.Budget())
		}
	}
	stop := errors.New("stop callback")
	err = index.ForAllIndexesInOrder(ctx, repo, repo, func(vaultic.ID, *index.Index, error) error { return stop })
	if !errors.Is(err, stop) || policy.Budget().Used != 0 {
		t.Fatal("cancelled read-ahead leaked", err, policy.Budget())
	}
	tiny, _ := workingkv.NewPolicy(workingkv.ModeRAM, 1, "")
	err = index.ForAllIndexesWorkers(workingkv.WithPolicy(t.Context(), tiny), repo, repo, 2, func(vaultic.ID, *index.Index, error) error { return nil })
	if !errors.Is(err, workingkv.ErrWorkingMemoryLimitExceeded) || tiny.Budget().Used != 0 {
		t.Fatal("admission failure became success", err, tiny.Budget())
	}
}

func (loader delayedIndexLoader) LoadUnpacked(ctx context.Context, fileType vaultic.FileType, id vaultic.ID) ([]byte, error) {
	if id == loader.delayed {
		select {
		case <-loader.laterLoaded:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	encoded, err := loader.LoaderUnpacked.LoadUnpacked(ctx, fileType, id)
	if id != loader.delayed && err == nil {
		loader.notifyOnce.Do(func() { close(loader.laterLoaded) })
	}
	return encoded, err
}

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

func TestRepositoryForAllIndexesInOrder(t *testing.T) {
	repo, unpacked, _ := repository.TestRepositoryWithVersion(t, vaultic.StableRepoVersion)
	mi := index.NewMasterIndex()
	for range 3 {
		blob := pack.Blob{BlobHandle: vaultic.NewRandomBlobHandle(), Length: uint(crypto.CiphertextLength(10))}
		rtest.OK(t, mi.StorePack(context.Background(), vaultic.NewRandomID(), pack.Blobs{blob}, unpacked))
		rtest.OK(t, mi.Flush(context.Background(), unpacked))
	}

	lister, err := vaultic.MemorizeList(context.Background(), repo, vaultic.IndexFile)
	rtest.OK(t, err)
	var listed []vaultic.ID
	rtest.OK(t, lister.List(context.Background(), vaultic.IndexFile, func(id vaultic.ID, _ int64) error {
		listed = append(listed, id)
		return nil
	}))
	rtest.Assert(t, len(listed) > 1, "test repo should have multiple indexes")

	loader := delayedIndexLoader{
		LoaderUnpacked: repo, delayed: listed[0], laterLoaded: make(chan struct{}), notifyOnce: &sync.Once{},
	}
	var visited []vaultic.ID
	rtest.OK(t, index.ForAllIndexesInOrder(context.Background(), lister, loader, func(id vaultic.ID, _ *index.Index, err error) error {
		rtest.OK(t, err)
		visited = append(visited, id)
		return nil
	}))
	rtest.Equals(t, listed, visited)

	iterErr := errors.New("stop ordered traversal")
	callbacks := 0
	err = index.ForAllIndexesInOrder(context.Background(), lister, loader, func(vaultic.ID, *index.Index, error) error {
		callbacks++
		return iterErr
	})
	rtest.Equals(t, iterErr, err)
	rtest.Equals(t, 1, callbacks)
}
