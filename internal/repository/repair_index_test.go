package repository_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func listIndex(t *testing.T, repo vaultic.Lister) vaultic.IDSet {
	return listFiles(t, repo, vaultic.IndexFile)
}

func testRebuildIndex(t *testing.T, readAllPacks bool, damage func(t *testing.T, repo *repository.Repository, be backend.Backend)) {
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	t.Logf("rand initialized with seed %d", seed)

	repo, _, be := repository.TestRepositoryWithVersion(t, 0)
	createRandomBlobs(t, random, repo, 4, 0.5, true)
	createRandomBlobs(t, random, repo, 5, 0.5, true)
	indexes := listIndex(t, repo)
	t.Logf("old indexes %v", indexes)

	damage(t, repo, be)

	repo = repository.TestOpenBackend(t, be)
	rtest.OK(t, repository.RepairIndex(context.TODO(), repo, repository.RepairIndexOptions{
		ReadAllPacks: readAllPacks,
	}, vaultic.NewNoopPrinter()))

	repository.TestCheckRepo(t, repo)
}

func TestRebuildIndex(t *testing.T) {
	for _, test := range []struct {
		name   string
		damage func(t *testing.T, repo *repository.Repository, be backend.Backend)
	}{
		{
			"valid index",
			func(t *testing.T, repo *repository.Repository, be backend.Backend) {},
		},
		{
			"damaged index",
			func(t *testing.T, repo *repository.Repository, be backend.Backend) {
				index := listIndex(t, repo).List()[0]
				replaceFile(t, be, backend.Handle{Type: backend.IndexFile, Name: index.String()}, func(b []byte) []byte {
					b[0] ^= 0xff
					return b
				})
			},
		},
		{
			"missing index",
			func(t *testing.T, repo *repository.Repository, be backend.Backend) {
				index := listIndex(t, repo).List()[0]
				rtest.OK(t, be.Remove(context.TODO(), backend.Handle{Type: backend.IndexFile, Name: index.String()}))
			},
		},
		{
			"missing pack",
			func(t *testing.T, repo *repository.Repository, be backend.Backend) {
				pack := listPacks(t, repo).List()[0]
				rtest.OK(t, be.Remove(context.TODO(), backend.Handle{Type: backend.PackFile, Name: pack.String()}))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			testRebuildIndex(t, false, test.damage)
			testRebuildIndex(t, true, test.damage)
		})
	}
}
