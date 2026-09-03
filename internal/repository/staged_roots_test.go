package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type testStagedRoots struct {
	ids vaultic.IDSet
	err error
}

func (roots testStagedRoots) Current(context.Context) (vaultic.IDSet, error) {
	return roots.ids, roots.err
}

func TestExcludeStagedPackRootsRevalidatesAndFailsClosed(t *testing.T) {
	protected := vaultic.TestParseID("1111111111111111111111111111111111111111111111111111111111111111")
	ordinary := vaultic.TestParseID("2222222222222222222222222222222222222222222222222222222222222222")
	repo := &Repository{stagedPackRoots: testStagedRoots{ids: vaultic.NewIDSet(protected)}}
	candidates := vaultic.NewIDSet(protected, ordinary)
	if err := repo.excludeStagedPackRoots(context.Background(), candidates); err != nil {
		t.Fatal(err)
	}
	if candidates.Has(protected) || !candidates.Has(ordinary) {
		t.Fatalf("filtered candidates = %v", candidates)
	}
	repo.stagedPackRoots = testStagedRoots{err: errors.New("staging unavailable")}
	if err := repo.excludeStagedPackRoots(context.Background(), candidates); err == nil {
		t.Fatal("deletion did not fail closed when staging roots were unavailable")
	}
}
