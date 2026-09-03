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

type recordingRemover struct{ removed vaultic.IDSet }

func (remover *recordingRemover) Connections() uint { return 1 }
func (remover *recordingRemover) RemoveUnpacked(_ context.Context, _ vaultic.FileType, id vaultic.ID) error {
	remover.removed.Insert(id)
	return nil
}

func (roots testStagedRoots) Current(context.Context) (vaultic.IDSet, error) {
	return roots.ids, roots.err
}

func TestStagedRootRevalidatingRemoverBlocksNewRoot(t *testing.T) {
	protected := vaultic.TestParseID("1111111111111111111111111111111111111111111111111111111111111111")
	inner := &recordingRemover{removed: vaultic.NewIDSet()}
	remover := stagedRootRevalidatingRemover{RemoverUnpacked: inner, roots: testStagedRoots{ids: vaultic.NewIDSet(protected)}}
	if err := remover.RemoveUnpacked(context.Background(), vaultic.PackFile, protected); err == nil {
		t.Fatal("new staged root did not block immediate pack deletion")
	}
	if inner.removed.Has(protected) {
		t.Fatal("protected pack reached physical remover")
	}
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
