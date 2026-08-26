package checker

import (
	"context"
	"testing"

	"github.com/vaultic/vaultic/internal/data"
	"github.com/vaultic/vaultic/internal/vaultic"
)

// TestCheckRepo runs the checker on repo.
func TestCheckRepo(t testing.TB, repo checkerRepository) {
	chkr := New(repo, true)

	hints, errs := chkr.LoadIndex(context.TODO(), vaultic.NoopTerminalCounterFactory)
	if len(errs) != 0 {
		t.Fatalf("errors loading index: %v", errs)
	}

	if len(hints) != 0 {
		t.Fatalf("errors loading index: %v", hints)
	}

	err := chkr.LoadSnapshots(context.TODO(), &data.SnapshotFilter{}, nil)
	if err != nil {
		t.Error(err)
	}

	// packs
	errChan := make(chan error)
	go chkr.Packs(context.TODO(), errChan)

	for err := range errChan {
		t.Error(err)
	}

	// structure
	errChan = make(chan error)
	go chkr.Structure(context.TODO(), vaultic.NoopCounter, errChan)

	for err := range errChan {
		t.Error(err)
	}

	// unused blobs
	blobs, err := chkr.UnusedBlobs(context.TODO())
	if err != nil {
		t.Error(err)
	}
	if len(blobs) > 0 {
		t.Errorf("unused blobs found: %v", blobs)
	}

	// read data
	errChan = make(chan error)
	go chkr.ReadPacks(context.TODO(), func(packs map[vaultic.ID]int64) map[vaultic.ID]int64 {
		return packs
	}, vaultic.NewNoopPrinter(), errChan)

	for err := range errChan {
		t.Error(err)
	}
}
