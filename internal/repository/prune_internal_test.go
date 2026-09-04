package repository

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func newPackActionStateForTest(indexPack map[vaultic.ID]packInfo) *packActionState {
	return &packActionState{
		targetPackSize:   100,
		indexPack:        indexPack,
		stats:            &PruneStats{},
		printer:          vaultic.NewNoopPrinter(),
		removePacksFirst: vaultic.NewIDSet(),
		removePacks:      vaultic.NewIDSet(),
	}
}

func TestPackActionStateProcessPack(t *testing.T) {
	t.Run("unindexed", func(t *testing.T) {
		state := newPackActionStateForTest(map[vaultic.ID]packInfo{})
		id := vaultic.NewRandomID()

		processed, err := state.processPack(id, 42)
		rtest.OK(t, err)
		rtest.Equals(t, false, processed)
		rtest.Assert(t, state.removePacksFirst.Has(id), "unindexed pack was not selected for early removal")
		rtest.Equals(t, uint64(42), state.stats.Size.Unref)
	})

	t.Run("repack candidate", func(t *testing.T) {
		id := vaultic.NewRandomID()
		info := packInfo{usedBlobs: 1, unusedBlobs: 1, usedSize: 60, unusedSize: 40, tpe: vaultic.DataBlob}
		state := newPackActionStateForTest(map[vaultic.ID]packInfo{id: info})

		processed, err := state.processPack(id, 100)
		rtest.OK(t, err)
		rtest.Equals(t, true, processed)
		rtest.Equals(t, 0, len(state.indexPack))
		rtest.Equals(t, []packInfoWithID{{ID: id, packInfo: info}}, state.repackCandidates)
		rtest.Equals(t, uint(1), state.stats.Packs.PartlyUsed)
	})

	t.Run("size mismatch", func(t *testing.T) {
		id := vaultic.NewRandomID()
		info := packInfo{usedBlobs: 1, usedSize: 99, tpe: vaultic.DataBlob}
		state := newPackActionStateForTest(map[vaultic.ID]packInfo{id: info})

		processed, err := state.processPack(id, 100)
		rtest.Equals(t, false, processed)
		rtest.Equals(t, ErrSizeNotMatching, err)
		rtest.Assert(t, state.indexPack[id].usedBlobs == 1, "mismatched pack was removed from index map")
	})
}

// TestPruneMaxUnusedDuplicate checks that MaxUnused correctly accounts for duplicates.
//
// Create a repository containing blobs a to d that are stored in packs as follows:
// - a, d
// - b, d
// - c, d
// All blobs should be kept during prune, but the duplicates should be gone afterwards.
// The special construction ensures that each pack contains a used, non-duplicate blob.
// This ensures that special cases that delete completely duplicate packs files do not
// apply.
func TestPruneMaxUnusedDuplicate(t *testing.T) {
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	t.Logf("rand initialized with seed %d", seed)

	repo, _, _ := TestRepositoryWithVersion(t, 0)
	// ensure blobs are assembled into packs as expected
	repo.packerCount = 1
	// large blobs to prevent repacking due to too small packsize
	const blobSize = 1024 * 1024

	bufs := [][]byte{}
	for range 4 {
		// use uniform length for simpler control via MaxUnusedBytes
		buf := make([]byte, blobSize)
		random.Read(buf)
		bufs = append(bufs, buf)
	}
	keep := vaultic.NewBlobSet()

	for _, blobs := range [][][]byte{
		{bufs[0], bufs[3]},
		{bufs[1], bufs[3]},
		{bufs[2], bufs[3]},
	} {
		rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
			for _, blob := range blobs {
				id, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, blob, vaultic.ID{}, true)
				keep.Insert(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id})
				rtest.OK(t, err)
			}
			return nil
		}))
	}

	opts := PruneOptions{
		MaxRepackBytes: math.MaxUint64,
		// non-zero number of unused bytes, that is nevertheless smaller than a single blob
		// setting this to zero would bypass the unused/duplicate size accounting that should
		// be tested here
		MaxUnusedBytes: func(used uint64) (unused uint64) { return blobSize / 2 },
	}

	plan, err := PlanPrune(context.TODO(), opts, repo, func(ctx context.Context, repo vaultic.Repository, usedBlobs vaultic.FindBlobSet) error {
		for blob := range keep {
			usedBlobs.Insert(blob)
		}
		return nil
	}, vaultic.NewNoopPrinter())
	rtest.OK(t, err)

	rtest.OK(t, plan.Execute(context.TODO(), vaultic.NewNoopPrinter()))

	rsize := plan.Stats().Size
	remainingUnusedSize := rsize.Duplicate + rsize.Unused - rsize.Remove - rsize.Repackrm
	maxUnusedSize := opts.MaxUnusedBytes(rsize.Used)
	rtest.Assert(t, remainingUnusedSize <= maxUnusedSize, "too much unused data remains got %v, expected less than %v", remainingUnusedSize, maxUnusedSize)

	// divide by blobSize to ignore pack file overhead
	rtest.Equals(t, rsize.Used/blobSize, uint64(4))
	rtest.Equals(t, rsize.Duplicate/blobSize, uint64(2))
	rtest.Equals(t, rsize.Unused, uint64(0))
	rtest.Equals(t, rsize.Remove, uint64(0))
	rtest.Equals(t, rsize.Repack/blobSize, uint64(4))
	rtest.Equals(t, rsize.Repackrm/blobSize, uint64(2))
	rtest.Equals(t, rsize.Unref, uint64(0))
	rtest.Equals(t, rsize.Uncompressed, uint64(0))
}
