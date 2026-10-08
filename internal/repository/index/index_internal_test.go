package index

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/otuschhoff/vaultic/internal/repository/pack"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/workingkv"
	"github.com/otuschhoff/vaultic/internal/workingstate"
)

func encodedWorkingIndex(t *testing.T) []byte {
	t.Helper()
	source := NewIndex()
	packID := source.addToPacks(vaultic.NewRandomID())
	for range 512 {
		source.store(packID, pack.Blob{BlobHandle: vaultic.NewRandomBlobHandle(), Length: 100})
	}
	var encoded bytes.Buffer
	rtest.OK(t, source.Encode(&encoded))
	return encoded.Bytes()
}

func TestWorkingIndexRetainsCompactCapacity(t *testing.T) {
	encoded := encodedWorkingIndex(t)
	policy, err := workingkv.NewPolicy(workingkv.ModeRAM, 32<<20, "")
	rtest.OK(t, err)
	ctx := workingkv.WithPolicy(t.Context(), policy)
	release, err := workingstate.ReserveJSON(ctx, encoded)
	rtest.OK(t, err)
	decodeCharge := policy.Budget().Used
	release()
	idx, err := decodeWorkingIndex(ctx, encoded, vaultic.NewRandomID())
	rtest.OK(t, err)
	defer idx.releaseWorking(true)
	retained := policy.Budget().Used
	if retained == 0 || retained >= decodeCharge/4 {
		t.Fatalf("retained compact charge=%d, transient decode charge=%d", retained, decodeCharge)
	}
}

func TestWorkingIndexAdmissionFailures(t *testing.T) {
	encoded := encodedWorkingIndex(t)
	probe, err := workingkv.NewPolicy(workingkv.ModeRAM, 32<<20, "")
	rtest.OK(t, err)
	release, err := workingstate.ReserveJSON(workingkv.WithPolicy(t.Context(), probe), encoded)
	rtest.OK(t, err)
	decodeCharge := probe.Budget().Used
	release()
	decoded, err := DecodeIndex(encoded, vaultic.NewRandomID())
	rtest.OK(t, err)
	for _, limit := range []uint64{decoded.workingCapacity(), decodeCharge} {
		policy, err := workingkv.NewPolicy(workingkv.ModeRAM, limit, "")
		rtest.OK(t, err)
		idx, err := decodeWorkingIndex(workingkv.WithPolicy(t.Context(), policy), encoded, vaultic.NewRandomID())
		if idx != nil || !errors.Is(err, workingstate.ErrMemoryLimitExceeded) || policy.Budget().Used != 0 {
			t.Fatalf("limit=%d index=%v err=%v budget=%+v", limit, idx, err, policy.Budget())
		}
	}
}

type cancellingIndexPolicy struct {
	*workingkv.Policy
	retainedCharge uint64
	cancel         context.CancelFunc
}

func (policy *cancellingIndexPolicy) Reserve(size uint64) error {
	err := policy.Policy.Reserve(size)
	if err == nil && size == policy.retainedCharge {
		policy.cancel()
	}
	return err
}

func TestWorkingIndexCancellationAfterRetainedAdmission(t *testing.T) {
	encoded := encodedWorkingIndex(t)
	decoded, err := DecodeIndex(encoded, vaultic.NewRandomID())
	rtest.OK(t, err)
	base, err := workingkv.NewPolicy(workingkv.ModeRAM, 32<<20, "")
	rtest.OK(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	policy := &cancellingIndexPolicy{Policy: base, retainedCharge: decoded.workingCapacity(), cancel: cancel}
	idx, err := decodeWorkingIndex(workingstate.WithPolicy(ctx, policy), encoded, vaultic.NewRandomID())
	if idx != nil || !errors.Is(err, context.Canceled) || base.Budget().Used != 0 {
		t.Fatalf("index=%v err=%v budget=%+v", idx, err, base.Budget())
	}
}

func TestWorkingIndexCumulativeLoadAndMerge(t *testing.T) {
	policy, err := workingkv.NewPolicy(workingkv.ModeRAM, 16<<20, "")
	rtest.OK(t, err)
	ctx := workingkv.WithPolicy(t.Context(), policy)
	master := NewMasterIndex()
	defer func() {
		rtest.OK(t, master.Close())
		rtest.Equals(t, uint64(0), policy.Budget().Used)
	}()
	for range 64 {
		idx, err := decodeWorkingIndex(ctx, encodedWorkingIndex(t), vaultic.NewRandomID())
		rtest.OK(t, err)
		master.Insert(idx)
		idx.releaseWorking(false)
	}
	beforeMerge := policy.Budget().Used
	rtest.OK(t, master.MergeFinalIndexes())
	rtest.Equals(t, beforeMerge, policy.Budget().Used)
	master.idx[0].releaseWorking(false)
	rtest.Equals(t, beforeMerge, policy.Budget().Used)
	if beforeMerge < master.idx[0].workingCapacity()/2 {
		t.Fatalf("merge backing-capacity headroom insufficient: charged=%d merged=%d", beforeMerge, master.idx[0].workingCapacity())
	}
}

func TestCatalogBuilderPreservesLocationsAndReusesPacks(t *testing.T) {
	builder := NewCatalogBuilder()
	packIDs := []vaultic.ID{vaultic.NewRandomID(), vaultic.NewRandomID()}
	blob := pack.Blob{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob}, Length: 100}
	for ordinal := range 1000 {
		blob.Offset = uint(ordinal * 100)
		rtest.OK(t, builder.Add(packIDs[ordinal%2], blob))
	}
	seen := 0
	for _, summary := range builder.PackSizes() {
		rtest.Equals(t, vaultic.DataBlob, summary.Type)
		rtest.Equals(t, uint64(500*(100+pack.CalculateEntrySize(false))), summary.Entries)
		seen++
	}
	rtest.Equals(t, 2, seen)
	idx := builder.Build()
	rtest.Equals(t, 2, len(idx.packs))
	rtest.Equals(t, 1000, len(idx.Lookup(blob.BlobHandle, nil)))
	rtest.Assert(t, !idx.Final(), "recovery projection must remain mutable")
	rtest.Assert(t, builder.packs == nil, "finished builder retained the pack map")
	rtest.Assert(t, builder.sizes == nil, "finished builder retained pack summaries")
	rtest.Assert(t, builder.Build() == nil, "second build must not return the index again")
	rtest.Assert(t, builder.Add(packIDs[0], blob) != nil, "finished builder accepted an entry")
	idx.Finalize()
	rtest.Assert(t, idx.Final(), "ordinary projection can be finalized")
}

func TestCatalogBuilderRejectsInvalidEntryWithoutMutation(t *testing.T) {
	packID := vaultic.NewRandomID()
	for _, invalid := range []pack.Blob{
		{BlobHandle: vaultic.BlobHandle{Type: vaultic.InvalidBlob}},
		{BlobHandle: vaultic.BlobHandle{Type: vaultic.NumBlobTypes}},
	} {
		builder := NewCatalogBuilder()
		rtest.Assert(t, builder.Add(packID, invalid) != nil, "invalid type accepted")
		rtest.Equals(t, 0, len(builder.Build().packs))
	}
	if strconv.IntSize == 64 {
		overflow := uint64(math.MaxUint32) + 1
		for _, invalid := range []pack.Blob{
			{BlobHandle: vaultic.BlobHandle{Type: vaultic.DataBlob}, Offset: uint(overflow)},
			{BlobHandle: vaultic.BlobHandle{Type: vaultic.DataBlob}, Length: uint(overflow)},
			{BlobHandle: vaultic.BlobHandle{Type: vaultic.TreeBlob}, UncompressedLength: uint(overflow)},
		} {
			builder := NewCatalogBuilder()
			rtest.Assert(t, builder.Add(packID, invalid) != nil, "overflow accepted")
			rtest.Equals(t, 0, len(builder.Build().packs))
		}
	}
}

func BenchmarkCatalogProjection(b *testing.B) {
	packIDs := make([]vaultic.ID, 128)
	for ordinal := range packIDs {
		packIDs[ordinal] = vaultic.NewRandomID()
	}
	blobs := make(pack.Blobs, 65536)
	for ordinal := range blobs {
		blobs[ordinal] = pack.Blob{BlobHandle: vaultic.BlobHandle{ID: vaultic.NewRandomID(), Type: vaultic.DataBlob},
			Offset: uint(ordinal * 100), Length: 100}
	}
	b.Run("map_then_copy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			byPack := make(map[vaultic.ID]pack.Blobs)
			for ordinal, blob := range blobs {
				packID := packIDs[ordinal%len(packIDs)]
				byPack[packID] = append(byPack[packID], blob)
			}
			idx := NewIndex()
			for packID, entries := range byPack {
				idx.StorePack(packID, entries)
			}
		}
	})
	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			builder := NewCatalogBuilder()
			for ordinal, blob := range blobs {
				if err := builder.Add(packIDs[ordinal%len(packIDs)], blob); err != nil {
					b.Fatal(err)
				}
			}
			builder.Build()
		}
	})
}

func TestIndexOversized(t *testing.T) {
	idx := NewIndex()

	// Add blobs up to indexMaxBlobs + pack.MaxHeaderEntries - 1
	packID := idx.addToPacks(vaultic.NewRandomID())
	id := vaultic.NewRandomID()
	for i := uint(0); i < indexMaxBlobs+pack.MaxHeaderEntries-1; i++ {
		// Directly modify ID to avoid benchmarking NewRandomID
		id[0] = byte(i)
		id[1] = byte(i >> 8)
		id[2] = byte(i >> 16)
		id[3] = byte(i >> 24)

		idx.store(packID, pack.Blob{
			BlobHandle: vaultic.BlobHandle{
				Type: vaultic.DataBlob,
				ID:   id,
			},
			Length: 100,
			Offset: uint(i) * 100,
		})
	}

	rtest.Assert(t, !Oversized(idx), "index should not be considered oversized")

	// Add one more blob to exceed the limit
	idx.store(packID, pack.Blob{
		BlobHandle: vaultic.BlobHandle{
			Type: vaultic.DataBlob,
			ID:   vaultic.NewRandomID(),
		},
		Length: 100,
		Offset: uint(indexMaxBlobs+pack.MaxHeaderEntries) * 100,
	})

	rtest.Assert(t, Oversized(idx), "index should be considered oversized")
}
