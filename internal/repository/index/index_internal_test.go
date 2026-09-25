package index

import (
	"math"
	"strconv"
	"testing"

	"github.com/otuschhoff/vaultic/internal/repository/pack"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

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
