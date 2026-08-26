package index_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/vaultic/vaultic/internal/repository/index"
	"github.com/vaultic/vaultic/internal/repository/pack"
	rtest "github.com/vaultic/vaultic/internal/test"
	"github.com/vaultic/vaultic/internal/vaultic"
)

func TestIndexSerialize(t *testing.T) {
	tests := []*pack.PackedBlob{}

	idx := index.NewIndex()

	// create 50 packs with 20 blobs each
	for i := range 50 {
		packID := vaultic.NewRandomID()
		var blobs pack.Blobs

		pos := uint(0)
		for j := range 20 {
			length := uint(i*100 + j)
			uncompressedLength := uint(0)
			if i >= 25 {
				// test a mix of compressed and uncompressed packs
				uncompressedLength = 2 * length
			}
			pb := &pack.PackedBlob{
				Pack: packID,
				Blob: pack.Blob{
					BlobHandle:         vaultic.NewRandomBlobHandle(),
					Offset:             pos,
					Length:             length,
					UncompressedLength: uncompressedLength,
				},
			}
			blobs = append(blobs, pb.Blob)
			tests = append(tests, pb)
			pos += length
		}
		idx.StorePack(packID, blobs)
	}

	wr := bytes.NewBuffer(nil)
	err := idx.Encode(wr)
	rtest.OK(t, err)

	idx2ID := vaultic.NewRandomID()
	idx2, err := index.DecodeIndex(wr.Bytes(), idx2ID)
	rtest.OK(t, err)
	rtest.Assert(t, idx2 != nil, "nil returned for decoded index")
	indexID, err := idx2.IDs()
	rtest.OK(t, err)
	rtest.Equals(t, indexID, vaultic.IDs{idx2ID})

	wr2 := bytes.NewBuffer(nil)
	err = idx2.Encode(wr2)
	rtest.OK(t, err)

	for _, testBlob := range tests {
		list := idx.Lookup(testBlob.Handle(), nil)
		if len(list) != 1 {
			t.Errorf("expected one result for blob %v, got %v: %v", testBlob.Handle().ID.String(), len(list), list)
		}
		result := list[0]

		rtest.Equals(t, testBlob, result)

		list2 := idx2.Lookup(testBlob.Handle(), nil)
		if len(list2) != 1 {
			t.Errorf("expected one result for blob %v, got %v: %v", testBlob.Handle().ID.String(), len(list2), list2)
		}
		result2 := list2[0]

		rtest.Equals(t, testBlob, result2)
	}

	// add more blobs to idx
	newtests := []*pack.PackedBlob{}
	for i := range 10 {
		packID := vaultic.NewRandomID()
		var blobs pack.Blobs

		pos := uint(0)
		for j := range 10 {
			length := uint(i*100 + j)
			pb := &pack.PackedBlob{
				Pack: packID,
				Blob: pack.Blob{
					BlobHandle: vaultic.NewRandomBlobHandle(),
					Offset:     pos,
					Length:     length,
				},
			}
			blobs = append(blobs, pb.Blob)
			newtests = append(newtests, pb)
			pos += length
		}
		idx.StorePack(packID, blobs)
	}

	// finalize; serialize idx, unserialize to idx3
	idx.Finalize()
	wr3 := bytes.NewBuffer(nil)
	err = idx.Encode(wr3)
	rtest.OK(t, err)

	rtest.Assert(t, idx.Final(),
		"index not final after encoding")

	id := vaultic.NewRandomID()
	rtest.OK(t, idx.SetID(id))
	ids, err := idx.IDs()
	rtest.OK(t, err)
	rtest.Equals(t, vaultic.IDs{id}, ids)

	idx3, err := index.DecodeIndex(wr3.Bytes(), id)
	rtest.OK(t, err)
	rtest.Assert(t, idx3 != nil, "nil returned for decoded index")
	rtest.Assert(t, idx3.Final(), "decoded index is not final")

	// all new blobs must be in the index
	for _, testBlob := range newtests {
		list := idx3.Lookup(testBlob.Handle(), nil)
		if len(list) != 1 {
			t.Errorf("expected one result for blob %v, got %v: %v", testBlob.Handle().ID.String(), len(list), list)
		}

		blob := list[0]

		rtest.Equals(t, testBlob, blob)
	}
}

func TestIndexSize(t *testing.T) {
	idx := index.NewIndex()

	packs := 200
	blobCount := 100
	for i := range packs {
		packID := vaultic.NewRandomID()
		var blobs pack.Blobs

		pos := uint(0)
		for j := range blobCount {
			length := uint(i*100 + j)
			blobs = append(blobs, pack.Blob{
				BlobHandle: vaultic.NewRandomBlobHandle(),
				Offset:     pos,
				Length:     length,
			})

			pos += length
		}
		idx.StorePack(packID, blobs)
	}

	wr := bytes.NewBuffer(nil)

	err := idx.Encode(wr)
	rtest.OK(t, err)

	rtest.Equals(t, uint(packs*blobCount), idx.Len(vaultic.DataBlob))
	rtest.Equals(t, uint(0), idx.Len(vaultic.TreeBlob))

	t.Logf("Index file size for %d blobs in %d packs is %d", blobCount*packs, packs, wr.Len())
}

// example index serialization from doc/Design.rst
var docExampleV1 = []byte(`
{
  "supersedes": [
	"ed54ae36197f4745ebc4b54d10e0f623eaaaedd03013eb7ae90df881b7781452"
  ],
  "packs": [
	{
	  "id": "73d04e6125cf3c28a299cc2f3cca3b78ceac396e4fcf9575e34536b26782413c",
	  "blobs": [
		{
		  "id": "3ec79977ef0cf5de7b08cd12b874cd0f62bbaf7f07f3497a5b1bbcc8cb39b1ce",
		  "type": "data",
		  "offset": 0,
		  "length": 38
		},{
		  "id": "9ccb846e60d90d4eb915848add7aa7ea1e4bbabfc60e573db9f7bfb2789afbae",
		  "type": "tree",
		  "offset": 38,
		  "length": 112
		},
		{
		  "id": "d3dc577b4ffd38cc4b32122cabf8655a0223ed22edfd93b353dc0c3f2b0fdf66",
		  "type": "data",
		  "offset": 150,
		  "length": 123
		}
	  ]
	}
  ]
}
`)

var docExampleV2 = []byte(`
{
	"supersedes": [
	  "ed54ae36197f4745ebc4b54d10e0f623eaaaedd03013eb7ae90df881b7781452"
	],
	"packs": [
	  {
		"id": "73d04e6125cf3c28a299cc2f3cca3b78ceac396e4fcf9575e34536b26782413c",
		"blobs": [
		  {
			"id": "3ec79977ef0cf5de7b08cd12b874cd0f62bbaf7f07f3497a5b1bbcc8cb39b1ce",
			"type": "data",
			"offset": 0,
			"length": 38
		  },
		  {
			"id": "9ccb846e60d90d4eb915848add7aa7ea1e4bbabfc60e573db9f7bfb2789afbae",
			"type": "tree",
			"offset": 38,
			"length": 112,
			"uncompressed_length": 511
		  },
		  {
			"id": "d3dc577b4ffd38cc4b32122cabf8655a0223ed22edfd93b353dc0c3f2b0fdf66",
			"type": "data",
			"offset": 150,
			"length": 123,
			"uncompressed_length": 234
		  }
		]
	  }
	]
  }
`)

var exampleTests = []struct {
	id, packID         vaultic.ID
	tpe                vaultic.BlobType
	offset, length     uint
	uncompressedLength uint
}{
	{
		vaultic.TestParseID("3ec79977ef0cf5de7b08cd12b874cd0f62bbaf7f07f3497a5b1bbcc8cb39b1ce"),
		vaultic.TestParseID("73d04e6125cf3c28a299cc2f3cca3b78ceac396e4fcf9575e34536b26782413c"),
		vaultic.DataBlob, 0, 38, 0,
	}, {
		vaultic.TestParseID("9ccb846e60d90d4eb915848add7aa7ea1e4bbabfc60e573db9f7bfb2789afbae"),
		vaultic.TestParseID("73d04e6125cf3c28a299cc2f3cca3b78ceac396e4fcf9575e34536b26782413c"),
		vaultic.TreeBlob, 38, 112, 511,
	}, {
		vaultic.TestParseID("d3dc577b4ffd38cc4b32122cabf8655a0223ed22edfd93b353dc0c3f2b0fdf66"),
		vaultic.TestParseID("73d04e6125cf3c28a299cc2f3cca3b78ceac396e4fcf9575e34536b26782413c"),
		vaultic.DataBlob, 150, 123, 234,
	},
}

var exampleLookupTest = struct {
	packID vaultic.ID
	blobs  map[vaultic.ID]vaultic.BlobType
}{
	vaultic.TestParseID("73d04e6125cf3c28a299cc2f3cca3b78ceac396e4fcf9575e34536b26782413c"),
	map[vaultic.ID]vaultic.BlobType{
		vaultic.TestParseID("3ec79977ef0cf5de7b08cd12b874cd0f62bbaf7f07f3497a5b1bbcc8cb39b1ce"): vaultic.DataBlob,
		vaultic.TestParseID("9ccb846e60d90d4eb915848add7aa7ea1e4bbabfc60e573db9f7bfb2789afbae"): vaultic.TreeBlob,
		vaultic.TestParseID("d3dc577b4ffd38cc4b32122cabf8655a0223ed22edfd93b353dc0c3f2b0fdf66"): vaultic.DataBlob,
	},
}

func TestIndexUnserialize(t *testing.T) {
	for _, task := range []struct {
		idxBytes []byte
		version  int
	}{
		{docExampleV1, 1},
		{docExampleV2, 2},
	} {
		idx, err := index.DecodeIndex(task.idxBytes, vaultic.NewRandomID())
		rtest.OK(t, err)

		for _, test := range exampleTests {
			list := idx.Lookup(vaultic.BlobHandle{ID: test.id, Type: test.tpe}, nil)
			if len(list) != 1 {
				t.Errorf("expected one result for blob %v, got %v: %v", test.id.Str(), len(list), list)
			}
			blob := list[0]

			t.Logf("looking for blob %v/%v, got %v", test.tpe, test.id.Str(), blob)

			rtest.Equals(t, test.packID, blob.PackID())
			rtest.Equals(t, test.tpe, blob.Blob.Type)
			rtest.Equals(t, test.offset, blob.Blob.Offset)
			rtest.Equals(t, test.length, blob.Blob.Length)
			switch task.version {
			case 1:
				rtest.Equals(t, uint(0), blob.Blob.UncompressedLength)
			case 2:
				rtest.Equals(t, test.uncompressedLength, blob.Blob.UncompressedLength)
			default:
				t.Fatal("Invalid index version")
			}
		}

		blobs := listPack(t, idx, exampleLookupTest.packID)
		if len(blobs) != len(exampleLookupTest.blobs) {
			t.Fatalf("expected %d blobs in pack, got %d", len(exampleLookupTest.blobs), len(blobs))
		}

		for _, blob := range blobs {
			b, ok := exampleLookupTest.blobs[blob.Handle().ID]
			if !ok {
				t.Errorf("unexpected blob %v found", blob.Handle().ID.String())
			}
			if blob.Blob.Type != b {
				t.Errorf("unexpected type for blob %v: want %v, got %v", blob.Handle().ID.String(), b, blob.Blob.Type)
			}
		}
	}
}

func listPack(t testing.TB, idx *index.Index, id vaultic.ID) (pbs []*pack.PackedBlob) {
	for pb := range idx.Values() {
		if pb.PackID().Equal(id) {
			pbs = append(pbs, pb)
		}
	}
	return pbs
}

var (
	benchmarkIndexJSON     []byte
	benchmarkIndexJSONOnce sync.Once
)

func initBenchmarkIndexJSON() {
	idx, _ := createRandomIndex(rand.New(rand.NewSource(0)), 200000)
	var buf bytes.Buffer
	err := idx.Encode(&buf)
	if err != nil {
		panic(err)
	}

	benchmarkIndexJSON = buf.Bytes()
}

func BenchmarkDecodeIndex(b *testing.B) {
	benchmarkIndexJSONOnce.Do(initBenchmarkIndexJSON)

	id := vaultic.NewRandomID()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := index.DecodeIndex(benchmarkIndexJSON, id)
		rtest.OK(b, err)
	}
}

func BenchmarkDecodeIndexParallel(b *testing.B) {
	benchmarkIndexJSONOnce.Do(initBenchmarkIndexJSON)
	id := vaultic.NewRandomID()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := index.DecodeIndex(benchmarkIndexJSON, id)
			rtest.OK(b, err)
		}
	})
}

func BenchmarkEncodeIndex(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		idx, _ := createRandomIndex(rand.New(rand.NewSource(0)), n)

		b.Run(fmt.Sprint(n), func(b *testing.B) {
			buf := new(bytes.Buffer)
			err := idx.Encode(buf)
			rtest.OK(b, err)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				buf.Reset()
				_ = idx.Encode(buf)
			}
		})
	}
}

func TestIndexPacks(t *testing.T) {
	idx := index.NewIndex()
	packs := vaultic.NewIDSet()

	for range 20 {
		packID := vaultic.NewRandomID()
		idx.StorePack(packID, pack.Blobs{
			{
				BlobHandle: vaultic.NewRandomBlobHandle(),
				Offset:     0,
				Length:     23,
			},
		})

		packs.Insert(packID)
	}

	idxPacks := idx.Packs()
	rtest.Assert(t, packs.Equals(idxPacks), "packs in index do not match packs added to index")
}

const maxPackSize = 16 * 1024 * 1024

// This function generates a (insecure) random ID, similar to NewRandomID
func NewRandomTestID(rng *rand.Rand) vaultic.ID {
	id := vaultic.ID{}
	rng.Read(id[:])
	return id
}

func createRandomIndex(rng *rand.Rand, packfiles int) (idx *index.Index, lookupBh vaultic.BlobHandle) {
	idx = index.NewIndex()
	// the expectation is slightly above 8 blobs per pack, so preallocate 9 to be safe
	idx.Preallocate(vaultic.DataBlob, packfiles*9)

	// create index with given number of pack files
	for i := range packfiles {
		packID := NewRandomTestID(rng)
		var blobs pack.Blobs
		offset := 0
		for offset < maxPackSize {
			size := 2000 + rng.Intn(4*1024*1024)
			id := NewRandomTestID(rng)
			blobs = append(blobs, pack.Blob{
				BlobHandle: vaultic.BlobHandle{
					Type: vaultic.DataBlob,
					ID:   id,
				},
				Length:             uint(size),
				UncompressedLength: uint(2 * size),
				Offset:             uint(offset),
			})

			offset += size
		}
		idx.StorePack(packID, blobs)

		if i == 0 {
			lookupBh = vaultic.BlobHandle{
				Type: vaultic.DataBlob,
				ID:   blobs[rng.Intn(len(blobs))].ID,
			}
		}
	}

	return idx, lookupBh
}

func BenchmarkIndexHasUnknown(b *testing.B) {
	idx, _ := createRandomIndex(rand.New(rand.NewSource(0)), 200000)
	handles := make([]vaultic.BlobHandle, 0, 100000)
	for i := 0; i < cap(handles); i++ {
		handles = append(handles, vaultic.NewRandomBlobHandle())
	}

	for b.Loop() {
		// use multiple handles to reduce cache effects
		for _, handle := range handles {
			idx.Has(handle)
		}
	}
}

func BenchmarkIndexHasKnown(b *testing.B) {
	idx, _ := createRandomIndex(rand.New(rand.NewSource(0)), 200000)
	handles := make([]vaultic.BlobHandle, 0, 100000)
	for handle := range idx.Values() {
		handles = append(handles, handle.Handle())
		if len(handles) == cap(handles) {
			break
		}
	}

	for b.Loop() {
		// use multiple handles to reduce cache effects
		for _, handle := range handles {
			idx.Has(handle)
		}
	}
}

func BenchmarkIndexAlloc(b *testing.B) {
	rng := rand.New(rand.NewSource(0))
	b.ReportAllocs()

	for b.Loop() {
		createRandomIndex(rng, 200000)
	}
}

func BenchmarkIndexAllocParallel(b *testing.B) {
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(0))
		for pb.Next() {
			createRandomIndex(rng, 200000)
		}
	})
}

func TestIndexHas(t *testing.T) {
	tests := []*pack.PackedBlob{}

	idx := index.NewIndex()

	// create 50 packs with 20 blobs each
	for i := range 50 {
		packID := vaultic.NewRandomID()
		var blobs pack.Blobs

		pos := uint(0)
		for j := range 20 {
			length := uint(i*100 + j)
			uncompressedLength := uint(0)
			if i >= 25 {
				// test a mix of compressed and uncompressed packs
				uncompressedLength = 2 * length
			}
			pb := &pack.PackedBlob{
				Pack: packID,
				Blob: pack.Blob{
					BlobHandle:         vaultic.NewRandomBlobHandle(),
					Offset:             pos,
					Length:             length,
					UncompressedLength: uncompressedLength,
				},
			}
			blobs = append(blobs, pb.Blob)
			tests = append(tests, pb)
			pos += length
		}
		idx.StorePack(packID, blobs)
	}

	for _, testBlob := range tests {
		rtest.Assert(t, idx.Has(testBlob.Handle()), "Index reports not having data blob added to it")
	}

	rtest.Assert(t, !idx.Has(vaultic.NewRandomBlobHandle()), "Index reports having a data blob not added to it")
	rtest.Assert(t, !idx.Has(vaultic.BlobHandle{ID: tests[0].Handle().ID, Type: vaultic.TreeBlob}), "Index reports having a tree blob added to it with the same id as a data blob")
}

func TestMixedEachByPack(t *testing.T) {
	idx := index.NewIndex()

	expected := make(map[vaultic.ID]int)
	// create 50 packs with 2 blobs each
	for range 50 {
		packID := vaultic.NewRandomID()
		expected[packID] = 1
		blobs := pack.Blobs{
			{
				BlobHandle: vaultic.BlobHandle{Type: vaultic.DataBlob, ID: vaultic.NewRandomID()},
				Offset:     0,
				Length:     42,
			},
			{
				BlobHandle: vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: vaultic.NewRandomID()},
				Offset:     42,
				Length:     43,
			},
		}
		idx.StorePack(packID, blobs)
	}

	reported := make(map[vaultic.ID]int)
	for bp := range idx.EachByPack(context.TODO(), vaultic.NewIDSet()) {
		reported[bp.PackID]++

		rtest.Equals(t, 2, len(bp.Blobs)) // correct blob count
		bp.Blobs.Sort()
		b0 := bp.Blobs[0]
		rtest.Assert(t, b0.Type == vaultic.DataBlob && b0.Offset == 0 && b0.Length == 42, "wrong blob", b0)
		b1 := bp.Blobs[1]
		rtest.Assert(t, b1.Type == vaultic.TreeBlob && b1.Offset == 42 && b1.Length == 43, "wrong blob", b1)
	}
	rtest.Equals(t, expected, reported)
}

func TestEachByPackIgnoes(t *testing.T) {
	idx := index.NewIndex()

	ignores := vaultic.NewIDSet()
	expected := make(map[vaultic.ID]int)
	// create 50 packs with one blob each
	for i := range 50 {
		packID := vaultic.NewRandomID()
		if i < 3 {
			ignores.Insert(packID)
		} else {
			expected[packID] = 1
		}
		blobs := pack.Blobs{
			{
				BlobHandle: vaultic.BlobHandle{Type: vaultic.DataBlob, ID: vaultic.NewRandomID()},
				Offset:     0,
				Length:     42,
			},
		}
		idx.StorePack(packID, blobs)
	}
	idx.Finalize()

	reported := make(map[vaultic.ID]int)
	for bp := range idx.EachByPack(context.TODO(), ignores) {
		reported[bp.PackID]++
		rtest.Equals(t, 1, len(bp.Blobs)) // correct blob count
		b0 := bp.Blobs[0]
		rtest.Assert(t, b0.Type == vaultic.DataBlob && b0.Offset == 0 && b0.Length == 42, "wrong blob", b0)
	}
	rtest.Equals(t, expected, reported)
}
