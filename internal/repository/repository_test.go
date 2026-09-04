package repository_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/cache"
	"github.com/otuschhoff/vaultic/internal/backend/local"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	"github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/staging"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

var testSizes = []int{5, 23, 2<<18 + 23, 1 << 20}

func TestSave(t *testing.T) {
	repository.TestAllVersions(t, testSavePassID)
	repository.TestAllVersions(t, testSaveCalculateID)
}

func TestDeferredUploadDoesNotMutateNormalIndex(t *testing.T) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, 2)
	first, second := mem.New(), mem.New()
	data := []byte("deferred data")
	blobID := vaultic.Hash(data)
	result, err := repo.WithDeferredBlobUploader(context.Background(), repository.DeferredUploadOptions{
		Backends: []repository.DeferredBackend{
			{ID: "a", FailureDomain: "site-a", Backend: first},
			{ID: "b", FailureDomain: "site-b", Offsite: true, Backend: second},
		},
		Policy: staging.Policy{MinCopies: 2, MinDomains: 2, MinOffsite: 1},
	}, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		id, known, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, data, blobID, false)
		if err != nil {
			return err
		}
		if known || id != blobID {
			t.Fatalf("deferred blob result = %s, known=%v", id, known)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Packs) != 1 || len(result.Packs[0].Placements) != 2 || len(result.Records) != 1 {
		t.Fatalf("deferred result = %#v", result)
	}
	if entries := repo.LookupBlob(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: blobID}); len(entries) != 0 {
		t.Fatalf("deferred blob entered normal index: %#v", entries)
	}
	packID := result.Packs[0].ID
	for _, destination := range []backend.Backend{first, second} {
		if _, err := destination.Stat(context.Background(), backend.Handle{Type: backend.PackFile, Name: packID}); err != nil {
			t.Fatalf("staged pack missing: %v", err)
		}
	}
}

func TestDeferredUploadRefusesPackOverByteQuota(t *testing.T) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, 2)
	destination := mem.New()
	data := []byte("deferred data")
	_, err := repo.WithDeferredBlobUploader(context.Background(), repository.DeferredUploadOptions{
		Backends: []repository.DeferredBackend{{ID: "a", FailureDomain: "site-a", Backend: destination}},
		Policy:   staging.Policy{MinCopies: 1, MinDomains: 1}, MaxAdditionalBytes: 1,
	}, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		_, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, data, vaultic.Hash(data), false)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "byte quota exceeded") {
		t.Fatalf("quota error = %v", err)
	}
	err = destination.List(context.Background(), backend.PackFile, func(backend.FileInfo) error {
		t.Fatal("pack uploaded after quota refusal")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeferredJournalIndexReadsStagedBlob(t *testing.T) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, 2)
	data := []byte("emergency restore data")
	blobID := vaultic.Hash(data)
	result, err := repo.WithDeferredBlobUploader(context.Background(), repository.DeferredUploadOptions{
		Backends: []repository.DeferredBackend{{ID: "primary", FailureDomain: "site-a", Backend: repo.Backend()}},
		Policy:   staging.Policy{MinCopies: 1, MinDomains: 1},
	}, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		_, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, data, blobID, false)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.UseDeferredJournalIndex([]staging.Segment{{Packs: result.Packs, Records: result.Records}}); err != nil {
		t.Fatal(err)
	}
	loaded, err := repo.LoadBlob(context.Background(), vaultic.BlobHandle{Type: vaultic.DataBlob, ID: blobID}, nil)
	if err != nil || !bytes.Equal(loaded, data) {
		t.Fatalf("loaded staged blob = %q, %v", loaded, err)
	}
}

func testSavePassID(t *testing.T, version uint) {
	testSave(t, version, false)
}

func testSaveCalculateID(t *testing.T, version uint) {
	testSave(t, version, true)
}

func testSave(t *testing.T, version uint, calculateID bool) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, version)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	for _, size := range testSizes {
		data := make([]byte, size)
		_, err := io.ReadFull(rnd, data)
		rtest.OK(t, err)

		id := vaultic.Hash(data)

		rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
			// save
			inputID := vaultic.ID{}
			if !calculateID {
				inputID = id
			}
			sid, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, data, inputID, false)
			rtest.OK(t, err)
			rtest.Equals(t, id, sid)
			return nil
		}))

		// read back
		buf, err := repo.LoadBlob(context.TODO(), vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}, nil)
		rtest.OK(t, err)
		rtest.Equals(t, size, len(buf))

		rtest.Assert(t, len(buf) == len(data),
			"number of bytes read back does not match: expected %d, got %d",
			len(data), len(buf))

		rtest.Assert(t, bytes.Equal(buf, data),
			"data does not match: expected %02x, got %02x",
			data, buf)
	}
}

func TestSaveLoadZeroSizedBlob(t *testing.T) {
	repository.TestAllVersions(t, testSaveLoadZeroSizedBlob)
}

func testSaveLoadZeroSizedBlob(t *testing.T, version uint) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, version)

	var data []byte
	id := vaultic.Hash(data)

	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		sid, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, data, id, false)
		rtest.OK(t, err)
		rtest.Equals(t, id, sid)
		return nil
	}))

	buf, err := repo.LoadBlob(context.TODO(), vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}, nil)
	rtest.OK(t, err)
	rtest.Equals(t, 0, len(buf))
}

func TestSavePackMerging(t *testing.T) {
	t.Run("75%", func(t *testing.T) {
		testSavePackMerging(t, 75, 1)
	})
	t.Run("150%", func(t *testing.T) {
		testSavePackMerging(t, 175, 2)
	})
	t.Run("250%", func(t *testing.T) {
		testSavePackMerging(t, 275, 3)
	})
}

func testSavePackMerging(t *testing.T, targetPercentage int, expectedPacks int) {
	repo, _ := repository.TestRepositoryWithBackend(t, nil, 0, repository.Options{
		// minimum pack size to speed up test
		PackSize: repository.MinPackSize,
	})
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	var ids vaultic.IDs
	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		// add blobs with size targetPercentage / 100 * repo.PackSize to the repository
		blobSize := repository.MinPackSize / 100
		for range targetPercentage {
			data := make([]byte, blobSize)
			_, err := io.ReadFull(rnd, data)
			rtest.OK(t, err)

			sid, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, data, vaultic.ID{}, false)
			rtest.OK(t, err)
			ids = append(ids, sid)
		}
		return nil
	}))

	// check that all blobs are readable
	for _, id := range ids {
		_, err := repo.LoadBlob(context.TODO(), vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}, nil)
		rtest.OK(t, err)
	}

	// check for correct number of pack files
	packs := 0
	rtest.OK(t, repo.List(context.TODO(), vaultic.PackFile, func(id vaultic.ID, _ int64) error {
		packs++
		return nil
	}))
	rtest.Equals(t, expectedPacks, packs, "unexpected number of pack files")

	repository.TestCheckRepo(t, repo)
}

func BenchmarkSaveAndEncrypt(t *testing.B) {
	repository.BenchmarkAllVersions(t, benchmarkSaveAndEncrypt)
}

func benchmarkSaveAndEncrypt(t *testing.B, version uint) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, version)
	size := 4 << 20 // 4MiB

	data := make([]byte, size)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	_, err := io.ReadFull(rnd, data)
	rtest.OK(t, err)

	id := vaultic.ID(sha256.Sum256(data))

	t.ReportAllocs()
	t.ResetTimer()
	t.SetBytes(int64(size))

	_ = repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		for i := 0; i < t.N; i++ {
			_, _, _, err = uploader.SaveBlob(ctx, vaultic.DataBlob, data, id, true)
			rtest.OK(t, err)
		}
		return nil
	})
}

func TestLoadBlob(t *testing.T) {
	repository.TestAllVersions(t, testLoadBlob)
}

func testLoadBlob(t *testing.T, version uint) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, version)
	length := 1000000
	buf := crypto.NewBlobBuffer(length)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	_, err := io.ReadFull(rnd, buf)
	rtest.OK(t, err)

	var id vaultic.ID
	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var err error
		id, _, _, err = uploader.SaveBlob(ctx, vaultic.DataBlob, buf, vaultic.ID{}, false)
		return err
	}))

	base := crypto.CiphertextLength(length)
	for _, testlength := range []int{0, base - 20, base - 1, base, base + 7, base + 15, base + 1000} {
		buf = make([]byte, 0, testlength)
		buf, err := repo.LoadBlob(context.TODO(), vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}, buf)
		if err != nil {
			t.Errorf("LoadBlob() returned an error for buffer size %v: %v", testlength, err)
			continue
		}

		if len(buf) != length {
			t.Errorf("LoadBlob() returned the wrong number of bytes: want %v, got %v", length, len(buf))
			continue
		}
	}
}

func TestLoadBlobBroken(t *testing.T) {
	be := mem.New()
	repo, _ := repository.TestRepositoryWithBackend(t, &damageOnceBackend{Backend: be}, vaultic.StableRepoVersion, repository.Options{})
	buf := rtest.Random(42, 1000)

	var id vaultic.ID
	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var err error
		id, _, _, err = uploader.SaveBlob(ctx, vaultic.TreeBlob, buf, vaultic.ID{}, false)
		return err
	}))

	// setup cache after saving the blob to make sure that the damageOnceBackend damages the cached data
	c := cache.TestNewCache(t)
	repo.UseCache(c, t.Logf)

	data, err := repo.LoadBlob(context.TODO(), vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: id}, nil)
	rtest.OK(t, err)
	rtest.Assert(t, bytes.Equal(buf, data), "data mismatch")
	pack := repo.LookupBlob(vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: id})[0].PackID()
	rtest.Assert(t, c.Has(backend.Handle{Type: backend.PackFile, Name: pack.String()}), "expected tree pack to be cached")
}

func BenchmarkLoadBlob(b *testing.B) {
	repository.BenchmarkAllVersions(b, benchmarkLoadBlob)
}

func benchmarkLoadBlob(b *testing.B, version uint) {
	repo, _, _ := repository.TestRepositoryWithVersion(b, version)
	length := 1000000
	buf := crypto.NewBlobBuffer(length)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	_, err := io.ReadFull(rnd, buf)
	rtest.OK(b, err)

	var id vaultic.ID
	rtest.OK(b, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var err error
		id, _, _, err = uploader.SaveBlob(ctx, vaultic.DataBlob, buf, vaultic.ID{}, false)
		return err
	}))

	b.ResetTimer()
	b.SetBytes(int64(length))

	for i := 0; i < b.N; i++ {
		var err error
		buf, err = repo.LoadBlob(context.TODO(), vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}, buf)

		// Checking the SHA-256 with vaultic.Hash can make up 38% of the time
		// spent in this loop, so pause the timer.
		b.StopTimer()
		rtest.OK(b, err)
		if len(buf) != length {
			b.Errorf("wanted %d bytes, got %d", length, len(buf))
		}

		id2 := vaultic.Hash(buf)
		if !id.Equal(id2) {
			b.Errorf("wrong data returned, wanted %v, got %v", id.Str(), id2.Str())
		}
		b.StartTimer()
	}
}

func BenchmarkLoadUnpacked(b *testing.B) {
	repository.BenchmarkAllVersions(b, benchmarkLoadUnpacked)
}

func benchmarkLoadUnpacked(b *testing.B, version uint) {
	repo, _, _ := repository.TestRepositoryWithVersion(b, version)
	length := 1000000
	buf := crypto.NewBlobBuffer(length)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	_, err := io.ReadFull(rnd, buf)
	rtest.OK(b, err)

	dataID := vaultic.Hash(buf)

	storageID, err := repo.SaveUnpacked(context.TODO(), vaultic.WriteableSnapshotFile, buf)
	rtest.OK(b, err)
	// rtest.OK(b, repo.Flush())

	b.ResetTimer()
	b.SetBytes(int64(length))

	for i := 0; i < b.N; i++ {
		data, err := repo.LoadUnpacked(context.TODO(), vaultic.SnapshotFile, storageID)
		rtest.OK(b, err)

		// See comment in BenchmarkLoadBlob.
		b.StopTimer()
		if len(data) != length {
			b.Errorf("wanted %d bytes, got %d", length, len(data))
		}

		id2 := vaultic.Hash(data)
		if !dataID.Equal(id2) {
			b.Errorf("wrong data returned, wanted %v, got %v", storageID.Str(), id2.Str())
		}
		b.StartTimer()
	}
}

var repoFixture = filepath.Join("testdata", "test-repo.tar.gz")

func TestRepositoryLoadIndex(t *testing.T) {
	repository.TestInjectKey(
		t,
		vaultic.TestParseID("7bb3065bfb17da7430dc4dde4741d6db3dd83fdb0829500cf105755e067f879a"),
		`{"mac":{"k":"W1Y8bmQNJg6TAmuDt7lbpQ==","r":"r43DBmAdmwtQneoBTGAABQ=="},"encrypt":"JuZGBs6joRiLzqkyMWhmbZMLHe8+5oH6MDE5I6M8R/I="}`,
	)
	repo, _ := repository.TestFromFixture(t, repoFixture)

	rtest.OK(t, repo.LoadIndex(context.TODO(), vaultic.NoopTerminalCounterFactory))
}

// loadIndex loads the index id from backend and returns it.
func loadIndex(ctx context.Context, repo vaultic.LoaderUnpacked, id vaultic.ID) (*index.Index, error) {
	buf, err := repo.LoadUnpacked(ctx, vaultic.IndexFile, id)
	if err != nil {
		return nil, err
	}

	return index.DecodeIndex(buf, id)
}

func TestRepositoryLoadUnpackedBroken(t *testing.T) {
	repo, _, be := repository.TestRepositoryWithVersion(t, 0)

	data := rtest.Random(23, 12345)
	id := vaultic.Hash(data)
	h := backend.Handle{Type: backend.IndexFile, Name: id.String()}
	// damage buffer
	data[0] ^= 0xff

	// store broken file
	err := be.Save(context.TODO(), h, backend.NewByteReader(data, be.Hasher()))
	rtest.OK(t, err)

	_, err = repo.LoadUnpacked(context.TODO(), vaultic.IndexFile, id)
	rtest.Assert(t, errors.Is(err, vaultic.ErrInvalidData), "unexpected error: %v", err)
}

type damageOnceBackend struct {
	backend.Backend
	m sync.Map
}

func (be *damageOnceBackend) Load(ctx context.Context, h backend.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	// don't break the config file as we can't retry it
	if h.Type == backend.ConfigFile {
		return be.Backend.Load(ctx, h, length, offset, fn)
	}

	h.IsMetadata = false
	_, isRetry := be.m.LoadOrStore(h, true)
	if !isRetry {
		// return broken data on the first try
		offset++
	}
	return be.Backend.Load(ctx, h, length, offset, fn)
}

func TestRepositoryLoadUnpackedRetryBroken(t *testing.T) {
	repository.TestInjectKey(
		t,
		vaultic.TestParseID("7bb3065bfb17da7430dc4dde4741d6db3dd83fdb0829500cf105755e067f879a"),
		`{"mac":{"k":"W1Y8bmQNJg6TAmuDt7lbpQ==","r":"r43DBmAdmwtQneoBTGAABQ=="},"encrypt":"JuZGBs6joRiLzqkyMWhmbZMLHe8+5oH6MDE5I6M8R/I="}`,
	)
	repodir := rtest.Env(t, repoFixture)

	be, err := local.Open(context.TODO(), local.Config{Path: repodir, Connections: 2}, t.Logf)
	rtest.OK(t, err)
	repo := repository.TestOpenBackend(t, &damageOnceBackend{Backend: be})

	rtest.OK(t, repo.LoadIndex(context.TODO(), vaultic.NoopTerminalCounterFactory))
}

// saveRandomDataBlobs generates random data blobs and saves them to the repository.
func saveRandomDataBlobs(t testing.TB, repo vaultic.Repository, num int, sizeMax int) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		for range num {
			size := rnd.Int() % sizeMax

			buf := make([]byte, size)
			_, err := io.ReadFull(rnd, buf)
			rtest.OK(t, err)

			_, _, _, err = uploader.SaveBlob(ctx, vaultic.DataBlob, buf, vaultic.ID{}, false)
			rtest.OK(t, err)
		}
		return nil
	}))
}

func TestRepositoryIncrementalIndex(t *testing.T) {
	repository.TestAllVersions(t, testRepositoryIncrementalIndex)
}

func testRepositoryIncrementalIndex(t *testing.T, version uint) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, version)

	// add a few rounds of packs
	for range 5 {
		// add some packs and write index
		saveRandomDataBlobs(t, repo, 20, 1<<15)
	}

	packEntries := make(map[vaultic.ID]map[vaultic.ID]struct{})

	err := repo.List(context.TODO(), vaultic.IndexFile, func(id vaultic.ID, size int64) error {
		idx, err := loadIndex(context.TODO(), repo, id)
		rtest.OK(t, err)

		for pb := range idx.Values() {
			packID := pb.PackID()
			if _, ok := packEntries[packID]; !ok {
				packEntries[packID] = make(map[vaultic.ID]struct{})
			}

			packEntries[packID][id] = struct{}{}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for packID, ids := range packEntries {
		if len(ids) > 1 {
			t.Errorf("pack %v listed in %d indexes\n", packID, len(ids))
		}
	}
}

func TestInvalidCompression(t *testing.T) {
	var comp repository.CompressionMode
	err := comp.Set("nope")
	rtest.Assert(t, err != nil, "missing error")
	_, err = repository.New(nil, repository.Options{Compression: comp})
	rtest.Assert(t, err != nil, "missing error")
}

func TestListPack(t *testing.T) {
	be := mem.New()
	repo, _ := repository.TestRepositoryWithBackend(t, &damageOnceBackend{Backend: be}, vaultic.StableRepoVersion, repository.Options{})
	buf := rtest.Random(42, 1000)

	var id vaultic.ID
	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var err error
		id, _, _, err = uploader.SaveBlob(ctx, vaultic.TreeBlob, buf, vaultic.ID{}, false)
		return err
	}))

	// setup cache after saving the blob to make sure that the damageOnceBackend damages the cached data
	c := cache.TestNewCache(t)
	repo.UseCache(c, t.Logf)

	// Forcibly cache pack file
	packID := repo.LookupBlob(vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: id})[0].PackID()
	rtest.OK(
		t,
		be.Load(context.TODO(), backend.Handle{Type: backend.PackFile, IsMetadata: true, Name: packID.String()}, 0, 0, func(rd io.Reader) error { return nil }),
	)

	// Get size to list pack
	var size int64
	rtest.OK(t, repo.List(context.TODO(), vaultic.PackFile, func(id vaultic.ID, sz int64) error {
		if id == packID {
			size = sz
		}
		return nil
	}))

	handles, err := repo.ListPackHandles(context.TODO(), packID, size)
	rtest.OK(t, err)
	rtest.Assert(t, len(handles) == 1 && handles[0].ID == id, "unexpected blobs in pack: %v", handles)

	rtest.Assert(
		t,
		!c.Has(backend.Handle{Type: backend.PackFile, Name: packID.String()}),
		"tree pack should no longer be cached as listPack does not set IsMetadata in the backend.Handle",
	)
}

func TestNoDoubleInit(t *testing.T) {
	r, _, be := repository.TestRepositoryWithVersion(t, vaultic.StableRepoVersion)

	repo, err := repository.New(be, repository.Options{})
	rtest.OK(t, err)

	pol := r.Config().ChunkerPolynomial
	err = repo.Init(context.TODO(), r.Config().Version, rtest.TestPassword, &pol)
	rtest.Assert(t, strings.Contains(err.Error(), "repository master key and config already initialized"), "expected config exist error, got %q", err)

	// must also prevent init if only keys exist
	rtest.OK(t, be.Remove(context.TODO(), backend.Handle{Type: backend.ConfigFile}))
	err = repo.Init(context.TODO(), r.Config().Version, rtest.TestPassword, &pol)
	rtest.Assert(t, strings.Contains(err.Error(), "repository already contains keys"), "expected already contains keys error, got %q", err)

	// must also prevent init if a snapshot exists and keys were deleted
	var data [32]byte
	hash := vaultic.Hash(data[:])
	rtest.OK(t, be.Save(context.TODO(), backend.Handle{Type: backend.SnapshotFile, Name: hash.String()}, backend.NewByteReader(data[:], be.Hasher())))
	rtest.OK(t, be.List(context.TODO(), backend.KeyFile, func(fi backend.FileInfo) error {
		return be.Remove(context.TODO(), backend.Handle{Type: backend.KeyFile, Name: fi.Name})
	}))
	err = repo.Init(context.TODO(), r.Config().Version, rtest.TestPassword, &pol)
	rtest.Assert(t, strings.Contains(err.Error(), "repository already contains snapshots"), "expected already contains snapshots error, got %q", err)
}

func TestSaveBlobAsync(t *testing.T) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, 2)
	ctx := context.Background()

	type result struct {
		id    vaultic.ID
		known bool
		size  int
		err   error
	}
	numCalls := 10
	results := make([]result, numCalls)
	var resultsMutex sync.Mutex

	err := repo.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		var wg sync.WaitGroup
		wg.Add(numCalls)
		for i := range numCalls {
			// Use unique data for each call
			testData := fmt.Appendf(nil, "test blob data %d", i)
			uploader.SaveBlobAsync(ctx, vaultic.DataBlob, testData, vaultic.ID{}, false,
				func(newID vaultic.ID, known bool, size int, err error) {
					defer wg.Done()
					resultsMutex.Lock()
					results[i] = result{newID, known, size, err}
					resultsMutex.Unlock()
				})
		}
		wg.Wait()
		return nil
	})
	rtest.OK(t, err)

	for i, result := range results {
		testData := fmt.Appendf(nil, "test blob data %d", i)
		expectedID := vaultic.Hash(testData)
		rtest.Assert(t, result.err == nil, "result %d: unexpected error %v", i, result.err)
		rtest.Assert(t, result.id.Equal(expectedID), "result %d: expected ID %v, got %v", i, expectedID, result.id)
		rtest.Assert(t, !result.known, "result %d: expected unknown blob", i)
	}
}

func TestSaveBlobAsyncErrorHandling(t *testing.T) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, 2)
	ctx, cancel := context.WithCancel(context.Background())

	var callbackCalled atomic.Bool

	err := repo.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		cancel()
		// Callback must be called even if the context is canceled
		uploader.SaveBlobAsync(ctx, vaultic.DataBlob, []byte("test blob data"), vaultic.ID{}, false,
			func(newID vaultic.ID, known bool, size int, err error) {
				callbackCalled.Store(true)
			})
		return nil
	})

	rtest.Assert(t, errors.Is(err, context.Canceled), "expected context canceled error, got %v", err)
	rtest.Assert(t, callbackCalled.Load(), "callback was not called")
}
