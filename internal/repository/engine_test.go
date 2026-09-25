package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/feature"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type catalogLoadCounter struct {
	vaultic.Counter
	count  atomic.Uint64
	cancel context.CancelFunc
}

type failingAdmissionEngine struct {
	*enginepkg.LegacyEngine
	err          error
	legacyCalled bool
}

type batchLookupEngine struct {
	*enginepkg.LegacyEngine
	results []vaultic.BlobSize
	err     error
	calls   int
}

func (engine *batchLookupEngine) LookupSizesContext(context.Context, []vaultic.BlobHandle) ([]vaultic.BlobSize, error) {
	engine.calls++
	return engine.results, engine.err
}

func TestRepositoryBatchLookup(t *testing.T) {
	repo := TestRepository(t)
	engine := &batchLookupEngine{LegacyEngine: enginepkg.NewLegacyEngine(), results: []vaultic.BlobSize{{Size: 123, Found: true}, {}}}
	repo.SetEngine(engine)
	reader := repo.AppendTransaction().(vaultic.ContextBlobSizeBatchLookup)
	handles := []vaultic.BlobHandle{vaultic.NewRandomBlobHandle(), vaultic.NewRandomBlobHandle()}
	results, err := reader.LookupBlobSizesContext(t.Context(), handles)
	if err != nil || len(results) != 2 || results[0] != engine.results[0] || results[1].Found || engine.calls != 1 {
		t.Fatalf("batch=%v err=%v calls=%d", results, err, engine.calls)
	}
	engine.results = engine.results[:1]
	if results, err := reader.LookupBlobSizesContext(t.Context(), handles); results != nil || !errors.Is(err, vaultic.ErrMetadataLookup) {
		t.Fatalf("short reply accepted: %v, %v", results, err)
	}
	engine.err = errors.New("batch unavailable")
	if results, err := reader.LookupBlobSizesContext(t.Context(), handles); results != nil || !errors.Is(err, engine.err) {
		t.Fatalf("provider failure lost: %v, %v", results, err)
	}
	calls := engine.calls
	for _, invalid := range [][]vaultic.BlobHandle{make([]vaultic.BlobHandle, vaultic.BlobLookupBatchSize+1), {{Type: vaultic.InvalidBlob}}} {
		if _, err := reader.LookupBlobSizesContext(t.Context(), invalid); !errors.Is(err, vaultic.ErrMetadataLookup) {
			t.Fatalf("invalid batch accepted: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := reader.LookupBlobSizesContext(ctx, handles); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
	if _, err := reader.LookupBlobSizesContext(t.Context(), nil); err != nil || engine.calls != calls {
		t.Fatalf("invalid, canceled or empty request reached provider: err=%v calls=%d want=%d", err, engine.calls, calls)
	}
	legacy := enginepkg.NewLegacyEngine()
	legacy.AddPending(handles[0], 321)
	repo.SetEngine(legacy)
	results, err = reader.LookupBlobSizesContext(t.Context(), handles)
	if err != nil || results[0] != (vaultic.BlobSize{Size: 321, Found: true}) || results[1].Found {
		t.Fatalf("legacy fallback=%v err=%v", results, err)
	}
}

func (engine *failingAdmissionEngine) AddPendingContext(context.Context, vaultic.BlobHandle, uint) (bool, error) {
	return false, engine.err
}

func (engine *failingAdmissionEngine) AddPending(vaultic.BlobHandle, uint) bool {
	engine.legacyCalled = true
	return false
}

func (engine *failingAdmissionEngine) LookupContext(context.Context, vaultic.BlobHandle) ([]*pack.PackedBlob, error) {
	return nil, engine.err
}

func (engine *failingAdmissionEngine) LookupSizeContext(context.Context, vaultic.BlobHandle) (uint, bool, error) {
	return 123, true, engine.err
}

func TestRepositoryContextLookupFailure(t *testing.T) {
	repo := TestRepository(t)
	failure := errors.New("metadata unavailable")
	repo.SetEngine(&failingAdmissionEngine{LegacyEngine: enginepkg.NewLegacyEngine(), err: failure})
	handle := vaultic.NewRandomBlobHandle()
	if _, err := repo.LoadBlob(t.Context(), handle, nil); !errors.Is(err, failure) || !errors.Is(err, vaultic.ErrMetadataLookup) {
		t.Fatalf("blob load lost metadata failure: %v", err)
	}
	reader := repo.AppendTransaction().(vaultic.ContextBlobSizeLookup)
	size, found, err := reader.LookupBlobSizeContext(t.Context(), handle)
	if size != 0 || found || !errors.Is(err, failure) || !errors.Is(err, vaultic.ErrMetadataLookup) {
		t.Fatalf("size lookup: size=%d found=%v err=%v", size, found, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := repo.LoadBlob(ctx, handle, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("blob load ignored cancellation: %v", err)
	}
}

func TestSaveBlobAdmissionFailurePreventsUpload(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprint(duplicate), func(t *testing.T) {
			repo := TestRepository(t)
			failure := errors.New("metadata lookup unavailable")
			engine := &failingAdmissionEngine{LegacyEngine: enginepkg.NewLegacyEngine(), err: failure}
			repo.SetEngine(engine)
			id, known, size, err := repo.saveBlob(t.Context(), vaultic.DataBlob, []byte("payload"), vaultic.ID{}, duplicate)
			if !errors.Is(err, failure) || known || size != 0 || !id.IsNull() || engine.legacyCalled {
				t.Fatalf("admission failure: id=%v known=%v size=%d err=%v legacy=%v", id, known, size, err, engine.legacyCalled)
			}
		})
	}
}

func (counter *catalogLoadCounter) Add(amount uint64) {
	if counter.count.Add(amount) >= 1000 && counter.cancel != nil {
		counter.cancel()
	}
}

func TestAuthoritativeCatalogLoadStreamsAndCleansUp(t *testing.T) {
	ctx := context.Background()
	client, err := daemon.Ensure(ctx, daemon.Options{
		Socket: gcTestSocket(t), RepositoryID: t.Name(), DaemonPath: testGCDaemonPath(t),
		DataDir: t.TempDir(), ObjectStore: "memory",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	if !client.Limits().ScanStream {
		t.Fatal("native daemon must support streaming")
	}
	store := daemon.NewSchemaStore(client)
	expected := make(map[vaultic.BlobHandle][]schema.BlobLocation)
	var expectedSizes [vaultic.NumBlobTypes]uint64
	for packNumber := range 4 {
		packID := schema.ID(vaultic.Hash(fmt.Appendf(nil, "pack-%d", packNumber)))
		published := daemon.PublishedPack{PackID: packID,
			Record: schema.PackRecord{Type: schema.PackMixed, BlobCount: 6001, PayloadSize: 600100, Lifecycle: schema.PackExportPending},
			Blobs:  make(map[schema.ID]schema.BlobRecord),
		}
		if packNumber >= 2 {
			blobType := vaultic.DataBlob
			published.Record.Type = schema.PackData
			if packNumber == 3 {
				blobType = vaultic.TreeBlob
				published.Record.Type = schema.PackTree
			}
			expectedSizes[blobType] = uint64(pack.CalculateHeaderSize(nil)) + 6001*uint64(100+pack.CalculateEntrySize(true))
		}
		for ordinal := range 6001 {
			blobID := vaultic.Hash(fmt.Appendf(nil, "blob-%d-%d", packNumber, ordinal))
			if ordinal == 0 {
				blobID = vaultic.Hash([]byte("shared-blob"))
			}
			kind, blobType := schema.BlobData, vaultic.DataBlob
			if (packNumber < 2 && ordinal%2 != 0) || packNumber == 3 {
				kind, blobType = schema.BlobTree, vaultic.TreeBlob
			}
			location := schema.BlobLocation{PackID: packID, Type: kind, Offset: uint64(ordinal * 100), Length: 100, UncompressedSize: 123}
			published.Blobs[schema.ID(blobID)] = schema.BlobRecord{Locations: []schema.BlobLocation{location}}
			handle := vaultic.BlobHandle{ID: blobID, Type: blobType}
			expected[handle] = append(expected[handle], location)
		}
		if err := store.PublishPack(ctx, published); err != nil {
			t.Fatal(err)
		}
	}
	repo := newEngineTestRepository(t, mem.New())
	for _, canceled := range []bool{true, false} {
		engine := enginepkg.NewDaemonEngine(client)
		loadCtx, cancel := context.WithCancel(ctx)
		counter := &catalogLoadCounter{Counter: vaultic.NoopCounter}
		if canceled {
			counter.cancel = cancel
		}
		err := engine.Load(loadCtx, repo, counter, nil)
		cancel()
		if canceled {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled catalog: %v", err)
			}
		} else if err != nil || counter.count.Load() != 24004 {
			t.Fatalf("load: count=%d err=%v", counter.count.Load(), err)
		}
		sizes, available, sizeErr := engine.BlobSizes(ctx)
		if sizeErr != nil || available == canceled || (!canceled && sizes != expectedSizes) {
			t.Fatalf("catalog sizes=%v available=%v err=%v, want %v", sizes, available, sizeErr, expectedSizes)
		}
		for handle, locations := range expected {
			actual := engine.Lookup(handle)
			if canceled {
				if len(actual) != 0 {
					t.Fatal("canceled load installed a partial projection")
				}
				continue
			}
			if len(actual) != len(locations) {
				t.Fatalf("lookup %v: got %d locations, want %d", handle, len(actual), len(locations))
			}
			for _, location := range locations {
				found := false
				for _, blob := range actual {
					found = found || blob.PackID() == vaultic.ID(location.PackID) && blob.Blob.Offset == uint(location.Offset) &&
						blob.Blob.Length == uint(location.Length) && blob.Blob.UncompressedLength == uint(location.UncompressedSize)
				}
				if !found {
					t.Fatalf("lookup %v missing location %+v", handle, location)
				}
			}
		}
		if !canceled {
			repo.SetEngine(engine)
			actual, err := repo.currentBlobSizes(ctx)
			if err != nil || actual != expectedSizes {
				t.Fatalf("fast sizing=%v, %v; want %v", actual, err, expectedSizes)
			}
			blob := pack.Blob{BlobHandle: vaultic.NewRandomBlobHandle(), Length: 100}
			if err := engine.StorePack(ctx, vaultic.NewRandomID(), pack.Blobs{blob}, &internalRepository{repo}); err != nil {
				t.Fatal(err)
			}
			if _, available, err := engine.BlobSizes(ctx); err != nil || available {
				t.Fatalf("write retained stale sizing: available=%v err=%v", available, err)
			}
			expectedSizes[blob.Type] += 100 + uint64(pack.CalculateHeaderSize(pack.Blobs{blob}))
			actual, err = repo.currentBlobSizes(ctx)
			if err != nil || actual != expectedSizes {
				t.Fatalf("fallback sizing=%v, %v; want %v", actual, err, expectedSizes)
			}
		}
		status, err := client.WriterStatus(ctx)
		if err != nil || status.ActiveTransactions != 0 || status.ActiveWriteIntents != 0 {
			t.Fatalf("catalog session leaked: status=%+v err=%v", status, err)
		}
	}
}

func newEngineTestRepository(t *testing.T, be backend.Backend) *Repository {
	t.Helper()
	repo, err := New(be, Options{})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	repo.setConfig(vaultic.Config{ID: "test-repository", Version: 2})
	return repo
}

func saveEngineManifest(t *testing.T, be backend.Backend, manifest enginepkg.Manifest) {
	t.Helper()
	payload, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	err = be.Save(
		context.Background(),
		backend.Handle{Type: backend.SlateDBFile, Name: enginepkg.ManifestName, IsMetadata: true},
		backend.NewByteReader(payload, be.Hasher()),
	)
	if err != nil {
		t.Fatalf("save manifest: %v", err)
	}
}

func TestResolveEngineFromBackendUsesLiveLegacyMasterIndex(t *testing.T) {
	repo := newEngineTestRepository(t, mem.New())
	engine, err := repo.ResolveEngineFromBackend(context.Background())
	if err != nil {
		t.Fatalf("ResolveEngineFromBackend returned error: %v", err)
	}
	legacy, ok := engine.(enginepkg.LegacyIndexEngine)
	if !ok {
		t.Fatalf("engine type %T does not implement LegacyIndexEngine", engine)
	}

	pending := vaultic.NewRandomBlobHandle()
	if !legacy.AddPending(pending, 99) {
		t.Fatal("AddPending rejected a new blob")
	}
	if size, found := repo.LookupBlobSize(pending); !found || size != 99 {
		t.Fatalf("LookupBlobSize = %d, %v, want 99, true", size, found)
	}

	handle := vaultic.NewRandomBlobHandle()
	idx := legacyindex.NewIndex()
	idx.StorePack(vaultic.NewRandomID(), pack.Blobs{{BlobHandle: handle, Length: 42}})
	repo.idx.Insert(idx)
	seen := 0
	if err := repo.ListBlobs(context.Background(), func(blob vaultic.PackBlob) {
		if blob.Handle() == handle {
			seen++
		}
	}); err != nil {
		t.Fatalf("ListBlobs returned error: %v", err)
	}
	if seen != 1 {
		t.Fatalf("ListBlobs saw matching blob %d times, want 1", seen)
	}
}

func TestResolveEngineFromBackendFailsClosedForSlateDBManifest(t *testing.T) {
	be := mem.New()
	repo := newEngineTestRepository(t, be)
	saveEngineManifest(t, be, enginepkg.Manifest{
		FormatVersion: enginepkg.ManifestFormatVersion,
		SchemaVersion: enginepkg.ManifestSchemaVersion,
		RepositoryID:  repo.Config().ID,
		Authoritative: true,
	})

	_, err := repo.ResolveEngineFromBackend(context.Background())
	if !errors.Is(err, enginepkg.ErrUnavailable) {
		t.Fatalf("ResolveEngineFromBackend error = %v, want ErrUnavailable", err)
	}
	if repo.Engine().Mode() != enginepkg.ModeLegacy {
		t.Fatalf("failed resolution changed engine mode to %q", repo.Engine().Mode())
	}
}

func TestResolveEngineFromBackendRequiresSlateDBAuthorityGate(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, false)()
	be := mem.New()
	repo := newEngineTestRepository(t, be)
	saveEngineManifest(t, be, enginepkg.Manifest{
		FormatVersion: enginepkg.ManifestFormatVersion,
		SchemaVersion: enginepkg.ManifestSchemaVersion,
		RepositoryID:  repo.Config().ID,
		Authoritative: true,
	})

	_, err := repo.ResolveEngineFromBackend(context.Background())
	if !errors.Is(err, enginepkg.ErrUnavailable) {
		t.Fatalf("ResolveEngineFromBackend error = %v, want ErrUnavailable", err)
	}
}

func TestResolveEngineFromBackendFailsClosedWhenAuthoritativeDaemonUnavailable(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.SlateDBAuthoritative, true)()
	be := mem.New()
	repo := newEngineTestRepository(t, be)
	saveEngineManifest(t, be, enginepkg.Manifest{
		FormatVersion: enginepkg.ManifestFormatVersion,
		SchemaVersion: enginepkg.ManifestSchemaVersion,
		RepositoryID:  repo.Config().ID,
		Authoritative: true,
	})

	_, err := repo.ResolveEngineFromBackend(context.Background())
	if !errors.Is(err, enginepkg.ErrUnavailable) {
		t.Fatalf("ResolveEngineFromBackend error = %v, want ErrUnavailable", err)
	}
	if repo.Engine().Mode() != enginepkg.ModeLegacy {
		t.Fatalf("failed resolution changed engine mode to %q", repo.Engine().Mode())
	}
}

func TestResolveEngineFromBackendRejectsMalformedManifest(t *testing.T) {
	be := mem.New()
	repo := newEngineTestRepository(t, be)
	err := be.Save(
		context.Background(),
		backend.Handle{Type: backend.SlateDBFile, Name: enginepkg.ManifestName},
		backend.NewByteReader([]byte("{"), be.Hasher()),
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := repo.ResolveEngineFromBackend(context.Background()); err == nil {
		t.Fatal("expected malformed manifest error")
	}
}
