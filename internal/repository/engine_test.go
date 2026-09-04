package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/feature"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

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
