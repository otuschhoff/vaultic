package index

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/backend/mock"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func saveSlateDBFile(t *testing.T, be backend.Backend, name string, payload []byte) {
	t.Helper()
	err := be.Save(context.Background(), backend.Handle{Type: backend.SlateDBFile, Name: name, IsMetadata: true}, backend.NewByteReader(payload, be.Hasher()))
	if err != nil {
		t.Fatalf("save slatedb file %q: %v", name, err)
	}
}

func manifestPayload(t *testing.T, manifest Manifest) []byte {
	t.Helper()
	payload, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestRecoveryLegacyEngineRejectsMutations(t *testing.T) {
	engine := NewRecoveryLegacyEngine(legacyindex.NewMasterIndex())
	if engine.AddPending(vaultic.BlobHandle{}, 1) {
		t.Fatal("recovery engine accepted a pending blob")
	}
	if err := engine.Flush(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("recovery engine flush error = %v", err)
	}
}

type collectingLegacySink struct {
	blobs  []vaultic.PackBlob
	closed bool
}

func (sink *collectingLegacySink) WriteBlob(blob vaultic.PackBlob) error {
	sink.blobs = append(sink.blobs, blob)
	return nil
}

func (sink *collectingLegacySink) Close() error {
	sink.closed = true
	return nil
}

func TestLegacyEngineDefaultsToEmptyMasterIndex(t *testing.T) {
	engine := NewLegacyEngine()
	if got := engine.Lookup(vaultic.NewRandomBlobHandle()); len(got) != 0 {
		t.Fatalf("Lookup returned %d entries, want none", len(got))
	}
}

func TestResolveBackendManifestStates(t *testing.T) {
	ctx := context.Background()
	t.Run("absent", func(t *testing.T) {
		resolution, err := Resolve(ctx, mem.New(), "repo")
		if err != nil || resolution.Mode != ModeLegacy || resolution.State != ManifestAbsent {
			t.Fatalf("Resolve = %#v, %v", resolution, err)
		}
	})

	t.Run("partial namespace", func(t *testing.T) {
		be := mem.New()
		saveSlateDBFile(t, be, "lock", []byte("partial"))
		resolution, err := Resolve(ctx, be, "repo")
		if err == nil || resolution.State != ManifestCorrupt {
			t.Fatalf("Resolve = %#v, %v", resolution, err)
		}
	})

	t.Run("malformed", func(t *testing.T) {
		be := mem.New()
		saveSlateDBFile(t, be, ManifestName, []byte("{"))
		resolution, err := Resolve(ctx, be, "repo")
		if err == nil || resolution.State != ManifestCorrupt {
			t.Fatalf("Resolve = %#v, %v", resolution, err)
		}
	})

	t.Run("unreadable", func(t *testing.T) {
		be := mock.NewBackend()
		be.StatFn = func(context.Context, backend.Handle) (backend.FileInfo, error) {
			return backend.FileInfo{Name: ManifestName, Size: 10}, nil
		}
		be.OpenReaderFn = func(context.Context, backend.Handle, int, int64) (io.ReadCloser, error) {
			return nil, fmt.Errorf("unreadable")
		}
		resolution, err := Resolve(ctx, be, "repo")
		if err == nil || resolution.State != ManifestCorrupt {
			t.Fatalf("Resolve = %#v, %v", resolution, err)
		}
	})

	t.Run("unsupported", func(t *testing.T) {
		be := mem.New()
		saveSlateDBFile(t, be, ManifestName, manifestPayload(t, Manifest{FormatVersion: 2, SchemaVersion: ManifestSchemaVersion, RepositoryID: "repo", Authoritative: true}))
		resolution, err := Resolve(ctx, be, "repo")
		if err == nil || resolution.State != ManifestUnsupported {
			t.Fatalf("Resolve = %#v, %v", resolution, err)
		}
	})

	t.Run("wrong repository", func(t *testing.T) {
		be := mem.New()
		saveSlateDBFile(t, be, ManifestName, manifestPayload(t, Manifest{FormatVersion: ManifestFormatVersion, SchemaVersion: ManifestSchemaVersion, RepositoryID: "other", Authoritative: true}))
		resolution, err := Resolve(ctx, be, "repo")
		if err == nil || resolution.State != ManifestCorrupt {
			t.Fatalf("Resolve = %#v, %v", resolution, err)
		}
	})

	t.Run("valid", func(t *testing.T) {
		be := mem.New()
		saveSlateDBFile(t, be, ManifestName, manifestPayload(t, Manifest{FormatVersion: ManifestFormatVersion, SchemaVersion: ManifestSchemaVersion, RepositoryID: "repo", Authoritative: true}))
		resolution, err := Resolve(ctx, be, "repo")
		if err != nil || resolution.Mode != ModeSlateDB || resolution.State != ManifestValid {
			t.Fatalf("Resolve = %#v, %v", resolution, err)
		}
	})
}

func TestLegacyEngineConcurrentLookup(t *testing.T) {
	handle := vaultic.NewRandomBlobHandle()
	packID := vaultic.NewRandomID()
	idx := legacyindex.NewIndex()
	idx.StorePack(packID, pack.Blobs{{BlobHandle: handle, Length: 42}})
	master := legacyindex.NewMasterIndex()
	master.Insert(idx)
	engine := NewLegacyEngine(master)

	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 100 {
				entries := engine.Lookup(handle)
				if len(entries) != 1 || !entries[0].PackID().Equal(packID) {
					t.Errorf("unexpected lookup result: %v", entries)
					return
				}
			}
		}()
	}
	wait.Wait()

	if strings.TrimSpace(engine.Mode().String()) != "legacy" {
		t.Fatalf("unexpected mode %q", engine.Mode())
	}
}

func TestLegacyEngineExportPreservesBlobRecords(t *testing.T) {
	handle := vaultic.NewRandomBlobHandle()
	packID := vaultic.NewRandomID()
	idx := legacyindex.NewIndex()
	idx.StorePack(packID, pack.Blobs{{BlobHandle: handle, Offset: 17, Length: 42, UncompressedLength: 99}})
	master := legacyindex.NewMasterIndex()
	master.Insert(idx)
	sink := &collectingLegacySink{}

	if err := NewLegacyEngine(master).ExportLegacy(context.Background(), sink); err != nil {
		t.Fatalf("ExportLegacy returned error: %v", err)
	}
	if !sink.closed || len(sink.blobs) != 1 {
		t.Fatalf("sink closed=%v blobs=%d, want true and 1", sink.closed, len(sink.blobs))
	}
	got := sink.blobs[0]
	if got.Handle() != handle || !got.PackID().Equal(packID) || got.CiphertextLength() != 42 || got.PlaintextLength() != 99 {
		t.Fatalf("export changed blob record: %#v", got)
	}
}
