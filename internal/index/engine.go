package index

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"

	"github.com/otuschhoff/vaultic/internal/backend"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const (
	ManifestName          = "manifest"
	ManifestFormatVersion = 1
	ManifestSchemaVersion = "0"
)

// Mode describes which metadata engine is configured for a repository.
type Mode string

const (
	ModeLegacy  Mode = "legacy"
	ModeSlateDB Mode = "slatedb"
)

func (m Mode) String() string {
	return string(m)
}

// ManifestState records the detection result for a SlateDB manifest namespace.
type ManifestState string

const (
	ManifestAbsent      ManifestState = "absent"
	ManifestValid       ManifestState = "valid"
	ManifestCorrupt     ManifestState = "corrupt"
	ManifestUnsupported ManifestState = "unsupported"
)

func (s ManifestState) String() string {
	return string(s)
}

var (
	ErrUnavailable = fmt.Errorf("slatedb engine unavailable")
)

// Manifest is the vaultic-owned marker that makes SlateDB authoritative for a
// repository. SlateDB's internal manifests are not used for engine selection.
type Manifest struct {
	FormatVersion uint   `json:"format_version"`
	SchemaVersion string `json:"schema_version"`
	RepositoryID  string `json:"repository_id"`
	Authoritative bool   `json:"authoritative"`
}

// Resolution is the deterministic result of inspecting a repository backend.
type Resolution struct {
	Mode     Mode
	State    ManifestState
	Manifest *Manifest
}

// LegacySink is used by export-mode adapters to write deterministic legacy data.
type LegacySink interface {
	WriteBlob(vaultic.PackBlob) error
	Close() error
}

// Engine is the common lifecycle and identity surface for metadata engines.
type Engine interface {
	Mode() Mode
	Close() error
}

// ReadEngine exposes point lookups without granting scan or write capabilities.
type ReadEngine interface {
	Engine
	Lookup(vaultic.BlobHandle) []*pack.PackedBlob
	LookupSize(vaultic.BlobHandle) (uint, bool)
}

// ScanEngine exposes read-only iteration for diagnostics and pack inspection.
type ScanEngine interface {
	Engine
	Values() iter.Seq[*pack.PackedBlob]
	ListPacks(context.Context, vaultic.IDSet) <-chan legacyindex.PackBlobs
}

// WriteEngine exposes legacy index loading and publication operations.
type WriteEngine interface {
	Engine
	AddPending(vaultic.BlobHandle, uint) bool
	StorePack(context.Context, vaultic.ID, pack.Blobs, vaultic.SaverUnpacked[vaultic.FileType]) error
	Load(context.Context, vaultic.ListerLoaderUnpacked, vaultic.Counter, func(vaultic.ID, *legacyindex.Index, error) error) error
	Flush(context.Context, vaultic.SaverUnpacked[vaultic.FileType]) error
}

// ExportEngine exposes the legacy export path used while a daemon-backed engine is
// still being brought online.
type ExportEngine interface {
	Engine
	ExportLegacy(context.Context, LegacySink) error
}

// LegacyIndexEngine is the compatibility adapter around the existing JSON
// MasterIndex. It deliberately preserves the exact MasterIndex data types.
type LegacyIndexEngine interface {
	ReadEngine
	ScanEngine
	WriteEngine
	ExportEngine
}

// LegacyEngine adapts the current JSON-index repository to the engine contract.
// This keeps legacy behavior centralized while allowing the daemon-backed engine
// to be introduced behind the same interface.
type LegacyEngine struct {
	master *legacyindex.MasterIndex
}

var _ LegacyIndexEngine = (*LegacyEngine)(nil)

func NewLegacyEngine(master ...*legacyindex.MasterIndex) *LegacyEngine {
	var idx *legacyindex.MasterIndex
	if len(master) > 0 {
		idx = master[0]
	}
	if idx == nil {
		idx = legacyindex.NewMasterIndex()
	}
	return &LegacyEngine{master: idx}
}

func (*LegacyEngine) Mode() Mode { return ModeLegacy }

func (e *LegacyEngine) Lookup(handle vaultic.BlobHandle) []*pack.PackedBlob {
	return e.master.Lookup(handle)
}

func (e *LegacyEngine) LookupSize(handle vaultic.BlobHandle) (uint, bool) {
	return e.master.LookupSize(handle)
}

func (e *LegacyEngine) Values() iter.Seq[*pack.PackedBlob] {
	return e.master.Values()
}

func (e *LegacyEngine) AddPending(handle vaultic.BlobHandle, size uint) bool {
	return e.master.AddPending(handle, size)
}

func (e *LegacyEngine) StorePack(ctx context.Context, id vaultic.ID, blobs pack.Blobs, repo vaultic.SaverUnpacked[vaultic.FileType]) error {
	return e.master.StorePack(ctx, id, blobs, repo)
}

func (e *LegacyEngine) Load(ctx context.Context, repo vaultic.ListerLoaderUnpacked, progress vaultic.Counter, callback func(vaultic.ID, *legacyindex.Index, error) error) error {
	return e.master.Load(ctx, repo, progress, callback)
}

func (e *LegacyEngine) Flush(ctx context.Context, repo vaultic.SaverUnpacked[vaultic.FileType]) error {
	return e.master.Flush(ctx, repo)
}

func (e *LegacyEngine) ListPacks(ctx context.Context, packs vaultic.IDSet) <-chan legacyindex.PackBlobs {
	return e.master.ListPacks(ctx, packs)
}

func (e *LegacyEngine) ExportLegacy(ctx context.Context, sink LegacySink) error {
	for value := range e.master.Values() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sink.WriteBlob(value); err != nil {
			return err
		}
	}
	return sink.Close()
}

func (*LegacyEngine) Close() error {
	return nil
}

// Resolve inspects the dedicated backend namespace and validates the manifest.
func Resolve(ctx context.Context, be backend.Backend, repositoryID string) (Resolution, error) {
	manifestHandle := backend.Handle{Type: backend.SlateDBFile, Name: ManifestName, IsMetadata: true}
	_, err := be.Stat(ctx, manifestHandle)
	if err != nil {
		if !be.IsNotExist(err) {
			return Resolution{Mode: ModeSlateDB, State: ManifestCorrupt}, fmt.Errorf("stat slatedb manifest: %w", err)
		}

		hasNamespace := false
		if listErr := be.List(ctx, backend.SlateDBFile, func(backend.FileInfo) error {
			hasNamespace = true
			return nil
		}); listErr != nil && !be.IsNotExist(listErr) {
			return Resolution{Mode: ModeSlateDB, State: ManifestCorrupt}, fmt.Errorf("list slatedb namespace: %w", listErr)
		}
		if hasNamespace {
			return Resolution{Mode: ModeSlateDB, State: ManifestCorrupt}, fmt.Errorf("partial slatedb namespace without manifest")
		}
		return Resolution{Mode: ModeLegacy, State: ManifestAbsent}, nil
	}

	var payload []byte
	err = be.Load(ctx, manifestHandle, 0, 0, func(reader io.Reader) error {
		var readErr error
		payload, readErr = io.ReadAll(io.LimitReader(reader, 64*1024+1))
		return readErr
	})
	if err != nil {
		return Resolution{Mode: ModeSlateDB, State: ManifestCorrupt}, fmt.Errorf("read slatedb manifest: %w", err)
	}
	if len(payload) > 64*1024 {
		return Resolution{Mode: ModeSlateDB, State: ManifestCorrupt}, fmt.Errorf("read slatedb manifest: exceeds 64 KiB")
	}

	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Resolution{Mode: ModeSlateDB, State: ManifestCorrupt}, fmt.Errorf("decode slatedb manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Resolution{Mode: ModeSlateDB, State: ManifestCorrupt}, fmt.Errorf("decode slatedb manifest: trailing data")
	}
	if manifest.FormatVersion != ManifestFormatVersion || manifest.SchemaVersion != ManifestSchemaVersion {
		return Resolution{Mode: ModeSlateDB, State: ManifestUnsupported, Manifest: &manifest}, fmt.Errorf("unsupported slatedb manifest format %d schema %q", manifest.FormatVersion, manifest.SchemaVersion)
	}
	if !manifest.Authoritative || manifest.RepositoryID == "" || manifest.RepositoryID != repositoryID {
		return Resolution{Mode: ModeSlateDB, State: ManifestCorrupt, Manifest: &manifest}, fmt.Errorf("invalid slatedb manifest repository or authority")
	}
	return Resolution{Mode: ModeSlateDB, State: ManifestValid, Manifest: &manifest}, nil
}
