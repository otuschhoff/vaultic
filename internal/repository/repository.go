package repository

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/cache"
	"github.com/otuschhoff/vaultic/internal/backend/dryrun"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/feature"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	"github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"

	"golang.org/x/sync/errgroup"
)

const MinPackSize = 4 * 1024 * 1024

const DefaultPackSize = 16 * 1024 * 1024

// MaxPackSize is the largest supported target pack size (4 GiB, matching rustic).
const MaxPackSize = 4 * 1024 * 1024 * 1024

// Repository is used to access a repository in a backend.
type Repository struct {
	be                     backend.Backend
	cfg                    vaultic.Config
	key                    *crypto.Key
	keyID                  vaultic.ID
	idx                    *index.MasterIndex
	engine                 enginepkg.Engine
	cache                  *cache.Cache
	placementBackends      map[uint64]backend.Backend
	ownedPlacementBackends []backend.Backend
	ownedClosers           []io.Closer
	stagedPackRoots        StagedPackRoots

	opts Options

	packerWg    *errgroup.Group
	mainWg      *errgroup.Group
	blobSaver   *sync.WaitGroup
	uploader    *packerUploader
	treePM      *packerManager
	dataPM      *packerManager
	packerCount int

	allocEnc sync.Once
	allocDec sync.Once
	enc      *zstd.Encoder
	dec      *zstd.Decoder
	encErr   error
	decErr   error

	zeroChunkOnce sync.Once
	zeroChunkID   vaultic.ID
}

// AttachStagedPackRoots installs authenticated deferred-journal reachability
// for every destructive operation performed through this repository.
func (r *Repository) AttachStagedPackRoots(roots StagedPackRoots) {
	r.stagedPackRoots = roots
}

// PlacementBackend returns an opened placement backend for authenticated operator workflows.
func (r *Repository) PlacementBackend(id uint64) (backend.Backend, bool) {
	placementBackend, ok := r.placementBackends[id]
	return placementBackend, ok
}

// AddOwnedCloser binds a local capability lease to the repository lifetime.
func (r *Repository) AddOwnedCloser(closer io.Closer) {
	r.ownedClosers = append(r.ownedClosers, closer)
}

type snapshotAuthority interface {
	MarkSnapshotPending(context.Context, vaultic.ID, []byte) error
	MarkSnapshotFailed(context.Context, vaultic.ID, error) error
}

type daemonOptionsContextKey struct{}

type metadataLossRecoveryContextKey struct{}

// WithDaemonOptions configures metadata daemon attachment for operator
// workflows that must open an already-authoritative repository.
func WithDaemonOptions(ctx context.Context, options daemon.Options) context.Context {
	return context.WithValue(ctx, daemonOptionsContextKey{}, options)
}

// WithMetadataLossRecovery explicitly selects the legacy JSON index after
// total loss of authoritative SlateDB metadata.
func WithMetadataLossRecovery(ctx context.Context) context.Context {
	return context.WithValue(ctx, metadataLossRecoveryContextKey{}, true)
}

// internalRepository allows using SaveUnpacked and RemoveUnpacked with all FileTypes
type internalRepository struct {
	*Repository
}

// appendTransaction exposes only additive repository capabilities. It does
// not own locking; commands must acquire the Shared lock policy before asking
// for one. Keeping RemoveUnpacked out of this type makes accidental deletion
// by an append workflow a compile-time error.
type appendTransaction struct {
	repo *Repository
}

type deferredTransaction struct {
	repo *Repository
}

var _ vaultic.AppendRepository = (*appendTransaction)(nil)

// AppendTransaction returns the restricted capability for an append-only
// operation such as backup, copy destination writes, or merge.
func (r *Repository) AppendTransaction() vaultic.AppendRepository {
	return &appendTransaction{repo: r}
}

// DeferredTransaction forces a full crawl and cannot publish ordinary metadata.
func (r *Repository) DeferredTransaction() vaultic.AppendRepository {
	return &deferredTransaction{repo: r}
}

func (tx *deferredTransaction) Connections() uint { return tx.repo.Connections() }

func (tx *deferredTransaction) LoadBlob(context.Context, vaultic.BlobHandle, []byte) ([]byte, error) {
	return nil, fmt.Errorf("deferred crawl cannot load a metadata basis")
}

func (tx *deferredTransaction) LookupBlobSize(vaultic.BlobHandle) (uint, bool) { return 0, false }

func (tx *deferredTransaction) WithBlobUploader(context.Context, func(context.Context, vaultic.BlobSaverWithAsync) error) error {
	return fmt.Errorf("deferred crawl requires an explicit staging uploader")
}

func (tx *deferredTransaction) SaveUnpacked(context.Context, vaultic.WriteableFileType, []byte) (vaultic.ID, error) {
	return vaultic.ID{}, fmt.Errorf("deferred crawl cannot publish ordinary metadata")
}

func (tx *deferredTransaction) PackSize() uint { return tx.repo.PackSize() }

func (tx *deferredTransaction) ChunkerFactory() vaultic.ChunkerFactory {
	return tx.repo.ChunkerFactory()
}

func (tx *appendTransaction) Connections() uint { return tx.repo.Connections() }

func (tx *appendTransaction) LoadBlob(ctx context.Context, h vaultic.BlobHandle, buf []byte) ([]byte, error) {
	return tx.repo.LoadBlob(ctx, h, buf)
}

func (tx *appendTransaction) LookupBlobSize(h vaultic.BlobHandle) (uint, bool) {
	return tx.repo.LookupBlobSize(h)
}

func (tx *appendTransaction) WithBlobUploader(ctx context.Context, fn func(context.Context, vaultic.BlobSaverWithAsync) error) error {
	return tx.repo.WithBlobUploader(ctx, fn)
}

func (tx *appendTransaction) SaveUnpacked(ctx context.Context, t vaultic.WriteableFileType, buf []byte) (vaultic.ID, error) {
	return tx.repo.SaveUnpacked(ctx, t, buf)
}

func (tx *appendTransaction) PackSize() uint { return tx.repo.PackSize() }

func (tx *appendTransaction) ChunkerFactory() vaultic.ChunkerFactory {
	return tx.repo.ChunkerFactory()
}

type Options struct {
	Compression   CompressionMode
	PackSize      uint
	NoExtraVerify bool

	// TreePackSize and DataPackSize optionally override PackSize for tree and
	// data packs respectively (from the in-repo config). Zero means PackSize.
	TreePackSize       uint64
	DataPackSize       uint64
	TreePackGrowFactor uint32
	DataPackGrowFactor uint32
	// TreePackSizeLimit and DataPackSizeLimit cap the per-type pack size
	// (0 = no extra limit beyond MaxPackSize).
	TreePackSizeLimit uint64
	DataPackSizeLimit uint64
}

// CompressionMode configures if data should be compressed.
type CompressionMode uint

// Constants for the different compression levels.
const (
	CompressionAuto    CompressionMode = 0
	CompressionOff     CompressionMode = 1
	CompressionMax     CompressionMode = 2
	CompressionFastest CompressionMode = 3
	CompressionBetter  CompressionMode = 4
	CompressionInvalid CompressionMode = 5
)

// Set implements the method needed for pflag command flag parsing.
func (c *CompressionMode) Set(s string) error {
	switch s {
	case "auto":
		*c = CompressionAuto
	case "off":
		*c = CompressionOff
	case "max":
		*c = CompressionMax
	case "fastest":
		*c = CompressionFastest
	case "better":
		*c = CompressionBetter
	default:
		*c = CompressionInvalid
		return fmt.Errorf("invalid compression mode %q, must be one of (auto|off|fastest|better|max)", s)
	}

	return nil
}

func (c *CompressionMode) String() string {
	switch *c {
	case CompressionAuto:
		return "auto"
	case CompressionOff:
		return "off"
	case CompressionMax:
		return "max"
	case CompressionFastest:
		return "fastest"
	case CompressionBetter:
		return "better"
	default:
		return "invalid"
	}

}

func (c *CompressionMode) Type() string {
	return "mode"
}

// New returns a new repository with backend be.
func New(be backend.Backend, opts Options) (*Repository, error) {
	if opts.Compression == CompressionInvalid {
		return nil, errors.New("invalid compression mode")
	}

	if opts.PackSize == 0 {
		opts.PackSize = DefaultPackSize
	}
	if opts.PackSize > MaxPackSize {
		return nil, fmt.Errorf("pack size larger than limit of %v MiB", MaxPackSize/1024/1024)
	} else if opts.PackSize < MinPackSize {
		return nil, fmt.Errorf("pack size smaller than minimum of %v MiB", MinPackSize/1024/1024)
	}

	// validate per-type pack sizes
	for _, ps := range []struct {
		name  string
		value uint64
	}{
		{"tree", opts.TreePackSize},
		{"data", opts.DataPackSize},
	} {
		if ps.value != 0 && (ps.value < MinPackSize || ps.value > MaxPackSize) {
			return nil, fmt.Errorf("%s pack size %v MiB out of range (%v-%v MiB)", ps.name, ps.value/1024/1024, MinPackSize/1024/1024, MaxPackSize/1024/1024)
		}
	}

	repo := &Repository{
		be:                be,
		opts:              opts,
		idx:               index.NewMasterIndex(),
		packerCount:       defaultPackerCount,
		placementBackends: make(map[uint64]backend.Backend),
	}
	repo.engine = enginepkg.NewLegacyEngine(repo.idx)

	return repo, nil
}

// setConfig assigns the given config and updates the repository parameters accordingly
func (r *Repository) setConfig(cfg vaultic.Config) {
	r.cfg = cfg
}

// Config returns the repository configuration.
func (r *Repository) Config() vaultic.Config {
	return r.cfg
}

// Engine returns the currently selected metadata engine.
func (r *Repository) Engine() enginepkg.Engine {
	if r == nil {
		return nil
	}
	if r.engine == nil {
		r.engine = enginepkg.NewLegacyEngine(r.idx)
	}
	return r.engine
}

// SetEngine replaces the active engine for this repository.
func (r *Repository) SetEngine(engine enginepkg.Engine) {
	if engine == nil {
		r.engine = enginepkg.NewLegacyEngine(r.idx)
		return
	}
	r.engine = engine
}

func (r *Repository) legacyIndexEngine() (enginepkg.LegacyIndexEngine, error) {
	engine, ok := r.Engine().(enginepkg.LegacyIndexEngine)
	if !ok {
		return nil, ErrLegacyEngineRequired
	}
	return engine, nil
}

// ResolveEngineFromBackend validates the authoritative manifest through the
// backend abstraction and selects the corresponding engine.
func (r *Repository) ResolveEngineFromBackend(ctx context.Context) (enginepkg.Engine, error) {
	resolution, err := enginepkg.Resolve(ctx, r.be, r.cfg.ID)
	if err != nil {
		return nil, err
	}
	if resolution.Mode == enginepkg.ModeSlateDB {
		if recovery, _ := ctx.Value(metadataLossRecoveryContextKey{}).(bool); recovery {
			engine := enginepkg.NewRecoveryLegacyEngine(r.idx)
			r.SetEngine(engine)
			return engine, nil
		}
		if !feature.Flag.Enabled(feature.SlateDBAuthoritative) {
			return nil, fmt.Errorf("repository %s requires the slatedb-authoritative feature: %w", r.cfg.ID, enginepkg.ErrUnavailable)
		}
		options, _ := ctx.Value(daemonOptionsContextKey{}).(daemon.Options)
		options.RepositoryID = r.cfg.ID
		var client *daemon.Client
		var connectErr error
		if options.DaemonPath != "" {
			client, connectErr = daemon.Ensure(ctx, options)
		} else {
			client, connectErr = daemon.Connect(ctx, options)
		}
		if connectErr != nil {
			return nil, fmt.Errorf("connect authoritative metadata daemon for repository %s: %w: %w", r.cfg.ID, enginepkg.ErrUnavailable, connectErr)
		}
		engine := enginepkg.NewDaemonEngine(client, enginepkg.NewLegacyEngine(r.idx))
		engine.SetTierPolicy(r.tierPolicy())
		r.SetEngine(engine)
		return engine, nil
	}
	engine := enginepkg.NewLegacyEngine(r.idx)
	r.SetEngine(engine)
	return engine, nil
}

// EnableSlateDBAuthority activates a validated daemon-backed metadata engine.
// Daemon connection/start configuration remains an operator workflow concern.
func (r *Repository) EnableSlateDBAuthority(ctx context.Context, client *daemon.Client) error {
	if !feature.Flag.Enabled(feature.SlateDBAuthoritative) {
		return fmt.Errorf("slatedb authority requires the slatedb-authoritative feature")
	}
	if client == nil {
		return fmt.Errorf("slatedb authority requires a validated daemon client")
	}
	if err := enginepkg.Activate(ctx, r.be, r.cfg.ID); err != nil {
		return err
	}
	engine := enginepkg.NewDaemonEngine(client, enginepkg.NewLegacyEngine(r.idx))
	engine.SetTierPolicy(r.tierPolicy())
	r.SetEngine(engine)
	return nil
}

// tierPolicy derives pack routing from the repository's actual backend layout,
// so tier is recorded from how bytes are really placed rather than from a
// separate configuration that could drift from it.
func (r *Repository) tierPolicy() enginepkg.TierPolicy {
	model, err := r.PlacementModel()
	if err != nil {
		_, _, hotCold := r.HotCold()
		return enginepkg.TierPolicy{Resolved: true, HotCold: hotCold}
	}
	policy := enginepkg.TierPolicy{Resolved: true, HotCold: model.HotCold}
	policy.Backends = make([]enginepkg.PlacementBackendPolicy, 0, len(model.Backends))
	for _, backend := range model.Backends {
		policy.Backends = append(policy.Backends, enginepkg.PlacementBackendPolicy{
			ID: backend.ID, Hash: backend.Hash, Role: backend.Role,
			Ingest: backend.Ingest, ReadEnabled: backend.ReadEnabled,
			Offsite: backend.Offsite, FailureDomain: backend.FailureDomain,
			MinRetention:         backend.MinRetention(),
			PricePerGBMonth:      backend.PricePerGBMonth,
			PricePerGBEgress:     backend.PricePerGBEgress,
			PricePer1KRequests:   backend.PricePer1KRequests,
			MaxBandwidthBytes:    backend.MaxBandwidthBytes,
			MaxRequestsPerSecond: backend.MaxRequestsPerSecond,
			ObjectOverheadBytes:  backend.ObjectOverheadBytes,
		})
	}
	return policy
}

// PackSize return the target size of a pack file when uploading
func (r *Repository) PackSize() uint {
	return r.opts.PackSize
}

// SetCompression changes the compression mode (used to apply the in-repo config).
func (r *Repository) SetCompression(c CompressionMode) {
	r.opts.Compression = c
}

// SetNoExtraVerify changes whether data is verified before upload (used to
// apply the in-repo config).
func (r *Repository) SetNoExtraVerify(v bool) {
	r.opts.NoExtraVerify = v
}

// SetTreePackSize sets the target size and optional size limit for tree packs.
func (r *Repository) SetTreePackSize(size, limit uint64) {
	r.opts.TreePackSize = size
	r.opts.TreePackSizeLimit = limit
}

// SetTreePackSizeConfig applies the complete in-repo tree pack sizing policy.
func (r *Repository) SetTreePackSizeConfig(size, limit uint64, growFactor uint32) {
	r.opts.TreePackSize = size
	r.opts.TreePackSizeLimit = limit
	r.opts.TreePackGrowFactor = growFactor
}

// SetDataPackSize sets the target size and optional size limit for data packs.
func (r *Repository) SetDataPackSize(size, limit uint64) {
	r.opts.DataPackSize = size
	r.opts.DataPackSizeLimit = limit
}

// SetDataPackSizeConfig applies the complete in-repo data pack sizing policy.
func (r *Repository) SetDataPackSizeConfig(size, limit uint64, growFactor uint32) {
	r.opts.DataPackSize = size
	r.opts.DataPackSizeLimit = limit
	r.opts.DataPackGrowFactor = growFactor
}

// TreePackSizeBytes returns the effective target size for tree packs.
func (r *Repository) TreePackSizeBytes() uint64 {
	size, _, _ := r.packSizing(vaultic.TreeBlob)
	return size
}

// DataPackSizeBytes returns the effective target size for data packs.
// The pack managers are created on demand (when the first blob is saved),
// so applying the in-repo config after opening the repository still takes
// effect for new packs.
func (r *Repository) DataPackSizeBytes() uint64 {
	size, _, _ := r.packSizing(vaultic.DataBlob)
	return size
}

func (r *Repository) packSizing(t vaultic.BlobType) (size, limit uint64, growFactor uint32) {
	cfg := r.Config()
	switch t {
	case vaultic.TreeBlob:
		if r.opts.TreePackSize != 0 {
			return r.opts.TreePackSize, r.opts.TreePackSizeLimit, r.opts.TreePackGrowFactor
		}
		if cfg.TreePackSizeBytes != 0 || cfg.TreePackGrowFactor != nil || cfg.TreePackSizeLimitBytes != 0 {
			return cfg.TreePackSize()
		}
		if r.opts.PackSize != DefaultPackSize {
			return uint64(r.opts.PackSize), 0, 0
		}
		return cfg.TreePackSize()
	case vaultic.DataBlob:
		if r.opts.DataPackSize != 0 {
			return r.opts.DataPackSize, r.opts.DataPackSizeLimit, r.opts.DataPackGrowFactor
		}
		if cfg.DataPackSizeBytes != 0 || cfg.DataPackGrowFactor != nil || cfg.DataPackSizeLimitBytes != 0 {
			return cfg.DataPackSize()
		}
		if r.opts.PackSize != DefaultPackSize {
			return uint64(r.opts.PackSize), 0, 0
		}
		return cfg.DataPackSize()
	}
	return uint64(r.opts.PackSize), 0, 0
}

func (r *Repository) currentBlobSize(t vaultic.BlobType) uint64 {
	types := make(map[vaultic.ID]vaultic.BlobType)
	engine, err := r.legacyIndexEngine()
	if err != nil {
		return 0
	}
	for blob := range engine.Values() {
		packID := blob.PackID()
		if previous, ok := types[packID]; !ok {
			types[packID] = blob.Handle().Type
		} else if previous != vaultic.NumBlobTypes && previous != blob.Handle().Type {
			types[packID] = vaultic.NumBlobTypes
		}
	}
	packSizes, err := pack.Size(context.Background(), r, false)
	if err != nil {
		return 0
	}
	var size uint64
	for packID, packType := range types {
		if packType == t {
			size += uint64(packSizes[packID])
		}
	}
	return size
}

// UseCache replaces the backend with the wrapped cache.
func (r *Repository) UseCache(c *cache.Cache, errorLog func(string, ...any)) {
	if c == nil {
		return
	}
	debug.Log("using cache")
	r.cache = c
	r.be = c.Wrap(r.be, errorLog)
}

func (r *Repository) Cache() *cache.Cache {
	return r.cache
}

// Backend returns the backend used by the repository (used for internal
// plumbing such as hot/cold metadata mirroring).
func (r *Repository) Backend() backend.Backend {
	return r.be
}

// AttachPlacementBackend makes a configured placement location addressable by
// the scheduler and read router. The repository owns and closes the backend.
func (r *Repository) AttachPlacementBackend(hash uint64, placement backend.Backend) {
	if hash == 0 || placement == nil {
		return
	}
	r.placementBackends[hash] = placement
	r.ownedPlacementBackends = append(r.ownedPlacementBackends, placement)
}

func (r *Repository) placementBackend(hash uint64) (backend.Backend, bool) {
	placement, found := r.placementBackends[hash]
	return placement, found
}

// HotCold returns the hot and cold backends if this repository is a hot/cold
// repository (opened with --repo-hot), or nil otherwise. It unwraps the
// wrapper backends (cache, retry, logging) to find the composite.
func (r *Repository) HotCold() (hot, cold backend.Backend, ok bool) {
	type hotcolder interface {
		Hot() backend.Backend
		Cold() backend.Backend
	}
	be := r.be
	for be != nil {
		if hc, isHC := be.(hotcolder); isHC {
			return hc.Hot(), hc.Cold(), true
		}
		u, isUnwrapper := be.(backend.Unwrapper)
		if !isUnwrapper {
			break
		}
		be = u.Unwrap()
	}
	return nil, nil, false
}

// SetDryRun sets the repo backend into dry-run mode.
func (r *Repository) SetDryRun() {
	r.be = dryrun.New(r.be)
}

func (r *Repository) Checker() *Checker {
	return newChecker(r)
}

// LoadUnpacked loads and decrypts the file with the given type and ID.
func (r *Repository) LoadUnpacked(ctx context.Context, t vaultic.FileType, id vaultic.ID) ([]byte, error) {
	debug.Log("load %v with id %v", t, id)

	if t == vaultic.ConfigFile {
		id = vaultic.ID{}
	}

	buf, err := r.LoadRaw(ctx, t, id)
	if err != nil {
		return nil, err
	}

	if len(buf) < crypto.CiphertextLength(0) {
		return nil, fmt.Errorf("invalid data in %v file, too short", t)
	}

	nonce, ciphertext := buf[:r.key.NonceSize()], buf[r.key.NonceSize():]
	plaintext, err := r.key.Open(ciphertext[:0], nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	if t != vaultic.ConfigFile {
		return r.decompressUnpacked(plaintext)
	}

	return plaintext, nil
}

type haver interface {
	Has(backend.Handle) bool
}

// sortCachedPacksFirst moves all cached pack files to the front of blobs.
func sortCachedPacksFirst(cache haver, blobs []*pack.PackedBlob) {
	if cache == nil {
		return
	}

	// no need to sort a list with one element
	if len(blobs) == 1 {
		return
	}

	cached := blobs[:0]
	noncached := make([]*pack.PackedBlob, 0, len(blobs)/2)

	for _, blob := range blobs {
		if cache.Has(backend.Handle{Type: backend.PackFile, Name: blob.PackID().String()}) {
			cached = append(cached, blob)
			continue
		}
		noncached = append(noncached, blob)
	}

	copy(blobs[len(cached):], noncached)
}

// LoadBlob loads a blob from the repository.
// It may use all of buf[:cap(buf)] as scratch space.
func (r *Repository) LoadBlob(ctx context.Context, bh vaultic.BlobHandle, buf []byte) ([]byte, error) {
	debug.Log("load %v (buf len %v, cap %d)", bh, len(buf), cap(buf))

	// lookup packs
	engine, err := r.legacyIndexEngine()
	if err != nil {
		return nil, err
	}
	blobs := engine.Lookup(bh)
	if len(blobs) == 0 {
		debug.Log("id %v not found in index", bh.ID)
		return nil, errors.Errorf("id %v not found in repository", bh.ID)
	}

	// try cached pack files first
	sortCachedPacksFirst(r.cache, blobs)

	buf, err = r.loadBlob(ctx, blobs, buf)
	if err != nil {
		if r.cache != nil {
			for _, blob := range blobs {
				h := backend.Handle{Type: backend.PackFile, Name: blob.PackID().String(), IsMetadata: blob.Blob.Type.IsMetadata()}
				// ignore errors as there's not much we can do here
				_ = r.cache.Forget(h)
			}
		}

		buf, err = r.loadBlob(ctx, blobs, buf)
	}
	return buf, err
}
