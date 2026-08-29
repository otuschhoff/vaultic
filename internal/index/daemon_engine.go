package index

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// TierPolicy describes how a repository routes packs to storage tiers. It is
// applied at publish time and recorded, never recomputed on read, so a
// repository that later stops using --repo-hot can still explain where a pack
// came from.
//
// The zero value is deliberately "routing not established", which records
// tier-unknown. Claiming a single-backend layout by default would state a fact
// that is wrong for every hot/cold repository whose engine was wired without a
// policy.
type TierPolicy struct {
	// Resolved reports that the repository's backend layout was actually
	// inspected. Without it no tier is recorded.
	Resolved bool
	// HotCold reports whether the repository was opened with a hot/cold split.
	HotCold bool
	// MinRetention is the operator-configured minimum retention for cold
	// storage. Zero leaves packs retention-unknown; no deadline is invented.
	MinRetention time.Duration
	// StorageClass is the backend-reported class, when the operator supplied
	// one. It is free-form and advisory.
	StorageClass string
}

// tierFor maps a pack type onto the tier its bytes were actually routed to.
//
// A tree pack in a hot/cold repository is mirrored, not hot: hotcold.Save
// writes every hot file to the hot backend and then mirrors it to the cold
// backend, so a hot-only pack does not exist. Mixed and unclassified packs are
// never routed by vaultic itself, so their tier stays unknown rather than
// being guessed.
func (policy TierPolicy) tierFor(packType schema.PackType) schema.PackTier {
	if !policy.Resolved || packType == schema.PackUnknown {
		return schema.TierUnknown
	}
	if !policy.HotCold {
		return schema.TierSingle
	}
	switch packType {
	case schema.PackTree:
		return schema.TierMirrored
	case schema.PackData:
		return schema.TierCold
	default:
		return schema.TierUnknown
	}
}

// applyTo records tier and lifetime facts on a pack vaultic is publishing.
// The creation time is known precisely because vaultic is writing the pack
// now; imported packs never reach this path.
func (policy TierPolicy) applyTo(record *schema.PackRecord, now time.Time) {
	record.Tier = policy.tierFor(record.Type)
	record.StorageClass = policy.StorageClass
	record.CreationTime, record.CreationTimeKnown = now.UnixNano(), true
	// Unknown is stated explicitly rather than left as the zero value, so an
	// in-memory record carries the same facts as the persisted one.
	record.RetentionSource, record.MinRetentionUntil = schema.RetentionUnknown, 0
	if policy.MinRetention <= 0 {
		return
	}
	// Retention applies to bytes held in cold storage. A mirrored pack has a
	// cold copy, so it carries the deadline too.
	if record.Tier != schema.TierCold && record.Tier != schema.TierMirrored {
		return
	}
	record.MinRetentionUntil = now.Add(policy.MinRetention).UnixNano()
	record.RetentionSource = schema.RetentionConfig
}

// DaemonEngine keeps the legacy index as a synchronous compatibility
// projection while making the daemon's schema catalog durable first.
//
// The projection continues to provide the existing synchronous lookup API and
// emits Restic-compatible JSON indexes on Flush. A failed daemon publication
// prevents the projection from advancing, so an authoritative repository never
// silently accepts a pack that is absent from SlateDB.
type DaemonEngine struct {
	legacy           *LegacyEngine
	client           *daemon.Client
	store            *daemon.SchemaStore
	mu               sync.Mutex
	pendingSnapshots map[vaultic.ID][]byte
	nextSnapshotRoot []byte
	pendingPacks     map[vaultic.ID]struct{}
	tier             TierPolicy
	now              func() time.Time
}

var _ LegacyIndexEngine = (*DaemonEngine)(nil)

// NewDaemonEngine creates an authoritative engine over a validated daemon
// client. The optional legacy engine owns the in-process JSON projection.
func NewDaemonEngine(client *daemon.Client, legacy ...*LegacyEngine) *DaemonEngine {
	projection := NewLegacyEngine()
	if len(legacy) > 0 && legacy[0] != nil {
		projection = legacy[0]
	}
	return &DaemonEngine{legacy: projection, client: client, store: daemon.NewSchemaStore(client), pendingSnapshots: make(map[vaultic.ID][]byte), pendingPacks: make(map[vaultic.ID]struct{}), now: time.Now}
}

// SetTierPolicy records how this repository routes packs. It must be called
// before the first pack is published; packs published without it are recorded
// as tier-unknown rather than being assigned a guessed tier.
func (engine *DaemonEngine) SetTierPolicy(policy TierPolicy) {
	engine.mu.Lock()
	engine.tier = policy
	engine.mu.Unlock()
}

func (engine *DaemonEngine) tierPolicy() TierPolicy {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.tier
}

func (engine *DaemonEngine) SchemaStore() *daemon.SchemaStore { return engine.store }

func (engine *DaemonEngine) SetNextSnapshotRoot(rootKey []byte) {
	engine.mu.Lock()
	engine.nextSnapshotRoot = append([]byte(nil), rootKey...)
	engine.mu.Unlock()
}

func (engine *DaemonEngine) MarkSnapshotPending(ctx context.Context, id vaultic.ID, originalJSON []byte) error {
	engine.mu.Lock()
	rootKey := append([]byte(nil), engine.nextSnapshotRoot...)
	engine.nextSnapshotRoot = nil
	engine.mu.Unlock()
	if len(rootKey) == 0 {
		return fmt.Errorf("snapshot %s has no reconciled root", id.Str())
	}
	if err := engine.store.MarkExportPending(ctx, schema.ID(id), rootKey); err != nil {
		return err
	}
	engine.mu.Lock()
	engine.pendingSnapshots[id] = append([]byte(nil), originalJSON...)
	engine.mu.Unlock()
	return nil
}

func (engine *DaemonEngine) MarkSnapshotFailed(ctx context.Context, id vaultic.ID, failure error) error {
	engine.mu.Lock()
	delete(engine.pendingSnapshots, id)
	engine.mu.Unlock()
	return engine.store.MarkExportFailed(ctx, schema.ID(id), failure)
}

func (engine *DaemonEngine) PublishSnapshotScope(ctx context.Context, id vaultic.ID, rootKey []byte) error {
	engine.mu.Lock()
	originalJSON, found := engine.pendingSnapshots[id]
	engine.mu.Unlock()
	if !found {
		return fmt.Errorf("snapshot %s has no pending compatibility projection", id.Str())
	}
	if err := engine.store.PublishSnapshotScope(ctx, daemon.SnapshotScope{SnapshotID: schema.ID(id), RootKey: rootKey, OriginalJSON: originalJSON}); err != nil {
		return err
	}
	engine.mu.Lock()
	delete(engine.pendingSnapshots, id)
	engine.mu.Unlock()
	return nil
}

func (*DaemonEngine) Mode() Mode { return ModeSlateDB }

func (engine *DaemonEngine) Lookup(handle vaultic.BlobHandle) []*pack.PackedBlob {
	return engine.legacy.Lookup(handle)
}

func (engine *DaemonEngine) LookupSize(handle vaultic.BlobHandle) (uint, bool) {
	return engine.legacy.LookupSize(handle)
}

func (engine *DaemonEngine) Values() iter.Seq[*pack.PackedBlob] {
	return engine.legacy.Values()
}

func (engine *DaemonEngine) AddPending(handle vaultic.BlobHandle, size uint) bool {
	return engine.legacy.AddPending(handle, size)
}

func (engine *DaemonEngine) StorePack(ctx context.Context, id vaultic.ID, blobs pack.Blobs, repo vaultic.SaverUnpacked[vaultic.FileType]) error {
	return engine.storePack(ctx, id, blobs, repo, 0, false)
}

func (engine *DaemonEngine) StorePackSized(ctx context.Context, id vaultic.ID, blobs pack.Blobs, repo vaultic.SaverUnpacked[vaultic.FileType], physicalSize uint64) error {
	return engine.storePack(ctx, id, blobs, repo, physicalSize, true)
}

func (engine *DaemonEngine) storePack(ctx context.Context, id vaultic.ID, blobs pack.Blobs, repo vaultic.SaverUnpacked[vaultic.FileType], physicalSize uint64, physicalSizeKnown bool) error {
	clock := engine.now
	if clock == nil {
		clock = time.Now
	}
	published, err := schemaPack(id, blobs, physicalSize, physicalSizeKnown, engine.tierPolicy(), clock())
	if err != nil {
		return err
	}
	if err := engine.store.PublishPack(ctx, published); err != nil {
		return fmt.Errorf("publish pack %s to slatedb: %w", id.Str(), err)
	}
	if err := engine.legacy.StorePack(ctx, id, blobs, repo); err != nil {
		return err
	}
	engine.mu.Lock()
	engine.pendingPacks[id] = struct{}{}
	engine.mu.Unlock()
	return nil
}

func (engine *DaemonEngine) Load(ctx context.Context, repo vaultic.ListerLoaderUnpacked, progress vaultic.Counter, callback func(vaultic.ID, *legacyindex.Index, error) error) error {
	_ = callback
	if err := engine.recoverPendingSnapshots(ctx, repo); err != nil {
		return err
	}
	pendingPacks, err := engine.loadPendingPacks(ctx)
	if err != nil {
		return err
	}
	byPack := make(map[vaultic.ID]pack.Blobs)
	var after []byte
	for {
		entries, done, err := engine.store.ScanPrefix(ctx, []byte("b:"), after, 10_000)
		if err != nil {
			return fmt.Errorf("load authoritative blob catalog: %w", err)
		}
		for _, entry := range entries {
			parsed, parseErr := schema.ParseKey(entry.Key)
			if parseErr != nil || parsed.Kind != schema.KeyBlob {
				return fmt.Errorf("load authoritative blob catalog: invalid blob key")
			}
			record, decodeErr := schema.UnmarshalBlobRecord(entry.Value)
			if decodeErr != nil {
				return fmt.Errorf("load authoritative blob catalog: %w", decodeErr)
			}
			for _, location := range record.Locations {
				blobType := vaultic.DataBlob
				if location.Type == schema.BlobTree {
					blobType = vaultic.TreeBlob
				}
				packID := vaultic.ID(location.PackID)
				byPack[packID] = append(byPack[packID], pack.Blob{
					BlobHandle: vaultic.BlobHandle{ID: vaultic.ID(parsed.ID), Type: blobType},
					Offset:     uint(location.Offset), Length: uint(location.Length), UncompressedLength: uint(location.UncompressedSize),
				})
				progress.Add(1)
			}
			after = append(after[:0], entry.Key...)
		}
		if done {
			break
		}
		if len(entries) == 0 {
			return fmt.Errorf("load authoritative blob catalog: scan made no progress")
		}
	}
	projection := legacyindex.NewIndex()
	recoveryProjection := legacyindex.NewIndex()
	for packID, blobs := range byPack {
		if _, pending := pendingPacks[packID]; pending {
			recoveryProjection.StorePack(packID, blobs)
		} else {
			projection.StorePack(packID, blobs)
		}
	}
	projection.Finalize()
	engine.legacy.master.Insert(projection)
	if len(pendingPacks) > 0 {
		engine.legacy.master.Insert(recoveryProjection)
		engine.mu.Lock()
		for id := range pendingPacks {
			engine.pendingPacks[id] = struct{}{}
		}
		engine.mu.Unlock()
	}
	return nil
}

func (engine *DaemonEngine) loadPendingPacks(ctx context.Context) (map[vaultic.ID]struct{}, error) {
	pending := make(map[vaultic.ID]struct{})
	var after []byte
	for {
		entries, done, err := engine.store.ScanPrefix(ctx, []byte("p:"), after, 10_000)
		if err != nil {
			return nil, fmt.Errorf("scan authoritative pack catalog: %w", err)
		}
		for _, entry := range entries {
			parsed, parseErr := schema.ParseKey(entry.Key)
			if parseErr != nil || parsed.Kind != schema.KeyPack {
				return nil, fmt.Errorf("scan authoritative pack catalog: invalid pack key")
			}
			record, decodeErr := schema.UnmarshalPackRecord(entry.Value)
			if decodeErr != nil {
				return nil, fmt.Errorf("scan authoritative pack catalog: %w", decodeErr)
			}
			if record.Lifecycle == schema.PackExportPending {
				pending[vaultic.ID(parsed.ID)] = struct{}{}
			}
			after = append(after[:0], entry.Key...)
		}
		if done {
			return pending, nil
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("scan authoritative pack catalog: scan made no progress")
		}
	}
}

func (engine *DaemonEngine) recoverPendingSnapshots(ctx context.Context, repo vaultic.LoaderUnpacked) error {
	var after []byte
	for {
		entries, done, err := engine.store.ScanPrefix(ctx, []byte("meta:export-snapshot:"), after, 1_000)
		if err != nil {
			return fmt.Errorf("scan pending snapshot exports: %w", err)
		}
		for _, entry := range entries {
			parsed, parseErr := schema.ParseKey(entry.Key)
			if parseErr != nil || parsed.Kind != schema.KeyExportCheckpoint {
				return fmt.Errorf("scan pending snapshot exports: invalid checkpoint key")
			}
			checkpoint, decodeErr := schema.UnmarshalExportCheckpointRecord(entry.Value)
			if decodeErr != nil {
				return fmt.Errorf("scan pending snapshot exports: %w", decodeErr)
			}
			if checkpoint.State == schema.ExportPending {
				snapshotID := vaultic.ID(parsed.ID)
				originalJSON, loadErr := repo.LoadUnpacked(ctx, vaultic.SnapshotFile, snapshotID)
				if loadErr != nil {
					return fmt.Errorf("recover snapshot %s export: %w", snapshotID.Str(), loadErr)
				}
				if publishErr := engine.store.PublishSnapshotScope(ctx, daemon.SnapshotScope{SnapshotID: parsed.ID, RootKey: checkpoint.RootKey, OriginalJSON: originalJSON}); publishErr != nil {
					return fmt.Errorf("recover snapshot %s export: %w", snapshotID.Str(), publishErr)
				}
			}
			after = append(after[:0], entry.Key...)
		}
		if done {
			return nil
		}
		if len(entries) == 0 {
			return fmt.Errorf("scan pending snapshot exports: scan made no progress")
		}
	}
}

func (engine *DaemonEngine) Flush(ctx context.Context, repo vaultic.SaverUnpacked[vaultic.FileType]) error {
	if err := engine.legacy.Flush(ctx, repo); err != nil {
		return err
	}
	engine.mu.Lock()
	pending := make([]vaultic.ID, 0, len(engine.pendingPacks))
	for id := range engine.pendingPacks {
		pending = append(pending, id)
	}
	engine.mu.Unlock()
	for _, id := range pending {
		if err := engine.store.MarkPackPublished(ctx, schema.ID(id)); err != nil {
			return fmt.Errorf("complete pack %s compatibility export: %w", id.Str(), err)
		}
		engine.mu.Lock()
		delete(engine.pendingPacks, id)
		engine.mu.Unlock()
	}
	return nil
}

func (engine *DaemonEngine) ListPacks(ctx context.Context, packs vaultic.IDSet) <-chan legacyindex.PackBlobs {
	return engine.legacy.ListPacks(ctx, packs)
}

func (engine *DaemonEngine) ExportLegacy(ctx context.Context, sink LegacySink) error {
	return engine.legacy.ExportLegacy(ctx, sink)
}

func (engine *DaemonEngine) Close() error {
	return errors.Join(engine.legacy.Close(), engine.client.Close(context.Background()))
}

func schemaPack(id vaultic.ID, blobs pack.Blobs, physicalSize uint64, physicalSizeKnown bool, policy TierPolicy, now time.Time) (daemon.PublishedPack, error) {
	if len(blobs) == 0 {
		return daemon.PublishedPack{}, fmt.Errorf("published pack %s contains no blobs", id.Str())
	}
	record := schema.PackRecord{BlobCount: uint64(len(blobs)), Lifecycle: schema.PackExportPending}
	published := daemon.PublishedPack{PackID: schema.ID(id), Blobs: make(map[schema.ID]schema.BlobRecord, len(blobs))}
	types := make([]schema.BlobType, 0, len(blobs))
	for _, blob := range blobs {
		if uint64(blob.Length) > math.MaxUint32 || uint64(blob.UncompressedLength) > math.MaxUint32 {
			return daemon.PublishedPack{}, fmt.Errorf("published pack %s has oversized blob", id.Str())
		}
		locationType := schema.BlobData
		switch blob.Type {
		case vaultic.DataBlob:
		case vaultic.TreeBlob:
			locationType = schema.BlobTree
		default:
			return daemon.PublishedPack{}, fmt.Errorf("published pack %s has invalid blob type", id.Str())
		}
		if math.MaxUint64-record.PayloadSize < uint64(blob.Length) {
			return daemon.PublishedPack{}, fmt.Errorf("published pack %s payload size overflows", id.Str())
		}
		record.PayloadSize += uint64(blob.Length)
		types = append(types, locationType)
		blobID := schema.ID(blob.ID)
		blobRecord := published.Blobs[blobID]
		blobRecord.Locations = append(blobRecord.Locations, schema.BlobLocation{
			PackID: schema.ID(id), Offset: uint64(blob.Offset), Length: uint32(blob.Length),
			UncompressedSize: uint32(blob.UncompressedLength), Type: locationType,
		})
		published.Blobs[blobID] = blobRecord
	}
	record.Type = schema.ClassifyPack(types)
	policy.applyTo(&record, now)
	if physicalSizeKnown {
		if physicalSize < record.PayloadSize {
			return daemon.PublishedPack{}, fmt.Errorf("published pack %s is smaller than its payload", id.Str())
		}
		record.PhysicalSize = physicalSize
		record.HeaderSize = physicalSize - record.PayloadSize
		record.PhysicalSizeKnown = true
	}
	published.Record = record
	return published, nil
}
