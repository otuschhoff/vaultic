package legacyimport

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

var (
	ErrLimitReached = errors.New("legacy import limit reached")
	errPackTimeout  = errors.New("legacy pack import timeout")
)

const (
	maxDefaultPackWorkers = 8
	defaultPackTimeout    = 5 * time.Minute
)

type Options struct {
	Resume             bool
	DryRun             bool
	BatchSize          uint32
	PackWorkers        uint
	PackTimeout        time.Duration
	MaxErrors          uint64
	WorkBudget         uint64
	SnapshotDepth      uint
	SnapshotWorkBudget uint64
}

type Finding struct {
	SourceID vaultic.ID `json:"source_id"`
	Stage    string     `json:"stage"`
	Error    string     `json:"error"`
}

type Result struct {
	IndexesSeen       uint64    `json:"indexes_seen"`
	IndexesImported   uint64    `json:"indexes_imported"`
	IndexesResumed    uint64    `json:"indexes_resumed"`
	PacksImported     uint64    `json:"packs_imported"`
	BlobsImported     uint64    `json:"blobs_imported"`
	RecordsSeen       uint64    `json:"records_seen"`
	RecordsImported   uint64    `json:"records_imported"`
	RecordsSkipped    uint64    `json:"records_skipped"`
	CrawlDebtCreated  uint64    `json:"crawl_debt_created"`
	SnapshotsSeen     uint64    `json:"snapshots_seen"`
	SnapshotsImported uint64    `json:"snapshots_imported"`
	SnapshotsResumed  uint64    `json:"snapshots_resumed"`
	TreesVisited      uint64    `json:"trees_visited"`
	NodesVisited      uint64    `json:"nodes_visited"`
	NodesImported     uint64    `json:"nodes_imported"`
	WarningsSeen      uint64    `json:"warnings"`
	ErrorsSeen        uint64    `json:"errors"`
	Checkpoint        string    `json:"checkpoint,omitempty"`
	Findings          []Finding `json:"findings,omitempty"`
}

type Source interface {
	vaultic.Lister
	vaultic.LoaderUnpacked
}

type PackStatter interface {
	Stat(context.Context, backend.Handle) (backend.FileInfo, error)
}

type Store interface {
	Get(context.Context, []byte) ([]byte, bool, error)
	ImportLegacyPack(context.Context, daemon.LegacyPackImport) error
	Put(context.Context, []byte, []byte, bool) error
}

type packImportResult struct {
	imported daemon.LegacyPackImport
	debt     *schema.CrawlDebtRecord
	err      error
	complete bool
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func Import(ctx context.Context, source Source, statter PackStatter, store Store, options Options) (Result, error) {
	var result Result
	if options.PackTimeout < 0 {
		return result, fmt.Errorf("pack timeout must not be negative")
	}
	var workUsed uint64
	err := legacyindex.ForAllIndexes(ctx, source, source, func(indexID vaultic.ID, index *legacyindex.Index, loadErr error) error {
		result.IndexesSeen++
		if loadErr != nil {
			return recordFinding(&result, options, indexID, "decode-index", loadErr)
		}
		schemaIndexID := schema.ID(indexID)
		if options.Resume {
			value, found, err := store.Get(ctx, schema.ImportCheckpointKey(schemaIndexID))
			if err != nil {
				return fmt.Errorf("read import checkpoint for %s: %w", indexID.Str(), err)
			}
			if found {
				if _, err := schema.UnmarshalImportCheckpointRecord(value); err != nil {
					return fmt.Errorf("decode import checkpoint for %s: %w", indexID.Str(), err)
				}
				result.IndexesResumed++
				result.Checkpoint = indexID.String()
				return nil
			}
		}

		packs := collectPacks(ctx, index)
		if err := ctx.Err(); err != nil {
			return err
		}
		selected := packs

		limitReached := false
		for packIndex, indexedPack := range packs {
			work := uint64(len(indexedPack.Blobs))
			if options.WorkBudget > 0 && workUsed+work > options.WorkBudget {
				selected = packs[:packIndex]
				limitReached = true
				break
			}
			workUsed += work
			result.RecordsSeen += work
		}
		outcomes, failedPack := importPacks(ctx, statter, store, schemaIndexID, selected, options)
		var checkpoint schema.ImportCheckpointRecord
		for packIndex, outcome := range outcomes {
			if !outcome.complete || outcome.err != nil {
				continue
			}
			if outcome.debt != nil {
				result.CrawlDebtCreated++
			}
			result.PacksImported++
			result.BlobsImported += outcome.imported.Record.BlobCount
			result.RecordsImported += outcome.imported.Record.BlobCount
			result.RecordsSkipped += uint64(len(selected[packIndex].Blobs)) - outcome.imported.Record.BlobCount
			checkpoint.PacksImported++
			checkpoint.BlobsImported += outcome.imported.Record.BlobCount
			if outcome.debt != nil {
				checkpoint.ErrorsSeen++
				result.WarningsSeen++
				result.Findings = append(result.Findings, Finding{SourceID: indexID, Stage: "stat-pack", Error: outcome.debt.ErrorClass})
			}
		}
		if failedPack >= 0 {
			return fmt.Errorf("import pack %s from index %s: %w", selected[failedPack].PackID.Str(), indexID.Str(), outcomes[failedPack].err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if limitReached {
			return ErrLimitReached
		}
		if !options.DryRun {
			encoded, err := checkpoint.MarshalBinary()
			if err != nil {
				return err
			}
			if err := store.Put(ctx, schema.ImportCheckpointKey(schemaIndexID), encoded, true); err != nil {
				return fmt.Errorf("publish import checkpoint for %s: %w", indexID.Str(), err)
			}
			result.Checkpoint = indexID.String()
		}
		result.IndexesImported++
		return nil
	})
	if err != nil {
		return result, err
	}
	if options.SnapshotDepth > 0 || options.SnapshotWorkBudget > 0 {
		treeSource, ok := source.(SnapshotSource)
		if !ok {
			return result, fmt.Errorf("snapshot import requires blob loading support")
		}
		treeStore, ok := store.(TreeStore)
		if !ok {
			return result, fmt.Errorf("snapshot import requires revision storage support")
		}
		if err := importSnapshots(ctx, treeSource, treeStore, options, &result); err != nil {
			return result, err
		}
	}
	return result, nil
}

func importPacks(
	ctx context.Context,
	statter PackStatter,
	store Store,
	sourceIndex schema.ID,
	packs []legacyindex.PackBlobs,
	options Options,
) ([]packImportResult, int) {
	outcomes := make([]packImportResult, len(packs))
	workers := options.PackWorkers
	if workers == 0 {
		workers = min(uint(runtime.GOMAXPROCS(0)), maxDefaultPackWorkers)
	}
	workers = min(workers, uint(len(packs)))
	packTimeout := options.PackTimeout
	if packTimeout == 0 {
		packTimeout = defaultPackTimeout
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var group sync.WaitGroup
	var failOnce sync.Once
	failedPack := -1
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				var packIndex int
				var ok bool
				select {
				case <-workerCtx.Done():
					return
				case packIndex, ok = <-jobs:
					if !ok {
						return
					}
				}
				packCtx, cancelPack := context.WithTimeoutCause(workerCtx, packTimeout, errPackTimeout)
				indexedPack := packs[packIndex]
				imported, debt, err := buildPackImport(packCtx, statter, sourceIndex, indexedPack)
				if err == nil {
					imported.BatchSize = options.BatchSize
					if !options.DryRun {
						err = store.ImportLegacyPack(packCtx, imported)
					}
				}
				if err != nil && errors.Is(context.Cause(packCtx), errPackTimeout) {
					err = fmt.Errorf("pack import exceeded %s; increase --pack-timeout if the storage backend is healthy: %w", packTimeout, err)
				}
				cancelPack()
				outcomes[packIndex] = packImportResult{imported: imported, debt: debt, err: err, complete: true}
				if err != nil {
					failOnce.Do(func() {
						failedPack = packIndex
						cancel()
					})
					return
				}
			}
		}()
	}
dispatch:
	for packIndex := range packs {
		select {
		case jobs <- packIndex:
		case <-workerCtx.Done():
			break dispatch
		}
	}
	close(jobs)
	group.Wait()
	return outcomes, failedPack
}

func recordFinding(result *Result, options Options, sourceID vaultic.ID, stage string, err error) error {
	result.ErrorsSeen++
	result.Findings = append(result.Findings, Finding{SourceID: sourceID, Stage: stage, Error: err.Error()})
	if options.MaxErrors > 0 && result.ErrorsSeen >= options.MaxErrors {
		return ErrLimitReached
	}
	return nil
}

func collectPacks(ctx context.Context, index *legacyindex.Index) []legacyindex.PackBlobs {
	packs := make([]legacyindex.PackBlobs, 0)
	seen := make(vaultic.IDSet)
	for indexedPack := range index.EachByPack(ctx, nil) {
		packs = append(packs, indexedPack)
		seen.Insert(indexedPack.PackID)
	}
	for packID := range index.Packs() {
		if !seen.Has(packID) {
			packs = append(packs, legacyindex.PackBlobs{PackID: packID})
		}
	}
	sort.Slice(packs, func(left, right int) bool {
		return string(packs[left].PackID[:]) < string(packs[right].PackID[:])
	})
	return packs
}

func buildPackImport(
	ctx context.Context,
	statter PackStatter,
	sourceIndex schema.ID,
	indexedPack legacyindex.PackBlobs,
) (daemon.LegacyPackImport, *schema.CrawlDebtRecord, error) {
	packID := schema.ID(indexedPack.PackID)
	record := schema.PackRecord{Lifecycle: schema.PackImported, SourceIndexIDs: []schema.ID{sourceIndex}}
	blobs := make(map[schema.ID]schema.BlobRecord)
	types := make([]schema.BlobType, 0, len(indexedPack.Blobs))
	type physicalLocation struct {
		blobID schema.ID
		offset uint64
		length uint32
		typeID schema.BlobType
	}
	locations := make(map[physicalLocation]schema.BlobLocation, len(indexedPack.Blobs))
	for _, blob := range indexedPack.Blobs {
		if blob.Length > math.MaxUint32 || blob.UncompressedLength > math.MaxUint32 {
			return daemon.LegacyPackImport{}, nil, fmt.Errorf("pack %s contains an oversized blob location", indexedPack.PackID.Str())
		}
		blobType, err := convertBlobType(blob.Type)
		if err != nil {
			return daemon.LegacyPackImport{}, nil, err
		}
		location := schema.BlobLocation{
			PackID: packID, Offset: uint64(blob.Offset), Length: uint32(blob.Length),
			UncompressedSize: uint32(blob.UncompressedLength), Type: blobType,
		}
		id := schema.ID(blob.ID)
		locationKey := physicalLocation{blobID: id, offset: location.Offset, length: location.Length, typeID: location.Type}
		if existing, found := locations[locationKey]; found {
			if location.UncompressedSize > existing.UncompressedSize {
				locations[locationKey] = location
			}
			continue
		}
		locations[locationKey] = location
		if math.MaxUint64-record.PayloadSize < uint64(location.Length) {
			return daemon.LegacyPackImport{}, nil, fmt.Errorf("pack %s payload size overflows", indexedPack.PackID.Str())
		}
		record.PayloadSize += uint64(location.Length)
	}
	for locationKey, location := range locations {
		value := blobs[locationKey.blobID]
		value.Locations = append(value.Locations, location)
		blobs[locationKey.blobID] = value
		types = append(types, location.Type)
	}
	record.BlobCount = uint64(len(locations))
	record.Type = schema.ClassifyPack(types)

	var debt *schema.CrawlDebtRecord
	info, err := statter.Stat(ctx, backend.Handle{Type: backend.PackFile, Name: indexedPack.PackID.String()})
	if err != nil || info.Size < 0 || uint64(info.Size) < record.PayloadSize {
		errorClass := "pack-stat-unavailable"
		if err == nil {
			errorClass = "pack-size-smaller-than-index-payload"
			record.PhysicalSize = uint64(info.Size)
			record.PhysicalSizeKnown = true
		}
		debt = &schema.CrawlDebtRecord{
			SourceIndexOrPack: packID, SourceKnown: true, PathOrTree: indexedPack.PackID[:],
			Reason: schema.DebtUnavailablePack, Status: schema.DebtPending, ErrorClass: errorClass,
		}
	} else {
		record.PhysicalSize = uint64(info.Size)
		record.HeaderSize = record.PhysicalSize - record.PayloadSize
		record.PhysicalSizeKnown = true
	}
	imported := daemon.LegacyPackImport{SourceIndex: sourceIndex, PackID: packID, Record: record, Blobs: blobs}
	if debt != nil {
		imported.DebtKey = schema.CrawlDebtKey(schema.ID{}, packID)
		imported.Debt = debt
	}
	return imported, debt, nil
}

func convertBlobType(blobType vaultic.BlobType) (schema.BlobType, error) {
	switch blobType {
	case vaultic.DataBlob:
		return schema.BlobData, nil
	case vaultic.TreeBlob:
		return schema.BlobTree, nil
	default:
		return 0, fmt.Errorf("unsupported legacy blob type %d", blobType)
	}
}
