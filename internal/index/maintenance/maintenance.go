// Package maintenance implements operator-controlled SlateDB index workflows.
package maintenance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/analytics"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	monitor "github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/sync/errgroup"
)

const scanPageSize = 10_000
const MaxCheckRPCConcurrency = 4096

type Reader interface {
	Get(context.Context, []byte) ([]byte, bool, error)
	MultiGet(context.Context, [][]byte) ([]daemon.KeyValue, []bool, error)
	ScanPrefix(context.Context, []byte, []byte, uint32) ([]daemon.KeyValue, bool, error)
}

type Writer interface {
	MarkIndexPublished(context.Context, schema.ID, []schema.ID) (uint64, error)
	WriteMutableBatch(context.Context, []daemon.Mutation, [][]byte, bool) error
}

type Store interface {
	Reader
	Writer
}

type limitedStore struct {
	Store
	semaphore chan struct{}
	telemetry *CheckTelemetry
	operation *monitor.ActionGuard
}

func (store *limitedStore) acquire(ctx context.Context) error {
	if store.telemetry == nil {
		select {
		case store.semaphore <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	guard := store.telemetry.rpcWait.Start()
	select {
	case store.semaphore <- struct{}{}:
		guard.Succeeded()
		guard.Done()
		return nil
	default:
		guard.Contended()
	}
	select {
	case store.semaphore <- struct{}{}:
		guard.Succeeded()
		guard.Done()
		return nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			guard.TimedOut()
		}
		guard.Done()
		return ctx.Err()
	}
}

func (store *limitedStore) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if err := store.acquire(ctx); err != nil {
		return nil, false, err
	}
	defer func() { <-store.semaphore }()
	if store.telemetry == nil {
		return store.Store.Get(ctx, key)
	}
	request := store.telemetry.database.Start()
	value, found, err := store.Store.Get(ctx, key)
	request.AddBytes(uint64(len(value)))
	store.telemetry.process(store.operation, "database", uint64(len(value)))
	settleDependency(request, err)
	return value, found, err
}

func (store *limitedStore) MultiGet(ctx context.Context, keys [][]byte) ([]daemon.KeyValue, []bool, error) {
	if err := store.acquire(ctx); err != nil {
		return nil, nil, err
	}
	defer func() { <-store.semaphore }()
	if store.telemetry == nil {
		return store.Store.MultiGet(ctx, keys)
	}
	request := store.telemetry.database.Start()
	values, found, err := store.Store.MultiGet(ctx, keys)
	var bytes uint64
	for index := range values {
		bytes = saturatingAddCheck(bytes, uint64(len(values[index].Key)))
		bytes = saturatingAddCheck(bytes, uint64(len(values[index].Value)))
	}
	request.AddBytes(bytes)
	store.telemetry.process(store.operation, "database", bytes)
	settleDependency(request, err)
	return values, found, err
}

func (store *limitedStore) ScanPrefix(ctx context.Context, prefix, after []byte, limit uint32) ([]daemon.KeyValue, bool, error) {
	if err := store.acquire(ctx); err != nil {
		return nil, false, err
	}
	defer func() { <-store.semaphore }()
	if store.telemetry == nil {
		return store.Store.ScanPrefix(ctx, prefix, after, limit)
	}
	request := store.telemetry.database.Start()
	values, more, err := store.Store.ScanPrefix(ctx, prefix, after, limit)
	var bytes uint64
	for index := range values {
		bytes = saturatingAddCheck(bytes, uint64(len(values[index].Key)))
		bytes = saturatingAddCheck(bytes, uint64(len(values[index].Value)))
	}
	request.AddBytes(bytes)
	store.telemetry.process(store.operation, "database", bytes)
	settleDependency(request, err)
	return values, more, err
}

type rangeScanner interface {
	ScanRange(context.Context, []byte, uint32, func([]daemon.KeyValue) error) error
}

func scanRange(ctx context.Context, store Store, prefix []byte, limit uint32, consume func([]daemon.KeyValue) error) error {
	if scanner, ok := store.(rangeScanner); ok {
		return scanner.ScanRange(ctx, prefix, limit, consume)
	}
	var after []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, prefix, after, limit)
		if err != nil {
			return err
		}
		if !done && len(entries) == 0 {
			return fmt.Errorf("scan %q made no progress", prefix)
		}
		if len(entries) > 0 {
			after = append(after[:0], entries[len(entries)-1].Key...)
		}
		if err := consume(entries); err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func (store *limitedStore) ScanRange(ctx context.Context, prefix []byte, limit uint32, consume func([]daemon.KeyValue) error) error {
	if err := store.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-store.semaphore }()
	if store.telemetry == nil {
		return scanRange(ctx, store.Store, prefix, limit, consume)
	}
	request := store.telemetry.database.Start()
	err := scanRange(ctx, store.Store, prefix, limit, func(entries []daemon.KeyValue) error {
		var bytes uint64
		for _, entry := range entries {
			bytes = saturatingAddCheck(bytes, uint64(len(entry.Key)+len(entry.Value)))
		}
		request.AddBytes(bytes)
		store.telemetry.process(store.operation, "database", bytes)
		settleDependency(request, nil)
		err := consume(entries)
		request = store.telemetry.database.Start()
		return err
	})
	settleDependency(request, err)
	return err
}

func (store *limitedStore) CheckEncryption(ctx context.Context) (daemon.EncryptionAudit, error) {
	auditor, ok := store.Store.(EncryptionAuditor)
	if !ok {
		return daemon.EncryptionAudit{}, nil
	}
	if err := store.acquire(ctx); err != nil {
		return daemon.EncryptionAudit{}, err
	}
	defer func() { <-store.semaphore }()
	if store.telemetry == nil {
		return auditor.CheckEncryption(ctx)
	}
	request := store.telemetry.database.Start()
	result, err := auditor.CheckEncryption(ctx)
	settleDependency(request, err)
	return result, err
}

func saturatingAddCheck(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

type EncryptionAuditor interface {
	CheckEncryption(context.Context) (daemon.EncryptionAudit, error)
}

type LegacySource interface {
	vaultic.ListerLoaderUnpacked
}

type LegacyDestination interface {
	SaveLegacyIndex(context.Context, *legacyindex.Index) (vaultic.ID, error)
}

type legacyExportVerifier interface {
	vaultic.LoaderUnpacked
}

type ExportOptions struct {
	Full          bool
	DryRun        bool
	Verify        bool
	Since         uint64
	PacksPerIndex uint
}

type ExportResult struct {
	PacksSelected  uint64       `json:"packs_selected"`
	BlobsSelected  uint64       `json:"blobs_selected"`
	IndexesWritten uint64       `json:"indexes_written"`
	ExportSequence uint64       `json:"export_sequence,omitempty"`
	IndexIDs       []vaultic.ID `json:"index_ids,omitempty"`
}

type CheckResult struct {
	Coverage                  CheckCoverage    `json:"coverage"`
	Consistency               CheckConsistency `json:"consistency"`
	Resources                 CheckResources   `json:"resources"`
	EncryptionEnabled         bool             `json:"encryption_enabled"`
	EncryptionAlgorithm       string           `json:"encryption_algorithm,omitempty"`
	EnvelopeGeneration        uint64           `json:"envelope_generation,omitempty"`
	ActiveDEKVersion          uint32           `json:"active_dek_version,omitempty"`
	EncryptedObjects          uint64           `json:"encrypted_objects,omitempty"`
	PlaintextObjects          uint64           `json:"plaintext_objects,omitempty"`
	InvalidEncryptedObjects   uint64           `json:"invalid_encrypted_objects,omitempty"`
	OldDEKObjects             uint64           `json:"old_dek_objects,omitempty"`
	LegacyIndexes             uint64           `json:"legacy_indexes"`
	LegacySnapshots           uint64           `json:"legacy_snapshots"`
	SlateDBSnapshots          uint64           `json:"slatedb_snapshots"`
	LegacyLocations           uint64           `json:"legacy_locations"`
	SlateDBLocations          uint64           `json:"slatedb_locations"`
	MissingInSlateDB          uint64           `json:"missing_in_slatedb"`
	MissingInLegacy           uint64           `json:"missing_in_legacy"`
	MissingPacks              uint64           `json:"missing_packs"`
	InvalidPacks              uint64           `json:"invalid_packs"`
	AggregateMismatch         uint64           `json:"aggregate_mismatches"`
	ReverseEdgeMismatch       uint64           `json:"reverse_edge_mismatches"`
	UnresolvedReferences      uint64           `json:"unresolved_references"`
	SnapshotMismatch          uint64           `json:"snapshot_mismatches"`
	SnapshotCommitMismatch    uint64           `json:"snapshot_commit_mismatches"`
	PathVersionMismatch       uint64           `json:"path_version_mismatches"`
	UnresolvedSnapshots       uint64           `json:"unresolved_snapshots"`
	PendingCrawlDebt          uint64           `json:"pending_crawl_debt"`
	PendingExports            uint64           `json:"pending_exports"`
	FailedExports             uint64           `json:"failed_exports"`
	ExportCheckpoints         uint64           `json:"export_checkpoints"`
	MixedPacks                uint64           `json:"mixed_packs"`
	UnknownPacks              uint64           `json:"unknown_packs"`
	UnknownTierPacks          uint64           `json:"unknown_tier_packs"`
	RetentionUnknownPacks     uint64           `json:"retention_unknown_packs"`
	UsageUnaccountedPacks     uint64           `json:"usage_unaccounted_packs"`
	PlacementRecordsMalformed uint64           `json:"placement_records_malformed"`
	MissingPlacementRecords   uint64           `json:"missing_placement_records"`
	BackendPackMismatch       uint64           `json:"backend_pack_mismatches"`
	DerivedTierMismatch       uint64           `json:"derived_tier_mismatches"`
	PacksBelowDurability      uint64           `json:"packs_below_durability"`
	UnknownPlacementBackends  uint64           `json:"unknown_placement_backends"`
	VerificationStateMismatch uint64           `json:"verification_state_mismatches"`
	// TierAggregatesUnbuilt marks a repository written before the tier
	// dimension existed. It is a pending rebuild, not drift.
	TierAggregatesUnbuilt bool `json:"tier_aggregates_unbuilt,omitempty"`
	// HistoryEventsMalformed counts unreadable pack history records. History
	// is advisory and derived, so this is reported but never makes the check
	// dirty.
	HistoryEventsMalformed uint64    `json:"history_events_malformed"`
	AnalyticsMismatch      uint64    `json:"analytics_mismatches"`
	GCCandidates           uint64    `json:"gc_candidates"`
	Warnings               uint64    `json:"warnings"`
	QuorumChecked          bool      `json:"quorum_checked,omitempty"`
	QuorumNonCompliant     bool      `json:"quorum_non_compliant,omitempty"`
	MinimumCustodians      int       `json:"minimum_custodians,omitempty"`
	PrincipalVerified      bool      `json:"principal_verified,omitempty"`
	HardwareVerified       bool      `json:"hardware_verified,omitempty"`
	CustodyAssumed         bool      `json:"custody_assumed,omitempty"`
	Findings               []Finding `json:"findings,omitempty"`
}

type CheckConsistency struct {
	RepositoryID          string `json:"repository_id,omitempty"`
	Generation            uint64 `json:"generation,omitempty"`
	Sequence              uint64 `json:"sequence,omitempty"`
	SessionID             string `json:"session_id,omitempty"`
	SchemaVersion         uint8  `json:"schema_version"`
	OptionsDigest         string `json:"options_digest"`
	LegacyInventoryDigest string `json:"legacy_inventory_digest,omitempty"`
}

type CheckResources struct {
	MemoryLimitBytes  uint64                  `json:"memory_limit_bytes"`
	ScratchLimitBytes uint64                  `json:"scratch_limit_bytes"`
	ScratchPeakBytes  uint64                  `json:"scratch_peak_bytes"`
	MergePasses       uint64                  `json:"merge_passes"`
	Workers           uint                    `json:"workers"`
	RPCConcurrency    uint                    `json:"rpc_concurrency"`
	Scan              daemon.ScanStats        `json:"scan"`
	ScanRanges        []daemon.RangeScanStats `json:"scan_ranges,omitempty"`
}

type CheckCoverage struct {
	Mode     string   `json:"mode"`
	Complete bool     `json:"complete"`
	Included []string `json:"included"`
	Skipped  []string `json:"skipped,omitempty"`
}

var checkDomains = []string{
	"legacy_indexes", "legacy_snapshots", "metadata_encryption", "blob_locations", "pack_catalog",
	"pack_aggregates", "pack_lifetime", "placements", "verification", "pack_history", "operational_state",
	"export_provenance", "references", "snapshots", "snapshot_commits", "path_versions", "analytics",
}

func checkCoverage(options CheckOptions) CheckCoverage {
	coverage := CheckCoverage{Mode: "full", Complete: true, Included: append([]string(nil), checkDomains...)}
	if options.LegacyOnly {
		coverage.Mode = "legacy_only"
		coverage.Complete = false
		coverage.Included = []string{"legacy_indexes", "legacy_snapshots"}
		coverage.Skipped = append([]string(nil), checkDomains[2:]...)
	} else if options.SlateDBOnly {
		coverage.Mode = "slatedb_only"
		coverage.Complete = false
		coverage.Included = append([]string(nil), checkDomains[2:]...)
		coverage.Skipped = []string{"legacy_indexes", "legacy_snapshots", "export_provenance"}
		coverage.Included = slices.DeleteFunc(coverage.Included, func(domain string) bool { return domain == "export_provenance" })
	}
	return coverage
}

func checkOptionsDigest(options CheckOptions) (string, error) {
	paths := append([]string(nil), options.PathIndexPaths...)
	sort.Strings(paths)
	encoded, err := json.Marshal(struct {
		LegacyOnly, SlateDBOnly, IncludeCrawlDebt bool
		MaxFindings                               uint
		MemoryBytes, TempMaxBytes                 uint64
		Workers, RPCConcurrency                   uint
		ProgressInterval                          int64
		PathIndexPaths                            []string
		PlacementModel                            PlacementModel
	}{
		LegacyOnly: options.LegacyOnly, SlateDBOnly: options.SlateDBOnly,
		IncludeCrawlDebt: options.IncludeCrawlDebt, MaxFindings: options.MaxFindings,
		MemoryBytes: options.MemoryBytes, TempMaxBytes: options.TempMaxBytes,
		Workers: options.Workers, RPCConcurrency: options.RPCConcurrency,
		ProgressInterval: int64(options.ProgressInterval),
		PathIndexPaths:   paths, PlacementModel: options.PlacementModel,
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", digest[:]), nil
}

func (result CheckResult) Clean() bool {
	return !result.QuorumNonCompliant && result.PlaintextObjects == 0 && result.InvalidEncryptedObjects == 0 &&
		result.MissingInSlateDB == 0 &&
		result.MissingInLegacy == 0 &&
		result.MissingPacks == 0 &&
		result.InvalidPacks == 0 &&
		result.AggregateMismatch == 0 &&
		result.ReverseEdgeMismatch == 0 &&
		result.SnapshotMismatch == 0 &&
		result.SnapshotCommitMismatch == 0 &&
		result.PathVersionMismatch == 0 &&
		result.FailedExports == 0 &&
		result.MissingPlacementRecords == 0 &&
		result.BackendPackMismatch == 0 &&
		result.DerivedTierMismatch == 0 &&
		result.PacksBelowDurability == 0 &&
		result.VerificationStateMismatch == 0 &&
		result.AnalyticsMismatch == 0
}

func (result CheckResult) HasWarnings() bool { return result.Warnings != 0 }

type CheckOptions struct {
	LegacyOnly         bool
	SlateDBOnly        bool
	IncludeCrawlDebt   bool
	MaxFindings        uint
	MemoryBytes        uint64
	TempDir            string
	TempMaxBytes       uint64
	Workers            uint
	RPCConcurrency     uint
	ProgressInterval   time.Duration
	Progress           func(CheckProgress)
	PlacementModel     PlacementModel
	PathIndexPaths     []string
	Consistency        CheckConsistency
	Telemetry          *CheckTelemetry
	ScenarioForTesting *monitor.ExperimentController
}

type Finding struct {
	Kind string `json:"kind"`
	Key  string `json:"key"`
	Want string `json:"want,omitempty"`
	Got  string `json:"got,omitempty"`
}

type CheckProgress struct {
	Stage             string
	Elapsed           time.Duration
	Workers           uint
	RPCConcurrency    uint
	MemoryLimitBytes  uint64
	ScratchLimitBytes uint64
	ScratchPeakBytes  uint64
	Scan              daemon.ScanStats
}

type checkProgressReporter struct {
	mu        sync.Mutex
	started   time.Time
	stage     string
	options   CheckOptions
	scratch   *checkScratch
	stop      chan struct{}
	finished  chan struct{}
	telemetry *CheckTelemetry
	operation *monitor.ActionGuard
	scanStats func() daemon.ScanStats
}

func newCheckProgressReporter(options CheckOptions, scratch *checkScratch) *checkProgressReporter {
	reporter := &checkProgressReporter{
		started: time.Now(), options: options, scratch: scratch,
		stop: make(chan struct{}), finished: make(chan struct{}),
	}
	if options.Progress == nil || options.ProgressInterval <= 0 {
		close(reporter.finished)
		return reporter
	}
	go func() {
		defer close(reporter.finished)
		ticker := time.NewTicker(options.ProgressInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				reporter.emit()
			case <-reporter.stop:
				return
			}
		}
	}()
	return reporter
}

func (reporter *checkProgressReporter) set(stage string) {
	reporter.mu.Lock()
	reporter.stage = stage
	reporter.mu.Unlock()
	reporter.emit()
}

func (reporter *checkProgressReporter) emit() {
	if reporter.options.Progress == nil && reporter.telemetry == nil {
		return
	}
	reporter.mu.Lock()
	stage := reporter.stage
	scanStats := reporter.scanStats
	reporter.mu.Unlock()
	peak, _ := reporter.scratch.stats()
	update := CheckProgress{
		Stage: stage, Elapsed: time.Since(reporter.started), Workers: reporter.options.Workers,
		RPCConcurrency: reporter.options.RPCConcurrency, MemoryLimitBytes: reporter.options.MemoryBytes,
		ScratchLimitBytes: reporter.options.TempMaxBytes, ScratchPeakBytes: peak,
	}
	if scanStats != nil {
		update.Scan = scanStats()
	}
	if reporter.options.Progress != nil {
		reporter.options.Progress(update)
	}
	reporter.telemetry.progress(reporter.operation, update)
}

func (reporter *checkProgressReporter) close() {
	select {
	case <-reporter.finished:
		return
	default:
		close(reporter.stop)
		<-reporter.finished
	}
}

type RebuildResult struct {
	PacksScanned              uint64           `json:"packs_scanned"`
	AggregatesChanged         uint64           `json:"aggregates_changed"`
	PlacementRecordsChanged   uint64           `json:"placement_records_changed"`
	BackendPackRecordsChanged uint64           `json:"backend_pack_records_changed"`
	TierSummaryChanged        uint64           `json:"tier_summary_changed"`
	SnapshotCommitChanged     uint64           `json:"snapshot_commit_changed"`
	PathVersionChanged        uint64           `json:"path_version_changed"`
	PathVersionOverflow       uint64           `json:"path_version_overflow"`
	UpdateSequence            uint64           `json:"update_sequence"`
	Deltas                    []AggregateDelta `json:"deltas,omitempty"`
}

type AggregateDelta struct {
	Kind   schema.AggregateKind  `json:"kind,omitempty"`
	Tier   schema.PackTier       `json:"tier,omitempty"`
	Key    string                `json:"key"`
	Before *schema.PackAggregate `json:"before,omitempty"`
	After  schema.PackAggregate  `json:"after"`
}

const (
	packHasData uint8 = 1 << iota
	packHasTree
	packHasInvalid
)

func summarizePackType(summary uint8, blobType schema.BlobType) uint8 {
	switch blobType {
	case schema.BlobData:
		return summary | packHasData
	case schema.BlobTree:
		return summary | packHasTree
	default:
		return summary | packHasInvalid
	}
}

func classifyPackSummary(summary uint8) schema.PackType {
	switch summary {
	case packHasData:
		return schema.PackData
	case packHasTree:
		return schema.PackTree
	case packHasData | packHasTree:
		return schema.PackMixed
	default:
		return schema.PackUnknown
	}
}

func Export(
	ctx context.Context,
	store Store,
	destination LegacyDestination,
	options ExportOptions,
) (ExportResult, error) {
	var result ExportResult
	if options.PacksPerIndex == 0 {
		options.PacksPerIndex = 1_000
	}
	if options.PacksPerIndex > uint(^uint(0)>>1) {
		return result, fmt.Errorf("packs per index exceeds platform limit")
	}
	packs, err := loadPacks(ctx, store)
	if err != nil {
		return result, err
	}
	exported, err := loadExportProvenance(ctx, store)
	if err != nil {
		return result, err
	}
	selected := make(map[vaultic.ID]schema.PackRecord)
	for id, record := range packs {
		if record.Lifecycle == schema.PackDeleted {
			continue
		}
		sequence, hasCheckpoint := exported[id]
		if options.Full || (options.Since > 0 && (!hasCheckpoint || sequence > options.Since)) ||
			(options.Since == 0 && record.Lifecycle != schema.PackPublished) {
			selected[id] = record
		}
	}
	byPack, err := loadBlobLocations(ctx, store, selected)
	if err != nil {
		return result, err
	}
	ids := sortedPackIDs(selected)
	result.PacksSelected = uint64(len(ids))
	for _, blobs := range byPack {
		result.BlobsSelected += uint64(len(blobs))
	}
	for start := 0; start < len(ids); start += int(options.PacksPerIndex) {
		end := min(start+int(options.PacksPerIndex), len(ids))
		indexID, sequence, written, err := exportIndexBatch(ctx, store, destination, options, ids[start:end], byPack)
		if err != nil {
			return result, err
		}
		if !written {
			continue
		}
		result.IndexesWritten++
		result.IndexIDs = append(result.IndexIDs, indexID)
		result.ExportSequence = max(result.ExportSequence, sequence)
	}
	return result, nil
}

func exportIndexBatch(
	ctx context.Context,
	store Store,
	destination LegacyDestination,
	options ExportOptions,
	ids []vaultic.ID,
	byPack map[vaultic.ID]pack.Blobs,
) (vaultic.ID, uint64, bool, error) {
	index := legacyindex.NewIndex()
	for _, id := range ids {
		index.StorePack(id, byPack[id])
	}
	index.Finalize()
	if options.DryRun {
		return vaultic.ID{}, 0, false, nil
	}
	indexID, err := destination.SaveLegacyIndex(ctx, index)
	if err != nil {
		return vaultic.ID{}, 0, false, fmt.Errorf("save legacy index: %w", err)
	}
	if options.Verify {
		verifier, ok := destination.(legacyExportVerifier)
		if !ok {
			return vaultic.ID{}, 0, false, fmt.Errorf("export destination does not support verification")
		}
		encoded, err := verifier.LoadUnpacked(ctx, vaultic.IndexFile, indexID)
		if err != nil {
			return vaultic.ID{}, 0, false, fmt.Errorf("verify exported index %s: %w", indexID.Str(), err)
		}
		if _, err := legacyindex.DecodeIndex(encoded, indexID); err != nil {
			return vaultic.ID{}, 0, false, fmt.Errorf("verify exported index %s: %w", indexID.Str(), err)
		}
	}
	packIDs := make([]schema.ID, len(ids))
	for index, id := range ids {
		packIDs[index] = schema.ID(id)
	}
	sequence, err := store.MarkIndexPublished(ctx, schema.ID(indexID), packIDs)
	if err != nil {
		return vaultic.ID{}, 0, false, fmt.Errorf("checkpoint exported index %s: %w", indexID.Str(), err)
	}
	return indexID, sequence, true, nil
}

func Check(ctx context.Context, source LegacySource, store Store, maxFindings uint) (CheckResult, error) {
	return CheckWithOptions(ctx, source, store, CheckOptions{MaxFindings: maxFindings})
}

func CheckWithOptions(
	ctx context.Context,
	source LegacySource,
	store Store,
	options CheckOptions,
) (result CheckResult, err error) {
	if options.LegacyOnly && options.SlateDBOnly {
		return CheckResult{}, fmt.Errorf("legacy-only and SlateDB-only checks are mutually exclusive")
	}
	workers := options.Workers
	if workers == 0 {
		workers = uint(max(runtime.GOMAXPROCS(0), 1))
	}
	rpcConcurrency := options.RPCConcurrency
	if rpcConcurrency == 0 {
		rpcConcurrency = workers
	}
	if workers > 1024 || rpcConcurrency > MaxCheckRPCConcurrency {
		return CheckResult{}, fmt.Errorf("checker worker or RPC concurrency limit is too large")
	}
	options.Workers, options.RPCConcurrency = workers, rpcConcurrency
	telemetry := options.Telemetry
	if telemetry == nil {
		telemetry = NewCheckTelemetryEnabled(false)
	}
	operation := telemetry.start()
	defer func() { telemetry.finish(operation, result, err) }()
	if options.Consistency.SessionID != "" {
		validator, ok := store.(interface{ Validate(context.Context) error })
		if !ok {
			return CheckResult{}, fmt.Errorf("checker store does not validate the declared read session")
		}
		defer func() {
			if err == nil {
				err = validator.Validate(ctx)
			}
		}()
	}
	if store != nil {
		store = &limitedStore{Store: store, semaphore: make(chan struct{}, rpcConcurrency), telemetry: telemetry, operation: operation}
	}
	memoryBytes := options.MemoryBytes
	if memoryBytes == 0 {
		memoryBytes = 64 << 20
	}
	minimumMemory := locationTupleMemorySize
	if !options.LegacyOnly && !options.SlateDBOnly {
		minimumMemory *= 4
	}
	if !options.LegacyOnly {
		minimumMemory = max(minimumMemory, uint64(analyticsWorkspaceSpools*(schema.MaxPathIndexPathBytes+1024)))
	}
	if memoryBytes < minimumMemory {
		return CheckResult{}, fmt.Errorf("checker memory limit must be at least %d bytes", minimumMemory)
	}
	tempMaxBytes := options.TempMaxBytes
	if tempMaxBytes == 0 {
		tempMaxBytes = 8 << 30
	}
	options.MemoryBytes, options.TempMaxBytes = memoryBytes, tempMaxBytes
	optionsDigest, err := checkOptionsDigest(options)
	if err != nil {
		return CheckResult{}, fmt.Errorf("encode checker options: %w", err)
	}
	scratch, err := newCheckScratchWithScenario(ctx, options.TempDir, tempMaxBytes, options.ScenarioForTesting)
	if err != nil {
		return CheckResult{}, err
	}
	scratch.telemetry = telemetry
	scratch.operation = operation
	progress := newCheckProgressReporter(options, scratch)
	if limited, ok := store.(*limitedStore); ok {
		if measured, ok := unwrapProductionStore(limited.Store).(interface{ ScanStats() daemon.ScanStats }); ok {
			progress.mu.Lock()
			progress.scanStats = measured.ScanStats
			progress.mu.Unlock()
			defer func() { result.Resources.Scan = measured.ScanStats() }()
		}
		if measured, ok := unwrapProductionStore(limited.Store).(interface {
			ScanRanges() []daemon.RangeScanStats
		}); ok {
			defer func() { result.Resources.ScanRanges = measured.ScanRanges() }()
		}
	}
	progress.telemetry = telemetry
	progress.operation = operation
	defer progress.close()
	defer func() {
		result.Resources.ScratchPeakBytes, result.Resources.MergePasses = scratch.stats()
		if closeErr := scratch.close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("clean checker scratch: %w", closeErr))
		}
	}()
	result = CheckResult{
		Coverage: checkCoverage(options), Consistency: options.Consistency,
		Resources: CheckResources{
			MemoryLimitBytes: memoryBytes, ScratchLimitBytes: tempMaxBytes,
			Workers: workers, RPCConcurrency: rpcConcurrency,
		},
	}
	result.Consistency.SchemaVersion = schema.Version
	result.Consistency.OptionsDigest = optionsDigest
	progress.set("inventory")
	if !options.SlateDBOnly {
		result.Consistency.LegacyInventoryDigest, err = legacyInventoryDigest(ctx, source, scratch, memoryBytes)
		if err != nil {
			return result, err
		}
		defer func() {
			if err != nil {
				return
			}
			finalDigest, digestErr := legacyInventoryDigest(ctx, source, scratch, memoryBytes)
			if digestErr != nil {
				err = fmt.Errorf("confirm legacy inventory: %w", digestErr)
			} else if finalDigest != result.Consistency.LegacyInventoryDigest {
				err = fmt.Errorf("legacy index or snapshot inventory changed during check")
			}
		}()
	}
	spoolMemory := max(memoryBytes/4, locationTupleMemorySize)
	legacy, err := newLocationSpool(ctx, scratch, spoolMemory, 32)
	if err != nil {
		return result, err
	}
	var legacyPacks *locationSpool
	if !options.SlateDBOnly && !options.LegacyOnly {
		legacyPacks, err = newLocationMultisetSpool(ctx, scratch, spoolMemory, 32)
		if err != nil {
			return result, err
		}
	}
	if !options.SlateDBOnly {
		progress.set("legacy_scan")
		legacy, result.LegacyIndexes, err = loadLegacyLocations(ctx, source, legacy, legacyPacks, workers)
		if err != nil {
			return result, err
		}
	}
	if options.LegacyOnly {
		result.LegacyLocations, err = countLocationSpool(legacy)
		if err != nil {
			return result, err
		}
		progress.set("finalization")
		return result, nil
	}
	progress.set("encryption_audit")
	if err := checkEncryption(ctx, store, &result, options.MaxFindings); err != nil {
		return result, err
	}
	slatedb, err := newLocationSpool(ctx, scratch, spoolMemory, 32)
	if err != nil {
		return result, err
	}
	slatedbPacks, err := newLocationMultisetSpool(ctx, scratch, spoolMemory, 32)
	if err != nil {
		return result, err
	}
	progress.set("slatedb_scan")
	if err := loadSlateDBLocations(ctx, store, slatedb, slatedbPacks, workers); err != nil {
		return result, err
	}
	progress.set("catalog_join")
	wantAggregates, wantTierAggregates, err := checkPackCatalog(
		ctx, store, legacyPacks, slatedbPacks, !options.SlateDBOnly, &result, options.MaxFindings,
	)
	if err != nil {
		return result, err
	}
	if !options.SlateDBOnly {
		if err := compareLocationSpools(legacy, slatedb, &result, options.MaxFindings); err != nil {
			return result, err
		}
	} else {
		result.SlateDBLocations, err = countLocationSpool(slatedb)
		if err != nil {
			return result, err
		}
	}
	if err := legacy.close(); err != nil {
		return result, err
	}
	if err := slatedb.close(); err != nil {
		return result, err
	}
	if legacyPacks != nil {
		if err := legacyPacks.close(); err != nil {
			return result, err
		}
	}
	if err := slatedbPacks.close(); err != nil {
		return result, err
	}
	if err := checkAggregateValues(ctx, store, wantAggregates, wantTierAggregates, &result, options.MaxFindings); err != nil {
		return result, err
	}
	progress.set("parallel_validation")
	if err := runValidationJobs(ctx, source, store, scratch, memoryBytes, workers, options, &result); err != nil {
		return result, err
	}
	progress.set("finalization")
	return result, nil
}

func runValidationJobs(
	ctx context.Context,
	source LegacySource,
	store Store,
	scratch *checkScratch,
	memoryBytes uint64,
	workers uint,
	options CheckOptions,
	result *CheckResult,
) error {
	type validationJob func(context.Context, *CheckResult, uint64) error
	jobs := []validationJob{
		func(ctx context.Context, local *CheckResult, memory uint64) error {
			return checkPlacementRecords(ctx, store, scratch, memory, options.PlacementModel, local, options.MaxFindings)
		},
		func(ctx context.Context, local *CheckResult, _ uint64) error {
			if err := checkVerificationState(ctx, store, local, options.MaxFindings); err != nil {
				return err
			}
			checkPackHistory(ctx, store, local)
			return nil
		},
		func(ctx context.Context, local *CheckResult, _ uint64) error {
			return checkOperationalState(ctx, store, options, local)
		},
		func(ctx context.Context, local *CheckResult, memory uint64) error {
			if !options.SlateDBOnly {
				if err := checkExportProvenance(ctx, source, store, local, options.MaxFindings); err != nil {
					return err
				}
			}
			return checkSnapshots(ctx, source, store, scratch, memory, options.SlateDBOnly, local, options.MaxFindings)
		},
		func(ctx context.Context, local *CheckResult, memory uint64) error {
			return checkReferences(ctx, store, scratch, memory, local, options.MaxFindings)
		},
		func(ctx context.Context, local *CheckResult, _ uint64) error {
			return checkPathVersionIndex(ctx, store, options.PathIndexPaths, local, options.MaxFindings)
		},
		func(ctx context.Context, local *CheckResult, memory uint64) error {
			workspace, err := newAnalyticsCheckWorkspace(ctx, scratch, memory)
			if err != nil {
				return err
			}
			findings, total, checkErr := analytics.CheckConsistencyWithWorkspace(ctx, store, options.MaxFindings, workspace)
			closeErr := workspace.close()
			if checkErr != nil {
				return checkErr
			}
			if closeErr != nil {
				return closeErr
			}
			local.AnalyticsMismatch = total
			for _, finding := range findings {
				addFinding(local, options.MaxFindings, Finding{Kind: finding.Kind, Key: finding.Key, Want: finding.Want, Got: finding.Got})
			}
			return nil
		},
	}
	parallel := min(int(workers), len(jobs))
	jobMemory := memoryBytes / uint64(parallel)
	if jobMemory < uint64(analyticsWorkspaceSpools*(schema.MaxPathIndexPathBytes+1024)) {
		return fmt.Errorf("checker memory limit is too small for %d parallel validation workers", parallel)
	}
	results := make([]CheckResult, len(jobs))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(parallel)
	for index, job := range jobs {
		index, job := index, job
		group.Go(func() error { return job(groupCtx, &results[index], jobMemory) })
	}
	if err := group.Wait(); err != nil {
		return err
	}
	for index := range results {
		mergeValidationResult(result, &results[index], options.MaxFindings)
	}
	return nil
}

func mergeValidationResult(result, local *CheckResult, maxFindings uint) {
	result.ReverseEdgeMismatch += local.ReverseEdgeMismatch
	result.UnresolvedReferences += local.UnresolvedReferences
	result.LegacySnapshots += local.LegacySnapshots
	result.SlateDBSnapshots += local.SlateDBSnapshots
	result.SnapshotMismatch += local.SnapshotMismatch
	result.SnapshotCommitMismatch += local.SnapshotCommitMismatch
	result.PathVersionMismatch += local.PathVersionMismatch
	result.UnresolvedSnapshots += local.UnresolvedSnapshots
	result.PendingCrawlDebt += local.PendingCrawlDebt
	result.PendingExports += local.PendingExports
	result.FailedExports += local.FailedExports
	result.ExportCheckpoints += local.ExportCheckpoints
	result.PlacementRecordsMalformed += local.PlacementRecordsMalformed
	result.MissingPlacementRecords += local.MissingPlacementRecords
	result.BackendPackMismatch += local.BackendPackMismatch
	result.DerivedTierMismatch += local.DerivedTierMismatch
	result.PacksBelowDurability += local.PacksBelowDurability
	result.UnknownPlacementBackends += local.UnknownPlacementBackends
	result.VerificationStateMismatch += local.VerificationStateMismatch
	result.HistoryEventsMalformed += local.HistoryEventsMalformed
	result.AnalyticsMismatch += local.AnalyticsMismatch
	result.GCCandidates += local.GCCandidates
	result.Warnings += local.Warnings
	for _, finding := range local.Findings {
		addFinding(result, maxFindings, finding)
	}
}

// aggregateTarget is one aggregate record to compare and rewrite. Type
// aggregates always exist once a repository has any pack; tier aggregates are
// only materialized for tiers that actually hold packs, so an absent record
// for an empty tier is correct rather than drift.
type aggregateTarget struct {
	key      []byte
	delta    AggregateDelta
	expected schema.PackAggregate
	optional bool
}

// typeAggregateCount is how many aggregate targets belong to the type
// dimension; the remainder are the tier dimension.
const typeAggregateCount = int(schema.AggregateAll - schema.AggregateData + 1)

func aggregateTargets(
	rebuilt map[schema.AggregateKind]schema.PackAggregate,
	tiers map[schema.PackTier]schema.PackAggregate,
) []aggregateTarget {
	targets := make([]aggregateTarget, 0, len(rebuilt)+len(tiers))
	for kind := schema.AggregateData; kind <= schema.AggregateAll; kind++ {
		key := schema.PackAggregateKey(kind)
		targets = append(
			targets,
			aggregateTarget{key: key, expected: rebuilt[kind], delta: AggregateDelta{Kind: kind, Key: string(key)}},
		)
	}
	for _, tier := range schema.TierAggregateKinds() {
		key := schema.TierAggregateKey(tier)
		targets = append(
			targets,
			aggregateTarget{
				key:      key,
				expected: tiers[tier],
				delta:    AggregateDelta{Tier: tier, Key: string(key)},
				optional: true,
			},
		)
	}
	return targets
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func RebuildPackAggregates(ctx context.Context, store Store, dryRun bool) (RebuildResult, error) {
	packs, err := loadPacks(ctx, store)
	if err != nil {
		return RebuildResult{}, err
	}
	sequence, err := nextAggregateSequence(ctx, store)
	if err != nil {
		return RebuildResult{}, err
	}
	records := make([]schema.PackRecord, 0, len(packs))
	for _, record := range packs {
		records = append(records, record)
	}
	rebuilt, err := schema.RebuildPackAggregates(records, sequence)
	if err != nil {
		return RebuildResult{}, err
	}
	rebuiltTiers, err := schema.RebuildTierAggregates(records, sequence)
	if err != nil {
		return RebuildResult{}, err
	}
	targets := aggregateTargets(rebuilt, rebuiltTiers)

	result := RebuildResult{PacksScanned: uint64(len(records)), UpdateSequence: sequence}
	needsRebuild := false
	write := make([]bool, len(targets))
	for index, target := range targets {
		current, found, getErr := store.Get(ctx, target.key)
		if getErr != nil {
			return result, getErr
		}
		var currentRecord schema.PackAggregate
		if found {
			currentRecord, getErr = schema.UnmarshalPackAggregate(current)
			if getErr != nil {
				if !errors.Is(getErr, schema.ErrMalformed) {
					return result, getErr
				}
				found = false
			}
		}
		write[index] = found || !target.optional || !emptyAggregate(target.expected)
		comparisonCurrent, comparisonExpected := currentRecord, target.expected
		comparisonCurrent.UpdateSequence, comparisonExpected.UpdateSequence = 0, 0
		if found && comparisonCurrent == comparisonExpected {
			continue
		}
		if !found && target.optional && emptyAggregate(target.expected) {
			continue
		}
		result.AggregatesChanged++
		needsRebuild = true
		delta := target.delta
		delta.After = target.expected
		if found {
			stored := currentRecord
			delta.Before = &stored
		}
		result.Deltas = append(result.Deltas, delta)
	}
	if needsRebuild && !dryRun {
		puts := make([]daemon.Mutation, 0, len(targets))
		for index, target := range targets {
			if !write[index] {
				continue
			}
			encoded, encodeErr := target.expected.MarshalBinary()
			if encodeErr != nil {
				return result, encodeErr
			}
			puts = append(puts, daemon.Mutation{Key: target.key, Value: encoded})
		}
		if err := store.WriteMutableBatch(ctx, puts, nil, true); err != nil {
			return result, fmt.Errorf("write pack aggregates: %w", err)
		}
	}
	return result, nil
}

func emptyAggregate(aggregate schema.PackAggregate) bool {
	aggregate.UpdateSequence = 0
	return aggregate == schema.PackAggregate{}
}

func loadPacks(ctx context.Context, store Store) (map[vaultic.ID]schema.PackRecord, error) {
	result := make(map[vaultic.ID]schema.PackRecord)
	err := scan(ctx, store, []byte("p:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyPack {
			return fmt.Errorf("invalid pack key %q", entry.Key)
		}
		record, err := schema.UnmarshalPackRecord(entry.Value)
		if err != nil {
			return fmt.Errorf("decode pack %x: %w", parsed.ID, err)
		}
		result[vaultic.ID(parsed.ID)] = record
		return nil
	})
	return result, err
}

func loadBlobLocations(
	ctx context.Context,
	store Store,
	selected map[vaultic.ID]schema.PackRecord,
) (map[vaultic.ID]pack.Blobs, error) {
	result := make(map[vaultic.ID]pack.Blobs, len(selected))
	err := scan(ctx, store, []byte("b:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyBlob {
			return fmt.Errorf("invalid blob key %q", entry.Key)
		}
		record, err := schema.UnmarshalBlobRecord(entry.Value)
		if err != nil {
			return err
		}
		for _, item := range record.Locations {
			packID := vaultic.ID(item.PackID)
			if _, found := selected[packID]; !found {
				continue
			}
			result[packID] = append(
				result[packID],
				pack.Blob{
					BlobHandle: vaultic.BlobHandle{
						ID:   vaultic.ID(parsed.ID),
						Type: vaultic.BlobType(item.Type),
					},
					Offset:             uint(item.Offset),
					Length:             uint(item.Length),
					UncompressedLength: uint(item.UncompressedSize),
				},
			)
		}
		return nil
	})
	return result, err
}

func loadLegacyLocations(
	ctx context.Context,
	source LegacySource,
	result *locationSpool,
	packs *locationSpool,
	workers uint,
) (*locationSpool, uint64, error) {
	var indexes uint64
	err := legacyindex.ForAllIndexesWorkers(
		ctx,
		source,
		source,
		workers,
		func(_ vaultic.ID, index *legacyindex.Index, loadErr error) error {
			indexes++
			if loadErr != nil {
				return loadErr
			}
			for item := range index.Values() {
				if err := result.add(locationTuple{BlobID: item.Blob.ID,
					PackID:             item.Pack,
					Type:               uint8(item.Blob.Type),
					Offset:             uint64(item.Blob.Offset),
					Length:             uint64(item.Blob.Length),
					UncompressedLength: uint64(item.Blob.UncompressedLength)}); err != nil {
					return err
				}
				if packs != nil {
					if err := packs.add(locationTuple{
						BlobID: item.Pack, PackID: item.Blob.ID, Type: uint8(item.Blob.Type),
						Offset: uint64(item.Blob.Offset), Length: uint64(item.Blob.Length),
						UncompressedLength: uint64(item.Blob.UncompressedLength),
					}); err != nil {
						return err
					}
				}
			}
			if packs != nil {
				for id := range index.Packs() {
					if err := packs.add(locationTuple{BlobID: id}); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
	return result, indexes, err
}

func checkReferences(
	ctx context.Context,
	store Store,
	scratch *checkScratch,
	memoryBytes uint64,
	result *CheckResult,
	maxFindings uint,
) (err error) {
	spool, err := newLocationSpool(ctx, scratch, max(memoryBytes, locationTupleMemorySize), 32)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, spool.close()) }()
	if err := scan(ctx, store, []byte("ri:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalReverseInodeRecord(entry.Value)
		if err != nil {
			return err
		}
		if record.State == schema.ReferenceUnresolved {
			result.UnresolvedReferences++
			result.Warnings++
			return nil
		}
		return spool.add(locationTuple{
			BlobID: vaultic.ID(parsed.ID), Type: 1, Offset: uint64(parsed.FSID), Length: parsed.Inode,
		})
	}); err != nil {
		return err
	}
	if err := scan(ctx, store, []byte("rm:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalReverseManifestRecord(entry.Value)
		if err != nil {
			return err
		}
		if record.State == schema.ReferenceUnresolved {
			result.UnresolvedReferences++
			result.Warnings++
			return nil
		}
		return spool.add(locationTuple{
			BlobID: vaultic.ID(parsed.ID), PackID: vaultic.ID(parsed.SecondID), Type: 2,
		})
	}); err != nil {
		return err
	}
	if err := scan(ctx, store, []byte("rc:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalReferenceCountRecord(entry.Value)
		if err != nil {
			return err
		}
		return spool.add(locationTuple{
			BlobID: vaultic.ID(parsed.ID), Type: 3, Offset: record.DistinctInodes,
			Length: record.DistinctManifests, UncompressedLength: record.TotalReferences,
		})
	}); err != nil {
		return err
	}
	return reduceReferenceSpool(spool, result, maxFindings)
}

func checkSnapshots(
	ctx context.Context,
	source LegacySource,
	store Store,
	scratch *checkScratch,
	memoryBytes uint64,
	slatedbOnly bool,
	result *CheckResult,
	maxFindings uint,
) (err error) {
	spoolMemory := max(memoryBytes/2, locationTupleMemorySize)
	legacy, err := newLocationSpool(ctx, scratch, spoolMemory, 32)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, legacy.close()) }()
	if !slatedbOnly {
		if err := source.List(ctx, vaultic.SnapshotFile, func(id vaultic.ID, _ int64) error {
			return legacy.add(locationTuple{BlobID: id})
		}); err != nil {
			return err
		}
	}
	slatedb, err := newLocationSpool(ctx, scratch, spoolMemory, 32)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, slatedb.close()) }()
	err = scan(ctx, store, []byte("s:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalSnapshotRecord(entry.Value)
		if err != nil {
			return err
		}
		id := vaultic.ID(parsed.ID)
		if err := slatedb.add(locationTuple{BlobID: id}); err != nil {
			return err
		}
		rootKey := schema.DirectoryRevisionKey(record.RootFSID, record.RootInode, record.RootRevision)
		if _, found, getErr := store.Get(ctx, rootKey); getErr != nil {
			return getErr
		} else if !found {
			result.SnapshotMismatch++
			addFinding(result, maxFindings, Finding{Kind: "missing_snapshot_root", Key: id.String()})
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := checkSnapshotCommitIndex(ctx, store, result, maxFindings); err != nil {
		return err
	}
	if slatedbOnly {
		var err error
		result.SlateDBSnapshots, err = countLocationSpool(slatedb)
		return err
	}
	legacyIterator, err := legacy.iterator()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, legacyIterator.close()) }()
	slatedbIterator, err := slatedb.iterator()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, slatedbIterator.close()) }()
	legacyTuple, hasLegacy, err := legacyIterator.next()
	if err != nil {
		return err
	}
	slatedbTuple, hasSlateDB, err := slatedbIterator.next()
	if err != nil {
		return err
	}
	for hasLegacy || hasSlateDB {
		comparison := 0
		switch {
		case !hasLegacy:
			comparison = 1
		case !hasSlateDB:
			comparison = -1
		default:
			comparison = bytes.Compare(legacyTuple.BlobID[:], slatedbTuple.BlobID[:])
		}
		if comparison < 0 {
			id := legacyTuple.BlobID
			checkpoint, checkpointFound, err := store.Get(ctx, schema.SnapshotImportCheckpointKey(schema.ID(id)))
			if err != nil {
				return err
			}
			if checkpointFound {
				if _, err := schema.UnmarshalSnapshotImportCheckpointRecord(checkpoint); err != nil {
					return err
				}
				result.UnresolvedSnapshots++
				result.Warnings++
				addFinding(
					result,
					maxFindings,
					Finding{
						Kind: "unresolved_snapshot",
						Key:  id.String(),
						Got:  "imported traversal has no normalized root identity",
					},
				)
			} else {
				result.SnapshotMismatch++
				addFinding(result, maxFindings, Finding{Kind: "missing_snapshot", Key: id.String(), Want: "slatedb"})
			}
			legacyTuple, hasLegacy, err = legacyIterator.next()
			if err != nil {
				return err
			}
		} else if comparison > 0 {
			id := slatedbTuple.BlobID
			result.SnapshotMismatch++
			addFinding(result, maxFindings, Finding{Kind: "missing_snapshot", Key: id.String(), Want: "legacy"})
			slatedbTuple, hasSlateDB, err = slatedbIterator.next()
			if err != nil {
				return err
			}
		} else {
			legacyTuple, hasLegacy, err = legacyIterator.next()
			if err == nil {
				slatedbTuple, hasSlateDB, err = slatedbIterator.next()
			}
			if err != nil {
				return err
			}
		}
	}
	result.LegacySnapshots, err = countLocationSpool(legacy)
	if err != nil {
		return err
	}
	result.SlateDBSnapshots, err = countLocationSpool(slatedb)
	if err != nil {
		return err
	}
	return nil
}

func checkPackCatalog(
	ctx context.Context,
	store Store,
	legacy, slatedb *locationSpool,
	compareLegacy bool,
	result *CheckResult,
	maxFindings uint,
) (aggregates map[schema.AggregateKind]schema.PackAggregate, tiers map[schema.PackTier]schema.PackAggregate, err error) {
	accumulator := schema.NewPackAggregateAccumulator()
	legacyIterator, err := optionalPackContributionIterator(legacy)
	if err != nil {
		return nil, nil, err
	}
	if legacyIterator != nil {
		defer func() { err = errors.Join(err, legacyIterator.close()) }()
	}
	slatedbIterator, err := newPackContributionIterator(slatedb)
	if err != nil {
		return nil, nil, err
	}
	defer func() { err = errors.Join(err, slatedbIterator.close()) }()
	legacySummary, hasLegacy, err := nextPackContribution(legacyIterator)
	if err != nil {
		return nil, nil, err
	}
	slatedbSummary, hasSlateDB, err := slatedbIterator.next()
	if err != nil {
		return nil, nil, err
	}
	reportMissingLegacyPack := func(summary packContributionSummary) error {
		if _, found, err := store.Get(ctx, schema.PackKey(schema.ID(summary.id))); err != nil {
			return err
		} else if found {
			return fmt.Errorf("pack scan omitted existing pack %s", summary.id.String())
		}
		if summary.count == 0 {
			result.Warnings++
			addFinding(result, maxFindings, Finding{Kind: "catalog_only_pack", Key: summary.id.String(), Got: "zero blob locations"})
		} else {
			result.MissingPacks++
			addFinding(result, maxFindings, Finding{
				Kind: "missing_pack", Key: summary.id.String(), Want: "slatedb", Got: fmt.Sprintf("legacy blobs=%d", summary.count),
			})
		}
		return nil
	}
	err = scan(ctx, store, []byte("p:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil || parsed.Kind != schema.KeyPack {
			return fmt.Errorf("invalid pack key %q", entry.Key)
		}
		record, err := schema.UnmarshalPackRecord(entry.Value)
		if err != nil {
			return err
		}
		id := vaultic.ID(parsed.ID)
		if err := accumulator.Add(record); err != nil {
			return err
		}
		checkPackRecordState(id, record, result, maxFindings)
		for hasLegacy && bytes.Compare(legacySummary.id[:], id[:]) < 0 {
			if err := reportMissingLegacyPack(legacySummary); err != nil {
				return err
			}
			legacySummary, hasLegacy, err = nextPackContribution(legacyIterator)
			if err != nil {
				return err
			}
		}
		if compareLegacy {
			if !hasLegacy || legacySummary.id != id {
				result.MissingPacks++
				addFinding(result, maxFindings, Finding{Kind: "missing_pack", Key: id.String(), Want: "legacy"})
			} else {
				legacySummary, hasLegacy, err = nextPackContribution(legacyIterator)
				if err != nil {
					return err
				}
			}
		}
		for hasSlateDB && bytes.Compare(slatedbSummary.id[:], id[:]) < 0 {
			slatedbSummary, hasSlateDB, err = slatedbIterator.next()
			if err != nil {
				return err
			}
		}
		var stats packContributionSummary
		if hasSlateDB && slatedbSummary.id == id {
			stats = slatedbSummary
			slatedbSummary, hasSlateDB, err = slatedbIterator.next()
			if err != nil {
				return err
			}
		}
		if record.BlobCount != 0 || stats.types != 0 {
			actualType := classifyPackSummary(stats.types)
			if record.BlobCount != stats.count || record.PayloadSize != stats.payload || record.Type != actualType {
				result.InvalidPacks++
				addFinding(result, maxFindings, Finding{
					Kind: "pack_metadata_mismatch", Key: id.String(),
					Want: fmt.Sprintf("type=%d blobs=%d payload=%d", actualType, stats.count, stats.payload),
					Got:  fmt.Sprintf("type=%d blobs=%d payload=%d", record.Type, record.BlobCount, record.PayloadSize),
				})
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	for hasLegacy {
		if err := reportMissingLegacyPack(legacySummary); err != nil {
			return nil, nil, err
		}
		legacySummary, hasLegacy, err = nextPackContribution(legacyIterator)
		if err != nil {
			return nil, nil, err
		}
	}
	want, wantTiers := accumulator.Results(0)
	return want, wantTiers, nil
}

func loadSlateDBLocations(ctx context.Context, store Store, result, packs *locationSpool, workers uint) error {
	group, groupContext := errgroup.WithContext(ctx)
	group.SetLimit(int(workers))
	partitionMemory := max(result.memoryBytes/256, locationTupleMemorySize)
	locationPartitions := make([]*locationSpool, 256)
	packPartitions := make([]*locationSpool, 256)
	defer func() {
		for partition := range locationPartitions {
			if locationPartitions[partition] != nil {
				_ = locationPartitions[partition].close()
			}
			if packPartitions[partition] != nil {
				_ = packPartitions[partition].close()
			}
		}
	}()
	for partition := 0; partition < 256; partition++ {
		prefix := []byte{'b', ':', byte(partition)}
		group.Go(func() error {
			locations, err := newLocationSpool(groupContext, result.scratch, partitionMemory, result.fanIn)
			if err != nil {
				return err
			}
			locationPartitions[partition] = locations
			packLocations, err := newLocationMultisetSpool(groupContext, packs.scratch, partitionMemory, packs.fanIn)
			if err != nil {
				return err
			}
			packPartitions[partition] = packLocations
			return scanRange(groupContext, store, prefix, scanPageSize, func(entries []daemon.KeyValue) error {
				for _, entry := range entries {
					parsed, err := schema.ParseKey(entry.Key)
					if err != nil {
						return err
					}
					record, err := schema.UnmarshalBlobRecord(entry.Value)
					if err != nil {
						return err
					}
					for _, item := range record.Locations {
						if err := locations.add(locationTuple{BlobID: vaultic.ID(parsed.ID),
							PackID:             vaultic.ID(item.PackID),
							Type:               uint8(item.Type),
							Offset:             item.Offset,
							Length:             uint64(item.Length),
							UncompressedLength: uint64(item.UncompressedSize)}); err != nil {
							return err
						}
						if err := packLocations.add(locationTuple{
							BlobID: vaultic.ID(item.PackID), PackID: vaultic.ID(parsed.ID), Type: uint8(item.Type),
							Offset: item.Offset, Length: uint64(item.Length), UncompressedLength: uint64(item.UncompressedSize),
						}); err != nil {
							return err
						}
					}
				}
				return nil
			})
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	for partition := range locationPartitions {
		if err := result.adopt(locationPartitions[partition]); err != nil {
			return err
		}
		if err := packs.adopt(packPartitions[partition]); err != nil {
			return err
		}
	}
	return nil
}

func checkAggregates(
	ctx context.Context,
	store Store,
	packs map[vaultic.ID]schema.PackRecord,
	result *CheckResult,
	maxFindings uint,
) error {
	records := make([]schema.PackRecord, 0, len(packs))
	for _, record := range packs {
		records = append(records, record)
	}
	want, err := schema.RebuildPackAggregates(records, 0)
	if err != nil {
		return err
	}
	wantTiers, err := schema.RebuildTierAggregates(records, 0)
	if err != nil {
		return err
	}
	return checkAggregateValues(ctx, store, want, wantTiers, result, maxFindings)
}

func checkAggregateValues(
	ctx context.Context,
	store Store,
	want map[schema.AggregateKind]schema.PackAggregate,
	wantTiers map[schema.PackTier]schema.PackAggregate,
	result *CheckResult,
	maxFindings uint,
) error {
	targets := aggregateTargets(want, wantTiers)
	stored := make([]schema.PackAggregate, len(targets))
	found := make([]bool, len(targets))
	malformed := make([]error, len(targets))
	for index, target := range targets {
		value, ok, getErr := store.Get(ctx, target.key)
		if getErr != nil {
			return getErr
		}
		if ok {
			stored[index], getErr = schema.UnmarshalPackAggregate(value)
			if getErr != nil {
				if !errors.Is(getErr, schema.ErrMalformed) {
					return getErr
				}
				malformed[index] = getErr
			}
		}
		found[index] = ok
	}
	for index, target := range targets {
		if malformed[index] != nil {
			result.AggregateMismatch++
			addFinding(
				result,
				maxFindings,
				Finding{Kind: "aggregate_drift", Key: target.delta.Key, Got: malformed[index].Error()},
			)
			continue
		}
		expected, got := target.expected, stored[index]
		got.UpdateSequence, expected.UpdateSequence = 0, 0
		if found[index] && got == expected {
			continue
		}
		// An absent tier record is a pending rebuild rather than drift: the
		// repository may predate the tier dimension, and the dimension is an
		// accelerator that index check can rebuild. A tier record that exists
		// but disagrees with the catalog is real drift and is reported below.
		if target.optional && !found[index] {
			if !emptyAggregate(expected) {
				result.TierAggregatesUnbuilt = true
			}
			continue
		}
		result.AggregateMismatch++
		addFinding(
			result,
			maxFindings,
			Finding{
				Kind: "aggregate_drift",
				Key:  target.delta.Key,
				Want: fmt.Sprintf("%+v", expected),
				Got:  fmt.Sprintf("%+v", got),
			},
		)
	}
	return nil
}

// checkPackHistory reports unreadable history records. A corrupt or missing
// history record must never change the check's verdict: history is derived and
// advisory, and a gap in it is not repository damage.
func checkPackHistory(ctx context.Context, store Store, result *CheckResult) {
	scanned, err := ScanHistory(ctx, store, 0, 0)
	if err != nil {
		return
	}
	result.HistoryEventsMalformed = scanned.Malformed
}

// checkPackLifetime reports how much of the catalog carries trustworthy tier
// and lifetime facts. These are counts rather than findings: packs inherited
// from a legacy import are legitimately tier-unknown and retention-unknown
// forever, so they must neither fail an otherwise clean check nor crowd out
// real findings on a repository with millions of packs.
func checkPackRecordState(id vaultic.ID, record schema.PackRecord, result *CheckResult, maxFindings uint) {
	if record.Tier == 0 || record.Tier == schema.TierUnknown {
		result.UnknownTierPacks++
	}
	if record.RetentionSource == 0 || record.RetentionSource == schema.RetentionUnknown {
		result.RetentionUnknownPacks++
	}
	if !record.UsageKnown {
		result.UsageUnaccountedPacks++
	}
	switch record.Type {
	case schema.PackMixed:
		result.MixedPacks++
	case schema.PackUnknown:
		result.UnknownPacks++
		result.Warnings++
		addFinding(result, maxFindings, Finding{Kind: "unknown_pack_type", Key: id.String()})
	case schema.PackData, schema.PackTree:
	}
	if record.Lifecycle == schema.PackImported || record.Lifecycle == schema.PackExportPending {
		result.PendingExports++
		result.Warnings++
	}
}

func nextAggregateSequence(ctx context.Context, store Store) (uint64, error) {
	var maximum uint64
	keys := make([][]byte, 0, typeAggregateCount+len(schema.TierAggregateKinds()))
	for kind := schema.AggregateData; kind <= schema.AggregateAll; kind++ {
		keys = append(keys, schema.PackAggregateKey(kind))
	}
	// The tier dimension is maintained incrementally too, so its sequence can
	// already exceed the type dimension's. Ignoring it would let a rebuild
	// write a sequence lower than one already published.
	for _, tier := range schema.TierAggregateKinds() {
		keys = append(keys, schema.TierAggregateKey(tier))
	}
	for _, key := range keys {
		value, found, err := store.Get(ctx, key)
		if err != nil {
			return 0, err
		}
		if found {
			record, decodeErr := schema.UnmarshalPackAggregate(value)
			if decodeErr != nil {
				if errors.Is(decodeErr, schema.ErrMalformed) {
					continue
				}
				return 0, decodeErr
			}
			maximum = max(maximum, record.UpdateSequence)
		}
	}
	if maximum == math.MaxUint64 {
		return 0, fmt.Errorf("pack aggregate update sequence overflow")
	}
	return maximum + 1, nil
}

func scan(ctx context.Context, store Store, prefix []byte, visit func(daemon.KeyValue) error) error {
	var after []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, prefix, after, scanPageSize)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := visit(entry); err != nil {
				return err
			}
			after = append(after[:0], entry.Key...)
		}
		if done {
			return nil
		}
		if len(entries) == 0 {
			return fmt.Errorf("scan %q made no progress", prefix)
		}
	}
}

func loadExportProvenance(ctx context.Context, store Store) (map[vaultic.ID]uint64, error) {
	byPack := make(map[vaultic.ID]uint64)
	err := scan(ctx, store, []byte("meta:export-index:"), func(entry daemon.KeyValue) error {
		record, err := schema.UnmarshalExportIndexCheckpointRecord(entry.Value)
		if err != nil {
			return err
		}
		for _, packID := range record.PackIDs {
			id := vaultic.ID(packID)
			byPack[id] = max(byPack[id], record.Sequence)
		}
		return nil
	})
	return byPack, err
}

func checkExportProvenance(
	ctx context.Context,
	source LegacySource,
	store Store,
	result *CheckResult,
	maxFindings uint,
) error {
	return scan(ctx, store, []byte("meta:export-index:"), func(entry daemon.KeyValue) error {
		parsed, err := schema.ParseKey(entry.Key)
		if err != nil {
			return err
		}
		record, err := schema.UnmarshalExportIndexCheckpointRecord(entry.Value)
		if err != nil {
			return err
		}
		result.ExportCheckpoints++
		indexID := vaultic.ID(parsed.ID)
		encoded, err := source.LoadUnpacked(ctx, vaultic.IndexFile, indexID)
		if err != nil {
			result.FailedExports++
			addFinding(result, maxFindings, Finding{Kind: "stale_export", Key: indexID.String(), Got: err.Error()})
			return nil
		}
		index, err := legacyindex.DecodeIndex(encoded, indexID)
		if err != nil {
			result.FailedExports++
			addFinding(result, maxFindings, Finding{Kind: "hash_mismatch", Key: indexID.String(), Got: err.Error()})
			return nil
		}
		actualPackIDs := make([]schema.ID, 0, len(record.PackIDs))
		for packID := range index.Packs() {
			actualPackIDs = append(actualPackIDs, schema.ID(packID))
		}
		sort.Slice(
			actualPackIDs,
			func(left, right int) bool { return bytes.Compare(actualPackIDs[left][:], actualPackIDs[right][:]) < 0 },
		)
		if !slices.Equal(actualPackIDs, record.PackIDs) {
			result.FailedExports++
			addFinding(
				result,
				maxFindings,
				Finding{
					Kind: "stale_export",
					Key:  indexID.String(),
					Want: fmt.Sprintf("packs=%x", record.PackIDs),
					Got:  fmt.Sprintf("packs=%x", actualPackIDs),
				},
			)
			return nil
		}
		for _, packID := range record.PackIDs {
			id := vaultic.ID(packID)
			if _, found, err := store.Get(ctx, schema.PackKey(packID)); err != nil {
				return err
			} else if !found {
				result.FailedExports++
				addFinding(
					result,
					maxFindings,
					Finding{Kind: "stale_export", Key: indexID.String(), Got: "missing pack " + id.String()},
				)
			}
		}
		return nil
	})
}

func sortedPackIDs(packs map[vaultic.ID]schema.PackRecord) []vaultic.ID {
	ids := make([]vaultic.ID, 0, len(packs))
	for id := range packs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return bytes.Compare(ids[left][:], ids[right][:]) < 0 })
	return ids
}

func addFinding(result *CheckResult, maximum uint, finding Finding) {
	index, _ := slices.BinarySearchFunc(result.Findings, finding, compareFinding)
	if maximum == 0 || uint(len(result.Findings)) < maximum {
		result.Findings = slices.Insert(result.Findings, index, finding)
		return
	}
	if index < len(result.Findings) {
		result.Findings = slices.Insert(result.Findings, index, finding)
		result.Findings = result.Findings[:maximum]
	}
}

func compareFinding(left, right Finding) int {
	if compared := strings.Compare(left.Kind, right.Kind); compared != 0 {
		return compared
	}
	if compared := strings.Compare(left.Key, right.Key); compared != 0 {
		return compared
	}
	if compared := strings.Compare(left.Want, right.Want); compared != 0 {
		return compared
	}
	return strings.Compare(left.Got, right.Got)
}
