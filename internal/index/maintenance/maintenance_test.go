package maintenance

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type memoryStore struct {
	values      map[string][]byte
	batchWrites int
}

func TestCheckResultTreatsQuorumBypassAsDirty(t *testing.T) {
	result := CheckResult{QuorumChecked: true, QuorumNonCompliant: true}
	if result.Clean() {
		t.Fatal("quorum bypass was reported as a clean index check")
	}
}
type auditedMemoryStore struct {
	*memoryStore
	audit daemon.EncryptionAudit
}

func (store *auditedMemoryStore) CheckEncryption(context.Context) (daemon.EncryptionAudit, error) {
	return store.audit, nil
}

func TestCheckReportsEncryptionIntegrityAndRewriteDebt(t *testing.T) {
	store, _, _ := newMemoryStore(t, schema.PackPublished)
	audited := &auditedMemoryStore{memoryStore: store, audit: daemon.EncryptionAudit{
		Enabled:            true,
		Objects:            12,
		PlaintextObjects:   2,
		InvalidObjects:     1,
		OldVersionObjects:  3,
		EnvelopeGeneration: 4,
		ActiveDEKVersion:   2,
		Algorithm:          "AES-256-GCM",
	}}
	result, err := CheckWithOptions(context.Background(), nil, audited, CheckOptions{SlateDBOnly: true, MaxFindings: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !result.EncryptionEnabled || result.EncryptedObjects != 10 || result.EnvelopeGeneration != 4 || result.ActiveDEKVersion != 2 || result.Clean() || !result.HasWarnings() {
		t.Fatalf("encryption audit was not reflected in check result: %+v", result)
	}
	wantKinds := []string{"metadata_object_plaintext", "metadata_encryption_invalid", "metadata_dek_rewrite_pending"}
	for index, kind := range wantKinds {
		if len(result.Findings) <= index || result.Findings[index].Kind != kind {
			t.Fatalf("missing encryption finding %q: %+v", kind, result.Findings)
		}
	}
}

func TestCheckVerificationStateDetectsProjectionDrift(t *testing.T) {
	ctx := context.Background()
	store, packID, _ := newMemoryStore(t, schema.PackPublished)
	pack := schema.ID(packID)
	backend := uint64(7)
	placement := schema.PlacementRecord{State: schema.PlacementLive, Bytes: 10, LastVerifiedAt: 100, RetentionSource: schema.RetentionUnknown}
	state := schema.VerificationStateRecord{LastAttemptAt: 100, LastAttemptLevel: schema.VerificationChecksum, HeaderVerifiedAt: 100, ChecksumVerifiedAt: 100, Result: schema.VerificationHealthy, LastRunID: schema.ID{1}}
	store.set(t, schema.PackPlacementKey(pack, backend), placement)
	store.set(t, schema.VerificationStateKey(pack, backend), state)
	result := CheckResult{}
	if err := checkVerificationState(ctx, store, map[vaultic.ID]schema.PackRecord{packID: {}}, &result, 10); err != nil || result.VerificationStateMismatch != 0 {
		t.Fatalf("consistent verification state reported drift: %+v, %v", result, err)
	}
	placement.LastVerifiedAt = 99
	store.set(t, schema.PackPlacementKey(pack, backend), placement)
	result = CheckResult{}
	if err := checkVerificationState(ctx, store, map[vaultic.ID]schema.PackRecord{packID: {}}, &result, 10); err != nil || result.VerificationStateMismatch != 1 || result.Clean() {
		t.Fatalf("verification drift was not dirty: %+v, %v", result, err)
	}
}

func newMemoryStore(t *testing.T, lifecycle schema.PackLifecycle) (*memoryStore, vaultic.ID, vaultic.ID) {
	t.Helper()
	packID, blobID := vaultic.NewRandomID(), vaultic.NewRandomID()
	packRecord := schema.PackRecord{Type: schema.PackData, PayloadSize: 17, BlobCount: 1, Lifecycle: lifecycle}
	blobRecord := schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: schema.ID(packID), Offset: 3, Length: 17, Type: schema.BlobData}}}
	store := &memoryStore{values: make(map[string][]byte)}
	store.set(t, schema.PackKey(schema.ID(packID)), packRecord)
	store.set(t, schema.BlobKey(schema.ID(blobID)), blobRecord)
	aggregates, err := schema.RebuildPackAggregates([]schema.PackRecord{packRecord}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for kind, aggregate := range aggregates {
		store.set(t, schema.PackAggregateKey(kind), aggregate)
	}
	return store, packID, blobID
}

func (store *memoryStore) set(t *testing.T, key []byte, record interface{ MarshalBinary() ([]byte, error) }) {
	t.Helper()
	value, err := record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	store.values[string(key)] = value
}

func (store *memoryStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	value, found := store.values[string(key)]
	return append([]byte(nil), value...), found, nil
}

func (store *memoryStore) MultiGet(ctx context.Context, keys [][]byte) ([]daemon.KeyValue, []bool, error) {
	values := make([]daemon.KeyValue, len(keys))
	found := make([]bool, len(keys))
	for index, key := range keys {
		value, ok, err := store.Get(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		values[index], found[index] = daemon.KeyValue{Key: key, Value: value}, ok
	}
	return values, found, nil
}

func (store *memoryStore) ScanPrefix(_ context.Context, prefix, after []byte, limit uint32) ([]daemon.KeyValue, bool, error) {
	keys := make([]string, 0)
	for key := range store.values {
		if bytes.HasPrefix([]byte(key), prefix) && (len(after) == 0 || key > string(after)) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	done := len(keys) <= int(limit)
	if !done {
		keys = keys[:limit]
	}
	entries := make([]daemon.KeyValue, len(keys))
	for index, key := range keys {
		entries[index] = daemon.KeyValue{Key: []byte(key), Value: append([]byte(nil), store.values[key]...)}
	}
	return entries, done, nil
}

func (store *memoryStore) MarkPackPublished(_ context.Context, id schema.ID) error {
	key := schema.PackKey(id)
	value, found := store.values[string(key)]
	if !found {
		return fmt.Errorf("pack missing")
	}
	record, err := schema.UnmarshalPackRecord(value)
	if err != nil {
		return err
	}
	record.Lifecycle = schema.PackPublished
	encoded, err := record.MarshalBinary()
	if err == nil {
		store.values[string(key)] = encoded
	}
	return err
}

func (store *memoryStore) MarkIndexPublished(ctx context.Context, indexID schema.ID, packIDs []schema.ID) (uint64, error) {
	for _, id := range packIDs {
		if err := store.MarkPackPublished(ctx, id); err != nil {
			return 0, err
		}
	}
	sequence := uint64(1)
	if value, found := store.values[string(schema.NextExportSequenceKey())]; found {
		var err error
		sequence, err = schema.UnmarshalNextExportSequence(value)
		if err != nil {
			return 0, err
		}
	}
	checkpoint := schema.ExportIndexCheckpointRecord{Sequence: sequence, PackIDs: append([]schema.ID(nil), packIDs...)}
	sort.Slice(checkpoint.PackIDs, func(left, right int) bool {
		return bytes.Compare(checkpoint.PackIDs[left][:], checkpoint.PackIDs[right][:]) < 0
	})
	encoded, err := checkpoint.MarshalBinary()
	if err != nil {
		return 0, err
	}
	store.values[string(schema.ExportIndexCheckpointKey(indexID))] = encoded
	next, _ := schema.MarshalNextExportSequence(sequence + 1)
	store.values[string(schema.NextExportSequenceKey())] = next
	return sequence, nil
}

func (store *memoryStore) WriteMutableBatch(_ context.Context, puts []daemon.Mutation, deletes [][]byte, _ bool) error {
	store.batchWrites++
	for _, put := range puts {
		store.values[string(put.Key)] = append([]byte(nil), put.Value...)
	}
	for _, key := range deletes {
		delete(store.values, string(key))
	}
	return nil
}

func TestCheckAnalyticsConsistency(t *testing.T) {
	store, _, _ := newMemoryStore(t, schema.PackPublished)
	store.set(t, schema.AnalyticsMetadataKey(), schema.AnalyticsMetadataRecord{Enabled: false})
	result, err := CheckWithOptions(context.Background(), nil, store, CheckOptions{SlateDBOnly: true, MaxFindings: 1})
	if err != nil || result.AnalyticsMismatch != 0 {
		t.Fatalf("disabled analytics produced findings: %+v, %v", result, err)
	}

	generation := uint64(1)
	store.set(t, schema.AnalyticsMetadataKey(), schema.AnalyticsMetadataRecord{Enabled: true, Generation: generation, Facts: 1, BuiltAt: time.Now().UnixNano()})
	store.set(t, schema.AnalyticsManifestKey(generation), schema.AnalyticsManifestRecord{Generation: generation, Segments: []uint64{1}})
	store.set(t, schema.AnalyticsWatermarkKey(generation), schema.AnalyticsWatermarkRecord{RepositoryGeneration: generation, ManifestGeneration: generation, AppliedAt: time.Now().UnixNano()})
	store.values[string(schema.AnalyticsDerivedGenerationMarkerKey(generation))] = []byte{schema.Version}
	result, err = CheckWithOptions(context.Background(), nil, store, CheckOptions{SlateDBOnly: true, MaxFindings: 1})
	if err != nil || result.AnalyticsMismatch != 2 || result.Clean() || len(result.Findings) != 1 || result.Findings[0].Kind != "analytics_segment_pair_missing" {
		t.Fatalf("missing analytics segment not reported with finding cap: %+v, %v", result, err)
	}
}

type memoryDestination struct {
	indexes   map[vaultic.ID][]byte
	snapshots map[vaultic.ID][]byte
}

func (destination *memoryDestination) SaveLegacyIndex(_ context.Context, index *legacyindex.Index) (vaultic.ID, error) {
	var buffer bytes.Buffer
	if err := index.Encode(&buffer); err != nil {
		return vaultic.ID{}, err
	}
	id := vaultic.Hash(buffer.Bytes())
	if destination.indexes == nil {
		destination.indexes = make(map[vaultic.ID][]byte)
	}
	destination.indexes[id] = append([]byte(nil), buffer.Bytes()...)
	return id, nil
}

func (destination *memoryDestination) Connections() uint { return 1 }
func (destination *memoryDestination) List(_ context.Context, fileType vaultic.FileType, fn func(vaultic.ID, int64) error) error {
	values := destination.indexes
	if fileType == vaultic.SnapshotFile {
		values = destination.snapshots
	} else if fileType != vaultic.IndexFile {
		return nil
	}
	for id, value := range values {
		if err := fn(id, int64(len(value))); err != nil {
			return err
		}
	}
	return nil
}
func (destination *memoryDestination) LoadUnpacked(_ context.Context, _ vaultic.FileType, id vaultic.ID) ([]byte, error) {
	value, found := destination.indexes[id]
	if !found {
		return nil, fmt.Errorf("index missing")
	}
	return append([]byte(nil), value...), nil
}

func TestExportIsDeterministicCheckpointedAndResumable(t *testing.T) {
	store, packID, _ := newMemoryStore(t, schema.PackImported)
	dryDestination := &memoryDestination{}
	dryRun, err := Export(context.Background(), store, dryDestination, ExportOptions{DryRun: true})
	if err != nil || dryRun.PacksSelected != 1 || dryRun.IndexesWritten != 0 || len(dryDestination.indexes) != 0 {
		t.Fatalf("dry-run export = %#v, indexes=%d, err=%v", dryRun, len(dryDestination.indexes), err)
	}
	dryValue, _, _ := store.Get(context.Background(), schema.PackKey(schema.ID(packID)))
	dryRecord, err := schema.UnmarshalPackRecord(dryValue)
	if err != nil || dryRecord.Lifecycle != schema.PackImported {
		t.Fatalf("dry-run pack checkpoint = %#v, %v", dryRecord, err)
	}
	first := &memoryDestination{}
	result, err := Export(context.Background(), store, first, ExportOptions{PacksPerIndex: 1, Verify: true})
	if err != nil || result.PacksSelected != 1 || result.BlobsSelected != 1 || result.IndexesWritten != 1 {
		t.Fatalf("first export = %#v, %v", result, err)
	}
	value, _, _ := store.Get(context.Background(), schema.PackKey(schema.ID(packID)))
	record, err := schema.UnmarshalPackRecord(value)
	if err != nil || record.Lifecycle != schema.PackPublished {
		t.Fatalf("pack checkpoint = %#v, %v", record, err)
	}
	resumed, err := Export(context.Background(), store, &memoryDestination{}, ExportOptions{})
	if err != nil || resumed.PacksSelected != 0 || resumed.IndexesWritten != 0 {
		t.Fatalf("resumed export = %#v, %v", resumed, err)
	}
	second := &memoryDestination{}
	full, err := Export(context.Background(), store, second, ExportOptions{Full: true})
	if err != nil || full.IndexesWritten != 1 || len(result.IndexIDs) != 1 || len(full.IndexIDs) != 1 || result.IndexIDs[0] != full.IndexIDs[0] {
		t.Fatalf("full export = %#v, %v; first = %#v", full, err, result)
	}
}

func TestMaintenanceRejectsMalformedPackCatalog(t *testing.T) {
	store := &memoryStore{values: map[string][]byte{string(schema.PackKey(schema.ID(vaultic.NewRandomID()))): {0}}}
	if _, err := Export(context.Background(), store, &memoryDestination{}, ExportOptions{}); err == nil {
		t.Fatal("export accepted malformed pack record")
	}
	if _, err := RebuildPackAggregates(context.Background(), store, false); err == nil {
		t.Fatal("aggregate rebuild accepted malformed pack record")
	}
}

func TestMalformedAggregateIsReportedAndRepaired(t *testing.T) {
	store, _, _ := newMemoryStore(t, schema.PackImported)
	destination := &memoryDestination{}
	if _, err := Export(context.Background(), store, destination, ExportOptions{Full: true}); err != nil {
		t.Fatal(err)
	}
	key := schema.PackAggregateKey(schema.AggregateAll)
	store.values[string(key)] = []byte{0}
	result, err := Check(context.Background(), destination, store, 10)
	if err != nil || result.AggregateMismatch != 1 || result.Clean() {
		t.Fatalf("malformed aggregate check = %#v, %v", result, err)
	}
	rebuilt, err := RebuildPackAggregates(context.Background(), store, false)
	if err != nil || rebuilt.AggregatesChanged == 0 {
		t.Fatalf("malformed aggregate rebuild = %#v, %v", rebuilt, err)
	}
	if result, err = Check(context.Background(), destination, store, 10); err != nil || !result.Clean() {
		t.Fatalf("check after malformed repair = %#v, %v", result, err)
	}
}

func TestCheckFindsLocationAndAggregateDrift(t *testing.T) {
	store, _, _ := newMemoryStore(t, schema.PackImported)
	destination := &memoryDestination{}
	if _, err := Export(context.Background(), store, destination, ExportOptions{Full: true}); err != nil {
		t.Fatal(err)
	}
	clean, err := Check(context.Background(), destination, store, 10)
	if err != nil || !clean.Clean() {
		t.Fatalf("clean check = %#v, %v", clean, err)
	}
	for key := range store.values {
		if bytes.HasPrefix([]byte(key), []byte("b:")) {
			delete(store.values, key)
			break
		}
	}
	corrupt := schema.PackAggregate{PackCount: 99, UpdateSequence: 2}
	store.set(t, schema.PackAggregateKey(schema.AggregateAll), corrupt)
	drift, err := Check(context.Background(), destination, store, 1)
	if err != nil || drift.MissingInSlateDB != 1 || drift.AggregateMismatch != 1 || len(drift.Findings) != 1 || drift.Clean() {
		t.Fatalf("drift check = %#v, %v", drift, err)
	}
}

func TestCheckTreatsUnresolvedImportedMetadataAsWarnings(t *testing.T) {
	store, _, blobID := newMemoryStore(t, schema.PackPublished)
	source := &memoryDestination{}
	if _, err := Export(context.Background(), store, source, ExportOptions{Full: true}); err != nil {
		t.Fatal(err)
	}
	store.set(t, schema.ReverseInodeKey(schema.ID(blobID), 1, 2), schema.ReverseInodeRecord{LatestRevision: 1, State: schema.ReferenceUnresolved})
	snapshotID := vaultic.NewRandomID()
	store.set(t, schema.SnapshotImportCheckpointKey(schema.ID(snapshotID)), schema.SnapshotImportCheckpointRecord{TreesVisited: 1, DebtsCreated: 1})
	source.snapshots = map[vaultic.ID][]byte{snapshotID: {1}}
	result, err := Check(context.Background(), source, store, 10)
	if err != nil || !result.Clean() || !result.HasWarnings() || result.UnresolvedReferences != 1 || result.UnresolvedSnapshots != 1 {
		t.Fatalf("unresolved metadata check = %#v, %v", result, err)
	}
	if len(result.Findings) != 1 || result.Findings[0].Kind != "unresolved_snapshot" {
		t.Fatalf("unresolved metadata findings = %#v", result.Findings)
	}
}

func TestCheckFindsExportPackProvenanceDrift(t *testing.T) {
	store, _, _ := newMemoryStore(t, schema.PackImported)
	source := &memoryDestination{}
	result, err := Export(context.Background(), store, source, ExportOptions{})
	if err != nil || len(result.IndexIDs) != 1 {
		t.Fatalf("export = %#v, %v", result, err)
	}
	wrongPack := schema.ID(vaultic.NewRandomID())
	store.set(t, schema.ExportIndexCheckpointKey(schema.ID(result.IndexIDs[0])), schema.ExportIndexCheckpointRecord{Sequence: 1, PackIDs: []schema.ID{wrongPack}})
	checked, err := Check(context.Background(), source, store, 10)
	if err != nil || checked.FailedExports != 1 || checked.Clean() {
		t.Fatalf("provenance drift check = %#v, %v", checked, err)
	}
	found := false
	for _, finding := range checked.Findings {
		found = found || finding.Kind == "stale_export"
	}
	if !found {
		t.Fatalf("provenance findings = %#v", checked.Findings)
	}
}

func TestRebuildPackAggregatesSupportsDryRunAndAtomicWrite(t *testing.T) {
	store, _, _ := newMemoryStore(t, schema.PackImported)
	store.set(t, schema.PackAggregateKey(schema.AggregateAll), schema.PackAggregate{PackCount: 99, UpdateSequence: 5})
	dryRun, err := RebuildPackAggregates(context.Background(), store, true)
	if err != nil || dryRun.AggregatesChanged == 0 || store.batchWrites != 0 {
		t.Fatalf("dry-run rebuild = %#v, writes=%d, err=%v", dryRun, store.batchWrites, err)
	}
	result, err := RebuildPackAggregates(context.Background(), store, false)
	if err != nil || result.AggregatesChanged == 0 || store.batchWrites != 1 {
		t.Fatalf("rebuild = %#v, writes=%d, err=%v", result, store.batchWrites, err)
	}
	value, found, err := store.Get(context.Background(), schema.PackAggregateKey(schema.AggregateAll))
	aggregate, decodeErr := schema.UnmarshalPackAggregate(value)
	if err != nil || decodeErr != nil || !found || aggregate.PackCount != 1 || aggregate.PayloadSize != 17 {
		t.Fatalf("rebuilt aggregate = %#v, found=%t, err=%v/%v", aggregate, found, err, decodeErr)
	}
	converged, err := RebuildPackAggregates(context.Background(), store, false)
	if err != nil || converged.AggregatesChanged != 0 || store.batchWrites != 1 {
		t.Fatalf("converged rebuild = %#v, writes=%d, err=%v", converged, store.batchWrites, err)
	}
	delete(store.values, string(schema.PackAggregateKey(schema.AggregateTree)))
	missing, err := Check(context.Background(), &memoryDestination{}, store, 10)
	if err != nil || missing.AggregateMismatch == 0 {
		t.Fatalf("missing aggregate check = %#v, %v", missing, err)
	}
}
