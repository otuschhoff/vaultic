package maintenance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	monitor "github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type memoryStore struct {
	values      map[string][]byte
	batchWrites int
}

type validatingMemoryStore struct {
	*memoryStore
	validateErr error
	validated   bool
}

type blockingMemoryStore struct {
	*memoryStore
	active  atomic.Int64
	maximum atomic.Int64
	entered chan struct{}
	release chan struct{}
}

type gatedScanStore struct {
	*memoryStore
	active  atomic.Int64
	maximum atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (store *blockingMemoryStore) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	active := store.active.Add(1)
	defer store.active.Add(-1)
	for {
		maximum := store.maximum.Load()
		if active <= maximum || store.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	store.entered <- struct{}{}
	select {
	case <-store.release:
		return store.memoryStore.Get(ctx, key)
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

func (store *gatedScanStore) ScanPrefix(ctx context.Context, prefix, after []byte, limit uint32) ([]daemon.KeyValue, bool, error) {
	active := store.active.Add(1)
	defer store.active.Add(-1)
	for {
		maximum := store.maximum.Load()
		if active <= maximum || store.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	store.entered <- struct{}{}
	select {
	case <-store.release:
		return store.memoryStore.ScanPrefix(ctx, prefix, after, limit)
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

func (store *validatingMemoryStore) Validate(context.Context) error {
	store.validated = true
	return store.validateErr
}

func TestCheckResultTreatsQuorumBypassAsDirty(t *testing.T) {
	result := CheckResult{QuorumChecked: true, QuorumNonCompliant: true}
	if result.Clean() {
		t.Fatal("quorum bypass was reported as a clean index check")
	}
}

func TestCheckCoverageIdentifiesReducedModes(t *testing.T) {
	full := checkCoverage(CheckOptions{})
	if !full.Complete || full.Mode != "full" || len(full.Skipped) != 0 || len(full.Included) != len(checkDomains) {
		t.Fatalf("full coverage = %+v", full)
	}
	legacy := checkCoverage(CheckOptions{LegacyOnly: true})
	if legacy.Complete || legacy.Mode != "legacy_only" || len(legacy.Included) != 2 || len(legacy.Skipped) == 0 {
		t.Fatalf("legacy coverage = %+v", legacy)
	}
	slatedb := checkCoverage(CheckOptions{SlateDBOnly: true})
	if slatedb.Complete || slatedb.Mode != "slatedb_only" || slices.Contains(slatedb.Included, "export_provenance") || !slices.Contains(slatedb.Skipped, "export_provenance") {
		t.Fatalf("slatedb coverage = %+v", slatedb)
	}
}

func TestCheckOptionsDigestIsCanonicalAndSensitive(t *testing.T) {
	left, err := checkOptionsDigest(CheckOptions{PathIndexPaths: []string{"/b", "/a"}, MemoryBytes: 1024, TempMaxBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	right, err := checkOptionsDigest(CheckOptions{PathIndexPaths: []string{"/a", "/b"}, MemoryBytes: 1024, TempMaxBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := checkOptionsDigest(CheckOptions{PathIndexPaths: []string{"/a", "/b"}, MemoryBytes: 1025, TempMaxBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	if left != right || left == changed {
		t.Fatalf("digests left=%q right=%q changed=%q", left, right, changed)
	}
}

func TestCheckRejectsMemoryBelowTupleWorkingSet(t *testing.T) {
	if _, err := CheckWithOptions(context.Background(), nil, nil, CheckOptions{LegacyOnly: true, MemoryBytes: locationTupleMemorySize - 1}); err == nil {
		t.Fatal("undersized checker memory was accepted")
	}
}

func TestCheckValidatesDeclaredReadSession(t *testing.T) {
	options := CheckOptions{SlateDBOnly: true, Consistency: CheckConsistency{SessionID: "session"}}
	plain := &memoryStore{values: map[string][]byte{}}
	if _, err := CheckWithOptions(context.Background(), nil, plain, options); err == nil {
		t.Fatal("declared read session accepted a store without validation")
	}
	validationErr := fmt.Errorf("session expired")
	validating := &validatingMemoryStore{memoryStore: plain, validateErr: validationErr}
	if _, err := CheckWithOptions(context.Background(), nil, validating, options); !errors.Is(err, validationErr) {
		t.Fatalf("validation error = %v, want %v", err, validationErr)
	}
	if !validating.validated {
		t.Fatal("read session was not validated")
	}
}

func TestLoadSlateDBLocationsScansPartitionsConcurrently(t *testing.T) {
	packID := vaultic.NewRandomID()
	store := &gatedScanStore{
		memoryStore: &memoryStore{values: make(map[string][]byte)},
		entered:     make(chan struct{}, 256),
		release:     make(chan struct{}),
	}
	for _, partition := range []byte{0x00, 0x7f, 0xff} {
		var blobID schema.ID
		blobID[0] = partition
		blobID[31] = partition + 1
		store.set(t, schema.BlobKey(blobID), schema.BlobRecord{Locations: []schema.BlobLocation{{
			PackID: schema.ID(packID), Type: schema.BlobData, Length: 1,
		}}})
	}
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	locations, err := newLocationSpool(context.Background(), scratch, 1<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	packs, err := newLocationMultisetSpool(context.Background(), scratch, 1<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- loadSlateDBLocations(context.Background(), store, locations, packs, 4)
	}()
	for range 2 {
		<-store.entered
	}
	close(store.release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	locationCount, err := countLocationSpool(locations)
	if err != nil {
		t.Fatal(err)
	}
	packCount, err := countLocationSpool(packs)
	if err != nil {
		t.Fatal(err)
	}
	if store.maximum.Load() < 2 || locationCount != 3 || packCount != 3 {
		t.Fatalf("maximum scans=%d location count=%d pack count=%d", store.maximum.Load(), locationCount, packCount)
	}
}

func TestCheckTelemetryIncludesFinalReadSessionValidation(t *testing.T) {
	validationErr := fmt.Errorf("session expired")
	store := &validatingMemoryStore{
		memoryStore: &memoryStore{values: map[string][]byte{}},
		validateErr: validationErr,
	}
	telemetry := NewCheckTelemetry()
	_, err := CheckWithOptions(context.Background(), nil, store, CheckOptions{
		SlateDBOnly: true,
		Consistency: CheckConsistency{SessionID: "session"},
		Telemetry:   telemetry,
	})
	if !errors.Is(err, validationErr) {
		t.Fatalf("validation error = %v, want %v", err, validationErr)
	}
	metrics := telemetry.Component(time.Now()).Metrics
	if checkMetricValue(metrics, "operation_completed", map[string]string{"outcome": "failure"}) != 1 || checkMetricValue(metrics, "operation_completed", map[string]string{"outcome": "success"}) != 0 {
		t.Fatalf("operation telemetry = %+v", metrics)
	}
}

func TestLimitedStoreBoundsMetadataRPCs(t *testing.T) {
	base := &blockingMemoryStore{
		memoryStore: &memoryStore{values: map[string][]byte{}},
		entered:     make(chan struct{}, 8), release: make(chan struct{}, 8),
	}
	store := &limitedStore{Store: base, semaphore: make(chan struct{}, 2)}
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, _, _ = store.Get(context.Background(), schema.PackKey(schema.ID{1}))
		}()
	}
	<-base.entered
	<-base.entered
	if maximum := base.maximum.Load(); maximum != 2 {
		t.Fatalf("maximum in-flight RPCs = %d, want 2", maximum)
	}
	for range 8 {
		base.release <- struct{}{}
	}
	workers.Wait()
	if maximum := base.maximum.Load(); maximum != 2 {
		t.Fatalf("maximum in-flight RPCs = %d after completion, want 2", maximum)
	}
}

func TestLimitedStoreTelemetryClassifiesAdmissionOutcomes(t *testing.T) {
	telemetry := NewCheckTelemetry()
	store := &limitedStore{
		Store:     &memoryStore{values: map[string][]byte{}},
		semaphore: make(chan struct{}, 1),
		telemetry: telemetry,
	}
	store.semaphore <- struct{}{}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.acquire(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquire error = %v", err)
	}
	deadline, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelDeadline()
	if err := store.acquire(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline acquire error = %v", err)
	}
	<-store.semaphore
	if err := store.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-store.semaphore

	snapshot := telemetry.rpcWait.Snapshot()
	if snapshot.Attempts != 3 || snapshot.Contentions != 2 || snapshot.Active != 0 || snapshot.Completed[0] != 1 || snapshot.Completed[2] != 1 || snapshot.Completed[3] != 1 {
		t.Fatalf("wait snapshot = %+v", snapshot)
	}
}

func TestCheckTelemetryComponentAndResultEquivalence(t *testing.T) {
	store, _, _ := newMemoryStore(t, schema.PackPublished)
	store.set(t, schema.AnalyticsMetadataKey(), schema.AnalyticsMetadataRecord{Enabled: false})
	options := CheckOptions{SlateDBOnly: true, MaxFindings: 10, Workers: 1, RPCConcurrency: 1}
	want, err := CheckWithOptions(context.Background(), nil, store, options)
	if err != nil {
		t.Fatal(err)
	}

	telemetry := NewCheckTelemetry()
	options.Telemetry = telemetry
	got, err := CheckWithOptions(context.Background(), nil, store, options)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("enabled telemetry changed result\ngot:  %+v\nwant: %+v", got, want)
	}
	component := telemetry.Component(time.Now())
	if err := monitor.ValidateVaulticDBComponent(component); err != nil {
		t.Fatal(err)
	}
	if len(component.Operations) != 0 || checkMetricValue(component.Metrics, "operation_started", nil) != 1 || checkMetricValue(component.Metrics, "operation_completed", map[string]string{"outcome": "success"}) != 1 {
		t.Fatalf("operation telemetry = %+v", component)
	}
	if checkMetricValue(component.Metrics, "dependency_requests", map[string]string{"role": "database", "outcome": "success"}) == 0 || checkMetricValue(component.Metrics, "dependency_bytes", map[string]string{"role": "database", "outcome": "success"}) == 0 {
		t.Fatalf("database telemetry = %+v", component.Metrics)
	}
}

func TestCheckTelemetrySupportsConcurrentChecks(t *testing.T) {
	telemetry := NewCheckTelemetry()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	options := CheckOptions{
		SlateDBOnly: true,
		Telemetry:   telemetry,
		Progress: func(update CheckProgress) {
			if update.Stage == "inventory" {
				entered <- struct{}{}
				<-release
			}
		},
	}
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if _, err := CheckWithOptions(context.Background(), nil, &memoryStore{values: map[string][]byte{}}, options); err != nil {
				t.Errorf("check: %v", err)
			}
		}()
	}
	<-entered
	<-entered
	close(release)
	workers.Wait()
	component := telemetry.Component(time.Now())
	if len(component.Operations) != 0 || checkMetricValue(component.Metrics, "operation_started", nil) != 2 || checkMetricValue(component.Metrics, "operation_completed", map[string]string{"outcome": "success"}) != 2 {
		t.Fatalf("concurrent operation telemetry = %+v", component)
	}
}

func TestCheckScratchTelemetryAccountsDurableRunWrites(t *testing.T) {
	telemetry := NewCheckTelemetry()
	scratch, err := newCheckScratch(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	scratch.telemetry = telemetry
	spool := &locationSpool{ctx: context.Background(), scratch: scratch}
	run, err := spool.writeRun([]locationTuple{{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := scratch.close(); err != nil {
		t.Fatal(err)
	}
	metrics := telemetry.Component(time.Now()).Metrics
	if checkMetricValue(metrics, "dependency_requests", map[string]string{"role": "scratch", "outcome": "success"}) < 1 || checkMetricValue(metrics, "dependency_bytes", map[string]string{"role": "scratch", "outcome": "success"}) < run.size {
		t.Fatalf("scratch telemetry = %+v", metrics)
	}
}

func TestCheckScratchTelemetryDoesNotSucceedCanceledRun(t *testing.T) {
	telemetry := NewCheckTelemetry()
	scratch, err := newCheckScratch(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	scratch.telemetry = telemetry
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	spool := &locationSpool{ctx: ctx, scratch: scratch}
	if _, err := spool.writeRun([]locationTuple{{}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("write error = %v, want cancellation", err)
	}
	if err := scratch.close(); err != nil {
		t.Fatal(err)
	}
	metrics := telemetry.Component(time.Now()).Metrics
	if checkMetricValue(metrics, "dependency_requests", map[string]string{"role": "scratch", "outcome": "cancellation"}) != 1 || checkMetricValue(metrics, "dependency_bytes", map[string]string{"role": "scratch", "outcome": "cancellation"}) != checkRunHeaderSize || checkMetricValue(metrics, "dependency_requests", map[string]string{"role": "scratch", "outcome": "failure"}) != 0 {
		t.Fatalf("scratch telemetry = %+v", metrics)
	}
}

func TestCheckScratchTelemetryAccountsAnalyticsRunWrites(t *testing.T) {
	telemetry := NewCheckTelemetry()
	scratch, err := newCheckScratch(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	scratch.telemetry = telemetry
	spool, err := newCheckKVSpool(context.Background(), scratch, 128, 2)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := spool.newWriter()
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.append(checkKVRecord{key: []byte("key"), value: []byte("value")}); err != nil {
		writer.abort()
		t.Fatal(err)
	}
	run, err := writer.close()
	if err != nil {
		t.Fatal(err)
	}
	before := telemetry.Component(time.Now()).Metrics
	iterator, err := newCheckKVIterator(context.Background(), []checkRun{run}, scratch, 128)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := iterator.next(); err != nil || !found {
		t.Fatalf("read analytics run: found=%t err=%v", found, err)
	}
	if err := iterator.close(); err != nil {
		t.Fatal(err)
	}
	if err := scratch.close(); err != nil {
		t.Fatal(err)
	}
	metrics := telemetry.Component(time.Now()).Metrics
	if checkMetricValue(metrics, "dependency_requests", map[string]string{"role": "scratch", "outcome": "success"}) < 1 || checkMetricValue(metrics, "dependency_bytes", map[string]string{"role": "scratch", "outcome": "success"}) < run.size {
		t.Fatalf("analytics scratch telemetry = %+v", metrics)
	}
	if checkMetricValue(metrics, "dependency_requests", map[string]string{"role": "scratch", "outcome": "success"}) <= checkMetricValue(before, "dependency_requests", map[string]string{"role": "scratch", "outcome": "success"}) || checkMetricValue(metrics, "dependency_bytes", map[string]string{"role": "scratch", "outcome": "success"}) <= checkMetricValue(before, "dependency_bytes", map[string]string{"role": "scratch", "outcome": "success"}) {
		t.Fatalf("analytics scratch reads were not accounted: before=%+v after=%+v", before, metrics)
	}
}

type partialErrorWriter struct{ err error }

func (writer partialErrorWriter) Write(value []byte) (int, error) {
	return min(3, len(value)), writer.err
}

func TestCheckScratchTelemetryCountsPartialFailedWrite(t *testing.T) {
	telemetry := NewCheckTelemetry()
	scratch := &checkScratch{telemetry: telemetry}
	request := telemetry.startScratch()
	writeErr := errors.New("partial write")
	err := writeScratch(scratch, request, partialErrorWriter{err: writeErr}, []byte("abcdef"))
	settleDependency(request, err)
	if !errors.Is(err, writeErr) {
		t.Fatalf("write error = %v, want %v", err, writeErr)
	}
	metrics := telemetry.Component(time.Now()).Metrics
	if checkMetricValue(metrics, "dependency_requests", map[string]string{"role": "scratch", "outcome": "failure"}) != 1 || checkMetricValue(metrics, "dependency_bytes", map[string]string{"role": "scratch", "outcome": "failure"}) != 3 {
		t.Fatalf("partial scratch telemetry = %+v", metrics)
	}
}

func checkMetricValue(metrics []monitor.Metric, name string, labels map[string]string) uint64 {
	for _, metric := range metrics {
		if metric.Name != name {
			continue
		}
		matched := true
		for name, value := range labels {
			if !slices.Contains(metric.Labels, monitor.Label{Name: name, Value: value}) {
				matched = false
				break
			}
		}
		if matched {
			return metric.Value
		}
	}
	return 0
}

func TestCheckProgressReportsStagesAndEffectiveLimits(t *testing.T) {
	store := &memoryStore{values: map[string][]byte{}}
	var updates []CheckProgress
	result, err := CheckWithOptions(context.Background(), nil, store, CheckOptions{
		SlateDBOnly: true, Workers: 2, RPCConcurrency: 3,
		Progress: func(update CheckProgress) { updates = append(updates, update) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Resources.Workers != 2 || result.Resources.RPCConcurrency != 3 {
		t.Fatalf("resources = %+v", result.Resources)
	}
	if len(updates) == 0 || updates[0].Stage != "inventory" || updates[len(updates)-1].Stage != "finalization" {
		t.Fatalf("progress updates = %+v", updates)
	}
	for _, update := range updates {
		if update.Workers != 2 || update.RPCConcurrency != 3 || update.MemoryLimitBytes == 0 || update.ScratchLimitBytes == 0 {
			t.Fatalf("invalid progress update: %+v", update)
		}
	}
}

func TestCheckWorkerCountsPreserveResults(t *testing.T) {
	store, _, _ := newMemoryStore(t, schema.PackPublished)
	store.set(t, schema.PackAggregateKey(schema.AggregateAll), schema.PackAggregate{PackCount: 99})
	store.set(t, schema.AnalyticsMetadataKey(), schema.AnalyticsMetadataRecord{Enabled: false})
	var baseline CheckResult
	for _, workers := range []uint{1, 2, 4, 8} {
		result, err := CheckWithOptions(context.Background(), nil, store, CheckOptions{
			SlateDBOnly: true, MaxFindings: 10, Workers: workers, RPCConcurrency: 2,
		})
		if err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		result.Resources.Workers = 0
		result.Consistency.OptionsDigest = ""
		if workers == 1 {
			baseline = result
		} else if !reflect.DeepEqual(result, baseline) {
			t.Fatalf("workers=%d result differs\ngot:  %+v\nwant: %+v", workers, result, baseline)
		}
	}
}
func TestPackTypeSummaryMatchesClassifier(t *testing.T) {
	for _, types := range [][]schema.BlobType{
		nil,
		{schema.BlobData},
		{schema.BlobTree},
		{schema.BlobData, schema.BlobTree, schema.BlobData},
		{schema.BlobType(99)},
	} {
		var summary uint8
		for _, blobType := range types {
			summary = summarizePackType(summary, blobType)
		}
		if got, want := classifyPackSummary(summary), schema.ClassifyPack(types); got != want {
			t.Fatalf("types=%v got=%v want=%v", types, got, want)
		}
	}
}

func TestAddFindingSelectsCanonicalBoundedPrefix(t *testing.T) {
	result := CheckResult{}
	for _, finding := range []Finding{{Kind: "z", Key: "2"}, {Kind: "a", Key: "2"}, {Kind: "a", Key: "1"}} {
		addFinding(&result, 2, finding)
	}
	want := []Finding{{Kind: "a", Key: "1"}, {Kind: "a", Key: "2"}}
	if !slices.Equal(result.Findings, want) {
		t.Fatalf("findings = %+v, want %+v", result.Findings, want)
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
	if !result.EncryptionEnabled || result.EncryptedObjects != 10 || result.EnvelopeGeneration != 4 || result.ActiveDEKVersion != 2 || result.Clean() ||
		!result.HasWarnings() {
		t.Fatalf("encryption audit was not reflected in check result: %+v", result)
	}
	wantKinds := []string{"metadata_dek_rewrite_pending", "metadata_encryption_invalid", "metadata_object_plaintext"}
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
	state := schema.VerificationStateRecord{
		LastAttemptAt:      100,
		LastAttemptLevel:   schema.VerificationChecksum,
		HeaderVerifiedAt:   100,
		ChecksumVerifiedAt: 100,
		Result:             schema.VerificationHealthy,
		LastRunID:          schema.ID{1},
	}
	store.set(t, schema.PackPlacementKey(pack, backend), placement)
	store.set(t, schema.VerificationStateKey(pack, backend), state)
	result := CheckResult{}
	if err := checkVerificationState(ctx, store, &result, 10); err != nil ||
		result.VerificationStateMismatch != 0 {
		t.Fatalf("consistent verification state reported drift: %+v, %v", result, err)
	}
	placement.LastVerifiedAt = 99
	store.set(t, schema.PackPlacementKey(pack, backend), placement)
	result = CheckResult{}
	if err := checkVerificationState(ctx, store, &result, 10); err != nil ||
		result.VerificationStateMismatch != 1 ||
		result.Clean() {
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
	store.set(
		t,
		schema.AnalyticsWatermarkKey(generation),
		schema.AnalyticsWatermarkRecord{RepositoryGeneration: generation, ManifestGeneration: generation, AppliedAt: time.Now().UnixNano()},
	)
	store.values[string(schema.AnalyticsDerivedGenerationMarkerKey(generation))] = []byte{schema.Version}
	result, err = CheckWithOptions(context.Background(), nil, store, CheckOptions{SlateDBOnly: true, MaxFindings: 1})
	if err != nil || result.AnalyticsMismatch != 2 || result.Clean() || len(result.Findings) != 1 ||
		result.Findings[0].Kind != "analytics_fact_count_mismatch" {
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
	store.set(
		t,
		schema.ExportIndexCheckpointKey(schema.ID(result.IndexIDs[0])),
		schema.ExportIndexCheckpointRecord{Sequence: 1, PackIDs: []schema.ID{wrongPack}},
	)
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
