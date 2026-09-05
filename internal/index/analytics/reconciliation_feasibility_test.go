package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

const (
	reconciliationProfileSamples = 7
	reconciliationProfileWarmup  = 50
	reconciliationContentIDs     = 8
)

type reconciliationProfile struct {
	SchemaVersion              int                     `json:"schema_version"`
	Inodes                     int                     `json:"inodes_per_sample"`
	Samples                    int                     `json:"samples"`
	WarmupInodes               int                     `json:"warmup_inodes"`
	ContentIDsPerInode         int                     `json:"content_ids_per_inode"`
	GoVersion                  string                  `json:"go_version"`
	GOOS                       string                  `json:"goos"`
	GOARCH                     string                  `json:"goarch"`
	CPUs                       int                     `json:"cpus"`
	Hardware                   string                  `json:"hardware"`
	Date                       string                  `json:"date"`
	Commit                     string                  `json:"commit"`
	Baseline                   reconciliationAggregate `json:"baseline"`
	Enabled                    reconciliationAggregate `json:"analytics_enabled"`
	MedianTimeOverheadPercent  float64                 `json:"median_time_overhead_percent"`
	P95TimeOverheadPercent     float64                 `json:"p95_time_overhead_percent"`
	AuthoritativeWriteOverhead float64                 `json:"authoritative_write_overhead_percent"`
	CatchUp                    catchUpMeasurement      `json:"post_commit_catch_up"`
	Gates                      map[string]string       `json:"gates"`
	Methodology                []string                `json:"methodology"`
	Limitations                []string                `json:"limitations"`
}

type reconciliationAggregate struct {
	MedianSeconds        float64 `json:"median_seconds"`
	P95Seconds           float64 `json:"p95_seconds"`
	MedianInodesSecond   float64 `json:"median_inodes_per_second"`
	Mutations            uint64  `json:"authoritative_mutations"`
	EncodedBytes         uint64  `json:"authoritative_encoded_bytes"`
	EncodedBytesPerInode float64 `json:"authoritative_encoded_bytes_per_inode"`
}

type catchUpMeasurement struct {
	Deltas              uint64  `json:"deltas"`
	Seconds             float64 `json:"seconds"`
	DeltasPerSecond     float64 `json:"deltas_per_second"`
	DerivedBytes        uint64  `json:"retained_derived_bytes"`
	PeakDeltasBuffered  uint32  `json:"peak_deltas_buffered"`
	PeakWorkingSetBytes uint64  `json:"peak_working_set_estimate_bytes"`
}

type reconciliationRun struct {
	duration  time.Duration
	mutations uint64
	bytes     uint64
}

func TestReconciliationFeasibilityRegression(t *testing.T) {
	profile := runReconciliationProfile(t, 20, 1)
	if profile.Baseline.Mutations == 0 || profile.Enabled.Mutations <= profile.Baseline.Mutations {
		t.Fatalf("invalid authoritative accounting: baseline=%+v enabled=%+v", profile.Baseline, profile.Enabled)
	}
	if profile.CatchUp.Deltas != 20 {
		t.Fatalf("catch-up deltas = %d, want 20", profile.CatchUp.Deltas)
	}
	if profile.CatchUp.PeakDeltasBuffered > 4096 || profile.CatchUp.PeakWorkingSetBytes == 0 {
		t.Fatalf("catch-up buffer metrics = %+v", profile.CatchUp)
	}
}

func TestReferenceReconciliationFeasibilityProfile(t *testing.T) {
	value := os.Getenv("VAULTIC_ANALYTICS_RECONCILIATION_FEASIBILITY")
	if value == "" {
		t.Skip("set VAULTIC_ANALYTICS_RECONCILIATION_FEASIBILITY to the inode count per sample")
	}
	inodes, err := strconv.Atoi(value)
	if err != nil || inodes < 200 {
		t.Fatalf("VAULTIC_ANALYTICS_RECONCILIATION_FEASIBILITY must be at least 200")
	}
	profile := runReconciliationProfile(t, inodes, reconciliationProfileSamples)
	encoded, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if output := os.Getenv("VAULTIC_ANALYTICS_RECONCILIATION_JSON"); output != "" {
		if err := os.WriteFile(output, append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if output := os.Getenv("VAULTIC_ANALYTICS_RECONCILIATION_MD"); output != "" {
		if err := os.WriteFile(output, []byte(reconciliationMarkdown(profile)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%s", encoded)
}

func runReconciliationProfile(t *testing.T, inodes, samples int) reconciliationProfile {
	t.Helper()
	baseline := make([]reconciliationRun, samples)
	enabled := make([]reconciliationRun, samples)
	for sample := range samples {
		if sample%2 == 0 {
			baseline[sample] = runAuthoritativeReconciliation(t, inodes, false, sample, sample == 0)
			enabled[sample] = runAuthoritativeReconciliation(t, inodes, true, sample, sample == 0)
		} else {
			enabled[sample] = runAuthoritativeReconciliation(t, inodes, true, sample, false)
			baseline[sample] = runAuthoritativeReconciliation(t, inodes, false, sample, false)
		}
		t.Logf(
			"reconciliation feasibility sample %d/%d: baseline=%s enabled=%s",
			sample+1,
			samples,
			baseline[sample].duration,
			enabled[sample].duration,
		)
	}
	baselineAggregate := aggregateReconciliationRuns(baseline, inodes)
	enabledAggregate := aggregateReconciliationRuns(enabled, inodes)
	paired := make([]float64, samples)
	for index := range samples {
		paired[index] = percentOver(baseline[index].duration.Seconds(), enabled[index].duration.Seconds())
	}
	sort.Float64s(paired)
	catchUp := runCatchUpMeasurement(t, inodes)
	writeOverhead := percentOver(float64(baselineAggregate.EncodedBytes), float64(enabledAggregate.EncodedBytes))
	profile := reconciliationProfile{
		SchemaVersion: 2, Inodes: inodes, Samples: samples, WarmupInodes: reconciliationProfileWarmup,
		ContentIDsPerInode: reconciliationContentIDs, GoVersion: runtime.Version(), GOOS: runtime.GOOS,
		GOARCH: runtime.GOARCH, CPUs: runtime.NumCPU(), Hardware: feasibilityHardware(),
		Date: time.Now().UTC().Format(time.RFC3339), Commit: feasibilityCommit(), Baseline: baselineAggregate,
		Enabled: enabledAggregate, MedianTimeOverheadPercent: percentileFloat(paired, 0.5),
		P95TimeOverheadPercent: percentileFloat(paired, 0.95), AuthoritativeWriteOverhead: writeOverhead,
		CatchUp: catchUp, Gates: map[string]string{},
		Methodology: []string{
			("Each pair publishes identical deterministic first-seen inode revisions through " +
				"SchemaStore.PublishReconciledRevision and a real vaulticdb transaction."),
			("Fixture encoding, daemon startup, analytics metadata setup, revision " +
				"allocation, warm-up, validation reads, and catch-up are outside the " +
				"authoritative wall-time interval."),
			"Sample order alternates baseline-first and enabled-first; reported overhead is the median and p95 of paired sample ratios.",
			("Authoritative bytes are exact key plus encoded-value bytes for every " +
				"mutation produced by these first-seen reconciliations; enabled accounting " +
				"includes ae: deltas."),
			"Post-commit catch-up runs in a separate repository and is reported independently.",
			"Post-commit catch-up calls the production bounded outbox consumer and reports its maximum input buffer and conservative working-set estimate.",
		},
		Limitations: []string{
			("The CPU/time gate is evaluated with authoritative wall time because " +
				"vaulticdb executes in a separate process; process CPU attribution is not " +
				"portable through the public client API."),
			"Encoded-byte accounting is logical authoritative metadata, not physical SlateDB WAL, block-compression, or compaction amplification.",
		},
	}
	profile.Gates["reconciliation_cpu_time_5pct"] = passFail(profile.MedianTimeOverheadPercent <= 5)
	profile.Gates["authoritative_metadata_write_10pct"] = passFail(profile.AuthoritativeWriteOverhead <= 10)
	return profile
}

func runAuthoritativeReconciliation(
	t *testing.T,
	inodes int,
	enabled bool,
	sample int,
	account bool,
) reconciliationRun {
	t.Helper()
	client := reconciliationDaemonClient(t, fmt.Sprintf("phase16-reconcile-%t-%d", enabled, sample))
	defer func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("close reconciliation daemon: %v", err)
		}
	}()
	store := daemon.NewSchemaStore(client)
	ctx := context.Background()
	metadata := schema.AnalyticsMetadataRecord{
		Enabled:    enabled,
		Generation: 1,
		BuiltAt:    1735689600000000000,
		ConfigJSON: "{}",
	}
	if err := store.Put(ctx, schema.AnalyticsMetadataKey(), encodeFeasibilityRecord(t, metadata), true); err != nil {
		t.Fatal(err)
	}
	workload := makeReconciliationWorkload(t, store, inodes+reconciliationProfileWarmup)
	for _, item := range workload[:reconciliationProfileWarmup] {
		if err := store.PublishReconciledRevision(ctx, item); err != nil {
			t.Fatal(err)
		}
	}
	measured := workload[reconciliationProfileWarmup:]
	started := time.Now()
	for _, item := range measured {
		if err := store.PublishReconciledRevision(ctx, item); err != nil {
			t.Fatal(err)
		}
	}
	duration := time.Since(started)
	var mutations, bytes uint64
	if account {
		mutations, bytes = authoritativeReconciliationBytes(t, store, measured, enabled)
	}
	return reconciliationRun{duration: duration, mutations: mutations, bytes: bytes}
}

func makeReconciliationWorkload(t *testing.T, store *daemon.SchemaStore, count int) []daemon.ReconciledRevision {
	t.Helper()
	ctx := context.Background()
	items := make([]daemon.ReconciledRevision, count)
	for index := range items {
		revision, err := store.AllocateRevision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		inode := uint64(index + 1)
		content := make([]schema.ID, reconciliationContentIDs)
		for contentIndex := range content {
			content[contentIndex] = reconciliationID(index, contentIndex)
		}
		record := schema.InodeRevision{
			Known: schema.KnownPath | schema.KnownSize | schema.KnownUID | schema.KnownGID,
			Size:  uint64(4096 + index%1024), UID: uint32(1000 + index%128), GID: uint32(100 + index%32),
			ContentMode: schema.ContentInline, ContentCount: uint32(len(content)), ContentIDs: content,
			SourcePath: fmt.Sprintf("svm/volume/group/file-%08d", index),
			Freshness:  schema.FreshnessVerified,
		}
		items[index] = daemon.ReconciledRevision{
			CurrentKey: schema.CurrentInodeKey(1, inode), RevisionKey: schema.InodeRevisionKey(1, inode, revision),
			RevisionValue: encodeFeasibilityRecord(t, record), Revision: revision, ContentIDs: content,
		}
	}
	return items
}

func authoritativeReconciliationBytes(
	t *testing.T,
	store *daemon.SchemaStore,
	items []daemon.ReconciledRevision,
	enabled bool,
) (uint64, uint64) {
	t.Helper()
	ctx := context.Background()
	var mutations, bytes uint64
	add := func(key []byte) {
		value, found, err := store.Get(ctx, key)
		if err != nil || !found {
			t.Fatalf("authoritative key %x: found=%t err=%v", key, found, err)
		}
		mutations++
		bytes += uint64(len(key) + len(value))
	}
	for _, item := range items {
		add(item.CurrentKey)
		add(item.RevisionKey)
		parsed, _ := schema.ParseKey(item.CurrentKey)
		for _, id := range item.ContentIDs {
			add(schema.ReverseInodeKey(id, parsed.FSID, parsed.Inode))
			add(schema.ReferenceCountKey(id))
		}
		if enabled {
			add(schema.AnalyticsDeltaKey(item.Revision, 0))
		}
	}
	return mutations, bytes
}

func runCatchUpMeasurement(t *testing.T, inodes int) catchUpMeasurement {
	t.Helper()
	client := reconciliationDaemonClient(t, "phase16-reconcile-catch-up")
	defer func() { _ = client.Close(context.Background()) }()
	store := daemon.NewSchemaStore(client)
	ctx := context.Background()
	metadata := schema.AnalyticsMetadataRecord{
		Enabled:    true,
		Generation: 1,
		BuiltAt:    1735689600000000000,
		ConfigJSON: "{}",
	}
	if err := store.Put(ctx, schema.AnalyticsMetadataKey(), encodeFeasibilityRecord(t, metadata), true); err != nil {
		t.Fatal(err)
	}
	items := makeReconciliationWorkload(t, store, inodes)
	for _, item := range items {
		if err := store.PublishReconciledRevision(ctx, item); err != nil {
			t.Fatal(err)
		}
	}
	started := time.Now()
	var processed uint64
	var peakDeltas uint32
	var peakBytes uint64
	for processed < uint64(inodes) {
		result, err := CatchUp(ctx, store, CatchUpOptions{MaxDeltas: 1000})
		if err != nil {
			t.Fatal(err)
		}
		if result.Processed == 0 {
			t.Fatalf("catch-up stopped after %d/%d deltas", processed, inodes)
		}
		processed += uint64(result.Processed)
		if result.PeakDeltasBuffered > peakDeltas {
			peakDeltas = result.PeakDeltasBuffered
		}
		if result.PeakWorkingSetBytes > peakBytes {
			peakBytes = result.PeakWorkingSetBytes
		}
	}
	duration := time.Since(started)
	return catchUpMeasurement{
		Deltas:              processed,
		Seconds:             duration.Seconds(),
		DeltasPerSecond:     float64(processed) / duration.Seconds(),
		DerivedBytes:        analyticsDerivedBytes(t, store),
		PeakDeltasBuffered:  peakDeltas,
		PeakWorkingSetBytes: peakBytes,
	}
}

func analyticsDerivedBytes(t *testing.T, store *daemon.SchemaStore) uint64 {
	t.Helper()
	var total uint64
	for _, prefix := range [][]byte{[]byte("af:"), []byte("am:"), []byte("ai:"), []byte("ad:"), []byte("ar:"), []byte("aw:")} {
		var cursor []byte
		for {
			items, done, err := store.ScanPrefix(context.Background(), prefix, cursor, 1000)
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range items {
				total += uint64(len(item.Key) + len(item.Value))
			}
			if done || len(items) == 0 {
				break
			}
			cursor = items[len(items)-1].Key
		}
	}
	return total
}

func reconciliationDaemonClient(t *testing.T, repositoryID string) *daemon.Client {
	t.Helper()
	binary := os.Getenv("VAULTICDB_TEST_BINARY")
	if binary == "" {
		binary = filepath.Join("..", "..", "..", "vaulticdb", "target", "debug", "vaulticdb")
	}
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("compiled vaulticdb unavailable: %v", err)
	}
	socketDir, err := os.MkdirTemp("/tmp", "vd-feasibility-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	client, err := daemon.Ensure(context.Background(), daemon.Options{
		Socket: filepath.Join(socketDir, "daemon.sock"), RepositoryID: repositoryID,
		DaemonPath: binary, DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func reconciliationID(inode, ordinal int) schema.ID {
	var id schema.ID
	value := uint64(inode*reconciliationContentIDs + ordinal + 1)
	for index := range 8 {
		id[index] = byte(value >> (index * 8))
	}
	id[8] = 0xa5
	return id
}

func encodeFeasibilityRecord(t *testing.T, record interface{ MarshalBinary() ([]byte, error) }) []byte {
	t.Helper()
	value, err := record.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func aggregateReconciliationRuns(runs []reconciliationRun, inodes int) reconciliationAggregate {
	durations := make([]float64, len(runs))
	for index, run := range runs {
		durations[index] = run.duration.Seconds()
	}
	sort.Float64s(durations)
	median := percentileFloat(durations, 0.5)
	accounted := runs[0]
	for _, run := range runs {
		if run.mutations != 0 {
			accounted = run
			break
		}
	}
	return reconciliationAggregate{
		MedianSeconds: median, P95Seconds: percentileFloat(durations, 0.95), MedianInodesSecond: float64(inodes) / median,
		Mutations: accounted.mutations, EncodedBytes: accounted.bytes, EncodedBytesPerInode: float64(accounted.bytes) / float64(inodes),
	}
}

func percentileFloat(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1)*percentile + 0.5)
	return values[index]
}

func percentOver(baseline, enabled float64) float64 { return (enabled/baseline - 1) * 100 }

func reconciliationMarkdown(profile reconciliationProfile) string {
	return fmt.Sprintf(
		("# Phase 16 Reconciliation Feasibility\n\nDate: `%s`  \nCommit: `%s`  " +
			"\nEnvironment: `%s`; `%s %s/%s`, %d CPUs  \nWorkload: `%d` inodes/sample, " +
			"`%d` samples, `%d` warm-up inodes, `%d` unique content IDs/inode\n\n| Metric " +
			"| Baseline | Analytics enabled | Overhead |\n|---|---:|---:|---:|\n| Median " +
			"authoritative time | %.6f s | %.6f s | %.3f%% |\n| p95 authoritative time | " +
			"%.6f s | %.6f s | paired p95 %.3f%% |\n| Authoritative mutations | %d | %d | " +
			"%.3f%% |\n| Authoritative encoded bytes | %d | %d | %.3f%% |\n| Encoded " +
			"bytes/inode | %.3f | %.3f | %.3f%% |\n\nPost-commit catch-up: %d deltas in " +
			"%.6f s (%.0f deltas/s), %d retained derived bytes. Peak production buffer: " +
			"%d deltas, %d estimated working-set bytes.\n\n## Gates\n\n```json\n%s\n```\n\n## " +
			"Methodology\n\n%s\n\n## Limitations\n\n%s\n"),
		profile.Date,
		profile.Commit,
		profile.Hardware,
		profile.GoVersion,
		profile.GOOS,
		profile.GOARCH,
		profile.CPUs,
		profile.Inodes,
		profile.Samples,
		profile.WarmupInodes,
		profile.ContentIDsPerInode,
		profile.Baseline.MedianSeconds,
		profile.Enabled.MedianSeconds,
		profile.MedianTimeOverheadPercent,
		profile.Baseline.P95Seconds,
		profile.Enabled.P95Seconds,
		profile.P95TimeOverheadPercent,
		profile.Baseline.Mutations,
		profile.Enabled.Mutations,
		percentOver(float64(profile.Baseline.Mutations), float64(profile.Enabled.Mutations)),
		profile.Baseline.EncodedBytes,
		profile.Enabled.EncodedBytes,
		profile.AuthoritativeWriteOverhead,
		profile.Baseline.EncodedBytesPerInode,
		profile.Enabled.EncodedBytesPerInode,
		profile.AuthoritativeWriteOverhead,
		profile.CatchUp.Deltas,
		profile.CatchUp.Seconds,
		profile.CatchUp.DeltasPerSecond,
		profile.CatchUp.DerivedBytes,
		profile.CatchUp.PeakDeltasBuffered,
		profile.CatchUp.PeakWorkingSetBytes,
		mustJSON(profile.Gates),
		"- "+joinLines(profile.Methodology),
		"- "+joinLines(profile.Limitations),
	)
}

func joinLines(values []string) string { return joinWith(values, "\n- ") }

func joinWith(values []string, separator string) string {
	if len(values) == 0 {
		return ""
	}
	result := values[0]
	for _, value := range values[1:] {
		result += separator + value
	}
	return result
}
