package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

const (
	feasibilitySeed        = int64(160019)
	feasibilitySegmentRows = 262144
	feasibilityOracleFacts = 100000
)

type feasibilityProfile struct {
	SchemaVersion int               `json:"schema_version"`
	Facts         uint64            `json:"facts"`
	Seed          int64             `json:"seed"`
	SegmentRows   int               `json:"segment_rows"`
	GoVersion     string            `json:"go_version"`
	GOOS          string            `json:"goos"`
	GOARCH        string            `json:"goarch"`
	CPUs          int               `json:"cpus"`
	Hardware      string            `json:"hardware"`
	Date          string            `json:"date"`
	Commit        string            `json:"commit"`
	Namespaces    map[string]uint64 `json:"namespace_bytes"`
	CoreBytes     uint64            `json:"core_bytes"`
	CoreBytesFact float64           `json:"core_bytes_per_fact"`
	ProjectedCore uint64            `json:"projected_core_bytes_1_4b"`
	TotalWrites   uint64            `json:"logical_bytes_written"`
	WriteAmp      float64           `json:"logical_write_amplification"`
	BuildSeconds  float64           `json:"build_seconds"`
	BuildFactsSec float64           `json:"build_facts_per_second"`
	BitmapBytes   uint64            `json:"bitmap_bytes"`
	BitmapValues  uint64            `json:"bitmap_values"`
	BroadSeconds  float64           `json:"cold_named_query_seconds"`
	BroadFiles    uint64            `json:"cold_named_query_files"`
	ProjectedSecs float64           `json:"projected_query_seconds_1_4b"`
	OracleFacts   uint64            `json:"oracle_sample_facts"`
	OracleMatches int               `json:"oracle_matches"`
	OracleQueries int               `json:"oracle_queries"`
	CacheP95      float64           `json:"cache_p95_seconds"`
	CatchUpFacts  uint64            `json:"catch_up_facts"`
	CatchUpSecs   float64           `json:"catch_up_seconds"`
	CatchUpRate   float64           `json:"catch_up_facts_per_second"`
	RebuildSecs   float64           `json:"rebuild_seconds"`
	MetadataWrite *float64          `json:"authoritative_write_overhead_percent"`
	ReconcileCPU  *float64          `json:"reconciliation_cpu_overhead_percent"`
	Gates         map[string]string `json:"gates"`
	Notes         []string          `json:"notes"`
}

type feasibilityStore struct {
	dir        string
	values     map[string][]byte
	segments   map[string]string
	namespaces map[string]uint64
	writes     map[string]uint64
	facts      uint64
	bitmaps    uint64
}

func newFeasibilityStore(t *testing.T, facts uint64) *feasibilityStore {
	t.Helper()
	dir := t.TempDir()
	return &feasibilityStore{
		dir:        dir,
		values:     map[string][]byte{},
		segments:   map[string]string{},
		namespaces: map[string]uint64{},
		writes:     map[string]uint64{},
		facts:      facts,
	}
}

func (store *feasibilityStore) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	if path, found := store.segments[string(key)]; found {
		value, err := os.ReadFile(path)
		return value, err == nil, err
	}
	if parsed, err := schema.ParseKey(key); err == nil && parsed.Kind == schema.KeyAnalyticsResidency {
		value, err := feasibilityResidency(parsed.Inode).MarshalBinary()
		return value, err == nil, err
	}
	value, found := store.values[string(key)]
	return append([]byte(nil), value...), found, nil
}

func (store *feasibilityStore) ScanPrefix(
	_ context.Context,
	prefix, cursor []byte,
	limit uint32,
) ([]daemon.KeyValue, bool, error) {
	keys := make([]string, 0)
	for key := range store.values {
		if strings.HasPrefix(key, string(prefix)) && (len(cursor) == 0 || bytes.Compare([]byte(key), cursor) >= 0) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	done := len(keys) <= int(limit)
	if !done {
		keys = keys[:limit]
	}
	items := make([]daemon.KeyValue, len(keys))
	for index, key := range keys {
		items[index] = daemon.KeyValue{Key: []byte(key), Value: append([]byte(nil), store.values[key]...)}
	}
	return items, done, nil
}

func (store *feasibilityStore) WriteMutableBatch(
	_ context.Context,
	puts []daemon.Mutation,
	deletes [][]byte,
	_ bool,
) error {
	for _, put := range puts {
		if previous, found := store.values[string(put.Key)]; found {
			store.namespaces[feasibilityNamespace(put.Key)] -= uint64(len(put.Key) + len(previous))
		}
		store.values[string(put.Key)] = append([]byte(nil), put.Value...)
		store.namespaces[feasibilityNamespace(put.Key)] += uint64(len(put.Key) + len(put.Value))
		store.writes[feasibilityNamespace(put.Key)] += uint64(len(put.Key) + len(put.Value))
	}
	for _, key := range deletes {
		delete(store.values, string(key))
	}
	return nil
}

func (store *feasibilityStore) ingest(puts []daemon.Mutation, retainAllIndexes bool) error {
	for _, put := range puts {
		namespace := feasibilityNamespace(put.Key)
		store.namespaces[namespace] += uint64(len(put.Key) + len(put.Value))
		store.writes[namespace] += uint64(len(put.Key) + len(put.Value))
		switch namespace {
		case "af":
			path := filepath.Join(store.dir, fmt.Sprintf("segment-%08d", len(store.segments)))
			if err := os.WriteFile(path, put.Value, 0o600); err != nil {
				return err
			}
			store.segments[string(put.Key)] = path
		case "ai":
			store.bitmaps++
			parsed, err := schema.ParseKey(put.Key)
			if retainAllIndexes ||
				err == nil &&
					((parsed.Dimension == schema.AnalyticsDimensionUID &&
						parsed.Value == 600) ||
						(parsed.Dimension == schema.AnalyticsDimensionCalendarYear &&
							parsed.Value == 2024)) {
				store.values[string(put.Key)] = append([]byte(nil), put.Value...)
			}
		default:
			store.values[string(put.Key)] = append([]byte(nil), put.Value...)
		}
	}
	return nil
}

func TestFeasibilityHarnessRegression(t *testing.T) {
	profile := runFeasibilityProfile(t, 10000, 100)
	if profile.OracleMatches != profile.OracleQueries {
		t.Fatalf("oracle matches = %d/%d", profile.OracleMatches, profile.OracleQueries)
	}
	if profile.CoreBytesFact <= 0 || profile.BroadFiles == 0 {
		t.Fatalf("invalid profile: %+v", profile)
	}
}

func TestFeasibilityProductionCodecs(t *testing.T) {
	dict := feasibilityDictionaries()
	facts := make([]buildFact, 1000)
	for index := range facts {
		facts[index] = feasibilityFact(uint64(index))
	}
	puts, err := buildSegment(1, 1, facts, dict)
	if err != nil {
		t.Fatal(err)
	}
	var sawSegment, sawIndex, sawMetadata bool
	for _, put := range puts {
		parsed, err := schema.ParseKey(put.Key)
		if err != nil {
			t.Fatal(err)
		}
		switch parsed.Kind {
		case schema.KeyAnalyticsFactSegment:
			record, err := schema.UnmarshalAnalyticsFactSegmentRecord(put.Value)
			if err != nil {
				t.Fatal(err)
			}
			for _, column := range record.Columns {
				if column.Codec != schema.AnalyticsCodecZstd {
					t.Fatalf("column %d codec = %d", column.Kind, column.Codec)
				}
			}
			sawSegment = true
		case schema.KeyAnalyticsDimensionIndex:
			record, err := schema.UnmarshalAnalyticsDimensionIndexRecord(put.Value)
			if err != nil || record.Codec != schema.AnalyticsCodecZstd {
				t.Fatalf("index codec = %d, err=%v", record.Codec, err)
			}
			sawIndex = true
		case schema.KeyAnalyticsSegmentMetadata:
			record, err := schema.UnmarshalAnalyticsSegmentMetadataRecord(put.Value)
			if err != nil || record.CodecParameters != "json-columns-v1;zstd=3" {
				t.Fatalf("codec parameters = %q, err=%v", record.CodecParameters, err)
			}
			sawMetadata = true
		}
	}
	if !sawSegment || !sawIndex || !sawMetadata {
		t.Fatalf("missing production records: segment=%t index=%t metadata=%t", sawSegment, sawIndex, sawMetadata)
	}
}

func BenchmarkFeasibilityBuildSegment(b *testing.B) {
	dict := feasibilityDictionaries()
	facts := make([]buildFact, 65536)
	for index := range facts {
		facts[index] = feasibilityFact(uint64(index))
	}
	b.ResetTimer()
	for b.Loop() {
		if _, err := buildSegment(1, 1, facts, dict); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(facts)), "facts/op")
}

func TestReferenceFeasibilityProfile(t *testing.T) {
	value := os.Getenv("VAULTIC_ANALYTICS_FEASIBILITY")
	if value == "" {
		t.Skip("set VAULTIC_ANALYTICS_FEASIBILITY=10000000 or 100000000")
	}
	facts, err := strconv.ParseUint(value, 10, 64)
	if err != nil || facts != 10000000 && facts != 100000000 {
		t.Fatalf("VAULTIC_ANALYTICS_FEASIBILITY must be exactly 10000000 or 100000000")
	}
	profile := runFeasibilityProfile(t, facts, 1000)
	encoded, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if output := os.Getenv("VAULTIC_ANALYTICS_FEASIBILITY_JSON"); output != "" {
		if err := os.WriteFile(output, append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if output := os.Getenv("VAULTIC_ANALYTICS_FEASIBILITY_MD"); output != "" {
		if err := os.WriteFile(output, []byte(feasibilityMarkdown(profile)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%s", encoded)
}

func runFeasibilityProfile(t *testing.T, facts uint64, oracleQueries int) feasibilityProfile {
	t.Helper()
	store := newFeasibilityStore(t, facts)
	dict := feasibilityDictionaries()
	if err := installFeasibilityMetadata(store, dict, facts); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	segments := make([]uint64, 0, (facts+feasibilitySegmentRows-1)/feasibilitySegmentRows)
	nextProgress := uint64(10)
	for start, ordinal := uint64(0), uint64(1); start < facts; start, ordinal = start+feasibilitySegmentRows, ordinal+1 {
		count := uint64(feasibilitySegmentRows)
		if remaining := facts - start; remaining < count {
			count = remaining
		}
		batch := make([]buildFact, count)
		for index := range batch {
			batch[index] = feasibilityFact(start + uint64(index))
		}
		puts, err := buildSegment(ordinal, 1, batch, dict)
		if err != nil || store.ingest(puts, facts <= feasibilityOracleFacts) != nil {
			t.Fatalf("build segment %d: %v", ordinal, err)
		}
		segments = append(segments, ordinal)
		completed := start + count
		if facts >= 1_000_000 && completed*100/facts >= nextProgress {
			t.Logf("analytics feasibility build: %d%% (%d/%d facts)", nextProgress, completed, facts)
			nextProgress += 10
		}
	}
	buildDuration := time.Since(started)
	if err := publishFeasibilityManifest(store, segments, facts); err != nil {
		t.Fatal(err)
	}
	store.namespaces["ar"] = feasibilityResidencyBytes(facts)
	store.namespaces["outbox"] = feasibilityOutboxBytes(maxUint64(1, facts*6/1000))
	store.writes["ar"] = store.namespaces["ar"]
	store.writes["outbox"] = store.namespaces["outbox"]
	for _, namespace := range []string{"af", "am", "ai", "ad", "ar", "views", "outbox", "cache"} {
		if _, found := store.namespaces[namespace]; !found {
			store.namespaces[namespace] = 0
		}
	}
	minimum, maximum := uint64(1<<20), uint64(10<<20)
	named := Query{
		UIDs:              []uint32{600},
		Years:             []int{2024},
		SizeMin:           &minimum,
		SizeMax:           &maximum,
		Residencies:       []string{"archive-only"},
		IncludeIncomplete: true,
	}
	t.Logf("analytics feasibility: starting cold named query")
	queryStarted := time.Now()
	result, err := Execute(context.Background(), store, named)
	if err != nil {
		t.Fatal(err)
	}
	broadDuration := time.Since(queryStarted)
	oracleFacts := minUint64(facts, feasibilityOracleFacts)
	t.Logf(
		"analytics feasibility: cold named query completed in %s; starting %d-query oracle over %d facts",
		broadDuration,
		oracleQueries,
		oracleFacts,
	)
	matches := runFeasibilityOracle(t, oracleFacts, oracleQueries)
	t.Logf(
		"analytics feasibility: oracle completed with %d/%d matches; starting cache measurements",
		matches,
		oracleQueries,
	)
	for iteration := 0; iteration < 3; iteration++ {
		if _, err := Execute(context.Background(), store, named); err != nil {
			t.Fatal(err)
		}
	}
	latencies := make([]time.Duration, 100)
	for iteration := range latencies {
		start := time.Now()
		cached, err := Execute(context.Background(), store, named)
		if err != nil || !cached.Cached {
			t.Fatalf("cached query %d: cached=%t err=%v", iteration, cached.Cached, err)
		}
		latencies[iteration] = time.Since(start)
	}
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	catchUpFacts := maxUint64(1, facts*6/1000)
	t.Logf("analytics feasibility: cache measurements completed; starting %d-fact catch-up", catchUpFacts)
	catchStarted := time.Now()
	for start := uint64(0); start < catchUpFacts; start += feasibilitySegmentRows {
		count := minUint64(feasibilitySegmentRows, catchUpFacts-start)
		batch := make([]buildFact, count)
		for index := range batch {
			batch[index] = feasibilityFact(facts + start + uint64(index))
		}
		if _, err := buildSegment(1, 1, batch, dict); err != nil {
			t.Fatal(err)
		}
	}
	catchDuration := time.Since(catchStarted)
	core := store.namespaces["af"] + store.namespaces["am"] + store.namespaces["ai"]
	core += store.namespaces["ad"] + store.namespaces["ar"] + store.namespaces["views"]
	var totalWrites uint64
	for _, bytes := range store.writes {
		totalWrites += bytes
	}
	bytesFact := float64(core) / float64(facts)
	projectedCore := uint64(bytesFact * 1_400_000_000)
	projectedQuery := broadDuration.Seconds() * 1_400_000_000 / float64(facts)
	profile := feasibilityProfile{
		SchemaVersion: 1,
		Facts:         facts,
		Seed:          feasibilitySeed,
		SegmentRows:   feasibilitySegmentRows,
		GoVersion:     runtime.Version(),
		GOOS:          runtime.GOOS,
		GOARCH:        runtime.GOARCH,
		CPUs:          runtime.NumCPU(),
		Hardware:      feasibilityHardware(),
		Date:          time.Now().UTC().Format(time.RFC3339),
		Commit:        feasibilityCommit(),
		Namespaces:    store.namespaces,
		CoreBytes:     core,
		CoreBytesFact: bytesFact,
		ProjectedCore: projectedCore,
		TotalWrites:   totalWrites,
		WriteAmp:      float64(totalWrites) / float64(core),
		BuildSeconds:  buildDuration.Seconds(),
		BuildFactsSec: float64(facts) / buildDuration.Seconds(),
		BitmapBytes:   store.namespaces["ai"],
		BitmapValues:  feasibilityBitmapValues(store),
		BroadSeconds:  broadDuration.Seconds(),
		BroadFiles:    result.Files,
		ProjectedSecs: projectedQuery,
		OracleFacts:   oracleFacts,
		OracleMatches: matches,
		OracleQueries: oracleQueries,
		CacheP95:      latencies[94].Seconds(),
		CatchUpFacts:  catchUpFacts,
		CatchUpSecs:   catchDuration.Seconds(),
		CatchUpRate:   float64(catchUpFacts) / catchDuration.Seconds(),
		RebuildSecs:   buildDuration.Seconds(),
		Gates:         map[string]string{},
		Notes: []string{
			"Direct segment build avoids duplicating authoritative iv: input at 10M/100M.",
			"The 1,000-query oracle uses a deterministic 100,000-fact sample.",
			"The incremental profile contains 0.1% creations and 0.5% residency changes.",
			"Authoritative reconciliation CPU and write baselines are not available in the direct segment profile.",
		},
	}
	profile.Gates["core_175gb"] = passFail(projectedCore <= 175_000_000_000)
	profile.Gates["peak_250gb"] = passFail(projectedCore+10_000_000_000+core <= 250_000_000_000)
	profile.Gates["broad_100m_120s"] = passFail(profile.BroadSeconds*100_000_000/float64(facts) <= 120)
	profile.Gates["broad_1.4b_30m"] = passFail(projectedQuery < 1800)
	profile.Gates["cache_p95_2s"] = passFail(profile.CacheP95 <= 2)
	profile.Gates["oracle_1000"] = passFail(oracleQueries == 1000 && matches == 1000)
	profile.Gates["catch_up_24h"] = passFail(profile.CatchUpSecs <= 24*60*60)
	profile.Gates["reconciliation_cpu_5pct"] = "not_measured"
	profile.Gates["metadata_write_10pct"] = "not_measured"
	return profile
}

func feasibilityFact(index uint64) buildFact {
	mixed := index*0x9e3779b97f4a7c15 + uint64(feasibilitySeed)
	year := 2020 + int(mixed%7)
	month := time.Month(1 + mixed/7%12)
	day := 1 + int(mixed/97%28)
	created := time.Date(year, month, day, int(mixed%24), 0, 0, 0, time.UTC)
	uid := uint32(1000 + mixed%2048)
	if index%20 == 0 {
		uid = 600
	}
	known := schema.KnownUID | schema.KnownGID | schema.KnownSize
	size := uint64(128 + mixed%(32<<20))
	if index%100 == 0 {
		known &^= schema.KnownSize
		size = 0
	} else if index%50 == 0 {
		size = 0
	}
	isoYear, week := created.ISOWeek()
	continuity := schema.AnalyticsContinuityProven
	if index%100 == 1 {
		continuity = schema.AnalyticsContinuityUnknown
	} else if index%10 == 1 {
		continuity = schema.AnalyticsContinuitySourceGeneration
	}
	return buildFact{
		identity: segmentIdentity{
			FSID:       uint32(1 + index%32),
			Inode:      index + 1,
			Generation: index + 1,
			Revision:   index + 1,
			Known:      known,
		},
		fact: schema.AnalyticsFactRecord{
			Revision:           index + 1,
			UID:                uid,
			GID:                uint32(100 + mixed%256),
			Known:              known,
			CreatedAt:          created.UnixNano(),
			LogicalSize:        size,
			CalendarYear:       int32(year),
			CalendarMonth:      uint8(month),
			ISOYear:            int32(isoYear),
			Workweek:           uint8(week),
			SVM:                fmt.Sprintf("svm-%02d", mixed%64),
			Volume:             fmt.Sprintf("volume-%03d", mixed%256),
			PathGroup:          fmt.Sprintf("group-%04d", mixed%1024),
			Residency:          feasibilityResidency(index + 1).State,
			CreationBasis:      schema.AnalyticsBirthTime,
			IdentityGeneration: index + 1,
			IdentityContinuity: continuity,
		},
	}
}

func feasibilityResidency(inode uint64) schema.AnalyticsResidencyRecord {
	state := schema.AnalyticsLive
	retained := uint64(0)
	if inode%20 == 1 {
		state, retained = schema.AnalyticsArchiveOnly, 1
	} else if inode%20 == 2 {
		state = schema.AnalyticsUnknown
	}
	return schema.AnalyticsResidencyRecord{
		State:                state,
		LastCompleteCrawl:    1735689600000000000,
		RetainedSnapshotRefs: retained,
		ClassificationEpoch:  1,
		FactSegment:          (inode-1)/feasibilitySegmentRows + 1,
		Row:                  uint32((inode - 1) % feasibilitySegmentRows),
	}
}

func feasibilityDictionaries() dictionaries {
	dict := dictionaries{
		ids:    map[schema.AnalyticsDictionaryKind]map[string]uint32{},
		values: map[schema.AnalyticsDictionaryKind][]string{},
	}
	for kind, count := range map[schema.AnalyticsDictionaryKind]int{schema.AnalyticsDictionarySVM: 64,
		schema.AnalyticsDictionaryVolume:    256,
		schema.AnalyticsDictionaryPathGroup: 1024} {
		dict.ids[kind] = map[string]uint32{}
		prefix := map[schema.AnalyticsDictionaryKind]string{schema.AnalyticsDictionarySVM: "svm-",
			schema.AnalyticsDictionaryVolume:    "volume-",
			schema.AnalyticsDictionaryPathGroup: "group-"}[kind]
		width := map[schema.AnalyticsDictionaryKind]int{schema.AnalyticsDictionarySVM: 2,
			schema.AnalyticsDictionaryVolume:    3,
			schema.AnalyticsDictionaryPathGroup: 4}[kind]
		for index := 0; index < count; index++ {
			value := fmt.Sprintf("%s%0*d", prefix, width, index)
			dict.values[kind] = append(dict.values[kind], value)
			dict.ids[kind][value] = uint32(index + 1)
		}
	}
	return dict
}

func installFeasibilityMetadata(store *feasibilityStore, dict dictionaries, facts uint64) error {
	puts, err := marshalDictionaries(dict)
	if err != nil {
		return err
	}
	if err := store.ingest(puts, true); err != nil {
		return err
	}
	config, _ := json.Marshal(Config{SegmentRows: feasibilitySegmentRows, CacheAfter: 3}.normalized())
	metadata,
		err := (schema.AnalyticsMetadataRecord{Enabled: true,
		Generation: 1,
		Facts:      facts,
		BuiltAt:    1735689600000000000,
		ConfigJSON: string(config)}).MarshalBinary()
	if err != nil {
		return err
	}
	store.values[string(schema.AnalyticsMetadataKey())] = metadata
	return nil
}

func publishFeasibilityManifest(store *feasibilityStore, segments []uint64, facts uint64) error {
	manifest, err := (schema.AnalyticsManifestRecord{Generation: 1, Segments: segments}).MarshalBinary()
	if err != nil {
		return err
	}
	watermark,
		err := (schema.AnalyticsWatermarkRecord{RepositoryGeneration: 1,
		AppliedCommit:      facts,
		ManifestGeneration: 1,
		AppliedAt:          time.Now().UnixNano()}).MarshalBinary()
	if err != nil {
		return err
	}
	store.values[string(schema.AnalyticsManifestKey(1))] = manifest
	store.values[string(schema.AnalyticsWatermarkKey(1))] = watermark
	return nil
}

func runFeasibilityOracle(t *testing.T, facts uint64, queries int) int {
	t.Helper()
	store := newFeasibilityStore(t, facts)
	dict := feasibilityDictionaries()
	if err := installFeasibilityMetadata(store, dict, facts); err != nil {
		t.Fatal(err)
	}
	oracleFacts := make([]schema.AnalyticsFactRecord, facts)
	var segments []uint64
	for start, ordinal := uint64(0), uint64(1); start < facts; start, ordinal = start+feasibilitySegmentRows, ordinal+1 {
		count := minUint64(feasibilitySegmentRows, facts-start)
		batch := make([]buildFact, count)
		for index := range batch {
			batch[index] = feasibilityFact(start + uint64(index))
			oracleFacts[start+uint64(index)] = batch[index].fact
		}
		puts, err := buildSegment(ordinal, 1, batch, dict)
		if err != nil || store.ingest(puts, true) != nil {
			t.Fatal(err)
		}
		segments = append(segments, ordinal)
	}
	if err := publishFeasibilityManifest(store, segments, facts); err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewSource(feasibilitySeed))
	matches := 0
	for iteration := 0; iteration < queries; iteration++ {
		query := feasibilityQuery(random, iteration)
		got, err := Execute(context.Background(), store, query)
		if err != nil {
			t.Fatalf("oracle query %d: %v", iteration, err)
		}
		var wantFiles, wantBytes uint64
		for _, fact := range oracleFacts {
			if oracleMatches(fact, query) {
				wantFiles++
				if fact.Known&schema.KnownSize != 0 {
					wantBytes += fact.LogicalSize
				}
			}
		}
		if got.Files == wantFiles && got.LogicalBytes == wantBytes {
			matches++
		}
	}
	return matches
}

func feasibilityQuery(random *rand.Rand, iteration int) Query {
	minimum := uint64(1 << (10 + random.Intn(14)))
	maximum := minimum + uint64(1<<(20+random.Intn(5)))
	query := Query{IncludeIncomplete: true, SizeMin: &minimum, SizeMax: &maximum}
	if iteration%2 == 0 {
		query.UIDs = []uint32{uint32(1000 + random.Intn(2048))}
	}
	if iteration%3 == 0 {
		query.Years = []int{2020 + random.Intn(7)}
	}
	if iteration%5 == 0 {
		query.GIDs = []uint32{uint32(100 + random.Intn(256))}
	}
	if iteration%7 == 0 {
		query.Residencies = []string{"archive-only"}
	}
	return query
}

func oracleMatches(fact schema.AnalyticsFactRecord, query Query) bool {
	containsUint32 := func(values []uint32, value uint32) bool {
		for _, item := range values {
			if item == value {
				return true
			}
		}
		return len(values) == 0
	}
	containsInt := func(values []int, value int) bool {
		for _, item := range values {
			if item == value {
				return true
			}
		}
		return len(values) == 0
	}
	if !containsUint32(query.UIDs, fact.UID) || !containsUint32(query.GIDs, fact.GID) ||
		!containsInt(query.Years, int(fact.CalendarYear)) {
		return false
	}
	if query.SizeMin != nil && (fact.Known&schema.KnownSize == 0 || fact.LogicalSize < *query.SizeMin) ||
		query.SizeMax != nil && (fact.Known&schema.KnownSize == 0 || fact.LogicalSize >= *query.SizeMax) {
		return false
	}
	if len(query.Residencies) != 0 && query.Residencies[0] == "archive-only" &&
		fact.Residency != schema.AnalyticsArchiveOnly {
		return false
	}
	return true
}

func feasibilityResidencyBytes(facts uint64) uint64 {
	var total uint64
	for remainder := uint64(0); remainder < 20; remainder++ {
		inode := remainder + 1
		value, _ := feasibilityResidency(inode).MarshalBinary()
		key := schema.AnalyticsResidencyKey(1, inode, inode)
		count := (facts + 19 - remainder) / 20
		total += count * uint64(len(key)+len(value))
	}
	return total
}

func feasibilityOutboxBytes(facts uint64) uint64 {
	delta := schema.AnalyticsDeltaRecord{
		Kind:                schema.AnalyticsDeltaCreation,
		FSID:                1,
		Inode:               1,
		IdentityGeneration:  1,
		Revision:            1,
		UID:                 600,
		GID:                 100,
		Known:               schema.KnownUID | schema.KnownGID | schema.KnownSize,
		CreatedAt:           1735689600000000000,
		LogicalSize:         1 << 20,
		CreationBasis:       schema.AnalyticsBirthTime,
		IdentityContinuity:  schema.AnalyticsContinuityProven,
		State:               schema.AnalyticsLive,
		ClassificationEpoch: 1,
	}
	value, _ := delta.MarshalBinary()
	return facts * uint64(len(schema.AnalyticsDeltaKey(1, 0))+len(value))
}

func feasibilityBitmapValues(store *feasibilityStore) uint64 { return store.bitmaps }

func feasibilityNamespace(key []byte) string {
	value := string(key)
	for _, namespace := range []string{"af", "am", "ai", "ad", "ar"} {
		if strings.HasPrefix(value, namespace+":") {
			return namespace
		}
	}
	if strings.HasPrefix(value, "aq:view:") || strings.HasPrefix(value, "g:") || strings.HasPrefix(value, "u:") {
		return "views"
	}
	if strings.HasPrefix(value, "ae:") {
		return "outbox"
	}
	if strings.HasPrefix(value, "aq:") {
		return "cache"
	}
	return "other"
}

func feasibilityCommit() string {
	if value := os.Getenv("VAULTIC_ANALYTICS_FEASIBILITY_COMMIT"); value != "" {
		return value
	}
	return "set VAULTIC_ANALYTICS_FEASIBILITY_COMMIT from git describe --always --dirty"
}

func feasibilityHardware() string {
	if value := os.Getenv("VAULTIC_ANALYTICS_FEASIBILITY_HARDWARE"); value != "" {
		return value
	}
	return "unspecified"
}

func feasibilityMarkdown(profile feasibilityProfile) string {
	return fmt.Sprintf(
		("# Phase 16 Analytics Feasibility: %d Facts\n\nDate: `%s`  \nCommit: `%s`  " +
			"\nEnvironment: `%s`; `%s %s/%s`, %d CPUs  \nSeed: `%d`; segment rows: `%d`; " +
			"codec: `json-columns-v1;zstd=3`\n\n| Metric | Result |\n|---|---:|\n| Core " +
			"bytes/fact | %.3f |\n| Projected core at 1.4B | %.3f GB |\n| Logical write " +
			"amplification | %.6fx |\n| Build/rebuild | %.3f s (%.0f facts/s) |\n| Cold " +
			"named query | %.3f s (%d files) |\n| Projected query at 1.4B | %.3f s |\n| " +
			"Oracle | %d/%d on %d facts |\n| Cached p95 | %.6f s |\n| Catch-up | %.3f s " +
			"(%.0f facts/s) |\n\n## Namespace Bytes\n\n```json\n%s\n```\n\n## " +
			"Gates\n\n```json\n%s\n```\n\nAuthoritative reconciliation CPU and metadata-write " +
			"baselines are `not_measured` by the direct-segment profile.\n"),
		profile.Facts,
		profile.Date,
		profile.Commit,
		profile.Hardware,
		profile.GoVersion,
		profile.GOOS,
		profile.GOARCH,
		profile.CPUs,
		profile.Seed,
		profile.SegmentRows,
		profile.CoreBytesFact,
		float64(profile.ProjectedCore)/1e9,
		profile.WriteAmp,
		profile.BuildSeconds,
		profile.BuildFactsSec,
		profile.BroadSeconds,
		profile.BroadFiles,
		profile.ProjectedSecs,
		profile.OracleMatches,
		profile.OracleQueries,
		profile.OracleFacts,
		profile.CacheP95,
		profile.CatchUpSecs,
		profile.CatchUpRate,
		mustJSON(profile.Namespaces),
		mustJSON(profile.Gates),
	)
}

func mustJSON(value any) string {
	encoded, _ := json.MarshalIndent(value, "", "  ")
	return string(encoded)
}
func passFail(pass bool) string {
	if pass {
		return "pass"
	}
	return "fail"
}
func minUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}
func maxUint64(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}
