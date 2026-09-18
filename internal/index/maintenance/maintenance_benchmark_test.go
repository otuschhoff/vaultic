package maintenance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const benchmarkBlobsPerPack = 64

type benchmarkStore struct {
	*memoryStore
	keys []string
}

func (store *benchmarkStore) ScanPrefix(_ context.Context, prefix, after []byte, limit uint32) ([]daemon.KeyValue, bool, error) {
	start := sort.SearchStrings(store.keys, string(prefix))
	if len(after) != 0 {
		start = sort.Search(len(store.keys), func(index int) bool { return store.keys[index] > string(after) })
	}
	entries := make([]daemon.KeyValue, 0, limit)
	for index := start; index < len(store.keys) && bytes.HasPrefix([]byte(store.keys[index]), prefix); index++ {
		key := store.keys[index]
		entries = append(entries, daemon.KeyValue{Key: []byte(key), Value: store.values[key]})
		if len(entries) == int(limit) {
			return entries, false, nil
		}
	}
	return entries, true, nil
}

type checkBenchmarkFixture struct {
	source    *memoryDestination
	store     *benchmarkStore
	locations uint64
}

func benchmarkID(domain byte, number uint64) vaultic.ID {
	var id vaultic.ID
	id[0] = domain
	binary.BigEndian.PutUint64(id[len(id)-8:], number)
	return id
}

func benchmarkSet(tb testing.TB, store *memoryStore, key []byte, record interface{ MarshalBinary() ([]byte, error) }) {
	tb.Helper()
	value, err := record.MarshalBinary()
	if err != nil {
		tb.Fatal(err)
	}
	store.values[string(key)] = value
}

func newCheckBenchmarkFixture(tb testing.TB, packCount int) checkBenchmarkFixture {
	tb.Helper()
	store := &memoryStore{values: make(map[string][]byte, packCount*(benchmarkBlobsPerPack+1)+16)}
	source := &memoryDestination{indexes: make(map[vaultic.ID][]byte)}
	accumulator := schema.NewPackAggregateAccumulator()
	const packsPerIndex = 64
	for firstPack := 0; firstPack < packCount; firstPack += packsPerIndex {
		index := legacyindex.NewIndex()
		for packNumber := firstPack; packNumber < min(firstPack+packsPerIndex, packCount); packNumber++ {
			packID := benchmarkID(1, uint64(packNumber))
			blobs := make(pack.Blobs, benchmarkBlobsPerPack)
			for blobNumber := range benchmarkBlobsPerPack {
				blobID := benchmarkID(2, uint64(packNumber*benchmarkBlobsPerPack+blobNumber))
				offset := uint64(blobNumber * 128)
				blobs[blobNumber] = pack.Blob{
					BlobHandle: vaultic.BlobHandle{ID: blobID, Type: vaultic.DataBlob},
					Offset:     uint(offset), Length: 128, UncompressedLength: 128,
				}
				benchmarkSet(tb, store, schema.BlobKey(schema.ID(blobID)), schema.BlobRecord{Locations: []schema.BlobLocation{{
					PackID: schema.ID(packID), Offset: offset, Length: 128, UncompressedSize: 128, Type: schema.BlobData,
				}}})
			}
			record := schema.PackRecord{
				Type: schema.PackData, PhysicalSize: benchmarkBlobsPerPack * 128,
				PayloadSize: benchmarkBlobsPerPack * 128, BlobCount: benchmarkBlobsPerPack,
				PhysicalSizeKnown: true, Lifecycle: schema.PackPublished, Tier: schema.TierHot,
				UsageKnown: true, UsedPayloadBytes: benchmarkBlobsPerPack * 128,
			}
			benchmarkSet(tb, store, schema.PackKey(schema.ID(packID)), record)
			if err := accumulator.Add(record); err != nil {
				tb.Fatal(err)
			}
			index.StorePack(packID, blobs)
		}
		index.Finalize()
		var encoded bytes.Buffer
		if err := index.Encode(&encoded); err != nil {
			tb.Fatal(err)
		}
		data := encoded.Bytes()
		source.indexes[vaultic.Hash(data)] = append([]byte(nil), data...)
	}
	aggregates, tierAggregates := accumulator.Results(1)
	for kind, aggregate := range aggregates {
		benchmarkSet(tb, store, schema.PackAggregateKey(kind), aggregate)
	}
	for tier, aggregate := range tierAggregates {
		benchmarkSet(tb, store, schema.TierAggregateKey(tier), aggregate)
	}
	benchmarkSet(tb, store, schema.AnalyticsMetadataKey(), schema.AnalyticsMetadataRecord{Enabled: false})
	keys := make([]string, 0, len(store.values))
	for key := range store.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return checkBenchmarkFixture{
		source: source, store: &benchmarkStore{memoryStore: store, keys: keys},
		locations: uint64(packCount * benchmarkBlobsPerPack),
	}
}

func checkBenchmarkDigest(result CheckResult) [sha256.Size]byte {
	result.Resources = CheckResources{}
	result.Consistency.OptionsDigest = ""
	encoded, _ := json.Marshal(result)
	return sha256.Sum256(encoded)
}

func checkBenchmarkMemory(b *testing.B) uint64 {
	b.Helper()
	configured := os.Getenv("VAULTIC_CHECK_BENCH_MEMORY_BYTES")
	if configured == "" {
		return 8 << 20
	}
	memory, err := strconv.ParseUint(configured, 10, 64)
	if err != nil {
		b.Fatalf("parse VAULTIC_CHECK_BENCH_MEMORY_BYTES: %v", err)
	}
	return memory
}

func BenchmarkCheckWithOptions(b *testing.B) {
	for _, scale := range []struct {
		name  string
		packs int
	}{{"synthetic-1x", 256}, {"synthetic-10x", 2560}} {
		b.Run(scale.name, func(b *testing.B) {
			b.StopTimer()
			fixture := newCheckBenchmarkFixture(b, scale.packs)
			memoryBytes := checkBenchmarkMemory(b)
			var expectedDigest [sha256.Size]byte
			for _, workers := range []uint{1, 2, 4, 8, 0} {
				name := fmt.Sprintf("workers=%d", workers)
				if workers == 0 {
					name = "workers=available"
				}
				b.Run(name, func(b *testing.B) {
					tempDir := os.Getenv("VAULTIC_CHECK_BENCH_TEMP_DIR")
					if tempDir == "" {
						tempDir = b.TempDir()
					}
					loggedDigest := false
					b.ReportAllocs()
					b.SetBytes(int64(fixture.locations * locationTupleSize))
					for b.Loop() {
						result, err := CheckWithOptions(context.Background(), fixture.source, fixture.store, CheckOptions{
							MaxFindings: 100, MemoryBytes: memoryBytes, TempDir: tempDir,
							TempMaxBytes: 8 << 30, Workers: workers, RPCConcurrency: workers,
						})
						if err != nil {
							b.Fatal(err)
						}
						if !result.Clean() || result.LegacyLocations != fixture.locations || result.SlateDBLocations != fixture.locations {
							b.Fatalf("unexpected benchmark result: %+v", result)
						}
						digest := checkBenchmarkDigest(result)
						if expectedDigest == ([sha256.Size]byte{}) {
							expectedDigest = digest
						} else if digest != expectedDigest {
							b.Fatalf("result digest changed: got %x, want %x", digest, expectedDigest)
						}
						if !loggedDigest {
							b.Logf("input_inventory_sha256=%s result_sha256=%x", result.Consistency.LegacyInventoryDigest, digest)
							loggedDigest = true
						}
						b.ReportMetric(float64(result.Resources.ScratchPeakBytes), "scratch-peak-B")
						b.ReportMetric(float64(result.Resources.MergePasses), "merge-passes")
						b.ReportMetric(float64(memoryBytes), "memory-limit-B")
						b.ReportMetric(float64(fixture.locations), "locations")
					}
				})
			}
		})
	}
}
