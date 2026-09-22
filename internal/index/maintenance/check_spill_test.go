package maintenance

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/schema"
	monitor "github.com/otuschhoff/vaultic/internal/telemetry"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func testLocation(value byte) locationTuple {
	return locationTuple{BlobID: vaultic.ID{value}, PackID: vaultic.ID{value + 1}, Type: uint8(value % 2), Offset: uint64(value)}
}

func TestLocationTupleRoundTripAndOrder(t *testing.T) {
	left, right := testLocation(1), testLocation(2)
	encoded := left.marshalBinary()
	decoded, err := unmarshalLocationTuple(encoded[:])
	if err != nil || decoded != left || compareLocationTuple(left, right) >= 0 {
		t.Fatalf("decoded=%+v order=%d err=%v", decoded, compareLocationTuple(left, right), err)
	}
}

func TestLocationTupleDirectComparisonMatchesEncoding(t *testing.T) {
	random := rand.New(rand.NewSource(33)) //nolint:gosec // Deterministic test data does not require cryptographic randomness.
	for range 10_000 {
		var left, right locationTuple
		if _, err := random.Read(left.BlobID[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := random.Read(left.PackID[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := random.Read(right.BlobID[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := random.Read(right.PackID[:]); err != nil {
			t.Fatal(err)
		}
		left.Type, right.Type = uint8(random.Uint32()), uint8(random.Uint32())
		left.Offset, right.Offset = random.Uint64(), random.Uint64()
		left.Length, right.Length = random.Uint64(), random.Uint64()
		left.UncompressedLength, right.UncompressedLength = random.Uint64(), random.Uint64()
		leftEncoded, rightEncoded := left.marshalBinary(), right.marshalBinary()
		if got, want := compareLocationTuple(left, right), bytes.Compare(leftEncoded[:], rightEncoded[:]); cmpSign(got) != cmpSign(want) {
			t.Fatalf("comparison sign = %d, want %d", got, want)
		}
	}
}

func cmpSign(value int) int {
	switch {
	case value < 0:
		return -1
	case value > 0:
		return 1
	default:
		return 0
	}
}

func TestLocationSpoolSortsAndDeduplicatesWithinBound(t *testing.T) {
	parent := t.TempDir()
	scratch, err := newCheckScratch(parent, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := scratch.close(); err != nil {
			t.Fatal(err)
		}
	}()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleMemorySize*4, 8)
	if err != nil {
		t.Fatal(err)
	}
	for _, tuple := range []locationTuple{testLocation(3), testLocation(1), testLocation(3), testLocation(2)} {
		if err := spool.add(tuple); err != nil {
			t.Fatal(err)
		}
	}
	iterator, err := spool.iterator()
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.close()
	for expected := byte(1); expected <= 3; expected++ {
		tuple, found, err := iterator.next()
		if err != nil || !found || tuple != testLocation(expected) {
			t.Fatalf("tuple=%+v found=%t err=%v", tuple, found, err)
		}
	}
	if _, found, err := iterator.next(); err != nil || found {
		t.Fatalf("extra tuple found=%t err=%v", found, err)
	}
	if peak, _ := scratch.stats(); peak != 0 {
		t.Fatalf("fitting location spool used %d bytes of disk scratch", peak)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 || scratch.dir != "" {
		t.Fatalf("memory-only spool created disk scratch: entries=%d dir=%q err=%v", len(entries), scratch.dir, err)
	}
}

func TestLocationSpoolOverflowsToDiskAfterMemoryBudget(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleMemorySize*2, 8)
	if err != nil {
		t.Fatal(err)
	}
	for _, tuple := range []locationTuple{testLocation(3), testLocation(1), testLocation(2)} {
		if err := spool.add(tuple); err != nil {
			t.Fatal(err)
		}
	}
	iterator, err := spool.iterator()
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.close()
	if peak, _ := scratch.stats(); peak == 0 {
		t.Fatal("location spool exceeding memory budget did not use disk scratch")
	}
}

func TestLocationSpoolScenarioPreservesDiskResultsAndCleanup(t *testing.T) {
	profile, err := monitor.DecodeExperimentProfile([]byte(`{
		"schema_version":1,"profile_id":"check-scratch-test","enabled":true,"test_only":true,
		"scenario":"local","backend":"scratch","mode":"service","operation":"check",
		"role":"scratch","method":"put","access_pattern":"sequential","target_id":"scratch-target",
		"resource_id":"scratch-device","placement":"inside_service","latency_semantics":"service_completion",
		"interpretation":"additive","endpoint":"dependency","acknowledgement":"unknown",
		"delay_us":1000,"jitter_us":0,"tail_delay_us":0,"tail_every":0,"correlated_for":0,
		"bandwidth_bytes_per_second":0,"concurrency":1,"deadline_ms":0,"max_retries":0,
		"retry_error":"none","seed":34,"holds":["backend_capacity"]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	controller, err := monitor.NewScenarioHarness().Controller(profile, monitor.ExperimentTarget{
		ID: "scratch-target", Disposable: true, Confirmed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := newCheckScratchWithScenario(context.Background(), t.TempDir(), 1<<20, controller)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleMemorySize*2, 8)
	if err != nil {
		t.Fatal(err)
	}
	for _, tuple := range []locationTuple{testLocation(3), testLocation(1), testLocation(2)} {
		if err := spool.add(tuple); err != nil {
			t.Fatal(err)
		}
	}
	iterator, err := spool.iterator()
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.close()
	for expected := byte(1); expected <= 3; expected++ {
		tuple, found, nextErr := iterator.next()
		if nextErr != nil || !found || tuple != testLocation(expected) {
			t.Fatalf("tuple=%+v found=%t err=%v", tuple, found, nextErr)
		}
	}
	observation := controller.Observation()
	if observation.Completed == 0 || observation.Active != 0 {
		t.Fatalf("scratch scenario observation = %+v", observation)
	}
}

func TestLocationSpoolMergesBoundedMemoryChunks(t *testing.T) {
	parent := t.TempDir()
	scratch, err := newCheckScratch(parent, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleMemorySize*6, 8)
	if err != nil {
		t.Fatal(err)
	}
	spool.chunkItems = 2
	for _, tuple := range []locationTuple{
		testLocation(5), testLocation(1), testLocation(3), testLocation(1), testLocation(4), testLocation(2),
	} {
		if err := spool.add(tuple); err != nil {
			t.Fatal(err)
		}
	}
	iterator, err := spool.iterator()
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.close()
	for expected := byte(1); expected <= 5; expected++ {
		tuple, found, err := iterator.next()
		if err != nil || !found || tuple != testLocation(expected) {
			t.Fatalf("tuple=%+v found=%t err=%v", tuple, found, err)
		}
	}
	if _, found, err := iterator.next(); err != nil || found {
		t.Fatalf("extra tuple found=%t err=%v", found, err)
	}
	var capacityBytes uint64
	for _, run := range spool.memoryRuns {
		capacityBytes += uint64(cap(run)) * locationTupleMemorySize
	}
	if capacityBytes != spool.memoryUsed || capacityBytes > spool.memoryBytes {
		t.Fatalf("memory capacity=%d accounted=%d limit=%d", capacityBytes, spool.memoryUsed, spool.memoryBytes)
	}
	if entries, err := os.ReadDir(parent); err != nil || len(entries) != 0 {
		t.Fatalf("bounded memory runs created disk scratch: entries=%d err=%v", len(entries), err)
	}
}

func TestLocationSpoolRetriesPartialMemoryRunSpillWithoutDuplicates(t *testing.T) {
	recordBytes := uint64(4 + locationTupleSize + 16)
	scratch, err := newCheckScratch(t.TempDir(), checkRunHeaderSize+2*recordBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleMemorySize*2, 8)
	if err != nil {
		t.Fatal(err)
	}
	spool.chunkItems = 1
	if err := spool.add(testLocation(2)); err != nil {
		t.Fatal(err)
	}
	if err := spool.add(testLocation(1)); err != nil {
		t.Fatal(err)
	}
	if err := spool.add(testLocation(3)); err == nil {
		t.Fatal("partial memory-run spill unexpectedly fit scratch budget")
	}
	if len(spool.runs) != 1 || len(spool.memoryRuns) != 1 || spool.memoryUsed != locationTupleMemorySize {
		t.Fatalf("partial spill state: disk=%d memory=%d used=%d", len(spool.runs), len(spool.memoryRuns), spool.memoryUsed)
	}
	scratch.maxBytes += 2 * (checkRunHeaderSize + recordBytes)
	if err := spool.add(testLocation(3)); err != nil {
		t.Fatal(err)
	}
	iterator, err := spool.iterator()
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.close()
	for expected := byte(1); expected <= 3; expected++ {
		tuple, found, err := iterator.next()
		if err != nil || !found || tuple != testLocation(expected) {
			t.Fatalf("tuple=%+v found=%t err=%v", tuple, found, err)
		}
	}
	if _, found, err := iterator.next(); err != nil || found {
		t.Fatalf("extra tuple found=%t err=%v", found, err)
	}
}

func TestLocationSpoolUsesBoundedFanInMergePasses(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleMemorySize, 2)
	if err != nil {
		t.Fatal(err)
	}
	for value := byte(9); value > 0; value-- {
		if err := spool.add(testLocation(value)); err != nil {
			t.Fatal(err)
		}
	}
	iterator, err := spool.iterator()
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.close()
	for expected := byte(1); expected <= 9; expected++ {
		tuple, found, err := iterator.next()
		if err != nil || !found || tuple != testLocation(expected) {
			t.Fatalf("tuple=%+v found=%t err=%v", tuple, found, err)
		}
	}
}

func TestLocationMultisetSpoolPreservesDuplicatesAcrossMergePasses(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationMultisetSpool(context.Background(), scratch, locationTupleMemorySize, 2)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if err := spool.add(testLocation(1)); err != nil {
			t.Fatal(err)
		}
	}
	iterator, err := spool.iterator()
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.close()
	var count int
	for {
		_, found, err := iterator.next()
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
		count++
	}
	if count != 5 {
		t.Fatalf("duplicate count = %d, want 5", count)
	}
}

func TestPackContributionIteratorPreservesAccountingMultiplicity(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationMultisetSpool(context.Background(), scratch, locationTupleMemorySize, 2)
	if err != nil {
		t.Fatal(err)
	}
	packID := vaultic.ID{9}
	for _, tuple := range []locationTuple{
		{BlobID: packID},
		{BlobID: packID, PackID: vaultic.ID{1}, Type: uint8(schema.BlobData), Length: 7},
		{BlobID: packID, PackID: vaultic.ID{1}, Type: uint8(schema.BlobData), Length: 7},
		{BlobID: packID, PackID: vaultic.ID{2}, Type: uint8(schema.BlobTree), Length: 11},
	} {
		if err := spool.add(tuple); err != nil {
			t.Fatal(err)
		}
	}
	iterator, err := newPackContributionIterator(spool)
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.close()
	summary, found, err := iterator.next()
	if err != nil || !found {
		t.Fatalf("summary found=%t err=%v", found, err)
	}
	if summary.id != packID || !summary.present || summary.count != 3 || summary.payload != 25 ||
		classifyPackSummary(summary.types) != schema.PackMixed {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

func TestCompareLocationSpoolsCountsAsymmetricInputsOnce(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	legacy, err := newLocationSpool(context.Background(), scratch, locationTupleMemorySize, 2)
	if err != nil {
		t.Fatal(err)
	}
	slatedb, err := newLocationSpool(context.Background(), scratch, locationTupleMemorySize, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []byte{1, 2, 3} {
		if err := legacy.add(testLocation(value)); err != nil {
			t.Fatal(err)
		}
	}
	for _, value := range []byte{2, 4} {
		if err := slatedb.add(testLocation(value)); err != nil {
			t.Fatal(err)
		}
	}
	var result CheckResult
	if err := compareLocationSpools(legacy, slatedb, &result, 0); err != nil {
		t.Fatal(err)
	}
	if result.LegacyLocations != 3 || result.SlateDBLocations != 2 ||
		result.MissingInSlateDB != 2 || result.MissingInLegacy != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestLocationSpoolRejectsScratchOverflowAndCorruption(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), checkRunHeaderSize+4+locationTupleSize+16)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleMemorySize, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.add(testLocation(1)); err != nil {
		t.Fatal(err)
	}
	if err := spool.add(testLocation(2)); err == nil {
		t.Fatal("scratch overflow was accepted")
	}
	data, err := os.ReadFile(spool.runs[0].path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err := os.WriteFile(spool.runs[0].path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.iterator(); err == nil {
		t.Fatal("corrupt encrypted run was accepted")
	}
}

func TestLocationSpoolHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(ctx, scratch, locationTupleMemorySize, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.add(testLocation(1)); err != context.Canceled {
		t.Fatalf("add error = %v", err)
	}
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

type chunkWriter struct{ bytes []byte }

func (writer *chunkWriter) Write(value []byte) (int, error) {
	count := min(2, len(value))
	writer.bytes = append(writer.bytes, value[:count]...)
	return count, nil
}

func TestWriteAllHandlesShortWrites(t *testing.T) {
	writer := &chunkWriter{}
	if err := writeAll(writer, []byte("abcdef")); err != nil || string(writer.bytes) != "abcdef" {
		t.Fatalf("bytes=%q err=%v", writer.bytes, err)
	}
	if err := writeAll(zeroWriter{}, []byte("x")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero-length write error = %v", err)
	}
}

func TestBufferedScratchAccountingPreservesBytes(t *testing.T) {
	telemetry := NewCheckTelemetry()
	operation := telemetry.start()
	defer operation.Done(monitor.OutcomeSuccess)
	scratch := &checkScratch{ctx: context.Background(), telemetry: telemetry, operation: operation}
	request := telemetry.startScratch()
	var output bytes.Buffer
	buffered := bufio.NewWriterSize(&scratchOutputWriter{scratch: scratch, request: request, writer: &output}, 16)
	for range 100 {
		if err := writeAll(buffered, []byte("abcdef")); err != nil {
			t.Fatal(err)
		}
	}
	if err := buffered.Flush(); err != nil {
		t.Fatal(err)
	}
	settleDependency(request, nil)
	partial := &chunkWriter{}
	request = telemetry.startScratch()
	if err := writeScratch(scratch, request, partial, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	settleDependency(request, nil)
	if output.Len() != 600 || string(partial.bytes) != "abcdef" {
		t.Fatal("buffered or short-write output changed")
	}
	var actionBytes, dependencyBytes uint64
	for _, metric := range telemetry.Component(time.Now()).Metrics {
		for _, label := range metric.Labels {
			if label.Name == "role" && label.Value == "scratch" {
				if metric.Name == "operation_processed_bytes" {
					actionBytes += metric.Value
				}
				if metric.Name == "dependency_bytes" {
					dependencyBytes += metric.Value
				}
			}
		}
	}
	if actionBytes != 606 || dependencyBytes != 606 {
		t.Fatalf("action bytes=%d dependency bytes=%d, want 606", actionBytes, dependencyBytes)
	}
}

func TestLocationSpoolRejectsTruncatedRun(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleMemorySize, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.add(testLocation(1)); err != nil {
		t.Fatal(err)
	}
	if err := spool.finishBuffer(); err != nil {
		t.Fatal(err)
	}
	if err := spool.spillMemoryRuns(); err != nil {
		t.Fatal(err)
	}
	run := spool.runs[0]
	if err := os.Truncate(run.path, int64(run.size-1)); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.iterator(); err == nil {
		t.Fatal("truncated encrypted run was accepted")
	}
}

func TestCheckScratchRefusesCleanupWithoutOwnershipMarker(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := scratch.nextPath(); err != nil {
		t.Fatal(err)
	}
	markerPath := filepath.Join(scratch.dir, ".vaultic-check-owned")
	if err := os.WriteFile(markerPath, []byte("not-the-marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := scratch.close(); err == nil {
		t.Fatal("cleanup accepted a mismatched ownership marker")
	}
	if _, err := os.Stat(scratch.dir); err != nil {
		t.Fatalf("unowned scratch directory was removed: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte(scratch.marker), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := scratch.close(); err != nil {
		t.Fatal(err)
	}
}

func TestLocationSpoolMergeHonorsScratchBudget(t *testing.T) {
	recordBytes := uint64(4 + locationTupleSize + 16)
	scratch, err := newCheckScratch(t.TempDir(), 3*(checkRunHeaderSize+recordBytes))
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleMemorySize, 2)
	if err != nil {
		t.Fatal(err)
	}
	for value := byte(1); value <= 3; value++ {
		if err := spool.add(testLocation(value)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := spool.iterator(); err == nil {
		t.Fatal("merge exceeded the scratch budget")
	}
}

func TestPackSummaryCompactsMemoryWithinHeadroom(t *testing.T) {
	for _, mode := range []string{"compact", "no_headroom", "limited", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			scratch, err := newCheckScratch(t.TempDir(), 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			defer scratch.close()
			spool, err := newLocationMultisetSpool(ctx, scratch, 1<<20, 2)
			if err != nil {
				t.Fatal(err)
			}
			spool.packSummaries = true
			for range 32 {
				records := make([]locationTuple, 128)
				for index := range records {
					records[index] = locationTuple{BlobID: vaultic.ID{byte(index)}, Type: 1, Offset: 2, Length: 6}
				}
				spool.memoryRuns = append(spool.memoryRuns, records)
				spool.memoryUsed += uint64(cap(records)) * locationTupleMemorySize
			}
			initialUsed := spool.memoryUsed
			switch mode {
			case "no_headroom":
				spool.memoryBytes = initialUsed
			case "limited":
				spool.memoryBytes = initialUsed + packContributionEntryBudget
			case "cancel":
				cancel()
			}
			iterator, err := newPackContributionIterator(spool)
			if mode == "cancel" {
				if !errors.Is(err, context.Canceled) || spool.memoryUsed != initialUsed {
					t.Fatalf("cancel err=%v used=%d", err, spool.memoryUsed)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				wantRuns := 32
				if mode == "compact" {
					wantRuns = 1
				}
				if len(spool.memoryRuns) != wantRuns || spool.memoryUsed > spool.memoryBytes {
					t.Fatal("unexpected memory compaction")
				}
				for index := range 128 {
					summary, found, err := iterator.next()
					if err != nil || !found || summary.id != (vaultic.ID{byte(index)}) || summary.count != 64 || summary.payload != 192 {
						t.Fatalf("summary=%+v found=%t err=%v", summary, found, err)
					}
				}
				if _, found, err := iterator.next(); found || err != nil {
					t.Fatalf("EOF found=%t err=%v", found, err)
				}
				if err := iterator.close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := spool.close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPackContributionBufferMatchesSpool(t *testing.T) {
	for _, limit := range []int{1, 3, 128} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			var expected []packContributionSummary
			for _, aggregate := range []bool{false, true} {
				scratch, err := newCheckScratch(t.TempDir(), 8<<20)
				if err != nil {
					t.Fatal(err)
				}
				defer scratch.close()
				spool, err := newLocationMultisetSpool(context.Background(), scratch, 8*locationTupleMemorySize, 2)
				if err != nil {
					t.Fatal(err)
				}
				spool.packSummaries = true
				buffer := &packContributionBuffer{spool: spool, limit: limit}
				for ordinal := 0; ordinal < 256; ordinal++ {
					for _, blobType := range []schema.BlobType{schema.BlobData, schema.BlobTree, schema.BlobData} {
						id := vaultic.ID{byte(ordinal % 17)}
						if aggregate {
							err = buffer.add(id, blobType, 7)
							if len(buffer.packs) > limit {
								t.Fatal("map exceeded entry limit")
							}
						} else {
							err = spool.add(locationTuple{BlobID: id, Type: uint8(blobType), Length: 7})
						}
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := buffer.flush(); err != nil {
					t.Fatal(err)
				}
				if err := buffer.flush(); err != nil {
					t.Fatal(err)
				}
				iterator, err := newPackContributionIterator(spool)
				if err != nil {
					t.Fatal(err)
				}
				var actual []packContributionSummary
				for {
					summary, found, err := iterator.next()
					if err != nil {
						t.Fatal(err)
					}
					if !found {
						break
					}
					actual = append(actual, summary)
				}
				if aggregate && !slices.Equal(actual, expected) {
					t.Fatalf("summaries=%+v want=%+v", actual, expected)
				}
				expected = actual
				if err := errors.Join(iterator.close(), spool.close()); err != nil {
					t.Fatal(err)
				}
				if scratch.used != 0 {
					t.Fatal("reservation leak")
				}
			}
		})
	}
}

func TestPackContributionBufferFailureIsSticky(t *testing.T) {
	for _, mode := range []string{"cancel", "scratch", "count", "payload"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			scratch, err := newCheckScratch(t.TempDir(), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer scratch.close()
			spool, err := newLocationMultisetSpool(ctx, scratch, locationTupleMemorySize, 2)
			if err != nil {
				t.Fatal(err)
			}
			spool.packSummaries, spool.diskMode = true, true
			buffer := &packContributionBuffer{spool: spool, limit: 1}
			id := vaultic.ID{1}
			if err := buffer.add(id, schema.BlobData, 1); err != nil {
				t.Fatal(err)
			}
			summary := buffer.packs[id]
			switch mode {
			case "cancel":
				cancel()
			case "count":
				summary.count = ^uint64(0)
			case "payload":
				summary.payload = ^uint64(0)
			}
			buffer.packs[id] = summary
			if mode == "scratch" {
				err = buffer.flush()
			} else {
				err = buffer.add(id, schema.BlobData, 1)
			}
			if err == nil {
				t.Fatal("accepted failed aggregation")
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel: %v", err)
			}
			scratch.maxBytes = 1 << 20
			if buffer.flush() != err || buffer.add(id, schema.BlobData, 1) != err {
				t.Fatal("failure was not sticky")
			}
			if err := spool.close(); err != nil {
				t.Fatal(err)
			}
			if scratch.used != 0 {
				t.Fatal("reservation leak")
			}
		})
	}
}

func TestPackSummarySpoolMatchesMultiset(t *testing.T) {
	for _, memory := range []uint64{8 * locationTupleMemorySize, 1 << 20} {
		t.Run(fmt.Sprintf("memory=%d", memory), func(t *testing.T) {
			var expected []packContributionSummary
			var baselineBytes uint64
			for _, compact := range []bool{false, true} {
				scratch, err := newCheckScratch(t.TempDir(), 8<<20)
				if err != nil {
					t.Fatal(err)
				}
				defer scratch.close()
				spool, err := newLocationMultisetSpool(context.Background(), scratch, memory, 2)
				if err != nil {
					t.Fatal(err)
				}
				spool.packSummaries, spool.mergeWorkers = compact, 4
				for ordinal := 0; ordinal < 256; ordinal++ {
					for _, tuple := range []locationTuple{
						{BlobID: vaultic.ID{1}, PackID: vaultic.ID{byte(ordinal)}, Type: uint8(schema.BlobData), Length: 3},
						{BlobID: vaultic.ID{1}, PackID: vaultic.ID{byte(ordinal)}, Type: uint8(schema.BlobData), Length: 3},
						{BlobID: vaultic.ID{1}, Type: uint8(schema.BlobTree), Length: 5},
						{BlobID: vaultic.ID{2}},
					} {
						if err := spool.add(tuple); err != nil {
							t.Fatal(err)
						}
					}
				}
				iterator, err := newPackContributionIterator(spool)
				if err != nil {
					t.Fatal(err)
				}
				var actual []packContributionSummary
				for {
					summary, found, err := iterator.next()
					if err != nil {
						t.Fatal(err)
					}
					if !found {
						break
					}
					actual = append(actual, summary)
				}
				if err := iterator.close(); err != nil {
					t.Fatal(err)
				}
				if compact {
					if !slices.Equal(actual, expected) {
						t.Fatalf("summaries=%+v want=%+v", actual, expected)
					}
					if len(spool.runs) > 0 && scratch.used >= baselineBytes {
						t.Fatal("summary runs did not shrink")
					}
				} else {
					expected, baselineBytes = actual, scratch.used
				}
				if len(actual) != 2 || actual[0].count != 768 || actual[0].payload != 2816 || !actual[1].present {
					t.Fatalf("unexpected summaries: %+v", actual)
				}
				if err := spool.close(); err != nil {
					t.Fatal(err)
				}
				if scratch.used != 0 {
					t.Fatal("reservations leaked")
				}
			}
		})
	}
}

func TestPackSummarySpoolRetriesFailedWrite(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationMultisetSpool(context.Background(), scratch, 8*locationTupleMemorySize, 2)
	if err != nil {
		t.Fatal(err)
	}
	spool.packSummaries, spool.diskMode = true, true
	for range 4 {
		if err := spool.add(locationTuple{BlobID: vaultic.ID{1}, Type: uint8(schema.BlobData), Length: 3}); err != nil {
			t.Fatal(err)
		}
	}
	if err := spool.finishBuffer(); err == nil {
		t.Fatal("accepted output without scratch headroom")
	}
	scratch.maxBytes = 1 << 20
	iterator, err := newPackContributionIterator(spool)
	if err != nil {
		t.Fatal(err)
	}
	summary, found, err := iterator.next()
	if err != nil || !found || summary.count != 4 || summary.payload != 12 {
		t.Fatalf("retry summary=%+v found=%t err=%v", summary, found, err)
	}
	if _, found, err := iterator.next(); found || err != nil {
		t.Fatalf("EOF found=%t err=%v", found, err)
	}
	if err := errors.Join(iterator.close(), spool.close()); err != nil {
		t.Fatal(err)
	}
	if scratch.used != 0 {
		t.Fatal("reservations leaked")
	}
}

func TestPackPartialSummaryOverflow(t *testing.T) {
	for _, countOverflow := range []bool{false, true} {
		first := locationTuple{BlobID: vaultic.ID{1}, Type: 1, Offset: 1, Length: 1}
		second := first
		if countOverflow {
			first.Offset = ^uint64(0)
		} else {
			first.Length = ^uint64(0)
		}
		iterator, err := newLocationIterator(context.Background(), nil, [][]locationTuple{{first}, {second}}, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		contributions := &packContributionIterator{iterator: iterator, partials: true}
		if _, _, err := contributions.next(); err == nil {
			t.Fatal("accepted partial summary overflow")
		}
		if err := contributions.close(); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkLocationRunReader(b *testing.B) {
	scratch, err := newCheckScratch(b.TempDir(), 1<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(context.Background(), scratch, 1<<20, 32)
	if err != nil {
		b.Fatal(err)
	}
	records := make([]locationTuple, 4096)
	for index := range records {
		records[index] = testLocation(byte(index))
	}
	run, err := spool.writeRun(records)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		reader, err := openLocationRun(run, scratch)
		if err != nil {
			b.Fatal(err)
		}
		for _, expected := range records {
			actual, found, err := reader.next()
			if err != nil || !found || actual != expected {
				b.Fatalf("record mismatch: found=%t err=%v", found, err)
			}
		}
		if _, found, err := reader.next(); found || err != nil {
			b.Fatalf("EOF: found=%t err=%v", found, err)
		}
		if err := reader.close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLocationIteratorMerge(b *testing.B) {
	memoryRuns := make([][]locationTuple, 32)
	for runIndex := range memoryRuns {
		memoryRuns[runIndex] = make([]locationTuple, 1024)
		for tupleIndex := range memoryRuns[runIndex] {
			ordinal := tupleIndex*len(memoryRuns) + runIndex
			memoryRuns[runIndex][tupleIndex] = locationTuple{BlobID: vaultic.ID{byte(ordinal >> 8), byte(ordinal)}}
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		iterator, err := newLocationIterator(context.Background(), nil, memoryRuns, nil, false)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for {
			_, found, err := iterator.next()
			if err != nil {
				b.Fatal(err)
			}
			if !found {
				break
			}
			count++
		}
		if count != 32*1024 {
			b.Fatalf("read %d tuples", count)
		}
		if err := iterator.close(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestLocationSpoolParallelMergeAdmissionAndCleanup(t *testing.T) {
	for _, mode := range []string{"complete", "headroom", "memory", "cancel", "corrupt"} {
		for _, deduplicate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deduplicate=%t", mode, deduplicate), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					parent := t.TempDir()
					scratch, err := newCheckScratchWithScenario(ctx, parent, 1<<20, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer scratch.close()
					scratch.telemetry = NewCheckTelemetry()
					spool, err := newLocationSpoolMode(ctx, scratch, 16<<20, 2, deduplicate)
					if err != nil {
						t.Fatal(err)
					}
					spool.mergeWorkers = 4
					var expected []locationTuple
					for ordinal := byte(0); ordinal < 8; ordinal++ {
						records := []locationTuple{testLocation(ordinal + 1), testLocation(ordinal + 2)}
						run, err := spool.writeRun(records)
						if err != nil {
							t.Fatal(err)
						}
						spool.runs = append(spool.runs, run)
						expected = append(expected, records...)
					}
					initialUsed := scratch.used
					if mode == "headroom" {
						scratch.maxBytes = initialUsed + checkRunHeaderSize + 8*(4+locationTupleSize+16)
					}
					if mode == "memory" {
						spool.memoryBytes = 3 << 20
					}
					if mode == "corrupt" {
						if err := os.WriteFile(spool.runs[2].path, []byte("corrupt"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					profile, err := monitor.DecodeExperimentProfile([]byte(`{
						"schema_version":1,"profile_id":"check-merge-test","enabled":true,"test_only":true,
						"scenario":"local","backend":"scratch","mode":"service","operation":"check",
						"role":"scratch","method":"put","access_pattern":"sequential","target_id":"scratch-target",
						"resource_id":"scratch-device","placement":"inside_service","latency_semantics":"service_completion",
						"interpretation":"additive","endpoint":"dependency","acknowledgement":"unknown",
						"delay_us":1000000,"jitter_us":0,"tail_delay_us":0,"tail_every":0,"correlated_for":0,
						"bandwidth_bytes_per_second":0,"concurrency":32,"deadline_ms":0,"max_retries":0,
						"retry_error":"none","seed":34,"holds":["backend_capacity"]
					}`))
					if err != nil {
						t.Fatal(err)
					}
					controller, err := monitor.NewScenarioHarness().Controller(profile, monitor.ExperimentTarget{
						ID: "scratch-target", Disposable: true, Confirmed: true,
					})
					if err != nil {
						t.Fatal(err)
					}
					scratch.scenario = controller
					result := make(chan error, 1)
					go func() { result <- spool.seal() }()
					synctest.Wait()
					if mode != "corrupt" {
						want := uint64(4)
						if mode == "headroom" || mode == "memory" {
							want = 1
						}
						if active := controller.Observation().Active; active != want {
							t.Errorf("active merge writers=%d, want %d", active, want)
						}
					}
					if mode == "cancel" {
						cancel()
					}
					err = <-result
					if mode == "cancel" {
						if !errors.Is(err, context.Canceled) {
							t.Fatalf("cancellation: %v", err)
						}
					} else if mode == "corrupt" {
						if err == nil {
							t.Fatal("accepted corrupt run")
						}
					} else if err != nil {
						t.Fatal(err)
					}
					if controller.Observation().Active != 0 {
						t.Fatal("merge workers still active")
					}
					scratch.scenario = nil
					if mode == "cancel" || mode == "corrupt" {
						if scratch.used != initialUsed {
							t.Fatalf("output reservations leaked: %d vs %d", scratch.used, initialUsed)
						}
					} else {
						iterator, err := spool.iterator()
						if err != nil {
							t.Fatal(err)
						}
						var actual []locationTuple
						for {
							tuple, found, err := iterator.next()
							if err != nil {
								t.Fatal(err)
							}
							if !found {
								break
							}
							actual = append(actual, tuple)
						}
						if err := iterator.close(); err != nil {
							t.Fatal(err)
						}
						slices.SortFunc(expected, compareLocationTuple)
						if deduplicate {
							expected = slices.Compact(expected)
						}
						if !slices.Equal(actual, expected) {
							t.Fatal("merge changed ordered tuples")
						}
					}
					if err := spool.close(); err != nil {
						t.Fatal(err)
					}
					if scratch.used != 0 || scratch.peak > scratch.maxBytes {
						t.Fatalf("scratch used=%d peak=%d limit=%d", scratch.used, scratch.peak, scratch.maxBytes)
					}
					if err := scratch.close(); err != nil {
						t.Fatal(err)
					}
					entries, err := os.ReadDir(parent)
					if err != nil || len(entries) != 0 {
						t.Fatalf("cleanup entries=%d err=%v", len(entries), err)
					}
				})
			})
		}
	}
}

func TestLegacyInventoryDigestIsCanonicalAndSensitive(t *testing.T) {
	first, second := vaultic.ID{1}, vaultic.ID{2}
	source := &memoryDestination{
		indexes:   map[vaultic.ID][]byte{second: []byte("bb"), first: []byte("a")},
		snapshots: map[vaultic.ID][]byte{first: []byte("snapshot")},
	}
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	left, err := legacyInventoryDigest(context.Background(), source, scratch, 512)
	if err != nil {
		t.Fatal(err)
	}
	right, err := legacyInventoryDigest(context.Background(), source, scratch, 512)
	if err != nil {
		t.Fatal(err)
	}
	source.indexes[first] = []byte("changed-size")
	changed, err := legacyInventoryDigest(context.Background(), source, scratch, 512)
	if err != nil {
		t.Fatal(err)
	}
	if left != right || left == changed {
		t.Fatalf("digests left=%q right=%q changed=%q", left, right, changed)
	}
}
