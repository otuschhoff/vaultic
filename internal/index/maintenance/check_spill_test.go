package maintenance

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
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
