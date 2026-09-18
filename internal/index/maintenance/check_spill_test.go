package maintenance

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/otuschhoff/vaultic/internal/index/schema"
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

func TestLocationSpoolSortsAndDeduplicatesWithinBound(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	dir := scratch.dir
	defer func() {
		if err := scratch.close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("scratch still exists: %v", err)
		}
	}()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleSize*2, 8)
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
}

func TestLocationSpoolUsesBoundedFanInMergePasses(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleSize, 2)
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
	spool, err := newLocationMultisetSpool(context.Background(), scratch, locationTupleSize, 2)
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
	spool, err := newLocationMultisetSpool(context.Background(), scratch, locationTupleSize, 2)
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
	legacy, err := newLocationSpool(context.Background(), scratch, locationTupleSize, 2)
	if err != nil {
		t.Fatal(err)
	}
	slatedb, err := newLocationSpool(context.Background(), scratch, locationTupleSize, 2)
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
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleSize, 2)
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
	spool, err := newLocationSpool(ctx, scratch, locationTupleSize, 2)
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

func TestLocationSpoolRejectsTruncatedRun(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleSize, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.add(testLocation(1)); err != nil {
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
	spool, err := newLocationSpool(context.Background(), scratch, locationTupleSize, 2)
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
