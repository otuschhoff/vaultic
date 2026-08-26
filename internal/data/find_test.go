package data_test

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func loadIDSet(t testing.TB, filename string) vaultic.BlobSet {
	f, err := os.Open(filename)
	if err != nil {
		t.Logf("unable to open golden file %v: %v", filename, err)
		return vaultic.NewBlobSet()
	}

	sc := bufio.NewScanner(f)

	blobs := vaultic.NewBlobSet()
	for sc.Scan() {
		var h vaultic.BlobHandle
		err := json.Unmarshal([]byte(sc.Text()), &h)
		if err != nil {
			t.Errorf("file %v contained invalid blob: %#v", filename, err)
			continue
		}

		blobs.Insert(h)
	}

	if err = f.Close(); err != nil {
		t.Errorf("closing file %v failed with error %v", filename, err)
	}

	return blobs
}

func saveIDSet(t testing.TB, filename string, s vaultic.BlobSet) {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("unable to update golden file %v: %v", filename, err)
		return
	}

	var hs vaultic.BlobHandles
	for h := range s {
		hs = append(hs, h)
	}

	sort.Sort(hs)

	enc := json.NewEncoder(f)
	for _, h := range hs {
		err = enc.Encode(h)
		if err != nil {
			t.Fatalf("Encode() returned error: %v", err)
		}
	}

	if err = f.Close(); err != nil {
		t.Fatalf("close file %v returned error: %v", filename, err)
	}
}

var updateGoldenFiles = flag.Bool("update", false, "update golden files in testdata/")

const (
	findTestSnapshots = 3
	findTestDepth     = 2
)

var findTestTime = time.Unix(1469960361, 23)

func TestFindUsedBlobs(t *testing.T) {
	repo := repository.TestRepository(t)

	var snapshots []*data.Snapshot
	for i := range findTestSnapshots {
		sn := data.TestCreateSnapshot(t, repo, findTestTime.Add(time.Duration(i)*time.Second), findTestDepth)
		t.Logf("snapshot %v saved, tree %v", sn.ID().Str(), sn.Tree.Str())
		snapshots = append(snapshots, sn)
	}

	p := progress.NewCounter(time.Second, findTestSnapshots, func(value uint64, total uint64, runtime time.Duration, final bool) {})
	defer p.Done()

	for i, sn := range snapshots {
		usedBlobs := vaultic.NewBlobSet()
		err := data.FindUsedBlobs(context.TODO(), repo, vaultic.IDs{*sn.Tree}, usedBlobs, p)
		if err != nil {
			t.Errorf("FindUsedBlobs returned error: %v", err)
			continue
		}

		if len(usedBlobs) == 0 {
			t.Errorf("FindUsedBlobs returned an empty set")
			continue
		}

		v, _ := p.Get()
		test.Equals(t, v, uint64(i+1))

		goldenFilename := filepath.Join("testdata", fmt.Sprintf("used_blobs_snapshot%d", i))
		want := loadIDSet(t, goldenFilename)

		if !want.Equals(usedBlobs) {
			t.Errorf("snapshot %d: wrong list of blobs returned:\n  missing blobs: %v\n  extra blobs: %v",
				i, want.Sub(usedBlobs), usedBlobs.Sub(want))
		}

		if *updateGoldenFiles {
			saveIDSet(t, goldenFilename, usedBlobs)
		}
	}
}

func TestMultiFindUsedBlobs(t *testing.T) {
	repo := repository.TestRepository(t)

	var snapshotTrees vaultic.IDs
	for i := range findTestSnapshots {
		sn := data.TestCreateSnapshot(t, repo, findTestTime.Add(time.Duration(i)*time.Second), findTestDepth)
		t.Logf("snapshot %v saved, tree %v", sn.ID().Str(), sn.Tree.Str())
		snapshotTrees = append(snapshotTrees, *sn.Tree)
	}

	want := vaultic.NewBlobSet()
	for i := range snapshotTrees {
		goldenFilename := filepath.Join("testdata", fmt.Sprintf("used_blobs_snapshot%d", i))
		want.Merge(loadIDSet(t, goldenFilename))
	}

	p := progress.NewCounter(time.Second, findTestSnapshots, func(value uint64, total uint64, runtime time.Duration, final bool) {})
	defer p.Done()

	// run twice to check progress bar handling of duplicate tree roots
	usedBlobs := vaultic.NewBlobSet()
	for i := 1; i < 3; i++ {
		err := data.FindUsedBlobs(context.TODO(), repo, snapshotTrees, usedBlobs, p)
		test.OK(t, err)
		v, _ := p.Get()
		test.Equals(t, v, uint64(i*len(snapshotTrees)))

		if !want.Equals(usedBlobs) {
			t.Errorf("wrong list of blobs returned:\n  missing blobs: %v\n  extra blobs: %v",
				want.Sub(usedBlobs), usedBlobs.Sub(want))
		}
	}
}

type ForbiddenRepo struct{}

func (r ForbiddenRepo) LoadBlob(context.Context, vaultic.BlobHandle, []byte) ([]byte, error) {
	return nil, errors.New("should not be called")
}

func (r ForbiddenRepo) LookupBlobSize(_ vaultic.BlobHandle) (uint, bool) {
	return 0, false
}

func (r ForbiddenRepo) Connections() uint {
	return 2
}

func TestFindUsedBlobsSkipsSeenBlobs(t *testing.T) {
	repo := repository.TestRepository(t)

	snapshot := data.TestCreateSnapshot(t, repo, findTestTime, findTestDepth)
	t.Logf("snapshot %v saved, tree %v", snapshot.ID().Str(), snapshot.Tree.Str())

	usedBlobs := vaultic.NewBlobSet()
	err := data.FindUsedBlobs(context.TODO(), repo, vaultic.IDs{*snapshot.Tree}, usedBlobs, vaultic.NoopCounter)
	if err != nil {
		t.Fatalf("FindUsedBlobs returned error: %v", err)
	}

	err = data.FindUsedBlobs(context.TODO(), ForbiddenRepo{}, vaultic.IDs{*snapshot.Tree}, usedBlobs, vaultic.NoopCounter)
	if err != nil {
		t.Fatalf("FindUsedBlobs returned error: %v", err)
	}
}

func BenchmarkFindUsedBlobs(b *testing.B) {
	repo := repository.TestRepository(b)

	sn := data.TestCreateSnapshot(b, repo, findTestTime, findTestDepth)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		blobs := vaultic.NewBlobSet()
		err := data.FindUsedBlobs(context.TODO(), repo, vaultic.IDs{*sn.Tree}, blobs, vaultic.NoopCounter)
		if err != nil {
			b.Error(err)
		}

		b.Logf("found %v blobs", len(blobs))
	}
}
