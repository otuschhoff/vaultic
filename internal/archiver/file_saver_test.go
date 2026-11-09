package archiver

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/test"
	"golang.org/x/sync/errgroup"
)

func createTestFiles(t testing.TB, num int) (files []string) {
	tempdir := test.TempDir(t)

	for i := range num {
		filename := fmt.Sprintf("testfile-%d", i)
		err := os.WriteFile(filepath.Join(tempdir, filename), []byte(filename), 0600)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, filepath.Join(tempdir, filename))
	}

	return files
}

func startFileSaver(ctx context.Context, t testing.TB, _ fs.FS) (*fileSaver, *mockSaver, context.Context, *errgroup.Group) {
	wg, ctx := errgroup.WithContext(ctx)

	workers := uint(runtime.NumCPU())
	chunkerFactory := repository.TestRepository(t).ChunkerFactory()

	saver := &mockSaver{saved: make(map[string]int)}
	s := newFileSaver(ctx, wg, saver, chunkerFactory, workers)
	s.NodeFromFileInfo = func(snPath, filename string, meta toNoder, ignoreXattrListError bool) (*data.Node, error) {
		return meta.ToNode(ignoreXattrListError, t.Logf)
	}

	return s, saver, ctx, wg
}

func TestMeasuredSourceReader(t *testing.T) {
	counters := &sourceReadCounters{}
	reader := measuredSourceReader{reader: iotest.OneByteReader(strings.NewReader("source")), counters: counters}
	contents, err := io.ReadAll(reader)
	if err != nil || string(contents) != "source" {
		t.Fatalf("measured reader changed contents: %q, %v", contents, err)
	}
	metrics := counters.snapshot()
	var histogramCalls uint64
	for _, count := range metrics.LatencyBuckets {
		histogramCalls += count
	}
	if metrics.Bytes != 6 || metrics.Calls != 7 || metrics.Errors != 0 || histogramCalls != metrics.Calls {
		t.Fatalf("incorrect source-read counters: %+v", metrics)
	}
	reader.reader = iotest.ErrReader(fmt.Errorf("source failure"))
	if _, err := reader.Read(make([]byte, 1)); err == nil || counters.snapshot().Errors != 1 {
		t.Fatal("source read error was lost")
	}
}

func TestFileSaver(t *testing.T) {
	ctx := t.Context()

	startFn := func() {}
	completeReadingFn := func() {}
	completeFn := func(*data.Node, ItemStats) {}

	files := createTestFiles(t, 15)
	testFs := fs.NewLocal()
	s, saver, ctx, wg := startFileSaver(ctx, t, testFs)

	var results []futureNode

	for _, filename := range files {
		f, err := testFs.OpenFile(filename, os.O_RDONLY, false)
		if err != nil {
			t.Fatal(err)
		}

		ff := s.Save(ctx, filename, filename, f, startFn, completeReadingFn, completeFn)
		results = append(results, ff)
	}

	for _, file := range results {
		fnr := file.take(ctx)
		if fnr.err != nil {
			t.Errorf("unable to save file: %v", fnr.err)
		}
	}

	test.Assert(t, len(saver.saved) == len(files), "expected %d saved files, got %d", len(files), len(saver.saved))
	var expectedBytes uint64
	for _, filename := range files {
		expectedBytes += uint64(len(filepath.Base(filename)))
	}
	if metrics := s.sourceReads.snapshot(); metrics.Bytes != expectedBytes || metrics.Calls < uint64(len(files)) || metrics.Errors != 0 {
		t.Fatalf("file saver source reads were not measured: %+v", metrics)
	}

	s.TriggerShutdown()

	err := wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
}
