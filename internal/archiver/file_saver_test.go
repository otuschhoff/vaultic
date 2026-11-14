package archiver

import (
	"context"
	"errors"
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
	"github.com/otuschhoff/vaultic/internal/vaultic"
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

func TestTraversalMetrics(t *testing.T) {
	root := t.TempDir()
	arch := &Archiver{FS: fs.NewLocal()}
	meta, err := arch.openMetadata(root)
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()
	if _, err := arch.statMetadata(meta); err != nil {
		t.Fatal(err)
	}
	contentMeta, err := arch.FS.OpenFile(root, fs.O_NOFOLLOW, true)
	if err != nil {
		t.Fatal(err)
	}
	defer contentMeta.Close()
	if err := arch.openContent(contentMeta); err != nil {
		t.Fatal(err)
	}
	if _, _, err := arch.dirToNodeAndEntries("/", root, meta); err != nil {
		t.Fatal(err)
	}
	finder := data.NewTreeFinder(nil)
	defer finder.Close()
	if _, err := arch.findPreviousNode(finder, "absent"); err != nil {
		t.Fatal(err)
	}
	if _, err := arch.loadSubtree(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	metrics := arch.SourceReadMetrics()
	for _, stage := range []TraversalStageMetrics{metrics.Traversal.OpenMetadata, metrics.Traversal.OpenContent, metrics.Traversal.StatMetadata, metrics.Traversal.Directory, metrics.Traversal.NodeMetadata, metrics.Traversal.PreviousNode, metrics.Traversal.LoadSubtree} {
		if stage.Calls != 1 || stage.Nanoseconds == 0 {
			t.Fatalf("missing traversal stage: %+v", metrics)
		}
	}
	if metrics.Calls != 0 || metrics.Bytes != 0 || metrics.Traversal.Delta(metrics.Traversal) != (TraversalMetrics{}) {
		t.Fatalf("traversal changed source read counters or delta: %+v", metrics)
	}
}

func TestFileSaverWorkerOpen(t *testing.T) {
	filename := createTestFiles(t, 1)[0]
	filesystem := fs.NewLocal()
	arch := &Archiver{FS: filesystem}
	saver, _, ctx, workers := startFileSaver(t.Context(), t, filesystem)
	saver.sourceReads = &arch.sourceReads
	saver.PrepareFile = arch.prepareFile
	file, err := filesystem.OpenFile(filename, fs.O_NOFOLLOW, true)
	if err != nil {
		t.Fatal(err)
	}
	future := saver.Save(ctx, filename, filename, file, func() {}, func() {}, func(*data.Node, ItemStats) {})
	result := future.take(ctx)
	if result.err != nil || result.node == nil || result.node.Type != data.NodeTypeFile || result.node.Size != uint64(len(filepath.Base(filename))) {
		t.Fatalf("worker opening changed the saved file: %+v", result)
	}
	saver.TriggerShutdown()
	if err := workers.Wait(); err != nil {
		t.Fatal(err)
	}
	metrics := arch.SourceReadMetrics()
	if metrics.FilePipeline.WorkerOpenCalls != 1 || metrics.Traversal.OpenContent.Calls != 1 || metrics.Traversal.StatMetadata.Calls != 1 || metrics.FilePipeline.Errors != 0 {
		t.Fatalf("worker opening was not attributed: %+v", metrics)
	}
}

func TestPrepareFileSafety(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%t", canceled), func(t *testing.T) {
			filename := createTestFiles(t, 1)[0]
			arch := &Archiver{FS: fs.NewLocal()}
			file, err := arch.FS.OpenFile(filename, fs.O_NOFOLLOW, true)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if canceled {
				cancel()
			} else {
				if err := os.Remove(filename); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filename, 0700); err != nil {
					t.Fatal(err)
				}
			}
			err = arch.prepareFile(ctx, filename, file)
			if canceled {
				if !errors.Is(err, context.Canceled) || arch.SourceReadMetrics().Traversal.OpenContent.Calls != 0 {
					t.Fatalf("canceled preparation opened content or lost error: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), filename) || !strings.Contains(err.Error(), "changed type") {
				t.Fatalf("post-open type change was not rejected with its source path: %v", err)
			}
		})
	}
}

func TestFileSaverPreparationFailure(t *testing.T) {
	filename := createTestFiles(t, 1)[0]
	filesystem := fs.NewLocal()
	saver, _, ctx, workers := startFileSaver(t.Context(), t, filesystem)
	failure := errors.New("injected file preparation failure")
	saver.PrepareFile = func(context.Context, string, fs.File) error { return failure }
	file, err := filesystem.OpenFile(filename, os.O_RDONLY, false)
	if err != nil {
		t.Fatal(err)
	}
	var completed int
	future := saver.Save(ctx, filename, filename, file, func() {}, func() {
		t.Error("preparation failure completed reading")
	}, func(*data.Node, ItemStats) { completed++ })
	result := future.take(ctx)
	if !errors.Is(result.err, failure) || result.node != nil || completed != 1 {
		t.Fatalf("preparation error or completion changed: result=%+v completed=%d", result, completed)
	}
	if _, err := file.Read(make([]byte, 1)); err == nil {
		t.Fatal("preparation failure left the source handle open")
	}
	saver.TriggerShutdown()
	if err := workers.Wait(); err != nil {
		t.Fatal(err)
	}
	metrics := saver.sourceReads.snapshot()
	if metrics.Calls != 0 || metrics.FilePipeline.Started != 1 || metrics.FilePipeline.Completed != 1 || metrics.FilePipeline.Errors != 1 || metrics.FilePipeline.Active != 0 {
		t.Fatalf("preparation failure did not drain: %+v", metrics)
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
	metrics := s.sourceReads.snapshot().FilePipeline
	if metrics.Scheduled != uint64(len(files)) || metrics.Started != metrics.Scheduled || metrics.ReadComplete != metrics.Started ||
		metrics.Completed != metrics.Started || metrics.Errors != 0 || metrics.Admitting != 0 || metrics.Active != 0 ||
		metrics.AwaitingBlobs != 0 || metrics.PendingBlobs != 0 {
		t.Fatalf("file pipeline did not drain: %+v", metrics)
	}
}

type heldBlobSaver struct {
	vaultic.BlobSaverAsync
	callbacks chan func()
}

func (saver heldBlobSaver) SaveBlobAsync(ctx context.Context, blobType vaultic.BlobType, contents []byte, id vaultic.ID, duplicate bool, callback func(vaultic.ID, bool, int, error)) {
	saver.callbacks <- func() { saver.BlobSaverAsync.SaveBlobAsync(ctx, blobType, contents, id, duplicate, callback) }
}

func TestFileSaverCompletionWait(t *testing.T) {
	files := createTestFiles(t, 1)
	filesystem := fs.NewLocal()
	saver, uploader, ctx, workers := startFileSaver(t.Context(), t, filesystem)
	held := heldBlobSaver{BlobSaverAsync: uploader, callbacks: make(chan func(), 1)}
	saver.uploader = held
	file, err := filesystem.OpenFile(files[0], os.O_RDONLY, false)
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan struct{})
	future := saver.Save(ctx, files[0], files[0], file, func() {}, func() { close(readDone) }, nil)
	select {
	case <-readDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	metrics := saver.sourceReads.snapshot().FilePipeline
	if metrics.ReadComplete != 1 || metrics.Completed != 0 || metrics.PendingBlobs != 1 || metrics.AwaitingBlobs != 1 {
		t.Fatalf("pending completion was not distinguished from source reading: %+v", metrics)
	}
	(<-held.callbacks)()
	if result := future.take(ctx); result.err != nil {
		t.Fatal(result.err)
	}
	saver.TriggerShutdown()
	if err := workers.Wait(); err != nil {
		t.Fatal(err)
	}
	metrics = saver.sourceReads.snapshot().FilePipeline
	if metrics.Completed != 1 || metrics.Active != 0 || metrics.PendingBlobs != 0 || metrics.AwaitingBlobs != 0 || metrics.PostReadNS == 0 {
		t.Fatalf("completion wait did not drain: %+v", metrics)
	}
}
