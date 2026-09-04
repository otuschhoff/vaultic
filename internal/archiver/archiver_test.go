package archiver

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/sync/errgroup"
)

func prepareTempdirRepoSrc(t testing.TB, src TestDir) (string, *repository.Repository) {
	tempdir := rtest.TempDir(t)
	repo := repository.TestRepository(t)

	TestCreateFiles(t, tempdir, src)

	return tempdir, repo
}

func saveFile(t testing.TB, repo archiverRepo, filename string, filesystem fs.FS) (*data.Node, ItemStats) {
	var (
		completeReadingCallback bool

		completeCallbackNode  *data.Node
		completeCallbackStats ItemStats
		completeCallback      bool

		startCallback bool
		fnr           futureNodeResult
	)

	arch := New(repo, filesystem, Options{})
	arch.Error = func(item string, err error) error {
		t.Errorf("archiver error for %v: %v", item, err)
		return err
	}

	err := repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		wg, ctx := errgroup.WithContext(ctx)
		arch.runWorkers(ctx, wg, uploader)

		completeReading := func() {
			completeReadingCallback = true
			if completeCallback {
				t.Error("callbacks called in wrong order")
			}
		}

		complete := func(node *data.Node, stats ItemStats) {
			completeCallback = true
			completeCallbackNode = node
			completeCallbackStats = stats
		}

		start := func() {
			startCallback = true
		}

		file, err := arch.FS.OpenFile(filename, fs.O_NOFOLLOW, false)
		if err != nil {
			t.Fatal(err)
		}

		res := arch.fileSaver.Save(ctx, "/", filename, file, start, completeReading, complete)

		fnr = res.take(ctx)
		if fnr.err != nil {
			t.Fatal(fnr.err)
		}

		arch.stopWorkers()
		return wg.Wait()
	})
	if err != nil {
		t.Fatal(err)
	}

	if !startCallback {
		t.Errorf("start callback did not happen")
	}

	if !completeReadingCallback {
		t.Errorf("completeReading callback did not happen")
	}

	if !completeCallback {
		t.Errorf("complete callback did not happen")
	}

	if completeCallbackNode == nil {
		t.Errorf("no node returned for complete callback")
	}

	if completeCallbackNode != nil && !fnr.node.Equals(*completeCallbackNode) {
		t.Errorf("different node returned for complete callback")
	}

	if completeCallbackStats != fnr.stats {
		t.Errorf("different stats return for complete callback, want:\n  %v\ngot:\n  %v", fnr.stats, completeCallbackStats)
	}

	return fnr.node, fnr.stats
}

func TestArchiverSaveFile(t *testing.T) {
	var tests = []TestFile{
		{Content: ""},
		{Content: "foo"},
		{Content: string(rtest.Random(23, 12*1024*1024+1287898))},
	}

	for _, testfile := range tests {
		t.Run("", func(t *testing.T) {
			ctx := t.Context()

			tempdir, repo := prepareTempdirRepoSrc(t, TestDir{"file": testfile})
			node, stats := saveFile(t, repo, filepath.Join(tempdir, "file"), fs.Track{FS: fs.NewLocal()})

			TestEnsureFileContent(ctx, t, repo, "file", node, testfile)
			if stats.DataSize != uint64(len(testfile.Content)) {
				t.Errorf("wrong stats returned in DataSize, want %d, got %d", len(testfile.Content), stats.DataSize)
			}
			if stats.DataBlobs <= 0 && len(testfile.Content) > 0 {
				t.Errorf("wrong stats returned in DataBlobs, want > 0, got %d", stats.DataBlobs)
			}
			if stats.TreeSize != 0 {
				t.Errorf("wrong stats returned in TreeSize, want 0, got %d", stats.TreeSize)
			}
			if stats.TreeBlobs != 0 {
				t.Errorf("wrong stats returned in DataBlobs, want 0, got %d", stats.DataBlobs)
			}
		})
	}
}

func TestArchiverSaveFileReaderFS(t *testing.T) {
	var tests = []struct {
		Data string
	}{
		{Data: "foo"},
		{Data: string(rtest.Random(23, 12*1024*1024+1287898))},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			ctx := t.Context()

			repo := repository.TestRepository(t)

			ts := time.Now()
			filename := "xx"
			readerFs, err := fs.NewReader(filename, io.NopCloser(strings.NewReader(test.Data)), fs.ReaderOptions{
				ModTime: ts,
				Mode:    0123,
			})
			rtest.OK(t, err)

			node, stats := saveFile(t, repo, filename, readerFs)

			TestEnsureFileContent(ctx, t, repo, "file", node, TestFile{Content: test.Data})
			if stats.DataSize != uint64(len(test.Data)) {
				t.Errorf("wrong stats returned in DataSize, want %d, got %d", len(test.Data), stats.DataSize)
			}
			if stats.DataBlobs <= 0 && len(test.Data) > 0 {
				t.Errorf("wrong stats returned in DataBlobs, want > 0, got %d", stats.DataBlobs)
			}
			if stats.TreeSize != 0 {
				t.Errorf("wrong stats returned in TreeSize, want 0, got %d", stats.TreeSize)
			}
			if stats.TreeBlobs != 0 {
				t.Errorf("wrong stats returned in DataBlobs, want 0, got %d", stats.DataBlobs)
			}
		})
	}
}

func TestArchiverSave(t *testing.T) {
	var tests = []TestFile{
		{Content: ""},
		{Content: "foo"},
		{Content: string(rtest.Random(23, 12*1024*1024+1287898))},
	}

	for _, testfile := range tests {
		t.Run("", func(t *testing.T) {
			ctx := t.Context()

			tempdir, repo := prepareTempdirRepoSrc(t, TestDir{"file": testfile})

			arch := New(repo, fs.Track{FS: fs.NewLocal()}, Options{})
			arch.Error = func(item string, err error) error {
				t.Errorf("archiver error for %v: %v", item, err)
				return err
			}
			arch.summary = &Summary{}

			var fnr futureNodeResult
			err := repo.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
				wg, ctx := errgroup.WithContext(ctx)
				arch.runWorkers(ctx, wg, uploader)

				node, excluded, err := arch.save(ctx, "/", filepath.Join(tempdir, "file"), nil, false)
				if err != nil {
					t.Fatal(err)
				}

				if excluded {
					t.Errorf("Save() excluded the node, that's unexpected")
				}

				fnr = node.take(ctx)
				if fnr.err != nil {
					t.Fatal(fnr.err)
				}

				if fnr.node == nil {
					t.Fatalf("returned node is nil")
				}

				arch.stopWorkers()
				return wg.Wait()
			})
			if err != nil {
				t.Fatal(err)
			}

			TestEnsureFileContent(ctx, t, repo, "file", fnr.node, testfile)
			stats := fnr.stats
			if stats.DataSize != uint64(len(testfile.Content)) {
				t.Errorf("wrong stats returned in DataSize, want %d, got %d", len(testfile.Content), stats.DataSize)
			}
			if stats.DataBlobs <= 0 && len(testfile.Content) > 0 {
				t.Errorf("wrong stats returned in DataBlobs, want > 0, got %d", stats.DataBlobs)
			}
			if stats.TreeSize != 0 {
				t.Errorf("wrong stats returned in TreeSize, want 0, got %d", stats.TreeSize)
			}
			if stats.TreeBlobs != 0 {
				t.Errorf("wrong stats returned in DataBlobs, want 0, got %d", stats.DataBlobs)
			}
		})
	}
}

func TestArchiverSaveReaderFS(t *testing.T) {
	var tests = []struct {
		Data string
	}{
		{Data: "foo"},
		{Data: string(rtest.Random(23, 12*1024*1024+1287898))},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			ctx := t.Context()

			repo := repository.TestRepository(t)

			ts := time.Now()
			filename := "xx"
			readerFs, err := fs.NewReader(filename, io.NopCloser(strings.NewReader(test.Data)), fs.ReaderOptions{
				ModTime: ts,
				Mode:    0123,
			})
			rtest.OK(t, err)
			arch := New(repo, readerFs, Options{})
			arch.Error = func(item string, err error) error {
				t.Errorf("archiver error for %v: %v", item, err)
				return err
			}
			arch.summary = &Summary{}

			var fnr futureNodeResult
			err = repo.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
				wg, ctx := errgroup.WithContext(ctx)
				arch.runWorkers(ctx, wg, uploader)

				node, excluded, err := arch.save(ctx, "/", filename, nil, false)
				t.Logf("Save returned %v %v", node, err)
				if err != nil {
					t.Fatal(err)
				}

				if excluded {
					t.Errorf("Save() excluded the node, that's unexpected")
				}

				fnr = node.take(ctx)
				if fnr.err != nil {
					t.Fatal(fnr.err)
				}

				if fnr.node == nil {
					t.Fatalf("returned node is nil")
				}

				arch.stopWorkers()
				return wg.Wait()
			})
			if err != nil {
				t.Fatal(err)
			}

			TestEnsureFileContent(ctx, t, repo, "file", fnr.node, TestFile{Content: test.Data})
			stats := fnr.stats
			if stats.DataSize != uint64(len(test.Data)) {
				t.Errorf("wrong stats returned in DataSize, want %d, got %d", len(test.Data), stats.DataSize)
			}
			if stats.DataBlobs <= 0 && len(test.Data) > 0 {
				t.Errorf("wrong stats returned in DataBlobs, want > 0, got %d", stats.DataBlobs)
			}
			if stats.TreeSize != 0 {
				t.Errorf("wrong stats returned in TreeSize, want 0, got %d", stats.TreeSize)
			}
			if stats.TreeBlobs != 0 {
				t.Errorf("wrong stats returned in DataBlobs, want 0, got %d", stats.DataBlobs)
			}
		})
	}
}

func BenchmarkArchiverSaveFileSmall(b *testing.B) {
	const fileSize = 4 * 1024
	d := TestDir{"file": TestFile{
		Content: string(rtest.Random(23, fileSize)),
	}}

	b.SetBytes(fileSize)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tempdir, repo := prepareTempdirRepoSrc(b, d)
		b.StartTimer()

		_, stats := saveFile(b, repo, filepath.Join(tempdir, "file"), fs.Track{FS: fs.NewLocal()})

		b.StopTimer()
		if stats.DataSize != fileSize {
			b.Errorf("wrong stats returned in DataSize, want %d, got %d", fileSize, stats.DataSize)
		}
		if stats.DataBlobs <= 0 {
			b.Errorf("wrong stats returned in DataBlobs, want > 0, got %d", stats.DataBlobs)
		}
		if stats.TreeSize != 0 {
			b.Errorf("wrong stats returned in TreeSize, want 0, got %d", stats.TreeSize)
		}
		if stats.TreeBlobs != 0 {
			b.Errorf("wrong stats returned in DataBlobs, want 0, got %d", stats.DataBlobs)
		}
		b.StartTimer()
	}
}

func BenchmarkArchiverSaveFileLarge(b *testing.B) {
	const fileSize = 40*1024*1024 + 1287898
	d := TestDir{"file": TestFile{
		Content: string(rtest.Random(23, fileSize)),
	}}

	b.SetBytes(fileSize)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tempdir, repo := prepareTempdirRepoSrc(b, d)
		b.StartTimer()

		_, stats := saveFile(b, repo, filepath.Join(tempdir, "file"), fs.Track{FS: fs.NewLocal()})

		b.StopTimer()
		if stats.DataSize != fileSize {
			b.Errorf("wrong stats returned in DataSize, want %d, got %d", fileSize, stats.DataSize)
		}
		if stats.DataBlobs <= 0 {
			b.Errorf("wrong stats returned in DataBlobs, want > 0, got %d", stats.DataBlobs)
		}
		if stats.TreeSize != 0 {
			b.Errorf("wrong stats returned in TreeSize, want 0, got %d", stats.TreeSize)
		}
		if stats.TreeBlobs != 0 {
			b.Errorf("wrong stats returned in DataBlobs, want 0, got %d", stats.DataBlobs)
		}
		b.StartTimer()
	}
}

type blobCountingRepo struct {
	archiverRepo

	m     sync.Mutex
	saved map[vaultic.BlobHandle]uint
}

func (repo *blobCountingRepo) WithBlobUploader(ctx context.Context, fn func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error) error {
	return repo.archiverRepo.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		return fn(ctx, &blobCountingSaver{saver: uploader, blobCountingRepo: repo})
	})
}

type blobCountingSaver struct {
	saver            vaultic.BlobSaverWithAsync
	blobCountingRepo *blobCountingRepo
}

func (repo *blobCountingSaver) count(exists bool, h vaultic.BlobHandle) {
	if exists {
		return
	}
	repo.blobCountingRepo.m.Lock()
	repo.blobCountingRepo.saved[h]++
	repo.blobCountingRepo.m.Unlock()
}

func (repo *blobCountingSaver) SaveBlob(ctx context.Context, t vaultic.BlobType, buf []byte, id vaultic.ID, storeDuplicate bool) (vaultic.ID, bool, int, error) {
	id, exists, size, err := repo.saver.SaveBlob(ctx, t, buf, id, storeDuplicate)
	repo.count(exists, vaultic.BlobHandle{ID: id, Type: t})
	return id, exists, size, err
}

func (repo *blobCountingSaver) SaveBlobAsync(ctx context.Context, t vaultic.BlobType, buf []byte, id vaultic.ID, storeDuplicate bool, cb func(newID vaultic.ID, known bool, size int, err error)) {
	repo.saver.SaveBlobAsync(ctx, t, buf, id, storeDuplicate, func(newID vaultic.ID, known bool, size int, err error) {
		repo.count(known, vaultic.BlobHandle{ID: newID, Type: t})
		cb(newID, known, size, err)
	})
}

func appendToFile(t testing.TB, filename string, data []byte) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Write(data)
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestArchiverSaveFileIncremental(t *testing.T) {
	tempdir := rtest.TempDir(t)

	repo := &blobCountingRepo{
		archiverRepo: repository.TestRepository(t),
		saved:        make(map[vaultic.BlobHandle]uint),
	}

	data := rtest.Random(23, 512*1024+887898)
	testfile := filepath.Join(tempdir, "testfile")

	for i := range 3 {
		appendToFile(t, testfile, data)
		node, _ := saveFile(t, repo, testfile, fs.Track{FS: fs.NewLocal()})

		t.Logf("node blobs: %v", node.Content)

		for h, n := range repo.saved {
			if n > 1 {
				t.Errorf("iteration %v: blob %v saved more than once (%d times)", i, h, n)
			}
		}
	}
}

func save(t testing.TB, filename string, data []byte) {
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Sync()
	if err != nil {
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func chmodTwice(t testing.TB, name string) {
	// POSIX says that ctime is updated "even if the file status does not
	// change", but let's make sure it does change, just in case.
	err := os.Chmod(name, 0700)
	rtest.OK(t, err)

	sleep()

	err = os.Chmod(name, 0600)
	rtest.OK(t, err)
}

func lstat(t testing.TB, name string) *fs.ExtendedFileInfo {
	fi, err := os.Lstat(name)
	if err != nil {
		t.Fatal(err)
	}

	return fs.ExtendedStat(fi)
}

func setTimestamp(t testing.TB, filename string, atime, mtime time.Time) {
	var utimes = [...]syscall.Timespec{
		syscall.NsecToTimespec(atime.UnixNano()),
		syscall.NsecToTimespec(mtime.UnixNano()),
	}

	err := syscall.UtimesNano(filename, utimes[:])
	if err != nil {
		t.Fatal(err)
	}
}

func remove(t testing.TB, filename string) {
	err := os.Remove(filename)
	if err != nil {
		t.Fatal(err)
	}
}

func rename(t testing.TB, oldname, newname string) {
	err := os.Rename(oldname, newname)
	if err != nil {
		t.Fatal(err)
	}
}

func nodeFromFile(t testing.TB, localFs fs.FS, filename string) *data.Node {
	meta, err := localFs.OpenFile(filename, fs.O_NOFOLLOW, true)
	rtest.OK(t, err)
	node, err := meta.ToNode(false, t.Logf)
	rtest.OK(t, err)
	rtest.OK(t, meta.Close())

	return node
}

// sleep sleeps long enough to ensure a timestamp change.
func sleep() {
	time.Sleep(5 * time.Millisecond)
}

func TestFileChanged(t *testing.T) {
	var defaultContent = []byte("foobar")

	var tests = []struct {
		Name           string
		SkipForWindows bool
		Content        []byte
		Modify         func(t testing.TB, filename string)
		ChangeIgnore   uint
		SameFile       bool
	}{
		{
			Name: "same-content-new-file",
			Modify: func(t testing.TB, filename string) {
				remove(t, filename)
				sleep()
				save(t, filename, defaultContent)
			},
		},
		{
			Name: "same-content-new-timestamp",
			Modify: func(t testing.TB, filename string) {
				sleep()
				save(t, filename, defaultContent)
			},
		},
		{
			Name: "new-content-same-timestamp",
			// on Windows, there's no "create time" field users cannot modify,
			// so we're unable to detect if a file has been modified when the
			// timestamps are reset, so we skip this test for Windows
			SkipForWindows: true,
			Modify: func(t testing.TB, filename string) {
				fi, err := os.Stat(filename)
				if err != nil {
					t.Fatal(err)
				}
				extFI := fs.ExtendedStat(fi)
				save(t, filename, bytes.ToUpper(defaultContent))
				sleep()
				setTimestamp(t, filename, extFI.AccessTime, extFI.ModTime)
			},
		},
		{
			Name: "other-content",
			Modify: func(t testing.TB, filename string) {
				remove(t, filename)
				sleep()
				save(t, filename, []byte("xxxxxx"))
			},
		},
		{
			Name: "longer-content",
			Modify: func(t testing.TB, filename string) {
				save(t, filename, []byte("xxxxxxxxxxxxxxxxxxxxxx"))
			},
		},
		{
			Name: "new-file",
			Modify: func(t testing.TB, filename string) {
				remove(t, filename)
				sleep()
				save(t, filename, defaultContent)
			},
		},
		{
			Name:           "ctime-change",
			Modify:         chmodTwice,
			SameFile:       false,
			SkipForWindows: true, // No ctime on Windows, so this test would fail.
		},
		{
			Name:           "ignore-ctime-change",
			Modify:         chmodTwice,
			ChangeIgnore:   ChangeIgnoreCtime,
			SameFile:       true,
			SkipForWindows: true, // No ctime on Windows, so this test is meaningless.
		},
		{
			Name: "ignore-inode",
			Modify: func(t testing.TB, filename string) {
				fi := lstat(t, filename)
				// First create the new file, then remove the old one,
				// so that the old file retains its inode number.
				tempname := filename + ".old"
				rename(t, filename, tempname)
				save(t, filename, defaultContent)
				remove(t, tempname)
				setTimestamp(t, filename, fi.ModTime, fi.ModTime)
			},
			ChangeIgnore: ChangeIgnoreCtime | ChangeIgnoreInode,
			SameFile:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			if runtime.GOOS == "windows" && test.SkipForWindows {
				t.Skip("don't run test on Windows")
			}

			tempdir := rtest.TempDir(t)

			filename := filepath.Join(tempdir, "file")
			content := defaultContent
			if test.Content != nil {
				content = test.Content
			}
			save(t, filename, content)

			fs := fs.NewLocal()
			fiBefore, err := fs.Lstat(filename)
			rtest.OK(t, err)
			node := nodeFromFile(t, fs, filename)

			if fileChanged(fiBefore, node, 0) {
				t.Fatalf("unchanged file detected as changed")
			}

			test.Modify(t, filename)

			fiAfter := lstat(t, filename)

			if test.SameFile {
				// file should be detected as unchanged
				if fileChanged(fiAfter, node, test.ChangeIgnore) {
					t.Fatalf("unmodified file detected as changed")
				}
			} else {
				// file should be detected as changed
				if !fileChanged(fiAfter, node, test.ChangeIgnore) && !test.SameFile {
					t.Fatalf("modified file detected as unchanged")
				}
			}
		})
	}
}
