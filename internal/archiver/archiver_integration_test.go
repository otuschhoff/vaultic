package archiver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/otuschhoff/vaultic/internal/checker"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/sync/errgroup"
)

func (m *TrackFS) OpenFile(name string, flag int, metadataOnly bool) (fs.File, error) {
	m.m.Lock()
	m.opened[name]++
	m.m.Unlock()

	return m.FS.OpenFile(name, flag, metadataOnly)
}

type failSaveRepo struct {
	archiverRepo
	failAfter int32
	cnt       atomic.Int32
	err       error
}

func (f *failSaveRepo) WithBlobUploader(ctx context.Context, fn func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error) error {
	outerCtx, outerCancel := context.WithCancelCause(ctx)
	defer outerCancel(f.err)
	return f.archiverRepo.WithBlobUploader(outerCtx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		return fn(ctx, &failSaveSaver{saver: uploader, failSaveRepo: f, semaphore: make(chan struct{}, 1), outerCancel: outerCancel})
	})
}

type failSaveSaver struct {
	saver        vaultic.BlobSaverWithAsync
	failSaveRepo *failSaveRepo
	semaphore    chan struct{}
	outerCancel  context.CancelCauseFunc
}

func (f *failSaveSaver) SaveBlob(ctx context.Context, t vaultic.BlobType, buf []byte, id vaultic.ID, storeDuplicate bool) (vaultic.ID, bool, int, error) {
	val := f.failSaveRepo.cnt.Add(1)
	if val >= f.failSaveRepo.failAfter {
		return vaultic.ID{}, false, 0, f.failSaveRepo.err
	}

	return f.saver.SaveBlob(ctx, t, buf, id, storeDuplicate)
}

func (f *failSaveSaver) SaveBlobAsync(ctx context.Context, t vaultic.BlobType, buf []byte, id vaultic.ID, storeDuplicate bool, cb func(newID vaultic.ID, known bool, size int, err error)) {
	// limit concurrency to make test reliable
	f.semaphore <- struct{}{}

	val := f.failSaveRepo.cnt.Add(1)
	if val >= f.failSaveRepo.failAfter {
		// kill the outer context to make SaveBlobAsync fail
		// precisely injecting a specific error into the repository is not possible, so just cancel the context
		f.outerCancel(f.failSaveRepo.err)
	}

	f.saver.SaveBlobAsync(ctx, t, buf, id, storeDuplicate, func(newID vaultic.ID, known bool, size int, err error) {
		if val >= f.failSaveRepo.failAfter {
			if err == nil {
				panic("expected error")
			}
		}
		cb(newID, known, size, err)
		<-f.semaphore
	})
}

func TestArchiverAbortEarlyOnError(t *testing.T) {
	var tests = []struct {
		src       TestDir
		wantOpen  map[string]uint
		failAfter uint // error after so many blobs have been saved to the repo
	}{
		{
			src: TestDir{
				"dir": TestDir{
					"bar": TestFile{Content: "foobar"},
					"baz": TestFile{Content: "foobar"},
					"foo": TestFile{Content: "foobar"},
				},
			},
			wantOpen: map[string]uint{
				filepath.FromSlash("dir/bar"): 1,
			},
		},
		{
			src: TestDir{
				"dir": TestDir{
					"file0": TestFile{Content: string(rtest.Random(0, 1024))},
					"file1": TestFile{Content: string(rtest.Random(1, 1024))},
					"file2": TestFile{Content: string(rtest.Random(2, 1024))},
					"file3": TestFile{Content: string(rtest.Random(3, 1024))},
					"file4": TestFile{Content: string(rtest.Random(4, 1024))},
					"file5": TestFile{Content: string(rtest.Random(5, 1024))},
					"file6": TestFile{Content: string(rtest.Random(6, 1024))},
					"file7": TestFile{Content: string(rtest.Random(7, 1024))},
					"file8": TestFile{Content: string(rtest.Random(8, 1024))},
					"file9": TestFile{Content: string(rtest.Random(9, 1024))},
				},
			},
			wantOpen: map[string]uint{
				filepath.FromSlash("dir/file0"): 1,
				filepath.FromSlash("dir/file1"): 1,
				filepath.FromSlash("dir/file2"): 1,
				filepath.FromSlash("dir/file3"): 1,
				filepath.FromSlash("dir/file8"): 0,
				filepath.FromSlash("dir/file9"): 0,
			},
			// fails after four to seven files were opened, as the ReadConcurrency allows for
			// two queued files and one blob queued for saving.
			failAfter: 4,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			ctx := t.Context()

			tempdir, repo := prepareTempdirRepoSrc(t, test.src)

			back := rtest.Chdir(t, tempdir)
			defer back()

			testFS := &TrackFS{
				FS:     fs.Track{FS: fs.NewLocal()},
				opened: make(map[string]uint),
			}

			testErr := context.Canceled
			testRepo := &failSaveRepo{
				archiverRepo: repo,
				failAfter:    int32(test.failAfter),
				err:          testErr,
			}

			// at most two files may be queued
			arch := New(testRepo, testFS, Options{
				ReadConcurrency: 2,
			})
			arch.Error = func(item string, err error) error {
				t.Logf("archiver error for %q: %v", item, err)
				return err
			}

			_, _, _, err := arch.Snapshot(ctx, []string{"."}, SnapshotOptions{Time: time.Now()})
			if !errors.Is(err, testErr) {
				t.Errorf("expected error (%v) not found, got %v", testErr, err)
			}

			t.Logf("Snapshot return error: %v", err)

			t.Logf("track fs: %v", testFS.opened)

			for k, v := range test.wantOpen {
				if testFS.opened[k] != v {
					t.Errorf("opened %v %d times, want %d", k, testFS.opened[k], v)
				}
			}
		})
	}
}

func snapshot(t testing.TB, repo archiverRepo, fs fs.FS, parent *data.Snapshot, filename string) (*data.Snapshot, *data.Node) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	arch := New(repo, fs, Options{})

	sopts := SnapshotOptions{
		Time:           time.Now(),
		ParentSnapshot: parent,
	}
	snapshot, _, _, err := arch.Snapshot(ctx, []string{filename}, sopts)
	rtest.OK(t, err)
	tree, err := data.LoadTree(ctx, repo, *snapshot.Tree)
	rtest.OK(t, err)

	finder := data.NewTreeFinder(tree)
	defer finder.Close()
	node, err := finder.Find(filename)
	rtest.OK(t, err)
	rtest.Assert(t, node != nil, "unable to find node for testfile in snapshot")

	return snapshot, node
}

type overrideFS struct {
	fs.FS
	overrideFI    *fs.ExtendedFileInfo
	resetFIOnRead bool
	overrideNode  *data.Node
	overrideErr   error
}

func (m *overrideFS) OpenFile(name string, flag int, metadataOnly bool) (fs.File, error) {
	f, err := m.FS.OpenFile(name, flag, metadataOnly)
	if err != nil {
		return f, err
	}

	if filepath.Base(name) == "testfile" || filepath.Base(name) == "testdir" {
		return &overrideFile{f, m}, nil
	}
	return f, nil
}

type overrideFile struct {
	fs.File
	ofs *overrideFS
}

func (f overrideFile) Stat() (*fs.ExtendedFileInfo, error) {
	if f.ofs.overrideFI == nil {
		return f.File.Stat()
	}
	return f.ofs.overrideFI, nil

}

func (f overrideFile) MakeReadable() error {
	if f.ofs.resetFIOnRead {
		f.ofs.overrideFI = nil
	}
	return f.File.MakeReadable()
}

func (f overrideFile) ToNode(ignoreXattrListError bool, warnf func(format string, args ...any)) (*data.Node, error) {
	if f.ofs.overrideNode == nil {
		return f.File.ToNode(ignoreXattrListError, warnf)
	}
	return f.ofs.overrideNode, f.ofs.overrideErr
}

// used by wrapFileInfo, use untyped const in order to avoid having a version
// of wrapFileInfo for each OS
const (
	mockFileInfoMode = 0400
	mockFileInfoUID  = 51234
	mockFileInfoGID  = 51235
)

func TestMetadataChanged(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.DeviceIDForHardlinks, true)()

	files := TestDir{
		"testfile": TestFile{
			Content: "foo bar test file",
		},
	}

	tempdir, repo := prepareTempdirRepoSrc(t, files)

	back := rtest.Chdir(t, tempdir)
	defer back()

	// get metadata
	fi := lstat(t, "testfile")
	localFS := fs.NewLocal()
	meta, err := localFS.OpenFile("testfile", fs.O_NOFOLLOW, true)
	rtest.OK(t, err)
	want, err := meta.ToNode(false, t.Logf)
	rtest.OK(t, err)
	rtest.OK(t, meta.Close())

	fs := &overrideFS{
		FS:           localFS,
		overrideFI:   fi,
		overrideNode: &data.Node{},
	}
	*fs.overrideNode = *want

	sn, node2 := snapshot(t, repo, fs, nil, "testfile")

	// set some values so we can then compare the nodes
	want.DeviceID = 0
	want.Content = node2.Content
	want.Path = ""
	if len(want.ExtendedAttributes) == 0 {
		want.ExtendedAttributes = nil
	}

	want.AccessTime = want.ModTime

	// make sure that metadata was recorded successfully
	if !cmp.Equal(want, node2) {
		t.Fatalf("metadata does not match:\n%v", cmp.Diff(want, node2))
	}

	// modify the mode and UID/GID
	modFI := *fi
	modFI.Mode = mockFileInfoMode
	if runtime.GOOS != "windows" {
		modFI.UID = mockFileInfoUID
		modFI.GID = mockFileInfoGID
	}

	fs.overrideFI = &modFI
	rtest.Assert(t, !fileChanged(fs.overrideFI, node2, 0), "testfile must not be considered as changed")

	// set the override values in the 'want' node which
	want.Mode = mockFileInfoMode
	// ignore UID and GID on Windows
	if runtime.GOOS != "windows" {
		want.UID = mockFileInfoUID
		want.GID = mockFileInfoGID
	}
	// update mock node accordingly
	fs.overrideNode.Mode = want.Mode
	fs.overrideNode.UID = want.UID
	fs.overrideNode.GID = want.GID

	// make another snapshot
	_, node3 := snapshot(t, repo, fs, sn, "testfile")

	// make sure that metadata was recorded successfully
	if !cmp.Equal(want, node3) {
		t.Fatalf("metadata does not match:\n%v", cmp.Diff(want, node3))
	}

	// make sure the content matches
	TestEnsureFileContent(context.Background(), t, repo, "testfile", node3, files["testfile"].(TestFile))

	checker.TestCheckRepo(t, repo)
}

func TestRacyFileTypeSwap(t *testing.T) {
	files := TestDir{
		"testfile": TestFile{
			Content: "foo bar test file",
		},
		"testdir": TestDir{},
	}

	for _, dirError := range []bool{false, true} {
		desc := "file changed type"
		if dirError {
			desc = "dir changed type"
		}
		t.Run(desc, func(t *testing.T) {
			tempdir, repo := prepareTempdirRepoSrc(t, files)

			back := rtest.Chdir(t, tempdir)
			defer back()

			// get metadata of current folder
			var fakeName, realName string
			if dirError {
				// lstat claims this is a directory, but it's actually a file
				fakeName = "testdir"
				realName = "testfile"
			} else {
				fakeName = "testfile"
				realName = "testdir"
			}
			fakeFI := lstat(t, fakeName)
			tempfile := filepath.Join(tempdir, realName)

			statfs := &overrideFS{
				FS:            fs.NewLocal(),
				overrideFI:    fakeFI,
				resetFIOnRead: true,
			}

			ctx := t.Context()

			_ = repo.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
				wg, ctx := errgroup.WithContext(ctx)

				arch := New(repo, fs.Track{FS: statfs}, Options{})
				arch.Error = func(item string, err error) error {
					t.Logf("archiver error as expected for %v: %v", item, err)
					return err
				}
				arch.runWorkers(ctx, wg, uploader)

				// fs.Track will panic if the file was not closed
				_, excluded, err := arch.save(ctx, "/", tempfile, nil, false)
				rtest.Assert(t, err != nil && strings.Contains(err.Error(), "changed type, refusing to archive"), "save() returned wrong error: %v", err)
				tpe := "file"
				if dirError {
					tpe = "directory"
				}
				rtest.Assert(t, strings.Contains(err.Error(), tpe+" "), "unexpected item type in error: %v", err)
				rtest.Assert(t, !excluded, "Save() excluded the node, that's unexpected")
				return nil
			})
		})
	}
}

type mockToNoder struct {
	node *data.Node
	err  error
}

func (m *mockToNoder) ToNode(_ bool, _ func(format string, args ...any)) (*data.Node, error) {
	return m.node, m.err
}

func TestMetadataBackupErrorFiltering(t *testing.T) {
	tempdir := t.TempDir()
	filename := filepath.Join(tempdir, "file")
	repo := repository.TestRepository(t)

	arch := New(repo, fs.NewLocal(), Options{})

	var filteredErr error
	replacementErr := fmt.Errorf("replacement")
	arch.Error = func(item string, err error) error {
		filteredErr = err
		return replacementErr
	}

	nonExistNoder := &mockToNoder{
		node: &data.Node{Type: data.NodeTypeFile},
		err:  fmt.Errorf("not found"),
	}

	// check that errors from reading extended metadata are properly filtered
	node, err := arch.nodeFromFileInfo("file", filename+"invalid", nonExistNoder, false)
	rtest.Assert(t, node != nil, "node is missing")
	rtest.Assert(t, err == replacementErr, "expected %v got %v", replacementErr, err)
	rtest.Assert(t, filteredErr != nil, "missing inner error")

	// check that errors from reading irregular file are not filtered
	filteredErr = nil
	nonExistNoder = &mockToNoder{
		node: &data.Node{Type: data.NodeTypeIrregular},
		err:  fmt.Errorf(`unsupported file type "irregular"`),
	}
	node, err = arch.nodeFromFileInfo("file", filename, nonExistNoder, false)
	rtest.Assert(t, node != nil, "node is missing")
	rtest.Assert(t, filteredErr == nil, "error for irregular node should not have been filtered")
	rtest.Assert(t, strings.Contains(err.Error(), "irregular"), "unexpected error %q does not warn about irregular file mode", err)
}

func TestIrregularFile(t *testing.T) {
	files := TestDir{
		"testfile": TestFile{
			Content: "foo bar test file",
		},
	}
	tempdir, repo := prepareTempdirRepoSrc(t, files)

	back := rtest.Chdir(t, tempdir)
	defer back()

	tempfile := filepath.Join(tempdir, "testfile")
	fi := lstat(t, "testfile")
	// patch mode to irregular
	fi.Mode = (fi.Mode &^ os.ModeType) | os.ModeIrregular

	override := &overrideFS{
		FS:         fs.NewLocal(),
		overrideFI: fi,
		overrideNode: &data.Node{
			Type: data.NodeTypeIrregular,
		},
		overrideErr: fmt.Errorf(`unsupported file type "irregular"`),
	}

	ctx := t.Context()

	arch := New(repo, fs.Track{FS: override}, Options{})
	_, excluded, err := arch.save(ctx, "/", tempfile, nil, false)
	if err == nil {
		t.Fatalf("Save() should have failed")
	}
	rtest.Assert(t, strings.Contains(err.Error(), "irregular"), "unexpected error %q does not warn about irregular file mode", err)

	if excluded {
		t.Errorf("Save() excluded the node, that's unexpected")
	}
}

type missingFS struct {
	fs.FS
	errorOnOpen bool
}

func (fs *missingFS) OpenFile(_ string, _ int, _ bool) (fs.File, error) {
	if fs.errorOnOpen {
		return nil, os.ErrNotExist
	}

	return &missingFile{}, nil
}

type missingFile struct {
	fs.File
}

func (f *missingFile) Stat() (*fs.ExtendedFileInfo, error) {
	return nil, os.ErrNotExist
}

func (f *missingFile) Close() error {
	// prevent segfault in test
	return nil
}

func TestDisappearedFile(t *testing.T) {
	tempdir, repo := prepareTempdirRepoSrc(t, TestDir{})

	back := rtest.Chdir(t, tempdir)
	defer back()

	ctx := t.Context()

	// depending on the underlying FS implementation a missing file may be detected by OpenFile or
	// the subsequent file.Stat() call. Thus test both cases.
	for _, errorOnOpen := range []bool{false, true} {
		arch := New(repo, fs.Track{FS: &missingFS{FS: fs.NewLocal(), errorOnOpen: errorOnOpen}}, Options{})
		_, excluded, err := arch.save(ctx, "/", filepath.Join(tempdir, "testdir"), nil, false)
		rtest.OK(t, err)
		rtest.Assert(t, excluded, "testfile should have been excluded")
	}
}
