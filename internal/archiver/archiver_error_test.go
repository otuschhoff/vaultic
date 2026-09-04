package archiver

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/checker"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func TestArchiverSelectiveSubtreeReuse(t *testing.T) {
	ctx := t.Context()
	tempdir, repo := prepareTempdirRepoSrc(t, TestDir{
		"changed":   TestDir{"file": TestFile{Content: "before"}},
		"unchanged": TestDir{"file": TestFile{Content: "stable"}},
	})
	back := rtest.Chdir(t, tempdir)
	defer back()

	first := New(repo, fs.NewLocal(), Options{})
	parent, _, _, err := first.Snapshot(ctx, []string{"."}, SnapshotOptions{Time: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("changed", "file"), []byte("after"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("unchanged", "file"), []byte("not crawled"), 0o600); err != nil {
		t.Fatal(err)
	}

	var reused []string
	second := New(repo, fs.NewLocal(), Options{CWalkConcurrency: 8, CWalkQueue: 1, CWalkRoots: []string{"changed"}})
	second.ReuseSubtree = func(_ string, sourcePath string, _ *data.Node) bool {
		if filepath.Base(sourcePath) == "unchanged" {
			reused = append(reused, sourcePath)
			return true
		}
		return false
	}
	_, snapshotID, _, err := second.Snapshot(ctx, []string{"."}, SnapshotOptions{Time: time.Now(), ParentSnapshot: parent})
	if err != nil {
		t.Fatal(err)
	}
	if len(reused) != 1 {
		t.Fatalf("reused subtrees = %q, want one unchanged subtree", reused)
	}
	TestEnsureSnapshot(t, repo, snapshotID, TestDir{
		"changed":   TestDir{"file": TestFile{Content: "after"}},
		"unchanged": TestDir{"file": TestFile{Content: "stable"}},
	})
}

func TestArchiverCWalkTraversal(t *testing.T) {
	ctx := t.Context()
	source := TestDir{
		"a":    TestDir{"nested": TestDir{"file": TestFile{Content: "a"}}},
		"b":    TestDir{},
		"root": TestFile{Content: "root"},
	}
	tempdir, repo := prepareTempdirRepoSrc(t, source)
	back := rtest.Chdir(t, tempdir)
	defer back()

	arch := New(repo, fs.NewLocal(), Options{CWalkConcurrency: 8, CWalkQueue: 1})
	_, snapshotID, _, err := arch.Snapshot(ctx, []string{"."}, SnapshotOptions{Time: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	TestEnsureSnapshot(t, repo, snapshotID, source)
}

func TestArchiverMandatorySelectRejectsExplicitTarget(t *testing.T) {
	ctx := t.Context()
	tempdir, repo := prepareTempdirRepoSrc(t, TestDir{"blocked": TestFile{Content: "must not be archived"}})
	arch := New(repo, fs.Track{FS: fs.NewLocal()}, Options{})
	arch.MandatorySelect = func(string, *fs.ExtendedFileInfo, fs.FS) bool { return false }
	back := rtest.Chdir(t, tempdir)
	defer back()
	if _, _, _, err := arch.Snapshot(ctx, []string{"blocked"}, SnapshotOptions{Time: time.Now()}); err == nil || err.Error() != "snapshot is empty" {
		t.Fatalf("mandatory policy did not reject explicit target: %v", err)
	}
}

// MockFS keeps track which files are read.
type MockFS struct {
	fs.FS

	m         sync.Mutex
	bytesRead map[string]int // tracks bytes read from all opened files
}

func (m *MockFS) OpenFile(name string, flag int, metadataOnly bool) (fs.File, error) {
	f, err := m.FS.OpenFile(name, flag, metadataOnly)
	if err != nil {
		return f, err
	}

	return MockFile{File: f, fs: m, filename: name}, nil
}

type MockFile struct {
	fs.File
	filename string

	fs *MockFS
}

func (f MockFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	if n > 0 {
		f.fs.m.Lock()
		f.fs.bytesRead[f.filename] += n
		f.fs.m.Unlock()
	}
	return n, err
}

func checkSnapshotStats(t *testing.T, sn *data.Snapshot, stat Summary) {
	t.Helper()
	rtest.Equals(t, stat.BackupStart, sn.Summary.BackupStart, "BackupStart")
	// BackupEnd is set to time.Now() and can't be compared to a fixed value
	rtest.Equals(t, stat.Files.New, sn.Summary.FilesNew, "FilesNew")
	rtest.Equals(t, stat.Files.Changed, sn.Summary.FilesChanged, "FilesChanged")
	rtest.Equals(t, stat.Files.Unchanged, sn.Summary.FilesUnmodified, "FilesUnmodified")
	rtest.Equals(t, stat.Dirs.New, sn.Summary.DirsNew, "DirsNew")
	rtest.Equals(t, stat.Dirs.Changed, sn.Summary.DirsChanged, "DirsChanged")
	rtest.Equals(t, stat.Dirs.Unchanged, sn.Summary.DirsUnmodified, "DirsUnmodified")
	rtest.Equals(t, stat.ProcessedBytes, sn.Summary.TotalBytesProcessed, "TotalBytesProcessed")
	rtest.Equals(t, stat.Files.New+stat.Files.Changed+stat.Files.Unchanged, sn.Summary.TotalFilesProcessed, "TotalFilesProcessed")
	bothZeroOrNeither(t, uint64(stat.DataBlobs), uint64(sn.Summary.DataBlobs))
	bothZeroOrNeither(t, uint64(stat.TreeBlobs), uint64(sn.Summary.TreeBlobs))
	bothZeroOrNeither(t, stat.DataSize+stat.TreeSize, sn.Summary.DataAdded)
	bothZeroOrNeither(t, stat.DataSizeInRepo+stat.TreeSizeInRepo, sn.Summary.DataAddedPacked)
}

func TestArchiverParent(t *testing.T) {
	var tests = []struct {
		src         TestDir
		modify      func(path string)
		opts        SnapshotOptions
		statInitial Summary
		statSecond  Summary
	}{
		{
			src: TestDir{
				"targetfile": TestFile{Content: string(rtest.Random(888, 2*1024*1024+5000))},
			},
			statInitial: Summary{
				Files:          ChangeStats{1, 0, 0},
				Dirs:           ChangeStats{0, 0, 0},
				ProcessedBytes: 2102152,
				ItemStats:      ItemStats{3, 0x201593, 0x201632, 1, 0, 0},
			},
			statSecond: Summary{
				Files:          ChangeStats{0, 0, 1},
				Dirs:           ChangeStats{0, 0, 0},
				ProcessedBytes: 2102152,
			},
		},
		{
			src: TestDir{
				"targetfile": TestFile{Content: string(rtest.Random(888, 2*1024*1024+5000))},
			},
			opts: SnapshotOptions{
				SkipIfUnchanged: true,
			},
			statInitial: Summary{
				Files:          ChangeStats{1, 0, 0},
				Dirs:           ChangeStats{0, 0, 0},
				ProcessedBytes: 2102152,
				ItemStats:      ItemStats{3, 0x201593, 0x201632, 1, 0, 0},
			},
			statSecond: Summary{
				Files:          ChangeStats{0, 0, 1},
				Dirs:           ChangeStats{0, 0, 0},
				ProcessedBytes: 2102152,
			},
		},
		{
			src: TestDir{
				"targetDir": TestDir{
					"targetfile":  TestFile{Content: string(rtest.Random(888, 1234))},
					"targetfile2": TestFile{Content: string(rtest.Random(888, 1235))},
				},
			},
			statInitial: Summary{
				Files:          ChangeStats{2, 0, 0},
				Dirs:           ChangeStats{1, 0, 0},
				ProcessedBytes: 2469,
				ItemStats:      ItemStats{2, 0xe1c, 0xcd9, 2, 0, 0},
			},
			statSecond: Summary{
				Files:          ChangeStats{0, 0, 2},
				Dirs:           ChangeStats{0, 0, 1},
				ProcessedBytes: 2469,
			},
		},
		{
			src: TestDir{
				"targetDir": TestDir{
					"targetfile": TestFile{Content: string(rtest.Random(888, 1234))},
				},
				"targetfile2": TestFile{Content: string(rtest.Random(888, 1235))},
			},
			modify: func(path string) {
				remove(t, filepath.Join(path, "targetDir", "targetfile"))
				save(t, filepath.Join(path, "targetfile2"), []byte("foobar"))
			},
			statInitial: Summary{
				Files:          ChangeStats{2, 0, 0},
				Dirs:           ChangeStats{1, 0, 0},
				ProcessedBytes: 2469,
				ItemStats:      ItemStats{2, 0xe13, 0xcf8, 2, 0, 0},
			},
			statSecond: Summary{
				Files:          ChangeStats{0, 1, 0},
				Dirs:           ChangeStats{0, 1, 0},
				ProcessedBytes: 6,
				ItemStats:      ItemStats{1, 0x305, 0x233, 2, 0, 0},
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			ctx := t.Context()

			tempdir, repo := prepareTempdirRepoSrc(t, test.src)

			testFS := &MockFS{
				FS:        fs.Track{FS: fs.NewLocal()},
				bytesRead: make(map[string]int),
			}

			arch := New(repo, testFS, Options{})

			back := rtest.Chdir(t, tempdir)
			defer back()

			opts := test.opts
			opts.Time = time.Now()
			firstSnapshot, firstSnapshotID, summary, err := arch.Snapshot(ctx, []string{"."}, opts)
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("first backup saved as %v", firstSnapshotID.Str())
			t.Logf("testfs: %v", testFS)

			// check that all files have been read exactly once
			TestWalkFiles(t, ".", test.src, func(filename string, item any) error {
				file, ok := item.(TestFile)
				if !ok {
					return nil
				}

				n, ok := testFS.bytesRead[filename]
				if !ok {
					t.Fatalf("file %v was not read at all", filename)
				}

				if n != len(file.Content) {
					t.Fatalf("file %v: read %v bytes, wanted %v bytes", filename, n, len(file.Content))
				}
				return nil
			})
			rtest.Equals(t, test.statInitial.Files, summary.Files)
			rtest.Equals(t, test.statInitial.Dirs, summary.Dirs)
			rtest.Equals(t, test.statInitial.ProcessedBytes, summary.ProcessedBytes)
			rtest.Assert(t, summary.BackupStart.Before(summary.BackupEnd), "BackupStart %v is not before BackupEnd %v", summary.BackupStart, summary.BackupEnd)

			checkSnapshotStats(t, firstSnapshot, test.statInitial)

			if test.modify != nil {
				test.modify(tempdir)
			}

			opts = test.opts
			opts.Time = time.Now()
			opts.ParentSnapshot = firstSnapshot
			testFS.bytesRead = map[string]int{}
			secondSnapshot, secondSnapshotID, summary, err := arch.Snapshot(ctx, []string{"."}, opts)
			if err != nil {
				t.Fatal(err)
			}

			if test.modify == nil {
				// check that no files were read this time
				rtest.Equals(t, map[string]int{}, testFS.bytesRead)
			}
			rtest.Equals(t, test.statSecond.Files, summary.Files)
			rtest.Equals(t, test.statSecond.Dirs, summary.Dirs)
			rtest.Equals(t, test.statSecond.ProcessedBytes, summary.ProcessedBytes)
			rtest.Assert(t, summary.BackupStart.Before(summary.BackupEnd), "BackupStart %v is not before BackupEnd %v", summary.BackupStart, summary.BackupEnd)

			if secondSnapshot != nil {
				checkSnapshotStats(t, secondSnapshot, test.statSecond)

				t.Logf("second backup saved as %v", secondSnapshotID.Str())
				t.Logf("testfs: %v", testFS)
			}

			checker.TestCheckRepo(t, repo)
		})
	}
}

func TestArchiverErrorReporting(t *testing.T) {
	ignoreErrorForBasename := func(basename string) ErrorFunc {
		return func(item string, err error) error {
			if filepath.Base(item) == basename {
				t.Logf("ignoring error for %v: %v", basename, err)
				return nil
			}

			t.Errorf("error handler called for unexpected file %v: %v", item, err)
			return err
		}
	}

	chmodUnreadable := func(filename string) func(testing.TB) {
		return func(t testing.TB) {
			if runtime.GOOS == "windows" {
				t.Skip("Skipping this test for windows")
			}

			err := os.Chmod(filepath.FromSlash(filename), 0004)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	var tests = []struct {
		name    string
		targets []string
		src     TestDir
		want    TestDir
		prepare func(t testing.TB)
		errFn   ErrorFunc
		errStr  []string
	}{
		{
			name: "no-error",
			src: TestDir{
				"targetfile": TestFile{Content: "foobar"},
			},
		},
		{
			name: "file-unreadable",
			src: TestDir{
				"targetfile": TestFile{Content: "foobar"},
			},
			prepare: chmodUnreadable("targetfile"),
			errStr:  []string{"open targetfile: permission denied"},
		},
		{
			name: "file-unreadable-ignore-error",
			src: TestDir{
				"targetfile": TestFile{Content: "foobar"},
				"other":      TestFile{Content: "xxx"},
			},
			want: TestDir{
				"other": TestFile{Content: "xxx"},
			},
			prepare: chmodUnreadable("targetfile"),
			errFn:   ignoreErrorForBasename("targetfile"),
		},
		{
			name: "file-subdir-unreadable",
			src: TestDir{
				"subdir": TestDir{
					"targetfile": TestFile{Content: "foobar"},
				},
			},
			prepare: chmodUnreadable("subdir/targetfile"),
			errStr:  []string{"open subdir/targetfile: permission denied"},
		},
		{
			name: "file-subdir-unreadable-ignore-error",
			src: TestDir{
				"subdir": TestDir{
					"targetfile": TestFile{Content: "foobar"},
					"other":      TestFile{Content: "xxx"},
				},
			},
			want: TestDir{
				"subdir": TestDir{
					"other": TestFile{Content: "xxx"},
				},
			},
			prepare: chmodUnreadable("subdir/targetfile"),
			errFn:   ignoreErrorForBasename("targetfile"),
		},
		{
			name:    "parent-dir-missing",
			targets: []string{"subdir/missing"},
			src:     TestDir{},
			errStr:  []string{"stat subdir: no such file or directory", "CreateFile subdir: The system cannot find the file specified", "GetFileAttributesEx subdir: The system cannot find the file specified"},
		},
		{
			name:    "parent-dir-missing-filtered",
			targets: []string{"targetfile", "subdir/missing"},
			src: TestDir{
				"targetfile": TestFile{Content: "foobar"},
			},
			errFn: ignoreErrorForBasename("subdir"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()

			tempdir, repo := prepareTempdirRepoSrc(t, test.src)

			back := rtest.Chdir(t, tempdir)
			defer back()

			if test.prepare != nil {
				test.prepare(t)
			}

			arch := New(repo, fs.Track{FS: fs.NewLocal()}, Options{})
			arch.Error = test.errFn

			target := test.targets
			if len(target) == 0 {
				target = []string{"."}
			}
			_, snapshotID, _, err := arch.Snapshot(ctx, target, SnapshotOptions{Time: time.Now()})
			if test.errStr != nil {
				// check if any of the expected errors are contained in the error message
				for _, errStr := range test.errStr {
					if strings.Contains(err.Error(), errStr) {
						t.Logf("found expected error (%v)", err)
						return
					}
				}

				t.Fatalf("expected error (%v) not returned by archiver, got (%v)", test.errStr, err)
				return
			}

			if err != nil {
				t.Fatalf("unexpected error of type %T found: %v", err, err)
			}

			t.Logf("saved as %v", snapshotID.Str())

			want := test.want
			if want == nil {
				want = test.src
			}
			TestEnsureSnapshot(t, repo, snapshotID, want)

			checker.TestCheckRepo(t, repo)
		})
	}
}

type noCancelBackend struct {
	backend.Backend
}

func (c *noCancelBackend) Remove(_ context.Context, h backend.Handle) error {
	return c.Backend.Remove(context.Background(), h)
}

func (c *noCancelBackend) Save(_ context.Context, h backend.Handle, rd backend.RewindReader) error {
	return c.Backend.Save(context.Background(), h, rd)
}

func (c *noCancelBackend) Load(_ context.Context, h backend.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	return c.Backend.Load(context.Background(), h, length, offset, fn)
}

func (c *noCancelBackend) Stat(_ context.Context, h backend.Handle) (backend.FileInfo, error) {
	return c.Backend.Stat(context.Background(), h)
}

func (c *noCancelBackend) List(_ context.Context, t backend.FileType, fn func(backend.FileInfo) error) error {
	return c.Backend.List(context.Background(), t, fn)
}

func (c *noCancelBackend) Delete(_ context.Context) error {
	return c.Backend.Delete(context.Background())
}

func TestArchiverContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tempdir := rtest.TempDir(t)
	TestCreateFiles(t, tempdir, TestDir{
		"targetfile": TestFile{Content: "foobar"},
	})

	// Ensure that the archiver itself reports the canceled context and not just the backend
	repo, _ := repository.TestRepositoryWithBackend(t, &noCancelBackend{mem.New()}, 0, repository.Options{})

	back := rtest.Chdir(t, tempdir)
	defer back()

	arch := New(repo, fs.Track{FS: fs.NewLocal()}, Options{})

	_, snapshotID, _, err := arch.Snapshot(ctx, []string{"."}, SnapshotOptions{Time: time.Now()})

	if err != nil {
		t.Logf("found expected error (%v)", err)
		return
	}
	if snapshotID.IsNull() {
		t.Fatalf("no error returned but found null id")
	}

	t.Fatalf("expected error not returned by archiver")
}

// TrackFS keeps track which files are opened. For some files, an error is injected.
type TrackFS struct {
	fs.FS

	opened map[string]uint
	m      sync.Mutex
}
