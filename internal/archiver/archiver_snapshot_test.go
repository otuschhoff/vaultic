package archiver

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/checker"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestArchiverSnapshot(t *testing.T) {
	var tests = []struct {
		name    string
		src     TestDir
		want    TestDir
		chdir   string
		targets []string
	}{
		{
			name: "single-file",
			src: TestDir{
				"foo": TestFile{Content: "foo"},
			},
			targets: []string{"foo"},
		},
		{
			name: "file-current-dir",
			src: TestDir{
				"foo": TestFile{Content: "foo"},
			},
			targets: []string{"./foo"},
		},
		{
			name: "dir",
			src: TestDir{
				"target": TestDir{
					"foo": TestFile{Content: "foo"},
				},
			},
			targets: []string{"target"},
		},
		{
			name: "dir-current-dir",
			src: TestDir{
				"target": TestDir{
					"foo": TestFile{Content: "foo"},
				},
			},
			targets: []string{"./target"},
		},
		{
			name: "content-dir-current-dir",
			src: TestDir{
				"target": TestDir{
					"foo": TestFile{Content: "foo"},
				},
			},
			targets: []string{"./target/."},
		},
		{
			name: "current-dir",
			src: TestDir{
				"target": TestDir{
					"foo": TestFile{Content: "foo"},
				},
			},
			targets: []string{"."},
		},
		{
			name: "subdir",
			src: TestDir{
				"subdir": TestDir{
					"foo": TestFile{Content: "foo"},
					"subsubdir": TestDir{
						"foo": TestFile{Content: "foo in subsubdir"},
					},
				},
				"other": TestFile{Content: "another file"},
			},
			targets: []string{"subdir"},
			want: TestDir{
				"subdir": TestDir{
					"foo": TestFile{Content: "foo"},
					"subsubdir": TestDir{
						"foo": TestFile{Content: "foo in subsubdir"},
					},
				},
			},
		},
		{
			name: "subsubdir",
			src: TestDir{
				"subdir": TestDir{
					"foo": TestFile{Content: "foo"},
					"subsubdir": TestDir{
						"foo": TestFile{Content: "foo in subsubdir"},
					},
				},
				"other": TestFile{Content: "another file"},
			},
			targets: []string{"subdir/subsubdir"},
			want: TestDir{
				"subdir": TestDir{
					"subsubdir": TestDir{
						"foo": TestFile{Content: "foo in subsubdir"},
					},
				},
			},
		},
		{
			name: "parent-dir",
			src: TestDir{
				"subdir": TestDir{
					"foo": TestFile{Content: "foo"},
				},
				"other": TestFile{Content: "another file"},
			},
			chdir:   "subdir",
			targets: []string{".."},
		},
		{
			name: "parent-parent-dir",
			src: TestDir{
				"subdir": TestDir{
					"foo": TestFile{Content: "foo"},
					"subsubdir": TestDir{
						"empty": TestFile{Content: ""},
					},
				},
				"other": TestFile{Content: "another file"},
			},
			chdir:   "subdir/subsubdir",
			targets: []string{"../.."},
		},
		{
			name: "parent-parent-dir-slash",
			src: TestDir{
				"subdir": TestDir{
					"subsubdir": TestDir{
						"foo": TestFile{Content: "foo"},
					},
				},
				"other": TestFile{Content: "another file"},
			},
			chdir:   "subdir/subsubdir",
			targets: []string{"../../"},
			want: TestDir{
				"subdir": TestDir{
					"subsubdir": TestDir{
						"foo": TestFile{Content: "foo"},
					},
				},
				"other": TestFile{Content: "another file"},
			},
		},
		{
			name: "parent-subdir",
			src: TestDir{
				"subdir": TestDir{
					"foo": TestFile{Content: "foo"},
				},
				"other": TestFile{Content: "another file"},
			},
			chdir:   "subdir",
			targets: []string{"../subdir"},
			want: TestDir{
				"subdir": TestDir{
					"foo": TestFile{Content: "foo"},
				},
			},
		},
		{
			name: "parent-parent-dir-subdir",
			src: TestDir{
				"subdir": TestDir{
					"subsubdir": TestDir{
						"foo": TestFile{Content: "foo"},
					},
				},
				"other": TestFile{Content: "another file"},
			},
			chdir:   "subdir/subsubdir",
			targets: []string{"../../subdir/subsubdir"},
			want: TestDir{
				"subdir": TestDir{
					"subsubdir": TestDir{
						"foo": TestFile{Content: "foo"},
					},
				},
			},
		},
		{
			name: "included-multiple1",
			src: TestDir{
				"subdir": TestDir{
					"subsubdir": TestDir{
						"foo": TestFile{Content: "foo"},
					},
					"other": TestFile{Content: "another file"},
				},
			},
			targets: []string{"subdir", "subdir/subsubdir"},
		},
		{
			name: "included-multiple2",
			src: TestDir{
				"subdir": TestDir{
					"subsubdir": TestDir{
						"foo": TestFile{Content: "foo"},
					},
					"other": TestFile{Content: "another file"},
				},
			},
			targets: []string{"subdir/subsubdir", "subdir"},
		},
		{
			name: "collision",
			src: TestDir{
				"subdir": TestDir{
					"foo": TestFile{Content: "foo in subdir"},
					"subsubdir": TestDir{
						"foo": TestFile{Content: "foo in subsubdir"},
					},
				},
				"foo": TestFile{Content: "another file"},
			},
			chdir:   "subdir",
			targets: []string{".", "../foo"},
			want: TestDir{

				"foo": TestFile{Content: "foo in subdir"},
				"subsubdir": TestDir{
					"foo": TestFile{Content: "foo in subsubdir"},
				},
				"foo-1": TestFile{Content: "another file"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()

			tempdir, repo := prepareTempdirRepoSrc(t, test.src)

			arch := New(repo, fs.Track{FS: fs.NewLocal()}, Options{})

			chdir := tempdir
			if test.chdir != "" {
				chdir = filepath.Join(chdir, filepath.FromSlash(test.chdir))
			}

			back := rtest.Chdir(t, chdir)
			defer back()

			var targets []string
			for _, target := range test.targets {
				targets = append(targets, os.ExpandEnv(target))
			}

			t.Logf("targets: %v", targets)
			sn, snapshotID, _, err := arch.Snapshot(ctx, targets, SnapshotOptions{Time: time.Now()})
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("saved as %v", snapshotID.Str())

			want := test.want
			if want == nil {
				want = test.src
			}
			TestEnsureSnapshot(t, repo, snapshotID, want)

			checker.TestCheckRepo(t, repo)

			// check that the snapshot contains the targets with absolute paths
			for i, target := range sn.Paths {
				atarget, err := filepath.Abs(test.targets[i])
				if err != nil {
					t.Fatal(err)
				}

				if target != atarget {
					t.Errorf("wrong path in snapshot: want %v, got %v", atarget, target)
				}
			}
		})
	}
}

func TestDeferredSnapshotRemainsUnpublished(t *testing.T) {
	tempdir, repo := prepareTempdirRepoSrc(t, TestDir{"foo": TestFile{Content: "foo"}})
	arch := New(repo, fs.Track{FS: fs.NewLocal()}, Options{})
	publishCalled := false
	arch.BeforeSnapshot = func() error {
		publishCalled = true
		return nil
	}
	back := rtest.Chdir(t, tempdir)
	defer back()

	snapshot, snapshotID, _, err := arch.Snapshot(context.Background(), []string{"foo"}, SnapshotOptions{
		Time:             time.Now(),
		DeferredUploader: repo.WithBlobUploader,
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot == nil || snapshot.Tree == nil || !snapshotID.IsNull() {
		t.Fatalf("prospective snapshot = %#v, id=%s", snapshot, snapshotID)
	}
	if publishCalled {
		t.Fatal("deferred snapshot invoked publication hook")
	}
	_, _, _, err = arch.Snapshot(context.Background(), []string{"foo"}, SnapshotOptions{
		Time: time.Now(), ParentSnapshot: snapshot, DeferredUploader: repo.WithBlobUploader,
	})
	if err == nil {
		t.Fatal("deferred snapshot accepted a parent metadata basis")
	}
}

func TestArchiverReconcileNodeReceivesFinalNodes(t *testing.T) {
	repo := repository.TestRepository(t)
	testFS := fs.NewLocal()
	workingDirectory := t.TempDir()
	oldWorkingDirectory, err := os.Getwd()
	rtest.OK(t, err)
	rtest.OK(t, os.Chdir(workingDirectory))
	t.Cleanup(func() { rtest.OK(t, os.Chdir(oldWorkingDirectory)) })
	rtest.OK(t, os.Mkdir("dir", 0o755))
	rtest.OK(t, os.WriteFile(filepath.Join("dir", "file"), []byte("phase5"), 0o644))

	arch := New(repo, testFS, Options{})
	type observedNode struct {
		snapshotPath string
		sourcePath   string
		node         data.Node
	}
	var mu sync.Mutex
	var observed []observedNode
	arch.ReconcileNode = func(snapshotPath, sourcePath string, node *data.Node) {
		mu.Lock()
		defer mu.Unlock()
		copyNode := *node
		copyNode.Content = append(vaultic.IDs(nil), node.Content...)
		observed = append(observed, observedNode{snapshotPath: snapshotPath, sourcePath: sourcePath, node: copyNode})
	}
	_, _, _, err = arch.Snapshot(context.Background(), []string{"dir"}, SnapshotOptions{Time: time.Now()})
	rtest.OK(t, err)

	mu.Lock()
	defer mu.Unlock()
	var sawFile, sawDirectory bool
	for _, item := range observed {
		switch item.node.Type {
		case data.NodeTypeFile:
			sawFile = item.snapshotPath == "/dir/file" && filepath.Clean(item.sourcePath) == filepath.Clean("dir/file") && len(item.node.Content) > 0
		case data.NodeTypeDir:
			sawDirectory = item.snapshotPath == "/dir" && filepath.Clean(item.sourcePath) == "dir" && item.node.Subtree != nil
		}
	}
	if !sawFile || !sawDirectory {
		t.Fatalf("final reconciliation nodes: file=%t directory=%t observed=%#v", sawFile, sawDirectory, observed)
	}
}

func TestResolveRelativeTargetsSpecial(t *testing.T) {
	var tests = []struct {
		name     string
		targets  []string
		expected []string
		win      bool
	}{
		{
			name:     "basic relative path",
			targets:  []string{filepath.FromSlash("some/path")},
			expected: []string{filepath.FromSlash("some/path")},
		},
		{
			name:     "partial relative path",
			targets:  []string{filepath.FromSlash("../some/path")},
			expected: []string{filepath.FromSlash("../some/path")},
		},
		{
			name:     "basic absolute path",
			targets:  []string{filepath.FromSlash("/some/path")},
			expected: []string{filepath.FromSlash("/some/path")},
		},
		{
			name:     "volume name",
			targets:  []string{"C:"},
			expected: []string{"C:\\"},
			win:      true,
		},
		{
			name:     "volume root path",
			targets:  []string{"C:\\"},
			expected: []string{"C:\\"},
			win:      true,
		},
		{
			name:     "UNC path",
			targets:  []string{"\\\\server\\volume"},
			expected: []string{"\\\\server\\volume\\"},
			win:      true,
		},
		{
			name:     "UNC path with trailing slash",
			targets:  []string{"\\\\server\\volume\\"},
			expected: []string{"\\\\server\\volume\\"},
			win:      true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.win && runtime.GOOS != "windows" {
				t.Skip("skip test on unix")
			}

			targets, err := resolveRelativeTargets(fs.NewLocal(), test.targets)
			rtest.OK(t, err)
			paths := make([]string, len(targets))
			for i, tgt := range targets {
				paths[i] = tgt.Path
			}
			rtest.Equals(t, test.expected, paths)
		})
	}
}

func TestArchiverSnapshotSelect(t *testing.T) {
	var tests = []struct {
		name  string
		src   TestDir
		want  TestDir
		selFn SelectFunc
		err   string
	}{
		{
			name: "include-all",
			src: TestDir{
				"work": TestDir{
					"foo":     TestFile{Content: "foo"},
					"foo.txt": TestFile{Content: "foo text file"},
					"subdir": TestDir{
						"other":   TestFile{Content: "other in subdir"},
						"bar.txt": TestFile{Content: "bar.txt in subdir"},
					},
				},
				"other": TestFile{Content: "another file"},
			},
			selFn: func(item string, fi *fs.ExtendedFileInfo, _ fs.FS) bool {
				return true
			},
		},
		{
			name: "exclude-all",
			src: TestDir{
				"work": TestDir{
					"foo":     TestFile{Content: "foo"},
					"foo.txt": TestFile{Content: "foo text file"},
					"subdir": TestDir{
						"other":   TestFile{Content: "other in subdir"},
						"bar.txt": TestFile{Content: "bar.txt in subdir"},
					},
				},
				"other": TestFile{Content: "another file"},
			},
			selFn: func(item string, fi *fs.ExtendedFileInfo, _ fs.FS) bool {
				return false
			},
			err: "snapshot is empty",
		},
		{
			name: "exclude-txt-files",
			src: TestDir{
				"work": TestDir{
					"foo":     TestFile{Content: "foo"},
					"foo.txt": TestFile{Content: "foo text file"},
					"subdir": TestDir{
						"other":   TestFile{Content: "other in subdir"},
						"bar.txt": TestFile{Content: "bar.txt in subdir"},
					},
				},
				"other": TestFile{Content: "another file"},
			},
			want: TestDir{
				"work": TestDir{
					"foo": TestFile{Content: "foo"},
					"subdir": TestDir{
						"other": TestFile{Content: "other in subdir"},
					},
				},
				"other": TestFile{Content: "another file"},
			},
			selFn: func(item string, fi *fs.ExtendedFileInfo, _ fs.FS) bool {
				return filepath.Ext(item) != ".txt"
			},
		},
		{
			name: "exclude-dir",
			src: TestDir{
				"work": TestDir{
					"foo":     TestFile{Content: "foo"},
					"foo.txt": TestFile{Content: "foo text file"},
					"subdir": TestDir{
						"other":   TestFile{Content: "other in subdir"},
						"bar.txt": TestFile{Content: "bar.txt in subdir"},
					},
				},
				"other": TestFile{Content: "another file"},
			},
			want: TestDir{
				"work": TestDir{
					"foo":     TestFile{Content: "foo"},
					"foo.txt": TestFile{Content: "foo text file"},
				},
				"other": TestFile{Content: "another file"},
			},
			selFn: func(item string, fi *fs.ExtendedFileInfo, fs fs.FS) bool {
				return fs.Base(item) != "subdir"
			},
		},
		{
			name: "select-absolute-paths",
			src: TestDir{
				"foo": TestFile{Content: "foo"},
			},
			selFn: func(item string, fi *fs.ExtendedFileInfo, fs fs.FS) bool {
				return fs.IsAbs(item)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()

			tempdir, repo := prepareTempdirRepoSrc(t, test.src)

			arch := New(repo, fs.Track{FS: fs.NewLocal()}, Options{})
			arch.Select = test.selFn

			back := rtest.Chdir(t, tempdir)
			defer back()

			targets := []string{"."}
			_, snapshotID, _, err := arch.Snapshot(ctx, targets, SnapshotOptions{Time: time.Now()})
			if test.err != "" {
				if err == nil {
					t.Fatalf("expected error not found, got %v, wanted %q", err, test.err)
				}

				if err.Error() != test.err {
					t.Fatalf("unexpected error, want %q, got %q", test.err, err)
				}

				return
			}

			if err != nil {
				t.Fatal(err)
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

// TestArchiverExplicitBackupTarget checks that tree.Explicit (paths the user
// listed literally, after resolveRelativeTargets) skips Select/SelectByName for
// that path only, while descendants and expanded targets still obey Select.
func TestArchiverExplicitBackupTarget(t *testing.T) {
	includeExceptTxtFiles := func(item string, fi *fs.ExtendedFileInfo, _ fs.FS) bool {
		if fi.Mode.IsDir() {
			return true
		}
		return filepath.Ext(item) != ".txt"
	}

	var tests = []struct {
		name    string
		src     TestDir
		targets []string
		want    TestDir
		selFn   SelectFunc
	}{
		{
			name: "explicit-file-skips-select-for-that-path",
			src: TestDir{
				"important.txt": TestFile{Content: "keep me"},
			},
			targets: []string{filepath.FromSlash("important.txt")},
			want: TestDir{
				"important.txt": TestFile{Content: "keep me"},
			},
			selFn: includeExceptTxtFiles,
		},
		{
			name: "explicit-dir-children-still-filtered",
			src: TestDir{
				"vault": TestDir{
					"keep.bin": TestFile{Content: "bin"},
					"skip.txt": TestFile{Content: "txt"},
				},
			},
			targets: []string{"vault"},
			want: TestDir{
				"vault": TestDir{
					"keep.bin": TestFile{Content: "bin"},
				},
			},
			selFn: includeExceptTxtFiles,
		},
		{
			name: "expanded-paths-from-dot-stay-filtered",
			src: TestDir{
				"work": TestDir{
					"a.txt": TestFile{Content: "a"},
					"b.bin": TestFile{Content: "b"},
				},
				"noise.txt": TestFile{Content: "n"},
			},
			targets: []string{"."},
			want: TestDir{
				"work": TestDir{
					"b.bin": TestFile{Content: "b"},
				},
			},
			selFn: includeExceptTxtFiles,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()

			tempdir, repo := prepareTempdirRepoSrc(t, test.src)

			arch := New(repo, fs.Track{FS: fs.NewLocal()}, Options{})
			arch.Select = test.selFn

			back := rtest.Chdir(t, tempdir)
			defer back()

			_, snapshotID, _, err := arch.Snapshot(ctx, test.targets, SnapshotOptions{Time: time.Now()})
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("saved as %v", snapshotID.Str())

			TestEnsureSnapshot(t, repo, snapshotID, test.want)
			checker.TestCheckRepo(t, repo)
		})
	}
}
