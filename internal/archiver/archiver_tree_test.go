package archiver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/sync/errgroup"
)

func TestFilChangedSpecialCases(t *testing.T) {
	tempdir := rtest.TempDir(t)

	filename := filepath.Join(tempdir, "file")
	content := []byte("foobar")
	save(t, filename, content)

	t.Run("nil-node", func(t *testing.T) {
		fi := lstat(t, filename)
		if !fileChanged(fi, nil, 0) {
			t.Fatal("nil node detected as unchanged")
		}
	})

	t.Run("type-change", func(t *testing.T) {
		fi := lstat(t, filename)
		node := nodeFromFile(t, fs.NewLocal(), filename)
		node.Type = data.NodeTypeSymlink
		if !fileChanged(fi, node, 0) {
			t.Fatal("node with changed type detected as unchanged")
		}
	})
}

func TestArchiverSaveDir(t *testing.T) {
	const targetNodeName = "targetdir"

	var tests = []struct {
		src    TestDir
		chdir  string
		target string
		want   TestDir
	}{
		{
			src: TestDir{
				"targetfile": TestFile{Content: string(rtest.Random(888, 20*1024+5000))},
			},
			target: ".",
			want: TestDir{
				"targetdir": TestDir{
					"targetfile": TestFile{Content: string(rtest.Random(888, 20*1024+5000))},
				},
			},
		},
		{
			src: TestDir{
				"targetdir": TestDir{
					"foo":        TestFile{Content: "foo"},
					"emptyfile":  TestFile{Content: ""},
					"bar":        TestFile{Content: "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"},
					"largefile":  TestFile{Content: string(rtest.Random(888, 1*1024*1024+5000))},
					"largerfile": TestFile{Content: string(rtest.Random(234, 3*1024*1024+5000))},
				},
			},
			target: "targetdir",
		},
		{
			src: TestDir{
				"foo":       TestFile{Content: "foo"},
				"emptyfile": TestFile{Content: ""},
				"bar":       TestFile{Content: "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"},
			},
			target: ".",
			want: TestDir{
				"targetdir": TestDir{
					"foo":       TestFile{Content: "foo"},
					"emptyfile": TestFile{Content: ""},
					"bar":       TestFile{Content: "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"},
				},
			},
		},
		{
			src: TestDir{
				"foo": TestDir{
					"subdir": TestDir{
						"x": TestFile{Content: "xxx"},
						"y": TestFile{Content: "yyyyyyyyyyyyyyyy"},
						"z": TestFile{Content: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
					},
					"file": TestFile{Content: "just a test"},
				},
			},
			chdir:  "foo/subdir",
			target: "../../",
			want: TestDir{
				"targetdir": TestDir{
					"foo": TestDir{
						"subdir": TestDir{
							"x": TestFile{Content: "xxx"},
							"y": TestFile{Content: "yyyyyyyyyyyyyyyy"},
							"z": TestFile{Content: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
						},
						"file": TestFile{Content: "just a test"},
					},
				},
			},
		},
		{
			src: TestDir{
				"foo": TestDir{
					"file":  TestFile{Content: "just a test"},
					"file2": TestFile{Content: "again"},
				},
			},
			target: "./foo",
			want: TestDir{
				"targetdir": TestDir{
					"file":  TestFile{Content: "just a test"},
					"file2": TestFile{Content: "again"},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			tempdir, repo := prepareTempdirRepoSrc(t, test.src)

			testFS := fs.Track{FS: fs.NewLocal()}
			arch := New(repo, testFS, Options{})
			arch.summary = &Summary{}

			chdir := tempdir
			if test.chdir != "" {
				chdir = filepath.Join(chdir, test.chdir)
			}

			back := rtest.Chdir(t, chdir)
			defer back()

			var treeID vaultic.ID
			err := repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
				wg, ctx := errgroup.WithContext(ctx)
				arch.runWorkers(ctx, wg, uploader)
				meta, err := testFS.OpenFile(test.target, fs.O_NOFOLLOW, true)
				rtest.OK(t, err)
				ft, err := arch.saveDir(ctx, "/", test.target, meta, nil, nil)
				rtest.OK(t, err)
				rtest.OK(t, meta.Close())

				fnr := ft.take(ctx)
				node, stats := fnr.node, fnr.stats

				t.Logf("stats: %v", stats)
				if stats.DataSize != 0 {
					t.Errorf("wrong stats returned in DataSize, want 0, got %d", stats.DataSize)
				}
				if stats.DataBlobs != 0 {
					t.Errorf("wrong stats returned in DataBlobs, want 0, got %d", stats.DataBlobs)
				}
				if stats.TreeSize == 0 {
					t.Errorf("wrong stats returned in TreeSize, want > 0, got %d", stats.TreeSize)
				}
				if stats.TreeBlobs <= 0 {
					t.Errorf("wrong stats returned in TreeBlobs, want > 0, got %d", stats.TreeBlobs)
				}

				node.Name = targetNodeName
				treeID = data.TestSaveNodes(t, ctx, uploader, []*data.Node{node})
				arch.stopWorkers()
				return wg.Wait()
			})
			if err != nil {
				t.Fatal(err)
			}

			want := test.want
			if want == nil {
				want = test.src
			}
			TestEnsureTree(context.TODO(), t, "/", repo, treeID, want)
		})
	}
}

type duplicateReaddirFS struct {
	fs.FS
	dir   string
	names []string
}

func (d *duplicateReaddirFS) OpenFile(name string, flag int, metadataOnly bool) (fs.File, error) {
	f, err := d.FS.OpenFile(name, flag, metadataOnly)
	if err != nil {
		return nil, err
	}

	if name == d.dir {
		return &duplicateReaddirFile{File: f, names: d.names}, nil
	}
	return f, nil
}

type duplicateReaddirFile struct {
	fs.File
	names []string
}

func (f *duplicateReaddirFile) Readdirnames(int) ([]string, error) {
	return append([]string(nil), f.names...), nil
}

func TestArchiverSaveDirDuplicateExcludedEntry(t *testing.T) {
	const targetNodeName = "targetdir"

	src := TestDir{
		"excluded": TestFile{Content: "skip me"},
		"keep":     TestFile{Content: "keep me"},
	}
	tempdir, repo := prepareTempdirRepoSrc(t, src)

	testFS := fs.Track{FS: &duplicateReaddirFS{
		FS:    fs.NewLocal(),
		dir:   ".",
		names: []string{"excluded", "excluded", "keep"},
	}}
	arch := New(repo, testFS, Options{})
	arch.summary = &Summary{}
	arch.Select = func(item string, fi *fs.ExtendedFileInfo, _ fs.FS) bool {
		return filepath.Base(item) != "excluded"
	}
	arch.Error = func(item string, err error) error {
		t.Errorf("unexpected archiver error for %v: %v", item, err)
		return err
	}

	back := rtest.Chdir(t, tempdir)
	defer back()

	// duplicate node check in tree finder is only done if the previous tree is not nil
	previousTree, err := data.NewTreeNodeIterator(strings.NewReader(`{"nodes":[]}`))
	rtest.OK(t, err)

	var treeID vaultic.ID
	err = repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		wg, ctx := errgroup.WithContext(ctx)
		arch.runWorkers(ctx, wg, uploader)
		meta, err := testFS.OpenFile(".", fs.O_NOFOLLOW, true)
		rtest.OK(t, err)
		ft, err := arch.saveDir(ctx, "/", ".", meta, previousTree, nil)
		rtest.OK(t, err)
		rtest.OK(t, meta.Close())

		fnr := ft.take(ctx)
		node := fnr.node
		node.Name = targetNodeName
		treeID = data.TestSaveNodes(t, ctx, uploader, []*data.Node{node})
		arch.stopWorkers()
		return wg.Wait()
	})
	rtest.OK(t, err)

	TestEnsureTree(context.TODO(), t, "/", repo, treeID, TestDir{
		"targetdir": TestDir{
			"keep": TestFile{Content: "keep me"},
		},
	})
}

func TestArchiverSaveDirIncremental(t *testing.T) {
	tempdir := rtest.TempDir(t)

	repo := &blobCountingRepo{
		archiverRepo: repository.TestRepository(t),
		saved:        make(map[vaultic.BlobHandle]uint),
	}

	appendToFile(t, filepath.Join(tempdir, "testfile"), []byte("foobar"))

	// save the empty directory several times in a row, then have a look if the
	// archiver did save the same tree several times
	for i := range 5 {
		testFS := fs.Track{FS: fs.NewLocal()}
		arch := New(repo, testFS, Options{})
		arch.summary = &Summary{}

		var fnr futureNodeResult
		err := repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
			wg, ctx := errgroup.WithContext(ctx)
			arch.runWorkers(ctx, wg, uploader)
			meta, err := testFS.OpenFile(tempdir, fs.O_NOFOLLOW, true)
			rtest.OK(t, err)
			ft, err := arch.saveDir(ctx, "/", tempdir, meta, nil, nil)
			rtest.OK(t, err)
			rtest.OK(t, meta.Close())

			fnr = ft.take(ctx)

			arch.stopWorkers()
			return wg.Wait()
		})
		if err != nil {
			t.Fatal(err)
		}

		node, stats := fnr.node, fnr.stats
		if i == 0 {
			// operation must have added new tree data
			if stats.DataSize != 0 {
				t.Errorf("wrong stats returned in DataSize, want 0, got %d", stats.DataSize)
			}
			if stats.DataBlobs != 0 {
				t.Errorf("wrong stats returned in DataBlobs, want 0, got %d", stats.DataBlobs)
			}
			if stats.TreeSize == 0 {
				t.Errorf("wrong stats returned in TreeSize, want > 0, got %d", stats.TreeSize)
			}
			if stats.TreeBlobs <= 0 {
				t.Errorf("wrong stats returned in TreeBlobs, want > 0, got %d", stats.TreeBlobs)
			}
		} else {
			// operation must not have added any new data
			if stats.DataSize != 0 {
				t.Errorf("wrong stats returned in DataSize, want 0, got %d", stats.DataSize)
			}
			if stats.DataBlobs != 0 {
				t.Errorf("wrong stats returned in DataBlobs, want 0, got %d", stats.DataBlobs)
			}
			if stats.TreeSize != 0 {
				t.Errorf("wrong stats returned in TreeSize, want 0, got %d", stats.TreeSize)
			}
			if stats.TreeBlobs != 0 {
				t.Errorf("wrong stats returned in TreeBlobs, want 0, got %d", stats.TreeBlobs)
			}
		}

		t.Logf("node subtree %v", node.Subtree)

		for h, n := range repo.saved {
			if n > 1 {
				t.Errorf("iteration %v: blob %v saved more than once (%d times)", i, h, n)
			}
		}
	}
}

// bothZeroOrNeither fails the test if only one of exp, act is zero.
func bothZeroOrNeither(tb testing.TB, exp, act uint64) {
	tb.Helper()
	if (exp == 0 && act != 0) || (exp != 0 && act == 0) {
		rtest.Equals(tb, exp, act)
	}
}

func TestArchiverSaveTree(t *testing.T) {
	symlink := func(from, to string) func(t testing.TB) {
		return func(t testing.TB) {
			err := os.Symlink(from, to)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// The toplevel directory is not counted in the ItemStats
	var tests = []struct {
		src     TestDir
		prepare func(t testing.TB)
		targets []string
		want    TestDir
		stat    Summary
	}{
		{
			src: TestDir{
				"targetfile": TestFile{Content: "foobar"},
			},
			targets: []string{"targetfile"},
			want: TestDir{
				"targetfile": TestFile{Content: "foobar"},
			},
			stat: Summary{
				ItemStats:      ItemStats{1, 6, 32 + 6, 0, 0, 0},
				ProcessedBytes: 6,
				Files:          ChangeStats{1, 0, 0},
				Dirs:           ChangeStats{0, 0, 0},
			},
		},
		{
			src: TestDir{
				"targetfile": TestFile{Content: "foobar"},
			},
			prepare: symlink("targetfile", "filesymlink"),
			targets: []string{"targetfile", "filesymlink"},
			want: TestDir{
				"targetfile":  TestFile{Content: "foobar"},
				"filesymlink": TestSymlink{Target: "targetfile"},
			},
			stat: Summary{
				ItemStats:      ItemStats{1, 6, 32 + 6, 0, 0, 0},
				ProcessedBytes: 6,
				Files:          ChangeStats{1, 0, 0},
				Dirs:           ChangeStats{0, 0, 0},
			},
		},
		{
			src: TestDir{
				"dir": TestDir{
					"subdir": TestDir{
						"subsubdir": TestDir{
							"targetfile": TestFile{Content: "foobar"},
						},
					},
					"otherfile": TestFile{Content: "xxx"},
				},
			},
			prepare: symlink("subdir", filepath.FromSlash("dir/symlink")),
			targets: []string{filepath.FromSlash("dir/symlink")},
			want: TestDir{
				"dir": TestDir{
					"symlink": TestSymlink{Target: "subdir"},
				},
			},
			stat: Summary{
				ItemStats:      ItemStats{0, 0, 0, 1, 0x154, 0x16a},
				ProcessedBytes: 0,
				Files:          ChangeStats{0, 0, 0},
				Dirs:           ChangeStats{1, 0, 0},
			},
		},
		{
			src: TestDir{
				"dir": TestDir{
					"subdir": TestDir{
						"subsubdir": TestDir{
							"targetfile": TestFile{Content: "foobar"},
						},
					},
					"otherfile": TestFile{Content: "xxx"},
				},
			},
			prepare: symlink("subdir", filepath.FromSlash("dir/symlink")),
			targets: []string{filepath.FromSlash("dir/symlink/subsubdir")},
			want: TestDir{
				"dir": TestDir{
					"symlink": TestDir{
						"subsubdir": TestDir{
							"targetfile": TestFile{Content: "foobar"},
						},
					},
				},
			},
			stat: Summary{
				ItemStats:      ItemStats{1, 6, 32 + 6, 3, 0x47f, 0x4c1},
				ProcessedBytes: 6,
				Files:          ChangeStats{1, 0, 0},
				Dirs:           ChangeStats{3, 0, 0},
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			tempdir, repo := prepareTempdirRepoSrc(t, test.src)

			testFS := fs.Track{FS: fs.NewLocal()}

			arch := New(repo, testFS, Options{})
			arch.summary = &Summary{}

			back := rtest.Chdir(t, tempdir)
			defer back()

			if test.prepare != nil {
				test.prepare(t)
			}

			var treeID vaultic.ID
			err := repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
				wg, ctx := errgroup.WithContext(ctx)
				arch.runWorkers(ctx, wg, uploader)

				bt, err := resolveRelativeTargets(testFS, test.targets)
				if err != nil {
					t.Fatal(err)
				}
				atree, err := newTree(testFS, bt)
				if err != nil {
					t.Fatal(err)
				}

				fn, _, err := arch.saveTree(ctx, "/", atree, nil, nil)
				if err != nil {
					t.Fatal(err)
				}

				fnr := fn.take(ctx)
				if fnr.err != nil {
					t.Fatal(fnr.err)
				}

				treeID = *fnr.node.Subtree

				arch.stopWorkers()
				return wg.Wait()
			})
			if err != nil {
				t.Fatal(err)
			}

			want := test.want
			if want == nil {
				want = test.src
			}
			TestEnsureTree(context.TODO(), t, "/", repo, treeID, want)
			stat := arch.summary
			bothZeroOrNeither(t, uint64(test.stat.DataBlobs), uint64(stat.DataBlobs))
			bothZeroOrNeither(t, uint64(test.stat.TreeBlobs), uint64(stat.TreeBlobs))
			bothZeroOrNeither(t, test.stat.DataSize, stat.DataSize)
			bothZeroOrNeither(t, test.stat.DataSizeInRepo, stat.DataSizeInRepo)
			bothZeroOrNeither(t, test.stat.TreeSizeInRepo, stat.TreeSizeInRepo)
			rtest.Equals(t, test.stat.ProcessedBytes, stat.ProcessedBytes)
			rtest.Equals(t, test.stat.Files, stat.Files)
			rtest.Equals(t, test.stat.Dirs, stat.Dirs)
		})
	}
}
