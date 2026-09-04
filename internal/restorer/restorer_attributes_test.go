package restorer

import (
	"bytes"
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type TraverseTreeCheck func(testing.TB) treeVisitor

type TreeVisit struct {
	funcName string   // name of the function
	location string   // location passed to the function
	files    []string // file list passed to the function
}

func checkVisitOrder(list []TreeVisit) TraverseTreeCheck {
	var pos int

	return func(t testing.TB) treeVisitor {
		check := func(funcName string) func(*data.Node, string, string, []string) error {
			return func(node *data.Node, target, location string, expectedFilenames []string) error {
				if pos >= len(list) {
					t.Errorf("step %v, %v(%v): expected no more than %d function calls", pos, funcName, location, len(list))
					pos++
					return nil
				}

				v := list[pos]

				if v.funcName != funcName {
					t.Errorf("step %v, location %v: want function %v, but %v was called",
						pos, location, v.funcName, funcName)
				}

				if location != filepath.FromSlash(v.location) {
					t.Errorf("step %v: want location %v, got %v", pos, list[pos].location, location)
				}

				if !reflect.DeepEqual(expectedFilenames, v.files) {
					t.Errorf("step %v: want files %v, got %v", pos, list[pos].files, expectedFilenames)
				}

				pos++
				return nil
			}
		}
		checkNoFilename := func(funcName string) func(*data.Node, string, string) error {
			f := check(funcName)
			return func(node *data.Node, target, location string) error {
				return f(node, target, location, nil)
			}
		}

		return treeVisitor{
			enterDir:  checkNoFilename("enterDir"),
			visitNode: checkNoFilename("visitNode"),
			leaveDir:  check("leaveDir"),
		}
	}
}

func TestRestorerTraverseTree(t *testing.T) {
	var tests = []struct {
		Snapshot
		Select  func(item string, isDir bool) (selectForRestore bool, childMayBeSelected bool)
		Visitor TraverseTreeCheck
	}{
		{
			// select everything
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"dir": Dir{Nodes: map[string]Node{
						"otherfile": File{Data: "x"},
						"subdir": Dir{Nodes: map[string]Node{
							"file": File{Data: "content: file\n"},
						}},
					}},
					"foo": File{Data: "content: foo\n"},
				},
			},
			Select: func(item string, isDir bool) (selectForRestore bool, childMayBeSelected bool) {
				return true, true
			},
			Visitor: checkVisitOrder([]TreeVisit{
				{"enterDir", "/", nil},
				{"enterDir", "/dir", nil},
				{"visitNode", "/dir/otherfile", nil},
				{"enterDir", "/dir/subdir", nil},
				{"visitNode", "/dir/subdir/file", nil},
				{"leaveDir", "/dir/subdir", []string{"file"}},
				{"leaveDir", "/dir", []string{"otherfile", "subdir"}},
				{"visitNode", "/foo", nil},
				{"leaveDir", "/", []string{"dir", "foo"}},
			}),
		},

		// select only the top-level file
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"dir": Dir{Nodes: map[string]Node{
						"otherfile": File{Data: "x"},
						"subdir": Dir{Nodes: map[string]Node{
							"file": File{Data: "content: file\n"},
						}},
					}},
					"foo": File{Data: "content: foo\n"},
				},
			},
			Select: func(item string, isDir bool) (selectForRestore bool, childMayBeSelected bool) {
				if item == "/foo" {
					return true, false
				}
				return false, false
			},
			Visitor: checkVisitOrder([]TreeVisit{
				{"enterDir", "/", nil},
				{"visitNode", "/foo", nil},
				{"leaveDir", "/", []string{"dir", "foo"}},
			}),
		},
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"aaa": File{Data: "content: foo\n"},
					"dir": Dir{Nodes: map[string]Node{
						"otherfile": File{Data: "x"},
						"subdir": Dir{Nodes: map[string]Node{
							"file": File{Data: "content: file\n"},
						}},
					}},
				},
			},
			Select: func(item string, isDir bool) (selectForRestore bool, childMayBeSelected bool) {
				if item == "/aaa" {
					return true, false
				}
				return false, false
			},
			Visitor: checkVisitOrder([]TreeVisit{
				{"enterDir", "/", nil},
				{"visitNode", "/aaa", nil},
				{"leaveDir", "/", []string{"aaa", "dir"}},
			}),
		},

		// select dir/
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"dir": Dir{Nodes: map[string]Node{
						"otherfile": File{Data: "x"},
						"subdir": Dir{Nodes: map[string]Node{
							"file": File{Data: "content: file\n"},
						}},
					}},
					"foo": File{Data: "content: foo\n"},
				},
			},
			Select: func(item string, isDir bool) (selectForRestore bool, childMayBeSelected bool) {
				if strings.HasPrefix(item, "/dir") {
					return true, true
				}
				return false, false
			},
			Visitor: checkVisitOrder([]TreeVisit{
				{"enterDir", "/", nil},
				{"enterDir", "/dir", nil},
				{"visitNode", "/dir/otherfile", nil},
				{"enterDir", "/dir/subdir", nil},
				{"visitNode", "/dir/subdir/file", nil},
				{"leaveDir", "/dir/subdir", []string{"file"}},
				{"leaveDir", "/dir", []string{"otherfile", "subdir"}},
				{"leaveDir", "/", []string{"dir", "foo"}},
			}),
		},

		// select only dir/otherfile
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"dir": Dir{Nodes: map[string]Node{
						"otherfile": File{Data: "x"},
						"subdir": Dir{Nodes: map[string]Node{
							"file": File{Data: "content: file\n"},
						}},
					}},
					"foo": File{Data: "content: foo\n"},
				},
			},
			Select: func(item string, isDir bool) (selectForRestore bool, childMayBeSelected bool) {
				switch item {
				case "/dir":
					return false, true
				case "/dir/otherfile":
					return true, false
				default:
					return false, false
				}
			},
			Visitor: checkVisitOrder([]TreeVisit{
				{"enterDir", "/", nil},
				{"visitNode", "/dir/otherfile", nil},
				{"leaveDir", "/dir", []string{"otherfile", "subdir"}},
				{"leaveDir", "/", []string{"dir", "foo"}},
			}),
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			repo := repository.TestRepository(t)
			sn, _ := saveSnapshot(t, repo, test.Snapshot, noopGetGenericAttributes)

			// set Delete option to enable tracking filenames in a directory
			res := NewRestorer(repo, sn, Options{Delete: true})

			res.SelectFilter = test.Select

			tempdir := rtest.TempDir(t)
			ctx := t.Context()

			// make sure we're creating a new subdir of the tempdir
			target := filepath.Join(tempdir, "target")

			err := res.traverseTree(ctx, target, *sn.Tree, test.Visitor(t))
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func normalizeFileMode(mode os.FileMode) os.FileMode {
	if runtime.GOOS == "windows" {
		if mode.IsDir() {
			return 0555 | os.ModeDir
		}
		return os.FileMode(0444)
	}
	return mode
}

func checkConsistentInfo(t testing.TB, file string, fi os.FileInfo, modtime time.Time, mode os.FileMode) {
	if fi.Mode() != mode {
		t.Errorf("checking %q, Mode() returned wrong value, want 0%o, got 0%o", file, mode, fi.Mode())
	}

	if !fi.ModTime().Equal(modtime) {
		t.Errorf("checking %s, ModTime() returned wrong value, want %v, got %v", file, modtime, fi.ModTime())
	}
}

// test inspired from test case https://github.com/otuschhoff/vaultic/issues/1212
func TestRestorerConsistentTimestampsAndPermissions(t *testing.T) {
	timeForTest := time.Date(2019, time.January, 9, 1, 46, 40, 0, time.UTC)

	repo := repository.TestRepository(t)

	sn, _ := saveSnapshot(t, repo, Snapshot{
		Nodes: map[string]Node{
			"dir": Dir{
				Mode:    normalizeFileMode(0750 | os.ModeDir),
				ModTime: timeForTest,
				Nodes: map[string]Node{
					"file1": File{
						Mode:    normalizeFileMode(os.FileMode(0700)),
						ModTime: timeForTest,
						Data:    "content: file\n",
					},
					"anotherfile": File{
						Data: "content: file\n",
					},
					"subdir": Dir{
						Mode:    normalizeFileMode(0700 | os.ModeDir),
						ModTime: timeForTest,
						Nodes: map[string]Node{
							"file2": File{
								Mode:    normalizeFileMode(os.FileMode(0666)),
								ModTime: timeForTest,
								Links:   2,
								Inode:   1,
							},
						},
					},
				},
			},
		},
	}, noopGetGenericAttributes)

	res := NewRestorer(repo, sn, Options{})

	res.SelectFilter = func(item string, isDir bool) (selectedForRestore bool, childMayBeSelected bool) {
		switch filepath.ToSlash(item) {
		case "/dir":
			childMayBeSelected = true
		case "/dir/file1":
			selectedForRestore = true
			childMayBeSelected = false
		case "/dir/subdir":
			selectedForRestore = true
			childMayBeSelected = true
		case "/dir/subdir/file2":
			selectedForRestore = true
			childMayBeSelected = false
		}
		return selectedForRestore, childMayBeSelected
	}

	tempdir := rtest.TempDir(t)
	ctx := t.Context()

	_, err := res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)

	var testPatterns = []struct {
		path    string
		modtime time.Time
		mode    os.FileMode
	}{
		{"dir", timeForTest, normalizeFileMode(0750 | os.ModeDir)},
		{filepath.Join("dir", "file1"), timeForTest, normalizeFileMode(os.FileMode(0700))},
		{filepath.Join("dir", "subdir"), timeForTest, normalizeFileMode(0700 | os.ModeDir)},
		{filepath.Join("dir", "subdir", "file2"), timeForTest, normalizeFileMode(os.FileMode(0666))},
	}

	for _, test := range testPatterns {
		f, err := os.Stat(filepath.Join(tempdir, test.path))
		rtest.OK(t, err)
		checkConsistentInfo(t, test.path, f, test.modtime, test.mode)
	}
}

// VerifyFiles must not report cancellation of its context through res.Error.
func TestVerifyCancel(t *testing.T) {
	snapshot := Snapshot{
		Nodes: map[string]Node{
			"foo": File{Data: "content: foo\n"},
		},
	}

	repo := repository.TestRepository(t)
	sn, _ := saveSnapshot(t, repo, snapshot, noopGetGenericAttributes)

	res := NewRestorer(repo, sn, Options{})

	tempdir := rtest.TempDir(t)
	ctx := t.Context()
	countRestoredFiles, err := res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)
	err = os.WriteFile(filepath.Join(tempdir, "foo"), []byte("bar"), 0644)
	rtest.OK(t, err)

	var errs []error
	res.Error = func(filename string, err error) error {
		errs = append(errs, err)
		return err
	}

	nverified, err := res.VerifyFiles(ctx, tempdir, countRestoredFiles, vaultic.NoopCounter)
	rtest.Equals(t, 0, nverified)
	rtest.Assert(t, err != nil, "nil error from VerifyFiles")
	rtest.Equals(t, 1, len(errs))
	rtest.Assert(t, strings.Contains(errs[0].Error(), "Invalid file size for"), "wrong error %q", errs[0].Error())
}

func TestRestorerSparseFiles(t *testing.T) {
	repo := repository.TestRepository(t)

	var zeros [1<<20 + 13]byte

	target, err := fs.NewReader("/zeros", io.NopCloser(bytes.NewReader(zeros[:])), fs.ReaderOptions{
		Mode: 0600,
	})
	rtest.OK(t, err)
	sc := archiver.NewScanner(target)
	err = sc.Scan(context.TODO(), []string{"/zeros"})
	rtest.OK(t, err)

	arch := archiver.New(repo, target, archiver.Options{})
	sn, _, _, err := arch.Snapshot(context.Background(), []string{"/zeros"},
		archiver.SnapshotOptions{})
	rtest.OK(t, err)

	res := NewRestorer(repo, sn, Options{Sparse: true})

	tempdir := rtest.TempDir(t)
	ctx := t.Context()

	_, err = res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)

	filename := filepath.Join(tempdir, "zeros")
	content, err := os.ReadFile(filename)
	rtest.OK(t, err)

	rtest.Equals(t, len(zeros[:]), len(content))
	rtest.Equals(t, zeros[:], content)

	blocks := getBlockCount(t, filename)
	if blocks < 0 {
		return
	}

	// st.Blocks is the size in 512-byte blocks.
	denseBlocks := math.Ceil(float64(len(zeros)) / 512)
	sparsity := 1 - float64(blocks)/denseBlocks

	// This should report 100% sparse. We don't assert that,
	// as the behavior of sparse writes depends on the underlying
	// file system as well as the OS.
	t.Logf("wrote %d zeros as %d blocks, %.1f%% sparse",
		len(zeros), blocks, 100*sparsity)
}

func saveSnapshotsAndOverwrite(t *testing.T, baseSnapshot Snapshot, overwriteSnapshot Snapshot, baseOptions, overwriteOptions Options) string {
	repo := repository.TestRepository(t)
	tempdir := filepath.Join(rtest.TempDir(t), "target")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// base snapshot
	sn, id := saveSnapshot(t, repo, baseSnapshot, noopGetGenericAttributes)
	t.Logf("base snapshot saved as %v", id.Str())

	res := NewRestorer(repo, sn, baseOptions)
	_, err := res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)

	// overwrite snapshot
	sn, id = saveSnapshot(t, repo, overwriteSnapshot, noopGetGenericAttributes)
	t.Logf("overwrite snapshot saved as %v", id.Str())
	res = NewRestorer(repo, sn, overwriteOptions)
	countRestoredFiles, err := res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)

	_, err = res.VerifyFiles(ctx, tempdir, countRestoredFiles, vaultic.NoopCounter)
	rtest.OK(t, err)

	return tempdir
}

func TestRestorerSparseOverwrite(t *testing.T) {
	baseSnapshot := Snapshot{
		Nodes: map[string]Node{
			"foo": File{Data: "content: new\n"},
		},
	}
	var zero [14]byte
	sparseSnapshot := Snapshot{
		Nodes: map[string]Node{
			"foo": File{Data: string(zero[:])},
		},
	}

	opts := Options{Sparse: true, Overwrite: OverwriteAlways}
	saveSnapshotsAndOverwrite(t, baseSnapshot, sparseSnapshot, opts, opts)
}

func TestRestorerOverwriteBehavior(t *testing.T) {
	baseTime := time.Now()
	baseSnapshot := Snapshot{
		Nodes: map[string]Node{
			"foo": File{Data: "content: foo\n", ModTime: baseTime},
			"dirtest": Dir{
				Nodes: map[string]Node{
					"file": File{Data: "content: file\n", ModTime: baseTime},
					"foo":  File{Data: "content: foobar", ModTime: baseTime},
				},
				ModTime: baseTime,
			},
		},
	}
	overwriteSnapshot := Snapshot{
		Nodes: map[string]Node{
			"foo": File{Data: "content: new\n", ModTime: baseTime.Add(time.Second)},
			"dirtest": Dir{
				Nodes: map[string]Node{
					"file": File{Data: "content: file2\n", ModTime: baseTime.Add(-time.Second)},
					"foo":  File{Data: "content: foo", ModTime: baseTime},
				},
			},
		},
	}

	var tests = []struct {
		Overwrite OverwriteBehavior
		Files     map[string]string
		Progress  progressState
	}{
		{
			Overwrite: OverwriteAlways,
			Files: map[string]string{
				"foo":          "content: new\n",
				"dirtest/file": "content: file2\n",
				"dirtest/foo":  "content: foo",
			},
			Progress: progressState{
				FilesFinished:   4,
				FilesTotal:      4,
				FilesSkipped:    0,
				AllBytesWritten: 40,
				AllBytesTotal:   40,
				AllBytesSkipped: 0,
			},
		},
		{
			Overwrite: OverwriteIfChanged,
			Files: map[string]string{
				"foo":          "content: new\n",
				"dirtest/file": "content: file2\n",
				"dirtest/foo":  "content: foo",
			},
			Progress: progressState{
				FilesFinished:   4,
				FilesTotal:      4,
				FilesSkipped:    0,
				AllBytesWritten: 40,
				AllBytesTotal:   40,
				AllBytesSkipped: 0,
			},
		},
		{
			Overwrite: OverwriteIfNewer,
			Files: map[string]string{
				"foo":          "content: new\n",
				"dirtest/file": "content: file\n",
				"dirtest/foo":  "content: foobar",
			},
			Progress: progressState{
				FilesFinished:   2,
				FilesTotal:      2,
				FilesSkipped:    2,
				AllBytesWritten: 13,
				AllBytesTotal:   13,
				AllBytesSkipped: 27,
			},
		},
		{
			Overwrite: OverwriteNever,
			Files: map[string]string{
				"foo":          "content: foo\n",
				"dirtest/file": "content: file\n",
				"dirtest/foo":  "content: foobar",
			},
			Progress: progressState{
				FilesFinished:   1,
				FilesTotal:      1,
				FilesSkipped:    3,
				AllBytesWritten: 0,
				AllBytesTotal:   0,
				AllBytesSkipped: 40,
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			progress := newTestProgress()
			tempdir := saveSnapshotsAndOverwrite(t, baseSnapshot, overwriteSnapshot, Options{}, Options{Overwrite: test.Overwrite, Progress: progress})

			for filename, content := range test.Files {
				data, err := os.ReadFile(filepath.Join(tempdir, filepath.FromSlash(filename)))
				if err != nil {
					t.Errorf("unable to read file %v: %v", filename, err)
					continue
				}

				if !bytes.Equal(data, []byte(content)) {
					t.Errorf("file %v has wrong content: want %q, got %q", filename, content, data)
				}
			}

			rtest.Equals(t, test.Progress, progress.state())
		})
	}
}
