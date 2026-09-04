package restorer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestRestorerOverwritePartial(t *testing.T) {
	parts := make([]string, 100)
	size := 0
	for i := range parts {
		parts[i] = fmt.Sprint(i)
		size += len(parts[i])
		if i < 8 {
			// small file
			size += len(parts[i])
		}
	}

	// the data of both snapshots is stored in different pack files
	// thus both small an foo in the overwriteSnapshot contain blobs from
	// two different pack files. This tests basic handling of blobs from
	// different pack files.
	baseTime := time.Now()
	baseSnapshot := Snapshot{
		Nodes: map[string]Node{
			"foo":   File{DataParts: parts[0:5], ModTime: baseTime},
			"small": File{DataParts: parts[0:5], ModTime: baseTime},
		},
	}
	overwriteSnapshot := Snapshot{
		Nodes: map[string]Node{
			"foo":   File{DataParts: parts, ModTime: baseTime},
			"small": File{DataParts: parts[0:8], ModTime: baseTime},
		},
	}

	progress := newTestProgress()
	saveSnapshotsAndOverwrite(t, baseSnapshot, overwriteSnapshot, Options{}, Options{Overwrite: OverwriteAlways, Progress: progress})
	rtest.Equals(t, progressState{
		FilesFinished:   2,
		FilesTotal:      2,
		FilesSkipped:    0,
		AllBytesWritten: uint64(size),
		AllBytesTotal:   uint64(size),
		AllBytesSkipped: 0,
	}, progress.state())
}

func TestRestorerOverwriteSpecial(t *testing.T) {
	baseTime := time.Now()
	baseSnapshot := Snapshot{
		Nodes: map[string]Node{
			"dirtest":  Dir{ModTime: baseTime},
			"link":     Symlink{Target: "foo", ModTime: baseTime},
			"file":     File{Data: "content: file\n", Inode: 42, Links: 2, ModTime: baseTime},
			"hardlink": File{Data: "content: file\n", Inode: 42, Links: 2, ModTime: baseTime},
			"newdir":   File{Data: "content: dir\n", ModTime: baseTime},
		},
	}
	overwriteSnapshot := Snapshot{
		Nodes: map[string]Node{
			"dirtest":  Symlink{Target: "foo", ModTime: baseTime},
			"link":     File{Data: "content: link\n", Inode: 42, Links: 2, ModTime: baseTime.Add(time.Second)},
			"file":     Symlink{Target: "foo2", ModTime: baseTime},
			"hardlink": File{Data: "content: link\n", Inode: 42, Links: 2, ModTime: baseTime.Add(time.Second)},
			"newdir":   Dir{ModTime: baseTime},
		},
	}

	files := map[string]string{
		"link":     "content: link\n",
		"hardlink": "content: link\n",
	}
	links := map[string]string{
		"dirtest": "foo",
		"file":    "foo2",
	}

	opts := Options{Overwrite: OverwriteAlways}
	tempdir := saveSnapshotsAndOverwrite(t, baseSnapshot, overwriteSnapshot, opts, opts)

	for filename, content := range files {
		data, err := os.ReadFile(filepath.Join(tempdir, filepath.FromSlash(filename)))
		if err != nil {
			t.Errorf("unable to read file %v: %v", filename, err)
			continue
		}

		if !bytes.Equal(data, []byte(content)) {
			t.Errorf("file %v has wrong content: want %q, got %q", filename, content, data)
		}
	}
	for filename, target := range links {
		link, err := os.Readlink(filepath.Join(tempdir, filepath.FromSlash(filename)))
		rtest.OK(t, err)
		rtest.Equals(t, link, target, "wrong symlink target")
	}
}

func TestRestoreModified(t *testing.T) {
	// overwrite files between snapshots and also change their filesize
	snapshots := []Snapshot{
		{
			Nodes: map[string]Node{
				"foo": File{Data: "content: foo\n", ModTime: time.Now()},
				"bar": File{Data: "content: a\n", ModTime: time.Now()},
			},
		},
		{
			Nodes: map[string]Node{
				"foo": File{Data: "content: a\n", ModTime: time.Now()},
				"bar": File{Data: "content: bar\n", ModTime: time.Now()},
			},
		},
	}

	repo := repository.TestRepository(t)
	tempdir := filepath.Join(rtest.TempDir(t), "target")
	ctx := t.Context()

	for _, snapshot := range snapshots {
		sn, id := saveSnapshot(t, repo, snapshot, noopGetGenericAttributes)
		t.Logf("snapshot saved as %v", id.Str())

		res := NewRestorer(repo, sn, Options{Overwrite: OverwriteIfChanged})
		countRestoredFiles, err := res.RestoreTo(ctx, tempdir)
		rtest.OK(t, err)
		n, err := res.VerifyFiles(ctx, tempdir, countRestoredFiles, vaultic.NoopCounter)
		rtest.OK(t, err)
		rtest.Equals(t, 2, n, "unexpected number of verified files")
	}
}

func TestRestoreIfChanged(t *testing.T) {
	origData := "content: foo\n"
	modData := "content: bar\n"
	rtest.Equals(t, len(modData), len(origData), "broken testcase")
	snapshot := Snapshot{
		Nodes: map[string]Node{
			"foo": File{Data: origData, ModTime: time.Now()},
		},
	}

	repo := repository.TestRepository(t)
	tempdir := filepath.Join(rtest.TempDir(t), "target")
	ctx := t.Context()

	sn, id := saveSnapshot(t, repo, snapshot, noopGetGenericAttributes)
	t.Logf("snapshot saved as %v", id.Str())

	res := NewRestorer(repo, sn, Options{})
	_, err := res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)

	// modify file but maintain size and timestamp
	path := filepath.Join(tempdir, "foo")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	rtest.OK(t, err)
	fi, err := f.Stat()
	rtest.OK(t, err)
	_, err = f.Write([]byte(modData))
	rtest.OK(t, err)
	rtest.OK(t, f.Close())
	var utimes = [...]syscall.Timespec{
		syscall.NsecToTimespec(fi.ModTime().UnixNano()),
		syscall.NsecToTimespec(fi.ModTime().UnixNano()),
	}
	rtest.OK(t, syscall.UtimesNano(path, utimes[:]))

	for _, overwrite := range []OverwriteBehavior{OverwriteIfChanged, OverwriteAlways} {
		res = NewRestorer(repo, sn, Options{Overwrite: overwrite})
		_, err := res.RestoreTo(ctx, tempdir)
		rtest.OK(t, err)
		data, err := os.ReadFile(path)
		rtest.OK(t, err)
		if overwrite == OverwriteAlways {
			// restore should notice the changed file content
			rtest.Equals(t, origData, string(data), "expected original file content")
		} else {
			// restore should not have noticed the changed file content
			rtest.Equals(t, modData, string(data), "expected modified file content")
		}
	}
}

func TestRestoreDryRun(t *testing.T) {
	snapshot := Snapshot{
		Nodes: map[string]Node{
			"foo":  File{Data: "content: foo\n", Links: 2, Inode: 42},
			"foo2": File{Data: "content: foo\n", Links: 2, Inode: 42},
			"dirtest": Dir{
				Nodes: map[string]Node{
					"file": File{Data: "content: file\n"},
				},
			},
			"link": Symlink{Target: "foo"},
		},
	}

	repo := repository.TestRepository(t)
	tempdir := filepath.Join(rtest.TempDir(t), "target")
	ctx := t.Context()

	sn, id := saveSnapshot(t, repo, snapshot, noopGetGenericAttributes)
	t.Logf("snapshot saved as %v", id.Str())

	res := NewRestorer(repo, sn, Options{DryRun: true})
	_, err := res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)

	_, err = os.Stat(tempdir)
	rtest.Assert(t, errors.Is(err, os.ErrNotExist), "expected no file to be created, got %v", err)
}

func TestRestoreDryRunDelete(t *testing.T) {
	snapshot := Snapshot{
		Nodes: map[string]Node{
			"foo": File{Data: "content: foo\n"},
		},
	}

	repo := repository.TestRepository(t)
	tempdir := filepath.Join(rtest.TempDir(t), "target")
	tempfile := filepath.Join(tempdir, "existing")
	ctx := t.Context()

	rtest.OK(t, os.Mkdir(tempdir, 0o755))
	f, err := os.Create(tempfile)
	rtest.OK(t, err)
	rtest.OK(t, f.Close())

	sn, _ := saveSnapshot(t, repo, snapshot, noopGetGenericAttributes)
	res := NewRestorer(repo, sn, Options{DryRun: true, Delete: true})
	_, err = res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)

	_, err = os.Stat(tempfile)
	rtest.Assert(t, err == nil, "expected file to still exist, got error %v", err)
}

func TestRestoreOverwriteDirectory(t *testing.T) {
	saveSnapshotsAndOverwrite(t,
		Snapshot{
			Nodes: map[string]Node{
				"dir": Dir{
					Mode: normalizeFileMode(0755 | os.ModeDir),
					Nodes: map[string]Node{
						"anotherfile": File{Data: "content: file\n"},
					},
				},
			},
		},
		Snapshot{
			Nodes: map[string]Node{
				"dir": File{Data: "content: file\n"},
			},
		},
		Options{},
		Options{Delete: true},
	)
}

func TestRestoreDelete(t *testing.T) {
	repo := repository.TestRepository(t)
	tempdir := rtest.TempDir(t)

	sn, _ := saveSnapshot(t, repo, Snapshot{
		Nodes: map[string]Node{
			"dir": Dir{
				Mode: normalizeFileMode(0755 | os.ModeDir),
				Nodes: map[string]Node{
					"file1":       File{Data: "content: file\n"},
					"anotherfile": File{Data: "content: file\n"},
				},
			},
			"dir2": Dir{
				Mode: normalizeFileMode(0755 | os.ModeDir),
				Nodes: map[string]Node{
					"anotherfile": File{Data: "content: file\n"},
				},
			},
			"anotherfile": File{Data: "content: file\n"},
		},
	}, noopGetGenericAttributes)

	// should delete files that no longer exist in the snapshot
	deleteSn, _ := saveSnapshot(t, repo, Snapshot{
		Nodes: map[string]Node{
			"dir": Dir{
				Mode: normalizeFileMode(0755 | os.ModeDir),
				Nodes: map[string]Node{
					"file1": File{Data: "content: file\n"},
				},
			},
		},
	}, noopGetGenericAttributes)

	tests := []struct {
		selectFilter func(item string, isDir bool) (selectedForRestore bool, childMayBeSelected bool)
		fileState    map[string]bool
	}{
		{
			selectFilter: nil,
			fileState: map[string]bool{
				"dir":                                true,
				filepath.Join("dir", "anotherfile"):  false,
				filepath.Join("dir", "file1"):        true,
				"dir2":                               false,
				filepath.Join("dir2", "anotherfile"): false,
				"anotherfile":                        false,
			},
		},
		{
			selectFilter: func(item string, isDir bool) (selectedForRestore bool, childMayBeSelected bool) {
				return false, false
			},
			fileState: map[string]bool{
				"dir":                                true,
				filepath.Join("dir", "anotherfile"):  true,
				filepath.Join("dir", "file1"):        true,
				"dir2":                               true,
				filepath.Join("dir2", "anotherfile"): true,
				"anotherfile":                        true,
			},
		},
		{
			selectFilter: func(item string, isDir bool) (selectedForRestore bool, childMayBeSelected bool) {
				switch item {
				case filepath.FromSlash("/dir"):
					selectedForRestore = true
				case filepath.FromSlash("/dir2"):
					selectedForRestore = true
				}
				return
			},
			fileState: map[string]bool{
				"dir":                                true,
				filepath.Join("dir", "anotherfile"):  true,
				filepath.Join("dir", "file1"):        true,
				"dir2":                               false,
				filepath.Join("dir2", "anotherfile"): false,
				"anotherfile":                        true,
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			res := NewRestorer(repo, sn, Options{})
			ctx := t.Context()

			_, err := res.RestoreTo(ctx, tempdir)
			rtest.OK(t, err)

			res = NewRestorer(repo, deleteSn, Options{Delete: true})
			if test.selectFilter != nil {
				res.SelectFilter = test.selectFilter
			}
			_, err = res.RestoreTo(ctx, tempdir)
			rtest.OK(t, err)

			for fn, shouldExist := range test.fileState {
				_, err := os.Stat(filepath.Join(tempdir, fn))
				if shouldExist {
					rtest.OK(t, err)
				} else {
					rtest.Assert(t, errors.Is(err, os.ErrNotExist), "file %v: unexpected error got %v, expected ErrNotExist", fn, err)
				}
			}
		})
	}
}

func TestRestoreToFile(t *testing.T) {
	snapshot := Snapshot{
		Nodes: map[string]Node{
			"foo": File{Data: "content: foo\n"},
		},
	}

	repo := repository.TestRepository(t)
	tempdir := filepath.Join(rtest.TempDir(t), "target")

	// create a file in the place of the target directory
	rtest.OK(t, os.WriteFile(tempdir, []byte{}, 0o700))

	ctx := t.Context()

	sn, _ := saveSnapshot(t, repo, snapshot, noopGetGenericAttributes)
	res := NewRestorer(repo, sn, Options{})
	_, err := res.RestoreTo(ctx, tempdir)
	rtest.Assert(t, strings.Contains(err.Error(), "cannot create target directory"), "unexpected error %v", err)
}

func TestRestorerLongPath(t *testing.T) {
	tmp := t.TempDir()

	longPath := tmp
	for range 20 {
		longPath = filepath.Join(longPath, "aaaaaaaaaaaaaaaaaaaa")
	}

	rtest.OK(t, os.MkdirAll(longPath, 0o700))
	f, err := fs.OpenFile(filepath.Join(longPath, "file"), fs.O_CREATE|fs.O_RDWR, 0o600)
	rtest.OK(t, err)
	_, err = f.WriteString("Hello, World!")
	rtest.OK(t, err)
	rtest.OK(t, f.Close())

	repo := repository.TestRepository(t)

	local := fs.NewLocal()
	sc := archiver.NewScanner(local)
	rtest.OK(t, sc.Scan(context.TODO(), []string{tmp}))
	arch := archiver.New(repo, local, archiver.Options{})
	sn, _, _, err := arch.Snapshot(context.Background(), []string{tmp}, archiver.SnapshotOptions{})
	rtest.OK(t, err)

	res := NewRestorer(repo, sn, Options{})
	ctx := t.Context()

	countRestoredFiles, err := res.RestoreTo(ctx, tmp)
	rtest.OK(t, err)
	_, err = res.VerifyFiles(ctx, tmp, countRestoredFiles, vaultic.NoopCounter)
	rtest.OK(t, err)
}
