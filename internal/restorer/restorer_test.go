package restorer

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/repository"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type Node any

type Snapshot struct {
	Nodes map[string]Node
}

type File struct {
	Data       string
	DataParts  []string
	Links      uint64
	Inode      uint64
	Mode       os.FileMode
	ModTime    time.Time
	attributes *FileAttributes
}

type Symlink struct {
	Target  string
	ModTime time.Time
}

type Dir struct {
	Nodes      map[string]Node
	Mode       os.FileMode
	ModTime    time.Time
	attributes *FileAttributes
}

type FileAttributes struct {
	ReadOnly  bool
	Hidden    bool
	System    bool
	Archive   bool
	Encrypted bool
}

func saveFile(t testing.TB, repo vaultic.BlobSaver, data string) vaultic.ID {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	id, _, _, err := repo.SaveBlob(ctx, vaultic.DataBlob, []byte(data), vaultic.ID{}, false)
	if err != nil {
		t.Fatal(err)
	}

	return id
}

func saveDir(
	t testing.TB,
	repo vaultic.BlobSaver,
	nodes map[string]Node,
	inode uint64,
	getGenericAttributes func(attr *FileAttributes, isDir bool) (genericAttributes map[data.GenericAttributeType]json.RawMessage),
) vaultic.ID {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tree := make([]*data.Node, 0, len(nodes))
	for name, n := range nodes {
		inode++
		switch node := n.(type) {
		case File:
			fi := node.Inode
			if fi == 0 {
				fi = inode
			}
			lc := node.Links
			if lc == 0 {
				lc = 1
			}
			fc := []vaultic.ID{}
			size := 0
			if len(node.Data) > 0 {
				size = len(node.Data)
				fc = append(fc, saveFile(t, repo, node.Data))
			} else if len(node.DataParts) > 0 {
				for _, part := range node.DataParts {
					fc = append(fc, saveFile(t, repo, part))
					size += len(part)
				}
			}
			mode := node.Mode
			if mode == 0 {
				mode = 0644
			}
			tree = append(tree, &data.Node{
				Type:              data.NodeTypeFile,
				Mode:              mode,
				ModTime:           node.ModTime,
				Name:              name,
				UID:               uint32(os.Getuid()),
				GID:               uint32(os.Getgid()),
				Content:           fc,
				Size:              uint64(size),
				Inode:             fi,
				Links:             lc,
				GenericAttributes: getGenericAttributes(node.attributes, false),
			})
		case Symlink:
			tree = append(tree, &data.Node{
				Type:       data.NodeTypeSymlink,
				Mode:       os.ModeSymlink | 0o777,
				ModTime:    node.ModTime,
				Name:       name,
				UID:        uint32(os.Getuid()),
				GID:        uint32(os.Getgid()),
				LinkTarget: node.Target,
				Inode:      inode,
				Links:      1,
			})
		case Dir:
			id := saveDir(t, repo, node.Nodes, inode, getGenericAttributes)

			mode := node.Mode
			if mode == 0 {
				mode = 0755
			}

			tree = append(tree, &data.Node{
				Type:              data.NodeTypeDir,
				Mode:              mode,
				ModTime:           node.ModTime,
				Name:              name,
				UID:               uint32(os.Getuid()),
				GID:               uint32(os.Getgid()),
				Subtree:           &id,
				GenericAttributes: getGenericAttributes(node.attributes, false),
			})
		default:
			t.Fatalf("unknown node type %T", node)
		}
	}

	return data.TestSaveNodes(t, ctx, repo, tree)
}

func saveSnapshot(
	t testing.TB,
	repo vaultic.Repository,
	snapshot Snapshot,
	getGenericAttributes func(attr *FileAttributes, isDir bool) (genericAttributes map[data.GenericAttributeType]json.RawMessage),
) (*data.Snapshot, vaultic.ID) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var treeID vaultic.ID
	rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		treeID = saveDir(t, uploader, snapshot.Nodes, 1000, getGenericAttributes)
		return nil
	}))

	sn, err := data.NewSnapshot([]string{"test"}, nil, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	sn.Tree = &treeID
	id, err := data.SaveSnapshot(ctx, repo, sn)
	if err != nil {
		t.Fatal(err)
	}

	return sn, id
}

var noopGetGenericAttributes = func(attr *FileAttributes, isDir bool) (genericAttributes map[data.GenericAttributeType]json.RawMessage) {
	// No-op
	return nil
}

func TestRestorer(t *testing.T) {
	var tests = []struct {
		Snapshot
		Files      map[string]string
		ErrorsMust map[string]map[string]struct{}
		ErrorsMay  map[string]map[string]struct{}
		Select     func(item string, isDir bool) (selectForRestore bool, childMayBeSelected bool)
	}{
		// valid test cases
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"foo": File{Data: "content: foo\n"},
					"dirtest": Dir{
						Nodes: map[string]Node{
							"file": File{Data: "content: file\n"},
						},
					},
				},
			},
			Files: map[string]string{
				"foo":          "content: foo\n",
				"dirtest/file": "content: file\n",
			},
		},
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"top": File{Data: "toplevel file"},
					"dir": Dir{
						Nodes: map[string]Node{
							"file": File{Data: "file in dir"},
							"subdir": Dir{
								Nodes: map[string]Node{
									"file": File{Data: "file in subdir"},
								},
							},
						},
					},
				},
			},
			Files: map[string]string{
				"top":             "toplevel file",
				"dir/file":        "file in dir",
				"dir/subdir/file": "file in subdir",
			},
		},
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"dir": Dir{
						Mode: 0444,
					},
					"file": File{Data: "top-level file"},
				},
			},
			Files: map[string]string{
				"file": "top-level file",
			},
		},
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"dir": Dir{
						Mode: 0555,
						Nodes: map[string]Node{
							"file": File{Data: "file in dir"},
						},
					},
				},
			},
			Files: map[string]string{
				"dir/file": "file in dir",
			},
		},
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"topfile": File{Data: "top-level file"},
				},
			},
			Files: map[string]string{
				"topfile": "top-level file",
			},
		},
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"dir": Dir{
						Nodes: map[string]Node{
							"file": File{Data: "content: file\n"},
						},
					},
				},
			},
			Files: map[string]string{
				"dir/file": "content: file\n",
			},
			Select: func(item string, isDir bool) (selectedForRestore bool, childMayBeSelected bool) {
				switch item {
				case filepath.FromSlash("/dir"):
					childMayBeSelected = true
				case filepath.FromSlash("/dir/file"):
					selectedForRestore = true
					childMayBeSelected = true
				}

				return selectedForRestore, childMayBeSelected
			},
		},

		// test cases with invalid/constructed names
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					`..\test`:                      File{Data: "foo\n"},
					`..\..\foo\..\bar\..\xx\test2`: File{Data: "test2\n"},
				},
			},
			ErrorsMay: map[string]map[string]struct{}{
				`/`: {
					`invalid child node name ..\test`:                      struct{}{},
					`invalid child node name ..\..\foo\..\bar\..\xx\test2`: struct{}{},
				},
			},
		},
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					`../test`:                      File{Data: "foo\n"},
					`../../foo/../bar/../xx/test2`: File{Data: "test2\n"},
				},
			},
			ErrorsMay: map[string]map[string]struct{}{
				`/`: {
					`invalid child node name ../test`:                      struct{}{},
					`invalid child node name ../../foo/../bar/../xx/test2`: struct{}{},
				},
			},
		},
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"top": File{Data: "toplevel file"},
					"x": Dir{
						Nodes: map[string]Node{
							"file1": File{Data: "file1"},
							"..": Dir{
								Nodes: map[string]Node{
									"file2": File{Data: "file2"},
									"..": Dir{
										Nodes: map[string]Node{
											"file2": File{Data: "file2"},
										},
									},
								},
							},
						},
					},
				},
			},
			Files: map[string]string{
				"top": "toplevel file",
			},
			ErrorsMust: map[string]map[string]struct{}{
				`/x`: {
					`invalid child node name ..`: struct{}{},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			repo := repository.TestRepository(t)
			sn, id := saveSnapshot(t, repo, test.Snapshot, noopGetGenericAttributes)
			t.Logf("snapshot saved as %v", id.Str())

			res := NewRestorer(repo, sn, Options{})

			tempdir := rtest.TempDir(t)
			// make sure we're creating a new subdir of the tempdir
			tempdir = filepath.Join(tempdir, "target")

			res.SelectFilter = func(item string, isDir bool) (selectedForRestore bool, childMayBeSelected bool) {
				t.Logf("restore %v", item)
				if test.Select != nil {
					return test.Select(item, isDir)
				}

				return true, true
			}

			errors := make(map[string]map[string]struct{})
			res.Error = func(location string, err error) error {
				location = filepath.ToSlash(location)
				t.Logf("restore returned error for %q: %v", location, err)
				if errors[location] == nil {
					errors[location] = make(map[string]struct{})
				}
				errors[location][err.Error()] = struct{}{}
				return nil
			}

			ctx := t.Context()

			countRestoredFiles, err := res.RestoreTo(ctx, tempdir)
			if err != nil {
				t.Fatal(err)
			}

			if len(test.ErrorsMust)+len(test.ErrorsMay) == 0 {
				_, err = res.VerifyFiles(ctx, tempdir, countRestoredFiles, vaultic.NoopCounter)
				rtest.OK(t, err)
			}

			for location, expectedErrors := range test.ErrorsMust {
				actualErrors, ok := errors[location]
				if !ok {
					t.Errorf("expected error(s) for %v, found none", location)
					continue
				}

				rtest.Equals(t, expectedErrors, actualErrors)

				delete(errors, location)
			}

			for location, expectedErrors := range test.ErrorsMay {
				actualErrors, ok := errors[location]
				if !ok {
					continue
				}

				rtest.Equals(t, expectedErrors, actualErrors)

				delete(errors, location)
			}

			for filename, err := range errors {
				t.Errorf("unexpected error for %v found: %v", filename, err)
			}

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
		})
	}
}

func TestRestorerRelative(t *testing.T) {
	var tests = []struct {
		Snapshot
		Files map[string]string
	}{
		{
			Snapshot: Snapshot{
				Nodes: map[string]Node{
					"foo": File{Data: "content: foo\n"},
					"dirtest": Dir{
						Nodes: map[string]Node{
							"file": File{Data: "content: file\n"},
						},
					},
				},
			},
			Files: map[string]string{
				"foo":          "content: foo\n",
				"dirtest/file": "content: file\n",
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			repo := repository.TestRepository(t)

			sn, id := saveSnapshot(t, repo, test.Snapshot, noopGetGenericAttributes)
			t.Logf("snapshot saved as %v", id.Str())

			res := NewRestorer(repo, sn, Options{})

			tempdir := rtest.TempDir(t)
			cleanup := rtest.Chdir(t, tempdir)
			defer cleanup()

			errors := make(map[string]string)
			res.Error = func(location string, err error) error {
				t.Logf("restore returned error for %q: %v", location, err)
				errors[location] = err.Error()
				return nil
			}

			ctx := t.Context()

			countRestoredFiles, err := res.RestoreTo(ctx, "restore")
			if err != nil {
				t.Fatal(err)
			}
			p := progress.NewCounter(time.Second, countRestoredFiles, func(value uint64, total uint64, runtime time.Duration, final bool) {})
			defer p.Done()
			nverified, err := res.VerifyFiles(ctx, "restore", countRestoredFiles, p)
			rtest.OK(t, err)
			rtest.Equals(t, len(test.Files), nverified)
			counterValue, maxValue := p.Get()
			rtest.Equals(t, counterValue, uint64(2))
			rtest.Equals(t, maxValue, uint64(2))

			for filename, err := range errors {
				t.Errorf("unexpected error for %v found: %v", filename, err)
			}

			for filename, content := range test.Files {
				data, err := os.ReadFile(filepath.Join(tempdir, "restore", filepath.FromSlash(filename)))
				if err != nil {
					t.Errorf("unable to read file %v: %v", filename, err)
					continue
				}

				if !bytes.Equal(data, []byte(content)) {
					t.Errorf("file %v has wrong content: want %q, got %q", filename, content, data)
				}
			}

			// verify that restoring the same snapshot again results in countRestoredFiles == 0
			countRestoredFiles, err = res.RestoreTo(ctx, "restore")
			if err != nil {
				t.Fatal(err)
			}
			rtest.Equals(t, uint64(0), countRestoredFiles)
		})
	}
}
