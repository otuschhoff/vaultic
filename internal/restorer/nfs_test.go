package restorer

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	billy "github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/repository"
	gonfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs/helpers"
)

type nfsTestFilesystem struct {
	billy.Filesystem
}

func (filesystem nfsTestFilesystem) Chmod(name string, mode os.FileMode) error {
	return os.Chmod(filepath.Join(filesystem.Root(), name), mode)
}

func (filesystem nfsTestFilesystem) Chown(name string, uid, gid int) error {
	return os.Chown(filepath.Join(filesystem.Root(), name), uid, gid)
}

func (filesystem nfsTestFilesystem) Lchown(name string, uid, gid int) error {
	return os.Lchown(filepath.Join(filesystem.Root(), name), uid, gid)
}

func (filesystem nfsTestFilesystem) Chtimes(name string, access, modified time.Time) error {
	return os.Chtimes(filepath.Join(filesystem.Root(), name), access, modified)
}

func testNFSDestination(t *testing.T) (*fs.NFSRestore, string) {
	t.Helper()
	root := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	filesystem := nfsTestFilesystem{Filesystem: osfs.New(root)}
	server := &gonfs.Server{Handler: helpers.NewCachingHandler(helpers.NewNullAuthHandler(filesystem), 1024), Context: ctx}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { cancel(); _ = listener.Close(); <-done })
	host, portText, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	auth := rpc.AuthNull
	destination, err := fs.NewNFSRestore(ctx, "nfs://"+host+":/", fs.NFSOptions{
		Auth: &auth, AllowMissingMetadata: true, Connections: 1, MountPort: port, NFSPort: port,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = destination.Close() })
	return destination, root
}

func TestRestoreToNFS(t *testing.T) {
	destination, root := testNFSDestination(t)
	repo := repository.TestRepository(t)
	modified := time.Unix(1700000000, 0)
	snapshot, _ := saveSnapshot(t, repo, Snapshot{Nodes: map[string]Node{
		"directory": Dir{Mode: 0750, ModTime: modified, Nodes: map[string]Node{
			"file # %": File{DataParts: []string{"first chunk", "second chunk"}, Mode: 0640, ModTime: modified},
			"empty":    File{},
		}},
		"symlink": Symlink{Target: "directory/file # %"},
	}}, noopGetGenericAttributes)
	res := NewRestorer(repo, snapshot, Options{})
	count, err := res.RestoreToNFS(context.Background(), destination, true)
	if err != nil || count != 2 {
		t.Fatalf("restore = %d, %v", count, err)
	}
	content, err := os.ReadFile(filepath.Join(root, "directory", "file # %"))
	if err != nil || string(content) != "first chunksecond chunk" {
		t.Fatalf("content = %q, %v", content, err)
	}
	info, err := os.Stat(filepath.Join(root, "directory", "file # %"))
	if err != nil || info.Mode().Perm() != 0640 || !info.ModTime().Equal(modified) {
		t.Fatalf("metadata = %+v, %v", info, err)
	}
	if link, err := os.Readlink(filepath.Join(root, "symlink")); err != nil || link != "directory/file # %" {
		t.Fatalf("symlink = %q, %v", link, err)
	}
}

func TestRestoreToNFSPolicies(t *testing.T) {
	for _, test := range []struct {
		name                  string
		options               Options
		modified              time.Time
		verify, fail, replace bool
	}{
		{name: "dry-run", options: Options{DryRun: true}},
		{name: "never", options: Options{Overwrite: OverwriteNever}},
		{name: "older", options: Options{Overwrite: OverwriteIfNewer}, modified: time.Unix(100, 0)},
		{name: "newer", options: Options{Overwrite: OverwriteIfNewer}, modified: time.Unix(300, 0), replace: true},
		{name: "unchanged", options: Options{Overwrite: OverwriteIfChanged}, modified: time.Unix(200, 0)},
		{name: "corrupt-unchanged", options: Options{Overwrite: OverwriteIfChanged}, modified: time.Unix(200, 0), verify: true, fail: true},
		{name: "metadata-failure", modified: time.Unix(-1, 0), fail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			destination, root := testNFSDestination(t)
			name := filepath.Join(root, "existing")
			if err := os.WriteFile(name, []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(name, time.Unix(200, 0), time.Unix(200, 0)); err != nil {
				t.Fatal(err)
			}
			repo := repository.TestRepository(t)
			snapshot, _ := saveSnapshot(t, repo, Snapshot{Nodes: map[string]Node{
				"existing": File{Data: "new", ModTime: test.modified},
			}}, noopGetGenericAttributes)
			res := NewRestorer(repo, snapshot, test.options)
			_, err := res.RestoreToNFS(context.Background(), destination, test.verify)
			if (err != nil) != test.fail {
				t.Fatalf("restore error = %v", err)
			}
			want := "old"
			if test.replace {
				want = "new"
			}
			content, err := os.ReadFile(name)
			if err != nil || string(content) != want {
				t.Fatalf("destination = %q, %v; want %q", content, err, want)
			}
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != 1 || entries[0].Name() != "existing" {
				t.Fatalf("left unexpected temporary files: %v, %v", entries, err)
			}
		})
	}
}

func TestRestoreToNFSDryRunAndFilters(t *testing.T) {
	destination, root := testNFSDestination(t)
	repo := repository.TestRepository(t)
	snapshot, _ := saveSnapshot(t, repo, Snapshot{Nodes: map[string]Node{
		"directory": Dir{Nodes: map[string]Node{"selected": File{Data: "yes"}, "excluded": File{Data: "no"}}},
	}}, noopGetGenericAttributes)
	res := NewRestorer(repo, snapshot, Options{DryRun: true})
	if _, err := res.RestoreToNFS(context.Background(), destination, false); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(root); err != nil || len(entries) != 0 {
		t.Fatalf("dry run wrote files: %v, %v", entries, err)
	}
	res = NewRestorer(repo, snapshot, Options{})
	res.SelectFilter = func(name string, directory bool) (bool, bool) { return strings.HasSuffix(name, "/selected"), directory }
	if _, err := res.RestoreToNFS(context.Background(), destination, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "directory", "excluded")); !os.IsNotExist(err) {
		t.Fatalf("restored excluded file: %v", err)
	}
	if content, err := os.ReadFile(filepath.Join(root, "directory", "selected")); err != nil || string(content) != "yes" {
		t.Fatalf("selected content = %q, %v", content, err)
	}
}

func TestRestoreToNFSRejectsSymlinkParent(t *testing.T) {
	destination, root := testNFSDestination(t)
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "directory")); err != nil {
		t.Fatal(err)
	}
	repo := repository.TestRepository(t)
	snapshot, _ := saveSnapshot(t, repo, Snapshot{Nodes: map[string]Node{
		"directory": Dir{Nodes: map[string]Node{"escape": File{Data: "must not write"}}},
	}}, noopGetGenericAttributes)
	res := NewRestorer(repo, snapshot, Options{})
	if _, err := res.RestoreToNFS(context.Background(), destination, false); err == nil {
		t.Fatal("followed destination symlink parent")
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("wrote outside destination: %v, %v", entries, err)
	}
}
