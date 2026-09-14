//go:build darwin || freebsd || linux

package fuse

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/snapshotfs"
	"github.com/otuschhoff/vaultic/internal/vaultic"

	"github.com/anacrolix/fuse"
	"github.com/anacrolix/fuse/fs"

	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func testRead(t testing.TB, f fs.Handle, offset, length int, data []byte) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &fuse.ReadRequest{
		Offset: int64(offset),
		Size:   length,
	}
	resp := &fuse.ReadResponse{
		Data: data,
	}
	fr := f.(fs.HandleReader)
	rtest.OK(t, fr.Read(ctx, req, resp))
}

func firstSnapshotID(t testing.TB, repo vaultic.Lister) (first vaultic.ID) {
	err := repo.List(context.TODO(), vaultic.SnapshotFile, func(id vaultic.ID, size int64) error {
		if first.IsNull() {
			first = id
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	return first
}

func loadFirstSnapshot(t testing.TB, repo vaultic.ListerLoaderUnpacked) *data.Snapshot {
	id := firstSnapshotID(t, repo)
	sn, err := data.LoadSnapshot(context.TODO(), repo, id)
	rtest.OK(t, err)
	return sn
}

func loadTree(t testing.TB, repo vaultic.Loader, id vaultic.ID) data.TreeNodeIterator {
	tree, err := data.LoadTree(context.TODO(), repo, id)
	rtest.OK(t, err)
	return tree
}

func snapshotNodeForTest(t *testing.T, repo vaultic.Repository, raw *data.Node) *snapshotfs.Node {
	t.Helper()
	ctx := t.Context()
	node := *raw
	var treeID vaultic.ID
	if node.Name == "" {
		node.Name = "node"
	}
	err := repo.WithBlobUploader(ctx, func(_ context.Context, uploader vaultic.BlobSaverWithAsync) error {
		if node.Type == data.NodeTypeDir && node.Subtree == nil {
			treeID := data.TestSaveNodes(t, ctx, uploader, nil)
			node.Subtree = &treeID
		}
		treeID = data.TestSaveNodes(t, ctx, uploader, []*data.Node{&node})
		return nil
	})
	rtest.OK(t, err)
	snapshot := &data.Snapshot{Tree: &treeID, Time: time.Unix(1700000000, 0)}
	data.TestSetSnapshotID(t, snapshot, vaultic.Hash([]byte(t.Name()+node.Name)))
	filesystem, err := snapshotfs.New(ctx, repo, snapshot, "", snapshotfs.Config{})
	rtest.OK(t, err)
	t.Cleanup(func() { _ = filesystem.Close() })
	root, err := filesystem.Root()
	rtest.OK(t, err)
	result, err := root.Lookup(ctx, node.Name)
	rtest.OK(t, err)
	return result
}

func TestFuseFile(t *testing.T) {
	repo := repository.TestRepository(t)

	ctx := t.Context()

	timestamp, err := time.Parse(time.RFC3339, "2017-01-24T10:42:56+01:00")
	rtest.OK(t, err)
	data.TestCreateSnapshot(t, repo, timestamp, 2)

	sn := loadFirstSnapshot(t, repo)
	tree := loadTree(t, repo, *sn.Tree)

	var content vaultic.IDs
	for item := range tree {
		rtest.OK(t, item.Error)
		content = append(content, item.Node.Content...)
	}
	t.Logf("tree loaded, content: %v", content)

	var (
		filesize uint64
		memfile  []byte
	)
	for _, id := range content {
		size, found := repo.LookupBlobSize(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id})
		rtest.Assert(t, found, "Expected to find blob id %v", id)
		filesize += uint64(size)

		buf, err := repo.LoadBlob(context.TODO(), vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}, nil)
		rtest.OK(t, err)

		if len(buf) != int(size) {
			t.Fatalf("not enough bytes read for id %v: want %v, got %v", id.Str(), size, len(buf))
		}

		if uint(len(buf)) != size {
			t.Fatalf("buffer has wrong length for id %v: want %v, got %v", id.Str(), size, len(buf))
		}

		memfile = append(memfile, buf...)
	}

	t.Logf("filesize is %v, memfile has size %v", filesize, len(memfile))

	node := &data.Node{
		Name:    "foo",
		Type:    data.NodeTypeFile,
		Inode:   23,
		Mode:    0742,
		Size:    filesize,
		Content: content,
	}
	snapshotNode := snapshotNodeForTest(t, repo, node)
	f, err := newFile(func() {}, snapshotNode, nil)
	rtest.OK(t, err)
	of, err := f.Open(context.TODO(), nil, nil)
	rtest.OK(t, err)

	attr := fuse.Attr{}
	rtest.OK(t, f.Attr(ctx, &attr))

	rtest.Equals(t, snapshotNode.Identity(), attr.Inode)
	rtest.Equals(t, node.Mode, attr.Mode)
	rtest.Equals(t, node.Size, attr.Size)
	rtest.Equals(t, (node.Size/uint64(attr.BlockSize))+1, attr.Blocks)

	for i := range 200 {
		offset := rand.Intn(int(filesize))
		length := rand.Intn(int(filesize)-offset) + 100

		b := memfile[offset : offset+length]

		buf := make([]byte, length)

		testRead(t, of, offset, length, buf)
		if !bytes.Equal(b, buf) {
			t.Errorf("test %d failed, wrong data returned (offset %v, length %v)", i, offset, length)
		}
	}
}

func TestFuseDir(t *testing.T) {
	repo := repository.TestRepository(t)

	node := &data.Node{
		Name:       "foo",
		Type:       data.NodeTypeDir,
		Mode:       0755,
		UID:        42,
		GID:        43,
		AccessTime: time.Unix(1606773731, 0),
		ChangeTime: time.Unix(1606773732, 0),
		ModTime:    time.Unix(1606773733, 0),
	}
	parentInode := inodeFromName(0, "parent")
	snapshotNode := snapshotNodeForTest(t, repo, node)
	d, err := newDir(func() {}, parentInode, snapshotNode, nil)
	rtest.OK(t, err)

	// don't open the directory as that would require setting up a proper tree blob
	attr := fuse.Attr{}
	rtest.OK(t, d.Attr(context.TODO(), &attr))

	rtest.Equals(t, snapshotNode.Identity(), attr.Inode)
	rtest.Equals(t, node.UID, attr.Uid)
	rtest.Equals(t, node.GID, attr.Gid)
	for _, times := range []struct {
		name     string
		expected time.Time
		actual   time.Time
	}{
		{name: "access", expected: node.AccessTime, actual: attr.Atime},
		{name: "change", expected: node.ChangeTime, actual: attr.Ctime},
		{name: "modification", expected: node.ModTime, actual: attr.Mtime},
	} {
		if !times.expected.Equal(times.actual) {
			t.Errorf("%s time: expected %v, got %v", times.name, times.expected, times.actual)
		}
	}
}

// Test top-level directories for their UID and GID.
func TestTopUIDGID(t *testing.T) {
	repo := repository.TestRepository(t)
	data.TestCreateSnapshot(t, repo, time.Unix(1460289341, 207401672), 0)

	testTopUIDGID(t, Config{}, repo, uint32(os.Getuid()), uint32(os.Getgid()))
	testTopUIDGID(t, Config{OwnerIsRoot: true}, repo, 0, 0)
}

func testTopUIDGID(t *testing.T, cfg Config, repo vaultic.Repository, uid, gid uint32) {
	t.Helper()

	ctx := context.Background()
	root := NewRoot(repo, cfg)

	var attr fuse.Attr
	err := root.Attr(ctx, &attr)
	rtest.OK(t, err)
	rtest.Equals(t, uid, attr.Uid)
	rtest.Equals(t, gid, attr.Gid)

	idsdir, err := root.Lookup(ctx, "ids")
	rtest.OK(t, err)

	err = idsdir.Attr(ctx, &attr)
	rtest.OK(t, err)
	rtest.Equals(t, uid, attr.Uid)
	rtest.Equals(t, gid, attr.Gid)

	snapID := loadFirstSnapshot(t, repo).ID().Str()
	snapshotdir, err := idsdir.(fs.NodeStringLookuper).Lookup(ctx, snapID)
	rtest.OK(t, err)

	// data.TestCreateSnapshot does not set the UID/GID thus it must be always zero
	err = snapshotdir.Attr(ctx, &attr)
	rtest.OK(t, err)
	rtest.Equals(t, uint32(0), attr.Uid)
	rtest.Equals(t, uint32(0), attr.Gid)
}

// The Lookup method must return the same Node object unless it was forgotten in the meantime
func testStableLookup(t *testing.T, node fs.Node, path string) fs.Node {
	t.Helper()
	result, err := node.(fs.NodeStringLookuper).Lookup(context.TODO(), path)
	rtest.OK(t, err)
	result2, err := node.(fs.NodeStringLookuper).Lookup(context.TODO(), path)
	rtest.OK(t, err)
	rtest.Assert(t, result == result2, "%v are not the same object", path)

	result2.(fs.NodeForgetter).Forget()
	result2, err = node.(fs.NodeStringLookuper).Lookup(context.TODO(), path)
	rtest.OK(t, err)
	rtest.Assert(t, result != result2, "object for %v should change after forget", path)
	return result
}

func TestStableNodeObjects(t *testing.T) {
	repo := repository.TestRepository(t)
	data.TestCreateSnapshot(t, repo, time.Unix(1460289341, 207401672), 2)
	root := NewRoot(repo, Config{})

	idsdir := testStableLookup(t, root, "ids")
	snapID := loadFirstSnapshot(t, repo).ID().Str()
	snapshotdir, err := idsdir.(fs.NodeStringLookuper).Lookup(context.TODO(), snapID)
	rtest.OK(t, err)
	snapshotdirAgain, err := idsdir.(fs.NodeStringLookuper).Lookup(context.TODO(), snapID)
	rtest.OK(t, err)
	rtest.Assert(t, snapshotdir == snapshotdirAgain, "snapshot object should be stable")
	dir := testStableLookup(t, snapshotdir, "dir-0")
	testStableLookup(t, dir, "file-2")
}

func TestSnapshotsDirLatestSymlinkUpdatesAfterReload(t *testing.T) {
	repo := repository.TestRepository(t)
	timeTemplate := "2006-01-02T15:04:05"

	firstTime, err := time.Parse(time.RFC3339, "2017-01-24T10:42:56Z")
	rtest.OK(t, err)
	secondTime := firstTime.Add(time.Hour)

	data.TestCreateSnapshot(t, repo, firstTime, 0)
	root := NewRoot(repo, Config{
		TimeTemplate:  timeTemplate,
		PathTemplates: []string{"snapshots/%T"},
	})

	snapshotsDir := testStableLookup(t, root, "snapshots")
	rtest.Equals(t, firstTime.Format(timeTemplate), readLatestTarget(t, snapshotsDir))

	data.TestCreateSnapshot(t, repo, secondTime, 0)
	root.SnapshotsDir.dirStruct.lastCheck = time.Now().Add(-2 * minSnapshotsReloadTime)

	rtest.Equals(t, secondTime.Format(timeTemplate), readLatestTarget(t, snapshotsDir))
}

func readLatestTarget(t testing.TB, node fs.Node) string {
	t.Helper()

	latest, err := node.(fs.NodeStringLookuper).Lookup(context.TODO(), "latest")
	rtest.OK(t, err)
	target, err := latest.(fs.NodeReadlinker).Readlink(context.TODO(), nil)
	rtest.OK(t, err)
	return target
}

// Test reporting of fuse.Attr.Blocks in multiples of 512.
func TestBlocks(t *testing.T) {
	repo := repository.TestRepository(t)
	root := &Root{}

	for _, c := range []struct {
		size, blocks uint64
	}{
		{0, 0},
		{1, 1},
		{511, 1},
		{512, 1},
		{513, 2},
		{1024, 2},
		{1025, 3},
		{41253, 81},
	} {
		target := strings.Repeat("x", int(c.size))

		for _, n := range []fs.Node{
			&file{node: snapshotNodeForTest(t, repo, &data.Node{Name: fmt.Sprintf("file-%d", c.size), Type: data.NodeTypeFile, Size: uint64(c.size)})},
			&link{node: snapshotNodeForTest(t, repo, &data.Node{Name: fmt.Sprintf("link-%d", c.size), Type: data.NodeTypeSymlink, LinkTarget: target})},
			&snapshotLink{root: root, snapshot: &data.Snapshot{}, target: target},
		} {
			var a fuse.Attr
			err := n.Attr(context.TODO(), &a)
			rtest.OK(t, err)
			rtest.Equals(t, c.blocks, a.Blocks)
		}
	}
}

// Windows (and other non-POSIX) backups may store a link count of 0; FUSE
// must still report a positive nlink so tools that validate stat() (e.g.
// Samba) accept the file.
func TestFileAttrNlink(t *testing.T) {
	repo := repository.TestRepository(t)
	for _, tc := range []struct {
		links uint64
		want  uint32
	}{
		{0, 1},
		{1, 1},
		{42, 42},
	} {
		t.Run(fmt.Sprintf("links_%d", tc.links), func(t *testing.T) {
			f := &file{node: snapshotNodeForTest(t, repo, &data.Node{Name: "file", Type: data.NodeTypeFile, Links: tc.links})}
			var a fuse.Attr
			rtest.OK(t, f.Attr(context.TODO(), &a))
			rtest.Equals(t, tc.want, a.Nlink)
		})
	}
}

func TestLink(t *testing.T) {
	node := &data.Node{Name: "foo.txt", Type: data.NodeTypeSymlink, Links: 1, LinkTarget: "dst", ExtendedAttributes: []data.ExtendedAttribute{
		{Name: "foo", Value: []byte("bar")},
	}}

	repo := repository.TestRepository(t)
	snapshotNode := snapshotNodeForTest(t, repo, node)
	lnk, err := newLink(func() {}, snapshotNode, nil)
	rtest.OK(t, err)
	target, err := lnk.Readlink(context.TODO(), nil)
	rtest.OK(t, err)
	rtest.Equals(t, node.LinkTarget, target)

	exp := &fuse.ListxattrResponse{}
	exp.Append("foo")
	resp := &fuse.ListxattrResponse{}
	rtest.OK(t, lnk.Listxattr(context.TODO(), &fuse.ListxattrRequest{}, resp))
	rtest.Equals(t, exp.Xattr, resp.Xattr)

	getResp := &fuse.GetxattrResponse{}
	rtest.OK(t, lnk.Getxattr(context.TODO(), &fuse.GetxattrRequest{Name: "foo"}, getResp))
	rtest.Equals(t, node.ExtendedAttributes[0].Value, getResp.Xattr)

	err = lnk.Getxattr(context.TODO(), &fuse.GetxattrRequest{Name: "invalid"}, nil)
	rtest.Assert(t, err != nil, "missing error on reading invalid xattr")
}
