//go:build darwin || freebsd || linux

package fuse

import (
	"context"
	"errors"
	"syscall"

	"github.com/anacrolix/fuse"
	"github.com/anacrolix/fuse/fs"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/snapshotfs"
	"github.com/otuschhoff/vaultic/internal/telemetry"
)

// Statically ensure that *dir implement those interface
var _ = fs.HandleReadDirAller(&dir{})
var _ = fs.NodeForgetter(&dir{})
var _ = fs.NodeGetxattrer(&dir{})
var _ = fs.NodeListxattrer(&dir{})
var _ = fs.NodeStringLookuper(&dir{})

type dir struct {
	forget       forgetFn
	parentInode  uint64
	node         *snapshotfs.Node
	holder       *snapshotFSHolder
	reference    snapshotFSReference
	snapshotRoot bool
	cache        treeCache
}

func newDir(forget forgetFn, parentInode uint64, node *snapshotfs.Node, holder *snapshotFSHolder) (*dir, error) {
	debug.Log("new dir for %v", node.Path())
	return &dir{
		forget:      forget,
		node:        node,
		parentInode: parentInode,
		holder:      holder,
		reference:   retainSnapshotFS(holder),
		cache:       *newTreeCache(),
	}, nil
}

// returning a wrapped context.Canceled error will instead result in returning
// an input / output error to the user. Thus unwrap the error to match the
// expectations of bazil/fuse
func unwrapCtxCanceled(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	return err
}

func newDirFromSnapshot(ctx context.Context, root *Root, forget forgetFn, parentInode uint64, snapshot *data.Snapshot) (*dir, error) {
	debug.Log("new dir for snapshot %v (%v)", snapshot.ID(), snapshot.Tree)
	ctx = telemetry.InheritOperation(ctx, root.ctx)
	owner := snapshotfs.OwnerPreserved
	if root.cfg.OwnerIsRoot {
		owner = snapshotfs.OwnerRoot
	}
	filesystem, err := snapshotfs.New(ctx, root.repo, snapshot, "", snapshotfs.Config{
		Owner:       owner,
		Permissions: snapshotfs.PermissionsPreserved,
	})
	if err != nil {
		return nil, snapshotError(err)
	}
	node, err := filesystem.Root()
	if err != nil {
		_ = filesystem.Close() // Preserve the root error; cache cleanup is best effort.
		return nil, snapshotError(err)
	}
	holder := &snapshotFSHolder{fs: filesystem}
	directory, err := newDir(forget, parentInode, node, holder)
	if err != nil {
		_ = filesystem.Close() // Preserve the adapter error; cache cleanup is best effort.
		return nil, err
	}
	directory.snapshotRoot = true
	return directory, nil
}

func (d *dir) Attr(ctx context.Context, a *fuse.Attr) error {
	debug.Log("Attr()")
	if err := snapshotAttr(ctx, d.node, a); err != nil {
		return err
	}
	if d.snapshotRoot {
		a.Uid = 0
		a.Gid = 0
	}
	return nil
}

func (d *dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	debug.Log("ReadDirAll()")
	entries, err := d.node.ReadDir(ctx)
	if err != nil {
		return nil, snapshotError(err)
	}
	ret := make([]fuse.Dirent, 0, len(entries)+2)

	ret = append(ret, fuse.Dirent{
		Inode: d.node.Identity(),
		Name:  ".",
		Type:  fuse.DT_Dir,
	})

	ret = append(ret, fuse.Dirent{
		Inode: d.parentInode,
		Name:  "..",
		Type:  fuse.DT_Dir,
	})

	for _, entry := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var typ fuse.DirentType
		switch entry.Node.Type() {
		case data.NodeTypeDir:
			typ = fuse.DT_Dir
		case data.NodeTypeFile:
			typ = fuse.DT_File
		case data.NodeTypeSymlink:
			typ = fuse.DT_Link
		}

		ret = append(ret, fuse.Dirent{
			Inode: entry.Node.Identity(),
			Type:  typ,
			Name:  entry.Name,
		})
	}

	return ret, nil
}

func (d *dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	debug.Log("Lookup(%v)", name)

	return d.cache.lookupOrCreate(name, -1, func(forget forgetFn) (fs.Node, error) {
		node, err := d.node.Lookup(ctx, name)
		if err != nil {
			debug.Log("  Lookup(%v) -> not found", name)
			return nil, snapshotError(err)
		}
		switch node.Type() {
		case data.NodeTypeDir:
			return newDir(forget, d.node.Identity(), node, d.holder)
		case data.NodeTypeFile:
			return newFile(forget, node, d.holder)
		case data.NodeTypeSymlink:
			return newLink(forget, node, d.holder)
		case data.NodeTypeDev, data.NodeTypeCharDev, data.NodeTypeFifo, data.NodeTypeSocket:
			return newOther(forget, node, d.holder)
		default:
			debug.Log("  node %v has unknown type %v", name, node.Type)
			return nil, syscall.ENOENT
		}
	})
}

func (d *dir) Listxattr(_ context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	raw := d.node.RawNode()
	nodeToXattrList(&raw, req, resp)
	return nil
}

func (d *dir) Getxattr(_ context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	raw := d.node.RawNode()
	return nodeGetXattr(&raw, req, resp)
}

func (d *dir) Forget() {
	d.forget()
	d.reference.release()
}
