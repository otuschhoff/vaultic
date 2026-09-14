//go:build darwin || freebsd || linux

package fuse

import (
	"context"

	"github.com/anacrolix/fuse"
	"github.com/anacrolix/fuse/fs"
	"github.com/otuschhoff/vaultic/internal/snapshotfs"
)

// Statically ensure that *link implements the given interface
var _ = fs.NodeForgetter(&link{})
var _ = fs.NodeGetxattrer(&link{})
var _ = fs.NodeListxattrer(&link{})
var _ = fs.NodeReadlinker(&link{})

type link struct {
	forget    forgetFn
	node      *snapshotfs.Node
	holder    *snapshotFSHolder
	reference snapshotFSReference
}

func newLink(forget forgetFn, node *snapshotfs.Node, holder *snapshotFSHolder) (*link, error) {
	return &link{forget: forget, node: node, holder: holder, reference: retainSnapshotFS(holder)}, nil
}

func (l *link) Readlink(ctx context.Context, _ *fuse.ReadlinkRequest) (string, error) {
	target, err := l.node.Readlink(ctx)
	return target, snapshotError(err)
}

func (l *link) Attr(ctx context.Context, a *fuse.Attr) error {
	return snapshotAttr(ctx, l.node, a)
}

func (l *link) Listxattr(_ context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	raw := l.node.RawNode()
	nodeToXattrList(&raw, req, resp)
	return nil
}

func (l *link) Getxattr(_ context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	raw := l.node.RawNode()
	return nodeGetXattr(&raw, req, resp)
}

func (l *link) Forget() {
	l.forget()
	l.reference.release()
}
