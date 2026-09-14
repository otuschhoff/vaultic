//go:build darwin || freebsd || linux

package fuse

import (
	"context"

	"github.com/anacrolix/fuse"
	"github.com/anacrolix/fuse/fs"
	"github.com/otuschhoff/vaultic/internal/snapshotfs"
)

// Statically ensure that *other implements the given interface
var _ = fs.NodeForgetter(&other{})
var _ = fs.NodeReadlinker(&other{})

type other struct {
	forget    forgetFn
	node      *snapshotfs.Node
	holder    *snapshotFSHolder
	reference snapshotFSReference
}

func newOther(forget forgetFn, node *snapshotfs.Node, holder *snapshotFSHolder) (*other, error) {
	return &other{forget: forget, node: node, holder: holder, reference: retainSnapshotFS(holder)}, nil
}

func (l *other) Readlink(_ context.Context, _ *fuse.ReadlinkRequest) (string, error) {
	return l.node.RawNode().LinkTarget, nil
}

func (l *other) Attr(ctx context.Context, a *fuse.Attr) error {
	return snapshotAttr(ctx, l.node, a)
}

func (l *other) Forget() {
	l.forget()
	l.reference.release()
}
