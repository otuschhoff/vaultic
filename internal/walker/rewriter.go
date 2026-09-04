package walker

import (
	"context"
	"fmt"
	"path"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type NodeRewriteFunc func(node *data.Node, path string) *data.Node
type FailedTreeRewriteFunc func(nodeID vaultic.ID, path string, err error) (data.TreeNodeIterator, error)
type QueryRewrittenSizeFunc func() SnapshotSize
type NodeKeepEmptyDirectoryFunc func(path string) bool

type SnapshotSize struct {
	FileCount uint
	FileSize  uint64
}

type RewriteOpts struct {
	// return nil to remove the node
	RewriteNode        NodeRewriteFunc
	KeepEmptyDirectory NodeKeepEmptyDirectoryFunc
	// decide what to do with a tree that could not be loaded. Return nil to remove the node. By default the load error
	// is returned which causes the operation to fail.
	RewriteFailedTree FailedTreeRewriteFunc

	AllowUnstableSerialization bool
	DisableNodeCache           bool
}

type idMap map[vaultic.ID]vaultic.ID

type TreeRewriter struct {
	opts RewriteOpts

	replaces idMap
}

func NewTreeRewriter(opts RewriteOpts) *TreeRewriter {
	rw := &TreeRewriter{
		opts: opts,
	}
	if !opts.DisableNodeCache {
		rw.replaces = make(idMap)
	}
	// setup default implementations
	if rw.opts.RewriteNode == nil {
		rw.opts.RewriteNode = func(node *data.Node, _ string) *data.Node {
			return node
		}
	}
	if rw.opts.RewriteFailedTree == nil {
		// fail with error by default
		rw.opts.RewriteFailedTree = func(_ vaultic.ID, _ string, err error) (data.TreeNodeIterator, error) {
			return nil, err
		}
	}
	if rw.opts.KeepEmptyDirectory == nil {
		rw.opts.KeepEmptyDirectory = func(_ string) bool {
			return true
		}
	}
	return rw
}

func NewSnapshotSizeRewriter(
	rewriteNode NodeRewriteFunc,
	keepEmptyDirectoryFilter NodeKeepEmptyDirectoryFunc,
) (*TreeRewriter, QueryRewrittenSizeFunc) {
	var count uint
	var size uint64

	t := NewTreeRewriter(RewriteOpts{
		RewriteNode: func(node *data.Node, path string) *data.Node {
			node = rewriteNode(node, path)
			if node != nil && node.Type == data.NodeTypeFile {
				count++
				size += node.Size
			}
			return node
		},
		DisableNodeCache:   true,
		KeepEmptyDirectory: keepEmptyDirectoryFilter,
	})

	ss := func() SnapshotSize {
		return SnapshotSize{count, size}
	}

	return t, ss
}

func (t *TreeRewriter) RewriteTree(
	ctx context.Context,
	loader vaultic.BlobLoader,
	saver vaultic.BlobSaver,
	nodepath string,
	nodeID vaultic.ID,
) (newNodeID vaultic.ID, err error) {
	// check if tree was already changed
	newID, ok := t.replaces[nodeID]
	if ok {
		return newID, nil
	}

	// a nil nodeID will lead to a load error
	curTree, err := data.LoadTree(ctx, loader, nodeID)
	if err != nil {
		replacement, err := t.opts.RewriteFailedTree(nodeID, nodepath, err)
		if err != nil {
			return vaultic.ID{}, err
		}
		if replacement != nil {
			replacementID, err := data.SaveTree(ctx, saver, replacement)
			if err != nil {
				return vaultic.ID{}, err
			}
			return replacementID, nil
		}
		return vaultic.ID{}, nil
	}

	curTree, err = t.verifyTreeSerialization(ctx, loader, saver, nodepath, nodeID, curTree)
	if err != nil {
		return vaultic.ID{}, err
	}

	debug.Log("filterTree: %s, nodeId: %s\n", nodepath, nodeID.Str())

	tb := data.NewTreeWriter(saver)
	for item := range curTree {
		if ctx.Err() != nil {
			return vaultic.ID{}, ctx.Err()
		}
		if item.Error != nil {
			return vaultic.ID{}, item.Error
		}
		node, err := t.rewriteTreeNode(ctx, loader, saver, nodepath, item.Node)
		if err != nil {
			return vaultic.ID{}, err
		}
		if node == nil {
			continue
		}
		if err := tb.AddNode(node); err != nil {
			return vaultic.ID{}, err
		}
	}

	newTreeID, err := tb.Finalize(ctx)
	if err != nil {
		return vaultic.ID{}, err
	}
	if tb.Count() == 0 && !t.opts.KeepEmptyDirectory(nodepath) {
		return vaultic.ID{}, nil
	}

	if t.replaces != nil {
		t.replaces[nodeID] = newTreeID
	}
	if !newTreeID.Equal(nodeID) {
		debug.Log("filterTree: save new tree for %s as %v\n", nodepath, newTreeID)
	}
	return newTreeID, err
}

func (t *TreeRewriter) verifyTreeSerialization(
	ctx context.Context,
	loader vaultic.BlobLoader,
	saver vaultic.BlobSaver,
	nodepath string,
	nodeID vaultic.ID,
	tree data.TreeNodeIterator,
) (data.TreeNodeIterator, error) {
	if t.opts.AllowUnstableSerialization {
		return tree, nil
	}
	testID, err := data.SaveTree(ctx, saver, tree)
	if err != nil {
		return nil, err
	}
	if nodeID != testID {
		return nil, fmt.Errorf("cannot encode tree at %q without losing information", nodepath)
	}
	tree, err = data.LoadTree(ctx, loader, nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to reload tree %v: %w", nodeID, err)
	}
	return tree, nil
}

func (t *TreeRewriter) rewriteTreeNode(
	ctx context.Context,
	loader vaultic.BlobLoader,
	saver vaultic.BlobSaver,
	nodepath string,
	node *data.Node,
) (*data.Node, error) {
	childPath := path.Join(nodepath, node.Name)
	node = t.opts.RewriteNode(node, childPath)
	if node == nil || node.Type != data.NodeTypeDir {
		return node, nil
	}
	var subtree vaultic.ID
	if node.Subtree != nil {
		subtree = *node.Subtree
	}
	newID, err := t.RewriteTree(ctx, loader, saver, childPath, subtree)
	if err != nil || newID.IsNull() {
		return nil, err
	}
	node.Subtree = &newID
	return node, nil
}
