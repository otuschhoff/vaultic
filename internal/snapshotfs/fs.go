package snapshotfs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/otuschhoff/vaultic/internal/bloblru"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const (
	DefaultBlobCacheBytes = 64 << 20
	// DefaultTreeCacheBytes bounds decoded tree metadata to 16 MiB per filesystem.
	DefaultTreeCacheBytes = 16 << 20
)

type OwnerProjection uint8

const (
	OwnerPreserved OwnerProjection = iota
	OwnerServer
	OwnerRoot
)

type PermissionsProjection uint8

const (
	PermissionsPreserved PermissionsProjection = iota
	PermissionsReadable
)

type Config struct {
	TreeCacheBytes int
	BlobCacheBytes int
	Owner          OwnerProjection
	Permissions    PermissionsProjection
	ServerUID      uint32
	ServerGID      uint32
}

type CacheStats struct {
	TreeBytes      int
	TreeLimitBytes int
}

type Filesystem struct {
	repo         vaultic.Repository
	snapshotID   vaultic.ID
	repositoryID string
	cfg          Config
	trees        *treeCache
	blobs        *bloblru.Cache
	root         *Node

	mu     sync.RWMutex
	closed bool
}

func New(ctx context.Context, repo vaultic.Repository, snapshot *data.Snapshot, subfolder string, cfg Config) (*Filesystem, error) {
	if err := ctx.Err(); err != nil {
		return nil, canceledError(err)
	}
	if repo == nil || snapshot == nil || snapshot.Tree == nil || snapshot.ID() == nil {
		return nil, fmt.Errorf("%w: snapshot, tree, repository, and snapshot ID are required", ErrInvalidNode)
	}
	if cfg.TreeCacheBytes < 0 || cfg.BlobCacheBytes < 0 {
		return nil, fmt.Errorf("%w: cache sizes must not be negative", ErrInvalidNode)
	}
	if cfg.TreeCacheBytes == 0 {
		cfg.TreeCacheBytes = DefaultTreeCacheBytes
	}
	if cfg.BlobCacheBytes == 0 {
		cfg.BlobCacheBytes = DefaultBlobCacheBytes
	}
	if cfg.TreeCacheBytes < 1 || cfg.BlobCacheBytes < 128 {
		return nil, fmt.Errorf("%w: cache sizes are too small", ErrInvalidNode)
	}
	if cfg.Owner > OwnerRoot || cfg.Permissions > PermissionsReadable {
		return nil, fmt.Errorf("%w: unknown metadata projection", ErrInvalidNode)
	}

	fs := &Filesystem{
		repo:         repo,
		snapshotID:   *snapshot.ID(),
		repositoryID: repo.Config().ID,
		cfg:          cfg,
		trees:        newTreeCache(cfg.TreeCacheBytes),
		blobs:        bloblru.New(cfg.BlobCacheBytes),
	}
	rootData := &data.Node{
		Name:       "",
		Type:       data.NodeTypeDir,
		Mode:       os.ModeDir | 0555,
		ModTime:    snapshot.Time,
		AccessTime: snapshot.Time,
		ChangeTime: snapshot.Time,
		UID:        snapshot.UID,
		GID:        snapshot.GID,
		Subtree:    snapshot.Tree,
	}
	fs.root = fs.newNode("/", rootData)

	components, err := splitSubfolder(subfolder)
	if err != nil {
		fs.Close()
		return nil, err
	}
	for _, component := range components {
		fs.root, err = fs.root.lookup(ctx, component, false)
		if err != nil {
			fs.Close()
			return nil, err
		}
		if fs.root.node.Type != data.NodeTypeDir {
			fs.Close()
			return nil, fmt.Errorf("%w: subfolder component %q is not a directory", ErrNotFound, component)
		}
	}
	return fs, nil
}

func (fs *Filesystem) Root() (*Node, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if fs.closed {
		return nil, ErrClosed
	}
	return fs.root, nil
}

func (fs *Filesystem) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.closed {
		return nil
	}
	fs.closed = true
	fs.trees.clear()
	fs.blobs.Clear()
	return nil
}

func (fs *Filesystem) CacheStats() CacheStats {
	used, limit := fs.trees.usage()
	return CacheStats{TreeBytes: used, TreeLimitBytes: limit}
}

func (fs *Filesystem) loadTree(ctx context.Context, id vaultic.ID) ([]*data.Node, error) {
	return fs.trees.getOrCompute(ctx, id, func() ([]*data.Node, int, error) {
		buf, err := fs.repo.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: id}, nil)
		if err != nil {
			return nil, 0, repositoryError(ctx, err)
		}
		iterator, err := data.NewTreeNodeIterator(bytes.NewReader(buf))
		if err != nil {
			return nil, 0, fmt.Errorf("%w: decode tree %s: %v", ErrInvalidNode, id.Str(), err)
		}
		nodes := make([]*data.Node, 0)
		for item := range iterator {
			if item.Error != nil {
				return nil, 0, fmt.Errorf("%w: decode tree %s: %v", ErrInvalidNode, id.Str(), item.Error)
			}
			if err := ctx.Err(); err != nil {
				return nil, 0, canceledError(err)
			}
			nodes = append(nodes, item.Node)
		}
		return nodes, cap(buf) + len(nodes)*decodedNodeEstimate, nil
	})
}

const decodedNodeEstimate = 256

func splitSubfolder(subfolder string) ([]string, error) {
	if subfolder == "" || subfolder == "/" {
		return nil, nil
	}
	subfolder = strings.TrimPrefix(subfolder, "/")
	subfolder = strings.TrimSuffix(subfolder, "/")
	components := strings.Split(subfolder, "/")
	for _, component := range components {
		if err := validateComponent(component); err != nil {
			return nil, err
		}
	}
	return components, nil
}

func validateComponent(name string) error {
	switch {
	case len(name) > 255:
		return fmt.Errorf("%w: %d bytes", ErrNameTooLong, len(name))
	case name == "", name == ".", name == "..", strings.ContainsAny(name, "/\x00"):
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	default:
		return nil
	}
}

func canceledError(err error) error {
	return fmt.Errorf("%w: %w", ErrCanceled, err)
}

func repositoryError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return canceledError(ctxErr)
	}
	return fmt.Errorf("%w: %w", ErrRepository, err)
}
