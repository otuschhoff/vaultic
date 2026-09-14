package snapshotfs

import (
	"context"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/bloblru"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type Repository interface {
	vaultic.BlobLoader
	LookupBlobSize(vaultic.BlobHandle) (uint, bool)
}

type logicalRangeReader interface {
	ReadLogicalFileRange(context.Context, []vaultic.ID, []uint64, uint64, []byte) (int, error)
}

type File struct {
	repo    Repository
	cache   *bloblru.Cache
	node    *data.Node
	cumSize []uint64
	size    uint64
}

func NewFile(ctx context.Context, repo Repository, cache *bloblru.Cache, node *data.Node) (*File, error) {
	cumSize := make([]uint64, len(node.Content)+1)
	var size uint64
	for index, id := range node.Content {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		blobSize, found := repo.LookupBlobSize(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id})
		if !found {
			return nil, fmt.Errorf("id %v not found in repository", id)
		}
		size += uint64(blobSize)
		cumSize[index+1] = size
	}

	return &File{repo: repo, cache: cache, node: node, cumSize: cumSize, size: size}, nil
}

func (file *File) Size() uint64 {
	return file.size
}

func (file *File) ReadAt(ctx context.Context, offset uint64, dst []byte) (int, error) {
	if file.size == 0 || offset >= file.size || len(dst) == 0 {
		return 0, nil
	}
	if reader, ok := file.repo.(logicalRangeReader); ok {
		return reader.ReadLogicalFileRange(ctx, file.node.Content, file.cumSize, offset, dst)
	}

	return vaultic.ReadLogicalFileRange(
		ctx,
		file.node.Content,
		file.cumSize,
		offset,
		dst,
		func(ctx context.Context, index int, _ vaultic.ID) ([]byte, error) {
			return file.cache.GetOrCompute(file.node.Content[index], func() ([]byte, error) {
				return file.repo.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.DataBlob, ID: file.node.Content[index]}, nil)
			})
		},
	)
}
