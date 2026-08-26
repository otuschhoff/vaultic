package repository

import (
	"context"
	"io"

	"github.com/vaultic/vaultic/internal/backend"
	"github.com/vaultic/vaultic/internal/vaultic"
)

// CopyMetadata mirrors the metadata files (keys, snapshots, indexes) of src
// into dst, raw (still encrypted, identical bytes). It is used to build the
// hot part of a hot/cold repository (`init --hot-only`).
//
// The config file is not copied: dst is expected to have its own (marked
// is_hot) config.
func CopyMetadata(ctx context.Context, src, dst *Repository) error {
	srcBe := src.Backend()
	dstBe := dst.Backend()

	for _, t := range []vaultic.FileType{vaultic.KeyFile, vaultic.SnapshotFile, vaultic.IndexFile} {
		bt := backend.FileType(t)
		err := srcBe.List(ctx, bt, func(fi backend.FileInfo) error {
			return copyFile(ctx, srcBe, dstBe, bt, fi)
		})
		if err != nil {
			return err
		}
	}

	// also mirror the tree packs (they are stored as pack files marked
	// IsMetadata): the hot part must hold them so that listing trees never
	// touches the cold storage. Tree packs are identified via the index; data
	// packs are deliberately not copied.
	treePacks, err := treePackIDs(ctx, src)
	if err != nil {
		return err
	}
	err = srcBe.List(ctx, backend.PackFile, func(fi backend.FileInfo) error {
		id, err := vaultic.ParseID(fi.Name)
		if err != nil || !treePacks.Has(id) {
			return nil // not a tree pack (or unparseable): skip
		}
		return copyFile(ctx, srcBe, dstBe, backend.PackFile, fi)
	})
	if err != nil {
		return err
	}
	return ctx.Err()
}

// treePackIDs returns the set of pack IDs that hold tree blobs (per the index).
func treePackIDs(ctx context.Context, repo *Repository) (vaultic.IDSet, error) {
	if err := repo.LoadIndex(ctx, vaultic.NewNoopPrinter()); err != nil {
		return nil, err
	}
	treePacks := vaultic.NewIDSet()
	err := repo.ListBlobs(ctx, func(pb vaultic.PackBlob) {
		if pb.Handle().Type == vaultic.TreeBlob {
			treePacks.Insert(pb.PackID())
		}
	})
	return treePacks, err
}

// copyFile copies a single file verbatim (still encrypted) from src to dst,
// skipping files that already exist in dst with the same size.
func copyFile(ctx context.Context, srcBe, dstBe backend.Backend, bt backend.FileType, fi backend.FileInfo) error {
	h := backend.Handle{Type: bt, Name: fi.Name}

	// skip files that already exist in dst with identical size
	if existing, err := dstBe.Stat(ctx, h); err == nil && existing.Size == fi.Size {
		return nil
	}

	return srcBe.Load(ctx, h, 0, 0, func(rd io.Reader) error {
		buf, err := io.ReadAll(rd)
		if err != nil {
			return err
		}
		return dstBe.Save(ctx, h, backend.NewByteReader(buf, dstBe.Hasher()))
	})
}
