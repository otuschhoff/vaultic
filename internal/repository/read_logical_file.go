package repository

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// ReadLogicalFileRange serves protocol-neutral logical-file range reads through
// repository blob identities so FUSE, restore, dump, and future NFS can share
// the same authenticated read path and cache manager behavior.
func (r *Repository) ReadLogicalFileRange(ctx context.Context, content []vaultic.ID, cumSize []uint64, offset uint64, dst []byte) (int, error) {
	//nolint:contextcheck // The shared cache worker is owned by Repository.Close, while ctx still bounds this logical read.
	manager := r.readCacheManager()
	if manager != nil &&
		r.readCacheAuthoritativeEligibleForLogicalRange(ctx, content, cumSize, offset, len(dst)) &&
		manager.refreshPolicyForAccess(ctx) {
		return manager.readLogicalFileRange(ctx, content, cumSize, offset, dst, func(ctx context.Context, _ int, id vaultic.ID) ([]byte, error) {
			return r.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}, nil)
		})
	}
	return vaultic.ReadLogicalFileRange(
		ctx,
		content,
		cumSize,
		offset,
		dst,
		func(ctx context.Context, _ int, id vaultic.ID) ([]byte, error) {
			return r.LoadBlob(ctx, vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}, nil)
		},
	)
}
