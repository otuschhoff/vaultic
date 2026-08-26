package repository

import (
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// BlobsInPack returns index entries for blobs stored in packID, sorted by offset.
func BlobsInPack(repo *Repository, packID vaultic.ID) pack.Blobs {
	var blobs pack.Blobs
	for pb := range repo.idx.Values() {
		if pb.PackID().Equal(packID) {
			blobs = append(blobs, pb.Blob)
		}
	}
	blobs.Sort()
	return blobs
}
