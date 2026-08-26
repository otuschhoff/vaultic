package data

import (
	"context"
	"sync"

	"github.com/vaultic/vaultic/internal/vaultic"
)

// FindUsedBlobs traverses the tree ID and adds all seen blobs (trees and data
// blobs) to the set blobs. Already seen tree blobs will not be visited again.
func FindUsedBlobs(ctx context.Context, repo vaultic.Loader, treeIDs vaultic.IDs, blobs vaultic.FindBlobSet, p vaultic.Counter) error {
	var lock sync.Mutex

	return StreamTrees(ctx, repo, treeIDs, p, func(treeID vaultic.ID) bool {
		// locking is necessary the goroutine below concurrently adds data blobs
		lock.Lock()
		h := vaultic.BlobHandle{ID: treeID, Type: vaultic.TreeBlob}
		blobReferenced := blobs.Has(h)
		// noop if already referenced
		blobs.Insert(h)
		lock.Unlock()
		return blobReferenced
	}, func(_ vaultic.ID, err error, nodes TreeNodeIterator) error {
		if err != nil {
			return err
		}

		for item := range nodes {
			if item.Error != nil {
				return item.Error
			}
			lock.Lock()
			switch item.Node.Type {
			case NodeTypeFile:
				for _, blob := range item.Node.Content {
					blobs.Insert(vaultic.BlobHandle{ID: blob, Type: vaultic.DataBlob})
				}
			}
			lock.Unlock()
		}
		return nil
	})
}
