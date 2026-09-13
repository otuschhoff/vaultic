package vaultic

import (
	"context"
	"sort"
)

// BlobReader loads a full blob for the given index in a logical file layout.
// Implementations can apply process-local caching, retries, or tracing.
type BlobReader func(ctx context.Context, index int, id ID) ([]byte, error)

// ReadLogicalFileRange copies up to len(dst) bytes from a logical file assembled
// from ordered blob IDs. cumSize must contain one leading zero and then the
// cumulative content size for each blob.
func ReadLogicalFileRange(ctx context.Context, content []ID, cumSize []uint64, offset uint64, dst []byte, readBlob BlobReader) (int, error) {
	if len(dst) == 0 || len(content) == 0 {
		return 0, nil
	}
	if len(cumSize) != len(content)+1 {
		return 0, nil
	}
	startContent := -1 + sort.Search(len(cumSize), func(i int) bool {
		return cumSize[i] > offset
	})
	if startContent < 0 || startContent >= len(content) {
		return 0, nil
	}
	remaining := len(dst)
	readBytes := 0
	localOffset := offset - cumSize[startContent]
	for i := startContent; i < len(content) && remaining > 0; i++ {
		if err := ctx.Err(); err != nil {
			return readBytes, err
		}
		blob, err := readBlob(ctx, i, content[i])
		if err != nil {
			return readBytes, err
		}
		if localOffset > 0 {
			if localOffset >= uint64(len(blob)) {
				localOffset -= uint64(len(blob))
				continue
			}
			blob = blob[localOffset:]
			localOffset = 0
		}
		copied := copy(dst[readBytes:], blob)
		readBytes += copied
		remaining -= copied
	}
	return readBytes, nil
}
