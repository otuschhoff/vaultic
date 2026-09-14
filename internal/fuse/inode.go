//go:build darwin || freebsd || linux

package fuse

import (
	"path/filepath"

	"github.com/cespare/xxhash/v2"
)

const prime = 11400714785074694791 // prime1 from xxhash.

func cleanupNodeName(name string) string {
	return filepath.Base(name)
}

// inodeFromName generates an inode number for a file in a meta dir.
func inodeFromName(parent uint64, name string) uint64 {
	inode := prime*parent ^ xxhash.Sum64String(cleanupNodeName(name))

	// Inode 0 is invalid and 1 is the root. Remap those.
	if inode < 2 {
		inode += 2
	}
	return inode
}
