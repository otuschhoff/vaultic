//go:build !linux

package fs

import "fmt"

func readNFSMounts() ([]nfsMount, error) {
	return nil, fmt.Errorf("automatic direct NFS mount detection is supported only on Linux")
}
