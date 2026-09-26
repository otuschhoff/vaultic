//go:build linux

package fs

import (
	"github.com/moby/sys/mountinfo"
	"golang.org/x/sys/unix"
)

func readNFSMounts() ([]nfsMount, error) {
	entries, err := mountinfo.GetMounts(nil)
	if err != nil {
		return nil, err
	}
	mounts := make([]nfsMount, 0, len(entries))
	for _, entry := range entries {
		mounts = append(mounts, nfsMount{point: entry.Mountpoint, root: entry.Root, source: entry.Source,
			kind: entry.FSType, options: entry.VFSOptions + "," + entry.Options, device: unix.Mkdev(uint32(entry.Major), uint32(entry.Minor))})
	}
	return mounts, nil
}
