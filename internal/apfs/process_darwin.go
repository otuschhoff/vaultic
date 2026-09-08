//go:build darwin

// Package apfs provides consistent macOS backup sources using APFS snapshots.
package apfs

import "golang.org/x/sys/unix"

func processAlive(pid int) bool {
	return pid > 0 && (unix.Kill(pid, 0) == nil || unix.Kill(pid, 0) == unix.EPERM)
}
