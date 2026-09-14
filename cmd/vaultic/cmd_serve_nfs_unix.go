//go:build darwin || freebsd || linux

package main

import (
	"errors"
	"os"
	"syscall"
)

func nfsServerIdentity() (uint32, uint32) {
	return uint32(os.Getuid()), uint32(os.Getgid())
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func replaceReadinessFile(source, destination string) error {
	return os.Rename(source, destination)
}
