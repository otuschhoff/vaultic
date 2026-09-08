//go:build !darwin

package apfs

func processAlive(int) bool { return false }
