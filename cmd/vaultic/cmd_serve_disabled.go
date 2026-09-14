//go:build !darwin && !freebsd && !linux && !windows

package main

import (
	"os"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/spf13/cobra"
)

func registerServeCommand(_ *cobra.Command, _ *global.Options) {}

func nfsServerIdentity() (uint32, uint32) { return 0, 0 }

func processAlive(int) bool { return false }

func replaceReadinessFile(source, destination string) error {
	return os.Rename(source, destination)
}
