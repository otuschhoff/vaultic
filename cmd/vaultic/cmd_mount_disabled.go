//go:build !darwin && !freebsd && !linux

package main

import (
	"github.com/spf13/cobra"
	"github.com/vaultic/vaultic/internal/global"
)

func registerMountCommand(_ *cobra.Command, _ *global.Options) {
	// Mount command not supported on these platforms
}
