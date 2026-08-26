//go:build !selfupdate

package main

import (
	"github.com/spf13/cobra"
	"github.com/vaultic/vaultic/internal/global"
)

func registerSelfUpdateCommand(_ *cobra.Command, _ *global.Options) {
	// No commands to register in non-selfupdate mode
}
