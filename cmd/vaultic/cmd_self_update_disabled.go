//go:build !selfupdate

package main

import (
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/spf13/cobra"
)

func registerSelfUpdateCommand(_ *cobra.Command, _ *global.Options) {
	// No commands to register in non-selfupdate mode
}
