//go:build !debug

package main

import (
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/spf13/cobra"
)

func registerDebugCommand(_ *cobra.Command, _ *global.Options) {
	// No commands to register in non-debug mode
}
