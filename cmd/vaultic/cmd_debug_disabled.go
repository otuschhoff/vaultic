//go:build !debug

package main

import (
	"github.com/spf13/cobra"
	"github.com/vaultic/vaultic/internal/global"
)

func registerDebugCommand(_ *cobra.Command, _ *global.Options) {
	// No commands to register in non-debug mode
}
