package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/spf13/cobra"
)

var importantGoModules = map[string]struct{}{
	"cloud.google.com/go/storage":                                       {},
	"github.com/Azure/azure-sdk-for-go/sdk/azcore":                      {},
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity":                  {},
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets": {},
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob":              {},
	"github.com/cespare/xxhash/v2":                                      {},
	"github.com/minio/minio-go/v7":                                      {},
	"golang.org/x/crypto":                                               {},
	"google.golang.org/grpc":                                            {},
}

type dependencyVersion struct {
	Module  string `json:"module"`
	Version string `json:"version"`
}

type versionInfo struct {
	MessageType  string              `json:"message_type"`
	Version      string              `json:"version"`
	GoVersion    string              `json:"go_version"`
	GoOS         string              `json:"go_os"`
	GoArch       string              `json:"go_arch"`
	Dependencies []dependencyVersion `json:"dependencies"`
}

func collectVersionInfo() versionInfo {
	info := versionInfo{
		MessageType: "version",
		Version:     global.Version,
		GoVersion:   runtime.Version(),
		GoOS:        runtime.GOOS,
		GoArch:      runtime.GOARCH,
	}
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	for _, dependency := range build.Deps {
		if _, important := importantGoModules[dependency.Path]; !important {
			continue
		}
		version := dependency.Version
		if dependency.Replace != nil {
			version = fmt.Sprintf("%s (replaced by %s %s)", version, dependency.Replace.Path, dependency.Replace.Version)
		}
		info.Dependencies = append(info.Dependencies, dependencyVersion{Module: dependency.Path, Version: version})
	}
	sort.Slice(info.Dependencies, func(i, j int) bool {
		return info.Dependencies[i].Module < info.Dependencies[j].Module
	})
	return info
}

func textVersionReport(info versionInfo) string {
	var report strings.Builder
	fmt.Fprintf(&report, "vaultic %s compiled with %s on %s/%s\n", info.Version, info.GoVersion, info.GoOS, info.GoArch)
	if len(info.Dependencies) > 0 {
		report.WriteString("important dependencies:\n")
		for _, dependency := range info.Dependencies {
			fmt.Fprintf(&report, "%s %s\n", dependency.Module, dependency.Version)
		}
	}
	return report.String()
}

func newVersionCommand(globalOptions *global.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Long: `
The "version" command prints detailed information about the build environment
and the version of this software.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
`,
		DisableAutoGenTag: true,
		Run: func(_ *cobra.Command, _ []string) {
			printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
			info := collectVersionInfo()

			if globalOptions.JSON {
				err := json.NewEncoder(globalOptions.Term.OutputWriter()).Encode(info)
				if err != nil {
					printer.E("JSON encode failed: %v\n", err)
					return
				}
			} else {
				printer.S("%s", textVersionReport(info))
			}
		},
	}
	return cmd
}
