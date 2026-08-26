package main

import (
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/env"
	"github.com/spf13/pflag"
)

// initMultiSnapshotFilter is used for commands that work on multiple snapshots
// MUST be combined with (*data,SnapshotFilter).FindAll
// MUST be followed by finalizeSnapshotFilter after flag parsing
func initMultiSnapshotFilter(flags *pflag.FlagSet, filt *data.SnapshotFilter, addHostShorthand bool) {
	hostShorthand := "H"
	if !addHostShorthand {
		hostShorthand = ""
	}
	flags.StringArrayVarP(&filt.Hosts, "host", hostShorthand, nil, "only consider snapshots for this `host` (can be specified multiple times, use empty string to unset default value) (default: $VAULTIC_HOST)")
	flags.Var(&filt.Tags, "tag", "only consider snapshots including `tag[,tag,...]` (can be specified multiple times)")
	flags.StringArrayVar(&filt.Paths, "path", nil, "only consider snapshots including this (absolute) `path` (can be specified multiple times, snapshots must include all specified paths)")
	initExtendedSnapshotFilter(flags, filt, true)
}

// initSingleSnapshotFilter is used for commands that work on a single snapshot
// MUST be combined with (*data.SnapshotFilter).FindLatest
// MUST be followed by finalizeSnapshotFilter after flag parsing
func initSingleSnapshotFilter(flags *pflag.FlagSet, filt *data.SnapshotFilter) {
	flags.StringArrayVarP(&filt.Hosts, "host", "H", nil, "only consider snapshots for this `host`, when snapshot ID \"latest\" is given (can be specified multiple times, use empty string to unset default value) (default: $VAULTIC_HOST)")
	flags.Var(&filt.Tags, "tag", "only consider snapshots including `tag[,tag,...]`, when snapshot ID \"latest\" is given (can be specified multiple times)")
	flags.StringArrayVar(&filt.Paths, "path", nil, "only consider snapshots including this (absolute) `path`, when snapshot ID \"latest\" is given (can be specified multiple times, snapshots must include all specified paths)")
	initExtendedSnapshotFilter(flags, filt, true)
}

// finalizeSnapshotFilter applies VAULTIC_HOST default only if --host flag wasn't explicitly set.
// This allows users to override VAULTIC_HOST by providing --host="" or --host with explicit values.
func finalizeSnapshotFilter(filt *data.SnapshotFilter) {
	// Only apply VAULTIC_HOST default if the --host flag wasn't changed by the user
	if filt.Hosts == nil {
		if host := env.Get("HOST"); host != "" {
			filt.Hosts = []string{host}
		}
	}
	// If flag was set to empty string explicitly (e.g., --host=""),
	// filt.Hosts will be []string{""} which should be cleaned up to allow all hosts
	if len(filt.Hosts) == 1 && filt.Hosts[0] == "" {
		filt.Hosts = nil
	}
}
