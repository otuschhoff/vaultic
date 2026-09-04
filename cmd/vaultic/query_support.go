package main

import (
	"github.com/otuschhoff/vaultic/cmd/vaultic/querycmd"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/spf13/pflag"
)

func finalizeSnapshotFilter(filter *data.SnapshotFilter) {
	querycmd.FinalizeSnapshotFilter(filter)
}

func initMultiSnapshotFilter(flags *pflag.FlagSet, filter *data.SnapshotFilter, addHostShorthand bool) {
	querycmd.InitMultiSnapshotFilter(flags, filter, addHostShorthand)
}

func initSingleSnapshotFilter(flags *pflag.FlagSet, filter *data.SnapshotFilter) {
	querycmd.InitSingleSnapshotFilter(flags, filter)
}
