package querycmd

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/ui"
)

type FindConfig = findOptions
type LsConfig = lsOptions
type SnapshotConfig = snapshotOptions
type StatsConfig = statsOptions

var RejectResticCache = rejectResticCache
var FinalizeSnapshotFilter = finalizeSnapshotFilter
var InitMultiSnapshotFilter = initMultiSnapshotFilter
var InitSingleSnapshotFilter = initSingleSnapshotFilter
var RunFind = runFind
var RunLs = runLs
var RunSnapshots = runSnapshots

func RunLsDefault(ctx context.Context, globalOptions global.Options, args []string, term ui.Terminal) error {
	return runLs(ctx, lsOptions{}, globalOptions, args, term)
}
