package main

import "github.com/otuschhoff/vaultic/cmd/vaultic/indexcmd"

type indexBackendsOptions = indexcmd.BackendsConfig
type indexCheckOptions = indexcmd.CheckConfig
type indexDaemonOptions = indexcmd.DaemonConfig
type indexExportOptions = indexcmd.ExportConfig
type indexFileHistoryOptions = indexcmd.FileHistoryConfig
type indexGCOptions = indexcmd.GCConfig
type indexHistoryOptions = indexcmd.HistoryConfig
type indexHistoryPruneOptions = indexcmd.HistoryPruneConfig
type indexImportOptions = indexcmd.ImportConfig
type indexKeysOptions = indexcmd.KeysConfig
type indexPacksOptions = indexcmd.PacksConfig
type indexPathAtOptions = indexcmd.PathAtConfig
type indexPathIndexOptions = indexcmd.PathIndexConfig
type indexRebuildPackStatsOptions = indexcmd.RebuildPackStatsConfig
type indexStatsOptions = indexcmd.StatsConfig
type BackendsResult = indexcmd.BackendComparisonResult

var retireLegacyQuorumBypasses = indexcmd.RetireLegacyQuorumBypasses
var runIndexBackends = indexcmd.RunBackends
var runIndexCheck = indexcmd.RunCheck
var runIndexExport = indexcmd.RunExport
var runIndexFileHistory = indexcmd.RunFileHistory
var runIndexGC = indexcmd.RunGC
var runIndexHistory = indexcmd.RunHistory
var runIndexHistoryPrune = indexcmd.RunHistoryPrune
var runIndexImport = indexcmd.RunImport
var runIndexPacks = indexcmd.RunPacks
var runIndexPathAt = indexcmd.RunPathAt
var runIndexPathIndex = indexcmd.RunPathIndex
var runIndexRebuildPackStats = indexcmd.RunRebuildPackStats
var runIndexStats = indexcmd.RunStats
var openIndexStore = indexcmd.OpenIndexStore
var newIndexKeysAddSlotCommand = indexcmd.NewKeysAddSlotCommand
var errIndexDifferences = indexcmd.ErrDifferences
var errIndexIncomplete = indexcmd.ErrIncomplete
var indexDaemonContext = indexcmd.IndexDaemonContext
var writeNewProtectedJSON = indexcmd.WriteNewProtectedJSON
