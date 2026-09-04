package main

import "github.com/otuschhoff/vaultic/cmd/vaultic/indexcmd"

type indexBackendsOptions = indexcmd.BackendsOptions
type indexCheckOptions = indexcmd.CheckOptions
type indexDaemonOptions = indexcmd.DaemonOptions
type indexExportOptions = indexcmd.ExportOptions
type indexFileHistoryOptions = indexcmd.FileHistoryOptions
type indexGCOptions = indexcmd.GCOptions
type indexHistoryOptions = indexcmd.HistoryOptions
type indexHistoryPruneOptions = indexcmd.HistoryPruneOptions
type indexImportOptions = indexcmd.ImportOptions
type indexKeysOptions = indexcmd.KeysOptions
type indexPacksOptions = indexcmd.PacksOptions
type indexPathAtOptions = indexcmd.PathAtOptions
type indexPathIndexOptions = indexcmd.PathIndexOptions
type indexRebuildPackStatsOptions = indexcmd.RebuildPackStatsOptions
type indexStatsOptions = indexcmd.StatsOptions
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
