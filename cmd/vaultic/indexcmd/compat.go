package indexcmd

type BackendsConfig = indexBackendsOptions
type CheckConfig = indexCheckOptions
type DaemonConfig = indexDaemonOptions
type ExportConfig = indexExportOptions
type FileHistoryConfig = indexFileHistoryOptions
type GCConfig = indexGCOptions
type HistoryConfig = indexHistoryOptions
type HistoryPruneConfig = indexHistoryPruneOptions
type ImportConfig = indexImportOptions
type KeysConfig = indexKeysOptions
type PacksConfig = indexPacksOptions
type PathAtConfig = indexPathAtOptions
type PathIndexConfig = indexPathIndexOptions
type RebuildPackStatsConfig = indexRebuildPackStatsOptions
type StatsConfig = indexStatsOptions
type BackendComparisonResult = BackendsResult

var RetireLegacyQuorumBypasses = retireLegacyQuorumBypasses
var RunBackends = runIndexBackends
var RunCheck = runIndexCheck
var RunExport = runIndexExport
var RunFileHistory = runIndexFileHistory
var RunGC = runIndexGC
var RunHistory = runIndexHistory
var RunHistoryPrune = runIndexHistoryPrune
var RunImport = runIndexImport
var RunPacks = runIndexPacks
var RunPathAt = runIndexPathAt
var RunPathIndex = runIndexPathIndex
var RunRebuildPackStats = runIndexRebuildPackStats
var RunStats = runIndexStats
var OpenIndexStore = openIndexStore
var NewKeysAddSlotCommand = newIndexKeysAddSlotCommand
var IndexDaemonContext = indexDaemonContext
var WriteNewProtectedJSON = writeNewProtectedJSON
