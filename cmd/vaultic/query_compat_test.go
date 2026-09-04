package main

import "github.com/otuschhoff/vaultic/cmd/vaultic/querycmd"

type FindOptions = querycmd.FindOptions
type LsOptions = querycmd.LsOptions
type SnapshotOptions = querycmd.SnapshotOptions
type SnapshotGroup = querycmd.SnapshotGroup
type StatsOptions = querycmd.StatsOptions
type SortMode = querycmd.SortMode

const (
	SortModeName = querycmd.SortModeName
	SortModeSize = querycmd.SortModeSize
	SortModeExt  = querycmd.SortModeExt
)

var runFind = querycmd.RunFind
var runLs = querycmd.RunLs
var runSnapshots = querycmd.RunSnapshots
