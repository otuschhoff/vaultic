package main

import "github.com/otuschhoff/vaultic/cmd/vaultic/querycmd"

type findOptions = querycmd.FindConfig
type lsOptions = querycmd.LsConfig
type snapshotOptions = querycmd.SnapshotConfig
type SnapshotGroup = querycmd.SnapshotGroup
type SortMode = querycmd.SortMode

const (
	SortModeName = querycmd.SortModeName
	SortModeSize = querycmd.SortModeSize
	SortModeExt  = querycmd.SortModeExt
)

var runFind = querycmd.RunFind
var runLs = querycmd.RunLs
var runSnapshots = querycmd.RunSnapshots
