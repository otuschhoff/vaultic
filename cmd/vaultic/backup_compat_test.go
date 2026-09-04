package main

import "github.com/otuschhoff/vaultic/cmd/vaultic/backupcmd"

type BackupOptions = backupcmd.BackupOptions

var runBackup = backupcmd.Run
var ErrInvalidSourceData = backupcmd.ErrInvalidSourceData
var ErrNoSourceData = backupcmd.ErrNoSourceData
