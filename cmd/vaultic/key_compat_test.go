package main

import "github.com/otuschhoff/vaultic/cmd/vaultic/keycmd"

type keyAddOptions = keycmd.AddConfig
type keyPasswdOptions = keycmd.PasswdConfig

var runKeyAdd = keycmd.RunAdd
var runKeyAddWithPassword = keycmd.RunAddWithPassword
var runKeyList = keycmd.RunList
var runKeyPasswd = keycmd.RunPasswd
var runKeyPasswdWithPassword = keycmd.RunPasswdWithPassword
var runKeyRemove = keycmd.RunRemove
