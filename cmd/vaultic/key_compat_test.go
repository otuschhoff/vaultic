package main

import "github.com/otuschhoff/vaultic/cmd/vaultic/keycmd"

type KeyAddOptions = keycmd.AddOptions
type KeyPasswdOptions = keycmd.PasswdOptions

var runKeyAdd = keycmd.RunAdd
var runKeyList = keycmd.RunList
var runKeyPasswd = keycmd.RunPasswd
var runKeyRemove = keycmd.RunRemove
