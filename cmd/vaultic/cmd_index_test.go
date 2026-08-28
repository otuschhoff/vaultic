package main

import (
	"errors"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
)

func TestIndexCommandGroupDoesNotChangeListIndex(t *testing.T) {
	root := newRootCommand(&global.Options{})
	for _, path := range [][]string{{"index", "import"}, {"index", "export"}, {"index", "check"}, {"index", "rebuild-pack-stats"}, {"index", "gc"}} {
		command, args, err := root.Find(path)
		if err != nil || command == nil || len(args) != 0 || command.Name() != path[len(path)-1] {
			t.Fatalf("find %v = %v, %v, %v", path, command, args, err)
		}
	}
	command, args, err := root.Find([]string{"list", "index"})
	if err != nil || command == nil || command.Name() != "list" || len(args) != 1 || args[0] != "index" {
		t.Fatalf("list index = %v, %v, %v", command, args, err)
	}
}

func TestIndexDaemonOptionsRequireExplicitTCPConfiguration(t *testing.T) {
	for name, options := range map[string]indexDaemonOptions{
		"token without TCP":        {AuthToken: "token"},
		"allowlist without TCP":    {TCPAllowlist: []string{"127.0.0.0/8"}},
		"socket and TCP":           {Socket: "/tmp/vaulticdb.sock", TCPAddress: "127.0.0.1:9876"},
		"persistent without start": {Persistent: true},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := options.config("repository"); err == nil {
				t.Fatal("invalid daemon options accepted")
			}
		})
	}
	config, err := (indexDaemonOptions{TCPAddress: "127.0.0.1:9876", TCPAllowlist: []string{"127.0.0.0/8"}, AuthToken: "token", Start: true, DaemonPath: "vaulticdb"}).config("repository")
	if err != nil || config.RepositoryID != "repository" || config.DaemonPath != "vaulticdb" || config.Socket != "" {
		t.Fatalf("valid TCP config = %#v, %v", config, err)
	}
	s3, err := (indexDaemonOptions{Start: true, DaemonPath: "vaulticdb", ObjectStore: "s3", S3Bucket: "bucket", S3Prefix: "repo/index"}).config("repository")
	if err != nil || s3.ObjectStore != "s3" || s3.S3Bucket != "bucket" || s3.S3Prefix != "repo/index" {
		t.Fatalf("S3 config = %#v, %v", s3, err)
	}
}

func TestIndexPartialResultsUseWarningSentinels(t *testing.T) {
	if !errors.Is(errIndexDifferences, errIndexDifferences) || !errors.Is(errIndexIncomplete, errIndexIncomplete) {
		t.Fatal("index warning sentinels do not support errors.Is")
	}
	if code := exitCodeForError(errIndexDifferences); code != 2 {
		t.Fatalf("difference exit code = %d, want 2", code)
	}
	if code := exitCodeForError(errIndexIncomplete); code != 2 {
		t.Fatalf("incomplete exit code = %d, want 2", code)
	}
}
