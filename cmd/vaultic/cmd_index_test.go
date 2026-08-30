package main

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
)

func TestIndexCommandGroupDoesNotChangeListIndex(t *testing.T) {
	root := newRootCommand(&global.Options{})
	for _, path := range [][]string{{"index", "import"}, {"index", "export"}, {"index", "check"}, {"index", "rebuild-pack-stats"}, {"index", "gc"}, {"index", "analytics"}, {"index", "growth"}, {"index", "user-stats"}, {"index", "gdpr", "audit"}} {
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

func TestAnalyticsIDConversionRejectsOverflow(t *testing.T) {
	if values, err := toUint32([]uint{0, math.MaxUint32}); err != nil || len(values) != 2 || values[1] != math.MaxUint32 {
		t.Fatalf("valid IDs rejected: %v, %v", values, err)
	}
	if ^uint(0) > math.MaxUint32 {
		if _, err := toUint32([]uint{uint(math.MaxUint32) + 1}); err == nil {
			t.Fatal("overflowing ID accepted")
		}
	}
}

func TestAnalyticsQueryOptionValidation(t *testing.T) {
	for name, options := range map[string]indexAnalyticsOptions{
		"equal size bounds":    {HasSizeMin: true, HasSizeMax: true, SizeMin: 10, SizeMax: 10},
		"reversed size bounds": {HasSizeMin: true, HasSizeMax: true, SizeMin: 11, SizeMax: 10},
		"current and stale":    {RequireCurrent: true, AllowStale: true},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateAnalyticsQueryOptions(options); err == nil {
				t.Fatal("invalid analytics options accepted")
			}
		})
	}
	if err := validateAnalyticsQueryOptions(indexAnalyticsOptions{HasSizeMin: true, HasSizeMax: true, SizeMin: 10, SizeMax: 11}); err != nil {
		t.Fatalf("valid half-open size range rejected: %v", err)
	}
}

func TestAnalyticsPhase16CommandSurface(t *testing.T) {
	command := newIndexAnalyticsCommand(&global.Options{})
	for _, name := range []string{"uid", "gid", "year", "month", "iso-year", "workweek", "svm", "volume", "path-group", "size-min", "size-max", "size-log10", "residency", "creation-basis", "identity-continuity", "group-by", "include-incomplete", "require-current", "allow-stale", "explain", "async", "query-id", "resume", "cancel", "wait"} {
		if command.Flags().Lookup(name) == nil {
			t.Errorf("analytics flag --%s is not registered", name)
		}
	}
	for _, path := range [][]string{{"status"}, {"catch-up"}, {"cache"}, {"cache", "purge"}} {
		child, args, err := command.Find(path)
		if err != nil || child == nil || len(args) != 0 || child.Name() != path[len(path)-1] {
			t.Errorf("analytics command %v = %v, %v, %v", path, child, args, err)
		}
	}
}

func TestAnalyticsParsingAndQueryConstruction(t *testing.T) {
	id, err := parseAnalyticsID(strings.Repeat("01", 32))
	if err != nil || id[0] != 1 || id[31] != 1 {
		t.Fatalf("valid query ID rejected: %x, %v", id, err)
	}
	if _, err := parseAnalyticsID("01"); err == nil {
		t.Fatal("short query ID accepted")
	}
	since, until, err := parseTimeRange("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z")
	if err != nil || *since != time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() || *until <= *since {
		t.Fatalf("valid time range rejected: %v, %v, %v", since, until, err)
	}
	if _, _, err := parseTimeRange("2024-02-01T00:00:00Z", "2024-01-01T00:00:00Z"); err == nil {
		t.Fatal("reversed time range accepted")
	}
	groups := []string{"uid", "year"}
	query, err := analyticsQuery(indexAnalyticsOptions{GroupBy: groups, CreationBases: []string{"first-seen"}, IncludeIncomplete: true})
	if err != nil || len(query.GroupBy) != 2 || query.CreationBases[0] != "first-seen" {
		t.Fatalf("query construction failed: %+v, %v", query, err)
	}
	query.GroupBy[0] = "gid"
	if groups[0] != "uid" {
		t.Fatal("analytics query construction mutated caller-owned slices")
	}
}

func TestAnalyticsJobOptionValidation(t *testing.T) {
	for _, options := range []indexAnalyticsOptions{{Async: true, QueryID: strings.Repeat("0", 64)}, {Resume: true}, {Cancel: true}, {Resume: true, Cancel: true, QueryID: strings.Repeat("0", 64)}, {Wait: true}} {
		if err := validateAnalyticsJobOptions(options); err == nil {
			t.Fatalf("invalid job options accepted: %+v", options)
		}
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

func TestIndexCheckTreatsAnalyticsMismatchAsDirty(t *testing.T) {
	result := maintenance.CheckResult{AnalyticsMismatch: 1}
	if result.Clean() {
		t.Fatal("analytics consistency mismatch did not make index check dirty")
	}
}
