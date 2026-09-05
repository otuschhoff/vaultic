package main

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func testRunTag(t testing.TB, options tagOptions, globalOptions global.Options) {
	rtest.OK(t, withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runTag(context.TODO(), options, globalOptions, globalOptions.Term, []string{})
	}))
}

func TestTag(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{env.testdata}, backupOptions{}, env.globalOptions)
	testRunCheck(t, env.globalOptions)
	newest, _ := testRunSnapshots(t, env.globalOptions)
	if newest == nil {
		t.Fatal("expected a new backup, got nil")
	}

	rtest.Assert(t, len(newest.Tags) == 0,
		"expected no tags, got %v", newest.Tags)
	rtest.Assert(t, newest.Original == nil,
		"expected original ID to be nil, got %v", newest.Original)
	originalID := *newest.ID

	testRunTag(t, tagOptions{SetTags: data.TagLists{[]string{"NL"}}}, env.globalOptions)
	testRunCheck(t, env.globalOptions)
	newest, _ = testRunSnapshots(t, env.globalOptions)
	if newest == nil {
		t.Fatal("expected a backup, got nil")
	}
	rtest.Assert(t, len(newest.Tags) == 1 && newest.Tags[0] == "NL",
		"set failed, expected one NL tag, got %v", newest.Tags)
	rtest.Assert(t, newest.Original != nil, "expected original snapshot id, got nil")
	rtest.Assert(t, *newest.Original == originalID,
		"expected original ID to be set to the first snapshot id")

	testRunTag(t, tagOptions{AddTags: data.TagLists{[]string{"CH"}}}, env.globalOptions)
	testRunCheck(t, env.globalOptions)
	newest, _ = testRunSnapshots(t, env.globalOptions)
	if newest == nil {
		t.Fatal("expected a backup, got nil")
	}
	rtest.Assert(t, len(newest.Tags) == 2 && newest.Tags[0] == "NL" && newest.Tags[1] == "CH",
		"add failed, expected CH,NL tags, got %v", newest.Tags)
	rtest.Assert(t, newest.Original != nil, "expected original snapshot id, got nil")
	rtest.Assert(t, *newest.Original == originalID,
		"expected original ID to be set to the first snapshot id")

	testRunTag(t, tagOptions{RemoveTags: data.TagLists{[]string{"NL"}}}, env.globalOptions)
	testRunCheck(t, env.globalOptions)
	newest, _ = testRunSnapshots(t, env.globalOptions)
	if newest == nil {
		t.Fatal("expected a backup, got nil")
	}
	rtest.Assert(t, len(newest.Tags) == 1 && newest.Tags[0] == "CH",
		"remove failed, expected one CH tag, got %v", newest.Tags)
	rtest.Assert(t, newest.Original != nil, "expected original snapshot id, got nil")
	rtest.Assert(t, *newest.Original == originalID,
		"expected original ID to be set to the first snapshot id")

	testRunTag(t, tagOptions{AddTags: data.TagLists{[]string{"US", "RU"}}}, env.globalOptions)
	testRunTag(t, tagOptions{RemoveTags: data.TagLists{[]string{"CH", "US", "RU"}}}, env.globalOptions)
	testRunCheck(t, env.globalOptions)
	newest, _ = testRunSnapshots(t, env.globalOptions)
	if newest == nil {
		t.Fatal("expected a backup, got nil")
	}
	rtest.Assert(t, len(newest.Tags) == 0,
		"expected no tags, got %v", newest.Tags)
	rtest.Assert(t, newest.Original != nil, "expected original snapshot id, got nil")
	rtest.Assert(t, *newest.Original == originalID,
		"expected original ID to be set to the first snapshot id")

	// Check special case of removing all tags.
	testRunTag(t, tagOptions{SetTags: data.TagLists{[]string{""}}}, env.globalOptions)
	testRunCheck(t, env.globalOptions)
	newest, _ = testRunSnapshots(t, env.globalOptions)
	if newest == nil {
		t.Fatal("expected a backup, got nil")
	}
	rtest.Assert(t, len(newest.Tags) == 0,
		"expected no tags, got %v", newest.Tags)
	rtest.Assert(t, newest.Original != nil, "expected original snapshot id, got nil")
	rtest.Assert(t, *newest.Original == originalID,
		"expected original ID to be set to the first snapshot id")
}
