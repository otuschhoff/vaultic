package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/otuschhoff/vaultic/internal/filter"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func TestBackupFailsWhenUsingInvalidPatterns(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testRunInit(t, env.globalOptions)

	var err error

	// Test --exclude
	err = testRunBackupAssumeFailure(
		t,
		filepath.Dir(env.testdata),
		[]string{"testdata"},
		backupOptions{ExcludePatternOptions: filter.ExcludePatternOptions{Excludes: []string{"*[._]log[.-][0-9]", "!*[._]log[.-][0-9]"}}},
		env.globalOptions,
	)

	rtest.Equals(t, `Fatal: --exclude: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())

	// Test --iexclude
	err = testRunBackupAssumeFailure(
		t,
		filepath.Dir(env.testdata),
		[]string{"testdata"},
		backupOptions{ExcludePatternOptions: filter.ExcludePatternOptions{InsensitiveExcludes: []string{"*[._]log[.-][0-9]", "!*[._]log[.-][0-9]"}}},
		env.globalOptions,
	)

	rtest.Equals(t, `Fatal: --iexclude: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())
}

func TestBackupFailsWhenUsingInvalidPatternsFromFile(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testRunInit(t, env.globalOptions)

	// Create an exclude file with some invalid patterns
	excludeFile := env.base + "/excludefile"
	fileErr := os.WriteFile(excludeFile, []byte("*.go\n*[._]log[.-][0-9]\n!*[._]log[.-][0-9]"), 0644)
	if fileErr != nil {
		t.Fatalf("Could not write exclude file: %v", fileErr)
	}

	var err error

	// Test --exclude-file:
	err = testRunBackupAssumeFailure(
		t,
		filepath.Dir(env.testdata),
		[]string{"testdata"},
		backupOptions{ExcludePatternOptions: filter.ExcludePatternOptions{ExcludeFiles: []string{excludeFile}}},
		env.globalOptions,
	)

	rtest.Equals(t, `Fatal: --exclude-file: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())

	// Test --iexclude-file
	err = testRunBackupAssumeFailure(
		t,
		filepath.Dir(env.testdata),
		[]string{"testdata"},
		backupOptions{ExcludePatternOptions: filter.ExcludePatternOptions{InsensitiveExcludeFiles: []string{excludeFile}}},
		env.globalOptions,
	)

	rtest.Equals(t, `Fatal: --iexclude-file: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())
}

func TestRestoreFailsWhenUsingInvalidPatterns(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testRunInit(t, env.globalOptions)

	var err error

	// Test --exclude
	err = testRunRestoreAssumeFailure(
		t,
		"latest",
		restoreOptions{ExcludePatternOptions: filter.ExcludePatternOptions{Excludes: []string{"*[._]log[.-][0-9]", "!*[._]log[.-][0-9]"}}},
		env.globalOptions,
	)

	rtest.Equals(t, `Fatal: --exclude: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())

	// Test --iexclude
	err = testRunRestoreAssumeFailure(
		t,
		"latest",
		restoreOptions{ExcludePatternOptions: filter.ExcludePatternOptions{InsensitiveExcludes: []string{"*[._]log[.-][0-9]", "!*[._]log[.-][0-9]"}}},
		env.globalOptions,
	)

	rtest.Equals(t, `Fatal: --iexclude: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())

	// Test --include
	err = testRunRestoreAssumeFailure(
		t,
		"latest",
		restoreOptions{IncludePatternOptions: filter.IncludePatternOptions{Includes: []string{"*[._]log[.-][0-9]", "!*[._]log[.-][0-9]"}}},
		env.globalOptions,
	)

	rtest.Equals(t, `Fatal: --include: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())

	// Test --iinclude
	err = testRunRestoreAssumeFailure(
		t,
		"latest",
		restoreOptions{IncludePatternOptions: filter.IncludePatternOptions{InsensitiveIncludes: []string{"*[._]log[.-][0-9]", "!*[._]log[.-][0-9]"}}},
		env.globalOptions,
	)

	rtest.Equals(t, `Fatal: --iinclude: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())
}

func TestRestoreFailsWhenUsingInvalidPatternsFromFile(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testRunInit(t, env.globalOptions)

	// Create an include file with some invalid patterns
	patternsFile := env.base + "/patternsFile"
	fileErr := os.WriteFile(patternsFile, []byte("*.go\n*[._]log[.-][0-9]\n!*[._]log[.-][0-9]"), 0644)
	if fileErr != nil {
		t.Fatalf("Could not write include file: %v", fileErr)
	}

	err := testRunRestoreAssumeFailure(
		t,
		"latest",
		restoreOptions{IncludePatternOptions: filter.IncludePatternOptions{IncludeFiles: []string{patternsFile}}},
		env.globalOptions,
	)
	rtest.Equals(t, `Fatal: --include-file: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())

	err = testRunRestoreAssumeFailure(
		t,
		"latest",
		restoreOptions{ExcludePatternOptions: filter.ExcludePatternOptions{ExcludeFiles: []string{patternsFile}}},
		env.globalOptions,
	)
	rtest.Equals(t, `Fatal: --exclude-file: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())

	err = testRunRestoreAssumeFailure(
		t,
		"latest",
		restoreOptions{IncludePatternOptions: filter.IncludePatternOptions{InsensitiveIncludeFiles: []string{patternsFile}}},
		env.globalOptions,
	)
	rtest.Equals(t, `Fatal: --iinclude-file: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())

	err = testRunRestoreAssumeFailure(
		t,
		"latest",
		restoreOptions{ExcludePatternOptions: filter.ExcludePatternOptions{InsensitiveExcludeFiles: []string{patternsFile}}},
		env.globalOptions,
	)
	rtest.Equals(t, `Fatal: --iexclude-file: invalid pattern(s) provided:
*[._]log[.-][0-9]
!*[._]log[.-][0-9]`, err.Error())
}
