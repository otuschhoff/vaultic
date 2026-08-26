package test

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

var (
	TestPassword                = getStringVar("VAULTIC_TEST_PASSWORD", "geheim")
	TestCleanupTempDirs         = getBoolVar("VAULTIC_TEST_CLEANUP", true)
	TestTempDir                 = getStringVar("VAULTIC_TEST_TMPDIR", "")
	RunIntegrationTest          = getBoolVar("VAULTIC_TEST_INTEGRATION", true)
	RunFuseTest                 = getBoolVar("VAULTIC_TEST_FUSE", true)
	TestSFTPPath                = getStringVar("VAULTIC_TEST_SFTPPATH", "/usr/lib/ssh:/usr/lib/openssh:/usr/libexec")
	BenchArchiveDirectory       = getStringVar("VAULTIC_BENCH_DIR", ".")
	testIntegrationDisallowSkip = getStringVar("VAULTIC_TEST_DISALLOW_SKIP", "")
)

func getStringVar(name, defaultValue string) string {
	if e := os.Getenv(name); e != "" {
		return e
	}

	return defaultValue
}

func getBoolVar(name string, defaultValue bool) bool {
	if e := os.Getenv(name); e != "" {
		switch e {
		case "1", "true":
			return true
		case "0", "false":
			return false
		default:
			fmt.Fprintf(os.Stderr, "invalid value for variable %q, using default\n", name)
		}
	}

	return defaultValue
}

// SkipDisallowed fails the test if it needs to run. The environment
// variable VAULTIC_TEST_DISALLOW_SKIP contains a comma-separated list of test
// names that must be run. If name is in this list, the test is marked as
// failed.
func SkipDisallowed(t testing.TB, name string) {
	for s := range strings.SplitSeq(testIntegrationDisallowSkip, ",") {
		if s == name {
			t.Fatalf("test %v is in list of tests that need to run ($VAULTIC_TEST_DISALLOW_SKIP)", name)
		}
	}
}
