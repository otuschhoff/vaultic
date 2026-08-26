package test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/vaultic/vaultic/internal/env"
)

var (
	TestPassword                = getStringVar("TEST_PASSWORD", "geheim")
	TestCleanupTempDirs         = getBoolVar("TEST_CLEANUP", true)
	TestTempDir                 = getStringVar("TEST_TMPDIR", "")
	RunIntegrationTest          = getBoolVar("TEST_INTEGRATION", true)
	RunFuseTest                 = getBoolVar("TEST_FUSE", true)
	TestSFTPPath                = getStringVar("TEST_SFTPPATH", "/usr/lib/ssh:/usr/lib/openssh:/usr/libexec")
	BenchArchiveDirectory       = getStringVar("BENCH_DIR", ".")
	testIntegrationDisallowSkip = getStringVar("TEST_DISALLOW_SKIP", "")
)

// getStringVar reads VAULTIC_<name>, falling back to the legacy RESTIC_<name>.
func getStringVar(name, defaultValue string) string {
	if e := env.Get(name); e != "" {
		return e
	}

	return defaultValue
}

// getBoolVar reads VAULTIC_<name>, falling back to the legacy RESTIC_<name>.
func getBoolVar(name string, defaultValue bool) bool {
	if e := env.Get(name); e != "" {
		switch e {
		case "1", "true":
			return true
		case "0", "false":
			return false
		default:
			fmt.Fprintf(os.Stderr, "invalid value for variable %q, using default\n", env.PrimaryPrefix+name)
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
