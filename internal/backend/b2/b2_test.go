package b2_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/vaultic/vaultic/internal/backend/b2"
	"github.com/vaultic/vaultic/internal/backend/test"
	"github.com/vaultic/vaultic/internal/env"

	rtest "github.com/vaultic/vaultic/internal/test"
)

func newB2TestSuite() *test.Suite[b2.Config] {
	return &test.Suite[b2.Config]{
		// do not use excessive data
		MinimalData: true,

		// wait for at most 10 seconds for removed files to disappear
		WaitForDelayedRemoval: 10 * time.Second,

		// NewConfig returns a config for a new temporary backend that will be used in tests.
		NewConfig: func() (*b2.Config, error) {
			cfg, err := b2.ParseConfig(env.Get("TEST_B2_REPOSITORY"))
			if err != nil {
				return nil, err
			}

			cfg.ApplyEnvironment("VAULTIC_TEST_")
			cfg.ApplyEnvironment("RESTIC_TEST_") // legacy prefix fallback
			cfg.Prefix = fmt.Sprintf("test-%d", time.Now().UnixNano())
			return cfg, nil
		},

		Factory: b2.NewFactory(),
	}
}

func testVars(t testing.TB) {
	vars := []string{
		"VAULTIC_TEST_B2_ACCOUNT_ID",
		"VAULTIC_TEST_B2_ACCOUNT_KEY",
		"VAULTIC_TEST_B2_REPOSITORY",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("environment variable %v not set", v)
			return
		}
	}
}

func TestBackendB2(t *testing.T) {
	defer func() {
		if t.Skipped() {
			rtest.SkipDisallowed(t, "vaultic/backend/b2.TestBackendB2")
		}
	}()

	testVars(t)
	newB2TestSuite().RunTests(t)
}

func BenchmarkBackendb2(t *testing.B) {
	testVars(t)
	newB2TestSuite().RunBenchmarks(t)
}
