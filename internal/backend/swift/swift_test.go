package swift_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/vaultic/vaultic/internal/backend"
	"github.com/vaultic/vaultic/internal/backend/swift"
	"github.com/vaultic/vaultic/internal/backend/test"
	"github.com/vaultic/vaultic/internal/env"
	rtest "github.com/vaultic/vaultic/internal/test"
)

func newSwiftTestSuite(t testing.TB) *test.Suite[swift.Config] {
	return &test.Suite[swift.Config]{
		// do not use excessive data
		MinimalData: true,

		// wait for removals for at least 5m
		WaitForDelayedRemoval: 5 * time.Minute,

		ErrorHandler: func(t testing.TB, be backend.Backend, err error) error {
			if err == nil {
				return nil
			}

			if be.IsNotExist(err) {
				t.Logf("swift: ignoring error %v", err)
				return nil
			}

			return err
		},

		// NewConfig returns a config for a new temporary backend that will be used in tests.
		NewConfig: func() (*swift.Config, error) {
			cfg, err := swift.ParseConfig(env.Get("TEST_SWIFT"))
			if err != nil {
				return nil, err
			}

			cfg.ApplyEnvironment("VAULTIC_TEST_")
			cfg.ApplyEnvironment("RESTIC_TEST_") // legacy prefix fallback
			cfg.Prefix += fmt.Sprintf("/test-%d", time.Now().UnixNano())
			t.Logf("using prefix %v", cfg.Prefix)
			return cfg, nil
		},

		Factory: swift.NewFactory(),
	}
}

func TestBackendSwift(t *testing.T) {
	defer func() {
		if t.Skipped() {
			rtest.SkipDisallowed(t, "vaultic/backend/swift.TestBackendSwift")
		}
	}()

	if env.Get("TEST_SWIFT") == "" {
		t.Skip("VAULTIC_TEST_SWIFT unset, skipping test")
		return
	}

	t.Logf("run tests")
	newSwiftTestSuite(t).RunTests(t)
}

func BenchmarkBackendSwift(t *testing.B) {
	if env.Get("TEST_SWIFT") == "" {
		t.Skip("VAULTIC_TEST_SWIFT unset, skipping test")
		return
	}

	t.Logf("run tests")
	newSwiftTestSuite(t).RunBenchmarks(t)
}
