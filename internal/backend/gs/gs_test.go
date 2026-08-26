package gs_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/vaultic/vaultic/internal/backend/gs"
	"github.com/vaultic/vaultic/internal/backend/test"
	rtest "github.com/vaultic/vaultic/internal/test"
)

func newGSTestSuite() *test.Suite[gs.Config] {
	return &test.Suite[gs.Config]{
		// do not use excessive data
		MinimalData: true,

		// NewConfig returns a config for a new temporary backend that will be used in tests.
		NewConfig: func() (*gs.Config, error) {
			cfg, err := gs.ParseConfig(os.Getenv("VAULTIC_TEST_GS_REPOSITORY"))
			if err != nil {
				return nil, err
			}

			cfg.ProjectID = os.Getenv("VAULTIC_TEST_GS_PROJECT_ID")
			cfg.Prefix = fmt.Sprintf("test-%d", time.Now().UnixNano())
			return cfg, nil
		},

		Factory: gs.NewFactory(),
	}
}

func TestBackendGS(t *testing.T) {
	defer func() {
		if t.Skipped() {
			rtest.SkipDisallowed(t, "vaultic/backend/gs.TestBackendGS")
		}
	}()

	vars := []string{
		"VAULTIC_TEST_GS_PROJECT_ID",
		"VAULTIC_TEST_GS_REPOSITORY",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("environment variable %v not set", v)
			return
		}
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")+os.Getenv("GOOGLE_ACCESS_TOKEN") == "" {
		t.Skipf("environment variable GOOGLE_APPLICATION_CREDENTIALS not set, nor GOOGLE_ACCESS_TOKEN")
		return
	}

	t.Logf("run tests")
	newGSTestSuite().RunTests(t)
}

func BenchmarkBackendGS(t *testing.B) {
	vars := []string{
		"VAULTIC_TEST_GS_PROJECT_ID",
		"VAULTIC_TEST_GS_REPOSITORY",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("environment variable %v not set", v)
			return
		}
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")+os.Getenv("GOOGLE_ACCESS_TOKEN") == "" {
		t.Skipf("environment variable GOOGLE_APPLICATION_CREDENTIALS not set, nor GOOGLE_ACCESS_TOKEN")
		return
	}

	t.Logf("run tests")
	newGSTestSuite().RunBenchmarks(t)
}
