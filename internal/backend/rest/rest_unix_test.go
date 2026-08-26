//go:build !windows

package rest_test

import (
	"fmt"
	"path"
	"testing"

	rtest "github.com/vaultic/vaultic/internal/test"
)

func TestBackendRESTWithUnixSocket(t *testing.T) {
	defer func() {
		if t.Skipped() {
			rtest.SkipDisallowed(t, "vaultic/backend/rest.TestBackendREST")
		}
	}()

	ctx := t.Context()

	dir := rtest.TempDir(t)
	serverURL, cleanup := runRESTServer(ctx, t, path.Join(dir, "data"), fmt.Sprintf("unix:%s", path.Join(dir, "sock")))
	defer cleanup()

	newTestSuite(serverURL, false).RunTests(t)
}
