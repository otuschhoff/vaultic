package cache

import (
	"os"
	"testing"

	"github.com/otuschhoff/vaultic/internal/env"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

// DefaultDir should honor VAULTIC_CACHE_DIR on all platforms.
func TestCacheDirEnv(t *testing.T) {
	cachedir := env.Get("CACHE_DIR")

	if cachedir == "" {
		cachedir = "/doesnt/exist"
		err := os.Setenv("VAULTIC_CACHE_DIR", cachedir)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err := os.Unsetenv("VAULTIC_CACHE_DIR")
			if err != nil {
				t.Fatal(err)
			}
		}()
	}

	dir, err := DefaultDir()
	rtest.Equals(t, cachedir, dir)
	rtest.OK(t, err)
}
