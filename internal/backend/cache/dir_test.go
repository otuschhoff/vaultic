package cache

import (
	"os"
	"testing"

	rtest "github.com/vaultic/vaultic/internal/test"
)

// DefaultDir should honor VAULTIC_CACHE_DIR on all platforms.
func TestCacheDirEnv(t *testing.T) {
	cachedir := os.Getenv("VAULTIC_CACHE_DIR")

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
