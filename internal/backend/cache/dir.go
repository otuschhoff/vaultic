package cache

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/vaultic/vaultic/internal/env"
)

// EnvDir returns the cache directory configured via the environment
// ($VAULTIC_CACHE_DIR, legacy fallback $RESTIC_CACHE_DIR).
func EnvDir() string {
	return env.Get("CACHE_DIR")
}

// DefaultDir returns $VAULTIC_CACHE_DIR, or the default cache directory
// for the current OS if that variable is not set.
func DefaultDir() (cachedir string, err error) {
	cachedir = EnvDir()
	if cachedir != "" {
		return cachedir, nil
	}

	cachedir, err = os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("unable to locate cache directory: %v", err)
	}

	return filepath.Join(cachedir, "vaultic"), nil
}
