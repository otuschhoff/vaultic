//go:build windows

package daemon

import "os"

func runtimeUserID() string {
	return "user"
}

func runtimeTempDir() string {
	return os.TempDir()
}
