//go:build !windows

package daemon

import (
	"os"
	"strconv"
)

func runtimeUserID() string {
	return strconv.Itoa(os.Geteuid())
}