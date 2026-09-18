//go:build !linux && !darwin

package telemetry

import "fmt"

func readProtectedTokenFile(path string) ([]byte, error) {
	return nil, fmt.Errorf("protected token files are unsupported on this platform; use an environment variable")
}
