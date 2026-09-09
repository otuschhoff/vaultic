//go:build !rados

package rados

import "context"

const nativeEnabled = false

func openNative(context.Context, Config) (driver, error) {
	return nil, ErrUnsupported
}
