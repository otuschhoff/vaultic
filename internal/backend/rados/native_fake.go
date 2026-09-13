//go:build radosfake && !rados

package rados

import "context"

func openNative(context.Context, Config) (driver, error) {
	return nil, ErrUnsupported
}
