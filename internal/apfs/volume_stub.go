//go:build !darwin

package apfs

import "context"

func resolveVolume(context.Context, Runner, string) (Volume, error) {
	return Volume{}, &Error{Kind: ErrorNotAvailable, Op: "inspect APFS volume", Err: ErrUnsupported}
}

func resolveVolumes(context.Context, Runner, string, bool) ([]Volume, error) {
	return nil, &Error{Kind: ErrorNotAvailable, Op: "inspect APFS volumes", Err: ErrUnsupported}
}
