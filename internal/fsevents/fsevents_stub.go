//go:build !darwin || !cgo

package fsevents

import (
	"context"
	"time"
)

func currentEventID(uint64) (uint64, error) {
	return 0, ErrUnsupported
}

func journalUUID(uint64) (string, error) {
	return "", ErrUnsupported
}

func replay(context.Context, uint64, []string, uint64, time.Duration) (ReplayResult, error) {
	return ReplayResult{}, ErrUnsupported
}
