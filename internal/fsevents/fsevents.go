// Package fsevents exposes macOS filesystem event journals for incremental backup planning.
package fsevents

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrUnsupported = errors.New("FSEvents is unsupported on this platform")

type Flags uint32

const (
	FlagMustScanSubDirs Flags = 1 << iota
	FlagUserDropped
	FlagKernelDropped
	FlagEventIDsWrapped
	FlagHistoryDone
	FlagRootChanged
	FlagMount
	FlagUnmount
)

type Event struct {
	Path      string
	ID        uint64
	Flags     Flags
	Recursive bool
}

type ReplayResult struct {
	Events      []Event
	Flags       Flags
	HistoryDone bool
}

type Source interface {
	CurrentEventID(uint64) (uint64, error)
	JournalUUID(uint64) (string, error)
	Replay(context.Context, uint64, []string, uint64, time.Duration) (ReplayResult, error)
}

type NativeSource struct{}

func (NativeSource) CurrentEventID(device uint64) (uint64, error) {
	return currentEventID(device)
}

func (NativeSource) JournalUUID(device uint64) (string, error) {
	return journalUUID(device)
}

func (NativeSource) Replay(ctx context.Context, device uint64, paths []string, since uint64, timeout time.Duration) (ReplayResult, error) {
	if device == 0 || len(paths) == 0 || since == 0 || timeout <= 0 {
		return ReplayResult{}, fmt.Errorf("invalid FSEvents replay request")
	}
	return replay(ctx, device, paths, since, timeout)
}

func CoverageFailure(flags Flags, historyDone bool) string {
	checks := []struct {
		flag Flags
		name string
	}{
		{FlagKernelDropped, "kFSEventStreamEventFlagKernelDropped"},
		{FlagUserDropped, "kFSEventStreamEventFlagUserDropped"},
		{FlagEventIDsWrapped, "kFSEventStreamEventFlagEventIdsWrapped"},
		{FlagRootChanged, "kFSEventStreamEventFlagRootChanged"},
		{FlagMount, "kFSEventStreamEventFlagMount"},
		{FlagUnmount, "kFSEventStreamEventFlagUnmount"},
	}
	for _, check := range checks {
		if flags&check.flag != 0 {
			return check.name
		}
	}
	if !historyDone || flags&FlagHistoryDone == 0 {
		return "FSEvents historical replay did not complete"
	}
	return ""
}
