//go:build darwin && cgo

// Package fsevents exposes macOS filesystem event journals for incremental backup planning.
package fsevents

/*
#cgo CFLAGS: -Wno-deprecated-declarations
#cgo LDFLAGS: -framework CoreServices -framework CoreFoundation
#include <stdlib.h>
#include "fsevents_darwin.h"
*/
import "C"

import (
	"context"
	"fmt"
	"runtime/cgo"
	"time"
	"unsafe"
)

type replayCollector struct {
	events []Event
	flags  Flags
}

//export vaulticFSEventsCallback
func vaulticFSEventsCallback(handle C.uintptr_t, path *C.char, eventID C.uint64_t, nativeFlags C.uint32_t) {
	collector := cgo.Handle(handle).Value().(*replayCollector)
	flags := fromNativeFlags(uint32(nativeFlags))
	collector.flags |= flags
	if flags&FlagHistoryDone == 0 && path != nil {
		collector.events = append(collector.events, Event{
			Path: C.GoString(path), ID: uint64(eventID), Flags: flags,
			Recursive: flags&FlagMustScanSubDirs != 0,
		})
	}
}

func currentEventID(device uint64) (uint64, error) {
	value := uint64(C.vaultic_fsevents_current_id(C.uint64_t(device)))
	if value == 0 {
		return 0, fmt.Errorf("FSEvents returned no current event ID for device %d", device)
	}
	return value, nil
}

func journalUUID(device uint64) (string, error) {
	buffer := make([]byte, 64)
	if C.vaultic_fsevents_journal_uuid(C.uint64_t(device), (*C.char)(unsafe.Pointer(&buffer[0])), C.size_t(len(buffer))) == 0 {
		return "", fmt.Errorf("FSEvents journal UUID is unavailable for device %d", device)
	}
	for index, value := range buffer {
		if value == 0 {
			return string(buffer[:index]), nil
		}
	}
	return "", fmt.Errorf("FSEvents journal UUID exceeded the expected size")
}

func replay(ctx context.Context, device uint64, paths []string, since uint64, timeout time.Duration) (ReplayResult, error) {
	cPaths := make([]*C.char, len(paths))
	for index, path := range paths {
		cPaths[index] = C.CString(path)
		defer C.free(unsafe.Pointer(cPaths[index]))
	}
	collector := &replayCollector{}
	handle := cgo.NewHandle(collector)
	defer handle.Delete()
	stream := C.vaultic_fsevents_create(
		C.uint64_t(device), (**C.char)(unsafe.Pointer(&cPaths[0])), C.size_t(len(cPaths)),
		C.uint64_t(since), C.uintptr_t(handle),
	)
	if stream == nil {
		return ReplayResult{}, fmt.Errorf("create FSEvents historical stream")
	}
	defer C.vaultic_fsevents_destroy(stream)

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for collector.flags&FlagHistoryDone == 0 && CoverageFailure(collector.flags|FlagHistoryDone, true) == "" {
		select {
		case <-ctx.Done():
			return ReplayResult{}, context.Cause(ctx)
		case <-deadline.C:
			return ReplayResult{}, fmt.Errorf("FSEvents historical replay timed out after %s", timeout)
		default:
			C.vaultic_fsevents_poll(0.05)
		}
	}
	return ReplayResult{
		Events: collector.events, Flags: collector.flags,
		HistoryDone: collector.flags&FlagHistoryDone != 0,
	}, nil
}

func fromNativeFlags(flags uint32) Flags {
	var result Flags
	mapping := []struct {
		native uint32
		flag   Flags
	}{
		{uint32(C.vaultic_fsevents_flag_must_scan()), FlagMustScanSubDirs},
		{uint32(C.vaultic_fsevents_flag_user_dropped()), FlagUserDropped},
		{uint32(C.vaultic_fsevents_flag_kernel_dropped()), FlagKernelDropped},
		{uint32(C.vaultic_fsevents_flag_ids_wrapped()), FlagEventIDsWrapped},
		{uint32(C.vaultic_fsevents_flag_history_done()), FlagHistoryDone},
		{uint32(C.vaultic_fsevents_flag_root_changed()), FlagRootChanged},
		{uint32(C.vaultic_fsevents_flag_mount()), FlagMount},
		{uint32(C.vaultic_fsevents_flag_unmount()), FlagUnmount},
	}
	for _, entry := range mapping {
		if flags&entry.native != 0 {
			result |= entry.flag
		}
	}
	return result
}
