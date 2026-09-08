//go:build darwin && cgo

package fsevents

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestNativeSourceReplaysCurrentJournalPosition(t *testing.T) {
	info, err := os.Stat(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("temporary directory has no Darwin stat data")
	}
	source := NativeSource{}
	device := uint64(stat.Dev)
	journalUUID, err := source.JournalUUID(device)
	if err != nil || journalUUID == "" {
		t.Fatalf("journal UUID = %q, %v", journalUUID, err)
	}
	eventID, err := source.CurrentEventID(device)
	if err != nil || eventID == 0 {
		t.Fatalf("current event ID = %d, %v", eventID, err)
	}
	result, err := source.Replay(t.Context(), device, []string{"."}, eventID, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if reason := CoverageFailure(result.Flags, result.HistoryDone); reason != "" {
		t.Fatalf("native replay coverage failure: %s (%#v)", reason, result)
	}
}
