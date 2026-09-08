package fsevents

import "testing"

func TestCoverageFailure(t *testing.T) {
	tests := []struct {
		name        string
		flags       Flags
		historyDone bool
		want        string
	}{
		{name: "complete", flags: FlagHistoryDone, historyDone: true},
		{name: "missing-history", historyDone: false, want: "FSEvents historical replay did not complete"},
		{name: "kernel", flags: FlagKernelDropped | FlagHistoryDone, historyDone: true, want: "kFSEventStreamEventFlagKernelDropped"},
		{name: "user", flags: FlagUserDropped | FlagHistoryDone, historyDone: true, want: "kFSEventStreamEventFlagUserDropped"},
		{name: "wrapped", flags: FlagEventIDsWrapped | FlagHistoryDone, historyDone: true, want: "kFSEventStreamEventFlagEventIdsWrapped"},
		{name: "root", flags: FlagRootChanged | FlagHistoryDone, historyDone: true, want: "kFSEventStreamEventFlagRootChanged"},
		{name: "mount", flags: FlagMount | FlagHistoryDone, historyDone: true, want: "kFSEventStreamEventFlagMount"},
		{name: "unmount", flags: FlagUnmount | FlagHistoryDone, historyDone: true, want: "kFSEventStreamEventFlagUnmount"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := CoverageFailure(test.flags, test.historyDone); got != test.want {
				t.Fatalf("coverage failure = %q, want %q", got, test.want)
			}
		})
	}
}
