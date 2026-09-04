package main

import (
	"context"
	"errors"
	"testing"

	"github.com/otuschhoff/vaultic/cmd/vaultic/indexcmd"
	vaulticerrors "github.com/otuschhoff/vaultic/internal/errors"
)

func TestIndexPartialResultsUseWarningSentinels(t *testing.T) {
	for _, err := range []error{indexcmd.ErrDifferences, indexcmd.ErrIncomplete} {
		if code := exitCodeForError(err); code != 2 {
			t.Fatalf("index exit code = %d, want 2", code)
		}
	}
}

func TestClassifiedErrorExitCodes(t *testing.T) {
	cause := errors.New("classified")
	tests := []struct {
		err  error
		code int
	}{
		{&vaulticerrors.Transient{Err: cause}, 1},
		{&vaulticerrors.Rejected{Err: cause}, 1},
		{&vaulticerrors.Unavailable{Err: cause}, 1},
		{&vaulticerrors.Integrity{Err: cause}, 2},
		{&vaulticerrors.Unauthorized{Err: cause}, 12},
		{context.Canceled, 130},
	}
	for _, test := range tests {
		if got := exitCodeForError(test.err); got != test.code {
			t.Errorf("exitCodeForError(%T) = %d, want %d", test.err, got, test.code)
		}
	}
}
