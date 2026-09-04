package errors

import (
	stderrors "errors"
	"fmt"
	"testing"
)

type failingCloser struct{ err error }

func (closer failingCloser) Close() error { return closer.err }

func TestLogCloseReportsFailure(t *testing.T) {
	var message string
	LogClose(failingCloser{err: stderrors.New("close failed")}, "close fixture", func(format string, args ...any) {
		message = fmt.Sprintf(format, args...)
	})
	if message != "unable to close fixture: close failed" {
		t.Fatalf("cleanup log = %q", message)
	}
}
