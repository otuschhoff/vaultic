package errors

import "io"

// CloseQuietly documents cleanup whose failure cannot affect the operation.
func CloseQuietly(closer io.Closer) {
	_ = closer.Close()
}

// LogClose closes a resource and reports a best-effort cleanup failure.
func LogClose(closer io.Closer, description string, logf func(string, ...any)) {
	LogCleanup(description, closer.Close, logf)
}

// LogCleanup runs cleanup and reports a failure through the supplied logger.
func LogCleanup(description string, cleanup func() error, logf func(string, ...any)) {
	if err := cleanup(); err != nil {
		logf("unable to %s: %v", description, err)
	}
}
