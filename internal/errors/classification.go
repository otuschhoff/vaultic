package errors

// Transient marks an operation that can succeed when retried.
type Transient struct{ Err error }

func (e *Transient) Error() string { return e.Err.Error() }
func (e *Transient) Unwrap() error { return e.Err }

// Rejected marks input or an operation that was intentionally refused.
type Rejected struct{ Err error }

func (e *Rejected) Error() string { return e.Err.Error() }
func (e *Rejected) Unwrap() error { return e.Err }

// Integrity marks inconsistent, corrupt, or unauthenticated data.
type Integrity struct{ Err error }

func (e *Integrity) Error() string { return e.Err.Error() }
func (e *Integrity) Unwrap() error { return e.Err }

// Unauthorized marks a failed authentication or authorization decision.
type Unauthorized struct{ Err error }

func (e *Unauthorized) Error() string { return e.Err.Error() }
func (e *Unauthorized) Unwrap() error { return e.Err }

// Unavailable marks a required service or capability that is unavailable.
type Unavailable struct{ Err error }

func (e *Unavailable) Error() string { return e.Err.Error() }
func (e *Unavailable) Unwrap() error { return e.Err }

func IsTransient(err error) bool {
	var target *Transient
	return As(err, &target)
}

func IsRejected(err error) bool {
	var target *Rejected
	return As(err, &target)
}

func IsIntegrity(err error) bool {
	var target *Integrity
	return As(err, &target)
}

func IsUnauthorized(err error) bool {
	var target *Unauthorized
	return As(err, &target)
}

func IsUnavailable(err error) bool {
	var target *Unavailable
	return As(err, &target)
}
