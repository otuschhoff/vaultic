package snapshotfs

import "errors"

var (
	ErrCanceled    = errors.New("snapshot filesystem operation canceled")
	ErrClosed      = errors.New("snapshot filesystem is closed")
	ErrCollision   = errors.New("snapshot tree name collision")
	ErrInvalidName = errors.New("invalid snapshot path component")
	ErrNameTooLong = errors.New("snapshot path component is too long")
	ErrNotFound    = errors.New("snapshot node not found")
	ErrInvalidNode = errors.New("invalid snapshot node")
	ErrRepository  = errors.New("snapshot repository error")
)
