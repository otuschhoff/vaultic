package backend

import (
	"context"
	"fmt"
	"hash"
	"io"
)

var ErrNoRepository = fmt.Errorf("repository does not exist")
var ErrConditionalWriteUnsupported = fmt.Errorf("conditional write is not supported")

// Backend is used to store and access data.
//
// Backend operations that return an error will be retried when a Backend is
// wrapped in a RetryBackend. To prevent that from happening, the operations
// should return a github.com/cenkalti/backoff/v4.PermanentError. Errors from
// the context package need not be wrapped, as context cancellation is checked
// separately by the retrying logic.
type Backend interface {
	// Properties returns information about the backend
	Properties() Properties

	// Hasher may return a hash function for calculating a content hash for the backend
	Hasher() hash.Hash

	// Remove removes a File described by h.
	Remove(ctx context.Context, h Handle) error

	// Close the backend
	Close() error

	// Save stores the data from rd under the given handle.
	Save(ctx context.Context, h Handle, rd RewindReader) error

	// Load runs fn with a reader that yields the contents of the file at h at the
	// given offset. If length is larger than zero, only a portion of the file
	// is read. If the length is larger than zero and the file is too short to return
	// the requested length bytes, then an error MUST be returned that is recognized
	// by IsPermanentError().
	//
	// The function fn may be called multiple times during the same Load invocation
	// and therefore must be idempotent.
	//
	// Implementations are encouraged to use util.DefaultLoad
	Load(ctx context.Context, h Handle, length int, offset int64, fn func(rd io.Reader) error) error

	// Stat returns information about the File identified by h.
	Stat(ctx context.Context, h Handle) (FileInfo, error)

	// List runs fn for each file in the backend which has the type t. When an
	// error occurs (or fn returns an error), List stops and returns it.
	//
	// The function fn is called exactly once for each file during successful
	// execution and at most once in case of an error.
	//
	// The function fn is called in the same Goroutine that List() is called
	// from.
	List(ctx context.Context, t FileType, fn func(FileInfo) error) error

	// IsNotExist returns true if the error was caused by a non-existing file
	// in the backend.
	//
	// The argument may be a wrapped error. The implementation is responsible
	// for unwrapping it.
	IsNotExist(err error) bool

	// IsPermanentError returns true if the error can very likely not be resolved
	// by retrying the operation. Backends should return true if the file is missing,
	// the requested range does not (completely) exist in the file or the user is
	// not authorized to perform the requested operation.
	IsPermanentError(err error) bool

	// Delete removes all data in the backend.
	Delete(ctx context.Context) error

	// Warmup ensures that the specified handles are ready for upcoming reads.
	// This is particularly useful for transitioning files from cold to hot
	// storage.
	//
	// The method is non-blocking. WarmupWait can be used to wait for
	// completion.
	//
	// Returns:
	// - Handles currently warming up.
	// - An error if warmup fails.
	Warmup(ctx context.Context, h []Handle) ([]Handle, error)

	// WarmupWait waits until all given handles are warm.
	WarmupWait(ctx context.Context, h []Handle) error
}

type Properties struct {
	// Connections states the maximum number of concurrent backend operations.
	Connections uint

	// HasAtomicReplace states whether Save() can atomically replace files
	HasAtomicReplace bool

	// HasFlakyErrors states whether the backend may temporarily return errors
	// that are considered as permanent for existing files.
	HasFlakyErrors bool

	StorageProfile *StorageProfile
}

type StorageProfile struct {
	Provider           string
	EndpointHost       string
	Region             string
	Bucket             string
	PrefixSHA256       string
	BucketLookup       string
	SignatureV4        bool
	MultipartUpload    bool
	RangeReads         bool
	ListObjectsV2      bool
	ConditionalCreate  string
	VersionRetention   string
	ObjectImmutability string
	STSRoleAssumption  string
	GlacierRestore     bool
}

type StorageCapabilityProber interface {
	ProbeStorageCapabilities(ctx context.Context) (*StorageProfile, error)
}

// ReadAuthorization is an optional capability for backends whose read
// authorization can change at runtime (for example, renewable credential
// wrappers). Returning false must fail closed for read-cache hits that rely on
// this backend as authoritative source eligibility.
type ReadAuthorization interface {
	ReadAuthorizedNow() bool
}

// ReclamationStatus reports whether physical storage associated with a
// logically removed handle is still awaiting backend garbage collection.
type ReclamationStatus interface {
	ReclamationPending(ctx context.Context, handle Handle) (bool, error)
}

type Unwrapper interface {
	// Unwrap returns the underlying backend or nil if there is none.
	Unwrap() Backend
}

func AsBackend[B Backend](b Backend) B {
	for b != nil {
		if be, ok := b.(B); ok {
			return be
		}

		if be, ok := b.(Unwrapper); ok {
			b = be.Unwrap()
		} else {
			// not the backend we're looking for
			break
		}
	}
	var be B
	return be
}

// AsCapability unwraps nested backends to locate an optional capability interface.
func AsCapability[C any](b Backend) C {
	for b != nil {
		if capability, ok := any(b).(C); ok {
			return capability
		}
		if wrapped, ok := b.(Unwrapper); ok {
			b = wrapped.Unwrap()
			continue
		}
		break
	}
	var zero C
	return zero
}

type FreezeBackend interface {
	Backend
	// Freeze blocks all backend operations except those on lock files
	Freeze()
	// Unfreeze allows all backend operations to continue
	Unfreeze()
}

// FileInfo is contains information about a file in the backend.
type FileInfo struct {
	Size int64
	Name string
}

// ApplyEnvironmenter fills in a backend configuration from the environment
type ApplyEnvironmenter interface {
	ApplyEnvironment(prefix string)
}

// ConditionalWriter exposes an optional, backend-native compare-and-swap primitive.
//
// expected:
// - nil: update only when the target does not exist.
// - non-nil: update only when the current bytes are identical to expected.
//
// Returns the currently stored bytes when swapped is false and the object exists.
// For a create-if-missing miss, current is nil and swapped is false.
type ConditionalWriter interface {
	CompareAndSwap(ctx context.Context, h Handle, expected []byte, replacement []byte) (current []byte, swapped bool, err error)
}

// CapacityTelemetrySample describes optional backend-native capacity telemetry
// for budget controllers.
type CapacityTelemetrySample struct {
	TotalRawBytes           uint64
	FreeRawBytes            uint64
	EligibleTotalRawBytes   uint64
	EligibleFreeRawBytes    uint64
	PoolMaxAvailRawBytes    uint64
	PoolQuotaRawBytes       uint64
	PoolMaxAvailBytes       uint64
	PoolMaxAvailKnown       bool
	PoolQuotaAvailableBytes uint64
	PoolQuotaKnown          bool
	ObjectHeadroomRawBytes  uint64
	RawAmplification        float64
	Health                  string
	SourceGeneration        uint64
	Denied                  bool
	Inconsistent            bool
	PoolReplicaSize         uint64
	PoolReplicaSizeKnown    bool
	PoolMinSize             uint64
	PoolMinSizeKnown        bool
}

// CapacityTelemetry exposes optional backend-native storage capacity facts.
type CapacityTelemetry interface {
	SampleCapacity(ctx context.Context) (CapacityTelemetrySample, error)
}
