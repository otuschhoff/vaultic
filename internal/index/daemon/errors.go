package daemon

import (
	"context"
	"errors"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const errorDetailTypeURL = "type.googleapis.com/vaulticdb.v1.ErrorDetail"

var (
	ErrUnavailable         = errors.New("vaulticdb unavailable")
	ErrWriterFenced        = errors.New("vaulticdb writer fenced")
	ErrWriterDemoted       = errors.New("vaulticdb writer demoted")
	ErrWriterTransitioning = errors.New("vaulticdb writer transitioning")
	ErrGenerationChanged   = errors.New("vaulticdb generation changed")
	ErrGenerationPending   = errors.New("vaulticdb generation reconciliation pending")
	ErrNamespaceMismatch   = errors.New("vaulticdb namespace mismatch")
	ErrEncryptionIntegrity = errors.New("vaulticdb encryption integrity failure")
	ErrIdempotencyConflict = errors.New("vaulticdb idempotency conflict")
	ErrStorageUnavailable  = errors.New("vaulticdb storage unavailable")
	ErrStorageConflict     = errors.New("vaulticdb storage conflict")
	ErrStorageDataLoss     = errors.New("vaulticdb storage data loss")
	ErrInvalidRequest      = errors.New("vaulticdb invalid request")
	ErrPrecondition        = errors.New("vaulticdb precondition failed")
	ErrDeadlineExceeded    = errors.New("vaulticdb request deadline exceeded")
	ErrResourceExhausted   = errors.New("vaulticdb resource exhausted")
	ErrNotFound            = errors.New("vaulticdb object not found")
	ErrKeyManagement       = errors.New("vaulticdb key management failure")
	ErrWriterRole          = errors.New("vaulticdb writer role failure")
	ErrAuthentication      = errors.New("vaulticdb authentication failed")
	ErrAuthorization       = errors.New("vaulticdb authorization failed")
	ErrIncompatibleDaemon  = errors.New("incompatible vaulticdb daemon")
	ErrRepositoryMismatch  = errors.New("vaulticdb repository mismatch")
	ErrUnsafeEndpoint      = errors.New("unsafe vaulticdb endpoint")
)

// RPCError preserves the transport status while exposing a stable daemon error.
type RPCError struct {
	detail *vaulticdbv1.ErrorDetail
	cause  error
	kind   error
}

func (e *RPCError) Error() string                    { return e.cause.Error() }
func (e *RPCError) Unwrap() []error                  { return []error{e.kind, e.cause} }
func (e *RPCError) GRPCStatus() *status.Status       { return status.Convert(e.cause) }
func (e *RPCError) Detail() *vaulticdbv1.ErrorDetail { return e.detail }

func daemonErrorKind(code string) error {
	switch code {
	case "writer_fenced":
		return ErrWriterFenced
	case "writer_demoted":
		return ErrWriterDemoted
	case "writer_transitioning":
		return ErrWriterTransitioning
	case "generation_changed":
		return ErrGenerationChanged
	case "generation_reconciliation_pending":
		return ErrGenerationPending
	case "namespace_mismatch":
		return ErrNamespaceMismatch
	case "encryption_integrity":
		return ErrEncryptionIntegrity
	case "idempotency_conflict":
		return ErrIdempotencyConflict
	case "storage_unavailable":
		return ErrStorageUnavailable
	case "storage_conflict":
		return ErrStorageConflict
	case "storage_data_loss":
		return ErrStorageDataLoss
	case "invalid_request":
		return ErrInvalidRequest
	case "precondition_failed":
		return ErrPrecondition
	case "deadline_exceeded":
		return ErrDeadlineExceeded
	case "resource_exhausted":
		return ErrResourceExhausted
	case "not_found":
		return ErrNotFound
	case "key_management":
		return ErrKeyManagement
	case "writer_role":
		return ErrWriterRole
	case "authentication_failed":
		return ErrAuthentication
	case "authorization_failed":
		return ErrAuthorization
	default:
		return nil
	}
}

func classifyRPCError(err error) error {
	if err == nil {
		return nil
	}
	rpcStatus, ok := status.FromError(err)
	if !ok {
		return err
	}
	for _, rawDetail := range rpcStatus.Proto().GetDetails() {
		if rawDetail.GetTypeUrl() != errorDetailTypeURL {
			continue
		}
		detail := new(vaulticdbv1.ErrorDetail)
		if unmarshalErr := proto.Unmarshal(rawDetail.GetValue(), detail); unmarshalErr != nil {
			continue
		}
		kind := daemonErrorKind(detail.GetCode())
		if kind != nil {
			return &RPCError{detail: detail, cause: err, kind: kind}
		}
	}
	return err
}

func classifyUnaryClientError(
	ctx context.Context, method string, request, response any, connection *grpc.ClientConn,
	invoker grpc.UnaryInvoker, options ...grpc.CallOption,
) error {
	return classifyRPCError(invoker(ctx, method, request, response, connection, options...))
}
