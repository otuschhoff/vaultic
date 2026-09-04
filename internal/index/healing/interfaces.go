package healing

import (
	"context"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
)

// GenerationAuthority is the daemon capability consumed by healing decisions.
type GenerationAuthority interface {
	GenerationStatus(context.Context) (daemon.GenerationStatus, error)
	ActivateGeneration(context.Context, uint64, uint64, string, string, time.Duration) (daemon.GenerationStatus, error)
	VerifyGeneration(context.Context, uint64, string) (daemon.GenerationStatus, error)
	RollbackGeneration(context.Context, uint64, string, time.Duration) (daemon.GenerationStatus, error)
	RetireGeneration(context.Context, uint64, uint64, string) (daemon.GenerationStatus, error)
}

// WriterController is the daemon capability used to fence healing candidates.
type WriterController interface {
	WriterStatus(context.Context) (daemon.WriterStatus, error)
	DemoteWriter(context.Context, string, bool, time.Duration) (daemon.WriterStatus, error)
	PromoteWriter(context.Context, string) (daemon.WriterStatus, error)
}

var (
	_ GenerationAuthority = (*daemon.Client)(nil)
	_ WriterController    = (*daemon.Client)(nil)
)
