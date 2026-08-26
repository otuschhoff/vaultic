// Package observability provides opt-in instrumentation shared by commands.
package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// StartCommand starts a command span when enabled. It uses the global OTel
// provider, which is a no-op unless the embedding process configures an SDK or
// exporter; this keeps vaultic dependency-light while making instrumentation
// immediately useful to configured environments.
func StartCommand(ctx context.Context, enabled bool, name string) (context.Context, trace.Span) {
	if !enabled {
		return ctx, trace.SpanFromContext(ctx)
	}
	return otel.Tracer("github.com/otuschhoff/vaultic").Start(ctx, "vaultic."+name)
}
