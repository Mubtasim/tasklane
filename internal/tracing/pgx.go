package tracing

import (
	"context"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/trace"
)

type tracedKey struct{}

// ParentOnly passes a query to next only when the context already carries a
// span: queries made for an RPC nest under its span, while the reaper,
// pruner and depth refresh, which run every second or minute with no job in
// them, create no traces (design §18.1, decision 2).
func ParentOnly(next pgx.QueryTracer) pgx.QueryTracer { return parentOnly{next} }

type parentOnly struct{ next pgx.QueryTracer }

func (p parentOnly) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx
	}
	return context.WithValue(p.next.TraceQueryStart(ctx, conn, data), tracedKey{}, true)
}

func (p parentOnly) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if ctx.Value(tracedKey{}) != nil {
		p.next.TraceQueryEnd(ctx, conn, data)
	}
}
