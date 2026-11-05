// Package traceparent converts between OpenTelemetry span contexts and the
// W3C traceparent string Tasklane stores with each job (design §18). It uses
// only the OpenTelemetry API, so the Go SDK can import it without linking an
// exporter into its users' programs.
package traceparent

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// W3C is the only propagation format Tasklane speaks. The server and the SDK
// use it explicitly instead of the global propagator: the stored field is a
// W3C traceparent, and the global propagator is a no-op unless the
// application sets one, which would break traces without any error.
var W3C propagation.TextMapPropagator = propagation.TraceContext{}

const key = "traceparent"

// Format returns sc as a traceparent, or "" when sc is invalid. Trace state
// is not kept: the column holds the traceparent only.
func Format(sc trace.SpanContext) string {
	if !sc.IsValid() {
		return ""
	}
	c := propagation.MapCarrier{}
	W3C.Inject(trace.ContextWithSpanContext(context.Background(), sc), c)
	return c.Get(key)
}

// Parse returns the remote span context in a stored traceparent. ok is false
// for an empty or malformed value.
func Parse(s string) (trace.SpanContext, bool) {
	if s == "" {
		return trace.SpanContext{}, false
	}
	ctx := W3C.Extract(context.Background(), propagation.MapCarrier{key: s})
	sc := trace.SpanContextFromContext(ctx)
	return sc, sc.IsValid()
}
