// Package tracing sets up OpenTelemetry tracing for the server and the demo
// programs from the standard OTEL_* environment variables (design §18.3).
// The Go SDK never imports it: libraries leave exporter setup to the
// application.
package tracing

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Enabled reports whether an OTLP endpoint is configured and tracing is not
// switched off. The standard OTEL_SDK_DISABLED=true and
// OTEL_TRACES_EXPORTER=none win over an endpoint, so an operator can turn
// tracing off without removing the endpoint from a shared environment.
func Enabled(getenv func(string) string) bool {
	if strings.EqualFold(getenv("OTEL_SDK_DISABLED"), "true") || strings.EqualFold(getenv("OTEL_TRACES_EXPORTER"), "none") {
		return false
	}
	return getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// Setup returns a tracer provider and a shutdown function that flushes
// buffered spans. Without an OTLP endpoint the provider is a no-op and costs
// nothing. Otherwise spans go to the endpoint over OTLP/gRPC in batches; the
// exporter, the sampler (OTEL_TRACES_SAMPLER, default parentbased_always_on)
// and OTEL_SERVICE_NAME are read from the environment by the OpenTelemetry
// SDK itself. serviceName is the default when OTEL_SERVICE_NAME is unset.
func Setup(ctx context.Context, getenv func(string) string, serviceName string) (trace.TracerProvider, func(context.Context) error, error) {
	if !Enabled(getenv) {
		return noop.NewTracerProvider(), func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("otlp exporter: %w", err)
	}
	// Later options win: OTEL_SERVICE_NAME overrides the default name.
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", serviceName)),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("tracing resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	return tp, tp.Shutdown, nil
}
