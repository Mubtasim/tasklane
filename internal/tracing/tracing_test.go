package tracing

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestSetupWithoutAnEndpointIsANoOp(t *testing.T) {
	tp, shutdown, err := Setup(t.Context(), env(nil), "svc")
	require.NoError(t, err)
	require.IsType(t, noop.TracerProvider{}, tp)
	require.NoError(t, shutdown(t.Context()))
	require.False(t, Enabled(env(nil)))
}

func TestSetupWithAnEndpointExports(t *testing.T) {
	for _, k := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"} {
		t.Run(k, func(t *testing.T) {
			// Nothing listens on port 1; the exporter only dials on export.
			t.Setenv(k, "http://127.0.0.1:1")
			vars := env(map[string]string{k: "http://127.0.0.1:1"})
			require.True(t, Enabled(vars))
			tp, shutdown, err := Setup(t.Context(), vars, "svc")
			require.NoError(t, err)
			require.IsType(t, &sdktrace.TracerProvider{}, tp)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_ = shutdown(ctx) // may fail to flush to the closed port; must not hang
		})
	}
}

func TestSetupHonoursTheSamplerVariable(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	tp, shutdown, err := Setup(t.Context(), env(map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1"}), "svc")
	require.NoError(t, err)
	defer func() { _ = shutdown(context.Background()) }()
	_, span := tp.Tracer("t").Start(t.Context(), "s")
	require.False(t, span.SpanContext().IsSampled())
	span.End()
}

func TestEnabledHonoursTheStandardSwitches(t *testing.T) {
	const ep = "http://collector:4317"
	for name, tc := range map[string]struct {
		vars map[string]string
		want bool
	}{
		"endpoint only":          {map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": ep}, true},
		"traces endpoint only":   {map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": ep}, true},
		"no endpoint":            {nil, false},
		"sdk disabled":           {map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": ep, "OTEL_SDK_DISABLED": "true"}, false},
		"sdk disabled any case":  {map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": ep, "OTEL_SDK_DISABLED": "TRUE"}, false},
		"sdk disabled false":     {map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": ep, "OTEL_SDK_DISABLED": "false"}, true},
		"exporter none":          {map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": ep, "OTEL_TRACES_EXPORTER": "none"}, false},
		"exporter none any case": {map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": ep, "OTEL_TRACES_EXPORTER": "None"}, false},
		"exporter otlp":          {map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": ep, "OTEL_TRACES_EXPORTER": "otlp"}, true},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, Enabled(env(tc.vars)))
			if !tc.want {
				tp, _, err := Setup(t.Context(), env(tc.vars), "svc")
				require.NoError(t, err)
				require.IsType(t, noop.TracerProvider{}, tp)
			}
		})
	}
}
