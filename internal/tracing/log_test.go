package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestLogHandlerAddsTheTraceID(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(LogHandler{Handler: slog.NewJSONHandler(&buf, nil)}).With("app", "x")
	tid, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	sid, _ := trace.SpanIDFromHex("b7ad6b7169203331")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid}))

	log.InfoContext(ctx, "inside")
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	require.Equal(t, "0af7651916cd43dd8448eb211c80319c", rec["trace_id"])
	require.Equal(t, "b7ad6b7169203331", rec["span_id"])
	require.Equal(t, "x", rec["app"])

	buf.Reset()
	log.Info("outside")
	require.NotContains(t, buf.String(), "trace_id")
}
