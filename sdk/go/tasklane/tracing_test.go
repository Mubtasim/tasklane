package tasklane

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestALongHandlerErrorGivesABoundedSpanStatus(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	_, span := tp.Tracer("t").Start(t.Context(), "handle k")
	// 3-byte characters, so a cut at 4096 bytes would fall inside one.
	long := strings.Repeat("€", 5000)
	endHandleSpan(span, &LeasedJob{Attempt: 1, MaxAttempts: 5}, OutcomeNacked, errors.New(long))

	spans := rec.Ended()
	require.Len(t, spans, 1)
	desc := spans[0].Status().Description
	require.LessOrEqual(t, len(desc), maxSpanStatusBytes)
	require.Greater(t, len(desc), maxSpanStatusBytes-4)
	require.True(t, utf8.ValidString(desc))
	require.NotEmpty(t, spans[0].Events(), "RecordError keeps the full error as an event")
}
