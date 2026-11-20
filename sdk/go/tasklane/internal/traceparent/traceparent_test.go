package traceparent

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func sc(flags trace.TraceFlags) trace.SpanContext {
	tid, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	sid, _ := trace.SpanIDFromHex("b7ad6b7169203331")
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: flags})
}

func TestFormatAndParseRoundTrip(t *testing.T) {
	for _, flags := range []trace.TraceFlags{0, trace.FlagsSampled} {
		s := Format(sc(flags))
		got, ok := Parse(s)
		require.True(t, ok, s)
		require.Equal(t, sc(flags).TraceID(), got.TraceID())
		require.Equal(t, sc(flags).SpanID(), got.SpanID())
		require.Equal(t, flags.IsSampled(), got.IsSampled())
		require.True(t, got.IsRemote())
	}
	require.Equal(t, "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", Format(sc(trace.FlagsSampled)))
}

func TestFormatOfAnInvalidSpanContextIsEmpty(t *testing.T) {
	require.Empty(t, Format(trace.SpanContext{}))
}

func TestParseRejectsMalformedValues(t *testing.T) {
	for _, s := range []string{"", "garbage", "00-00000000000000000000000000000000-b7ad6b7169203331-01", "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01"} {
		_, ok := Parse(s)
		require.False(t, ok, s)
	}
}
