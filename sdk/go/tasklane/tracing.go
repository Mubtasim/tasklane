package tasklane

import (
	"context"
	"errors"
	"path"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/stats"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/traceparent"
)

func notHeartbeat(info *stats.RPCTagInfo) bool {
	return path.Base(info.FullMethodName) != "Heartbeat"
}

// startHandleSpan starts the run's span as a child of the traceparent stored
// with the job, so producer, server and every attempt form one trace. A
// missing or malformed traceparent starts a new trace.
func (w *Worker) startHandleSpan(pj *tasklanev1.Job) (context.Context, trace.Span) {
	ctx := context.Background()
	if parent, ok := traceparent.Parse(pj.GetTraceContext()); ok {
		ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
	}
	return w.client.tracer.Start(ctx, "handle "+pj.GetKind(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "tasklane"),
			attribute.String("messaging.destination.name", w.queue),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.message.id", strconv.FormatInt(pj.GetId(), 10)),
			attribute.String("tasklane.kind", pj.GetKind()),
			attribute.Int("tasklane.attempt", int(pj.GetAttempts())),
			attribute.Int("tasklane.priority", int(pj.GetPriority())),
		))
}

// endHandleSpan sets the span's status from the run's outcome (design §18.2)
// and ends it.
func endHandleSpan(span trace.Span, job *LeasedJob, o Outcome, err error) {
	switch o {
	case OutcomeAcked:
		span.SetStatus(otelcodes.Ok, "")
	case OutcomeNacked:
		switch {
		case errors.Is(err, errShutdown):
			span.SetStatus(otelcodes.Error, "shutdown")
		case err != nil:
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
		}
		// NackResponse carries nothing, but a Nack on the last attempt
		// always buries the job; the server's Nack span has the exact value.
		if job.MaxAttempts > 0 && job.Attempt >= job.MaxAttempts {
			span.SetAttributes(attribute.Bool("tasklane.dead", true))
		}
	case OutcomeLeaseLost:
		span.SetStatus(otelcodes.Error, "lease lost")
	case OutcomeGaveUp:
		span.SetStatus(otelcodes.Error, "gave up")
	}
	span.End()
}
