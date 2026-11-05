package api

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
)

// BenchmarkEnqueue measures one Enqueue through gRPC and Postgres with
// tracing off (the default) and at 100% sampling into an in-memory recorder.
// Run: go test ./internal/api/ -run '^$' -bench Enqueue -benchtime 2000x -count 6
func BenchmarkEnqueue(b *testing.B) {
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{"off", nil},
		{"sampled", []Option{WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(tracetest.NewSpanRecorder())))}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			e := newEnv(b, nil, tc.opts...)
			ctx := fromCaller(context.Background(), callerTP)
			_, err := e.client.CreateQueue(ctx, &tasklanev1.CreateQueueRequest{Queue: &tasklanev1.Queue{Name: "bench"}})
			if err != nil {
				b.Fatal(err)
			}
			req := &tasklanev1.EnqueueRequest{Queue: "bench", Job: &tasklanev1.JobSpec{Kind: "k"}}
			b.ResetTimer()
			for b.Loop() {
				if _, err := e.client.Enqueue(ctx, req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
