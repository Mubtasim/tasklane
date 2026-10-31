// Package demo holds what the demo producer and worker share: settings from
// the environment, dialing the server and creating the demo queue.
package demo

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

// Config comes from TASKLANE_ADDR, TASKLANE_API_KEY, DEMO_QUEUE and DEMO_MAX_DEPTH (0: the server default).
type Config struct {
	Addr     string
	APIKey   string
	Queue    string
	MaxDepth int64
}

func LoadConfig() Config {
	cfg := Config{
		Addr:   Env("TASKLANE_ADDR", "localhost:7070"),
		APIKey: os.Getenv("TASKLANE_API_KEY"),
		Queue:  Env("DEMO_QUEUE", "demo"),
	}
	// A malformed DEMO_MAX_DEPTH keeps the server default.
	cfg.MaxDepth, _ = strconv.ParseInt(os.Getenv("DEMO_MAX_DEPTH"), 10, 64)
	return cfg
}

// Env returns the environment variable key, or fallback when it is unset.
func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func Dial(cfg Config) (*tasklane.Client, error) {
	var opts []tasklane.Option
	if cfg.APIKey != "" {
		opts = append(opts, tasklane.WithAPIKey(cfg.APIKey))
	}
	return tasklane.Dial(cfg.Addr, opts...)
}

// EnsureQueue creates the demo queue: a 15 s visibility timeout, so a killed
// worker's jobs come back within about 20 s, and 5 attempts, so poison jobs
// reach the dead-letter queue quickly. A queue that already exists is fine,
// and while the server is starting it waits and tries again.
// An existing queue keeps its settings, so DEMO_MAX_DEPTH only applies to a new queue.
func EnsureQueue(ctx context.Context, c *tasklane.Client, cfg Config, log *slog.Logger) error {
	for {
		_, err := c.CreateQueue(ctx, tasklane.QueueConfig{Name: cfg.Queue, MaxDepth: cfg.MaxDepth, VisibilityTimeout: 15 * time.Second, MaxAttempts: 5})
		switch {
		case err == nil || status.Code(err) == codes.AlreadyExists:
			return nil
		case ctx.Err() != nil:
			return ctx.Err()
		case status.Code(err) != codes.Unavailable:
			return err
		}
		log.Info("waiting for the server", "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
