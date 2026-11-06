// Command tasklane runs the Tasklane server: the gRPC API, the HTTP/JSON
// gateway with health endpoints, the LISTEN/NOTIFY listener, the reaper and the pruner.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/mubtasim/tasklane/internal/api"
	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/metrics"
	"github.com/mubtasim/tasklane/internal/notify"
	"github.com/mubtasim/tasklane/internal/pgstore"
	"github.com/mubtasim/tasklane/internal/pruner"
	"github.com/mubtasim/tasklane/internal/reaper"
	"github.com/mubtasim/tasklane/internal/tracing"
)

// Each drain step on SIGINT/SIGTERM gets its own budget, so a slow HTTP
// drain can't eat the gRPC drain's time.
const (
	httpShutdownTimeout = 10 * time.Second
	grpcShutdownTimeout = 10 * time.Second
	// A scrape is short, and the last scrape during a drain is useful.
	metricsShutdownTimeout = 2 * time.Second
	// traceFlushTimeout bounds the final export of buffered spans.
	traceFlushTimeout = 2 * time.Second
)

func main() {
	log := slog.New(tracing.LogHandler{Handler: slog.NewJSONHandler(os.Stdout, nil)})
	if err := run(log); err != nil {
		log.Error("tasklane stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := LoadConfig(os.Getenv)
	if err != nil {
		return err
	}
	tp, flushTraces, err := tracing.Setup(context.Background(), os.Getenv, "tasklane-server")
	if err != nil {
		return err
	}
	// Deferred first, so it runs last: after the servers drained, the spans
	// of the last requests are still sent. A collector that can't be reached
	// costs at most traceFlushTimeout; exit never waits longer.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), traceFlushTimeout)
		defer cancel()
		if err := flushTraces(ctx); err != nil {
			log.Warn("flushing traces failed", "err", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database config: %w", err)
	}
	// JIT compilation pays off only for long analytic queries; for Tasklane's
	// short statements it was 243 ms of one 710 ms depth query (the query's
	// mean was 610 ms), so it is off unless the URL sets it. The setting is
	// sent as a connection startup parameter, so it overrides `ALTER ROLE` and
	// `ALTER DATABASE` settings of jit; to keep JIT, put `jit=on` in
	// TASKLANE_DATABASE_URL (for example `?jit=on`).
	if _, ok := poolCfg.ConnConfig.RuntimeParams["jit"]; !ok {
		poolCfg.ConnConfig.RuntimeParams["jit"] = "off"
	}
	poolCfg.ConnConfig.Tracer = tracing.ParentOnly(otelpgx.NewTracer(otelpgx.WithTracerProvider(tp)))
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("database config: %w", err)
	}
	defer pool.Close()
	m := metrics.New()
	m.WatchPool(pool.Stat)
	if err := pgstore.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	st := pgstore.New(pool, backoff.Default())

	// Background loops get their own context: they stop after the servers
	// have drained, not the moment the signal arrives. This deferred cleanup
	// runs before pool.Close (defers run last-in first-out), so on every
	// return path the loops are stopped and waited for while the pool is
	// still open.
	bgCtx, stopBackground := context.WithCancel(context.Background())
	var background sync.WaitGroup
	defer func() {
		stopBackground()
		background.Wait()
	}()
	listener, err := notify.New(cfg.DatabaseURL, log)
	if err != nil {
		return err
	}
	background.Go(func() { _ = listener.Run(bgCtx) }) // always nil once bgCtx is done
	background.Go(func() { reaper.Run(bgCtx, st, cfg.ReaperInterval, reaper.DefaultBatch, log, m) })
	background.Go(func() {
		pruner.Run(bgCtx, st, pruner.Config{Interval: cfg.PruneInterval, Retention: cfg.Retention, DeadRetention: cfg.DeadRetention, Metrics: m}, log)
	})

	// The publisher sends NOTIFY through the pool, so it is closed before the
	// pool is: this defer runs before the pool.Close deferred above. It runs
	// after the servers have drained, and a detached write that finishes
	// later only wakes this server's own leases.
	publisher := notify.NewPublisher(listener, pool, notify.DefaultCoalesceWindow, log)
	defer publisher.Close()
	srv := api.NewServer(st, publisher, log, api.WithMetrics(m), api.WithTracerProvider(tp))
	background.Go(func() { srv.RunDepthRefresh(bgCtx) })
	grpcServer := api.NewGRPCServer(srv, cfg.APIKeys, log)
	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}

	target, err := dialAddr(cfg.GRPCAddr)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(target, api.GatewayDialOptions(tp)...)
	if err != nil {
		return err
	}
	defer conn.Close()
	var shuttingDown atomic.Bool
	handler, err := api.NewHTTPHandler(bgCtx, conn, readyFunc(&shuttingDown, pool.Ping), cfg.APIKeys, tp)
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
		// No WriteTimeout: it runs from the end of the request headers, so it
		// would cut off long-polling leases (wait up to 30 s) mid-response.
	}

	// Metrics get their own port (design §16.2), so operators can expose the
	// API without exposing traffic numbers.
	metricsServer := newMetricsServer(cfg, m)
	var metricsLis net.Listener
	if metricsServer != nil {
		metricsLis, err = net.Listen("tcp", cfg.MetricsAddr)
		if err != nil {
			return err
		}
	}

	errc := make(chan error, 3)
	go func() { errc <- grpcServer.Serve(grpcLis) }()
	go func() {
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	if metricsServer != nil {
		go func() {
			if err := metricsServer.Serve(metricsLis); !errors.Is(err, http.ErrServerClosed) {
				errc <- err
			}
		}()
	}
	log.Info("tasklane started", "grpc_addr", cfg.GRPCAddr, "http_addr", cfg.HTTPAddr, "metrics_addr", cfg.MetricsAddr)

	var serveErr error
	select {
	case <-ctx.Done():
		stop() // restore default signal handling: a second Ctrl-C exits at once
		log.Info("shutting down")
	case serveErr = <-errc:
		log.Error("server failed; shutting down", "err", serveErr)
	}

	// Drain: report not ready, end long polls (HTTP leases are gRPC calls
	// underneath), stop taking HTTP requests, let in-flight RPCs finish, and
	// force-close whatever is left after each step's timeout.
	shuttingDown.Store(true)
	srv.Shutdown()
	httpCtx, cancelHTTP := context.WithTimeout(context.Background(), httpShutdownTimeout)
	if err := httpServer.Shutdown(httpCtx); err != nil {
		log.Warn("http shutdown timed out; closing remaining connections", "err", err)
		httpServer.Close()
	}
	cancelHTTP()
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(grpcShutdownTimeout):
		log.Warn("graceful stop timed out; closing remaining connections")
		grpcServer.Stop()
	}
	if metricsServer != nil {
		metricsCtx, cancelMetrics := context.WithTimeout(context.Background(), metricsShutdownTimeout)
		if err := metricsServer.Shutdown(metricsCtx); err != nil {
			log.Warn("metrics shutdown timed out; closing remaining connections", "err", err)
			metricsServer.Close()
		}
		cancelMetrics()
	}
	stopBackground()
	background.Wait()
	if serveErr == nil {
		log.Info("tasklane stopped cleanly")
	}
	return serveErr
}

// newMetricsServer returns the server for /metrics, or nil when metrics are
// switched off (an empty MetricsAddr).
func newMetricsServer(cfg Config, m *metrics.Metrics) *http.Server {
	if cfg.MetricsAddr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	return &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

var errShuttingDown = errors.New("shutting down")

// readyFunc reports ready while not shutting down and ping succeeds, so load
// balancers stop routing to a draining server.
func readyFunc(shuttingDown *atomic.Bool, ping func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		if shuttingDown.Load() {
			return errShuttingDown
		}
		return ping(ctx)
	}
}
