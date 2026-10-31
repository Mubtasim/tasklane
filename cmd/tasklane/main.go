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

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/mubtasim/tasklane/internal/api"
	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/notify"
	"github.com/mubtasim/tasklane/internal/pgstore"
	"github.com/mubtasim/tasklane/internal/pruner"
	"github.com/mubtasim/tasklane/internal/reaper"
)

// Each drain step on SIGINT/SIGTERM gets its own budget, so a slow HTTP
// drain can't eat the gRPC drain's time.
const (
	httpShutdownTimeout = 10 * time.Second
	grpcShutdownTimeout = 10 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database config: %w", err)
	}
	defer pool.Close()
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
	listener := notify.New(cfg.DatabaseURL, log)
	background.Go(func() { listener.Run(bgCtx) })
	background.Go(func() { reaper.Run(bgCtx, st, cfg.ReaperInterval, reaper.DefaultBatch, log) })
	background.Go(func() {
		pruner.Run(bgCtx, st, pruner.Config{Interval: cfg.PruneInterval, Retention: cfg.Retention, DeadRetention: cfg.DeadRetention}, log)
	})

	srv := api.NewServer(st, listener, log)
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
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(api.MaxMessageBytes), grpc.MaxCallSendMsgSize(api.MaxMessageBytes)),
	)
	if err != nil {
		return err
	}
	defer conn.Close()
	var shuttingDown atomic.Bool
	handler, err := api.NewHTTPHandler(bgCtx, conn, readyFunc(&shuttingDown, pool.Ping), cfg.APIKeys)
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

	errc := make(chan error, 2)
	go func() { errc <- grpcServer.Serve(grpcLis) }()
	go func() {
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	log.Info("tasklane started", "grpc_addr", cfg.GRPCAddr, "http_addr", cfg.HTTPAddr)

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
	stopBackground()
	background.Wait()
	if serveErr == nil {
		log.Info("tasklane stopped cleanly")
	}
	return serveErr
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
