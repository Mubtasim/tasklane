// Command tasklane runs the Tasklane server: the gRPC API, the HTTP/JSON
// gateway with health endpoints, the LISTEN/NOTIFY listener and the reaper.
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
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/mubtasim/tasklane/internal/api"
	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/notify"
	"github.com/mubtasim/tasklane/internal/pgstore"
	"github.com/mubtasim/tasklane/internal/reaper"
)

// shutdownTimeout bounds the graceful drain on SIGINT/SIGTERM.
const shutdownTimeout = 10 * time.Second

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
	// have drained, not the moment the signal arrives.
	bgCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()
	listener := notify.New(cfg.DatabaseURL, log)
	var background sync.WaitGroup
	background.Go(func() { listener.Run(bgCtx) })
	background.Go(func() { reaper.Run(bgCtx, st, cfg.ReaperInterval, reaper.DefaultBatch, log) })

	srv := api.NewServer(st, listener, log)
	grpcServer := api.NewGRPCServer(srv, cfg.APIKeys, log)
	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}

	target, err := dialAddr(cfg.GRPCAddr)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	handler, err := api.NewHTTPHandler(bgCtx, conn, pool.Ping)
	if err != nil {
		return err
	}
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: handler, ReadHeaderTimeout: 10 * time.Second}

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
		log.Info("shutting down")
	case serveErr = <-errc:
		log.Error("server failed; shutting down", "err", serveErr)
	}

	// Drain: end long polls first (HTTP leases are gRPC calls underneath),
	// stop taking HTTP requests, let in-flight RPCs finish, and force-close
	// whatever is left after the timeout.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	srv.Shutdown()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		log.Warn("graceful stop timed out; closing remaining connections")
		grpcServer.Stop()
	}
	stopBackground()
	background.Wait()
	log.Info("tasklane stopped cleanly")
	return serveErr
}
