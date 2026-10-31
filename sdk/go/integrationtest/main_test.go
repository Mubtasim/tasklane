package integrationtest

import (
	"context"
	"log"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/api"
	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/notify"
	"github.com/mubtasim/tasklane/internal/pgstore"
	"github.com/mubtasim/tasklane/internal/reaper"
	"github.com/mubtasim/tasklane/internal/testpg"
	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

// One Postgres and one notify.Listener are shared by the package's tests.
// Tests reset the tables first, so they must not run in parallel.
var (
	testDB       *testpg.DB
	testListener *notify.Listener
	discard      = slog.New(slog.DiscardHandler)
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	var err error
	testDB, err = testpg.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
	testListener, err = notify.New(testDB.DSN, discard)
	if err != nil {
		log.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = testListener.Run(ctx) // always nil once ctx is done
	}()
	for i := 0; !testListener.Listening(); i++ {
		if i == 500 {
			log.Fatal("notify listener did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	code := m.Run()
	cancel()
	<-done
	testDB.Close()
	os.Exit(code)
}

type env struct {
	client *tasklane.Client
	raw    tasklanev1.TasklaneServiceClient // the plain API, to act as a worker that dies
}

// newEnv resets the database and serves the real API (Postgres store, 50 ms
// lease re-check, reaper every 50 ms, zero retry backoff) over an in-memory
// connection.
func newEnv(t *testing.T) *env {
	t.Helper()
	require.NoError(t, testDB.Reset(context.Background()))
	st := pgstore.New(testDB.Pool, backoff.Policy{Base: time.Second, Cap: time.Hour, Rand: func() float64 { return 0 }})
	srv := api.NewServer(st, testListener, discard, api.WithPollInterval(50*time.Millisecond))
	gs := api.NewGRPCServer(srv, nil, discard)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	reaperCtx, stopReaper := context.WithCancel(context.Background())
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		reaper.Run(reaperCtx, st, 50*time.Millisecond, reaper.DefaultBatch, discard)
	}()

	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })
	client, err := tasklane.Dial("passthrough:///bufnet", tasklane.WithGRPCDialOptions(dialer))
	require.NoError(t, err)
	rawConn, err := grpc.NewClient("passthrough:///bufnet", dialer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		rawConn.Close()
		srv.Shutdown()
		gs.Stop()
		stopReaper()
		<-reaperDone
	})
	return &env{client: client, raw: tasklanev1.NewTasklaneServiceClient(rawConn)}
}

// runWorker runs w in the background. stop cancels Run and returns its
// error; it also runs when the test ends, before the server stops.
func runWorker(t *testing.T, w *tasklane.Worker) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	var stopped bool
	var err error
	stop = func() error {
		if !stopped {
			stopped = true
			cancel()
			err = <-errc
		}
		return err
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Errorf("Run: %v", err)
		}
	})
	return stop
}
