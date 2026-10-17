package api

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
	"github.com/mubtasim/tasklane/internal/backoff"
	"github.com/mubtasim/tasklane/internal/notify"
	"github.com/mubtasim/tasklane/internal/pgstore"
	"github.com/mubtasim/tasklane/internal/testpg"
)

// One Postgres and one notify.Listener are shared by the package's tests.
// Tests reset the tables first, so they must not run in parallel.
var (
	testDB       *testpg.DB
	testListener *notify.Listener
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	var err error
	testDB, err = testpg.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
	testListener = notify.New(testDB.DSN, slog.New(slog.DiscardHandler))
	done := make(chan struct{})
	go func() {
		defer close(done)
		testListener.Run(ctx)
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
	client tasklanev1.TasklaneServiceClient
	conn   *grpc.ClientConn  // for the HTTP gateway tests
	lis    *bufconn.Listener // to dial more connections, with other options
	store  *pgstore.Store
	server *Server
	keys   []string
}

// newEnv resets the database and serves the API over an in-memory connection
// (bufconn): real gRPC framing, interceptors and status codes, no TCP port.
func newEnv(t *testing.T, keys []string, opts ...Option) *env {
	t.Helper()
	require.NoError(t, testDB.Reset(context.Background()))
	st := pgstore.New(testDB.Pool, backoff.Policy{Base: time.Second, Cap: time.Hour, Rand: func() float64 { return 0 }})
	srv := NewServer(st, testListener, slog.New(slog.DiscardHandler), opts...)
	gs := NewGRPCServer(srv, keys, slog.New(slog.DiscardHandler))

	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageBytes), grpc.MaxCallSendMsgSize(MaxMessageBytes)),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
		srv.Shutdown()
		gs.Stop()
	})
	return &env{client: tasklanev1.NewTasklaneServiceClient(conn), conn: conn, store: st, server: srv, keys: keys, lis: lis}
}
