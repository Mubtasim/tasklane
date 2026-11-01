// Package invariant runs two Tasklane servers in the test process on one
// Postgres, pushes jobs through them with failing, slow and abruptly stopped
// workers, and checks the correctness rules of design §17.2 on the result.
package invariant

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"testing"

	"github.com/mubtasim/tasklane/internal/testpg"
)

var (
	full    = flag.Bool("invariant.full", false, "run the full size: 50 workers × 10,000 jobs")
	seedArg = flag.Int64("invariant.seed", 0, "seed for the job mix and the worker stops; 0 picks one from the clock")
)

// One Postgres is shared by the package's tests; each test resets it first.
var (
	testDB  *testpg.DB
	discard = slog.New(slog.DiscardHandler)
)

func TestMain(m *testing.M) {
	var err error
	testDB, err = testpg.Start(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	testDB.Close()
	os.Exit(code)
}
