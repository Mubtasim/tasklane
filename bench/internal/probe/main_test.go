package probe

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/mubtasim/tasklane/internal/testpg"
)

var testDB *testpg.DB

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
