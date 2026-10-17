package notify

import (
	"context"
	"io"
	"log"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mubtasim/tasklane/internal/pgstore"
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

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// startListener runs a Listener until the test ends and waits for its LISTEN.
func startListener(t *testing.T) *Listener {
	t.Helper()
	l := New(testDB.DSN, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := l.Run(ctx); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not stop after cancel")
		}
	})
	require.Eventually(t, l.Listening, 5*time.Second, 10*time.Millisecond)
	return l
}

func notifyQueue(t *testing.T, queue string) {
	t.Helper()
	_, err := testDB.Pool.Exec(context.Background(), "SELECT pg_notify($1, $2)", pgstore.NotifyChannel, queue)
	require.NoError(t, err)
}

func requireWoken(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber was not woken")
	}
}

func requireNotWoken(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("subscriber was woken unexpectedly")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestWakeIsNonBlockingAndCollapses(t *testing.T) {
	l := New("", quietLogger())
	ch, unsubscribe := l.Subscribe("emails")
	defer unsubscribe()

	l.wake("emails")
	l.wake("emails") // buffer full: dropped, must not block
	requireWoken(t, ch)
	requireNotWoken(t, ch)
}

func TestUnsubscribeStopsWakeUps(t *testing.T) {
	l := New("", quietLogger())
	ch, unsubscribe := l.Subscribe("emails")
	unsubscribe()
	unsubscribe() // idempotent

	l.wake("emails")
	requireNotWoken(t, ch)
	require.Empty(t, l.subs, "the empty queue entry is removed")
}

func TestNotifyWakesOnlySubscribersOfThatQueue(t *testing.T) {
	l := startListener(t)
	emails, unsubEmails := l.Subscribe("emails")
	defer unsubEmails()
	sms, unsubSMS := l.Subscribe("sms")
	defer unsubSMS()

	notifyQueue(t, "emails")
	requireWoken(t, emails)
	requireNotWoken(t, sms)
}

// listenerPID waits until the listener's backend has run LISTEN and returns
// its process ID; a PID equal to old is ignored.
func listenerPID(t *testing.T, old int) int {
	t.Helper()
	var pid int
	require.Eventually(t, func() bool {
		err := testDB.Pool.QueryRow(context.Background(),
			`SELECT pid FROM pg_stat_activity
			 WHERE application_name = $1 AND query LIKE 'LISTEN%' AND pid <> $2`,
			ApplicationName, old).Scan(&pid)
		return err == nil
	}, 5*time.Second, 20*time.Millisecond)
	return pid
}

func TestReconnectsAfterConnectionLoss(t *testing.T) {
	l := startListener(t)
	ch, unsubscribe := l.Subscribe("emails")
	defer unsubscribe()
	pid := listenerPID(t, 0)

	_, err := testDB.Pool.Exec(context.Background(), "SELECT pg_terminate_backend($1)", pid)
	require.NoError(t, err)

	// Losing the connection wakes every subscriber, since notifications may
	// have been missed.
	requireWoken(t, ch)

	listenerPID(t, pid) // a new connection is listening
	notifyQueue(t, "emails")
	requireWoken(t, ch)
}
