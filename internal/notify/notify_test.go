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
	l, err := New(testDB.DSN, quietLogger())
	require.NoError(t, err)
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

func (l *Listener) backendPID() uint32 { return l.pid.Load() }

// notifyQueue sends payload as is: a bare queue name, as servers before the
// sender tag sent, or "<queue> <sender>".
func notifyQueue(t *testing.T, payload string) {
	t.Helper()
	_, err := testDB.Pool.Exec(context.Background(), "SELECT pg_notify($1, $2)", pgstore.NotifyChannel, payload)
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

func TestNewRejectsBadDSN(t *testing.T) {
	_, err := New("postgres://%zz", quietLogger())
	require.Error(t, err)
}

func TestNewSetsConnectTimeout(t *testing.T) {
	l, err := New("postgres://u:p@localhost:5432/db", quietLogger())
	require.NoError(t, err)
	require.Equal(t, ConnectTimeout, l.cfg.ConnectTimeout)
	require.Equal(t, ApplicationName, l.cfg.RuntimeParams["application_name"])

	l, err = New("postgres://u:p@localhost:5432/db?connect_timeout=3", quietLogger())
	require.NoError(t, err)
	require.Equal(t, 3*time.Second, l.cfg.ConnectTimeout, "an explicit connect_timeout wins")
}

func TestWakeIsNonBlockingAndCollapses(t *testing.T) {
	l, err := New("", quietLogger())
	require.NoError(t, err)
	ch, unsubscribe := l.Subscribe("emails")
	defer unsubscribe()

	l.wake("emails")
	l.wake("emails") // buffer full: dropped, must not block
	requireWoken(t, ch)
	requireNotWoken(t, ch)
}

func TestUnsubscribeStopsWakeUps(t *testing.T) {
	l, err := New("", quietLogger())
	require.NoError(t, err)
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

func TestNewGivesEachListenerItsOwnSender(t *testing.T) {
	a, err := New("", quietLogger())
	require.NoError(t, err)
	b, err := New("", quietLogger())
	require.NoError(t, err)
	require.Len(t, a.sender, 16)
	require.NotEqual(t, a.sender, b.sender)
}

// The publishing server has already woken its own leases in memory, so its
// own NOTIFY coming back must not wake them again.
func TestListenerSkipsItsOwnNotifications(t *testing.T) {
	l := startListener(t)
	ch, unsubscribe := l.Subscribe("emails")
	defer unsubscribe()

	notifyQueue(t, "emails "+l.sender)
	requireNotWoken(t, ch)

	notifyQueue(t, "emails 0123456789abcdef") // another server's
	requireWoken(t, ch)
}

// During a rolling upgrade, older servers still send the bare queue name.
func TestListenerWakesOnAnUntaggedNotification(t *testing.T) {
	l := startListener(t)
	ch, unsubscribe := l.Subscribe("emails")
	defer unsubscribe()

	notifyQueue(t, "emails")
	requireWoken(t, ch)
}

func TestParsePayload(t *testing.T) {
	for payload, want := range map[string][2]string{
		"emails":                  {"emails", ""},
		"emails 0123456789abcdef": {"emails", "0123456789abcdef"},
		"a.b-c_d x":               {"a.b-c_d", "x"},
	} {
		queue, sender := parsePayload(payload)
		require.Equal(t, want, [2]string{queue, sender}, payload)
	}
}

func TestReconnectsAfterConnectionLoss(t *testing.T) {
	l := startListener(t)
	ch, unsubscribe := l.Subscribe("emails")
	defer unsubscribe()
	old := l.backendPID()
	require.NotZero(t, old)
	// Discard the wake-up from the initial connect.
	select {
	case <-ch:
	default:
	}

	_, err := testDB.Pool.Exec(context.Background(), "SELECT pg_terminate_backend($1)", old)
	require.NoError(t, err)

	// A new connection is listening.
	require.Eventually(t, func() bool {
		pid := l.backendPID()
		return pid != 0 && pid != old
	}, 15*time.Second, 20*time.Millisecond)

	// Re-establishing LISTEN wakes every subscriber, since notifications may
	// have been missed.
	requireWoken(t, ch)
	select {
	case <-ch:
	default:
	}

	notifyQueue(t, "emails")
	requireWoken(t, ch)
}
