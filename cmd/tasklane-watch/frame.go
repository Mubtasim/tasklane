package main

import (
	"fmt"

	"google.golang.org/grpc/status"

	"github.com/mubtasim/tasklane/sdk/go/tasklane"
)

// frame is one status line: the queue's live counts and its completion rate
// over the server's recent window, or the error from the last refresh.
func frame(queue string, s tasklane.QueueStats, err error) string {
	if err != nil {
		st := status.Convert(err)
		return fmt.Sprintf("queue=%s  %s: %s (retrying)", queue, st.Code(), st.Message())
	}
	rate := 0.0
	if s.RecentWindow > 0 {
		rate = float64(s.RecentlyCompleted) / s.RecentWindow.Seconds()
	}
	return fmt.Sprintf("queue=%s  available %d | leased %d | dead %d | completed %.1f/s", queue, s.Available, s.Leased, s.Dead, rate)
}
