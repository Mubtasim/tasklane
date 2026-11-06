// Package pace sends events on a fixed schedule (an open load model).
//
// In a closed loop, a producer that waits for each reply sends nothing while
// the server is stalled, so a stall shows up as a single slow sample
// (coordinated omission). Run keeps the schedule: every event planned during
// a stall still fires on time, in its own goroutine, and callers measure
// latency from the planned time, so the stall reaches p99 as it would for
// real users.
package pace

import (
	"context"
	"sync"
	"time"
)

// Planned is the send time of event i at rate events per second.
func Planned(start time.Time, rate float64, i int64) time.Time {
	return start.Add(time.Duration(float64(i) * float64(time.Second) / rate))
}

// Run fires one call per planned time from start until ctx is done, never
// waiting for a call to return. At most maxInFlight calls run at once; an
// event planned while that many are still running is dropped and counted,
// and a run with drops is invalid (the load generator, not the server, set
// the pace). If Run falls behind (a slow scheduler), it fires the late
// events at once instead of skipping them. It returns after ctx is done and
// every call has returned.
func Run(ctx context.Context, start time.Time, rate float64, maxInFlight int, fire func(planned time.Time)) (sent, dropped int64) {
	sem := make(chan struct{}, maxInFlight)
	var wg sync.WaitGroup
	defer wg.Wait()
	// A stopped timer that each wait Resets; one timer for the whole run
	// instead of one allocation per event.
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for i := int64(0); ; i++ {
		planned := Planned(start, rate, i)
		if wait := time.Until(planned); wait > 0 {
			timer.Reset(wait)
			select {
			case <-ctx.Done():
				return sent, dropped
			case <-timer.C:
			}
		}
		// Checked on both paths: once ctx is done, no event is fired or
		// counted as dropped. When the timer and ctx.Done are ready together,
		// select may pick the timer, and a call that returned after cancel
		// can free a semaphore slot.
		if ctx.Err() != nil {
			return sent, dropped
		}
		select {
		case sem <- struct{}{}:
			sent++
			wg.Add(1)
			go func() {
				defer func() { <-sem; wg.Done() }()
				fire(planned)
			}()
		default:
			dropped++
		}
	}
}
