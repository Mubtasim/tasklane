package backoff

import (
	"testing"
	"time"
)

func fixed(v float64) func() float64 { return func() float64 { return v } }

func TestDelayGrowsExponentially(t *testing.T) {
	p := Policy{Base: time.Second, Cap: time.Hour, Rand: fixed(1)}
	for attempt, want := range map[int]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second} {
		if got := p.Delay(attempt); got != want {
			t.Errorf("Delay(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestDelayIsCapped(t *testing.T) {
	p := Policy{Base: time.Second, Cap: time.Hour, Rand: fixed(1)}
	for _, attempt := range []int{12, 64, 1000} {
		if got := p.Delay(attempt); got != time.Hour {
			t.Errorf("Delay(%d) = %v, want cap %v", attempt, got, time.Hour)
		}
	}
}

func TestDelayIsJittered(t *testing.T) {
	p := Policy{Base: time.Second, Cap: time.Hour, Rand: fixed(0.5)}
	if got := p.Delay(1); got != time.Second {
		t.Errorf("Delay(1) with rand 0.5 = %v, want 1s", got)
	}
}

func TestDefaultPolicyStaysInRange(t *testing.T) {
	p := Default()
	for i := 0; i < 1000; i++ {
		if got := p.Delay(3); got < 0 || got >= 8*time.Second {
			t.Fatalf("Delay(3) = %v, want within [0, 8s)", got)
		}
	}
}
