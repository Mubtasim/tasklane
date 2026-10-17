// Package backoff computes retry delays: exponential growth with a cap and
// full jitter, so retries from many failed jobs spread out instead of arriving
// in lockstep.
package backoff

import (
	"math/rand/v2"
	"time"
)

type Policy struct {
	Base time.Duration
	Cap  time.Duration
	Rand func() float64 // returns a value in [0, 1); injectable for tests
}

// Default is base 1 s, cap 1 h.
func Default() Policy {
	return Policy{Base: time.Second, Cap: time.Hour, Rand: rand.Float64}
}

// Delay returns a random delay in [0, min(Cap, Base·2^attempt)).
func (p Policy) Delay(attempt int) time.Duration {
	ceiling := p.Base
	// Doubling stops at the cap, so large attempt counts can't overflow.
	for i := 0; i < attempt && ceiling < p.Cap; i++ {
		ceiling *= 2
	}
	if ceiling > p.Cap {
		ceiling = p.Cap
	}
	return time.Duration(p.Rand() * float64(ceiling))
}
