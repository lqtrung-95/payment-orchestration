// Package resilience holds the retry and failure-isolation primitives shared by
// the publisher, the workers, and the provider clients.
package resilience

import (
	"math"
	"math/rand"
	"time"
)

// Backoff computes retry delays with full jitter.
type Backoff struct {
	Base       time.Duration
	Max        time.Duration
	Multiplier float64
}

func NewBackoff(base, max time.Duration, multiplier float64) Backoff {
	if multiplier <= 1 {
		multiplier = 2
	}
	return Backoff{Base: base, Max: max, Multiplier: multiplier}
}

// Delay returns the wait before the given attempt, where attempt 1 is the first
// retry.
//
// The delay is drawn uniformly from [0, cap] rather than being set to cap —
// "full jitter" rather than plain exponential backoff. This matters most in the
// exact situation retries exist for: when a provider recovers from an outage,
// every caller that backed off deterministically wakes at the same instant and
// knocks it over again. Spreading the wake-ups across the whole window is what
// lets a recovering dependency actually recover.
//
// The trade-off is that an individual retry can fire almost immediately. That is
// acceptable here because the aggregate behaviour — load spread evenly across
// the window — is what protects the provider, not any single caller's patience.
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	capped := float64(b.Base) * math.Pow(b.Multiplier, float64(attempt-1))
	if capped > float64(b.Max) || math.IsInf(capped, 0) {
		capped = float64(b.Max)
	}
	if capped <= 0 {
		return 0
	}

	//nolint:gosec // G404: jitter needs spread, not unpredictability
	return time.Duration(rand.Int63n(int64(capped) + 1))
}

// Ceiling returns the un-jittered cap for an attempt, which is what the retry
// ladder uses to choose a delay tier. The tier has to be deterministic; the
// jitter is applied within it.
func (b Backoff) Ceiling(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	capped := float64(b.Base) * math.Pow(b.Multiplier, float64(attempt-1))
	if capped > float64(b.Max) || math.IsInf(capped, 0) {
		return b.Max
	}
	return time.Duration(capped)
}
