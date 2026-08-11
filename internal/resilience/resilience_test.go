package resilience

import (
	"testing"
	"time"
)

// Full jitter must actually spread. If every caller waited the same computed
// delay, a provider recovering from an outage would be hit by all of them at
// once and knocked straight back over — the failure mode retries exist to avoid.
func TestBackoffSpreadsDelaysAcrossTheWindow(t *testing.T) {
	b := NewBackoff(100*time.Millisecond, 10*time.Second, 2)

	seen := make(map[time.Duration]int)
	var maxSeen time.Duration
	for i := 0; i < 500; i++ {
		d := b.Delay(5)
		seen[d]++
		if d > maxSeen {
			maxSeen = d
		}
	}

	if len(seen) < 100 {
		t.Errorf("only %d distinct delays across 500 draws; the jitter is not spreading load", len(seen))
	}

	ceiling := b.Ceiling(5)
	if maxSeen > ceiling {
		t.Errorf("delay %v exceeded the ceiling %v", maxSeen, ceiling)
	}
}

func TestBackoffRespectsTheCap(t *testing.T) {
	b := NewBackoff(time.Second, 5*time.Second, 2)

	// Far enough out that the uncapped exponential would be astronomically large.
	for _, attempt := range []int{1, 5, 20, 100} {
		if got := b.Ceiling(attempt); got > 5*time.Second {
			t.Errorf("Ceiling(%d) = %v, want <= 5s", attempt, got)
		}
		if got := b.Delay(attempt); got > 5*time.Second {
			t.Errorf("Delay(%d) = %v, want <= 5s", attempt, got)
		}
	}
}

func TestBackoffCeilingGrowsThenPlateaus(t *testing.T) {
	b := NewBackoff(100*time.Millisecond, 3*time.Second, 2)

	if b.Ceiling(1) != 100*time.Millisecond {
		t.Errorf("Ceiling(1) = %v, want 100ms", b.Ceiling(1))
	}
	if b.Ceiling(2) != 200*time.Millisecond {
		t.Errorf("Ceiling(2) = %v, want 200ms", b.Ceiling(2))
	}
	if b.Ceiling(1) >= b.Ceiling(3) {
		t.Error("ceiling should grow with the attempt number")
	}
	if b.Ceiling(50) != 3*time.Second {
		t.Errorf("Ceiling(50) = %v, want the 3s cap", b.Ceiling(50))
	}
}

func TestCircuitBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	cb := NewCircuitBreaker("psp-test", CircuitConfig{
		FailureThreshold: 3, OpenDuration: time.Minute, HalfOpenProbes: 1,
	})

	for i := 0; i < 2; i++ {
		cb.Failure()
	}
	if cb.State() != StateClosed {
		t.Errorf("state = %s after 2 of 3 failures, want closed", cb.State())
	}

	cb.Failure()
	if cb.State() != StateOpen {
		t.Errorf("state = %s after reaching the threshold, want open", cb.State())
	}
	if cb.Allow() {
		t.Error("an open circuit must not admit calls")
	}
}

// A success clears the streak: it takes *consecutive* failures to trip, so an
// intermittent error does not eventually open the circuit on a healthy provider.
func TestCircuitBreakerResetsOnSuccess(t *testing.T) {
	cb := NewCircuitBreaker("psp-test", CircuitConfig{
		FailureThreshold: 3, OpenDuration: time.Minute, HalfOpenProbes: 1,
	})

	cb.Failure()
	cb.Failure()
	cb.Success()
	cb.Failure()
	cb.Failure()

	if cb.State() != StateClosed {
		t.Errorf("state = %s, want closed — the streak was broken by a success", cb.State())
	}
}

func TestCircuitBreakerProbesThenClosesAfterCooldown(t *testing.T) {
	cb := NewCircuitBreaker("psp-test", CircuitConfig{
		FailureThreshold: 1, OpenDuration: 50 * time.Millisecond, HalfOpenProbes: 2,
	})

	// Drive the clock rather than sleeping through the cooldown.
	now := time.Now()
	cb.now = func() time.Time { return now }

	cb.Failure()
	if cb.State() != StateOpen {
		t.Fatalf("state = %s, want open", cb.State())
	}

	now = now.Add(60 * time.Millisecond)
	if cb.State() != StateHalfOpen {
		t.Fatalf("state = %s after the cooldown, want half_open", cb.State())
	}

	// Only the configured number of probes is admitted; letting full traffic
	// through on the first sign of life is how a recovering provider is
	// knocked over again.
	firstProbe, secondProbe := cb.Allow(), cb.Allow()
	if !firstProbe || !secondProbe {
		t.Fatal("half-open should admit its configured probes")
	}
	if cb.Allow() {
		t.Error("half-open admitted more probes than configured")
	}

	cb.Success()
	cb.Success()
	if cb.State() != StateClosed {
		t.Errorf("state = %s after successful probes, want closed", cb.State())
	}
}

// One failed probe is enough to reopen: the dependency has not recovered, and
// admitting more traffic would only add to its load.
func TestCircuitBreakerReopensOnFailedProbe(t *testing.T) {
	cb := NewCircuitBreaker("psp-test", CircuitConfig{
		FailureThreshold: 1, OpenDuration: 50 * time.Millisecond, HalfOpenProbes: 2,
	})

	now := time.Now()
	cb.now = func() time.Time { return now }

	cb.Failure()
	now = now.Add(60 * time.Millisecond)
	if cb.State() != StateHalfOpen {
		t.Fatalf("state = %s, want half_open", cb.State())
	}

	if !cb.Allow() {
		t.Fatal("half-open should admit a probe")
	}
	cb.Failure()

	if cb.State() != StateOpen {
		t.Errorf("state = %s after a failed probe, want open", cb.State())
	}
}

func TestCircuitBreakerIsSafeUnderConcurrency(t *testing.T) {
	cb := NewCircuitBreaker("psp-test", DefaultCircuitConfig())

	done := make(chan struct{})
	for i := 0; i < 50; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				if cb.Allow() {
					if (i+j)%3 == 0 {
						cb.Failure()
					} else {
						cb.Success()
					}
				}
				_ = cb.State()
			}
		}(i)
	}
	for i := 0; i < 50; i++ {
		<-done
	}
}
