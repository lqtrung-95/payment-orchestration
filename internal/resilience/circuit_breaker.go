package resilience

import (
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned instead of calling a dependency that is currently
// failing. It is deliberately a distinct error: the call never happened, so the
// outcome is *known*, not ambiguous, and it must never be treated as a possible
// charge.
var ErrCircuitOpen = errors.New("circuit breaker is open")

type CircuitState string

const (
	// StateClosed is normal operation: calls pass through and failures are counted.
	StateClosed CircuitState = "closed"

	// StateOpen rejects immediately. Hammering a dependency that is already
	// failing lengthens its outage and burns the caller's own capacity.
	StateOpen CircuitState = "open"

	// StateHalfOpen admits a small number of probes to discover whether the
	// dependency has recovered, without exposing it to full traffic.
	StateHalfOpen CircuitState = "half_open"
)

type CircuitConfig struct {
	// FailureThreshold is how many consecutive failures trip the breaker.
	FailureThreshold int

	// OpenDuration is how long to reject before probing again.
	OpenDuration time.Duration

	// HalfOpenProbes is how many calls are admitted while probing, and how many
	// must succeed to close.
	HalfOpenProbes int
}

func DefaultCircuitConfig() CircuitConfig {
	return CircuitConfig{FailureThreshold: 5, OpenDuration: 10 * time.Second, HalfOpenProbes: 2}
}

// CircuitBreaker isolates one dependency — here, one payment provider.
//
// It is per provider rather than global on purpose: one provider failing must
// not stop traffic to the others. Opening it is a routing signal, not a hard
// failure; a later phase uses it to fail over rather than to give up.
type CircuitBreaker struct {
	name string
	cfg  CircuitConfig

	mu               sync.Mutex
	state            CircuitState
	consecutiveFails int
	openedAt         time.Time
	halfOpenInFlight int
	halfOpenSuccess  int

	// now is injectable so tests can drive the clock instead of sleeping.
	now func() time.Time
}

func NewCircuitBreaker(name string, cfg CircuitConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.HalfOpenProbes <= 0 {
		cfg.HalfOpenProbes = 1
	}
	return &CircuitBreaker{name: name, cfg: cfg, state: StateClosed, now: time.Now}
}

func (b *CircuitBreaker) Name() string { return b.name }

func (b *CircuitBreaker) State() CircuitState {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh()
	return b.state
}

// Allow reports whether a call may proceed, and reserves a probe slot when
// half-open.
func (b *CircuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh()

	switch b.state {
	case StateClosed:
		return true

	case StateHalfOpen:
		// Only a few probes are admitted at a time; letting the full load
		// through on the first sign of recovery is how a dependency gets
		// knocked straight back over.
		if b.halfOpenInFlight >= b.cfg.HalfOpenProbes {
			return false
		}
		b.halfOpenInFlight++
		return true

	default:
		return false
	}
}

// Success records a call that worked.
func (b *CircuitBreaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateHalfOpen:
		b.halfOpenSuccess++
		b.halfOpenInFlight--
		if b.halfOpenSuccess >= b.cfg.HalfOpenProbes {
			b.closeCircuit()
		}
	default:
		b.consecutiveFails = 0
	}
}

// Failure records a call that failed.
//
// Only failures of the dependency itself belong here. A provider that declines
// a card is working perfectly; counting declines would trip the breaker on a
// merchant with poor approval rates and take down a healthy provider.
func (b *CircuitBreaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateHalfOpen:
		// One failed probe is enough: the dependency has not recovered.
		b.halfOpenInFlight--
		b.openCircuit()

	default:
		b.consecutiveFails++
		if b.consecutiveFails >= b.cfg.FailureThreshold {
			b.openCircuit()
		}
	}
}

// refresh moves an expired open circuit into half-open. Called under the lock.
func (b *CircuitBreaker) refresh() {
	if b.state == StateOpen && b.now().Sub(b.openedAt) >= b.cfg.OpenDuration {
		b.state = StateHalfOpen
		b.halfOpenInFlight = 0
		b.halfOpenSuccess = 0
	}
}

func (b *CircuitBreaker) openCircuit() {
	b.state = StateOpen
	b.openedAt = b.now()
	b.consecutiveFails = 0
	b.halfOpenInFlight = 0
	b.halfOpenSuccess = 0
}

func (b *CircuitBreaker) closeCircuit() {
	b.state = StateClosed
	b.consecutiveFails = 0
	b.halfOpenInFlight = 0
	b.halfOpenSuccess = 0
}
