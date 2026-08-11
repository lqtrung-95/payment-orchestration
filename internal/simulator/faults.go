// Package simulator implements a payment provider that misbehaves on purpose.
//
// It is not a test fixture. Every correctness claim this project makes — no
// double charges, no lost payments — is only meaningful against a provider that
// actually fails, and fails in the specific ways real providers do. The
// catalogue below is the whole scope; it is deliberately not open-ended.
package simulator

import (
	"hash/fnv"
	"math/rand"
	"sync"
	"time"
)

// Fault is one way the provider can misbehave.
type Fault string

const (
	// FaultTimeoutAfterSuccess is the flagship case: the charge is recorded,
	// then the connection hangs. The caller never learns it succeeded. Any
	// system that retries here charges the customer twice.
	FaultTimeoutAfterSuccess Fault = "timeout_after_success"

	// FaultError5xxAfterSuccess is the same class wearing a different shape —
	// the charge is recorded and the caller is told the request failed.
	FaultError5xxAfterSuccess Fault = "error_5xx_after_success"

	FaultDuplicateWebhook      Fault = "duplicate_webhook"
	FaultOutOfOrderWebhook     Fault = "out_of_order_webhook"
	FaultWebhookBeforeResponse Fault = "webhook_before_response"

	// FaultSlowResponse exercises timeout budgets rather than correctness.
	FaultSlowResponse Fault = "slow_response"

	// FaultPartialCaptureDrift captures a different amount than requested,
	// which surfaces later as a reconciliation break rather than an error.
	FaultPartialCaptureDrift Fault = "partial_capture_drift"

	// FaultStaleStatus makes GetStatus lag reality, so recovery logic cannot
	// assume the provider's answer is immediately consistent.
	FaultStaleStatus Fault = "stale_status"
)

// AllFaults is the catalogue, used to validate configuration and to render the
// admin API.
var AllFaults = []Fault{
	FaultTimeoutAfterSuccess,
	FaultError5xxAfterSuccess,
	FaultDuplicateWebhook,
	FaultOutOfOrderWebhook,
	FaultWebhookBeforeResponse,
	FaultSlowResponse,
	FaultPartialCaptureDrift,
	FaultStaleStatus,
}

// Config is the runtime-tunable fault configuration.
//
// Probabilities are per request, in [0,1]. Outage is handled separately because
// it is a window rather than a coin flip: a provider that is down is down for
// every request until it comes back.
type Config struct {
	Seed          int64             `json:"seed"`
	Probabilities map[Fault]float64 `json:"probabilities"`

	// OutageUntil, when in the future, makes every request fail as unavailable.
	OutageUntil time.Time `json:"outage_until"`

	SlowResponseMinMs int `json:"slow_response_min_ms"`
	SlowResponseMaxMs int `json:"slow_response_max_ms"`

	// WebhookDelayMs staggers webhook delivery relative to the API response.
	WebhookDelayMs int `json:"webhook_delay_ms"`
}

// Preset names for the admin API.
const (
	PresetHealthy  = "healthy"
	PresetDegraded = "degraded"
	PresetChaos    = "chaos"
)

// Preset returns a named configuration, or false if the name is unknown.
func Preset(name string, seed int64) (Config, bool) {
	base := Config{Seed: seed, Probabilities: map[Fault]float64{}, WebhookDelayMs: 50}

	switch name {
	case PresetHealthy:
		return base, true

	case PresetDegraded:
		base.Probabilities = map[Fault]float64{
			FaultTimeoutAfterSuccess: 0.05,
			FaultSlowResponse:        0.15,
			FaultDuplicateWebhook:    0.10,
		}
		base.SlowResponseMinMs, base.SlowResponseMaxMs = 200, 800
		return base, true

	case PresetChaos:
		// Roughly a third of requests misbehave. Sized so a load run produces
		// meaningful failure counts without the happy path disappearing.
		base.Probabilities = map[Fault]float64{
			FaultTimeoutAfterSuccess:   0.12,
			FaultError5xxAfterSuccess:  0.08,
			FaultDuplicateWebhook:      0.25,
			FaultOutOfOrderWebhook:     0.15,
			FaultWebhookBeforeResponse: 0.10,
			FaultSlowResponse:          0.20,
			FaultPartialCaptureDrift:   0.05,
			FaultStaleStatus:           0.10,
		}
		base.SlowResponseMinMs, base.SlowResponseMaxMs = 300, 2000
		return base, true

	default:
		return Config{}, false
	}
}

// Engine decides which faults fire for a given request.
type Engine struct {
	mu  sync.RWMutex
	cfg Config
}

func NewEngine(cfg Config) *Engine {
	if cfg.Probabilities == nil {
		cfg.Probabilities = map[Fault]float64{}
	}
	return &Engine{cfg: cfg}
}

func (e *Engine) Config() Config {
	e.mu.RLock()
	defer e.mu.RUnlock()

	clone := e.cfg
	clone.Probabilities = make(map[Fault]float64, len(e.cfg.Probabilities))
	for k, v := range e.cfg.Probabilities {
		clone.Probabilities[k] = v
	}
	return clone
}

// SetConfig replaces the configuration. Faults are tunable at runtime rather
// than at compile time because the demo — and any useful chaos run — involves
// changing failure rates while traffic is flowing.
func (e *Engine) SetConfig(cfg Config) {
	if cfg.Probabilities == nil {
		cfg.Probabilities = map[Fault]float64{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg = cfg
}

// Fires reports whether a fault applies to the request identified by key.
//
// The random draw is derived from the seed and the request key rather than from
// a shared sequential generator. Under concurrency a shared generator produces
// draws in whatever order goroutines happen to reach it, so the same seed
// replays differently every run — which makes a "reproducible" chaos run a
// fiction. Deriving per request means a given key always gets the same verdict,
// no matter the ordering or the level of parallelism.
func (e *Engine) Fires(fault Fault, key string) bool {
	e.mu.RLock()
	p := e.cfg.Probabilities[fault]
	seed := e.cfg.Seed
	e.mu.RUnlock()

	if p <= 0 {
		return false
	}
	if p >= 1 {
		return true
	}
	return rngFor(seed, key, string(fault)).Float64() < p
}

// InOutage reports whether the provider is currently refusing all traffic.
func (e *Engine) InOutage(now time.Time) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return now.Before(e.cfg.OutageUntil)
}

// StartOutage takes the provider down for d.
func (e *Engine) StartOutage(d time.Duration) time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg.OutageUntil = time.Now().Add(d)
	return e.cfg.OutageUntil
}

// SlowResponseDelay returns the latency to inject, deterministic per key.
func (e *Engine) SlowResponseDelay(key string) time.Duration {
	e.mu.RLock()
	minMs, maxMs, seed := e.cfg.SlowResponseMinMs, e.cfg.SlowResponseMaxMs, e.cfg.Seed
	e.mu.RUnlock()

	if maxMs <= minMs {
		return time.Duration(minMs) * time.Millisecond
	}
	span := rngFor(seed, key, "slow").Intn(maxMs - minMs)
	return time.Duration(minMs+span) * time.Millisecond
}

func (e *Engine) WebhookDelay() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return time.Duration(e.cfg.WebhookDelayMs) * time.Millisecond
}

// rngFor builds a generator whose output depends only on the seed and the
// request identity, never on call ordering.
func rngFor(seed int64, parts ...string) *rand.Rand {
	h := fnv.New64a()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}
	// A seeded, predictable generator is the requirement here, not a weakness:
	// chaos runs have to replay identically or their results are anecdotes.
	// Nothing security-relevant depends on these draws.
	//nolint:gosec // G404: deterministic replay is the entire purpose
	return rand.New(rand.NewSource(seed ^ int64(h.Sum64())))
}
