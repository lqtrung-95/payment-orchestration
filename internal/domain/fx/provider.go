package fx

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

// Provider quotes a rate for a pair at an instant.
//
// Point-in-time rather than "current", because reconciliation asks what the
// rate was months ago and a provider that only knows today cannot answer.
type Provider interface {
	Name() string
	Quote(ctx context.Context, base, quote money.Currency, at time.Time) (Rate, error)
}

// SimulatedProvider is a deterministic rate feed.
//
// Rates move with time, which is the entire point: if the rate at settlement
// always equalled the rate at authorisation, the FX gain/loss account would
// stay empty and `fx_drift` breaks could not be produced at all. Movement is a
// smooth function of the day plus a per-pair phase offset, so a rate is
// reproducible for any instant — a reconciliation run over historical data
// gives the same answer today as it will next week.
type SimulatedProvider struct {
	// base holds the central rate for each pair, in nano units.
	base map[pairKey]int64

	// volatilityBps is the peak deviation from the central rate, in basis
	// points. Large enough that drift is visible against real amounts, small
	// enough to stay plausible.
	volatilityBps int64
}

type pairKey struct{ base, quote money.Currency }

// DefaultRates are the central rates the simulator oscillates around. Only the
// pairs the demo and tests need; an unknown pair is an error rather than an
// invented number, because a silently fabricated rate is worse than a failure.
var DefaultRates = map[string]int64{
	"EUR/USD": 1_085_000_000,
	"GBP/USD": 1_270_000_000,
	"USD/SGD": 1_345_000_000,
	"USD/JPY": 157_200_000_000,
	"USD/VND": 25_400_000_000_000,
	"AUD/USD": 655_000_000,
}

func NewSimulatedProvider(volatilityBps int64) *SimulatedProvider {
	p := &SimulatedProvider{
		base:          make(map[pairKey]int64, len(DefaultRates)*2),
		volatilityBps: volatilityBps,
	}

	for pair, nano := range DefaultRates {
		var b, q money.Currency
		if _, err := fmt.Sscanf(pair, "%3s/%3s", &b, &q); err != nil {
			continue
		}
		p.base[pairKey{b, q}] = nano

		// The reciprocal is registered too, so a caller asking for USD/EUR gets
		// a consistent answer instead of an unknown-pair error.
		if rate, err := NewRate(b, q, nano, "sim", time.Unix(0, 0)); err == nil {
			if inv, err := rate.Invert(); err == nil {
				p.base[pairKey{q, b}] = inv.Nano
			}
		}
	}
	return p
}

func (p *SimulatedProvider) Name() string { return "sim-fx" }

// ErrUnknownPair means no central rate is configured for the pair.
var ErrUnknownPair = fmt.Errorf("no fx rate configured for pair")

func (p *SimulatedProvider) Quote(_ context.Context, base, quote money.Currency, at time.Time) (Rate, error) {
	if base == quote {
		return Rate{}, fmt.Errorf("%w: both %s", ErrSamePair, base)
	}

	central, ok := p.base[pairKey{base, quote}]
	if !ok {
		return Rate{}, fmt.Errorf("%w: %s/%s", ErrUnknownPair, base, quote)
	}

	return NewRate(base, quote, p.rateAt(base, quote, central, at), p.Name(), at)
}

// rateAt applies a deterministic oscillation around the central rate.
//
// A sine wave over the day, phase-shifted per pair so that two pairs do not
// move in lockstep. Deterministic in the instant alone: no stored state, no
// sequence, so concurrent callers and replayed history all agree.
func (p *SimulatedProvider) rateAt(base, quote money.Currency, central int64, at time.Time) int64 {
	if p.volatilityBps <= 0 {
		return central
	}

	h := fnv.New64a()
	_, _ = h.Write([]byte(base + "/" + quote))
	phase := float64(h.Sum64()%1000) / 1000 * 2 * math.Pi

	// One full cycle per day, sampled at second resolution.
	const secondsPerDay = 86400
	position := float64(at.UTC().Unix()%secondsPerDay) / secondsPerDay
	deviation := math.Sin(position*2*math.Pi + phase)

	// central × (1 + deviation × volatility/10000), in integer arithmetic.
	offset := int64(deviation * float64(p.volatilityBps) / 10000 * float64(central))
	rate := central + offset
	if rate <= 0 {
		return central
	}
	return rate
}
