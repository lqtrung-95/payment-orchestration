// Package fx holds foreign exchange rates, the locks taken against them, and
// the conversion arithmetic between them.
//
// Two decisions run through the whole package. Rates are fixed-point integers
// rather than floats, for the same reason money is: 1.1 has no exact binary
// representation, and a conversion wrong by one ten-millionth becomes a
// reconciliation break nobody can explain. And a rate is quoted in *major*
// units, because minor units differ per currency — a rate expressed in them
// would silently mean something different for USD than for JPY.
package fx

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

// Scale is the fixed-point scale for rates. A EUR/USD rate of 1.085 is 1085000000.
//
// Nine decimal places is well beyond what any provider quotes, so the stored
// rate is exact rather than the nearest representable approximation of it.
const Scale = 1_000_000_000

var (
	ErrInvalidRate      = errors.New("fx rate must be positive")
	ErrSamePair         = errors.New("fx rate base and quote must differ")
	ErrCurrencyMismatch = errors.New("amount currency does not match the rate base")
)

// Rate is "one major unit of Base buys Nano/Scale major units of Quote".
type Rate struct {
	Base  money.Currency
	Quote money.Currency

	// Nano is the rate multiplied by Scale.
	Nano int64

	Source string

	// AsOf is the instant this rate was quoted for, not when it was stored.
	AsOf time.Time
}

func NewRate(base, quote money.Currency, nano int64, source string, asOf time.Time) (Rate, error) {
	if err := base.Validate(); err != nil {
		return Rate{}, fmt.Errorf("base: %w", err)
	}
	if err := quote.Validate(); err != nil {
		return Rate{}, fmt.Errorf("quote: %w", err)
	}
	if base == quote {
		return Rate{}, fmt.Errorf("%w: both %s", ErrSamePair, base)
	}
	if nano <= 0 {
		return Rate{}, fmt.Errorf("%w: got %d", ErrInvalidRate, nano)
	}
	return Rate{Base: base, Quote: quote, Nano: nano, Source: source, AsOf: asOf.UTC()}, nil
}

// Float renders the rate for display and logging only.
//
// Deliberately never used in arithmetic: reintroducing a float anywhere on the
// conversion path would undo the reason the rate is an integer at all.
func (r Rate) Float() float64 { return float64(r.Nano) / float64(Scale) }

func (r Rate) String() string {
	return fmt.Sprintf("%s/%s %.9f", r.Base, r.Quote, r.Float())
}

// Invert returns the reciprocal rate.
//
// The reciprocal is computed once at full precision and rounded once. Inverting
// a rounded rate and rounding again compounds the error, which shows up as a
// break when a round trip fails to return the original amount.
func (r Rate) Invert() (Rate, error) {
	if r.Nano <= 0 {
		return Rate{}, fmt.Errorf("%w: got %d", ErrInvalidRate, r.Nano)
	}

	// (Scale * Scale) / Nano, rounded half to even.
	num := new(big.Int).Mul(big.NewInt(Scale), big.NewInt(Scale))
	inverted, err := money.DivRoundHalfEven(num, big.NewInt(r.Nano))
	if err != nil {
		return Rate{}, err
	}
	if inverted <= 0 {
		return Rate{}, fmt.Errorf("%w: reciprocal of %d rounds to zero", ErrInvalidRate, r.Nano)
	}

	return Rate{
		Base: r.Quote, Quote: r.Base, Nano: inverted,
		Source: r.Source, AsOf: r.AsOf,
	}, nil
}
