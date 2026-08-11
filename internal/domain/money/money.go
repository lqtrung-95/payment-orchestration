// Package money provides the monetary value type used throughout the service.
//
// Amounts are integer minor units paired with an ISO-4217 currency, and there
// is deliberately no way to construct a Money from a float. Binary floating
// point cannot represent 0.10 exactly; a system that rounds a fraction of a
// minor unit per transaction loses real money at volume and — worse — produces
// reconciliation breaks that look like provider errors.
//
// Arithmetic is currency-checked and overflow-checked. Both failures return an
// error rather than a silently wrong number, because on a payment path a wrong
// number is indistinguishable from a correct one until settlement.
package money

import (
	"errors"
	"fmt"
	"math"
	"strconv"
)

var (
	ErrInvalidCurrency  = errors.New("invalid currency")
	ErrCurrencyMismatch = errors.New("currency mismatch")
	ErrOverflow         = errors.New("monetary overflow")
	ErrNegativeRatio    = errors.New("allocation ratio must not be negative")
	ErrEmptyAllocation  = errors.New("allocation requires at least one ratio")
	ErrZeroRatioTotal   = errors.New("allocation ratios must not sum to zero")
)

// Money is an immutable amount in a single currency. The zero value is not a
// valid amount: it carries no currency, and arithmetic on it returns an error.
type Money struct {
	amount   int64
	currency Currency
}

// New builds a Money from minor units, e.g. New(1050, "USD") is $10.50.
func New(minorUnits int64, currency Currency) (Money, error) {
	if err := currency.Validate(); err != nil {
		return Money{}, err
	}
	return Money{amount: minorUnits, currency: currency}, nil
}

// MustNew is New for cases where the currency is a compile-time constant and a
// failure would indicate a programming error. Never call it on external input.
func MustNew(minorUnits int64, currency Currency) Money {
	m, err := New(minorUnits, currency)
	if err != nil {
		panic(err)
	}
	return m
}

// Zero returns a zero amount in the given currency, the identity for Add.
func Zero(currency Currency) (Money, error) { return New(0, currency) }

func (m Money) Amount() int64      { return m.amount }
func (m Money) Currency() Currency { return m.currency }

func (m Money) IsZero() bool     { return m.amount == 0 }
func (m Money) IsPositive() bool { return m.amount > 0 }
func (m Money) IsNegative() bool { return m.amount < 0 }

// IsValid reports whether this Money carries a usable currency. A zero-value
// Money read from an uninitialised struct field is not valid.
func (m Money) IsValid() bool { return m.currency.Validate() == nil }

// Add returns m+other, failing on currency mismatch or int64 overflow.
func (m Money) Add(other Money) (Money, error) {
	if err := m.assertSameCurrency(other); err != nil {
		return Money{}, err
	}
	sum := m.amount + other.amount
	// Overflow is detectable by sign: adding a positive that yields a smaller
	// result, or a negative that yields a larger one, has wrapped.
	if (other.amount > 0 && sum < m.amount) || (other.amount < 0 && sum > m.amount) {
		return Money{}, fmt.Errorf("%w: %d + %d", ErrOverflow, m.amount, other.amount)
	}
	return Money{amount: sum, currency: m.currency}, nil
}

// Sub returns m-other, failing on currency mismatch or int64 overflow.
func (m Money) Sub(other Money) (Money, error) {
	neg, err := other.Neg()
	if err != nil {
		return Money{}, err
	}
	return m.Add(neg)
}

// Neg returns -m. It fails on the single value with no positive counterpart.
func (m Money) Neg() (Money, error) {
	if m.amount == math.MinInt64 {
		return Money{}, fmt.Errorf("%w: cannot negate %d", ErrOverflow, m.amount)
	}
	return Money{amount: -m.amount, currency: m.currency}, nil
}

// Mul scales the amount by an integer factor. There is no float or fractional
// multiply: percentage-style calculations such as fees go through Allocate,
// which cannot lose or invent minor units.
func (m Money) Mul(factor int64) (Money, error) {
	if m.amount == 0 || factor == 0 {
		return Money{amount: 0, currency: m.currency}, nil
	}
	product := m.amount * factor
	if product/factor != m.amount {
		return Money{}, fmt.Errorf("%w: %d * %d", ErrOverflow, m.amount, factor)
	}
	return Money{amount: product, currency: m.currency}, nil
}

// Cmp returns -1, 0, or 1 as m is less than, equal to, or greater than other.
func (m Money) Cmp(other Money) (int, error) {
	if err := m.assertSameCurrency(other); err != nil {
		return 0, err
	}
	switch {
	case m.amount < other.amount:
		return -1, nil
	case m.amount > other.amount:
		return 1, nil
	default:
		return 0, nil
	}
}

// Equal reports exact equality of both amount and currency. Unlike Cmp it
// cannot fail: amounts in different currencies are simply not equal.
func (m Money) Equal(other Money) bool {
	return m.amount == other.amount && m.currency == other.currency
}

func (m Money) assertSameCurrency(other Money) error {
	if err := m.currency.Validate(); err != nil {
		return err
	}
	if m.currency != other.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, other.currency)
	}
	return nil
}

// String renders the amount for human consumption, honouring the currency's
// minor-unit exponent. It is for logs and display only — never parse it back.
func (m Money) String() string {
	exp := m.currency.Exponent()
	if exp == 0 {
		return fmt.Sprintf("%d %s", m.amount, m.currency)
	}

	neg := m.amount < 0
	abs := m.amount
	if neg {
		// Negating MinInt64 overflows, so format from its absolute string form.
		if abs == math.MinInt64 {
			return fmt.Sprintf("%d %s", m.amount, m.currency)
		}
		abs = -abs
	}

	divisor := int64(1)
	for i := 0; i < exp; i++ {
		divisor *= 10
	}

	whole := abs / divisor
	frac := abs % divisor
	sign := ""
	if neg {
		sign = "-"
	}
	return fmt.Sprintf("%s%d.%0*d %s", sign, whole, exp, frac, m.currency)
}

// GoString makes %#v and test failure output readable.
func (m Money) GoString() string {
	return "money.MustNew(" + strconv.FormatInt(m.amount, 10) + ", \"" + string(m.currency) + "\")"
}
