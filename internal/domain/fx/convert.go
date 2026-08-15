package fx

import (
	"fmt"
	"math"
	"math/big"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

// Convert turns an amount in the rate's base currency into its quote currency.
//
// The currency exponent is part of the arithmetic, and this is the one place in
// the system where that is true. Within a single currency, minor units are
// minor units and the exponent is display-only. Across currencies it is not:
// 100.00 USD is 10000 minor units and 15000 JPY is 15000 minor units, so a
// conversion that ignores the two-versus-zero decimal difference is wrong by a
// factor of a hundred. That is not a rounding error — it is the kind of bug
// that moves a decimal point in a customer's charge.
//
//	quote_minor = base_minor × (nano / Scale) × 10^(exp_quote − exp_base)
//
// Evaluated as a single exact fraction over big integers and rounded once at
// the end. Rounding at each step instead would let the error compound.
func (r Rate) Convert(amount money.Money) (money.Money, error) {
	if amount.Currency() != r.Base {
		return money.Money{}, fmt.Errorf("%w: amount is %s, rate base is %s",
			ErrCurrencyMismatch, amount.Currency(), r.Base)
	}
	if r.Nano <= 0 {
		return money.Money{}, fmt.Errorf("%w: got %d", ErrInvalidRate, r.Nano)
	}

	num := new(big.Int).Mul(big.NewInt(amount.Amount()), big.NewInt(r.Nano))
	den := big.NewInt(Scale)

	// The exponent difference scales one side or the other, never both.
	if shift := r.Quote.Exponent() - r.Base.Exponent(); shift > 0 {
		num.Mul(num, pow10(shift))
	} else if shift < 0 {
		den.Mul(den, pow10(-shift))
	}

	converted, err := divRoundHalfEven(num, den)
	if err != nil {
		return money.Money{}, err
	}
	return money.New(converted, r.Quote)
}

// pow10 returns 10^n as a big integer, for n >= 0.
func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// divRoundHalfEven divides num by den using banker's rounding.
//
// Half-even rather than half-up because FX conversion is applied to a very large
// number of amounts, and half-up is biased: it rounds up on every exact tie, so
// the error accumulates in one direction and shows up as a systematic drift
// against a settlement file. Half-even splits ties between up and down, so the
// bias cancels. It is the rounding mode financial systems converge on for
// exactly this reason.
func divRoundHalfEven(num, den *big.Int) (int64, error) {
	if den.Sign() == 0 {
		return 0, fmt.Errorf("%w: division by zero", ErrInvalidRate)
	}

	// Sign is handled separately so the tie comparison below reasons about
	// magnitudes only, rather than about Go's truncation-toward-zero.
	negative := num.Sign()*den.Sign() < 0
	absNum := new(big.Int).Abs(num)
	absDen := new(big.Int).Abs(den)

	quo, rem := new(big.Int).QuoRem(absNum, absDen, new(big.Int))

	// Compare 2×remainder against the divisor to place the tie.
	twice := new(big.Int).Lsh(rem, 1)
	switch twice.Cmp(absDen) {
	case 1:
		quo.Add(quo, big.NewInt(1))
	case 0:
		// Exactly half: round to the even neighbour.
		if quo.Bit(0) == 1 {
			quo.Add(quo, big.NewInt(1))
		}
	}

	if negative {
		quo.Neg(quo)
	}
	if !quo.IsInt64() {
		return 0, fmt.Errorf("%w: result %s", ErrRateOverflow, quo)
	}

	result := quo.Int64()
	if result == math.MinInt64 {
		// Negating MinInt64 is not representable, so downstream arithmetic on it
		// would silently wrap.
		return 0, fmt.Errorf("%w: result is int64 minimum", ErrRateOverflow)
	}
	return result, nil
}
