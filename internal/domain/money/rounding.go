package money

import (
	"errors"
	"fmt"
	"math"
	"math/big"
)

// ErrRoundingOverflow means a rounded result does not fit in an int64.
var ErrRoundingOverflow = errors.New("rounded result overflows int64")

// DivRoundHalfEven divides num by den using banker's rounding.
//
// Half-even rather than half-up because these divisions are applied across a
// very large number of amounts — every FX conversion, every percentage fee.
// Half-up rounds every exact tie in the same direction, so the error
// accumulates one way and eventually surfaces as a systematic discrepancy
// against a provider's own figures. Half-even splits the ties, so the bias
// cancels. This is why financial systems converge on it.
//
// Exposed here rather than reimplemented per caller: two copies of a rounding
// rule drift apart, and the symptom is a fee that disagrees with a conversion
// by one minor unit in a way nobody can reproduce.
func DivRoundHalfEven(num, den *big.Int) (int64, error) {
	if den.Sign() == 0 {
		return 0, errors.New("division by zero")
	}

	// Sign is separated out so the tie comparison reasons about magnitudes
	// alone, rather than about Go's truncation-toward-zero.
	negative := num.Sign()*den.Sign() < 0
	absNum := new(big.Int).Abs(num)
	absDen := new(big.Int).Abs(den)

	quo, rem := new(big.Int).QuoRem(absNum, absDen, new(big.Int))

	// Compare twice the remainder against the divisor to place the tie.
	switch new(big.Int).Lsh(rem, 1).Cmp(absDen) {
	case 1:
		quo.Add(quo, big.NewInt(1))
	case 0:
		if quo.Bit(0) == 1 { // exactly half: move to the even neighbour
			quo.Add(quo, big.NewInt(1))
		}
	}

	if negative {
		quo.Neg(quo)
	}
	if !quo.IsInt64() {
		return 0, fmt.Errorf("%w: %s", ErrRoundingOverflow, quo)
	}

	result := quo.Int64()
	if result == math.MinInt64 {
		// Negating this is not representable, so later arithmetic would wrap
		// silently rather than fail.
		return 0, fmt.Errorf("%w: result is the int64 minimum", ErrRoundingOverflow)
	}
	return result, nil
}

// MulRatio scales an amount by num/den, rounding half to even.
//
// Used for percentage-style calculations — a fee in basis points, a share of a
// split — where the exact product is rarely a whole minor unit.
func (m Money) MulRatio(num, den int64) (Money, error) {
	if den == 0 {
		return Money{}, errors.New("MulRatio: division by zero")
	}

	scaled, err := DivRoundHalfEven(
		new(big.Int).Mul(big.NewInt(m.Amount()), big.NewInt(num)),
		big.NewInt(den),
	)
	if err != nil {
		return Money{}, err
	}
	return New(scaled, m.Currency())
}
