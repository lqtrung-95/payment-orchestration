package money

import "fmt"

// Allocate splits m across the given ratios so that the parts sum back to m
// exactly, with no minor unit created or destroyed.
//
// This is the only sanctioned way to compute a proportional amount such as a
// fee or a multi-party split. The naive alternative — converting to a float,
// multiplying by a percentage, and rounding — loses or invents a minor unit
// often enough to show up as a reconciliation break, and the break appears
// downstream at settlement where its cause is no longer obvious.
//
// Integer division truncates, leaving a remainder strictly smaller than the
// number of parts. That remainder is distributed one minor unit at a time from
// the first part onward, so the result is deterministic: callers that need a
// specific part to absorb the rounding should order the ratios accordingly.
func (m Money) Allocate(ratios ...int64) ([]Money, error) {
	if err := m.currency.Validate(); err != nil {
		return nil, err
	}
	if len(ratios) == 0 {
		return nil, ErrEmptyAllocation
	}

	var total int64
	for _, r := range ratios {
		if r < 0 {
			return nil, fmt.Errorf("%w: got %d", ErrNegativeRatio, r)
		}
		sum := total + r
		if sum < total {
			return nil, fmt.Errorf("%w: ratio total exceeds int64", ErrOverflow)
		}
		total = sum
	}
	if total == 0 {
		return nil, ErrZeroRatioTotal
	}

	parts := make([]Money, len(ratios))
	allocated := int64(0)

	for i, r := range ratios {
		product := m.amount * r
		// Guard the intermediate: a large amount times a large ratio can wrap
		// even though the final share is small.
		if r != 0 && product/r != m.amount {
			return nil, fmt.Errorf("%w: %d * %d in allocation", ErrOverflow, m.amount, r)
		}
		share := product / total
		parts[i] = Money{amount: share, currency: m.currency}
		allocated += share
	}

	// Truncation always leaves |remainder| < len(ratios), so a single pass
	// distributing one minor unit per part is enough to close the gap exactly.
	remainder := m.amount - allocated
	step := int64(1)
	if remainder < 0 {
		step = -1
	}
	for i := 0; remainder != 0; i++ {
		parts[i].amount += step
		remainder -= step
	}

	return parts, nil
}

// Split divides m into n equal parts, with any remainder distributed to the
// earliest parts. Splitting into zero or fewer parts is an error.
func (m Money) Split(n int) ([]Money, error) {
	if n <= 0 {
		return nil, fmt.Errorf("%w: got %d parts", ErrEmptyAllocation, n)
	}
	ratios := make([]int64, n)
	for i := range ratios {
		ratios[i] = 1
	}
	return m.Allocate(ratios...)
}

// Sum adds every amount, failing on currency mismatch or overflow. It is the
// inverse of Allocate: summing an allocation always reproduces the original.
func Sum(amounts ...Money) (Money, error) {
	if len(amounts) == 0 {
		return Money{}, ErrEmptyAllocation
	}
	total := amounts[0]
	if err := total.currency.Validate(); err != nil {
		return Money{}, err
	}
	for _, a := range amounts[1:] {
		var err error
		if total, err = total.Add(a); err != nil {
			return Money{}, err
		}
	}
	return total, nil
}
