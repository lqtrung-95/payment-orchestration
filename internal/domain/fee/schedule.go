// Package fee computes what the platform charges a merchant on a capture.
//
// The fee is deducted from what the merchant is owed, never added to what the
// customer pays: the customer authorised one amount and that is the amount
// taken. This is why a capture posts three legs rather than two — the money
// arriving splits between the merchant's payable and the platform's revenue.
package fee

import (
	"errors"
	"fmt"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

// DefaultMerchant is the merchant id under which the platform-wide fallback
// schedule is stored.
const DefaultMerchant = "*"

var (
	ErrNoSchedule       = errors.New("no fee schedule applies")
	ErrFeeExceedsAmount = errors.New("fee exceeds the captured amount")
)

// Schedule is a fee agreement in force over a window.
type Schedule struct {
	MerchantID    string
	Currency      money.Currency
	BasisPoints   int
	FixedMinor    int64
	EffectiveFrom time.Time
	EffectiveTo   *time.Time
}

// Applies reports whether the schedule is in force at an instant. The window is
// half-open: a schedule ending at midnight does not also apply at midnight, or
// two schedules would overlap on the boundary.
func (s Schedule) Applies(at time.Time) bool {
	if at.Before(s.EffectiveFrom) {
		return false
	}
	return s.EffectiveTo == nil || at.Before(*s.EffectiveTo)
}

// Calculate returns the fee on a captured amount.
//
// Percentage plus a fixed component, which is how card pricing actually works:
// the percentage covers interchange risk and the fixed part covers per-message
// cost, so a hundred small payments cost more to process than one large one.
//
// The percentage is rounded half to even for the reasons in money.DivRoundHalfEven —
// rounding every tie the same way would bias the platform's revenue against the
// merchant by a predictable amount, which is precisely the sort of thing that
// surfaces in an audit.
func (s Schedule) Calculate(amount money.Money) (money.Money, error) {
	if amount.Currency() != s.Currency {
		return money.Money{}, fmt.Errorf("fee schedule is %s, amount is %s", s.Currency, amount.Currency())
	}
	if !amount.IsPositive() {
		return money.Money{}, fmt.Errorf("fee on a non-positive amount %s", amount)
	}

	variable, err := amount.MulRatio(int64(s.BasisPoints), 10_000)
	if err != nil {
		return money.Money{}, err
	}

	fixed, err := money.New(s.FixedMinor, s.Currency)
	if err != nil {
		return money.Money{}, err
	}

	total, err := variable.Add(fixed)
	if err != nil {
		return money.Money{}, err
	}

	// A fee at or above the captured amount would leave the merchant owed
	// nothing or less than nothing, and the payable leg of the entry would be
	// zero or negative — which the ledger refuses. Catching it here names the
	// cause instead of surfacing as a posting constraint violation.
	cmp, err := total.Cmp(amount)
	if err != nil {
		return money.Money{}, err
	}
	if cmp >= 0 {
		return money.Money{}, fmt.Errorf("%w: fee %s on capture of %s", ErrFeeExceedsAmount, total, amount)
	}

	return total, nil
}
