package recon

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/fx"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/recon/breaks"
)

// Break is one classified disagreement.
type Break struct {
	Category breaks.Category

	// MatchKey is the natural identity of the disagreement — the provider
	// reference, or the transaction id when there is no settlement row. It is
	// what makes re-running a reconciliation idempotent.
	MatchKey string

	TransactionID   *uuid.UUID
	SettlementRowID *int64

	Expected *money.Money
	Actual   *money.Money
	Delta    *money.Money

	Detail string
}

// Tolerances bound what counts as explained rather than wrong.
type Tolerances struct {
	// FXDriftBps is how far a settlement rate may move from the locked rate and
	// still be called drift rather than an unexplained mismatch. Expressed in
	// basis points of the converted amount.
	FXDriftBps int64

	// TimingWindow is unused by the classifier itself; the caller decides
	// whether an unmatched record settles in an adjacent file. Held here so the
	// whole policy lives in one struct.
	TimingWindowFiles int
}

// DefaultTolerances are deliberately narrow.
//
// A wide tolerance closes real breaks as noise, which is the failure mode that
// matters: an operator who never sees a discrepancy cannot investigate it. The
// threshold is a policy value with an audit trail rather than a constant,
// because widening it is a decision somebody should have to justify.
func DefaultTolerances() Tolerances {
	return Tolerances{FXDriftBps: 50, TimingWindowFiles: 1}
}

// Classifier turns matched and unmatched records into classified breaks.
type Classifier struct {
	tolerances Tolerances
}

func NewClassifier(t Tolerances) *Classifier { return &Classifier{tolerances: t} }

// ClassifyPair examines a matched settlement row against its ledger record,
// returning nil when the two agree.
//
// The order of these checks is the design. A duplicate settlement also presents
// as an amount mismatch, and so does FX drift; testing the specific
// explanations before the general one is what stops every break landing in the
// vaguest bucket that happens to fit. Reordering this function silently
// degrades the taxonomy into a single "amounts differ" category.
func (c *Classifier) ClassifyPair(pair Pair, lock *fx.Lock) (*Break, error) {
	row, record := pair.Row, pair.Record

	// 1. Currencies disagreeing is a routing or configuration fault, and no
	//    amount comparison below would mean anything.
	if row.Gross.Currency() != record.Captured.Currency() {
		return &Break{
			Category:        breaks.CurrencyMismatch,
			MatchKey:        row.ProviderReference,
			TransactionID:   &record.TransactionID,
			SettlementRowID: &row.ID,
			Detail: fmt.Sprintf("settled in %s, captured in %s",
				row.Gross.Currency(), record.Captured.Currency()),
		}, nil
	}

	grossDelta, err := row.Gross.Sub(record.Captured)
	if err != nil {
		return nil, err
	}

	// 2. FX drift, before the generic amount check: the difference is real but
	//    explained, and calling it an unexplained mismatch would send somebody
	//    chasing a provider that did nothing wrong.
	if !grossDelta.IsZero() && row.HasFX() && lock != nil {
		explained, err := c.explainedByFX(*row, *record, *lock)
		if err != nil {
			return nil, err
		}
		if explained {
			return &Break{
				Category:        breaks.FXDrift,
				MatchKey:        row.ProviderReference,
				TransactionID:   &record.TransactionID,
				SettlementRowID: &row.ID,
				Expected:        &record.Captured,
				Actual:          &row.Gross,
				Delta:           &grossDelta,
				Detail: fmt.Sprintf("settled at %d nano against locked %d nano",
					row.SettlementRateNano, lock.Rate.Nano),
			}, nil
		}
	}

	// 3. Gross agrees but the net does not: the provider applied a different
	//    fee than the schedule we booked.
	if grossDelta.IsZero() {
		expectedNet, err := record.Net()
		if err != nil {
			return nil, err
		}
		netDelta, err := row.Net.Sub(expectedNet)
		if err != nil {
			return nil, err
		}
		if netDelta.IsZero() {
			return nil, nil // the two agree
		}
		return &Break{
			Category:        breaks.FeeMismatch,
			MatchKey:        row.ProviderReference,
			TransactionID:   &record.TransactionID,
			SettlementRowID: &row.ID,
			Expected:        &expectedNet,
			Actual:          &row.Net,
			Delta:           &netDelta,
			Detail: fmt.Sprintf("provider kept %s, schedule says %s",
				row.Fee, record.Fee),
		}, nil
	}

	// 4. Everything else: the amounts differ and nothing explains it. This is
	//    the bucket that needs a human, and keeping it last is what keeps it
	//    small.
	return &Break{
		Category:        breaks.AmountMismatch,
		MatchKey:        row.ProviderReference,
		TransactionID:   &record.TransactionID,
		SettlementRowID: &row.ID,
		Expected:        &record.Captured,
		Actual:          &row.Gross,
		Delta:           &grossDelta,
		Detail:          fmt.Sprintf("settled %s against captured %s", row.Gross, record.Captured),
	}, nil
}

// explainedByFX reports whether the gross difference is accounted for by the
// settlement rate differing from the locked one.
//
// The test is not "is the difference small". It is "does applying the rate the
// provider says it used reproduce the figure it sent" — a difference of the
// right size for the wrong reason is still unexplained, and treating size alone
// as the criterion is how a genuine error gets closed as drift.
func (c *Classifier) explainedByFX(row Row, record LedgerRecord, lock fx.Lock) (bool, error) {
	settlementRate, err := fx.NewRate(
		record.Captured.Currency(), row.SettlementCurrency,
		row.SettlementRateNano, "settlement", row.SettledAt,
	)
	if err != nil {
		return false, err
	}

	// What the provider's own rate implies it should have settled.
	implied, err := settlementRate.Convert(record.Captured)
	if err != nil {
		return false, err
	}

	// Compared against what it actually sent, in the settlement currency.
	actual, err := money.New(row.Gross.Amount(), row.SettlementCurrency)
	if err != nil {
		return false, err
	}
	residual, err := actual.Sub(implied)
	if err != nil {
		return false, err
	}

	// A small residual is rounding; a large one means the stated rate does not
	// explain the figure, whatever the size of the original difference.
	tolerance, err := implied.MulRatio(c.tolerances.FXDriftBps, 10_000)
	if err != nil {
		return false, err
	}
	return absMinor(residual) <= absMinor(tolerance), nil
}

func absMinor(m money.Money) int64 {
	if v := m.Amount(); v < 0 {
		return -v
	}
	return m.Amount()
}
