package ledger

import (
	"context"
	"fmt"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// Imbalance is a currency whose total debits and credits disagree. In a correct
// system this slice is always empty; a non-empty result means money has been
// created or destroyed somewhere and every figure derived from the ledger is
// suspect until it is explained.
type Imbalance struct {
	Currency money.Currency
	Debits   int64
	Credits  int64
	Delta    int64 // debits - credits
}

func (i Imbalance) String() string {
	return fmt.Sprintf("%s: debits=%d credits=%d delta=%d", i.Currency, i.Debits, i.Credits, i.Delta)
}

// CheckInvariant verifies that debits equal credits across the whole ledger, per
// currency.
//
// This is the single assertion the entire accounting model rests on. It is
// exposed rather than left to tests because it also runs continuously in
// deployment as a gauge: a load test that passes while this is non-zero is a
// failed load test, not a passing one.
//
// The scan is deliberately unfiltered. Checking a subset can pass while the
// ledger as a whole is broken, which is the case that matters.
func (r *Repository) CheckInvariant(ctx context.Context, q postgres.Querier) ([]Imbalance, error) {
	const query = `
		SELECT currency,
		       COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'debit'), 0)  AS debits,
		       COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'credit'), 0) AS credits
		FROM postings
		GROUP BY currency
		HAVING COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'debit'), 0)
		     <> COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'credit'), 0)`

	rows, err := q.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("check ledger invariant: %w", err)
	}
	defer rows.Close()

	var imbalances []Imbalance
	for rows.Next() {
		var (
			currency string
			debits   int64
			credits  int64
		)
		if err := rows.Scan(&currency, &debits, &credits); err != nil {
			return nil, fmt.Errorf("scan imbalance: %w", err)
		}
		imbalances = append(imbalances, Imbalance{
			Currency: money.Currency(currency),
			Debits:   debits,
			Credits:  credits,
			Delta:    debits - credits,
		})
	}
	// rows.Err reports a failure that happened partway through streaming, which
	// would otherwise look like a clean, balanced result.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate imbalances: %w", err)
	}

	return imbalances, nil
}
