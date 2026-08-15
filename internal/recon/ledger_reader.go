package recon

import (
	"context"
	"fmt"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// LedgerReader assembles what the books say about captured payments.
type LedgerReader struct{}

func NewLedgerReader() *LedgerReader { return &LedgerReader{} }

// CapturedInWindow returns every payment the ledger recorded as captured.
//
// Read from postings rather than from payment_transactions.captured_minor, and
// the difference is the whole point of reconciling against a ledger. The
// transaction column says what the service intended to record; the postings say
// what was actually accounted for. If those two ever disagree, reconciling
// against the column would compare the provider to our intent and quietly agree
// with itself.
//
// The window is deliberately widened either side of the file's period. A
// capture just outside it is not missing — it settles in the adjacent file —
// and pulling it in is what lets the classifier report timing_cutoff instead of
// crying about a lost payment.
func (r *LedgerReader) CapturedInWindow(
	ctx context.Context,
	q postgres.Querier,
	provider string,
	start, end time.Time,
) ([]LedgerRecord, error) {
	const window = 48 * time.Hour

	const query = `
		SELECT t.id,
		       t.merchant_id,
		       COALESCE(t.psp_reference, ''),
		       e.occurred_at,
		       clearing.currency,
		       -- Gross is what the clearing account was debited: the money the
		       -- provider owes us.
		       SUM(clearing.amount_minor) AS gross_minor,
		       -- Fee is what we credited to our own revenue on the same entry.
		       COALESCE(SUM(fee.amount_minor), 0) AS fee_minor
		FROM journal_entries e
		JOIN payment_transactions t ON t.id = e.transaction_id

		JOIN postings clearing ON clearing.entry_id = e.id AND clearing.direction = 'debit'
		JOIN ledger_accounts clearing_acct
		  ON clearing_acct.id = clearing.account_id
		 AND clearing_acct.purpose = 'clearing'
		 AND clearing_acct.owner_type = 'psp'
		 AND clearing_acct.owner_id = $1

		LEFT JOIN postings fee ON fee.entry_id = e.id AND fee.direction = 'credit'
		LEFT JOIN ledger_accounts fee_acct
		  ON fee_acct.id = fee.account_id AND fee_acct.purpose = 'fee_revenue'

		WHERE e.occurred_at >= $2 AND e.occurred_at < $3
		  AND (fee.id IS NULL OR fee_acct.id IS NOT NULL)
		GROUP BY t.id, t.merchant_id, t.psp_reference, e.occurred_at, clearing.currency
		ORDER BY e.occurred_at`

	rows, err := q.Query(ctx, query, provider, start.Add(-window), end.Add(window))
	if err != nil {
		return nil, fmt.Errorf("read captured payments: %w", err)
	}
	defer rows.Close()

	var out []LedgerRecord
	for rows.Next() {
		var (
			record        LedgerRecord
			currency      string
			gross, feeSum int64
		)
		if err := rows.Scan(&record.TransactionID, &record.MerchantID, &record.ProviderReference,
			&record.CapturedAt, &currency, &gross, &feeSum); err != nil {
			return nil, fmt.Errorf("scan captured payment: %w", err)
		}

		cur := money.Currency(currency)
		if record.Captured, err = money.New(gross, cur); err != nil {
			return nil, err
		}
		if record.Fee, err = money.New(feeSum, cur); err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}
