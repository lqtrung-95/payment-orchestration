// Package invariant continuously asserts the things this system claims cannot
// happen.
//
// A load test that reports 2,000 requests per second while the ledger is
// quietly unbalanced is a failed load test that looks like a passing one. The
// throughput number is only worth anything alongside evidence that correctness
// held while it was being measured, which means the checking has to run *during*
// the run rather than after it.
//
// Every check here is a SQL query against committed state. None of them trust
// application code, because application code is what they exist to catch.
package invariant

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/metrics"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// Result is one pass over the invariants.
type Result struct {
	LedgerImbalance int
	DoubleCharges   int
	LostPayments    int

	CheckedAt time.Time
	Duration  time.Duration
}

// Holds reports whether every invariant is satisfied.
func (r Result) Holds() bool {
	return r.LedgerImbalance == 0 && r.DoubleCharges == 0 && r.LostPayments == 0
}

func (r Result) String() string {
	return fmt.Sprintf("imbalance=%d double_charges=%d lost_payments=%d",
		r.LedgerImbalance, r.DoubleCharges, r.LostPayments)
}

// Checker runs the invariant queries and publishes the results.
//
// Every shard is queried and the counts are summed. Each database holds its own
// merchants' ledger, so a violation on any one of them is a violation of the
// system — and a checker that looked only at shard 0 would report zero while
// another database was unbalanced, which is the specific failure these gauges
// exist to make impossible.
type Checker struct {
	router  *postgres.Router
	metrics *metrics.Metrics
	logger  *slog.Logger
}

func NewChecker(router *postgres.Router, m *metrics.Metrics, logger *slog.Logger) *Checker {
	return &Checker{router: router, metrics: m, logger: logger}
}

// ledgerImbalanceQuery counts entries whose postings do not net to zero within
// a currency.
//
// A DEFERRABLE constraint trigger already makes this impossible, which is
// exactly why it is worth measuring: the check proves the trigger is doing its
// job rather than assuming it. Grouped by currency because an entry may span
// currencies legitimately and each must balance on its own.
const ledgerImbalanceQuery = `
	SELECT count(*) FROM (
		SELECT entry_id
		FROM postings
		GROUP BY entry_id, currency
		HAVING SUM(CASE WHEN direction = 'debit' THEN amount_minor ELSE -amount_minor END) <> 0
	) unbalanced`

// doubleChargeQuery counts transactions the books show as captured more than
// once.
//
// Counted from journal entries rather than from the transaction's own captured
// column: the column is a running total and cannot distinguish one capture of
// 100 from two of 50, whereas a second capture entry against one transaction is
// unambiguous. Partial captures are legitimate, so this looks for repeated
// *entries*, and a merchant capturing an order in two shipments would show
// here — which is why the demo captures in full and the metric is read as "must
// be zero for this workload" rather than as a universal law.
const doubleChargeQuery = `
	SELECT count(*) FROM (
		SELECT e.transaction_id
		FROM journal_entries e
		JOIN postings p ON p.entry_id = e.id
		JOIN ledger_accounts a ON a.id = p.account_id
		WHERE e.transaction_id IS NOT NULL
		  AND a.purpose = 'clearing'
		  AND p.direction = 'debit'
		  AND e.description LIKE 'capture%'
		GROUP BY e.transaction_id
		HAVING count(*) > 1
	) doubled`

// lostPaymentQuery counts transactions the state machine says were captured but
// that have no ledger entry behind them.
//
// Money taken and never accounted for. The capture path writes the state change
// and the entry in one database transaction precisely so this cannot happen;
// the query is the independent confirmation.
const lostPaymentQuery = `
	SELECT count(*)
	FROM payment_transactions t
	WHERE t.state IN ('captured', 'settled', 'partially_refunded', 'refunded')
	  AND NOT EXISTS (
		SELECT 1 FROM journal_entries e WHERE e.transaction_id = t.id
	  )`

// Check runs every invariant once.
func (c *Checker) Check(ctx context.Context) (Result, error) {
	started := time.Now()
	result := Result{CheckedAt: started.UTC()}

	for shard, db := range c.router.Shards() {
		for _, check := range []struct {
			name  string
			query string
			into  *int
		}{
			{"ledger imbalance", ledgerImbalanceQuery, &result.LedgerImbalance},
			{"double charges", doubleChargeQuery, &result.DoubleCharges},
			{"lost payments", lostPaymentQuery, &result.LostPayments},
		} {
			var count int
			if err := db.Pool().QueryRow(ctx, check.query).Scan(&count); err != nil {
				// A shard that cannot be read is reported as an error rather
				// than as zero. Publishing "no violations" when a database was
				// unreachable is the one answer these gauges must never give.
				return Result{}, fmt.Errorf("%s check on shard %d: %w", check.name, shard, err)
			}
			*check.into += count
		}
	}

	result.Duration = time.Since(started)

	if c.metrics != nil {
		c.metrics.LedgerImbalance.Set(float64(result.LedgerImbalance))
		c.metrics.DoubleCharges.Set(float64(result.DoubleCharges))
		c.metrics.LostPayments.Set(float64(result.LostPayments))
	}

	return result, nil
}

// Run checks on an interval until the context is cancelled.
//
// A violation is logged at error level every time it is seen rather than once.
// An invariant that broke and then stopped being mentioned looks identical to
// one that was fixed, and these are the three things about which that confusion
// is least affordable.
func (c *Checker) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	c.logger.InfoContext(ctx, "invariant checker started", slog.Duration("interval", interval))

	for {
		select {
		case <-ctx.Done():
			c.logger.InfoContext(ctx, "invariant checker stopped")
			return nil

		case <-ticker.C:
			result, err := c.Check(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				c.logger.ErrorContext(ctx, "invariant check failed", slog.Any("error", err))
				continue
			}

			if !result.Holds() {
				c.logger.ErrorContext(ctx, "INVARIANT VIOLATED",
					slog.Int("ledger_imbalance", result.LedgerImbalance),
					slog.Int("double_charges", result.DoubleCharges),
					slog.Int("lost_payments", result.LostPayments))
			}
		}
	}
}
