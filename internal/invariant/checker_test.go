package invariant_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	ledgerdomain "github.com/lequoctrung/payment-orchestrator/internal/domain/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	txdomain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/invariant"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/metrics"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	ledgerstore "github.com/lequoctrung/payment-orchestrator/internal/store/ledger"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
)

func newChecker(t *testing.T) (*invariant.Checker, *postgres.DB) {
	t.Helper()

	router := testsupport.FreshRouter(t, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return invariant.NewChecker(router, metrics.New(), logger), router.Global()
}

// capturedTransaction walks a transaction to captured. postEntry decides whether
// the ledger entry that should accompany it is written — which is the whole
// point: the checker exists to notice when it is not.
func capturedTransaction(t *testing.T, db *postgres.DB, postEntry bool, entries int) uuid.UUID {
	t.Helper()
	ctx := context.Background()

	repo := txstore.NewRepository()
	ledger := ledgerstore.NewRepository()
	amount := money.MustNew(100_00, "USD")

	tx, err := txdomain.New("m_invariant", "inv-"+uuid.NewString()[:8], amount)
	if err != nil {
		t.Fatalf("build transaction: %v", err)
	}

	err = db.WithTx(ctx, func(ctx context.Context, dbtx pgx.Tx) error {
		if err := repo.Insert(ctx, dbtx, tx); err != nil {
			return err
		}
		for _, step := range []txdomain.State{
			txdomain.StateAuthorizing, txdomain.StateAuthorized, txdomain.StateCapturing,
		} {
			if err := tx.TransitionTo(step); err != nil {
				return err
			}
			if err := repo.Update(ctx, dbtx, tx); err != nil {
				return err
			}
		}
		if err := tx.Capture(amount); err != nil {
			return err
		}
		if err := repo.Update(ctx, dbtx, tx); err != nil {
			return err
		}
		if !postEntry {
			return nil
		}

		clearing, err := ledger.EnsureAccount(ctx, dbtx, ledgerdomain.Account{
			Owner:    ledgerdomain.Owner{Type: "psp", ID: "psp-test"},
			Purpose:  ledgerdomain.PurposeClearing,
			Type:     ledgerdomain.AccountTypeAsset,
			Currency: "USD", ShardKey: tx.ShardKey,
		})
		if err != nil {
			return err
		}
		payable, err := ledger.EnsureAccount(ctx, dbtx, ledgerdomain.Account{
			Owner:    ledgerdomain.Owner{Type: "merchant", ID: "m_invariant"},
			Purpose:  ledgerdomain.PurposePayable,
			Type:     ledgerdomain.AccountTypeLiability,
			Currency: "USD", ShardKey: tx.ShardKey,
		})
		if err != nil {
			return err
		}

		for i := 0; i < entries; i++ {
			entry, err := ledgerdomain.NewEntry(&tx.ID, tx.ShardKey, "capture of 100.00 USD", time.Now().UTC(),
				ledgerdomain.Posting{AccountID: clearing.ID, Direction: ledgerdomain.Debit, Amount: amount},
				ledgerdomain.Posting{AccountID: payable.ID, Direction: ledgerdomain.Credit, Amount: amount},
			)
			if err != nil {
				return err
			}
			if err := ledger.RecordEntry(ctx, dbtx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed transaction: %v", err)
	}
	return tx.ID
}

func TestHealthyLedgerSatisfiesEveryInvariant(t *testing.T) {
	checker, db := newChecker(t)
	capturedTransaction(t, db, true, 1)

	result, err := checker.Check(context.Background())
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !result.Holds() {
		t.Errorf("invariants failed on a healthy ledger: %s", result)
	}
}

// The test that stops the checker being decoration. A checker that always
// returns zero passes every load test ever run against it, and the passing
// result is worth precisely nothing.
func TestCheckerDetectsAMissingLedgerEntry(t *testing.T) {
	checker, db := newChecker(t)

	// Captured according to the state machine, with nothing in the ledger —
	// money taken and never accounted for.
	capturedTransaction(t, db, false, 0)

	result, err := checker.Check(context.Background())
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if result.LostPayments != 1 {
		t.Errorf("lost payments = %d, want 1 — the checker did not notice", result.LostPayments)
	}
	if result.Holds() {
		t.Error("Holds() reported true with a lost payment")
	}
}

func TestCheckerDetectsADoubleCapture(t *testing.T) {
	checker, db := newChecker(t)

	// Two capture entries against one transaction: the books say the customer
	// was charged twice.
	capturedTransaction(t, db, true, 2)

	result, err := checker.Check(context.Background())
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if result.DoubleCharges != 1 {
		t.Errorf("double charges = %d, want 1 — the checker did not notice", result.DoubleCharges)
	}
	if result.Holds() {
		t.Error("Holds() reported true with a double capture")
	}
}

// The ledger imbalance check can only be exercised against a database that
// permits an unbalanced entry, and this one does not: a deferred constraint
// trigger rejects it at COMMIT. That refusal is the real assertion, and it is
// worth stating explicitly — the metric exists to prove the trigger is doing
// its job, not to catch something the trigger allows through.
func TestUnbalancedEntryIsRefusedBeforeItCanBeCounted(t *testing.T) {
	_, db := newChecker(t)
	ctx := context.Background()

	ledger := ledgerstore.NewRepository()
	err := db.WithTx(ctx, func(ctx context.Context, dbtx pgx.Tx) error {
		account, err := ledger.EnsureAccount(ctx, dbtx, ledgerdomain.Account{
			Owner:    ledgerdomain.Owner{Type: "psp", ID: "psp-test"},
			Purpose:  ledgerdomain.PurposeClearing,
			Type:     ledgerdomain.AccountTypeAsset,
			Currency: "USD", ShardKey: "shard-0",
		})
		if err != nil {
			return err
		}

		// A single debit with no matching credit, inserted directly so the
		// domain constructor cannot reject it first.
		entryID := uuid.New()
		if _, err := dbtx.Exec(ctx,
			`INSERT INTO journal_entries (id, shard_key, description) VALUES ($1, $2, $3)`,
			entryID, "shard-0", "deliberately unbalanced"); err != nil {
			return err
		}
		_, err = dbtx.Exec(ctx,
			`INSERT INTO postings (entry_id, account_id, direction, amount_minor, currency)
			 VALUES ($1, $2, 'debit', 5000, 'USD')`, entryID, account.ID)
		return err
	})

	if err == nil {
		t.Fatal("an unbalanced entry was committed; the deferred trigger is not enforcing balance")
	}
}
