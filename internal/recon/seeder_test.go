package recon_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	fxdomain "github.com/lequoctrung/payment-orchestrator/internal/domain/fx"
	ledgerdomain "github.com/lequoctrung/payment-orchestrator/internal/domain/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	txdomain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/recon"
	fxstore "github.com/lequoctrung/payment-orchestrator/internal/store/fx"
	ledgerstore "github.com/lequoctrung/payment-orchestrator/internal/store/ledger"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
)

// seeder writes captured payments straight into the ledger.
//
// It posts the same three legs the capture path does rather than inserting rows
// by hand, so reconciliation is tested against the shape the real system
// produces. A fixture that wrote a convenient shape would let the reconciler
// agree with a ledger nothing else could generate.
type seeder struct {
	db     *postgres.DB
	txRepo *txstore.Repository
	ledger *ledgerstore.Repository
	fxRepo *fxstore.Repository
}

func newSeeder(db *postgres.DB) *seeder {
	return &seeder{
		db:     db,
		txRepo: txstore.NewRepository(),
		ledger: ledgerstore.NewRepository(),
		fxRepo: fxstore.NewRepository(),
	}
}

// feeFor mirrors the platform default schedule for USD and EUR: 290bps plus a
// small fixed component.
func feeFor(amount money.Money) money.Money {
	variable, err := amount.MulRatio(290, 10_000)
	if err != nil {
		panic(err)
	}
	fixed := int64(30)
	if amount.Currency() == "EUR" {
		fixed = 25
	}
	total, err := variable.Add(money.MustNew(fixed, amount.Currency()))
	if err != nil {
		panic(err)
	}
	return total
}

// capture creates a transaction, walks it to captured, and posts the entry.
func (s *seeder) capture(t *testing.T, index int, amount money.Money, occurredAt time.Time) recon.LedgerRecord {
	t.Helper()
	ctx := context.Background()

	tx, err := txdomain.New("m_recon", fmt.Sprintf("recon-key-%d-%s", index, uuid.NewString()[:8]), amount)
	if err != nil {
		t.Fatalf("build transaction: %v", err)
	}
	reference := fmt.Sprintf("ch_recon_%d_%s", index, uuid.NewString()[:8])
	tx.PSP = testProvider
	tx.PSPReference = reference

	fee := feeFor(amount)
	payable, err := amount.Sub(fee)
	if err != nil {
		t.Fatalf("payable: %v", err)
	}

	err = s.db.WithTx(ctx, func(ctx context.Context, dbtx pgx.Tx) error {
		if err := s.txRepo.Insert(ctx, dbtx, tx); err != nil {
			return err
		}
		// Walked through the matrix one persisted step at a time; the database
		// trigger refuses a jump, which is behaviour under test elsewhere.
		for _, step := range []txdomain.State{
			txdomain.StateAuthorizing, txdomain.StateAuthorized, txdomain.StateCapturing,
		} {
			if err := tx.TransitionTo(step); err != nil {
				return err
			}
			if err := s.txRepo.Update(ctx, dbtx, tx); err != nil {
				return err
			}
		}
		if err := tx.Capture(amount); err != nil {
			return err
		}
		if err := s.txRepo.Update(ctx, dbtx, tx); err != nil {
			return err
		}

		accounts, err := s.ensureAccounts(ctx, dbtx, tx.ShardKey, amount.Currency())
		if err != nil {
			return err
		}

		entry, err := ledgerdomain.NewEntry(&tx.ID, tx.ShardKey, "seeded capture", occurredAt,
			ledgerdomain.Posting{AccountID: accounts.clearing, Direction: ledgerdomain.Debit, Amount: amount},
			ledgerdomain.Posting{AccountID: accounts.payable, Direction: ledgerdomain.Credit, Amount: payable},
			ledgerdomain.Posting{AccountID: accounts.fee, Direction: ledgerdomain.Credit, Amount: fee},
		)
		if err != nil {
			return err
		}
		return s.ledger.RecordEntry(ctx, dbtx, entry)
	})
	if err != nil {
		t.Fatalf("seed capture: %v", err)
	}

	return recon.LedgerRecord{
		TransactionID:     tx.ID,
		MerchantID:        tx.MerchantID,
		ProviderReference: reference,
		Captured:          amount,
		Fee:               fee,
		CapturedAt:        occurredAt.UTC(),
	}
}

type seededAccounts struct{ clearing, payable, fee uuid.UUID }

func (s *seeder) ensureAccounts(
	ctx context.Context,
	dbtx pgx.Tx,
	shardKey string,
	currency money.Currency,
) (seededAccounts, error) {
	var out seededAccounts

	wanted := []struct {
		into *uuid.UUID
		spec ledgerdomain.Account
	}{
		{&out.clearing, ledgerdomain.Account{
			Owner:   ledgerdomain.Owner{Type: "psp", ID: testProvider},
			Purpose: ledgerdomain.PurposeClearing, Type: ledgerdomain.AccountTypeAsset,
		}},
		{&out.payable, ledgerdomain.Account{
			Owner:   ledgerdomain.Owner{Type: "merchant", ID: "m_recon"},
			Purpose: ledgerdomain.PurposePayable, Type: ledgerdomain.AccountTypeLiability,
		}},
		{&out.fee, ledgerdomain.Account{
			Owner:   ledgerdomain.Owner{Type: "platform", ID: "platform"},
			Purpose: ledgerdomain.PurposeFeeRevenue, Type: ledgerdomain.AccountTypeRevenue,
		}},
	}

	for _, w := range wanted {
		w.spec.Currency = currency
		w.spec.ShardKey = shardKey

		account, err := s.ledger.EnsureAccount(ctx, dbtx, w.spec)
		if err != nil {
			return seededAccounts{}, err
		}
		*w.into = account.ID
	}
	return out, nil
}

// lockRate records the rate promised at authorisation, which settlement drift
// is measured against.
func (s *seeder) lockRate(t *testing.T, transactionID uuid.UUID, base, quote money.Currency, nano int64) {
	t.Helper()

	rate, err := fxdomain.NewRate(base, quote, nano, "test", time.Now().UTC())
	if err != nil {
		t.Fatalf("NewRate: %v", err)
	}
	lock, err := fxdomain.NewLock(transactionID, rate, time.Now().UTC(), time.Hour)
	if err != nil {
		t.Fatalf("NewLock: %v", err)
	}
	if err := s.fxRepo.SaveLock(context.Background(), s.db.Pool(), lock); err != nil {
		t.Fatalf("SaveLock: %v", err)
	}
}
