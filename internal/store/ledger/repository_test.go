package ledger_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	ledgerstore "github.com/lequoctrung/payment-orchestrator/internal/store/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
)

// forcedEntryID is a fixed identifier used by the tests that bypass the domain
// layer to poke the database directly. Each test starts from an empty schema,
// so reusing one value cannot collide.
var forcedEntryID = uuid.MustParse("aaaaaaaa-0000-0000-0000-0000000000ff")

func uuidZero() uuid.UUID { return uuid.Nil }

// accounts creates the clearing and payable pair used by most tests here: the
// two sides of a capture, where a provider owes us funds and we in turn owe the
// merchant.
func accounts(t *testing.T, ctx context.Context, repo *ledgerstore.Repository, q postgres.Querier, currency money.Currency) (domain.Account, domain.Account) {
	t.Helper()

	clearing, err := repo.EnsureAccount(ctx, q, domain.Account{
		Owner:    domain.Owner{Type: "psp", ID: "psp_sim"},
		Purpose:  domain.PurposeClearing,
		Type:     domain.AccountTypeAsset,
		Currency: currency,
		ShardKey: "s01",
	})
	if err != nil {
		t.Fatalf("EnsureAccount(clearing) returned error: %v", err)
	}

	payable, err := repo.EnsureAccount(ctx, q, domain.Account{
		Owner:    domain.Owner{Type: "merchant", ID: "m_1"},
		Purpose:  domain.PurposePayable,
		Type:     domain.AccountTypeLiability,
		Currency: currency,
		ShardKey: "s01",
	})
	if err != nil {
		t.Fatalf("EnsureAccount(payable) returned error: %v", err)
	}

	return clearing, payable
}

func TestEnsureAccountIsIdempotent(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := ledgerstore.NewRepository()
	ctx := context.Background()

	first, _ := accounts(t, ctx, repo, db.Pool(), "USD")
	second, _ := accounts(t, ctx, repo, db.Pool(), "USD")

	// A merchant's first payment in a new currency creates the account; the
	// second must reuse it rather than failing or duplicating.
	if first.ID != second.ID {
		t.Errorf("EnsureAccount produced two ids: %s and %s", first.ID, second.ID)
	}
}

func TestRecordEntryAndDeriveBalance(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := ledgerstore.NewRepository()
	ctx := context.Background()

	clearing, payable := accounts(t, ctx, repo, db.Pool(), "USD")

	entry, err := domain.NewEntry(nil, "s01", "capture", time.Now(),
		domain.Posting{AccountID: clearing.ID, Direction: domain.Debit, Amount: money.MustNew(10_000, "USD")},
		domain.Posting{AccountID: payable.ID, Direction: domain.Credit, Amount: money.MustNew(10_000, "USD")},
	)
	if err != nil {
		t.Fatalf("NewEntry returned error: %v", err)
	}

	if err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return repo.RecordEntry(ctx, tx, entry)
	}); err != nil {
		t.Fatalf("RecordEntry returned error: %v", err)
	}

	// Balances are oriented by account type, so a positive figure always reads
	// as "more of what this account is for" without the caller knowing the sign
	// convention. The asset was debited and the liability credited, so both are
	// positive.
	clearingBalance, err := repo.Balance(ctx, db.Pool(), clearing.ID)
	if err != nil {
		t.Fatalf("Balance(clearing) returned error: %v", err)
	}
	if want := money.MustNew(10_000, "USD"); !clearingBalance.Amount.Equal(want) {
		t.Errorf("clearing balance = %s, want %s", clearingBalance.Amount, want)
	}

	payableBalance, err := repo.Balance(ctx, db.Pool(), payable.ID)
	if err != nil {
		t.Fatalf("Balance(payable) returned error: %v", err)
	}
	if want := money.MustNew(10_000, "USD"); !payableBalance.Amount.Equal(want) {
		t.Errorf("payable balance = %s, want %s", payableBalance.Amount, want)
	}

	// Gross flows are retained alongside the net figure because a break is
	// often visible in the gross when the net happens to match.
	if payableBalance.Credits.Amount() != 10_000 || payableBalance.Debits.Amount() != 0 {
		t.Errorf("payable gross flows = debits %s credits %s, want 0 and 10000",
			payableBalance.Debits, payableBalance.Credits)
	}
}

// The domain constructor rejects an unbalanced entry before it reaches the
// database, so the failure names the offending postings.
func TestNewEntryRejectsImbalanceInDomain(t *testing.T) {
	_, err := domain.NewEntry(nil, "s01", "bad", time.Now(),
		domain.Posting{AccountID: uuidZero(), Direction: domain.Debit, Amount: money.MustNew(1000, "USD")},
		domain.Posting{AccountID: uuidZero(), Direction: domain.Credit, Amount: money.MustNew(999, "USD")},
	)
	if err == nil {
		t.Fatal("NewEntry accepted an unbalanced entry, want error")
	}
	if !strings.Contains(err.Error(), "does not balance") {
		t.Errorf("error = %v, want it to mention the imbalance", err)
	}
}

// Bypassing the domain entirely, the database must still refuse. The check is a
// DEFERRABLE constraint trigger, so the rejection lands at COMMIT rather than
// at the INSERT — an entry is legitimately unbalanced between its first and
// last posting.
func TestDatabaseRejectsUnbalancedEntryAtCommit(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := ledgerstore.NewRepository()
	ctx := context.Background()

	clearing, payable := accounts(t, ctx, repo, db.Pool(), "USD")

	err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO journal_entries (id, shard_key, description) VALUES ($1, 's01', 'forced imbalance')`,
			forcedEntryID,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO postings (entry_id, account_id, direction, amount_minor, currency)
			 VALUES ($1, $2, 'debit', 1000, 'USD'), ($1, $3, 'credit', 999, 'USD')`,
			forcedEntryID, clearing.ID, payable.ID,
		); err != nil {
			return err
		}
		return nil
	})

	if err == nil {
		t.Fatal("database accepted an unbalanced entry, want rejection at commit")
	}
	if !strings.Contains(err.Error(), "does not balance") {
		t.Errorf("error = %v, want it to report the imbalance", err)
	}

	// The whole transaction rolled back, so the ledger is untouched.
	imbalances, err := repo.CheckInvariant(ctx, db.Pool())
	if err != nil {
		t.Fatalf("CheckInvariant returned error: %v", err)
	}
	if len(imbalances) != 0 {
		t.Errorf("ledger imbalanced after a rejected entry: %v", imbalances)
	}
}

func TestDatabaseRejectsEntryWithNoPostings(t *testing.T) {
	db := testsupport.FreshDB(t)
	ctx := context.Background()

	err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO journal_entries (id, shard_key, description) VALUES ($1, 's01', 'empty')`,
			forcedEntryID)
		return err
	})
	if err == nil {
		t.Fatal("database accepted an entry with no postings, want rejection")
	}
	if !strings.Contains(err.Error(), "no postings") {
		t.Errorf("error = %v, want it to report the missing postings", err)
	}
}

// A posting may only touch an account holding the same currency, enforced by a
// composite foreign key rather than by convention.
func TestPostingCurrencyMustMatchAccount(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := ledgerstore.NewRepository()
	ctx := context.Background()

	_, usdPayable := accounts(t, ctx, repo, db.Pool(), "USD")

	err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO journal_entries (id, shard_key, description) VALUES ($1, 's01', 'mixed')`,
			forcedEntryID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO postings (entry_id, account_id, direction, amount_minor, currency)
			 VALUES ($1, $2, 'debit', 100, 'EUR')`,
			forcedEntryID, usdPayable.ID)
		return err
	})
	if err == nil {
		t.Fatal("database accepted a EUR posting to a USD account, want rejection")
	}
}

// The ledger invariant is the single assertion the whole accounting model rests
// on: across every currency, debits equal credits.
func TestCheckInvariantHoldsAcrossManyEntries(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := ledgerstore.NewRepository()
	ctx := context.Background()

	usdClearing, usdPayable := accounts(t, ctx, repo, db.Pool(), "USD")

	feeRevenue, err := repo.EnsureAccount(ctx, db.Pool(), domain.Account{
		Owner:    domain.Owner{Type: "platform", ID: "platform"},
		Purpose:  domain.PurposeFeeRevenue,
		Type:     domain.AccountTypeRevenue,
		Currency: "USD",
		ShardKey: "s01",
	})
	if err != nil {
		t.Fatalf("EnsureAccount(fee) returned error: %v", err)
	}

	// A capture split three ways: the provider owes the gross, the merchant is
	// owed the net, and the platform books the fee. Splitting via Allocate is
	// what keeps the three legs summing exactly.
	gross := money.MustNew(10_000, "USD")
	parts, err := gross.Allocate(97, 3) // 3% fee
	if err != nil {
		t.Fatalf("Allocate returned error: %v", err)
	}
	net, fee := parts[0], parts[1]

	for i := 0; i < 25; i++ {
		entry, err := domain.NewEntry(nil, "s01", "capture with fee", time.Now(),
			domain.Posting{AccountID: usdClearing.ID, Direction: domain.Debit, Amount: gross},
			domain.Posting{AccountID: usdPayable.ID, Direction: domain.Credit, Amount: net},
			domain.Posting{AccountID: feeRevenue.ID, Direction: domain.Credit, Amount: fee},
		)
		if err != nil {
			t.Fatalf("NewEntry returned error: %v", err)
		}
		if err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			return repo.RecordEntry(ctx, tx, entry)
		}); err != nil {
			t.Fatalf("RecordEntry returned error: %v", err)
		}
	}

	imbalances, err := repo.CheckInvariant(ctx, db.Pool())
	if err != nil {
		t.Fatalf("CheckInvariant returned error: %v", err)
	}
	if len(imbalances) != 0 {
		t.Fatalf("ledger does not balance: %v", imbalances)
	}

	payableBalance, err := repo.Balance(ctx, db.Pool(), usdPayable.ID)
	if err != nil {
		t.Fatalf("Balance returned error: %v", err)
	}
	if want := net.Amount() * 25; payableBalance.Amount.Amount() != want {
		t.Errorf("payable balance = %d, want %d", payableBalance.Amount.Amount(), want)
	}
}

func TestPostingsAreImmutable(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := ledgerstore.NewRepository()
	ctx := context.Background()

	clearing, payable := accounts(t, ctx, repo, db.Pool(), "USD")
	entry, err := domain.NewEntry(nil, "s01", "capture", time.Now(),
		domain.Posting{AccountID: clearing.ID, Direction: domain.Debit, Amount: money.MustNew(500, "USD")},
		domain.Posting{AccountID: payable.ID, Direction: domain.Credit, Amount: money.MustNew(500, "USD")},
	)
	if err != nil {
		t.Fatalf("NewEntry returned error: %v", err)
	}
	if err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return repo.RecordEntry(ctx, tx, entry)
	}); err != nil {
		t.Fatalf("RecordEntry returned error: %v", err)
	}

	// Correcting a posting means writing a reversing entry, never editing
	// history: an editable ledger cannot be reconciled against anything.
	if _, err := db.Pool().Exec(ctx, `UPDATE postings SET amount_minor = 1`); err == nil {
		t.Error("database accepted an UPDATE on postings, want rejection")
	}
	if _, err := db.Pool().Exec(ctx, `DELETE FROM postings`); err == nil {
		t.Error("database accepted a DELETE on postings, want rejection")
	}
}
