// Package ledger persists the double-entry ledger.
//
// Repository methods take a Querier so callers decide the transaction boundary.
// Recording an entry must happen in the same transaction as whatever domain
// change it accounts for; splitting them produces a payment with no ledger
// record, or a ledger record for a payment that never happened.
package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

var ErrAccountNotFound = errors.New("ledger account not found")

type Repository struct{}

func NewRepository() *Repository { return &Repository{} }

// EnsureAccount returns the account matching the natural key, creating it if
// absent. Accounts are created on demand because a merchant's first payment in
// a new currency should not require an out-of-band provisioning step.
func (r *Repository) EnsureAccount(ctx context.Context, q postgres.Querier, acct domain.Account) (domain.Account, error) {
	if err := acct.Type.Validate(); err != nil {
		return domain.Account{}, err
	}
	if err := acct.Currency.Validate(); err != nil {
		return domain.Account{}, err
	}

	const query = `
		INSERT INTO ledger_accounts (owner_type, owner_id, purpose, account_type, currency, shard_key)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (owner_type, owner_id, purpose, currency) DO UPDATE
			-- A no-op update so the existing row is returned by RETURNING.
			-- DO NOTHING would return no rows and force a second round trip.
			SET updated_at = ledger_accounts.updated_at
		RETURNING id, owner_type, owner_id, purpose, account_type, currency, shard_key, created_at, updated_at`

	var out domain.Account
	var currency string
	err := q.QueryRow(ctx, query,
		acct.Owner.Type, acct.Owner.ID, string(acct.Purpose),
		string(acct.Type), string(acct.Currency), acct.ShardKey,
	).Scan(
		&out.ID, &out.Owner.Type, &out.Owner.ID, &out.Purpose,
		&out.Type, &currency, &out.ShardKey, &out.CreatedAt, &out.UpdatedAt,
	)
	if err != nil {
		return domain.Account{}, fmt.Errorf("ensure ledger account: %w", err)
	}
	out.Currency = money.Currency(currency)
	return out, nil
}

// RecordEntry writes an entry and its postings.
//
// The balance check runs a third time here — after the domain constructor and
// before the database's deferred trigger — because this is the last point where
// the failure can name the specific postings involved.
func (r *Repository) RecordEntry(ctx context.Context, q postgres.Querier, entry *domain.Entry) error {
	if len(entry.Postings) == 0 {
		return domain.ErrNoPostings
	}

	const insertEntry = `
		INSERT INTO journal_entries (id, transaction_id, shard_key, description, occurred_at)
		VALUES ($1, $2, $3, $4, $5)`

	if _, err := q.Exec(ctx, insertEntry,
		entry.ID, entry.TransactionID, entry.ShardKey, entry.Description, entry.OccurredAt,
	); err != nil {
		return fmt.Errorf("insert journal entry: %w", err)
	}

	const insertPosting = `
		INSERT INTO postings (entry_id, account_id, direction, amount_minor, currency)
		VALUES ($1, $2, $3, $4, $5)`

	for i, p := range entry.Postings {
		if _, err := q.Exec(ctx, insertPosting,
			entry.ID, p.AccountID, string(p.Direction), p.Amount.Amount(), string(p.Amount.Currency()),
		); err != nil {
			return fmt.Errorf("insert posting %d of entry %s: %w", i, entry.ID, err)
		}
	}
	return nil
}

// Balance derives an account's position by aggregating its postings. It is
// oriented by account type, so a positive figure always means "more of what
// this account is for" rather than requiring the caller to know the sign
// convention.
func (r *Repository) Balance(ctx context.Context, q postgres.Querier, accountID uuid.UUID) (domain.Balance, error) {
	const query = `
		SELECT a.account_type,
		       a.currency,
		       COALESCE(SUM(p.amount_minor) FILTER (WHERE p.direction = 'debit'), 0)  AS debits,
		       COALESCE(SUM(p.amount_minor) FILTER (WHERE p.direction = 'credit'), 0) AS credits
		FROM ledger_accounts a
		LEFT JOIN postings p ON p.account_id = a.id
		WHERE a.id = $1
		GROUP BY a.account_type, a.currency`

	var (
		accountType string
		currency    string
		debits      int64
		credits     int64
	)
	err := q.QueryRow(ctx, query, accountID).Scan(&accountType, &currency, &debits, &credits)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Balance{}, fmt.Errorf("%w: %s", ErrAccountNotFound, accountID)
	}
	if err != nil {
		return domain.Balance{}, fmt.Errorf("query balance: %w", err)
	}

	cur := money.Currency(currency)
	net := debits - credits
	if !domain.AccountType(accountType).DebitIncreases() {
		net = -net
	}

	amount, err := money.New(net, cur)
	if err != nil {
		return domain.Balance{}, err
	}
	debitTotal, err := money.New(debits, cur)
	if err != nil {
		return domain.Balance{}, err
	}
	creditTotal, err := money.New(credits, cur)
	if err != nil {
		return domain.Balance{}, err
	}

	return domain.Balance{
		AccountID: accountID,
		Amount:    amount,
		Debits:    debitTotal,
		Credits:   creditTotal,
	}, nil
}
