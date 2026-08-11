// Package transaction persists the payment transaction aggregate.
package transaction

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

var (
	ErrNotFound = errors.New("transaction not found")

	// ErrVersionConflict means another writer modified the row since it was
	// read. The caller must re-read and decide again rather than retrying the
	// same write, whose preconditions no longer hold.
	ErrVersionConflict = errors.New("transaction version conflict")
)

type Repository struct{}

func NewRepository() *Repository { return &Repository{} }

const selectColumns = `
	id, merchant_id, shard_key, idempotency_key, state,
	amount_minor, currency, captured_minor, refunded_minor,
	COALESCE(psp, ''), COALESCE(psp_reference, ''),
	version, created_at, updated_at`

func (r *Repository) Insert(ctx context.Context, q postgres.Querier, t *domain.Transaction) error {
	const query = `
		INSERT INTO payment_transactions (
			id, merchant_id, shard_key, idempotency_key, state,
			amount_minor, currency, captured_minor, refunded_minor, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	_, err := q.Exec(ctx, query,
		t.ID, t.MerchantID, t.ShardKey, t.IdempotencyKey, string(t.State),
		t.Amount.Amount(), string(t.Amount.Currency()),
		t.Captured.Amount(), t.Refunded.Amount(), t.Version,
	)
	if err != nil {
		return fmt.Errorf("insert transaction: %w", err)
	}
	return nil
}

func (r *Repository) Get(ctx context.Context, q postgres.Querier, id uuid.UUID) (*domain.Transaction, error) {
	query := `SELECT ` + selectColumns + ` FROM payment_transactions WHERE id = $1`
	return scanTransaction(q.QueryRow(ctx, query, id))
}

// GetForUpdate reads the row holding a lock until the surrounding transaction
// ends. Used where a decision depends on current state and must not race — the
// version column alone detects a conflict but does not prevent the wasted work.
func (r *Repository) GetForUpdate(ctx context.Context, q postgres.Querier, id uuid.UUID) (*domain.Transaction, error) {
	query := `SELECT ` + selectColumns + ` FROM payment_transactions WHERE id = $1 FOR UPDATE`
	return scanTransaction(q.QueryRow(ctx, query, id))
}

// Update writes the aggregate using optimistic locking, and bumps the version.
//
// The version in the WHERE clause is what makes two concurrent captures safe:
// both read version N, both attempt the update, and exactly one matches. The
// loser gets ErrVersionConflict rather than silently overwriting.
func (r *Repository) Update(ctx context.Context, q postgres.Querier, t *domain.Transaction) error {
	const query = `
		UPDATE payment_transactions SET
			state = $1,
			captured_minor = $2,
			refunded_minor = $3,
			psp = NULLIF($4, ''),
			psp_reference = NULLIF($5, ''),
			version = version + 1
		WHERE id = $6 AND version = $7`

	tag, err := q.Exec(ctx, query,
		string(t.State), t.Captured.Amount(), t.Refunded.Amount(),
		t.PSP, t.PSPReference, t.ID, t.Version,
	)
	if err != nil {
		return fmt.Errorf("update transaction %s: %w", t.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: transaction %s at version %d", ErrVersionConflict, t.ID, t.Version)
	}

	t.Version++
	return nil
}

// StateChange is one row of the append-only audit trail.
type StateChange struct {
	TransactionID uuid.UUID
	From          domain.State // empty on the row recording creation
	To            domain.State
	Reason        string
	Actor         string
	SourceIP      string
}

// RecordStateChange appends to the audit trail. It is written in the same
// transaction as the state change itself, so the trail cannot disagree with the
// row it describes.
func (r *Repository) RecordStateChange(ctx context.Context, q postgres.Querier, c StateChange) error {
	const query = `
		INSERT INTO transaction_state_changes
			(transaction_id, from_state, to_state, reason, actor, source_ip)
		VALUES ($1, NULLIF($2, '')::transaction_state, $3, NULLIF($4, ''), $5, NULLIF($6, '')::inet)`

	if _, err := q.Exec(ctx, query,
		c.TransactionID, string(c.From), string(c.To), c.Reason, c.Actor, c.SourceIP,
	); err != nil {
		return fmt.Errorf("record state change: %w", err)
	}
	return nil
}

func scanTransaction(row pgx.Row) (*domain.Transaction, error) {
	var (
		t        domain.Transaction
		state    string
		currency string
		amount   int64
		captured int64
		refunded int64
	)

	err := row.Scan(
		&t.ID, &t.MerchantID, &t.ShardKey, &t.IdempotencyKey, &state,
		&amount, &currency, &captured, &refunded,
		&t.PSP, &t.PSPReference, &t.Version, &t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan transaction: %w", err)
	}

	cur := money.Currency(currency)
	if t.Amount, err = money.New(amount, cur); err != nil {
		return nil, err
	}
	if t.Captured, err = money.New(captured, cur); err != nil {
		return nil, err
	}
	if t.Refunded, err = money.New(refunded, cur); err != nil {
		return nil, err
	}
	t.State = domain.State(state)

	return &t, nil
}
