// Package fx persists exchange rates and the locks taken against them.
package fx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/fx"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

var (
	ErrRateNotFound = errors.New("no fx rate for that pair and instant")
	ErrLockNotFound = errors.New("no fx rate lock for that transaction")
)

type Repository struct{}

func NewRepository() *Repository { return &Repository{} }

// RecordRate stores a rate, closing the previous open-ended one for the pair.
//
// Closing the old window is what keeps point-in-time lookup unambiguous. Two
// overlapping open rates would make every historical query a coin flip over
// which one applied.
func (r *Repository) RecordRate(ctx context.Context, q postgres.Querier, rate domain.Rate) error {
	const closePrevious = `
		UPDATE fx_rates SET valid_to = $4
		WHERE base = $1 AND quote = $2 AND source = $3 AND valid_to IS NULL
		  AND valid_from < $4`

	if _, err := q.Exec(ctx, closePrevious,
		string(rate.Base), string(rate.Quote), rate.Source, rate.AsOf,
	); err != nil {
		return fmt.Errorf("close previous fx rate: %w", err)
	}

	const insert = `
		INSERT INTO fx_rates (base, quote, rate_nano, source, valid_from)
		VALUES ($1, $2, $3, $4, $5)`

	if _, err := q.Exec(ctx, insert,
		string(rate.Base), string(rate.Quote), rate.Nano, rate.Source, rate.AsOf,
	); err != nil {
		return fmt.Errorf("insert fx rate: %w", err)
	}
	return nil
}

// RateAt returns the rate in force for a pair at an instant.
func (r *Repository) RateAt(
	ctx context.Context,
	q postgres.Querier,
	base, quote money.Currency,
	at time.Time,
) (domain.Rate, error) {
	const query = `
		SELECT rate_nano, source, valid_from
		FROM fx_rates
		WHERE base = $1 AND quote = $2
		  AND valid_from <= $3
		  AND (valid_to IS NULL OR valid_to > $3)
		ORDER BY valid_from DESC
		LIMIT 1`

	var (
		nano      int64
		source    string
		validFrom time.Time
	)
	err := q.QueryRow(ctx, query, string(base), string(quote), at).Scan(&nano, &source, &validFrom)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Rate{}, fmt.Errorf("%w: %s/%s at %s", ErrRateNotFound, base, quote, at.Format(time.RFC3339))
	}
	if err != nil {
		return domain.Rate{}, fmt.Errorf("query fx rate: %w", err)
	}

	return domain.NewRate(base, quote, nano, source, validFrom)
}

// SaveLock persists a rate lock.
//
// The unique constraint on transaction_id is the enforcement point: a second
// lock for one payment would mean two rates were promised and nothing could say
// which was honoured.
func (r *Repository) SaveLock(ctx context.Context, q postgres.Querier, lock domain.Lock) error {
	const query = `
		INSERT INTO fx_rate_locks
			(id, transaction_id, base, quote, rate_nano, source, locked_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	if _, err := q.Exec(ctx, query,
		lock.ID, lock.TransactionID,
		string(lock.Rate.Base), string(lock.Rate.Quote), lock.Rate.Nano, lock.Rate.Source,
		lock.LockedAt, lock.ExpiresAt,
	); err != nil {
		return fmt.Errorf("insert fx rate lock: %w", err)
	}
	return nil
}

// GetLock returns the lock held for a transaction.
func (r *Repository) GetLock(ctx context.Context, q postgres.Querier, transactionID uuid.UUID) (domain.Lock, error) {
	const query = `
		SELECT id, base, quote, rate_nano, source, locked_at, expires_at
		FROM fx_rate_locks WHERE transaction_id = $1`

	var (
		lock        domain.Lock
		base, quote string
		nano        int64
		source      string
	)
	err := q.QueryRow(ctx, query, transactionID).Scan(
		&lock.ID, &base, &quote, &nano, &source, &lock.LockedAt, &lock.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Lock{}, fmt.Errorf("%w: transaction %s", ErrLockNotFound, transactionID)
	}
	if err != nil {
		return domain.Lock{}, fmt.Errorf("query fx rate lock: %w", err)
	}

	rate, err := domain.NewRate(money.Currency(base), money.Currency(quote), nano, source, lock.LockedAt)
	if err != nil {
		return domain.Lock{}, err
	}
	lock.TransactionID = transactionID
	lock.Rate = rate
	return lock, nil
}
