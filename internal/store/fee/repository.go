// Package fee persists merchant fee schedules.
package fee

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/fee"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

type Repository struct{}

func NewRepository() *Repository { return &Repository{} }

// ScheduleFor returns the schedule in force for a merchant and currency at an
// instant, falling back to the platform default.
//
// The fallback is a row in the same table rather than a constant in code, so
// "what were we charging in March" is answerable from data alone — which is the
// question a fee dispute actually asks.
func (r *Repository) ScheduleFor(
	ctx context.Context,
	q postgres.Querier,
	merchantID string,
	currency money.Currency,
	at time.Time,
) (domain.Schedule, error) {
	const query = `
		SELECT merchant_id, currency, basis_points, fixed_minor, effective_from, effective_to
		FROM fee_schedules
		WHERE merchant_id IN ($1, $2)
		  AND currency = $3
		  AND effective_from <= $4
		  AND (effective_to IS NULL OR effective_to > $4)
		-- A merchant's own negotiated schedule outranks the platform default,
		-- and within either, the most recently effective one wins.
		ORDER BY (merchant_id = $1) DESC, effective_from DESC
		LIMIT 1`

	var (
		s        domain.Schedule
		currCode string
	)
	err := q.QueryRow(ctx, query, merchantID, domain.DefaultMerchant, string(currency), at).Scan(
		&s.MerchantID, &currCode, &s.BasisPoints, &s.FixedMinor, &s.EffectiveFrom, &s.EffectiveTo,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Schedule{}, fmt.Errorf("%w: merchant %s, %s at %s",
			domain.ErrNoSchedule, merchantID, currency, at.Format(time.RFC3339))
	}
	if err != nil {
		return domain.Schedule{}, fmt.Errorf("query fee schedule: %w", err)
	}

	s.Currency = money.Currency(currCode)
	return s, nil
}

// Upsert records a schedule, closing any open-ended one it supersedes.
func (r *Repository) Upsert(ctx context.Context, q postgres.Querier, s domain.Schedule) error {
	const closePrevious = `
		UPDATE fee_schedules SET effective_to = $3
		WHERE merchant_id = $1 AND currency = $2 AND effective_to IS NULL
		  AND effective_from < $3`

	if _, err := q.Exec(ctx, closePrevious, s.MerchantID, string(s.Currency), s.EffectiveFrom); err != nil {
		return fmt.Errorf("close previous fee schedule: %w", err)
	}

	const insert = `
		INSERT INTO fee_schedules (merchant_id, currency, basis_points, fixed_minor, effective_from, effective_to)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (merchant_id, currency, effective_from) DO UPDATE
			SET basis_points = EXCLUDED.basis_points,
			    fixed_minor  = EXCLUDED.fixed_minor,
			    effective_to = EXCLUDED.effective_to`

	if _, err := q.Exec(ctx, insert,
		s.MerchantID, string(s.Currency), s.BasisPoints, s.FixedMinor, s.EffectiveFrom, s.EffectiveTo,
	); err != nil {
		return fmt.Errorf("insert fee schedule: %w", err)
	}
	return nil
}
