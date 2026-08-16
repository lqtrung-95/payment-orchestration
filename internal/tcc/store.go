package tcc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// Store persists coordinator state.
//
// Every method takes a Querier so the caller decides the transaction. The
// coordinator's own writes are single-statement, but the sweeper claims and
// advances a transfer together and needs them to be one.
type Store struct{}

func NewStore() *Store { return &Store{} }

const transferColumns = `
	id, state, source_merchant, source_shard_key, destination_merchant,
	destination_shard_key, amount_minor, currency, idempotency_key,
	timeout_at, attempts, COALESCE(last_error, ''), created_at, resolved_at`

// Create records a transfer in the trying state.
//
// A repeated idempotency key returns the existing transfer rather than starting
// a second one. Two submissions of one intent must not produce two movements,
// and the unique index is what arbitrates between them when they race.
func (s *Store) Create(ctx context.Context, q postgres.Querier, t *Transfer) (*Transfer, error) {
	const query = `
		INSERT INTO tcc_transfers (
			id, source_merchant, source_shard_key, destination_merchant,
			destination_shard_key, amount_minor, currency, idempotency_key, timeout_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING ` + transferColumns

	row := q.QueryRow(ctx, query,
		t.ID, t.SourceMerchant, t.SourceShardKey, t.DestMerchant, t.DestShardKey,
		t.Amount.Amount(), string(t.Amount.Currency()), t.IdempotencyKey, t.TimeoutAt)

	created, err := scanTransfer(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.GetByIdempotencyKey(ctx, q, t.IdempotencyKey)
	}
	if err != nil {
		return nil, fmt.Errorf("create transfer: %w", err)
	}
	return created, nil
}

func (s *Store) Get(ctx context.Context, q postgres.Querier, id uuid.UUID) (*Transfer, error) {
	row := q.QueryRow(ctx, `SELECT `+transferColumns+` FROM tcc_transfers WHERE id = $1`, id)

	t, err := scanTransfer(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrTransferNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("get transfer: %w", err)
	}
	return t, nil
}

func (s *Store) GetByIdempotencyKey(ctx context.Context, q postgres.Querier, key string) (*Transfer, error) {
	row := q.QueryRow(ctx, `SELECT `+transferColumns+` FROM tcc_transfers WHERE idempotency_key = $1`, key)

	t, err := scanTransfer(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: key %s", ErrTransferNotFound, key)
	}
	if err != nil {
		return nil, fmt.Errorf("get transfer by key: %w", err)
	}
	return t, nil
}

// Advance moves a transfer between states, refusing a move that does not start
// from the state the caller believed it was in.
//
// The `from` guard is what makes concurrent coordinators safe. Two instances
// resuming the same stranded transfer both read `trying`; only one Advance to
// `cancelling` reports a row, and the loser stops rather than cancelling a
// transfer the winner has since decided to confirm.
func (s *Store) Advance(ctx context.Context, q postgres.Querier, id uuid.UUID, from, to State) (bool, error) {
	// The casts are load-bearing: $3 is both assigned to an enum column and
	// compared against literals, and without them Postgres refuses to deduce a
	// single type for the parameter.
	const query = `
		UPDATE tcc_transfers
		SET state = $3::tcc_state,
		    resolved_at = CASE WHEN $3::tcc_state IN ('confirmed', 'cancelled')
		                       THEN now() ELSE resolved_at END
		WHERE id = $1 AND state = $2::tcc_state`

	tag, err := q.Exec(ctx, query, id, string(from), string(to))
	if err != nil {
		return false, fmt.Errorf("advance transfer %s from %s to %s: %w", id, from, to, err)
	}
	return tag.RowsAffected() == 1, nil
}

// RecordAttempt notes a failed step. Kept separate from Advance so a retry that
// leaves the state alone still leaves evidence of what went wrong.
func (s *Store) RecordAttempt(ctx context.Context, q postgres.Querier, id uuid.UUID, cause error) error {
	const query = `UPDATE tcc_transfers SET attempts = attempts + 1, last_error = $2 WHERE id = $1`

	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	if _, err := q.Exec(ctx, query, id, msg); err != nil {
		return fmt.Errorf("record transfer attempt: %w", err)
	}
	return nil
}

// Unresolved returns transfers past their deadline that no one has finished.
//
// FOR UPDATE SKIP LOCKED so several sweepers divide the work rather than
// queueing behind each other on the same row.
func (s *Store) Unresolved(ctx context.Context, q postgres.Querier, limit int) ([]*Transfer, error) {
	const query = `
		SELECT ` + transferColumns + `
		FROM tcc_transfers
		WHERE state IN ('trying', 'confirming', 'cancelling')
		  AND timeout_at <= now()
		ORDER BY timeout_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`

	rows, err := q.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("list unresolved transfers: %w", err)
	}
	defer rows.Close()

	var out []*Transfer
	for rows.Next() {
		t, err := scanTransfer(rows)
		if err != nil {
			return nil, fmt.Errorf("scan unresolved transfer: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ExtendDeadline pushes a transfer's deadline out, so a sweeper that has just
// picked one up is not immediately re-picked by the next pass while its
// participants are still being contacted.
func (s *Store) ExtendDeadline(ctx context.Context, q postgres.Querier, id uuid.UUID, d time.Duration) error {
	const query = `UPDATE tcc_transfers SET timeout_at = now() + $2::interval WHERE id = $1`

	interval := fmt.Sprintf("%d milliseconds", d.Milliseconds())
	if _, err := q.Exec(ctx, query, id, interval); err != nil {
		return fmt.Errorf("extend transfer deadline: %w", err)
	}
	return nil
}

func scanTransfer(row pgx.Row) (*Transfer, error) {
	var (
		t        Transfer
		state    string
		minor    int64
		currency string
	)
	if err := row.Scan(&t.ID, &state, &t.SourceMerchant, &t.SourceShardKey,
		&t.DestMerchant, &t.DestShardKey, &minor, &currency, &t.IdempotencyKey,
		&t.TimeoutAt, &t.Attempts, &t.LastError, &t.CreatedAt, &t.ResolvedAt); err != nil {
		return nil, err
	}

	amount, err := money.New(minor, money.Currency(currency))
	if err != nil {
		return nil, fmt.Errorf("transfer %s has an unusable amount: %w", t.ID, err)
	}

	t.State = State(state)
	t.Amount = amount
	return &t, nil
}
