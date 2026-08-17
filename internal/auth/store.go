package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// Record is a stored key. The secret is not part of it and never was.
type Record struct {
	ID         uuid.UUID
	MerchantID string
	Public     string
	Name       string
	CreatedAt  time.Time
	LastUsedAt *time.Time
	RevokedAt  *time.Time
}

// Store reads and writes api_keys.
type Store struct{}

func NewStore() *Store { return &Store{} }

// touchInterval is how stale last_used_at is allowed to get before a
// verification bothers to update it.
//
// Writing it on every request would put a row update on the hot path of every
// authenticated call — the busiest write in the system, in service of a column
// nobody reads in real time. An hour's resolution answers the only question it
// exists for: is this key still in use, or is it safe to revoke.
const touchInterval = time.Hour

// Issue stores a freshly generated key and returns the record.
func (s *Store) Issue(ctx context.Context, q postgres.Querier, merchantID, name string, key Key) (*Record, error) {
	const query = `
		INSERT INTO api_keys (merchant_id, key_prefix, key_hash, name)
		VALUES ($1, $2, $3, $4)
		RETURNING id, merchant_id, key_prefix, name, created_at, last_used_at, revoked_at`

	row := q.QueryRow(ctx, query, merchantID, key.Public, key.Hash, name)

	rec, err := scanRecord(row)
	if err != nil {
		return nil, fmt.Errorf("issue api key: %w", err)
	}
	return rec, nil
}

// Verify resolves a presented key to the merchant it belongs to.
//
// The comparison is constant time. A byte-by-byte compare that returns early
// leaks how much of a guess was correct, which over enough attempts recovers
// the key — and this is the one comparison in the system where that matters.
//
// A revoked key is treated as not found rather than as a distinct answer. The
// caller learns only that it cannot proceed.
func (s *Store) Verify(ctx context.Context, db *postgres.DB, presented string) (*Record, error) {
	public, err := PublicPart(presented)
	if err != nil {
		return nil, err
	}

	const query = `
		SELECT id, merchant_id, key_prefix, key_hash, name, created_at, last_used_at, revoked_at
		FROM api_keys
		WHERE key_prefix = $1`

	var (
		rec    Record
		stored []byte
	)
	err = db.Pool().QueryRow(ctx, query, public).Scan(
		&rec.ID, &rec.MerchantID, &rec.Public, &stored, &rec.Name,
		&rec.CreatedAt, &rec.LastUsedAt, &rec.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("look up api key: %w", err)
	}

	if subtle.ConstantTimeCompare(stored, Digest(presented)) != 1 {
		return nil, ErrKeyNotFound
	}
	if rec.RevokedAt != nil {
		return nil, ErrKeyNotFound
	}

	s.touch(ctx, db, &rec)
	return &rec, nil
}

// touch records use, at most once per touchInterval.
//
// Failures are swallowed on purpose. This is bookkeeping: refusing an
// authenticated request because a timestamp could not be written would turn a
// convenience into an outage.
func (s *Store) touch(ctx context.Context, db *postgres.DB, rec *Record) {
	if rec.LastUsedAt != nil && time.Since(*rec.LastUsedAt) < touchInterval {
		return
	}
	const query = `UPDATE api_keys SET last_used_at = now() WHERE id = $1`
	_, _ = db.Pool().Exec(ctx, query, rec.ID)
}

// List returns a merchant's keys, or every key when merchantID is empty.
func (s *Store) List(ctx context.Context, q postgres.Querier, merchantID string) ([]*Record, error) {
	const query = `
		SELECT id, merchant_id, key_prefix, name, created_at, last_used_at, revoked_at
		FROM api_keys
		WHERE ($1 = '' OR merchant_id = $1)
		ORDER BY created_at DESC`

	rows, err := q.Query(ctx, query, merchantID)
	if err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	defer rows.Close()

	var out []*Record
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("scan api key: %w", err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// Revoke disables a key by its public half. Returns whether anything changed,
// so revoking twice can be reported honestly rather than as a fresh success.
func (s *Store) Revoke(ctx context.Context, q postgres.Querier, public string) (bool, error) {
	const query = `UPDATE api_keys SET revoked_at = now() WHERE key_prefix = $1 AND revoked_at IS NULL`

	tag, err := q.Exec(ctx, query, public)
	if err != nil {
		return false, fmt.Errorf("revoke api key: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func scanRecord(row pgx.Row) (*Record, error) {
	var rec Record
	if err := row.Scan(&rec.ID, &rec.MerchantID, &rec.Public, &rec.Name,
		&rec.CreatedAt, &rec.LastUsedAt, &rec.RevokedAt); err != nil {
		return nil, err
	}
	return &rec, nil
}
