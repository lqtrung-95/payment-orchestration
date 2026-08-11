package idempotency

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

var (
	ErrNotFound = errors.New("idempotency key not found")

	// ErrClaimLost means the caller's claim lapsed and was taken over by another
	// request. Its result must be discarded rather than written.
	ErrClaimLost = errors.New("idempotency claim no longer held")
)

type State string

const (
	StateInFlight  State = "in_flight"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
)

// Outcome tells the caller what to do with an incoming request.
type Outcome int

const (
	// OutcomeAcquired means this caller owns the key and should do the work.
	OutcomeAcquired Outcome = iota

	// OutcomeReplay means the request already ran to a definitive answer; the
	// stored response must be returned verbatim rather than re-executed.
	OutcomeReplay

	// OutcomeInFlight means an identical request is running right now. The
	// caller is told to retry later rather than being allowed to start a second
	// attempt, which is the case that produces double charges.
	OutcomeInFlight

	// OutcomeFingerprintMismatch means the key was reused with a different
	// body. Replaying the first response would silently discard this request.
	OutcomeFingerprintMismatch
)

type Record struct {
	ID             uuid.UUID
	MerchantID     string
	Key            string
	Fingerprint    []byte
	State          State
	ResponseStatus int
	ResponseBody   []byte
	TransactionID  *uuid.UUID

	// ClaimToken must be presented to Complete. See Complete for why.
	ClaimToken uuid.UUID

	LockedAt  time.Time
	ExpiresAt time.Time
}

type ClaimResult struct {
	Outcome Outcome
	Record  *Record
}

// Repository arbitrates key ownership.
//
// LockTTL bounds how long an in-flight claim is honoured before another request
// may take it over; TTL is how long a completed record can answer a retry.
type Repository struct {
	LockTTL time.Duration
	TTL     time.Duration
}

func NewRepository(lockTTL, ttl time.Duration) *Repository {
	return &Repository{LockTTL: lockTTL, TTL: ttl}
}

// Claim attempts to take ownership of a key.
//
// Ownership is decided by the unique constraint inside the transaction, not by
// a read-then-write, because any gap between checking and inserting is exactly
// where two concurrent requests both conclude they may proceed.
//
// A claim whose owner died mid-request would otherwise block that key forever,
// so a claim older than LockTTL can be taken over. LockTTL must therefore
// exceed the maximum request duration; the safety of the takeover ultimately
// rests on the provider call itself being idempotent, which the per-provider
// key in a later phase supplies.
func (r *Repository) Claim(ctx context.Context, q postgres.Querier, merchantID, key string, fingerprint []byte) (ClaimResult, error) {
	const claim = `
		INSERT INTO idempotency_keys (merchant_id, key, request_fingerprint, state, locked_at, expires_at)
		VALUES ($1, $2, $3, 'in_flight', now(), now() + $4::interval)
		ON CONFLICT (merchant_id, key) DO UPDATE
			SET locked_at = now(),
			    state = 'in_flight',
			    request_fingerprint = EXCLUDED.request_fingerprint,
			    expires_at = EXCLUDED.expires_at,
			    -- A fresh token fences out the previous owner, whose claim has
			    -- lapsed but whose process may still be running.
			    claim_token = gen_random_uuid()
			WHERE idempotency_keys.state = 'in_flight'
			  AND idempotency_keys.locked_at < now() - $5::interval
		RETURNING id, merchant_id, key, request_fingerprint, state,
		          COALESCE(response_status, 0), COALESCE(response_body, ''::bytea),
		          transaction_id, claim_token, locked_at, expires_at`

	rec, err := scanRecord(q.QueryRow(ctx, claim,
		merchantID, key, fingerprint,
		intervalString(r.TTL), intervalString(r.LockTTL),
	))
	if err == nil {
		return ClaimResult{Outcome: OutcomeAcquired, Record: rec}, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return ClaimResult{}, err
	}

	// No row came back, so an existing record blocked the claim. Read it to
	// decide which of the remaining outcomes applies.
	existing, err := r.Get(ctx, q, merchantID, key)
	if err != nil {
		return ClaimResult{}, err
	}

	// The fingerprint is checked before anything else: a mismatch means this is
	// a different request wearing the same key, and neither replaying the
	// stored response nor executing it as new is safe.
	if !bytes.Equal(existing.Fingerprint, fingerprint) {
		return ClaimResult{Outcome: OutcomeFingerprintMismatch, Record: existing}, nil
	}

	if existing.State == StateInFlight {
		return ClaimResult{Outcome: OutcomeInFlight, Record: existing}, nil
	}
	return ClaimResult{Outcome: OutcomeReplay, Record: existing}, nil
}

// Complete stores the definitive response for a key.
//
// Both success and failure are recorded, because a retry must receive the same
// answer either way. A client that got a decline and retries the same key is
// entitled to the decline, not to a second attempt at the card.
//
// The claim token from Claim must be presented. A caller whose claim lapsed and
// was taken over holds a stale token and is refused, which is what stops a
// process that stalled past the lock TTL from overwriting the result of the
// request that legitimately replaced it.
func (r *Repository) Complete(ctx context.Context, q postgres.Querier, merchantID, key string, token uuid.UUID, status int, body []byte, transactionID *uuid.UUID) error {
	state := StateCompleted
	if status >= 400 {
		state = StateFailed
	}

	const query = `
		UPDATE idempotency_keys
		SET state = $1, response_status = $2, response_body = $3, transaction_id = $4, completed_at = now()
		WHERE merchant_id = $5 AND key = $6 AND state = 'in_flight' AND claim_token = $7`

	tag, err := q.Exec(ctx, query, string(state), status, body, transactionID, merchantID, key, token)
	if err != nil {
		return fmt.Errorf("complete idempotency key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: claim on %q is no longer held by this caller", ErrClaimLost, key)
	}
	return nil
}

func (r *Repository) Get(ctx context.Context, q postgres.Querier, merchantID, key string) (*Record, error) {
	const query = `
		SELECT id, merchant_id, key, request_fingerprint, state,
		       COALESCE(response_status, 0), COALESCE(response_body, ''::bytea),
		       transaction_id, claim_token, locked_at, expires_at
		FROM idempotency_keys
		WHERE merchant_id = $1 AND key = $2`

	return scanRecord(q.QueryRow(ctx, query, merchantID, key))
}

func scanRecord(row pgx.Row) (*Record, error) {
	var rec Record
	var state string

	err := row.Scan(
		&rec.ID, &rec.MerchantID, &rec.Key, &rec.Fingerprint, &state,
		&rec.ResponseStatus, &rec.ResponseBody, &rec.TransactionID,
		&rec.ClaimToken, &rec.LockedAt, &rec.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan idempotency record: %w", err)
	}

	rec.State = State(state)
	return &rec, nil
}

// intervalString renders a duration for Postgres interval casting. Seconds are
// used rather than Go's duration syntax, which Postgres does not parse.
func intervalString(d time.Duration) string {
	return fmt.Sprintf("%d seconds", int64(d.Seconds()))
}
