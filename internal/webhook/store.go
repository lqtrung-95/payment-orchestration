package webhook

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
)

var ErrEventNotFound = errors.New("webhook event not found")

// Outcome is what processing decided about an event.
//
// Every one of these is recorded. An event that was not applied is not an event
// that can be discarded: "we received this and deliberately did not act on it"
// is the answer an investigation needs, and it is unavailable if the event is
// dropped.
type Outcome string

const (
	// OutcomeReceived is the initial state: persisted and acknowledged.
	OutcomeReceived Outcome = "received"

	OutcomeApplied Outcome = "applied"

	// OutcomeSuperseded means the event is older than what has already been
	// applied, by its own sequence.
	OutcomeSuperseded Outcome = "superseded"

	// OutcomeRejected means the transition it implied is absent from the state
	// machine — refused structurally rather than by an ad-hoc check.
	OutcomeRejected Outcome = "rejected"

	// OutcomeIgnored means the event was understood and implies no state change.
	OutcomeIgnored Outcome = "ignored"

	// OutcomeUnmatched means no transaction could be correlated within the
	// parking window.
	OutcomeUnmatched Outcome = "unmatched"
)

// RawEvent is a stored delivery.
type RawEvent struct {
	ID              int64
	Provider        string
	ProviderEventID string
	Payload         []byte
	Signature       string
	Sequence        int64
	OccurredAt      time.Time
	Reference       string
	TransactionID   uuid.UUID
	Status          Outcome
	Note            string
	ReceivedAt      time.Time
}

// Normalized re-derives the parsed event from a stored row.
//
// Replay re-parses the stored bytes rather than reading a stored parse. A
// parsed copy would drift the moment the mapping changed, and the point of
// keeping raw bytes is that the interpretation can be corrected later.
func (e RawEvent) Normalized(v Verifier) (*Event, error) { return v.Parse(e.Payload) }

type Repository struct{}

func NewRepository() *Repository { return &Repository{} }

// Insert stores a delivery, reporting whether it was new.
//
// The unique constraint is the deduplication point, and a conflict is an
// ordinary outcome rather than an error: a provider retrying a delivery it
// believes failed is expected behaviour, and it must be answered 200. Returning
// an error here would surface a routine retry as a failure and cause the
// provider to retry harder.
func (r *Repository) Insert(ctx context.Context, q postgres.Querier, e RawEvent) (int64, bool, error) {
	const query = `
		INSERT INTO webhook_events_raw
			(provider, provider_event_id, payload, signature, sequence, occurred_at, reference)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (provider, provider_event_id) DO NOTHING
		RETURNING id`

	var id int64
	err := q.QueryRow(ctx, query,
		e.Provider, e.ProviderEventID, e.Payload, e.Signature,
		e.Sequence, e.OccurredAt, e.Reference,
	).Scan(&id)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("insert webhook event: %w", err)
	}
	return id, true, nil
}

const rawEventColumns = `
	id, provider, provider_event_id, payload, signature, sequence, occurred_at,
	reference, COALESCE(transaction_id, '00000000-0000-0000-0000-000000000000'::uuid),
	status, COALESCE(note, ''), received_at`

func (r *Repository) Get(ctx context.Context, q postgres.Querier, id int64) (*RawEvent, error) {
	query := `SELECT ` + rawEventColumns + ` FROM webhook_events_raw WHERE id = $1`
	return scanRawEvent(q.QueryRow(ctx, query, id))
}

// GetForUpdate reads the row holding a lock, so two deliveries of the same event
// racing through processing cannot both decide they are the one applying it.
func (r *Repository) GetForUpdate(ctx context.Context, q postgres.Querier, id int64) (*RawEvent, error) {
	query := `SELECT ` + rawEventColumns + ` FROM webhook_events_raw WHERE id = $1 FOR UPDATE`
	return scanRawEvent(q.QueryRow(ctx, query, id))
}

// Resolve records what processing decided.
//
// Written in the same database transaction as the state change it accompanies,
// so the log cannot claim an event was applied when the transaction it targeted
// rolled back.
func (r *Repository) Resolve(
	ctx context.Context,
	q postgres.Querier,
	id int64,
	outcome Outcome,
	transactionID uuid.UUID,
	note string,
) error {
	const query = `
		UPDATE webhook_events_raw
		SET status = $2, note = NULLIF($3, ''), processed_at = now(),
		    transaction_id = COALESCE($4, transaction_id)
		WHERE id = $1`

	var txID *uuid.UUID
	if transactionID != uuid.Nil {
		txID = &transactionID
	}
	if _, err := q.Exec(ctx, query, id, string(outcome), note, txID); err != nil {
		return fmt.Errorf("resolve webhook event %d: %w", id, err)
	}
	return nil
}

// ListForReplay returns stored events in the provider's own order, which is what
// a replay has to follow: arrival order is the thing being tested for
// irrelevance, so replaying in it would prove nothing.
func (r *Repository) ListForReplay(ctx context.Context, q postgres.Querier) ([]RawEvent, error) {
	query := `SELECT ` + rawEventColumns + ` FROM webhook_events_raw ORDER BY reference, sequence, id`

	rows, err := q.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list webhook events: %w", err)
	}
	defer rows.Close()

	var out []RawEvent
	for rows.Next() {
		e, err := scanRawEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

func scanRawEvent(row pgx.Row) (*RawEvent, error) {
	var (
		e      RawEvent
		status string
	)
	err := row.Scan(
		&e.ID, &e.Provider, &e.ProviderEventID, &e.Payload, &e.Signature,
		&e.Sequence, &e.OccurredAt, &e.Reference, &e.TransactionID,
		&status, &e.Note, &e.ReceivedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrEventNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan webhook event: %w", err)
	}
	e.Status = Outcome(status)
	return &e, nil
}

// statusImpliesNoChange reports whether a provider status is one this receiver
// understands but does not act on.
func statusImpliesNoChange(s psp.Status) bool {
	return s == psp.StatusPending || s == psp.StatusRequiresAction
}
