// Package worker consumes payment work from Kafka and drives it through the
// payment service, routing failures along the retry ladder.
package worker

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// Dedup records which events a consumer group has already handled.
//
// Kafka delivers at least once. Exactly-once *delivery* does not exist — the
// broker cannot know whether a consumer that stopped responding processed the
// record or died before it. Exactly-once *effect* does exist, and here it comes
// from two things composed:
//
//  1. This table, which stops an event being handled a second time in the
//     ordinary case.
//  2. The provider-scoped idempotency key, which makes the provider itself
//     collapse a repeated operation into the original charge.
//
// The second is what actually protects the money. There is a window between a
// handler succeeding and this row being written in which a crash causes
// reprocessing, and no single database transaction can close it, because the
// provider call is not part of any transaction we control. Trying to make this
// table alone the guarantee would be a false comfort.
type Dedup struct {
	group string
}

func NewDedup(consumerGroup string) *Dedup { return &Dedup{group: consumerGroup} }

// AlreadyHandled reports whether this consumer group has processed the event.
func (d *Dedup) AlreadyHandled(ctx context.Context, q postgres.Querier, eventID uuid.UUID) (bool, error) {
	const query = `SELECT EXISTS (
		SELECT 1 FROM processed_events WHERE event_id = $1 AND consumer_group = $2)`

	var exists bool
	if err := q.QueryRow(ctx, query, eventID, d.group).Scan(&exists); err != nil {
		return false, fmt.Errorf("check processed event: %w", err)
	}
	return exists, nil
}

// MarkHandled records the event as processed.
//
// Written only after the work succeeded. Recording it beforehand would mean a
// handler that failed halfway leaves the event marked done, and the retry would
// be discarded as a duplicate — turning a transient failure into a payment that
// silently never happens.
//
// ON CONFLICT DO NOTHING because two deliveries racing is exactly the situation
// this exists for; the second one losing is the correct outcome, not an error.
func (d *Dedup) MarkHandled(ctx context.Context, q postgres.Querier, eventID uuid.UUID) error {
	const query = `
		INSERT INTO processed_events (event_id, consumer_group)
		VALUES ($1, $2)
		ON CONFLICT (event_id, consumer_group) DO NOTHING`

	if _, err := q.Exec(ctx, query, eventID, d.group); err != nil {
		return fmt.Errorf("mark event processed: %w", err)
	}
	return nil
}
