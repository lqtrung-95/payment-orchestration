// Package outbox implements the transactional outbox.
//
// A message is written to the database in the same transaction as the domain
// change that produced it, and relayed to the broker afterwards by a separate
// loop. The alternative arrangements both lose: publishing inside the
// transaction emits events for work that later rolls back, and publishing after
// the commit drops events whenever the process dies in the gap between the two.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// Message is an event awaiting publication.
type Message struct {
	// EventID travels with the message into the broker and is what consumers
	// deduplicate on. It is generated here, before publication, so that a
	// republished message keeps its identity and a consumer can recognise it as
	// the same event rather than a new one.
	EventID uuid.UUID

	AggregateID uuid.UUID

	// PartitionKey becomes the Kafka partition key. Using the merchant means
	// every event for one merchant lands on one partition and is therefore
	// ordered with respect to the others.
	PartitionKey string

	Topic   string
	Payload any
}

type Writer struct{}

func NewWriter() *Writer { return &Writer{} }

// Enqueue writes a message using the caller's querier.
//
// It takes a Querier rather than opening its own transaction precisely so that
// it cannot accidentally commit separately from the domain write. Callers are
// expected to pass the transaction they are already in.
func (w *Writer) Enqueue(ctx context.Context, q postgres.Querier, msg Message) (uuid.UUID, error) {
	if msg.EventID == uuid.Nil {
		msg.EventID = uuid.New()
	}
	if msg.Topic == "" {
		return uuid.Nil, fmt.Errorf("outbox message requires a topic")
	}
	if msg.PartitionKey == "" {
		return uuid.Nil, fmt.Errorf("outbox message requires a partition key")
	}

	payload, err := json.Marshal(msg.Payload)
	if err != nil {
		return uuid.Nil, fmt.Errorf("marshal outbox payload: %w", err)
	}

	// The caller's trace context is stored with the message, so the relay can
	// publish inside the trace that caused the write rather than starting a new
	// one. See tracing.go.
	const query = `
		INSERT INTO outbox (event_id, aggregate_id, partition_key, topic, payload, traceparent)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''))`

	if _, err := q.Exec(ctx, query,
		msg.EventID, msg.AggregateID, msg.PartitionKey, msg.Topic, payload,
		traceparentFrom(ctx),
	); err != nil {
		return uuid.Nil, fmt.Errorf("enqueue outbox message: %w", err)
	}

	return msg.EventID, nil
}

// Record is a claimed outbox row on its way to the broker.
type Record struct {
	ID           int64
	EventID      uuid.UUID
	AggregateID  uuid.UUID
	PartitionKey string
	Topic        string
	Payload      []byte
	Attempts     int
	CreatedAt    time.Time

	// Traceparent is the W3C trace context active when the message was
	// enqueued, so the publish rejoins that trace instead of orphaning itself.
	Traceparent string
}
