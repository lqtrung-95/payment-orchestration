package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/outbox"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// Received is the message that carries an accepted delivery to its processing.
//
// It is a pointer into the raw log, not a copy of the event. The processor
// re-reads and re-parses the stored bytes, so a message that waited on a retry
// tier acts on the real payload rather than on a snapshot that a later mapping
// change would have invalidated.
type Received struct {
	RawEventID int64  `json:"raw_event_id"`
	Provider   string `json:"provider"`
	Reference  string `json:"reference"`
}

// Ingestor is the fast path: verify, persist, enqueue, done.
type Ingestor struct {
	db       *postgres.DB
	registry *Registry
	events   *Repository
	outbox   *outbox.Writer
	topics   messaging.Topics
	logger   *slog.Logger
}

func NewIngestor(
	db *postgres.DB,
	registry *Registry,
	events *Repository,
	outboxWriter *outbox.Writer,
	topics messaging.Topics,
	logger *slog.Logger,
) *Ingestor {
	return &Ingestor{
		db: db, registry: registry, events: events,
		outbox: outboxWriter, topics: topics, logger: logger,
	}
}

// Result reports what ingestion did, so the transport can log it without
// changing the response — a duplicate and a first delivery both get 200.
type Result struct {
	RawEventID int64
	Duplicate  bool
}

// Ingest authenticates a delivery, records it, and queues it for processing.
//
// The raw event and the message that will process it are written in one
// database transaction. Persisting the event and then publishing separately
// would leave events that are stored but never processed whenever the process
// dies in the gap, and a payment whose confirmation is sitting unread in a table
// is a payment that never completes.
func (i *Ingestor) Ingest(ctx context.Context, provider string, hdr Headers, body []byte) (Result, error) {
	verifier, err := i.registry.Get(provider)
	if err != nil {
		return Result{}, err
	}

	// Verification comes before anything is written. An unauthenticated payload
	// is not parked, not queued, and not stored: a public endpoint that persists
	// whatever it is sent is a free write amplifier for anyone who finds it.
	if err := verifier.Verify(hdr, body, time.Now()); err != nil {
		return Result{}, err
	}

	event, err := verifier.Parse(body)
	if err != nil {
		return Result{}, err
	}

	// The sequence is taken exactly as the adapter produced it. Substituting
	// anything here — say, treating zero as "absent" and deriving a timestamp —
	// would rewrite a legitimately low sequence into a very high one, turning the
	// oldest event in a batch into the newest and defeating the staleness guard
	// for the one case it exists to catch. Whether a provider has an ordering
	// token at all is the adapter's knowledge, not something to infer here.
	raw := RawEvent{
		Provider:        provider,
		ProviderEventID: event.ProviderEventID,
		Payload:         body,
		Signature:       hdr(verifier.SignatureHeader()),
		Sequence:        event.Sequence,
		OccurredAt:      event.OccurredAt,
		Reference:       event.Reference,
	}

	var result Result
	err = i.db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		id, inserted, err := i.events.Insert(ctx, tx, raw)
		if err != nil {
			return err
		}
		if !inserted {
			// Already seen. Nothing is queued, and the caller still gets 200:
			// a duplicate must never look like an error to a provider, or it
			// retries harder and turns its own redelivery into a flood.
			result.Duplicate = true
			return nil
		}

		result.RawEventID = id

		_, err = i.outbox.Enqueue(ctx, tx, outbox.Message{
			// Derived from the provider's own event identity rather than random,
			// so a redelivery that somehow reached the broker twice is still
			// recognisable as one event by the consumer's deduplication.
			EventID:     eventUUID(provider, event.ProviderEventID),
			AggregateID: eventUUID(provider, event.ProviderEventID),
			// Keyed by charge reference, so every event for one charge lands on
			// one partition and arrives in order in the ordinary case. Ordering
			// is not relied upon — the sequence guard is what makes correctness
			// independent of it — but not needlessly shuffling events means the
			// guard rarely has to fire.
			PartitionKey: event.Reference,
			Topic:        i.topics.Webhook,
			Payload: Received{
				RawEventID: id,
				Provider:   provider,
				Reference:  event.Reference,
			},
		})
		return err
	})
	if err != nil {
		return Result{}, fmt.Errorf("ingest webhook: %w", err)
	}

	return result, nil
}

// webhookEventNamespace scopes the derived event ids. Fixed, because the
// derivation has to produce the same identifier on every process and every
// restart for redelivery to be recognisable.
var webhookEventNamespace = uuid.MustParse("6f1e9b6c-5c1f-4a4e-9f3e-2a3d5b7c8d90")

func eventUUID(provider, providerEventID string) uuid.UUID {
	return uuid.NewSHA1(webhookEventNamespace, []byte(provider+":"+providerEventID))
}
