package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/metrics"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
)

// dispatcher is the machinery every handler shares: deduplication, the retry
// ladder, and the dead letter queue.
//
// Shared rather than reimplemented per handler, because these are the parts
// where a subtle divergence is invisible until it matters — one handler
// forgetting to carry the attempt count forward looks like nothing at all until
// a message retries without limit.
type dispatcher struct {
	db       *postgres.DB
	producer *messaging.Producer
	topics   messaging.Topics
	dedup    *Dedup
	metrics  *metrics.Metrics
	logger   *slog.Logger
}

// scheduleRetry moves a message to the next rung, or to the DLQ once the ladder
// is exhausted.
func (d *dispatcher) scheduleRetry(
	ctx context.Context,
	msg messaging.Message,
	decision psp.RetryDecision,
	cause error,
) error {
	nextAttempt := msg.Attempt + 1

	maxAttempts := decision.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = d.topics.MaxRetryAttempts()
	}
	if nextAttempt > maxAttempts {
		return d.sendToDLQ(ctx, msg, fmt.Sprintf("exhausted %d attempts: %v", maxAttempts, cause))
	}

	// A provider asking to be left alone gets the slower rungs immediately;
	// retrying a rate limit at the fast pace is what keeps the limit engaged.
	tierIndex := nextAttempt
	if decision.LongBackoff && tierIndex < 3 {
		tierIndex = 3
	}

	tier, ok := d.topics.TierForAttempt(tierIndex)
	if !ok {
		return d.sendToDLQ(ctx, msg, fmt.Sprintf("no retry tier for attempt %d: %v", tierIndex, cause))
	}

	d.metrics.RecordRetry(tier.Topic, nextAttempt)

	d.logger.WarnContext(ctx, "scheduling retry",
		slog.String("event_id", msg.EventID),
		slog.String("origin", msg.Origin()),
		slog.Int("attempt", nextAttempt),
		slog.String("tier", tier.Topic),
		slog.Any("cause", cause))

	return d.producer.PublishWithHeaders(ctx, tier.Topic, msg.PartitionKey, msg.EventID, msg.Payload,
		map[string]string{
			messaging.HeaderAttempt: strconv.Itoa(nextAttempt),
			// The tiers are shared by every kind of work, so a message that lost
			// its origin here could not be routed back to the handler that owns
			// it once it comes off the tier.
			messaging.HeaderOriginTopic: msg.Origin(),
		})
}

// sendToDLQ parks a message for human inspection. Nothing is ever dropped: a
// payment that could not be completed is evidence, and silently discarding it
// is how a customer's money goes missing with no trace of why.
func (d *dispatcher) sendToDLQ(ctx context.Context, msg messaging.Message, reason string) error {
	d.logger.ErrorContext(ctx, "sending message to DLQ",
		slog.String("event_id", msg.EventID),
		slog.String("topic", msg.Topic),
		slog.String("origin", msg.Origin()),
		slog.String("reason", reason))

	return d.producer.PublishWithHeaders(ctx, d.topics.DLQ, msg.PartitionKey, msg.EventID, msg.Payload,
		map[string]string{
			"dlq-reason":                reason,
			"dlq-origin-topic":          msg.Topic,
			messaging.HeaderOriginTopic: msg.Origin(),
			messaging.HeaderAttempt:     strconv.Itoa(msg.Attempt),
		})
}

func (d *dispatcher) markHandled(ctx context.Context, eventID uuid.UUID) error {
	return d.dedup.MarkHandled(ctx, d.db.Pool(), eventID)
}

// begin runs the checks every message goes through before its handler sees it:
// a usable identity, not already processed, and due. It reports whether the
// handler should proceed.
func (d *dispatcher) begin(ctx context.Context, msg messaging.Message) (uuid.UUID, bool, error) {
	eventID, err := uuid.Parse(msg.EventID)
	if err != nil {
		// Unparseable identity means it can never be deduplicated or traced.
		// Sending it to the DLQ preserves the evidence rather than dropping it.
		d.logger.ErrorContext(ctx, "message has no usable event id", slog.String("topic", msg.Topic))
		return uuid.Nil, false, d.sendToDLQ(ctx, msg, "unparseable event id")
	}

	handled, err := d.dedup.AlreadyHandled(ctx, d.db.Pool(), eventID)
	if err != nil {
		return uuid.Nil, false, err
	}
	if handled {
		d.logger.InfoContext(ctx, "skipping already-handled event",
			slog.String("event_id", msg.EventID))
		return eventID, false, nil
	}

	// A message that is not yet due never reaches a handler: the consumer pauses
	// and rewinds its partition instead. Waiting here would block every
	// partition the consumer owns, because one goroutine walks them all.
	return eventID, true, nil
}

// Router sends a message to the handler that owns its kind of work.
//
// Keyed on the origin topic rather than the topic the message arrived on,
// because the retry tiers are shared: once a message is deferred, where it sits
// no longer says what it is.
type Router struct {
	dispatcher
	handlers map[string]messaging.Handler
}

func NewRouter(
	db *postgres.DB,
	producer *messaging.Producer,
	topics messaging.Topics,
	dedup *Dedup,
	meters *metrics.Metrics,
	logger *slog.Logger,
) *Router {
	return &Router{
		dispatcher: dispatcher{db: db, producer: producer, topics: topics, dedup: dedup, metrics: meters, logger: logger},
		handlers:   make(map[string]messaging.Handler),
	}
}

// Register binds a handler to the topic its work originates on.
func (r *Router) Register(originTopic string, handler messaging.Handler) {
	r.handlers[originTopic] = handler
}

func (r *Router) Handle(ctx context.Context, msg messaging.Message) error {
	handler, ok := r.handlers[msg.Origin()]
	if !ok {
		// Work nobody owns. Parked rather than dropped, because the usual cause
		// is a handler that was removed while messages for it were still in
		// flight, and those messages still represent something someone asked for.
		return r.sendToDLQ(ctx, msg, "no handler registered for origin topic "+msg.Origin())
	}
	return handler(ctx, msg)
}
