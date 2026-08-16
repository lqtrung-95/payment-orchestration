package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/metrics"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
)

// WebhookHandler applies accepted provider callbacks.
//
// Ingestion has already verified, stored, and acknowledged the delivery, so
// nothing here is on a provider's clock. That separation is the point: the
// receiver can be slow, can retry, and can wait for a transaction to appear,
// without the provider interpreting any of it as a failed delivery.
type WebhookHandler struct {
	dispatcher
	processor *webhook.Processor
}

func NewWebhookHandler(
	db *postgres.DB,
	processor *webhook.Processor,
	producer *messaging.Producer,
	topics messaging.Topics,
	dedup *Dedup,
	meters *metrics.Metrics,
	logger *slog.Logger,
) *WebhookHandler {
	return &WebhookHandler{
		dispatcher: dispatcher{db: db, producer: producer, topics: topics, dedup: dedup, metrics: meters, logger: logger},
		processor:  processor,
	}
}

func (h *WebhookHandler) Handle(ctx context.Context, msg messaging.Message) error {
	eventID, proceed, err := h.begin(ctx, msg)
	if err != nil || !proceed {
		return err
	}

	var payload webhook.Received
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return h.sendToDLQ(ctx, msg, "undecodable payload: "+err.Error())
	}

	outcome, err := h.processor.Process(ctx, payload.RawEventID)

	if errors.Is(err, webhook.ErrNotCorrelated) {
		return h.awaitCorrelation(ctx, msg, payload, err)
	}
	if err != nil {
		// The event is stored and its status is unchanged, so a retry re-runs
		// the whole decision rather than resuming a half-applied one.
		h.logger.ErrorContext(ctx, "webhook processing failed",
			slog.Int64("raw_event_id", payload.RawEventID), slog.Any("error", err))
		return h.scheduleRetry(ctx, msg, psp.RetryDecision{Retry: true}, err)
	}

	h.logger.InfoContext(ctx, "webhook processed",
		slog.Int64("raw_event_id", payload.RawEventID),
		slog.String("outcome", string(outcome)))

	return h.markHandled(ctx, eventID)
}

// awaitCorrelation defers an event whose transaction is not visible yet.
//
// This is the webhook-before-response race, and it is common rather than exotic:
// the provider's callback and its HTTP reply are sent concurrently, so the
// callback regularly arrives before this service has recorded the reference that
// same reply was about to deliver.
//
// The retry ladder is the parking area. A dedicated pending-correlation table
// with its own TTL sweeper would be a second mechanism doing what the first
// already does — the same waiting, the same escalation, the same dead letter
// queue at the end — and two mechanisms for one job drift apart.
func (h *WebhookHandler) awaitCorrelation(
	ctx context.Context,
	msg messaging.Message,
	payload webhook.Received,
	cause error,
) error {
	if msg.Attempt < h.topics.MaxRetryAttempts() {
		h.logger.InfoContext(ctx, "webhook has no transaction yet, deferring",
			slog.Int64("raw_event_id", payload.RawEventID),
			slog.String("reference", payload.Reference),
			slog.Int("attempt", msg.Attempt))
		return h.scheduleRetry(ctx, msg, psp.RetryDecision{Retry: true}, cause)
	}

	// The window has closed. A provider reporting a charge this service has no
	// record of is either a correlation bug or a payment created outside the
	// system, and both need a person rather than another retry.
	note := "no transaction for reference " + payload.Reference
	if err := h.processor.MarkUnmatched(ctx, payload.RawEventID, note); err != nil {
		return err
	}
	return h.sendToDLQ(ctx, msg, note)
}
