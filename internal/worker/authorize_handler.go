package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	"github.com/lequoctrung/payment-orchestrator/internal/resilience"
	"github.com/lequoctrung/payment-orchestrator/internal/service/payment"
)

// AuthorizePayload is the message body for authorization work.
//
// Deliberately minimal: an identifier, not a snapshot. The handler reloads the
// transaction from the database, so a message that sat in a retry tier for half
// an hour acts on current state rather than on what was true when it was queued.
type AuthorizePayload struct {
	TransactionID uuid.UUID `json:"transaction_id"`
	MerchantID    string    `json:"merchant_id"`
}

type AuthorizeHandler struct {
	db       *postgres.DB
	service  *payment.Service
	producer *messaging.Producer
	topics   messaging.Topics
	dedup    *Dedup
	breakers map[string]*resilience.CircuitBreaker
	logger   *slog.Logger
}

func NewAuthorizeHandler(
	db *postgres.DB,
	service *payment.Service,
	producer *messaging.Producer,
	topics messaging.Topics,
	dedup *Dedup,
	breakers map[string]*resilience.CircuitBreaker,
	logger *slog.Logger,
) *AuthorizeHandler {
	return &AuthorizeHandler{
		db: db, service: service, producer: producer, topics: topics,
		dedup: dedup, breakers: breakers, logger: logger,
	}
}

// Handle processes one authorization message.
func (h *AuthorizeHandler) Handle(ctx context.Context, msg messaging.Message) error {
	eventID, err := uuid.Parse(msg.EventID)
	if err != nil {
		// Unparseable identity means it can never be deduplicated or traced.
		// Sending it to the DLQ preserves the evidence rather than dropping it.
		h.logger.ErrorContext(ctx, "message has no usable event id", slog.String("topic", msg.Topic))
		return h.sendToDLQ(ctx, msg, "unparseable event id")
	}

	handled, err := h.dedup.AlreadyHandled(ctx, h.db.Pool(), eventID)
	if err != nil {
		return err
	}
	if handled {
		h.logger.InfoContext(ctx, "skipping already-handled event",
			slog.String("event_id", msg.EventID))
		return nil
	}

	// Messages on a retry tier carry their own due time. Waiting here holds the
	// partition, which is intended: a delay topic is ordered by time, so the
	// message at the head is always the one that becomes due first.
	if err := waitUntilDue(ctx, msg, h.topics); err != nil {
		return err
	}

	var payload AuthorizePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return h.sendToDLQ(ctx, msg, "undecodable payload: "+err.Error())
	}

	return h.authorize(ctx, msg, eventID, payload)
}

func (h *AuthorizeHandler) authorize(
	ctx context.Context,
	msg messaging.Message,
	eventID uuid.UUID,
	payload AuthorizePayload,
) error {
	breaker := h.breakerFor(payload.MerchantID)
	if breaker != nil && !breaker.Allow() {
		// The provider is already failing. Not calling it is a *known* outcome,
		// never an ambiguous one, so this can be retried freely later.
		h.logger.WarnContext(ctx, "circuit open, deferring authorization",
			slog.String("transaction_id", payload.TransactionID.String()))
		return h.scheduleRetry(ctx, msg, psp.RetryDecision{Retry: true, LongBackoff: true},
			resilience.ErrCircuitOpen)
	}

	tx, authErr := h.service.Authorize(ctx, payload.TransactionID)

	switch {
	case authErr == nil:
		if breaker != nil {
			breaker.Success()
		}
		return h.markHandled(ctx, eventID)

	case tx == nil:
		// Nothing was established about the payment — an infrastructure failure
		// rather than a provider verdict. Retry without touching the breaker,
		// which tracks the provider's health, not ours.
		h.logger.ErrorContext(ctx, "authorization failed before reaching the provider",
			slog.String("transaction_id", payload.TransactionID.String()),
			slog.Any("error", authErr))
		return h.scheduleRetry(ctx, msg, psp.RetryDecision{Retry: true}, authErr)

	default:
		return h.handleProviderError(ctx, msg, eventID, payload, breaker, authErr)
	}
}

// handleProviderError applies the retry policy for the failure's class.
func (h *AuthorizeHandler) handleProviderError(
	ctx context.Context,
	msg messaging.Message,
	eventID uuid.UUID,
	payload AuthorizePayload,
	breaker *resilience.CircuitBreaker,
	authErr error,
) error {
	class := psp.ClassOf(authErr)
	decision := psp.RetryPolicyFor(class)

	// A decline is the provider working correctly. Counting it against the
	// breaker would trip on a merchant with poor approval rates and take a
	// healthy provider out of service.
	if breaker != nil {
		if class.IsTerminal() {
			breaker.Success()
		} else {
			breaker.Failure()
		}
	}

	if decision.Alert {
		h.logger.ErrorContext(ctx, "provider failure requires attention",
			slog.String("transaction_id", payload.TransactionID.String()),
			slog.String("class", string(class)),
			slog.String("reason", decision.Reason))
	}

	if !decision.Retry {
		// Terminal. The transaction already records the outcome, so the work is
		// complete — it simply did not succeed. Retrying a decline is
		// user-hostile and escalates issuer fraud controls.
		h.logger.InfoContext(ctx, "terminal provider outcome, not retrying",
			slog.String("transaction_id", payload.TransactionID.String()),
			slog.String("class", string(class)),
			slog.String("reason", decision.Reason))
		return h.markHandled(ctx, eventID)
	}

	// Ambiguous outcomes were already reconciled against the provider inside
	// Authorize; reaching here means that reconciliation did not settle it, so
	// the retry will begin by asking again rather than by charging again.
	return h.scheduleRetry(ctx, msg, decision, authErr)
}

// scheduleRetry moves the message to the next rung, or to the DLQ.
func (h *AuthorizeHandler) scheduleRetry(
	ctx context.Context,
	msg messaging.Message,
	decision psp.RetryDecision,
	cause error,
) error {
	nextAttempt := msg.Attempt + 1

	maxAttempts := decision.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = h.topics.MaxRetryAttempts()
	}
	if nextAttempt > maxAttempts {
		return h.sendToDLQ(ctx, msg, fmt.Sprintf("exhausted %d attempts: %v", maxAttempts, cause))
	}

	// A provider asking to be left alone gets the slower rungs immediately;
	// retrying a rate limit at the fast pace is what keeps the limit engaged.
	tierIndex := nextAttempt
	if decision.LongBackoff && tierIndex < 3 {
		tierIndex = 3
	}

	tier, ok := h.topics.TierForAttempt(tierIndex)
	if !ok {
		return h.sendToDLQ(ctx, msg, fmt.Sprintf("no retry tier for attempt %d: %v", tierIndex, cause))
	}

	h.logger.WarnContext(ctx, "scheduling retry",
		slog.String("event_id", msg.EventID),
		slog.Int("attempt", nextAttempt),
		slog.String("tier", tier.Topic),
		slog.Any("cause", cause))

	return h.producer.PublishWithHeaders(ctx, tier.Topic, msg.PartitionKey, msg.EventID, msg.Payload,
		map[string]string{messaging.HeaderAttempt: strconv.Itoa(nextAttempt)})
}

// sendToDLQ parks a message for human inspection. Nothing is ever dropped: a
// payment that could not be completed is evidence, and silently discarding it
// is how a customer's money goes missing with no trace of why.
func (h *AuthorizeHandler) sendToDLQ(ctx context.Context, msg messaging.Message, reason string) error {
	h.logger.ErrorContext(ctx, "sending message to DLQ",
		slog.String("event_id", msg.EventID),
		slog.String("topic", msg.Topic),
		slog.String("reason", reason))

	return h.producer.PublishWithHeaders(ctx, h.topics.DLQ, msg.PartitionKey, msg.EventID, msg.Payload,
		map[string]string{
			"dlq-reason":            reason,
			"dlq-origin-topic":      msg.Topic,
			messaging.HeaderAttempt: strconv.Itoa(msg.Attempt),
		})
}

func (h *AuthorizeHandler) markHandled(ctx context.Context, eventID uuid.UUID) error {
	return h.dedup.MarkHandled(ctx, h.db.Pool(), eventID)
}

// breakerFor returns the breaker for the provider this work will use. Breakers
// are per provider so one failing provider cannot stop traffic to the others.
func (h *AuthorizeHandler) breakerFor(string) *resilience.CircuitBreaker {
	// Routing by merchant arrives with the routing rules in a later phase; for
	// now every transaction uses the default provider.
	return h.breakers[defaultBreakerKey]
}

const defaultBreakerKey = "default"

// waitUntilDue sleeps until a retry-tier message is ready to be processed.
func waitUntilDue(ctx context.Context, msg messaging.Message, topics messaging.Topics) error {
	delay, isRetryTier := topics.DelayFor(msg.Topic)
	if !isRetryTier || delay == 0 {
		return nil
	}

	due := msg.Timestamp.Add(delay)
	remaining := time.Until(due)
	if remaining <= 0 {
		return nil
	}

	select {
	case <-time.After(remaining):
		return nil
	case <-ctx.Done():
		return errors.New("shutting down before retry became due")
	}
}
