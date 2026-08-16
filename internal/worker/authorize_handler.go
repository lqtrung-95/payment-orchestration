package worker

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/metrics"
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
	dispatcher
	service  *payment.Service
	breakers map[string]*resilience.CircuitBreaker
}

func NewAuthorizeHandler(
	db *postgres.DB,
	service *payment.Service,
	producer *messaging.Producer,
	topics messaging.Topics,
	dedup *Dedup,
	breakers map[string]*resilience.CircuitBreaker,
	meters *metrics.Metrics,
	logger *slog.Logger,
) *AuthorizeHandler {
	return &AuthorizeHandler{
		dispatcher: dispatcher{db: db, producer: producer, topics: topics, dedup: dedup, metrics: meters, logger: logger},
		service:    service,
		breakers:   breakers,
	}
}

// Handle processes one authorization message.
func (h *AuthorizeHandler) Handle(ctx context.Context, msg messaging.Message) error {
	eventID, proceed, err := h.begin(ctx, msg)
	if err != nil || !proceed {
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
	// Published after every interaction below, and here too: a breaker that
	// opened and then saw no further traffic would otherwise keep reporting the
	// state it held before it tripped.
	defer h.publishBreakerState(breaker)

	if breaker != nil && !breaker.Allow() {
		// The provider is already failing. Not calling it is a *known* outcome,
		// never an ambiguous one, so this can be retried freely later.
		h.logger.WarnContext(ctx, "circuit open, deferring authorization",
			slog.String("transaction_id", payload.TransactionID.String()))
		return h.scheduleRetry(ctx, msg, psp.RetryDecision{Retry: true, LongBackoff: true},
			resilience.ErrCircuitOpen)
	}

	tx, authErr := h.service.Authorize(ctx, payload.MerchantID, payload.TransactionID)

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

// publishBreakerState exports the breaker's position so an operator can see a
// provider being cut off without reading logs. It is the only external evidence
// that the breaker did anything at all.
func (h *AuthorizeHandler) publishBreakerState(breaker *resilience.CircuitBreaker) {
	if breaker == nil {
		return
	}
	h.metrics.SetBreakerState(breaker.Name(), string(breaker.State()))
}

// breakerFor returns the breaker for the provider this work will use. Breakers
// are per provider so one failing provider cannot stop traffic to the others.
func (h *AuthorizeHandler) breakerFor(string) *resilience.CircuitBreaker {
	// Routing by merchant arrives with the routing rules in a later phase; for
	// now every transaction uses the default provider.
	return h.breakers[defaultBreakerKey]
}

const defaultBreakerKey = "default"
