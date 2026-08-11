package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/service/payment"
	"github.com/lequoctrung/payment-orchestrator/internal/transport/http/middleware"
)

type Payment struct {
	service *payment.Service
	logger  *slog.Logger
}

func NewPayment(service *payment.Service, logger *slog.Logger) *Payment {
	return &Payment{service: service, logger: logger}
}

// createPaymentRequest is the wire shape for creating a payment.
//
// Amount is an integer in minor units. Accepting a decimal would invite clients
// to send 10.50 and this service to parse it as a float, which cannot represent
// that value exactly; the schema refuses the whole class of problem.
type createPaymentRequest struct {
	Amount   int64          `json:"amount"`
	Currency money.Currency `json:"currency"`
}

type paymentResponse struct {
	ID         uuid.UUID      `json:"id"`
	MerchantID string         `json:"merchant_id"`
	State      domain.State   `json:"state"`
	Amount     int64          `json:"amount"`
	Currency   money.Currency `json:"currency"`
	Captured   int64          `json:"captured"`
	Refunded   int64          `json:"refunded"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

func toResponse(t *domain.Transaction) paymentResponse {
	return paymentResponse{
		ID:         t.ID,
		MerchantID: t.MerchantID,
		State:      t.State,
		Amount:     t.Amount.Amount(),
		Currency:   t.Amount.Currency(),
		Captured:   t.Captured.Amount(),
		Refunded:   t.Refunded.Amount(),
		CreatedAt:  t.CreatedAt,
		UpdatedAt:  t.UpdatedAt,
	}
}

// Create handles POST /v1/payments. Idempotency is enforced by middleware, so
// by the time this runs the request is known to be the sole owner of its key.
func (h *Payment) Create(ctx context.Context, c *app.RequestContext) {
	var req createPaymentRequest

	dec := json.NewDecoder(bytes.NewReader(c.Request.Body()))
	// Unknown fields are rejected rather than ignored: a client sending
	// "currrency" should be told, not have the payment silently created with
	// whatever the typo left unset.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		c.AbortWithStatusJSON(consts.StatusBadRequest, utils.H{
			"error":   "invalid_request_body",
			"message": err.Error(),
		})
		return
	}

	amount, err := money.New(req.Amount, req.Currency)
	if err != nil {
		c.AbortWithStatusJSON(consts.StatusBadRequest, utils.H{
			"error":   "invalid_amount",
			"message": err.Error(),
		})
		return
	}

	t, err := h.service.Create(ctx, payment.CreateInput{
		MerchantID:     string(c.Request.Header.Peek(middleware.MerchantIDHeader)),
		IdempotencyKey: string(c.Request.Header.Peek(middleware.IdempotencyKeyHeader)),
		Amount:         amount,
		Actor:          "api:" + string(c.Request.Header.Peek(middleware.MerchantIDHeader)),
		SourceIP:       c.ClientIP(),
	})
	if err != nil {
		// Domain validation failures are the caller's fault and are reported as
		// such; anything else is ours and is logged with the request ID rather
		// than echoed, since the message may carry internal detail.
		if errors.Is(err, domain.ErrAmountNotPositive) ||
			errors.Is(err, domain.ErrMerchantRequired) ||
			errors.Is(err, money.ErrInvalidCurrency) {
			c.AbortWithStatusJSON(consts.StatusBadRequest, utils.H{
				"error":   "invalid_payment",
				"message": err.Error(),
			})
			return
		}

		h.logger.ErrorContext(ctx, "create payment failed", slog.Any("error", err))
		c.AbortWithStatusJSON(consts.StatusInternalServerError, utils.H{"error": "internal_error"})
		return
	}

	c.JSON(consts.StatusCreated, toResponse(t))
}

// Get handles GET /v1/payments/:id.
func (h *Payment) Get(ctx context.Context, c *app.RequestContext) {
	merchantID := string(c.Request.Header.Peek(middleware.MerchantIDHeader))
	if merchantID == "" {
		c.AbortWithStatusJSON(consts.StatusBadRequest, utils.H{
			"error":   "merchant_required",
			"message": "X-Merchant-Id header is required",
		})
		return
	}

	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		// A malformed identifier is answered as not-found rather than as a
		// parse error, so probing for valid identifier shapes learns nothing.
		c.AbortWithStatusJSON(consts.StatusNotFound, utils.H{"error": "payment_not_found"})
		return
	}

	t, err := h.service.Get(ctx, merchantID, id)
	if errors.Is(err, payment.ErrNotFound) {
		c.AbortWithStatusJSON(consts.StatusNotFound, utils.H{"error": "payment_not_found"})
		return
	}
	if err != nil {
		h.logger.ErrorContext(ctx, "get payment failed", slog.Any("error", err))
		c.AbortWithStatusJSON(consts.StatusInternalServerError, utils.H{"error": "internal_error"})
		return
	}

	c.JSON(consts.StatusOK, toResponse(t))
}
