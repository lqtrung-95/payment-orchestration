package handler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
)

// Webhook is the provider-facing ingestion endpoint.
//
// It does as little as possible on purpose: verify, persist, acknowledge. Every
// interpretation of the event happens afterwards, asynchronously. A provider
// that has to wait on our processing will time out and redeliver, which
// multiplies load at precisely the moment the receiver is least able to absorb
// it — the failure mode is self-amplifying, so the fast path has to stay fast
// even when everything behind it is slow.
type Webhook struct {
	ingestor *webhook.Ingestor
	logger   *slog.Logger
}

func NewWebhook(ingestor *webhook.Ingestor, logger *slog.Logger) *Webhook {
	return &Webhook{ingestor: ingestor, logger: logger}
}

// Receive handles POST /webhooks/:provider.
func (h *Webhook) Receive(ctx context.Context, c *app.RequestContext) {
	provider := c.Param("provider")
	body := c.Request.Body()

	// Read through a function so the framework's header representation does not
	// reach the verifiers, which have to work identically for a replay tool and
	// a test harness.
	headers := func(key string) string { return string(c.Request.Header.Peek(key)) }

	result, err := h.ingestor.Ingest(ctx, provider, headers, body)
	if err != nil {
		h.respondToIngestError(ctx, c, provider, err)
		return
	}

	if result.Duplicate {
		// 200, not a 409. A duplicate is the provider doing its job — retrying a
		// delivery it believes failed — and answering with an error makes it
		// retry harder, turning its redelivery into a flood.
		h.logger.InfoContext(ctx, "duplicate webhook delivery",
			slog.String("provider", provider))
		c.JSON(consts.StatusOK, utils.H{"status": "duplicate"})
		return
	}

	c.JSON(consts.StatusOK, utils.H{"status": "received"})
}

func (h *Webhook) respondToIngestError(ctx context.Context, c *app.RequestContext, provider string, err error) {
	switch {
	case errors.Is(err, webhook.ErrUnknownProvider):
		c.AbortWithStatusJSON(consts.StatusNotFound, utils.H{"error": "unknown_provider"})

	case errors.Is(err, webhook.ErrInvalidSignature), errors.Is(err, webhook.ErrTimestampOutsideWindow):
		// Logged loudly and stored nowhere. An unauthenticated payload is not
		// parked for later inspection: a public endpoint that writes whatever it
		// is sent is a free write amplifier for anyone who finds it.
		h.logger.WarnContext(ctx, "rejected unauthenticated webhook",
			slog.String("provider", provider),
			slog.String("client_ip", c.ClientIP()),
			slog.Any("error", err))
		c.AbortWithStatusJSON(consts.StatusUnauthorized, utils.H{"error": "invalid_signature"})

	case errors.Is(err, webhook.ErrMalformedPayload):
		// 400 so the provider stops resending something we can never process.
		h.logger.WarnContext(ctx, "rejected malformed webhook",
			slog.String("provider", provider), slog.Any("error", err))
		c.AbortWithStatusJSON(consts.StatusBadRequest, utils.H{"error": "malformed_payload"})

	default:
		// 500 so the provider *does* resend. The event was authentic and we
		// failed to store it, which is the one case where a redelivery is
		// exactly what we want.
		h.logger.ErrorContext(ctx, "webhook ingestion failed",
			slog.String("provider", provider), slog.Any("error", err))
		c.AbortWithStatusJSON(consts.StatusInternalServerError, utils.H{"error": "internal_error"})
	}
}
