package middleware

import (
	"context"
	"errors"
	"log/slog"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/store/idempotency"
)

const (
	IdempotencyKeyHeader = "Idempotency-Key"
	MerchantIDHeader     = "X-Merchant-Id"

	maxIdempotencyKeyLen = 255
)

// Idempotency makes a mutating endpoint safe to retry.
//
// The claim is committed in its own transaction before the handler runs. That
// is the whole mechanism: until the in-flight row is visible to other
// connections, a concurrent request carrying the same key would find nothing
// and conclude it may proceed. Holding the claim open inside the handler's
// transaction would defeat the unique constraint that decides ownership.
//
// The response is recorded afterwards so a retry can be answered identically —
// including a failure. A caller that received a decline and retries the same
// key is entitled to that decline, not to a second attempt at the instrument.
func Idempotency(db *postgres.DB, repo *idempotency.Repository, logger *slog.Logger) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		key := string(c.Request.Header.Peek(IdempotencyKeyHeader))
		if key == "" || len(key) > maxIdempotencyKeyLen {
			c.AbortWithStatusJSON(consts.StatusBadRequest, utils.H{
				"error":   "idempotency_key_required",
				"message": "a non-empty Idempotency-Key header of at most 255 characters is required",
			})
			return
		}

		merchantID := string(c.Request.Header.Peek(MerchantIDHeader))
		if merchantID == "" {
			c.AbortWithStatusJSON(consts.StatusBadRequest, utils.H{
				"error":   "merchant_required",
				"message": "X-Merchant-Id header is required",
			})
			return
		}

		fingerprint := idempotency.Fingerprint(
			string(c.Request.Method()),
			string(c.Request.URI().Path()),
			c.Request.Body(),
		)

		var claim idempotency.ClaimResult
		err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			var err error
			claim, err = repo.Claim(ctx, tx, merchantID, key, fingerprint)
			return err
		})
		if err != nil {
			logger.ErrorContext(ctx, "idempotency claim failed", slog.Any("error", err))
			c.AbortWithStatusJSON(consts.StatusInternalServerError, utils.H{"error": "internal_error"})
			return
		}

		switch claim.Outcome {
		case idempotency.OutcomeReplay:
			// Replayed byte-for-byte. The header lets a client tell a replay
			// from a fresh execution, which matters when debugging a retry loop.
			c.Response.Header.Set("Idempotency-Replayed", "true")
			c.Response.Header.SetContentType("application/json; charset=utf-8")
			c.AbortWithStatus(claim.Record.ResponseStatus)
			c.Response.SetBody(claim.Record.ResponseBody)
			return

		case idempotency.OutcomeInFlight:
			// 409 rather than waiting for the original: blocking would hold a
			// connection for the length of a provider call, and a client that
			// retries on a timeout would then queue behind itself.
			c.Response.Header.Set("Retry-After", "1")
			c.AbortWithStatusJSON(consts.StatusConflict, utils.H{
				"error":   "request_in_progress",
				"message": "a request with this Idempotency-Key is currently being processed",
			})
			return

		case idempotency.OutcomeFingerprintMismatch:
			c.AbortWithStatusJSON(consts.StatusConflict, utils.H{
				"error":   "idempotency_key_reused",
				"message": "this Idempotency-Key was already used with a different request body",
			})
			return

		case idempotency.OutcomeAcquired:
			// Fall through and run the handler.
		}

		c.Next(ctx)

		status := c.Response.StatusCode()
		body := append([]byte(nil), c.Response.Body()...)

		// The token proves this caller still owns the claim. If the claim lapsed
		// and was taken over mid-request, the write is refused rather than
		// overwriting the newer owner's result.
		if err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			return repo.Complete(ctx, tx, merchantID, key, claim.Record.ClaimToken, status, body, nil)
		}); err != nil {
			// The response is already correct and is still returned. What is
			// lost is the ability to replay it: the record is not updated, so a
			// retry re-executes rather than being answered from storage.
			logger.ErrorContext(ctx, "failed to record idempotent response",
				slog.Any("error", err),
				slog.Bool("claim_lost", errors.Is(err, idempotency.ErrClaimLost)),
			)
		}
	}
}
