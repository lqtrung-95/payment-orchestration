package middleware

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/lequoctrung/payment-orchestrator/internal/auth"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// merchantContextKey holds the authenticated merchant for the rest of the
// request.
//
// Deliberately not a header. Anything read from the request is caller-supplied
// no matter how it is named, and a handler that reads `X-Merchant-Id` cannot
// tell whether the middleware put it there or the caller did. Passing it out of
// band means a handler can only ever see an identity this file established.
const merchantContextKey = "authenticated_merchant"

// MerchantFrom returns the authenticated merchant.
//
// The boolean is not decoration: a handler reached without authentication must
// refuse rather than fall back to an empty merchant, which would read as a
// tenant that owns nothing and matches nothing.
func MerchantFrom(c *app.RequestContext) (string, bool) {
	value, ok := c.Get(merchantContextKey)
	if !ok {
		return "", false
	}
	merchant, ok := value.(string)
	return merchant, ok && merchant != ""
}

// Authenticate resolves an API key to the merchant that owns it.
//
// Keys are looked up on shard 0, because the merchant is not known until the
// key has been resolved — the same reason the webhook log lives there.
//
// The webhook route is deliberately not behind this: providers do not hold API
// keys, and that endpoint authenticates the other way round, by verifying an
// HMAC over the exact bytes it was sent.
func Authenticate(db *postgres.DB, keys *auth.Store, logger *slog.Logger) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		presented, ok := bearerToken(c)
		if !ok {
			unauthorized(c, "authorization_required",
				"an Authorization: Bearer <api key> header is required")
			return
		}

		record, err := keys.Verify(ctx, db, presented)
		switch {
		case err == nil:
			// Fall through.

		case errors.Is(err, auth.ErrKeyNotFound), errors.Is(err, auth.ErrMalformedKey):
			// One answer for an unknown key, a wrong key, a revoked key, and a
			// malformed one. Distinguishing them tells an attacker which half
			// of a guess was right, and whether an account exists at all.
			unauthorized(c, "invalid_api_key", "the API key is not valid")
			return

		default:
			// Infrastructure, not credentials. Answering 401 here would tell a
			// legitimate caller their key is bad when the database is simply
			// unreachable, and they would rotate a key that was fine.
			logger.ErrorContext(ctx, "api key verification failed", slog.Any("error", err))
			c.AbortWithStatusJSON(consts.StatusServiceUnavailable, utils.H{
				"error":   "unavailable",
				"message": "authentication is temporarily unavailable",
			})
			return
		}

		c.Set(merchantContextKey, record.MerchantID)
		c.Next(ctx)
	}
}

// bearerToken pulls the credential out of the Authorization header.
func bearerToken(c *app.RequestContext) (string, bool) {
	header := string(c.Request.Header.Peek("Authorization"))
	if header == "" {
		return "", false
	}
	// Case-insensitive on the scheme, because clients disagree about it and
	// rejecting "bearer" would be a support ticket rather than a defence.
	const scheme = "bearer "
	if len(header) <= len(scheme) || !strings.EqualFold(header[:len(scheme)], scheme) {
		return "", false
	}
	token := strings.TrimSpace(header[len(scheme):])
	return token, token != ""
}

func unauthorized(c *app.RequestContext, code, message string) {
	// WWW-Authenticate is what makes the 401 actionable rather than a wall.
	c.Response.Header.Set("WWW-Authenticate", `Bearer realm="payment-orchestrator"`)
	c.AbortWithStatusJSON(consts.StatusUnauthorized, utils.H{
		"error":   code,
		"message": message,
	})
}
