package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
)

// Logging emits one structured record per request. The request ID is attached
// automatically by the context-aware log handler, so it is not repeated here.
//
// Severity is derived from the response status: 5xx is an error the service
// owns, 4xx is a caller problem worth seeing but not alerting on, everything
// else is routine.
func Logging(logger *slog.Logger) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()

		c.Next(ctx)

		status := c.Response.StatusCode()
		attrs := []any{
			slog.String("method", string(c.Request.Method())),
			slog.String("path", string(c.Request.URI().PathOriginal())),
			slog.Int("status", status),
			slog.Duration("duration", time.Since(start)),
			slog.String("client_ip", c.ClientIP()),
		}

		switch {
		case status >= 500:
			logger.ErrorContext(ctx, "request failed", attrs...)
		case status >= 400:
			logger.WarnContext(ctx, "request rejected", attrs...)
		default:
			logger.InfoContext(ctx, "request completed", attrs...)
		}
	}
}
