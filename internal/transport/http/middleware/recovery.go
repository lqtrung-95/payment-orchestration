package middleware

import (
	"context"
	"log/slog"
	"runtime/debug"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// Recovery converts a panic into a 500 rather than letting it take down the
// connection, and records the stack for diagnosis.
//
// The response body deliberately carries no detail beyond the request ID: panic
// messages routinely contain internal state, and on a payment endpoint that can
// mean account identifiers or provider references. The request ID is enough for
// an operator to find the full stack in the logs.
func Recovery(logger *slog.Logger) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		defer func() {
			if p := recover(); p != nil {
				logger.ErrorContext(ctx, "panic recovered",
					slog.Any("panic", p),
					slog.String("method", string(c.Request.Method())),
					slog.String("path", string(c.Request.URI().PathOriginal())),
					slog.String("stack", string(debug.Stack())),
				)

				c.AbortWithStatusJSON(consts.StatusInternalServerError, utils.H{
					"error":      "internal_error",
					"request_id": c.Response.Header.Get(RequestIDHeader),
				})
			}
		}()

		c.Next(ctx)
	}
}
