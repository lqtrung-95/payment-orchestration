package middleware

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
)

// Timeout bounds the context available to a handler.
//
// Payment endpoints call external providers, and a provider that accepts a
// connection but never answers will otherwise hold a goroutine and a database
// connection indefinitely. Bounding the context lets every ctx-aware call
// downstream — pool acquisition, queries, PSP requests — abort together.
//
// This does not by itself stop a handler that ignores its context; server-level
// write timeouts remain the outer backstop.
func Timeout(d time.Duration) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if d <= 0 {
			c.Next(ctx)
			return
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, d)
		defer cancel()

		c.Next(timeoutCtx)
	}
}
