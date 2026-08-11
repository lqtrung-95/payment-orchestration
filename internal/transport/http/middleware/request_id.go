package middleware

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/telemetry"
)

const RequestIDHeader = "X-Request-ID"

// maxInboundRequestIDLen bounds a client-supplied correlation ID. The value is
// echoed into logs and response headers, so an unbounded one is both a log
// volume problem and a header injection surface.
const maxInboundRequestIDLen = 64

// RequestID establishes a correlation ID for the request, reusing a
// well-formed inbound value so a trace survives across service hops, and
// generating one otherwise. The ID is placed on the context and echoed back on
// the response so a caller can quote it when reporting a problem.
func RequestID() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id := sanitizeRequestID(string(c.Request.Header.Peek(RequestIDHeader)))
		if id == "" {
			id = uuid.NewString()
		}

		c.Response.Header.Set(RequestIDHeader, id)
		c.Next(telemetry.WithRequestID(ctx, id))
	}
}

// sanitizeRequestID accepts only printable ASCII within a length bound,
// rejecting anything else outright rather than attempting to repair it.
// Silently rewriting a caller's ID would break their correlation more
// confusingly than issuing a fresh one.
func sanitizeRequestID(raw string) string {
	if raw == "" || len(raw) > maxInboundRequestIDLen {
		return ""
	}
	for i := 0; i < len(raw); i++ {
		if c := raw[i]; c < 0x21 || c > 0x7E {
			return ""
		}
	}
	return raw
}
