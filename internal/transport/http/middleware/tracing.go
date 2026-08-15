package middleware

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/telemetry"
)

var tracer = otel.Tracer("payment-orchestrator/http")

// hertzHeaderCarrier adapts Hertz request headers to the propagation interface,
// so a caller that already has a trace continues it rather than starting a new
// one.
type hertzHeaderCarrier struct{ c *app.RequestContext }

func (h hertzHeaderCarrier) Get(key string) string {
	return string(h.c.Request.Header.Peek(key))
}
func (h hertzHeaderCarrier) Set(key, value string) { h.c.Request.Header.Set(key, value) }
func (h hertzHeaderCarrier) Keys() []string {
	var keys []string
	h.c.Request.Header.VisitAll(func(k, _ []byte) { keys = append(keys, string(k)) })
	return keys
}

var _ propagation.TextMapCarrier = hertzHeaderCarrier{}

// Tracing opens a server span per request.
//
// Named by the route pattern rather than the path, for the same reason the
// metrics are: a span name containing a transaction id makes every request its
// own operation, which defeats aggregation and publishes identifiers into a
// system that is usually more widely readable than the database.
func Tracing() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		ctx = otel.GetTextMapPropagator().Extract(ctx, hertzHeaderCarrier{c: c})

		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		method := string(c.Request.Method())

		ctx, span := tracer.Start(ctx, method+" "+route,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.request.method", method),
				attribute.String("http.route", route),
			))
		defer span.End()

		// The request id is already on every log line; putting it on the span
		// is what lets someone move between the two without a second search.
		if requestID := telemetry.RequestIDFromContext(ctx); requestID != "" {
			span.SetAttributes(attribute.String("request.id", requestID))
		}

		c.Next(ctx)

		status := c.Response.StatusCode()
		span.SetAttributes(attribute.Int("http.response.status_code", status))
		if status >= 500 {
			span.SetStatus(codes.Error, "server error")
		}
	}
}
