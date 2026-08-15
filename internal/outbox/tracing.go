package outbox

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// textCarrier is a minimal map carrier, used to squeeze a trace context into a
// single database column and back out again.
type textCarrier map[string]string

func (c textCarrier) Get(key string) string { return c[key] }
func (c textCarrier) Set(key, value string) { c[key] = value }
func (c textCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

var _ propagation.TextMapCarrier = textCarrier{}

// traceparentFrom serialises the active trace context for storage.
//
// Only the traceparent is kept, not the full carrier. Tracestate and baggage
// are vendor and application extensions that would need their own columns, and
// the parent link is what actually reconnects the relay's publish to the
// request that caused it.
func traceparentFrom(ctx context.Context) string {
	carrier := textCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier["traceparent"]
}

// contextFrom rebuilds a context carrying the stored trace.
//
// Returns the input unchanged when nothing was stored — messages enqueued
// before this column existed, or written while tracing was disabled, must still
// publish rather than being treated as malformed.
func contextFrom(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, textCarrier{"traceparent": traceparent})
}
