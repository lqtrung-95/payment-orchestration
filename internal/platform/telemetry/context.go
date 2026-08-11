package telemetry

import "context"

// ctxKey is unexported so no other package can collide with these keys.
type ctxKey int

const requestIDKey ctxKey = iota

// WithRequestID returns a context carrying the request correlation ID.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestIDFromContext returns the correlation ID, or "" when absent.
// Background work started outside a request legitimately has none, so callers
// treat the empty string as normal rather than as an error.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}
