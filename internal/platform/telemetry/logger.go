// Package telemetry owns structured logging, and later tracing and metrics.
//
// Logging is context-aware by construction: the request ID is pulled from the
// context by a handler wrapper rather than threaded through every call site.
// That matters once work crosses a queue boundary, where passing a pre-built
// logger down the stack stops being practical.
package telemetry

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

// NewLogger builds the process logger. Format and level come from config so the
// same binary emits human-readable output locally and JSON in deployment.
func NewLogger(w io.Writer, level, format string, addSource bool) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     parseLevel(level),
		AddSource: addSource,
	}

	var base slog.Handler
	if strings.EqualFold(format, "text") {
		base = slog.NewTextHandler(w, opts)
	} else {
		base = slog.NewJSONHandler(w, opts)
	}

	return slog.New(&contextHandler{Handler: base})
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// contextHandler copies correlation identifiers from the context onto every
// record. Without this, correlating logs across a request that fans out into
// goroutines and queue consumers means remembering to attach the ID by hand at
// each site — which is exactly what gets forgotten under pressure.
type contextHandler struct {
	slog.Handler
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestIDFromContext(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithGroup(name)}
}
