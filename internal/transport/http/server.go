// Package http wires the Hertz server, its middleware chain, and routes.
package http

import (
	"log/slog"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/transport/http/handler"
	"github.com/lequoctrung/payment-orchestrator/internal/transport/http/middleware"
)

// Deps carries everything the transport layer needs from the rest of the
// service. Passing an explicit struct rather than a container keeps the
// dependency surface visible as later phases add payment, instrument, and
// webhook handlers.
type Deps struct {
	Logger *slog.Logger
	Health []handler.NamedChecker
}

// New builds the HTTP server. Ordering of the middleware chain is deliberate:
// RequestID runs first so every later record can be correlated, Recovery wraps
// the handlers so a panic is still logged with its request ID, and Timeout sits
// innermost so it bounds handler work rather than middleware bookkeeping.
func New(cfg *config.Config, deps Deps) *server.Hertz {
	// ExitWaitTime bounds how long Hertz lets in-flight requests drain. main
	// drives shutdown explicitly via Run/Shutdown rather than Spin, so that
	// dependency teardown is ordered rather than racing the signal handler.
	h := server.New(
		server.WithHostPorts(cfg.HTTP.Addr),
		server.WithReadTimeout(cfg.HTTP.ReadTimeout),
		server.WithWriteTimeout(cfg.HTTP.WriteTimeout),
		server.WithIdleTimeout(cfg.HTTP.IdleTimeout),
		server.WithExitWaitTime(cfg.HTTP.ShutdownTimeout),
	)

	h.Use(
		middleware.RequestID(),
		middleware.Recovery(deps.Logger),
		middleware.Logging(deps.Logger),
		middleware.Timeout(cfg.HTTP.RequestTimeout),
	)

	health := handler.NewHealth(deps.Health...)
	h.GET("/healthz", health.Live)
	h.GET("/readyz", health.Ready)

	return h
}
