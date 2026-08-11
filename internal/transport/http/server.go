// Package http wires the Hertz server, its middleware chain, and routes.
package http

import (
	"log/slog"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/service/payment"
	"github.com/lequoctrung/payment-orchestrator/internal/store/idempotency"
	"github.com/lequoctrung/payment-orchestrator/internal/transport/http/handler"
	"github.com/lequoctrung/payment-orchestrator/internal/transport/http/middleware"
)

// Deps carries everything the transport layer needs from the rest of the
// service. Passing an explicit struct rather than a container keeps the
// dependency surface visible as later phases add instrument and webhook
// handlers.
type Deps struct {
	Logger          *slog.Logger
	DB              *postgres.DB
	Health          []handler.NamedChecker
	PaymentService  *payment.Service
	IdempotencyRepo *idempotency.Repository
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

	payments := handler.NewPayment(deps.PaymentService, deps.Logger)
	v1 := h.Group("/v1")

	// Idempotency is applied per route rather than to the whole group. A GET is
	// already idempotent, and requiring a key on it would reject valid requests
	// while consuming key storage for nothing.
	v1.POST("/payments",
		middleware.Idempotency(deps.DB, deps.IdempotencyRepo, deps.Logger),
		payments.Create,
	)
	v1.GET("/payments/:id", payments.Get)

	return h
}
