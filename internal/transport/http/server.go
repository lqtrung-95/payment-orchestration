// Package http wires the Hertz server, its middleware chain, and routes.
package http

import (
	"log/slog"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/lequoctrung/payment-orchestrator/internal/auth"
	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/metrics"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/service/payment"
	"github.com/lequoctrung/payment-orchestrator/internal/store/idempotency"
	"github.com/lequoctrung/payment-orchestrator/internal/transport/http/handler"
	"github.com/lequoctrung/payment-orchestrator/internal/transport/http/middleware"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
)

// Deps carries everything the transport layer needs from the rest of the
// service. Passing an explicit struct rather than a container keeps the
// dependency surface visible as later phases add instrument and webhook
// handlers.
type Deps struct {
	Logger          *slog.Logger
	Router          *postgres.Router
	Health          []handler.NamedChecker
	PaymentService  *payment.Service
	IdempotencyRepo *idempotency.Repository
	APIKeys         *auth.Store
	WebhookIngestor *webhook.Ingestor
	Metrics         *metrics.Metrics
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
		// Bodies are capped well below the framework default. The webhook
		// endpoint is public and unauthenticated until its signature is checked,
		// and checking a signature requires buffering the whole body first — so
		// without a cap, anyone who finds the URL can make this service allocate
		// megabytes per request before rejecting any of them.
		server.WithMaxRequestBodySize(cfg.HTTP.MaxBodyBytes),
	)

	chain := []app.HandlerFunc{
		middleware.RequestID(),
		// Before Recovery, so a panicking handler still produces a span that
		// records the error rather than vanishing from the trace entirely.
		middleware.Tracing(),
		middleware.Recovery(deps.Logger),
		middleware.Logging(deps.Logger),
	}
	if deps.Metrics != nil {
		// Outside the timeout so a request that times out is still counted —
		// the requests you most want measured are the ones that failed.
		chain = append(chain, middleware.Metrics(deps.Metrics))
	}
	chain = append(chain, middleware.Timeout(cfg.HTTP.RequestTimeout))
	h.Use(chain...)

	health := handler.NewHealth(deps.Health...)
	h.GET("/healthz", health.Live)
	h.GET("/readyz", health.Ready)

	// Scrape endpoint. Deliberately on the same listener for local simplicity;
	// a deployed environment binds it to an internal interface, because the
	// metric names alone describe the system's shape to anyone who reads them.
	if deps.Metrics != nil {
		h.GET("/metrics", handler.Prometheus(deps.Metrics))
	}

	// The merchant-facing surface is registered only when its dependencies are
	// all present, and refuses to be registered when they are not.
	//
	// A panic rather than a silent skip, because the failure it guards against
	// is a payment API that boots and serves without authentication. Refusing
	// to start is loud, immediate, and happens on somebody's laptop; the
	// alternative is discovered by whoever finds the endpoint first.
	if deps.PaymentService != nil {
		if deps.Router == nil || deps.APIKeys == nil {
			panic("payment routes need a shard router and an api key store to authenticate with")
		}
		registerPaymentRoutes(h, deps)
	}

	// Outside /v1 and outside the idempotency middleware. Providers do not send
	// an Idempotency-Key, and webhook deduplication is by the provider's own
	// event id against a unique index — a different mechanism for a different
	// party, deliberately not sharing the merchant-facing one.
	if deps.WebhookIngestor != nil {
		webhooks := handler.NewWebhook(deps.WebhookIngestor, deps.Metrics, deps.Logger)
		h.POST("/webhooks/:provider", webhooks.Receive)
	}

	return h
}

// registerPaymentRoutes mounts the authenticated merchant-facing surface.
//
// Authentication is applied to the group rather than to each route. Per route
// it would be one edit away from a new endpoint that silently serves anybody,
// and the endpoint that gets forgotten is the one added in a hurry.
func registerPaymentRoutes(h *server.Hertz, deps Deps) {
	payments := handler.NewPayment(deps.PaymentService, deps.Logger)

	v1 := h.Group("/v1",
		middleware.Authenticate(deps.Router.Global(), deps.APIKeys, deps.Logger))

	// Idempotency is applied per route rather than to the whole group. A GET is
	// already idempotent, and requiring a key on it would reject valid requests
	// while consuming key storage for nothing.
	v1.POST("/payments",
		middleware.Idempotency(deps.Router, deps.IdempotencyRepo, deps.Logger),
		payments.Create,
	)
	v1.GET("/payments/:id", payments.Get)

	// Capture carries an idempotency key for the same reason creation does: it
	// moves money, and a retried request must not take the funds twice.
	v1.POST("/payments/:id/capture",
		middleware.Idempotency(deps.Router, deps.IdempotencyRepo, deps.Logger),
		payments.Capture,
	)
}
