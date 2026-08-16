package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/metrics"
)

// serveObservability exposes the worker's own metrics.
//
// The worker had no scrape endpoint at all, which meant four of the instruments
// it is the only producer of — provider latency, provider errors, retry ladder
// traffic, and circuit breaker state — were registered in a process nothing
// could read. That is worse than not having them: the API process exports the
// same registered names at zero, so a dashboard would show a provider that
// never errs while the worker is failing every call.
//
// Plain net/http rather than Hertz. The worker serves two routes that exist for
// operators, and pulling in a web framework for that would make the process
// look like something it is not.
func serveObservability(ctx context.Context, addr string, meters *metrics.Metrics, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(meters.Registry(), promhttp.HandlerOpts{
		// A gather failure is surfaced rather than served as a partial scrape,
		// which would look like a healthy system with fewer metrics.
		ErrorHandling: promhttp.HTTPErrorOnError,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Warn("metrics server shutdown", slog.Any("error", err))
		}
	}()

	go func() {
		logger.InfoContext(ctx, "worker metrics listening", slog.String("addr", addr))
		// Logged, never fatal. Losing the scrape endpoint is bad; refusing to
		// process payments because a port was taken is worse.
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.ErrorContext(ctx, "worker metrics server stopped", slog.Any("error", err))
		}
	}()
}
