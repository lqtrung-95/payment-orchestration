// Command pspsim runs the fault-injecting payment provider simulator.
//
// It is a separate process from the orchestrator on purpose. A provider that
// can be killed mid-flow is the only way to test what happens when one really
// does, and an in-process fake — however faithful — cannot be killed without
// taking the orchestrator down with it.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/simulator"
)

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("pspsim failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	addr := envOr("PSPSIM_ADDR", ":9090")
	preset := envOr("PSPSIM_PRESET", simulator.PresetHealthy)
	webhookURL := envOr("PSPSIM_WEBHOOK_URL", "")
	webhookSecret := envOr("PSPSIM_WEBHOOK_SECRET", "sim-webhook-secret")

	seed, err := strconv.ParseInt(envOr("PSPSIM_SEED", "20260811"), 10, 64)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, ok := simulator.Preset(preset, seed)
	if !ok {
		return errors.New("unknown PSPSIM_PRESET: " + preset)
	}

	store := simulator.NewStore()
	engine := simulator.NewEngine(cfg)
	hooks := simulator.NewWebhookEmitter(webhookURL, webhookSecret, logger)

	srv := &http.Server{
		Addr:    addr,
		Handler: simulator.NewServer(store, engine, hooks, logger),

		// No write timeout. The timeout-after-success fault works by holding a
		// connection open without answering, and a server-side write deadline
		// would cut that short — turning the most important fault in the
		// catalogue into an ordinary error the client could distinguish.
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("pspsim listening",
			slog.String("addr", addr),
			slog.String("preset", preset),
			slog.Int64("seed", seed),
			slog.String("webhook_url", webhookURL))

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	hooks.Wait()

	logger.Info("pspsim stopped")
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
