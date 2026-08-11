// Command orchestrator is the payment orchestration service entrypoint.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/kafka"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/redis"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/telemetry"
	transport "github.com/lequoctrung/payment-orchestrator/internal/transport/http"
	"github.com/lequoctrung/payment-orchestrator/internal/transport/http/handler"
)

func main() {
	if err := run(); err != nil {
		// Config may have failed to load, so fall back to a plain logger rather
		// than assuming the configured one exists.
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("startup failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := telemetry.NewLogger(os.Stdout, cfg.Log.Level, cfg.Log.Format, cfg.Log.AddSource)
	slog.SetDefault(logger)

	// Signals are handled here rather than by Hertz's Spin so that shutdown is
	// ordered: stop accepting requests, drain, then close dependencies. Closing
	// a connection pool while requests are still in flight surfaces as spurious
	// errors that look like data problems.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.InfoContext(ctx, "starting service", slog.String("env", cfg.Env), slog.String("addr", cfg.HTTP.Addr))

	db, err := postgres.New(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer db.Close()
	logger.InfoContext(ctx, "connected to postgres")

	rdb, err := redis.New(ctx, cfg.Redis)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	logger.InfoContext(ctx, "connected to redis")

	kfk, err := kafka.New(ctx, cfg.Kafka)
	if err != nil {
		return err
	}
	defer kfk.Close()
	logger.InfoContext(ctx, "connected to kafka", slog.Any("brokers", cfg.Kafka.Brokers))

	srv := transport.New(cfg, transport.Deps{
		Logger: logger,
		Health: []handler.NamedChecker{
			{Name: "postgres", Checker: db},
			{Name: "redis", Checker: rdb},
			{Name: "kafka", Checker: kfk},
		},
	})

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	logger.InfoContext(ctx, "service ready", slog.String("addr", cfg.HTTP.Addr))

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining requests")
	}

	// Shutdown uses a context detached from the signal context, which is already
	// cancelled by the time we get here; reusing it would abort the drain
	// immediately and cut off in-flight payment requests.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.HTTP.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", slog.Any("error", err))
		return err
	}

	logger.Info("shutdown complete")
	return nil
}
