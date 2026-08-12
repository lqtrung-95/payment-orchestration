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
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/outbox"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/kafka"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/redis"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/telemetry"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	"github.com/lequoctrung/payment-orchestrator/internal/psp/simclient"
	"github.com/lequoctrung/payment-orchestrator/internal/service/payment"
	"github.com/lequoctrung/payment-orchestrator/internal/store/idempotency"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
	transport "github.com/lequoctrung/payment-orchestrator/internal/transport/http"
	"github.com/lequoctrung/payment-orchestrator/internal/transport/http/handler"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
	webhookproviders "github.com/lequoctrung/payment-orchestrator/internal/webhook/providers"
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

	// Lock TTL exceeds the HTTP request timeout so a claim is never taken over
	// while its owner is still working. Record TTL matches the 24h window
	// providers conventionally honour for key reuse.
	idempotencyRepo := idempotency.NewRepository(cfg.HTTP.RequestTimeout*2, 24*time.Hour)

	// Three providers with deliberately different shapes, all served by one
	// simulator process. Two adapters with identical semantics would prove
	// nothing; the differences are what force real orchestration rather than a
	// passthrough.
	providers := psp.NewRegistry(cfg.PSP.DefaultProvider,
		simclient.New(simclient.Config{
			Name: "psp-sync-sim", BaseURL: cfg.PSP.SimulatorURL,
			Mode: simclient.ModeSync, Timeout: cfg.PSP.Timeout,
		}),
		simclient.New(simclient.Config{
			Name: "psp-async-sim", BaseURL: cfg.PSP.SimulatorURL,
			Mode: simclient.ModeAsync, Timeout: cfg.PSP.Timeout,
		}),
		simclient.New(simclient.Config{
			Name: "psp-redirect-sim", BaseURL: cfg.PSP.SimulatorURL,
			Mode: simclient.ModeRedirect, Timeout: cfg.PSP.Timeout,
		}),
	)
	if _, err := providers.Default(); err != nil {
		return err
	}
	logger.InfoContext(ctx, "payment providers registered",
		slog.Any("providers", providers.Names()),
		slog.String("default", cfg.PSP.DefaultProvider))

	// Producer for the outbox relay. Full ISR acknowledgement: a message the
	// broker has not durably accepted is one that can vanish, and this one
	// represents a payment.
	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ClientID(cfg.Kafka.ClientID+"-outbox"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
	)
	if err != nil {
		return err
	}
	defer producerClient.Close()

	// PrefixedTopics with an empty prefix is exactly DefaultTopics, so the
	// production path is unchanged and a namespaced run needs no separate branch.
	topics := messaging.PrefixedTopics(cfg.Kafka.TopicPrefix, 0)
	if err := messaging.EnsureTopics(ctx, producerClient, topics); err != nil {
		return err
	}

	paymentService := payment.NewService(db, txstore.NewRepository(), providers,
		outbox.NewWriter(), topics, logger)

	// The relay runs alongside the API. It is safe to run in every instance —
	// the claim query uses FOR UPDATE SKIP LOCKED, so instances take disjoint
	// rows rather than blocking on each other.
	publisher := outbox.NewPublisher(db, messaging.NewProducer(producerClient),
		outbox.DefaultPublisherConfig(), logger)
	go func() {
		if err := publisher.Run(ctx); err != nil {
			logger.ErrorContext(ctx, "outbox publisher stopped", slog.Any("error", err))
		}
	}()

	// One verifier per provider, resolved by the route segment. Adding a real
	// provider means adding its scheme here, not widening a shared one.
	webhookRegistry := webhook.NewRegistry(
		webhookproviders.NewSimulator(cfg.Webhook.Provider, cfg.Webhook.Secret),
	)
	ingestor := webhook.NewIngestor(db, webhookRegistry, webhook.NewRepository(),
		outbox.NewWriter(), topics, logger)

	srv := transport.New(cfg, transport.Deps{
		Logger: logger,
		DB:     db,
		Health: []handler.NamedChecker{
			{Name: "postgres", Checker: db},
			{Name: "redis", Checker: rdb},
			{Name: "kafka", Checker: kfk},
		},
		PaymentService:  paymentService,
		IdempotencyRepo: idempotencyRepo,
		WebhookIngestor: ingestor,
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
