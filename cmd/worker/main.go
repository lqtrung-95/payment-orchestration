// Command worker consumes payment work from Kafka.
//
// Separate from the API process on purpose. The two scale on different signals —
// the API on request rate, the worker on provider latency and backlog depth —
// and a worker blocked on a slow provider must not consume capacity that would
// otherwise be serving requests.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/outbox"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/telemetry"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	"github.com/lequoctrung/payment-orchestrator/internal/psp/simclient"
	"github.com/lequoctrung/payment-orchestrator/internal/resilience"
	"github.com/lequoctrung/payment-orchestrator/internal/service/payment"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
	webhookproviders "github.com/lequoctrung/payment-orchestrator/internal/webhook/providers"
	"github.com/lequoctrung/payment-orchestrator/internal/worker"
)

// consumerGroup names the group whose progress and deduplication records are
// keyed by it. Changing it replays every retained message from the beginning.
const consumerGroup = "payment-workers"

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("worker failed", slog.Any("error", err))
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Tracing is installed before anything else does work, so every span
	// created below has a real provider to attach to rather than the no-op one.
	tracing, err := telemetry.StartTracing(ctx, telemetry.TracingConfig{
		Enabled:     cfg.Observability.TracingEnabled,
		ServiceName: "payment-worker",
		Environment: cfg.Env,
		Endpoint:    cfg.Observability.TracingEndpoint,
		SampleRatio: cfg.Observability.TraceSampleRatio,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := tracing.Shutdown(ctx); err != nil {
			logger.Error("flush traces", slog.Any("error", err))
		}
	}()

	db, err := postgres.New(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer db.Close()

	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ClientID(cfg.Kafka.ClientID+"-worker-producer"),
		// Wait for all in-sync replicas. A message the broker has not durably
		// accepted is a message that can vanish, and this one represents a
		// payment.
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
	producer := messaging.NewProducer(producerClient)

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

	service := payment.NewService(db, txstore.NewRepository(), providers, outbox.NewWriter(), topics, logger)

	breakers := map[string]*resilience.CircuitBreaker{
		"default": resilience.NewCircuitBreaker(cfg.PSP.DefaultProvider, resilience.DefaultCircuitConfig()),
	}

	dedup := worker.NewDedup(consumerGroup)

	webhookRegistry := webhook.NewRegistry(
		webhookproviders.NewSimulator(cfg.Webhook.Provider, cfg.Webhook.Secret),
	)
	processor := webhook.NewProcessor(db, webhookRegistry, webhook.NewRepository(),
		txstore.NewRepository(), logger)

	// One router across both kinds of work. They share the retry ladder and the
	// dead letter queue, so a message is routed by the topic it originated on
	// rather than by where it currently sits.
	router := worker.NewRouter(db, producer, topics, dedup, logger)
	router.Register(topics.Authorize,
		worker.NewAuthorizeHandler(db, service, producer, topics, dedup, breakers, logger).Handle)
	router.Register(topics.Webhook,
		worker.NewWebhookHandler(db, processor, producer, topics, dedup, logger).Handle)

	// One consumer group across the work topics and every retry tier. A message
	// on a retry tier is the same work, merely deferred, so it wants the same
	// handler rather than a parallel implementation that could drift.
	consumer, err := messaging.NewConsumer(cfg.Kafka.Brokers, consumerGroup,
		cfg.Kafka.ClientID+"-worker", topics.Consumed(), logger)
	if err != nil {
		return err
	}
	defer consumer.Close()

	// Deferred retries are honoured by pausing partitions rather than sleeping,
	// so a message waiting out the 30-minute tier does not stall live work.
	consumer = consumer.WithDue(topics.DueAt)

	logger.InfoContext(ctx, "worker starting",
		slog.String("group", consumerGroup),
		slog.Any("topics", topics.Consumed()),
		slog.String("provider", cfg.PSP.DefaultProvider))

	errCh := make(chan error, 1)
	go func() { errCh <- consumer.Run(ctx, router.Handle) }()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received, finishing in-flight work")
	}

	// Give an in-flight handler a moment to finish rather than abandoning a
	// provider call whose outcome nobody would then know.
	select {
	case <-errCh:
	case <-time.After(15 * time.Second):
		logger.Warn("worker shutdown timed out with work in flight")
	}

	if err := producerClient.Flush(context.WithoutCancel(ctx)); err != nil &&
		!errors.Is(err, context.Canceled) {
		logger.Error("flush producer", slog.Any("error", err))
	}

	logger.Info("worker stopped")
	return nil
}
