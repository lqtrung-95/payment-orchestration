package worker_test

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/outbox"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	"github.com/lequoctrung/payment-orchestrator/internal/psp/simclient"
	"github.com/lequoctrung/payment-orchestrator/internal/resilience"
	"github.com/lequoctrung/payment-orchestrator/internal/service/payment"
	"github.com/lequoctrung/payment-orchestrator/internal/simulator"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
	"github.com/lequoctrung/payment-orchestrator/internal/worker"
)

type pipeline struct {
	db      *postgres.DB
	service *payment.Service
	store   *simulator.Store
	engine  *simulator.Engine
	topics  messaging.Topics
	brokers []string
	sim     *httptest.Server
	cancel  context.CancelFunc
}

// newPipeline stands up the whole asynchronous path against real infrastructure:
// a real database, real Kafka, and a provider that misbehaves on request.
//
// Every run gets its own topics and consumer group. Sharing fixed names across
// runs would make each test see the previous run's backlog, which turns real
// failures into noise and hides genuine ones.
func newPipeline(t *testing.T, faults map[simulator.Fault]float64) *pipeline {
	t.Helper()

	brokers := kafkaBrokers(t)
	db := testsupport.FreshDB(t)
	logger := testLogger()

	cfg, _ := simulator.Preset(simulator.PresetHealthy, 20260811)
	for f, p := range faults {
		cfg.Probabilities[f] = p
	}
	store := simulator.NewStore()
	engine := simulator.NewEngine(cfg)
	hooks := simulator.NewWebhookEmitter("", "test-secret", logger)
	sim := httptest.NewServer(simulator.NewServer(store, engine, hooks, logger))
	t.Cleanup(sim.Close)

	run := strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	// Retry delays are collapsed: the routing decision is what is under test,
	// not the wall-clock wait, and a test cannot sit through the real ladder.
	topics := messaging.PrefixedTopics("test-"+run+".", 200*time.Millisecond)
	group := "test-workers-" + run

	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("test-producer-"+run),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
	)
	if err != nil {
		t.Fatalf("kafka producer: %v", err)
	}
	t.Cleanup(producerClient.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := messaging.EnsureTopics(ctx, producerClient, topics); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}
	producer := messaging.NewProducer(producerClient)

	adapter := simclient.New(simclient.Config{
		Name: "psp-test", BaseURL: sim.URL,
		Mode: simclient.ModeSync, Timeout: 300 * time.Millisecond,
	})
	service := payment.NewService(db, txstore.NewRepository(),
		psp.NewRegistry("psp-test", adapter), outbox.NewWriter(), topics, logger)

	breakers := map[string]*resilience.CircuitBreaker{
		"default": resilience.NewCircuitBreaker("psp-test", resilience.DefaultCircuitConfig()),
	}
	handler := worker.NewAuthorizeHandler(db, service, producer, topics,
		worker.NewDedup(group), breakers, logger)

	consumer, err := messaging.NewConsumer(brokers, group, "test-consumer-"+run, topics.Consumed(), logger)
	if err != nil {
		t.Fatalf("kafka consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	publisher := outbox.NewPublisher(db, producer, outbox.DefaultPublisherConfig(), logger)

	go func() { _ = publisher.Run(ctx) }()
	go func() { _ = consumer.Run(ctx, handler.Handle) }()

	return &pipeline{
		db: db, service: service, store: store, engine: engine,
		topics: topics, brokers: brokers, sim: sim, cancel: cancel,
	}
}

// testLogger is silent unless WORKER_TEST_LOG is set, so a failing pipeline can
// be diagnosed without making every passing run noisy.
func testLogger() *slog.Logger {
	if os.Getenv("WORKER_TEST_LOG") == "" {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func kafkaBrokers(t *testing.T) []string {
	t.Helper()

	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		t.Skip("KAFKA_BROKERS not set; skipping asynchronous pipeline test")
	}
	return strings.Split(raw, ",")
}

// awaitState polls until the transaction reaches one of the wanted states.
func (p *pipeline) awaitState(t *testing.T, id uuid.UUID, timeout time.Duration, wanted ...domain.State) *domain.Transaction {
	t.Helper()

	repo := txstore.NewRepository()
	deadline := time.Now().Add(timeout)
	var last *domain.Transaction

	for time.Now().Before(deadline) {
		tx, err := repo.Get(context.Background(), p.db.Pool(), id)
		if err == nil {
			last = tx
			for _, want := range wanted {
				if tx.State == want {
					return tx
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	state := "unknown"
	if last != nil {
		state = string(last.State)
	}
	t.Fatalf("transaction %s did not reach %v within %v (last state %s)", id, wanted, timeout, state)
	return nil
}

// topicDepth returns how many records a topic holds. Topics are fresh per run,
// so the end offset is the message count.
func (p *pipeline) topicDepth(t *testing.T, topic string) int64 {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(p.brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer client.Close()

	offsets, err := kadm.NewClient(client).ListEndOffsets(context.Background(), topic)
	if err != nil {
		t.Fatalf("list end offsets: %v", err)
	}

	var total int64
	offsets.Each(func(o kadm.ListedOffset) { total += o.Offset })
	return total
}

// eventIDFor returns the outbox event id emitted for a transaction, so a test
// can replay exactly the message the pipeline already delivered.
func (p *pipeline) eventIDFor(t *testing.T, transactionID uuid.UUID) string {
	t.Helper()

	var eventID uuid.UUID
	if err := p.db.Pool().QueryRow(context.Background(),
		`SELECT event_id FROM outbox WHERE aggregate_id = $1 LIMIT 1`, transactionID).Scan(&eventID); err != nil {
		t.Fatalf("read outbox event id: %v", err)
	}
	return eventID.String()
}

func (p *pipeline) payloadFor(t *testing.T, transactionID uuid.UUID) []byte {
	t.Helper()

	var payload []byte
	if err := p.db.Pool().QueryRow(context.Background(),
		`SELECT payload FROM outbox WHERE aggregate_id = $1 LIMIT 1`, transactionID).Scan(&payload); err != nil {
		t.Fatalf("read outbox payload: %v", err)
	}
	return payload
}

func (p *pipeline) create(t *testing.T, key string, amountMinor int64) uuid.UUID {
	t.Helper()

	tx, err := p.service.Create(context.Background(), payment.CreateInput{
		MerchantID:     "m_async",
		IdempotencyKey: key,
		Amount:         money.MustNew(amountMinor, "USD"),
		Actor:          "test",
	})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	// Create only enqueues; nothing has reached the provider yet.
	if tx.State != domain.StateCreated {
		t.Fatalf("state after Create = %s, want created — authorization should be asynchronous", tx.State)
	}
	return tx.ID
}

// The whole path: an HTTP request writes a transaction and an outbox row in one
// database transaction, the relay carries it to Kafka, and a worker authorizes
// it. The caller never waited on the provider.
func TestPaymentIsAuthorizedThroughTheAsynchronousPipeline(t *testing.T) {
	p := newPipeline(t, nil)

	id := p.create(t, "async-happy", 12550)
	tx := p.awaitState(t, id, 30*time.Second, domain.StateAuthorized)

	if tx.PSPReference == "" {
		t.Error("provider reference not recorded")
	}
	if got := p.store.Count(); got != 1 {
		t.Errorf("provider charges = %d, want 1", got)
	}
}

// A decline is terminal and must never travel the retry ladder. Repeating it
// cannot change the issuer's answer and escalates fraud controls.
func TestDeclineIsNotRetried(t *testing.T) {
	p := newPipeline(t, nil)

	// Amount ending in 51: ISO-8583 "not sufficient funds".
	id := p.create(t, "async-declined", 10051)
	p.awaitState(t, id, 30*time.Second, domain.StateFailed)

	// Give the ladder ample time to have produced a retry if it were going to.
	time.Sleep(2 * time.Second)

	if got := p.store.Count(); got != 0 {
		t.Errorf("provider charges = %d after a decline, want 0", got)
	}

	depth := p.topicDepth(t, p.topics.Retry[0].Topic)
	if depth != 0 {
		t.Errorf("%d messages on the first retry tier, want 0 — a decline must not be retried", depth)
	}
}

// An ambiguous provider failure that the worker recovers from must still
// produce exactly one charge.
func TestTimeoutAfterSuccessProducesOneChargeThroughThePipeline(t *testing.T) {
	p := newPipeline(t, map[simulator.Fault]float64{
		simulator.FaultTimeoutAfterSuccess: 1.0,
	})

	id := p.create(t, "async-timeout", 33300)
	p.awaitState(t, id, 30*time.Second, domain.StateAuthorized)

	if got := p.store.Count(); got != 1 {
		t.Errorf("provider charges = %d, want exactly 1", got)
	}
}

// A duplicate delivery must not run the work twice. The dedup table absorbs the
// ordinary case; the provider's idempotency key is what protects the money if a
// duplicate slips past it.
func TestDuplicateDeliveryIsNotProcessedTwice(t *testing.T) {
	p := newPipeline(t, nil)

	id := p.create(t, "async-dup", 4200)
	p.awaitState(t, id, 30*time.Second, domain.StateAuthorized)

	// Republish the same event, exactly as a redelivery would arrive.
	eventID := p.eventIDFor(t, id)
	producerClient, err := kgo.NewClient(kgo.SeedBrokers(p.brokers...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producerClient.Close()

	payload := p.payloadFor(t, id)
	if err := messaging.NewProducer(producerClient).Publish(
		context.Background(), p.topics.Authorize, "m_async", eventID, payload); err != nil {
		t.Fatalf("republish: %v", err)
	}

	time.Sleep(3 * time.Second)

	if got := p.store.Count(); got != 1 {
		t.Errorf("provider charges = %d after a duplicate delivery, want 1", got)
	}
}

// A payment whose provider never becomes reachable must travel the whole retry
// ladder and then be parked, not retried forever and not silently dropped. The
// DLQ is where a payment that could not be completed waits for a human.
func TestExhaustedRetriesLandInTheDeadLetterQueue(t *testing.T) {
	p := newPipeline(t, nil)

	// The provider is gone entirely: every attempt fails as a network error,
	// which is ambiguous and therefore retryable — so the ladder runs in full.
	p.sim.Close()

	id := p.create(t, "async-dlq", 8800)

	deadline := time.Now().Add(45 * time.Second)
	var depth int64
	for time.Now().Before(deadline) {
		if depth = p.topicDepth(t, p.topics.DLQ); depth > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if depth == 0 {
		t.Fatal("nothing reached the dead letter queue; the message was lost or is retrying forever")
	}

	// It must not be marked failed: nothing established that the payment could
	// not have succeeded, only that we never got an answer.
	tx, err := txstore.NewRepository().Get(context.Background(), p.db.Pool(), id)
	if err != nil {
		t.Fatalf("read transaction: %v", err)
	}
	if tx.State == domain.StateFailed {
		t.Error("an unreachable provider must not produce a failed transaction")
	}
}
