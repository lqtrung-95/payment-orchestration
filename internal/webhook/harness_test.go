package webhook_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/outbox"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/simulator"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook/providers"
)

const (
	testSecret   = "test-webhook-secret"
	testProvider = "psp-sim"
)

type harness struct {
	db        *postgres.DB
	ingestor  *webhook.Ingestor
	processor *webhook.Processor
	events    *webhook.Repository
	txRepo    *txstore.Repository
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	db := testsupport.FreshDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	registry := webhook.NewRegistry(providers.NewSimulator(testProvider, testSecret))
	events := webhook.NewRepository()
	txRepo := txstore.NewRepository()

	return &harness{
		db: db,
		ingestor: webhook.NewIngestor(db, registry, events,
			outbox.NewWriter(), messaging.DefaultTopics(), logger),
		processor: webhook.NewProcessor(db, registry, events, txRepo, logger),
		events:    events,
		txRepo:    txRepo,
	}
}

// seedTransaction creates a transaction parked in authorizing with a provider
// reference, which is the state a webhook is expected to resolve.
func (h *harness) seedTransaction(t *testing.T, reference string) *domain.Transaction {
	t.Helper()
	return h.seedTransactionInState(t, reference, domain.StateAuthorizing)
}

func (h *harness) seedTransactionInState(t *testing.T, reference string, state domain.State) *domain.Transaction {
	t.Helper()
	ctx := context.Background()

	tx, err := domain.New("m_hook", "key-"+reference, money.MustNew(5000, "USD"))
	if err != nil {
		t.Fatalf("build transaction: %v", err)
	}
	if err := h.txRepo.Insert(ctx, h.db.Pool(), tx); err != nil {
		t.Fatalf("insert transaction: %v", err)
	}

	tx.PSP = testProvider
	tx.PSPReference = reference

	// Walked through the matrix one persisted step at a time, so the fixture
	// cannot set up a state the system could never actually reach. The database
	// trigger enforces this independently, and jumping straight to the target
	// is refused by it — which is the behaviour under test elsewhere.
	for _, step := range pathTo(state) {
		if err := tx.TransitionTo(step); err != nil {
			t.Fatalf("transition to %s: %v", step, err)
		}
		if err := h.txRepo.Update(ctx, h.db.Pool(), tx); err != nil {
			t.Fatalf("persist transition to %s: %v", step, err)
		}
	}
	return tx
}

// pathTo returns the transitions needed to reach a state from created.
func pathTo(state domain.State) []domain.State {
	switch state {
	case domain.StateCreated:
		return nil
	case domain.StateAuthorizing:
		return []domain.State{domain.StateAuthorizing}
	case domain.StateAuthorized:
		return []domain.State{domain.StateAuthorizing, domain.StateAuthorized}
	case domain.StateCaptured:
		return []domain.State{
			domain.StateAuthorizing, domain.StateAuthorized,
			domain.StateCapturing, domain.StateCaptured,
		}
	default:
		return []domain.State{state}
	}
}

func (h *harness) reload(t *testing.T, id uuid.UUID) *domain.Transaction {
	t.Helper()

	tx, err := h.txRepo.Get(context.Background(), h.db.Pool(), id)
	if err != nil {
		t.Fatalf("reload transaction: %v", err)
	}
	return tx
}

func (h *harness) rawEvent(t *testing.T, id int64) *webhook.RawEvent {
	t.Helper()

	e, err := h.events.Get(context.Background(), h.db.Pool(), id)
	if err != nil {
		t.Fatalf("read raw event: %v", err)
	}
	return e
}

// countStateChanges reports how many audit rows a transaction has, which is the
// real measure of "exactly one state transition".
func (h *harness) countStateChanges(t *testing.T, id uuid.UUID, to domain.State) int {
	t.Helper()

	var n int
	err := h.db.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM transaction_state_changes WHERE transaction_id = $1 AND to_state = $2`,
		id, string(to)).Scan(&n)
	if err != nil {
		t.Fatalf("count state changes: %v", err)
	}
	return n
}

func (h *harness) countOutboxFor(t *testing.T, topic string) int {
	t.Helper()

	var n int
	if err := h.db.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE topic = $1`, topic).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

// event builds a signed delivery in the simulator's own scheme.
func event(t *testing.T, id, reference, status string, sequence int64) ([]byte, webhook.Headers) {
	t.Helper()

	body, err := json.Marshal(simulator.Event{
		ID:        id,
		Type:      "charge.updated",
		Reference: reference,
		Status:    status,
		CreatedAt: time.Now().UTC(),
		Sequence:  sequence,
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	signature := simulator.Sign(testSecret, ts, body)

	return body, func(key string) string {
		switch key {
		case "X-Sim-Timestamp":
			return ts
		case "X-Sim-Signature":
			return signature
		default:
			return ""
		}
	}
}

// deliver ingests a signed event and returns the ingestion result.
func (h *harness) deliver(t *testing.T, id, reference, status string, seq int64) webhook.Result {
	t.Helper()

	body, hdr := event(t, id, reference, status, seq)
	result, err := h.ingestor.Ingest(context.Background(), testProvider, hdr, body)
	if err != nil {
		t.Fatalf("ingest %s: %v", id, err)
	}
	return result
}

// deliverAndProcess ingests then processes, which is what the worker does once
// the message comes off Kafka.
func (h *harness) deliverAndProcess(t *testing.T, id, reference, status string, seq int64) webhook.Outcome {
	t.Helper()

	result := h.deliver(t, id, reference, status, seq)
	outcome, err := h.processor.Process(context.Background(), result.RawEventID)
	if err != nil {
		t.Fatalf("process %s: %v", id, err)
	}
	return outcome
}
