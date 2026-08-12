package outbox_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/outbox"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
)

// recordingBroker stands in for Kafka so publish failures can be provoked on
// demand. What is under test here is the outbox's own contract — atomicity with
// the caller's transaction, safe claiming, backoff — none of which involves a
// real broker.
type recordingBroker struct {
	mu        sync.Mutex
	published []string
	failNext  int
	failAll   bool
}

func (b *recordingBroker) Publish(_ context.Context, _, _, eventID string, _ []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.failAll || b.failNext > 0 {
		if b.failNext > 0 {
			b.failNext--
		}
		return errors.New("broker unavailable")
	}
	b.published = append(b.published, eventID)
	return nil
}

func (b *recordingBroker) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.published)
}

func newPublisher(db *postgres.DB, broker outbox.Broker) *outbox.Publisher {
	cfg := outbox.DefaultPublisherConfig()
	cfg.BatchSize = 50
	cfg.MaxAttempts = 3
	return outbox.NewPublisher(db, broker, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// pendingRows counts unpublished rows without a *testing.T, so it is safe to
// call from the publisher goroutines — t.Fatalf from a non-test goroutine does
// not do what it looks like it does.
func pendingRows(ctx context.Context, db *postgres.DB) (int, error) {
	var n int
	err := db.Pool().QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE status = 'pending'`).Scan(&n)
	return n, err
}

func countByStatus(t *testing.T, db *postgres.DB, status string) int {
	t.Helper()

	var n int
	if err := db.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE status = $1::outbox_status`, status).Scan(&n); err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	return n
}

// The reason the outbox exists: the message and the domain change share a
// transaction, so a rollback takes both. Publishing to a broker inside the
// transaction would have emitted work for a payment that never existed.
func TestRollbackDiscardsTheMessageWithTheDomainWrite(t *testing.T) {
	db := testsupport.FreshDB(t)
	writer := outbox.NewWriter()
	ctx := context.Background()

	sentinel := errors.New("domain write failed")
	err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := writer.Enqueue(ctx, tx, outbox.Message{
			AggregateID:  uuid.New(),
			PartitionKey: "m_1",
			Topic:        "payment.authorize",
			Payload:      map[string]string{"hello": "world"},
		}); err != nil {
			return err
		}
		return sentinel
	})

	if !errors.Is(err, sentinel) {
		t.Fatalf("WithTx error = %v, want the sentinel", err)
	}
	if got := countByStatus(t, db, "pending"); got != 0 {
		t.Errorf("%d pending messages survived a rolled-back transaction, want 0", got)
	}
}

func TestCommitKeepsTheMessage(t *testing.T) {
	db := testsupport.FreshDB(t)
	writer := outbox.NewWriter()
	ctx := context.Background()

	err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := writer.Enqueue(ctx, tx, outbox.Message{
			AggregateID:  uuid.New(),
			PartitionKey: "m_1",
			Topic:        "payment.authorize",
			Payload:      map[string]string{"hello": "world"},
		})
		return err
	})
	if err != nil {
		t.Fatalf("WithTx returned error: %v", err)
	}

	if got := countByStatus(t, db, "pending"); got != 1 {
		t.Errorf("pending messages = %d, want 1", got)
	}
}

func TestSweepPublishesAndMarksRows(t *testing.T) {
	db := testsupport.FreshDB(t)
	writer := outbox.NewWriter()
	broker := &recordingBroker{}
	publisher := newPublisher(db, broker)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			_, err := writer.Enqueue(ctx, tx, outbox.Message{
				AggregateID: uuid.New(), PartitionKey: "m_1",
				Topic: "payment.authorize", Payload: map[string]int{"n": i},
			})
			return err
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	published, err := publisher.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep returned error: %v", err)
	}
	if published != 10 {
		t.Errorf("published = %d, want 10", published)
	}
	if got := countByStatus(t, db, "published"); got != 10 {
		t.Errorf("rows marked published = %d, want 10", got)
	}
	if got := countByStatus(t, db, "pending"); got != 0 {
		t.Errorf("pending rows = %d, want 0", got)
	}
}

// A failed publish must leave the row claimable rather than losing it, and must
// back off rather than spinning.
func TestFailedPublishBacksOffAndRetains(t *testing.T) {
	db := testsupport.FreshDB(t)
	writer := outbox.NewWriter()
	broker := &recordingBroker{failNext: 1}
	publisher := newPublisher(db, broker)
	ctx := context.Background()

	if err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := writer.Enqueue(ctx, tx, outbox.Message{
			AggregateID: uuid.New(), PartitionKey: "m_1",
			Topic: "payment.authorize", Payload: map[string]string{"a": "b"},
		})
		return err
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	published, err := publisher.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep returned error: %v", err)
	}
	if published != 0 {
		t.Errorf("published = %d, want 0", published)
	}
	if got := countByStatus(t, db, "pending"); got != 1 {
		t.Fatalf("pending rows = %d, want 1 — a failed publish must not lose the message", got)
	}

	var attempts int
	var lastError *string
	if err := db.Pool().QueryRow(ctx,
		`SELECT attempts, last_error FROM outbox LIMIT 1`).Scan(&attempts, &lastError); err != nil {
		t.Fatalf("read outbox row: %v", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}
	if lastError == nil || *lastError == "" {
		t.Error("last_error not recorded; the failure would be undiagnosable")
	}

	// Backed off, so an immediate sweep finds nothing due.
	if n, err := publisher.Sweep(ctx); err != nil || n != 0 {
		t.Errorf("immediate re-sweep published %d (err %v), want 0 — the row should be backed off", n, err)
	}
}

// Repeated failures park the message rather than retrying it forever.
func TestMessageIsParkedAfterMaxAttempts(t *testing.T) {
	db := testsupport.FreshDB(t)
	writer := outbox.NewWriter()
	broker := &recordingBroker{failAll: true}

	cfg := outbox.DefaultPublisherConfig()
	cfg.MaxAttempts = 2
	// No backoff at all, so the test does not have to wait it out. The floor is
	// zeroed too, otherwise the row stays leased past the next sweep.
	cfg.Backoff.Base, cfg.Backoff.Max = 0, 0
	cfg.RetryFloor = 0
	publisher := outbox.NewPublisher(db, broker, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	if err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := writer.Enqueue(ctx, tx, outbox.Message{
			AggregateID: uuid.New(), PartitionKey: "m_1",
			Topic: "payment.authorize", Payload: map[string]string{"a": "b"},
		})
		return err
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := publisher.Sweep(ctx); err != nil {
			t.Fatalf("sweep %d: %v", i, err)
		}
	}

	if got := countByStatus(t, db, "failed"); got != 1 {
		t.Errorf("parked rows = %d, want 1", got)
	}
	// Parked, not deleted: a payment event that could not be published is
	// evidence, and dropping it would leave no trace of the problem.
	if got := countByStatus(t, db, "pending"); got != 0 {
		t.Errorf("pending rows = %d, want 0", got)
	}
}

// FOR UPDATE SKIP LOCKED is what lets several publishers run at once. Without
// it they serialise; with it, each takes rows the others have not locked. The
// property that matters is that no message is published twice.
func TestConcurrentPublishersDoNotDuplicate(t *testing.T) {
	db := testsupport.FreshDB(t)
	writer := outbox.NewWriter()
	broker := &recordingBroker{}
	ctx := context.Background()

	const messages = 120
	for i := 0; i < messages; i++ {
		if err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			_, err := writer.Enqueue(ctx, tx, outbox.Message{
				AggregateID: uuid.New(), PartitionKey: "m_1",
				Topic: "payment.authorize", Payload: map[string]int{"n": i},
			})
			return err
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	// Each publisher sweeps until the table is drained rather than a fixed
	// number of times. A fixed budget makes this test load-sensitive: sweeps
	// that race and claim nothing still consume it, so under a loaded machine
	// the publishers can run out of turns with rows still pending — which reads
	// as "work was dropped" when nothing was dropped at all.
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := newPublisher(db, broker)
			deadline := time.Now().Add(30 * time.Second)

			for time.Now().Before(deadline) {
				n, err := p.Sweep(ctx)
				if err != nil {
					t.Errorf("sweep: %v", err)
					return
				}
				if n > 0 {
					continue
				}
				// Nothing claimable. Either the work is finished, or another
				// publisher holds the remaining rows under a lease.
				pending, err := pendingRows(ctx, db)
				if err != nil {
					t.Errorf("count pending: %v", err)
					return
				}
				if pending == 0 {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Error("publisher did not drain the outbox within 30s")
		}()
	}
	wg.Wait()

	if got := broker.count(); got != messages {
		t.Errorf("broker received %d messages, want %d — publishers duplicated or dropped work", got, messages)
	}
	if got := countByStatus(t, db, "published"); got != messages {
		t.Errorf("rows marked published = %d, want %d", got, messages)
	}

	seen := make(map[string]bool, messages)
	broker.mu.Lock()
	defer broker.mu.Unlock()
	for _, id := range broker.published {
		if seen[id] {
			t.Fatalf("event %s was published more than once", id)
		}
		seen[id] = true
	}
}

func TestEnqueueRejectsIncompleteMessages(t *testing.T) {
	db := testsupport.FreshDB(t)
	writer := outbox.NewWriter()
	ctx := context.Background()

	err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := writer.Enqueue(ctx, tx, outbox.Message{AggregateID: uuid.New(), Topic: "t"})
		return err
	})
	if err == nil {
		t.Error("Enqueue accepted a message with no partition key")
	}

	err = db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := writer.Enqueue(ctx, tx, outbox.Message{AggregateID: uuid.New(), PartitionKey: "m_1"})
		return err
	})
	if err == nil {
		t.Error("Enqueue accepted a message with no topic")
	}
}

// A republished message keeps its identity, which is what lets a consumer
// recognise a duplicate delivery rather than treating it as new work.
func TestEventIDIsStableAcrossRepublication(t *testing.T) {
	db := testsupport.FreshDB(t)
	writer := outbox.NewWriter()
	broker := &recordingBroker{failNext: 1}

	cfg := outbox.DefaultPublisherConfig()
	cfg.Backoff.Base, cfg.Backoff.Max = 0, 0
	cfg.RetryFloor = 0
	publisher := outbox.NewPublisher(db, broker, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	var eventID uuid.UUID
	if err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		eventID, err = writer.Enqueue(ctx, tx, outbox.Message{
			AggregateID: uuid.New(), PartitionKey: "m_1",
			Topic: "payment.authorize", Payload: map[string]string{"a": "b"},
		})
		return err
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, err := publisher.Sweep(ctx); err != nil { // fails
		t.Fatalf("sweep: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := publisher.Sweep(ctx); err != nil { // succeeds
		t.Fatalf("sweep: %v", err)
	}

	broker.mu.Lock()
	defer broker.mu.Unlock()
	if len(broker.published) != 1 {
		t.Fatalf("broker received %d messages, want 1", len(broker.published))
	}
	if broker.published[0] != eventID.String() {
		t.Errorf("published event id = %s, want %s", broker.published[0], eventID)
	}
}
