package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/resilience"
)

// Broker is the publishing side of the message transport.
type Broker interface {
	Publish(ctx context.Context, topic, partitionKey string, eventID string, payload []byte) error
}

type PublisherConfig struct {
	// PollInterval is how often to look for work when the last sweep found
	// nothing. Kept short because it is the floor on end-to-end latency for
	// every asynchronous operation.
	PollInterval time.Duration

	// BatchSize bounds how many rows one instance claims per sweep, so a large
	// backlog is shared across instances rather than monopolised by whichever
	// one polls first.
	BatchSize int

	// MaxAttempts is how many publish failures a message tolerates before it is
	// parked as failed for a human to look at.
	MaxAttempts int

	// ClaimLease is how long a claimed row stays invisible to other publishers.
	// It must exceed the time to publish a batch, or a slow publisher's rows
	// become claimable while it is still sending them. A crashed publisher's
	// rows are recovered when the lease expires.
	ClaimLease time.Duration

	// RetryFloor is the shortest gap before retrying a failed publish. Full
	// jitter can otherwise draw a near-zero delay, and retrying an unreachable
	// broker immediately is wasted work.
	RetryFloor time.Duration

	Backoff resilience.Backoff
}

func DefaultPublisherConfig() PublisherConfig {
	return PublisherConfig{
		PollInterval: 100 * time.Millisecond,
		BatchSize:    100,
		MaxAttempts:  10,
		ClaimLease:   30 * time.Second,
		RetryFloor:   50 * time.Millisecond,
		Backoff:      resilience.NewBackoff(200*time.Millisecond, 30*time.Second, 2),
	}
}

// Publisher relays outbox rows to the broker.
//
// Delivery is at-least-once by construction: a message is marked published only
// after the broker acknowledges it, so a crash between the two republishes it.
// That is the correct trade — a duplicate is absorbed by the consumer's
// deduplication, whereas a lost payment event has nothing to recover it.
type Publisher struct {
	db     *postgres.DB
	broker Broker
	cfg    PublisherConfig
	logger *slog.Logger
}

func NewPublisher(db *postgres.DB, broker Broker, cfg PublisherConfig, logger *slog.Logger) *Publisher {
	return &Publisher{db: db, broker: broker, cfg: cfg, logger: logger}
}

// Run polls until the context is cancelled.
func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	p.logger.InfoContext(ctx, "outbox publisher started",
		slog.Duration("poll_interval", p.cfg.PollInterval),
		slog.Int("batch_size", p.cfg.BatchSize))

	for {
		select {
		case <-ctx.Done():
			p.logger.InfoContext(ctx, "outbox publisher stopped")
			return nil

		case <-ticker.C:
			published, err := p.Sweep(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				p.logger.ErrorContext(ctx, "outbox sweep failed", slog.Any("error", err))
				continue
			}
			// A full batch means there is probably more waiting, so poll again
			// immediately rather than idling through a backlog one tick at a time.
			if published >= p.cfg.BatchSize {
				ticker.Reset(time.Millisecond)
			} else {
				ticker.Reset(p.cfg.PollInterval)
			}
		}
	}
}

// Sweep claims a batch and publishes it, returning how many succeeded.
func (p *Publisher) Sweep(ctx context.Context) (int, error) {
	records, err := p.claim(ctx)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}

	published := 0
	for _, rec := range records {
		if err := p.broker.Publish(ctx, rec.Topic, rec.PartitionKey, rec.EventID.String(), rec.Payload); err != nil {
			p.markFailed(ctx, rec, err)
			continue
		}
		if err := p.markPublished(ctx, rec); err != nil {
			// The broker has the message but the row still says pending, so it
			// will be republished. Duplicates are handled downstream; the
			// alternative is marking it published before the broker has it,
			// which loses the message outright.
			p.logger.ErrorContext(ctx, "published but failed to mark",
				slog.String("event_id", rec.EventID.String()), slog.Any("error", err))
			continue
		}
		published++
	}

	return published, nil
}

// claim takes a batch of due rows and leases them.
//
// The claim is an UPDATE, not merely a locking SELECT. Row locks last only as
// long as the transaction holding them, and publishing happens *after* that
// transaction ends — so a lock-only claim releases the rows the instant the
// claim commits, and every other publisher immediately picks up the same batch
// and sends it again.
//
// Pushing available_at forward instead leases the rows: they stay invisible to
// other publishers for the lease duration, without holding a database
// transaction open across network I/O. A publisher that dies mid-batch simply
// stops renewing, and the rows become claimable again when the lease expires,
// which is also how a crashed instance's work is recovered.
//
// FOR UPDATE SKIP LOCKED still matters inside the statement: it lets concurrent
// claims select disjoint rows rather than blocking on one another.
func (p *Publisher) claim(ctx context.Context) ([]Record, error) {
	var records []Record

	err := p.db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		const query = `
			UPDATE outbox SET available_at = now() + $2::interval
			WHERE id IN (
				SELECT id FROM outbox
				WHERE status = 'pending' AND available_at <= now()
				ORDER BY id
				LIMIT $1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING id, event_id, aggregate_id, partition_key, topic, payload, attempts, created_at`

		lease := fmt.Sprintf("%d milliseconds", p.cfg.ClaimLease.Milliseconds())
		rows, err := tx.Query(ctx, query, p.cfg.BatchSize, lease)
		if err != nil {
			return fmt.Errorf("claim outbox rows: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var rec Record
			if err := rows.Scan(&rec.ID, &rec.EventID, &rec.AggregateID,
				&rec.PartitionKey, &rec.Topic, &rec.Payload, &rec.Attempts, &rec.CreatedAt); err != nil {
				return fmt.Errorf("scan outbox row: %w", err)
			}
			records = append(records, rec)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}

	return records, nil
}

func (p *Publisher) markPublished(ctx context.Context, rec Record) error {
	const query = `
		UPDATE outbox SET status = 'published', published_at = now(), attempts = attempts + 1
		WHERE id = $1`

	_, err := p.db.Pool().Exec(ctx, query, rec.ID)
	return err
}

// markFailed backs the row off, or parks it once the cap is reached.
func (p *Publisher) markFailed(ctx context.Context, rec Record, cause error) {
	attempts := rec.Attempts + 1

	// Logged on every failure, not only when parking. A message retrying
	// quietly in the background looks identical to a stalled pipeline from the
	// outside, and the difference matters at three in the morning.
	p.logger.WarnContext(ctx, "outbox publish failed",
		slog.String("event_id", rec.EventID.String()),
		slog.String("topic", rec.Topic),
		slog.Int("attempt", attempts),
		slog.Any("error", cause))

	if attempts >= p.cfg.MaxAttempts {
		const park = `UPDATE outbox SET status = 'failed', attempts = $2, last_error = $3 WHERE id = $1`
		if _, err := p.db.Pool().Exec(ctx, park, rec.ID, attempts, cause.Error()); err != nil {
			p.logger.ErrorContext(ctx, "failed to park outbox row", slog.Any("error", err))
		}
		p.logger.ErrorContext(ctx, "outbox message parked after repeated publish failures",
			slog.String("event_id", rec.EventID.String()),
			slog.Int("attempts", attempts),
			slog.Any("error", cause))
		return
	}

	delay := p.cfg.Backoff.Delay(attempts)
	if delay < p.cfg.RetryFloor {
		delay = p.cfg.RetryFloor
	}
	const retry = `
		UPDATE outbox SET attempts = $2, last_error = $3, available_at = now() + $4::interval
		WHERE id = $1`

	interval := fmt.Sprintf("%d milliseconds", delay.Milliseconds())
	if _, err := p.db.Pool().Exec(ctx, retry, rec.ID, attempts, cause.Error(), interval); err != nil {
		p.logger.ErrorContext(ctx, "failed to back off outbox row", slog.Any("error", err))
	}
}
