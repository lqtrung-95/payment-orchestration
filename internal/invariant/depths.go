package invariant

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/metrics"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// DepthSampler publishes how much work is waiting.
//
// Backlog is the metric this system was missing when a deferred retry stalled
// every partition a consumer owned: twenty payments sat at `created` and the
// only way to see it was to query Kafka by hand. Depth and lag are what turn
// that from an archaeology exercise into an alert.
type DepthSampler struct {
	db      *postgres.DB
	admin   *kadm.Client
	topics  messaging.Topics
	group   string
	metrics *metrics.Metrics
	logger  *slog.Logger
}

func NewDepthSampler(
	db *postgres.DB,
	client *kgo.Client,
	topics messaging.Topics,
	consumerGroup string,
	m *metrics.Metrics,
	logger *slog.Logger,
) *DepthSampler {
	var admin *kadm.Client
	if client != nil {
		admin = kadm.NewClient(client)
	}
	return &DepthSampler{
		db: db, admin: admin, topics: topics,
		group: consumerGroup, metrics: m, logger: logger,
	}
}

// Sample takes one reading.
func (s *DepthSampler) Sample(ctx context.Context) error {
	if err := s.sampleOutbox(ctx); err != nil {
		return err
	}
	return s.sampleConsumerLag(ctx)
}

// sampleOutbox reports rows the relay has not yet carried to the broker.
//
// Sustained growth means the relay is behind or failing; a spike that drains is
// just a burst. Neither is visible from request metrics, because the API has
// already returned by the time the row exists.
func (s *DepthSampler) sampleOutbox(ctx context.Context) error {
	const query = `SELECT count(*) FROM outbox WHERE status = 'pending'`

	var pending int
	if err := s.db.Pool().QueryRow(ctx, query).Scan(&pending); err != nil {
		return fmt.Errorf("sample outbox depth: %w", err)
	}
	s.metrics.OutboxPending.Set(float64(pending))
	return nil
}

// sampleConsumerLag reports uncommitted messages per topic, and the dead letter
// queue's depth.
//
// Lag is computed from the group's committed offsets against the log end
// offsets, which is the only view that accounts for a partition being paused:
// a message deferred onto the thirty-minute tier is legitimately unread, and it
// should show as lag rather than as nothing at all.
func (s *DepthSampler) sampleConsumerLag(ctx context.Context) error {
	if s.admin == nil {
		return nil
	}

	lags, err := s.admin.Lag(ctx, s.group)
	if err != nil {
		return fmt.Errorf("fetch consumer lag: %w", err)
	}

	for _, groupLag := range lags {
		if err := groupLag.Error(); err != nil {
			// A group with no members yet is the ordinary case before a worker
			// starts, not a failure worth escalating.
			s.logger.DebugContext(ctx, "group lag unavailable",
				slog.String("group", groupLag.Group), slog.Any("error", err))
			continue
		}
		for _, topicLag := range groupLag.Lag.TotalByTopic().Sorted() {
			s.metrics.ConsumerLag.WithLabelValues(topicLag.Topic).Set(float64(topicLag.Lag))
		}
	}

	// The DLQ is not consumed by anyone, so it has no group lag. Its depth is
	// the end offset itself: everything ever parked there is still waiting.
	ends, err := s.admin.ListEndOffsets(ctx, s.topics.DLQ)
	if err != nil {
		return fmt.Errorf("list dlq offsets: %w", err)
	}
	var depth int64
	ends.Each(func(o kadm.ListedOffset) { depth += o.Offset })
	s.metrics.DLQDepth.Set(float64(depth))

	return nil
}

// Run samples on an interval until the context is cancelled.
func (s *DepthSampler) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.logger.InfoContext(ctx, "depth sampler started", slog.Duration("interval", interval))

	for {
		select {
		case <-ctx.Done():
			s.logger.InfoContext(ctx, "depth sampler stopped")
			return nil

		case <-ticker.C:
			if err := s.Sample(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				// Logged, not fatal. Losing visibility is bad; taking the
				// service down because a gauge could not be read is worse.
				s.logger.WarnContext(ctx, "depth sample failed", slog.Any("error", err))
			}
		}
	}
}
