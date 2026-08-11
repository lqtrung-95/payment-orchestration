package messaging

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is a consumed record in the vocabulary handlers work in.
type Message struct {
	Topic        string
	PartitionKey string
	EventID      string
	Attempt      int
	Payload      []byte

	// Timestamp is when the broker received the record. The retry tiers use it
	// to decide whether a message is due yet.
	Timestamp time.Time
}

// Handler processes one message. Returning an error means the work did not
// succeed and the message should travel the retry ladder.
type Handler func(ctx context.Context, msg Message) error

// Consumer runs a consumer group.
type Consumer struct {
	client *kgo.Client
	group  string
	logger *slog.Logger
}

// NewConsumer builds a consumer group client with auto-commit disabled.
//
// Offsets are committed only after a handler succeeds. With auto-commit, an
// offset advances on a timer regardless of whether the work completed, so a
// crash mid-handler silently skips the message — which for a payment means an
// authorization that never happens and no record of why.
func NewConsumer(brokers []string, group, clientID string, topics []string, logger *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		// Start from the beginning for a group with no committed offset, so a
		// worker deployed after messages were produced still processes them.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Bound how long a slow handler can stall the group before its
		// partitions are reassigned.
		kgo.SessionTimeout(30*time.Second),
		kgo.RebalanceTimeout(60*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("create consumer group client: %w", err)
	}

	return &Consumer{client: client, group: group, logger: logger}, nil
}

func (c *Consumer) Close() { c.client.Close() }

// Run polls and dispatches until the context is cancelled.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	c.logger.InfoContext(ctx, "consumer started", slog.String("group", c.group))

	for {
		if ctx.Err() != nil {
			c.logger.InfoContext(ctx, "consumer stopped", slog.String("group", c.group))
			//nolint:nilerr // a cancelled context is a clean shutdown, not a failure
			return nil
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			// A cancelled context during a fetch is a shutdown, not a failure.
			// Checked before inspecting the errors so the two cannot be
			// confused for one another.
			if ctx.Err() != nil {
				c.logger.InfoContext(ctx, "consumer stopped", slog.String("group", c.group))
				//nolint:nilerr // a cancelled context is a clean shutdown, not a failure
				return nil
			}
			for _, e := range errs {
				c.logger.ErrorContext(ctx, "fetch error",
					slog.String("topic", e.Topic), slog.Any("error", e.Err))
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			msg := toMessage(record)

			if err := handler(ctx, msg); err != nil {
				// The handler owns the failure path: by the time it returns an
				// error it has already routed the message to a retry tier or the
				// DLQ. Not committing here would replay it forever and stall the
				// partition behind it.
				c.logger.ErrorContext(ctx, "handler failed",
					slog.String("topic", msg.Topic),
					slog.String("event_id", msg.EventID),
					slog.Any("error", err))
			}

			if err := c.client.CommitRecords(ctx, record); err != nil {
				// The offset did not advance, so this record is redelivered.
				// The consumer's deduplication absorbs the repeat.
				c.logger.ErrorContext(ctx, "commit failed",
					slog.String("event_id", msg.EventID), slog.Any("error", err))
			}
		})
	}
}

func toMessage(record *kgo.Record) Message {
	msg := Message{
		Topic:        record.Topic,
		PartitionKey: string(record.Key),
		Payload:      record.Value,
		Timestamp:    record.Timestamp,
	}

	for _, h := range record.Headers {
		switch h.Key {
		case HeaderEventID:
			msg.EventID = string(h.Value)
		case HeaderAttempt:
			if n, err := strconv.Atoi(string(h.Value)); err == nil {
				msg.Attempt = n
			}
		}
	}
	return msg
}
