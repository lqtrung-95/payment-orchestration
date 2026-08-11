package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
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

	// OriginTopic is the topic this work was first published to, carried
	// through the retry tiers. Empty on a message that has never been retried.
	OriginTopic string

	// Timestamp is when the broker received the record. The retry tiers use it
	// to decide whether a message is due yet.
	Timestamp time.Time
}

// Origin returns the topic that identifies what kind of work this is,
// regardless of which retry tier it currently sits on.
func (m Message) Origin() string {
	if m.OriginTopic != "" {
		return m.OriginTopic
	}
	return m.Topic
}

// Handler processes one message. Returning an error means the work did not
// succeed and the message should travel the retry ladder.
type Handler func(ctx context.Context, msg Message) error

// DueFunc reports when a message becomes eligible for processing. A zero time
// means immediately.
type DueFunc func(Message) time.Time

// pollTimeout bounds one poll so the loop regularly regains control to resume
// partitions whose delay has elapsed. Without it, a consumer whose partitions
// are all paused would block in PollFetches and never wake to unpause them.
const pollTimeout = time.Second

// Consumer runs a consumer group.
type Consumer struct {
	client *kgo.Client
	group  string
	logger *slog.Logger

	dueAt DueFunc

	// paused tracks partitions deferred until their message is due.
	mu     sync.Mutex
	paused map[topicPartition]time.Time
}

type topicPartition struct {
	topic     string
	partition int32
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

	return &Consumer{
		client: client,
		group:  group,
		logger: logger,
		paused: make(map[topicPartition]time.Time),
	}, nil
}

// WithDue makes the consumer honour per-message due times.
//
// A message that is not yet due does not block: its partition is paused and
// rewound, and the loop carries on with everything else. Sleeping instead —
// the obvious implementation — stalls every partition the consumer owns,
// because records from all of them are processed by one goroutine. It also
// stops the consumer from polling, so a wait longer than the rebalance timeout
// gets it evicted from the group.
func (c *Consumer) WithDue(due DueFunc) *Consumer {
	c.dueAt = due
	return c
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

		c.resumeDuePartitions(ctx)

		// Bounded so the loop reliably comes back to resume paused partitions.
		pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
		fetches := c.client.PollFetches(pollCtx)
		cancel()

		if fetches.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			c.logger.InfoContext(ctx, "consumer stopped", slog.String("group", c.group))
			//nolint:nilerr // a cancelled context is a clean shutdown, not a failure
			return nil
		}
		c.logFetchErrors(ctx, fetches)

		// Per partition rather than per record, so deferring one partition's
		// head message leaves the others untouched.
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			c.handlePartition(ctx, p, handler)
		})
	}
}

// handlePartition processes a partition's records in order, stopping at the
// first one that is not yet due.
func (c *Consumer) handlePartition(ctx context.Context, p kgo.FetchTopicPartition, handler Handler) {
	for _, record := range p.Records {
		msg := toMessage(record)

		if c.deferUntilDue(ctx, record, msg) {
			// Everything behind it on this partition waits too, which is right:
			// a delay topic is ordered by time, so the head message is always
			// the one that becomes due first.
			return
		}

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
	}
}

// deferUntilDue pauses and rewinds a partition whose head message is not ready,
// reporting whether it did so.
func (c *Consumer) deferUntilDue(ctx context.Context, record *kgo.Record, msg Message) bool {
	if c.dueAt == nil {
		return false
	}
	due := c.dueAt(msg)
	if due.IsZero() || !time.Now().Before(due) {
		return false
	}

	c.client.PauseFetchPartitions(map[string][]int32{record.Topic: {record.Partition}})

	// Rewound to this record so it is delivered again on resume. The offset was
	// never committed, but the client's own position has already advanced past
	// it in memory, and without the rewind the message would be skipped.
	c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		record.Topic: {record.Partition: {
			Epoch:  record.LeaderEpoch,
			Offset: record.Offset,
		}},
	})

	c.mu.Lock()
	c.paused[topicPartition{record.Topic, record.Partition}] = due
	c.mu.Unlock()

	c.logger.DebugContext(ctx, "deferring partition until its message is due",
		slog.String("topic", record.Topic),
		slog.Int("partition", int(record.Partition)),
		slog.Time("due", due))

	return true
}

// resumeDuePartitions unpauses partitions whose wait has elapsed.
func (c *Consumer) resumeDuePartitions(ctx context.Context) {
	c.mu.Lock()
	var ready map[string][]int32
	now := time.Now()
	for tp, due := range c.paused {
		if now.Before(due) {
			continue
		}
		if ready == nil {
			ready = make(map[string][]int32)
		}
		ready[tp.topic] = append(ready[tp.topic], tp.partition)
		delete(c.paused, tp)
	}
	c.mu.Unlock()

	if ready == nil {
		return
	}
	c.client.ResumeFetchPartitions(ready)
	c.logger.DebugContext(ctx, "resuming partitions", slog.Any("partitions", ready))
}

// logFetchErrors reports fetch problems, ignoring the deadline from the poll
// timeout, which is this loop's own doing rather than a broker failure.
func (c *Consumer) logFetchErrors(ctx context.Context, fetches kgo.Fetches) {
	for _, e := range fetches.Errors() {
		if errors.Is(e.Err, context.DeadlineExceeded) || errors.Is(e.Err, context.Canceled) {
			continue
		}
		c.logger.ErrorContext(ctx, "fetch error",
			slog.String("topic", e.Topic), slog.Any("error", e.Err))
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
		case HeaderOriginTopic:
			msg.OriginTopic = string(h.Value)
		}
	}
	return msg
}
