// Package messaging wraps Kafka: topic definitions, a producer keyed for
// per-merchant ordering, and a consumer group that commits only after work
// succeeds.
package messaging

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// TopicAuthorize carries authorization work off the request path.
	TopicAuthorize = "payment.authorize"

	// TopicWebhook carries accepted provider callbacks to their processing.
	// Ingestion only persists and acknowledges; everything after that happens
	// here, because a provider that waits on our processing retries and
	// amplifies load exactly when we are least able to absorb it.
	TopicWebhook = "webhook.received"

	// TopicDLQ holds messages that exhausted the retry ladder. Nothing is
	// deleted: a payment that could not be completed is evidence, and it needs a
	// human decision rather than a silent drop.
	TopicDLQ = "payment.dlq"
)

// RetryTier is one rung of the retry ladder.
//
// Kafka has no native delayed delivery, so delay is modelled as a set of topics
// whose consumers wait until each message is due. The alternative — sleeping
// inside the handler before retrying — loses every scheduled retry when a
// consumer restarts, and holds a partition hostage while it sleeps.
type RetryTier struct {
	Topic string
	Delay time.Duration
}

// RetryTiers escalate: a transient blip clears on the first rung, while a
// provider having a bad hour is retried at a pace that does not add to its
// problems.
var RetryTiers = []RetryTier{
	{Topic: "payment.retry.5s", Delay: 5 * time.Second},
	{Topic: "payment.retry.30s", Delay: 30 * time.Second},
	{Topic: "payment.retry.5m", Delay: 5 * time.Minute},
	{Topic: "payment.retry.30m", Delay: 30 * time.Minute},
}

// Topics is the set of topic names a process uses.
//
// Passed around rather than referenced as globals so tests can run against
// isolated topics. Sharing fixed names across runs makes every test see the
// previous run's backlog, which turns real failures into noise and vice versa.
type Topics struct {
	Authorize string
	Webhook   string
	DLQ       string
	Retry     []RetryTier
}

// DefaultTopics is the production set.
func DefaultTopics() Topics {
	return Topics{
		Authorize: TopicAuthorize,
		Webhook:   TopicWebhook,
		DLQ:       TopicDLQ,
		Retry:     RetryTiers,
	}
}

// PrefixedTopics returns the same shape under a namespace, for tests.
func PrefixedTopics(prefix string, retryDelay time.Duration) Topics {
	retry := make([]RetryTier, len(RetryTiers))
	for i, tier := range RetryTiers {
		delay := tier.Delay
		if retryDelay > 0 {
			// Tests cannot wait out the real ladder; the routing behaviour is
			// what is under test, not the wall-clock delay.
			delay = retryDelay
		}
		retry[i] = RetryTier{Topic: prefix + tier.Topic, Delay: delay}
	}
	return Topics{
		Authorize: prefix + TopicAuthorize,
		Webhook:   prefix + TopicWebhook,
		DLQ:       prefix + TopicDLQ,
		Retry:     retry,
	}
}

// All lists every topic, for provisioning and subscription.
func (t Topics) All() []string {
	out := []string{t.Authorize, t.Webhook, t.DLQ}
	for _, tier := range t.Retry {
		out = append(out, tier.Topic)
	}
	return out
}

// Consumed lists the topics a worker subscribes to. The DLQ is excluded: it is
// a parking area inspected deliberately, and consuming it automatically would
// put failed work straight back into the loop it just escaped.
func (t Topics) Consumed() []string {
	out := []string{t.Authorize, t.Webhook}
	for _, tier := range t.Retry {
		out = append(out, tier.Topic)
	}
	return out
}

// TierForAttempt returns the rung for a given retry attempt, and whether one
// exists. Exhausting the ladder means the message belongs in the DLQ.
func (t Topics) TierForAttempt(attempt int) (RetryTier, bool) {
	idx := attempt - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(t.Retry) {
		return RetryTier{}, false
	}
	return t.Retry[idx], true
}

// MaxRetryAttempts is the ladder's length: the default cap on retries before a
// message is parked.
func (t Topics) MaxRetryAttempts() int { return len(t.Retry) }

// DelayFor returns the configured delay for a retry topic, and whether the
// topic is one.
func (t Topics) DelayFor(topic string) (time.Duration, bool) {
	for _, tier := range t.Retry {
		if tier.Topic == topic {
			return tier.Delay, true
		}
	}
	return 0, false
}

// DueAt reports when a message may be processed: immediately for live work, and
// one tier delay after the broker received it for a deferred retry.
//
// Handed to the consumer rather than applied inside a handler. A handler that
// waits is a handler that blocks every partition the consumer owns, and a wait
// longer than the rebalance timeout gets the consumer evicted from its group —
// so the waiting has to happen where partitions can be paused instead.
func (t Topics) DueAt(msg Message) time.Time {
	delay, isRetryTier := t.DelayFor(msg.Topic)
	if !isRetryTier || delay == 0 {
		return time.Time{}
	}
	return msg.Timestamp.Add(delay)
}

// workPartitions is the partition count for topics carrying payment work.
//
// Ordering is guaranteed per partition, and messages are keyed by merchant, so
// this is also the ceiling on consumer parallelism for that work. Twelve is
// comfortably more than this system needs while still being divisible by common
// consumer counts.
const workPartitions = 12

// EnsureTopics creates the topics if they do not exist.
//
// Topics are provisioned explicitly rather than auto-created on first produce,
// because a topic that materialises with broker defaults gets the default
// partition count — and partition count is what carries the ordering guarantee
// this system depends on. Discovering that after the fact means a migration.
func EnsureTopics(ctx context.Context, client *kgo.Client, topics Topics) error {
	admin := kadm.NewClient(client)

	wanted := map[string]int32{
		topics.Authorize: workPartitions,
		topics.Webhook:   workPartitions,
		// One partition: the DLQ is inspected and replayed by hand, so ordering
		// across it is more useful than throughput.
		topics.DLQ: 1,
	}
	for _, tier := range topics.Retry {
		wanted[tier.Topic] = workPartitions
	}

	existing, err := admin.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}

	for topic, partitions := range wanted {
		if existing.Has(topic) {
			continue
		}
		// Replication factor 1 suits a single-broker development cluster; a
		// deployed cluster would raise this and set min.insync.replicas.
		resp, err := admin.CreateTopic(ctx, partitions, 1, nil, topic)
		if err != nil {
			return fmt.Errorf("create topic %s: %w", topic, err)
		}
		if resp.Err != nil && !isTopicExists(resp.Err) {
			return fmt.Errorf("create topic %s: %w", topic, resp.Err)
		}
	}

	return nil
}

// isTopicExists reports whether the error is a benign "already created" race
// between two instances starting at once.
func isTopicExists(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "already exists")
}
