package messaging

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tracer is resolved from the global provider, which is a no-op until tracing
// is started — so this file behaves identically whether or not it is enabled.
var tracer = otel.Tracer("payment-orchestrator/messaging")

// HeaderEventID carries the outbox event id through the broker so a consumer
// can deduplicate without parsing the payload.
const HeaderEventID = "event-id"

// HeaderAttempt carries how many times this message has already been tried,
// which is what selects the next rung of the retry ladder.
const HeaderAttempt = "retry-attempt"

// HeaderOriginTopic names the topic a message was first published to.
//
// The retry tiers are shared by every kind of work, so once a message is on
// one, its own topic no longer says what it is. Carrying the origin forward is
// what lets a single consumer group route a deferred message back to the
// handler that owns it, and it is also the first thing anyone wants to know
// when inspecting the dead letter queue.
const HeaderOriginTopic = "origin-topic"

// Producer publishes to Kafka.
type Producer struct {
	client *kgo.Client
}

func NewProducer(client *kgo.Client) *Producer { return &Producer{client: client} }

// Publish sends a message and waits for the broker to acknowledge it.
//
// Synchronous on purpose. The outbox marks a row published only once this
// returns, so an asynchronous produce would let the row be marked while the
// message was still in a buffer that a crash would discard — which is precisely
// the lost-event case the outbox exists to prevent.
func (p *Producer) Publish(ctx context.Context, topic, partitionKey, eventID string, payload []byte) error {
	return p.PublishWithHeaders(ctx, topic, partitionKey, eventID, payload, nil)
}

// PublishWithHeaders is Publish with extra headers, used by the retry ladder to
// carry the attempt count forward.
func (p *Producer) PublishWithHeaders(
	ctx context.Context,
	topic, partitionKey, eventID string,
	payload []byte,
	extra map[string]string,
) error {
	headers := []kgo.RecordHeader{{Key: HeaderEventID, Value: []byte(eventID)}}
	for k, v := range extra {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}

	record := &kgo.Record{
		Topic: topic,
		// The key determines the partition. Keying by merchant is what makes
		// every event for one merchant land on one partition, and therefore
		// ordered relative to the others.
		Key:     []byte(partitionKey),
		Value:   payload,
		Headers: headers,
	}

	ctx, span := tracer.Start(ctx, "kafka.publish "+topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.message.id", eventID),
		))
	defer span.End()

	// Injected after the span starts, so the headers carry *this* span as the
	// parent. Injecting before would propagate whatever was active on the way
	// in and orphan the publish from the work it caused.
	injectTrace(ctx, record)

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	return nil
}

// Close flushes any buffered records.
func (p *Producer) Close() { p.client.Close() }
