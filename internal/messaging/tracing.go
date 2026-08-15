package messaging

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// kafkaHeaderCarrier adapts Kafka record headers to the propagation interface.
//
// This is the whole reason a trace survives the queue. Most systems lose the
// trace at the broker, because the producing goroutine's context does not exist
// any more by the time a consumer picks the record up — the only thing that
// crosses is the record itself, so the trace context has to travel inside it.
type kafkaHeaderCarrier struct {
	headers *[]kgo.RecordHeader
}

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(key, value string) {
	// Replace rather than append. A record that is republished — onto a retry
	// tier, or by the outbox after a crash — would otherwise accumulate one
	// traceparent per hop, and a consumer reading the first would attach the
	// span to the wrong trace.
	for i := range *c.headers {
		if (*c.headers)[i].Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// injectTrace writes the active trace context into a record's headers.
func injectTrace(ctx context.Context, record *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, kafkaHeaderCarrier{headers: &record.Headers})
}

// extractTrace returns a context carrying the trace the record was produced in.
//
// The consumer's span then continues that trace rather than starting a new one,
// which is what turns "the API did something, and separately a worker did
// something" into one causal story.
func extractTrace(ctx context.Context, record *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, kafkaHeaderCarrier{headers: &record.Headers})
}

// TraceparentOf returns the W3C traceparent carried by a message, for tests and
// for logging a correlation id alongside a message that has no span of its own.
func TraceparentOf(headers []kgo.RecordHeader) string {
	return kafkaHeaderCarrier{headers: &headers}.Get("traceparent")
}

// compile-time check that the carrier satisfies the propagation contract.
var _ propagation.TextMapCarrier = kafkaHeaderCarrier{}
