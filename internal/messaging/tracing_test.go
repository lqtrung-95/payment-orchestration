package messaging

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// withPropagator installs the same propagator the services use.
func withPropagator(t *testing.T) trace.Tracer {
	t.Helper()

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return provider.Tracer("test")
}

// The claim that makes the whole pipeline legible: a span created on the
// consuming side belongs to the trace that produced the record, not to a new
// one. Most systems lose the trace here, because the producing goroutine's
// context is long gone by the time a consumer picks the record up — the only
// thing that crosses the broker is the record, so the context has to ride
// inside it.
func TestTraceContextSurvivesTheKafkaBoundary(t *testing.T) {
	tracer := withPropagator(t)

	produceCtx, produceSpan := tracer.Start(context.Background(), "produce")
	wantTrace := produceSpan.SpanContext().TraceID()

	record := &kgo.Record{Topic: "payment.authorize"}
	injectTrace(produceCtx, record)
	produceSpan.End()

	if TraceparentOf(record.Headers) == "" {
		t.Fatal("no traceparent header was written; the trace cannot cross the broker")
	}

	// A fresh context, as a consumer in another process would have.
	consumeCtx := extractTrace(context.Background(), record)
	_, consumeSpan := tracer.Start(consumeCtx, "consume")
	defer consumeSpan.End()

	gotTrace := consumeSpan.SpanContext().TraceID()
	if gotTrace != wantTrace {
		t.Errorf("consumer trace = %s, want %s — the span started a new trace instead of continuing one",
			gotTrace, wantTrace)
	}
	if !consumeSpan.SpanContext().IsValid() {
		t.Error("consumer span context is not valid")
	}
}

// A record republished onto a retry tier must not accumulate one traceparent
// per hop. A consumer reading the first of several would attach its span to a
// stale trace, which is worse than no trace at all: it looks like causality
// that did not happen.
func TestRepublishingReplacesTheTraceHeaderRatherThanAppending(t *testing.T) {
	tracer := withPropagator(t)

	record := &kgo.Record{Topic: "payment.authorize"}

	firstCtx, firstSpan := tracer.Start(context.Background(), "first publish")
	injectTrace(firstCtx, record)
	firstSpan.End()

	secondCtx, secondSpan := tracer.Start(context.Background(), "retry publish")
	injectTrace(secondCtx, record)
	secondSpan.End()

	var traceparents int
	for _, h := range record.Headers {
		if h.Key == "traceparent" {
			traceparents++
		}
	}
	if traceparents != 1 {
		t.Fatalf("traceparent headers = %d, want 1 — republishing appended instead of replacing", traceparents)
	}

	// And the one that survived is the most recent.
	got := extractTrace(context.Background(), record)
	if trace.SpanContextFromContext(got).TraceID() != secondSpan.SpanContext().TraceID() {
		t.Error("the surviving traceparent is not the most recently injected one")
	}
}

// Existing headers must be preserved: the event id and attempt count are what
// deduplication and the retry ladder run on, and losing them to make room for a
// traceparent would break both.
func TestInjectingATracePreservesTheOtherHeaders(t *testing.T) {
	tracer := withPropagator(t)

	record := &kgo.Record{
		Topic: "payment.authorize",
		Headers: []kgo.RecordHeader{
			{Key: HeaderEventID, Value: []byte("evt-1")},
			{Key: HeaderAttempt, Value: []byte("2")},
		},
	}

	ctx, span := tracer.Start(context.Background(), "publish")
	injectTrace(ctx, record)
	span.End()

	found := map[string]string{}
	for _, h := range record.Headers {
		found[h.Key] = string(h.Value)
	}
	if found[HeaderEventID] != "evt-1" {
		t.Errorf("event id = %q, want evt-1", found[HeaderEventID])
	}
	if found[HeaderAttempt] != "2" {
		t.Errorf("attempt = %q, want 2", found[HeaderAttempt])
	}
	if found["traceparent"] == "" {
		t.Error("traceparent missing")
	}
}

// With no propagator configured — the default state until tracing is started —
// injection must be a silent no-op rather than a panic or a malformed header.
func TestInjectionIsHarmlessWithoutATracer(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	record := &kgo.Record{Topic: "payment.authorize"}
	injectTrace(context.Background(), record)

	if len(record.Headers) != 0 {
		t.Errorf("headers = %v, want none when nothing is propagating", record.Headers)
	}
}
