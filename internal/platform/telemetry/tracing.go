package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracingConfig configures the exporter and sampling.
type TracingConfig struct {
	// Enabled turns tracing on. Off by default: an exporter that cannot reach
	// a collector retries in the background, and a service that will not start
	// because nothing is listening on 4317 is worse than one with no traces.
	Enabled bool

	ServiceName string
	Environment string

	// Endpoint is the OTLP gRPC collector address.
	Endpoint string

	// SampleRatio is the fraction of traces recorded. Full sampling is right
	// for a demo and wrong under load, where the exporter itself becomes part
	// of what is being measured — so the ratio is configuration, and every
	// published number has to state which was used.
	SampleRatio float64
}

// Tracing is an initialised tracer provider and its shutdown hook.
type Tracing struct {
	provider *sdktrace.TracerProvider
	Tracer   trace.Tracer
}

// StartTracing wires the SDK and installs it globally.
//
// When disabled it returns a no-op tracer rather than nil, so instrumented code
// has one path rather than a nil check at every call site — a nil check that is
// missed once panics in the middle of a payment.
func StartTracing(ctx context.Context, cfg TracingConfig) (*Tracing, error) {
	if !cfg.Enabled {
		return &Tracing{Tracer: noop.NewTracerProvider().Tracer(cfg.ServiceName)}, nil
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		// Plaintext because the collector is on the same host or network. A
		// deployed setup terminates TLS at the collector.
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}

	// Merged with the SDK's default resource, which carries the schema version
	// the SDK was built against. The semconv import has to match it exactly —
	// merging across schema versions is an error, and it surfaces at startup
	// rather than as a missing attribute later.
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		attribute.String("deployment.environment", cfg.Environment),
	))
	if err != nil {
		return nil, fmt.Errorf("build trace resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		// ParentBased so a sampling decision made at the edge is honoured all
		// the way through. Sampling independently per service produces traces
		// with holes in them, which are worse than no trace: the gap looks like
		// a component that did not run.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)

	otel.SetTracerProvider(provider)

	// W3C trace context, and baggage alongside it. The propagator is what makes
	// a trace survive a process boundary, and it has to be the same on both
	// sides — this is set identically in the API and the worker.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Tracing{provider: provider, Tracer: provider.Tracer(cfg.ServiceName)}, nil
}

// Shutdown flushes pending spans.
//
// Given its own context rather than the cancelled shutdown one: the spans worth
// keeping most are the ones recorded just before the process stopped, and
// reusing an already-cancelled context discards exactly those.
func (t *Tracing) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	return t.provider.Shutdown(flushCtx)
}
