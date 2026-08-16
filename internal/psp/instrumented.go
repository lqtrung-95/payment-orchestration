package psp

import (
	"context"
	"time"
)

// Observer records what a provider call did.
//
// Narrow on purpose: this package classifies provider failures and must not
// also know what a Prometheus histogram is. Two methods is the whole contract,
// and *metrics.Metrics satisfies it without either package importing the other.
type Observer interface {
	ObserveCall(provider, operation string, d time.Duration)
	ObserveError(provider, class string, declined bool)
}

// Instrument wraps an adapter so every call is timed and every failure is
// counted by its normalized class.
//
// A decorator rather than instrumentation at the call sites. The service calls
// providers from five places across authorize, capture, and the recovery path;
// instrumenting each one means five chances to forget, and the one that gets
// forgotten is the one whose absence looks like health.
//
// Passing a nil observer returns the adapter unchanged, so a command that does
// not export metrics is not forced to invent a no-op.
func Instrument(adapter Adapter, observer Observer) Adapter {
	if observer == nil {
		return adapter
	}
	return &instrumented{inner: adapter, observer: observer}
}

type instrumented struct {
	inner    Adapter
	observer Observer
}

func (i *instrumented) Name() string { return i.inner.Name() }

func (i *instrumented) Authorize(ctx context.Context, req AuthorizeRequest) (*Response, error) {
	return i.observe(ctx, "authorize", func() (*Response, error) { return i.inner.Authorize(ctx, req) })
}

func (i *instrumented) Capture(ctx context.Context, req CaptureRequest) (*Response, error) {
	return i.observe(ctx, "capture", func() (*Response, error) { return i.inner.Capture(ctx, req) })
}

func (i *instrumented) Refund(ctx context.Context, req RefundRequest) (*Response, error) {
	return i.observe(ctx, "refund", func() (*Response, error) { return i.inner.Refund(ctx, req) })
}

func (i *instrumented) Void(ctx context.Context, req VoidRequest) (*Response, error) {
	return i.observe(ctx, "void", func() (*Response, error) { return i.inner.Void(ctx, req) })
}

func (i *instrumented) GetStatus(ctx context.Context, req StatusRequest) (*Response, error) {
	return i.observe(ctx, "get_status", func() (*Response, error) { return i.inner.GetStatus(ctx, req) })
}

// observe times a call and records its outcome.
//
// The duration is recorded whether the call succeeded or failed, because a
// provider's timeouts are exactly the calls whose latency matters most —
// dropping them would make a hanging provider look fast.
func (i *instrumented) observe(_ context.Context, operation string, call func() (*Response, error)) (*Response, error) {
	started := time.Now()
	resp, err := call()
	i.observer.ObserveCall(i.inner.Name(), operation, time.Since(started))

	if err != nil {
		class := ClassOf(err)
		i.observer.ObserveError(i.inner.Name(), string(class), class.IsTerminal())
	}
	return resp, err
}

// InstrumentAll wraps every adapter in one call.
//
// Exists so a command cannot instrument two of its three providers and leave
// the third reporting nothing — which would read as a provider that never
// fails rather than one nobody is watching.
func InstrumentAll(observer Observer, adapters ...Adapter) []Adapter {
	out := make([]Adapter, 0, len(adapters))
	for _, a := range adapters {
		out = append(out, Instrument(a, observer))
	}
	return out
}
