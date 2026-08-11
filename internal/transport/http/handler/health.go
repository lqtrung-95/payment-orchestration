// Package handler holds HTTP request handlers.
package handler

import (
	"context"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// Checker is a named dependency that can report its own reachability.
type Checker interface {
	Ping(ctx context.Context) error
}

// NamedChecker pairs a dependency with the name reported in the readiness body.
type NamedChecker struct {
	Name    string
	Checker Checker
}

// checkTimeout bounds each dependency probe. A readiness endpoint that can hang
// is worse than one that reports a failure: orchestrators treat a timeout as
// indeterminate and may keep routing traffic to an instance that cannot serve.
const checkTimeout = 2 * time.Second

type Health struct {
	deps []NamedChecker
}

func NewHealth(deps ...NamedChecker) *Health {
	return &Health{deps: deps}
}

// Live answers the liveness probe. It deliberately checks nothing beyond the
// process being able to serve a request.
//
// Wiring dependency checks into liveness is a well-known way to turn a brief
// database blip into a rolling restart of every instance, which removes the
// capacity that would have absorbed the blip.
func (h *Health) Live(_ context.Context, c *app.RequestContext) {
	c.JSON(consts.StatusOK, utils.H{"status": "ok"})
}

// Ready answers the readiness probe, reporting per-dependency status and
// returning 503 if any dependency is unreachable. Probes run concurrently so
// total latency is bounded by the slowest dependency, not their sum.
func (h *Health) Ready(ctx context.Context, c *app.RequestContext) {
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()

	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		results = make(utils.H, len(h.deps))
		healthy = true
	)

	for _, dep := range h.deps {
		wg.Add(1)
		go func(dep NamedChecker) {
			defer wg.Done()

			status := "ok"
			if err := dep.Checker.Ping(ctx); err != nil {
				status = "unavailable"
			}

			mu.Lock()
			defer mu.Unlock()
			results[dep.Name] = status
			if status != "ok" {
				healthy = false
			}
		}(dep)
	}
	wg.Wait()

	code := consts.StatusOK
	overall := "ok"
	if !healthy {
		code = consts.StatusServiceUnavailable
		overall = "degraded"
	}

	// Dependency errors are logged elsewhere but never echoed here: readiness
	// endpoints are commonly reachable from outside, and a raw connection error
	// discloses internal hostnames and topology.
	c.JSON(code, utils.H{"status": overall, "dependencies": results})
}
