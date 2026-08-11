package simulator

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// faultKey identifies one *attempt* at an operation, not just the operation.
//
// Fault verdicts are derived from this key, and including the attempt number is
// what keeps the simulator both reproducible and escapable. Keyed on the
// idempotency key alone, a fault that fired once would fire on every retry
// forever, and no caller could ever recover — which is not how a real provider
// behaves and would make the recovery paths untestable. Keyed on the attempt,
// a given seed still replays exactly, because the sequence of attempts is
// itself deterministic.
func faultKey(idempotencyKey string, attempt int) string {
	return fmt.Sprintf("%s#%d", idempotencyKey, attempt)
}

// applyPreFaults handles failures that occur before any work is done. It
// reports whether the response has already been written.
func (s *Server) applyPreFaults(w http.ResponseWriter, r *http.Request, key string) bool {
	attempt := s.store.NextAttempt(key)
	fk := faultKey(key, attempt)

	if s.engine.InOutage(time.Now()) {
		s.logger.WarnContext(r.Context(), "rejecting request during outage", slog.String("key", key))
		writeError(w, http.StatusServiceUnavailable, "provider_unavailable", "provider is in an outage window")
		return true
	}

	if s.engine.Fires(FaultSlowResponse, fk) {
		delay := s.engine.SlowResponseDelay(fk)
		s.logger.WarnContext(r.Context(), "injecting slow response",
			slog.String("key", key), slog.Duration("delay", delay))

		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			// The client gave up while we were stalling, which is a legitimate
			// timeout. Nothing has happened yet, so there is nothing to record.
			return true
		}
	}

	return false
}

// applyPostSuccessFaults fires the two faults that matter most: the work has
// already been recorded, and now the caller's view of it is destroyed.
//
// These only fire when the operation actually did something. Breaking a replay
// of an already-recorded charge would be a different, less interesting bug.
func (s *Server) applyPostSuccessFaults(w http.ResponseWriter, r *http.Request, key string, created bool) bool {
	if !created {
		return false
	}

	attempt := s.store.CurrentAttempt(key)
	fk := faultKey(key, attempt)

	if s.engine.Fires(FaultTimeoutAfterSuccess, fk) {
		s.logger.WarnContext(r.Context(), "charge recorded, hanging connection",
			slog.String("key", key))

		// Hold the connection open without answering. From the caller's side
		// this is indistinguishable from a network black hole: the charge
		// exists, and they will never be told.
		select {
		case <-r.Context().Done():
		case <-time.After(s.hangCap):
		}
		return true
	}

	if s.engine.Fires(FaultError5xxAfterSuccess, fk) {
		s.logger.WarnContext(r.Context(), "charge recorded, returning 500",
			slog.String("key", key))
		writeError(w, http.StatusInternalServerError, "internal_error", "provider internal error")
		return true
	}

	return false
}
