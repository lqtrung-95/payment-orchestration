package simulator

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// Server is the fake provider's HTTP surface.
//
// It uses the standard library rather than the framework the orchestrator uses,
// which is deliberate: this stands in for a third party. Sharing a transport
// stack would blur a boundary that the whole design depends on being real.
type Server struct {
	store  *Store
	engine *Engine
	hooks  *WebhookEmitter
	logger *slog.Logger
	mux    *http.ServeMux

	// hangCap bounds how long a timeout fault holds a connection when the
	// client has no deadline of its own, so a stuck request cannot pin a
	// goroutine forever.
	hangCap time.Duration
}

func NewServer(store *Store, engine *Engine, hooks *WebhookEmitter, logger *slog.Logger) *Server {
	s := &Server{
		store:   store,
		engine:  engine,
		hooks:   hooks,
		logger:  logger,
		mux:     http.NewServeMux(),
		hangCap: 30 * time.Second,
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /v1/charges/authorize", s.handleAuthorize)
	s.mux.HandleFunc("POST /v1/charges/capture", s.handleCapture)
	s.mux.HandleFunc("POST /v1/charges/refund", s.handleRefund)
	s.mux.HandleFunc("POST /v1/charges/void", s.handleVoid)
	s.mux.HandleFunc("GET /v1/charges/status", s.handleStatus)

	// Drives a pending or redirect-based authorization to its outcome, standing
	// in for the customer completing a challenge out of band.
	s.mux.HandleFunc("POST /v1/simulate/complete", s.handleComplete)

	s.mux.HandleFunc("GET /admin/faults", s.handleGetFaults)
	s.mux.HandleFunc("PUT /admin/faults", s.handleSetFaults)
	s.mux.HandleFunc("PUT /admin/faults/preset", s.handleSetPreset)
	s.mux.HandleFunc("POST /admin/outage", s.handleOutage)
	s.mux.HandleFunc("POST /admin/reset", s.handleReset)

	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Wire types. These are the simulator's own vocabulary; the adapter translates
// between this and the normalized psp types, which is the translation every
// real provider integration has to do.

type chargeRequest struct {
	TransactionID string `json:"transaction_id"`
	Reference     string `json:"reference,omitempty"`
	AmountMinor   int64  `json:"amount_minor"`
	Currency      string `json:"currency"`
	ReturnURL     string `json:"return_url,omitempty"`
}

type chargeResponse struct {
	Reference   string `json:"reference"`
	Status      string `json:"status"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
	RedirectURL string `json:"redirect_url,omitempty"`
	RawStatus   string `json:"raw_status"`
	RawCode     string `json:"raw_code,omitempty"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Simulator status vocabulary, intentionally not identical to the
// orchestrator's. A provider that happened to share our words would hide the
// mapping work that every real integration requires.
const (
	simAuthorized     = "AUTH_OK"
	simCaptured       = "CAPTURED"
	simRefunded       = "REFUNDED"
	simVoided         = "VOIDED"
	simPending        = "PENDING_ASYNC"
	simRequiresAction = "ACTION_REQUIRED"
	simFailed         = "FAILED"
)

// Behaviour modes, selected per request so one process can present all three
// provider shapes.
const (
	ModeSync     = "sync"
	ModeAsync    = "async"
	ModeRedirect = "redirect"
)

func modeFrom(r *http.Request) string {
	switch m := r.Header.Get("X-Sim-Mode"); m {
	case ModeAsync, ModeRedirect:
		return m
	default:
		return ModeSync
	}
}

func idempotencyKeyFrom(r *http.Request) string { return r.Header.Get("Idempotency-Key") }

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Code: code, Message: message})
}

func decode(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}
