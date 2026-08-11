// Package simclient adapts the fault-injecting simulator to the psp.Adapter
// contract.
//
// The translation work here — provider status vocabulary to normalized status,
// transport and HTTP outcomes to normalized error classes — is exactly what a
// real provider integration consists of. Getting the error classification wrong
// is how a system ends up retrying a charge that already succeeded.
package simclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
)

// Config describes one simulated provider. Several instances point at the same
// simulator process with different modes, which is how one binary presents
// three structurally different providers.
type Config struct {
	Name    string
	BaseURL string

	// Mode selects the provider's shape: sync, async, or redirect.
	Mode string

	// Timeout is per provider. A provider known to be slow gets a longer budget
	// than one expected to answer immediately; a single global timeout would
	// either cut off the slow one or let the fast one hang.
	Timeout time.Duration
}

// Behaviour modes, matching the simulator's own vocabulary.
const (
	ModeSync     = "sync"
	ModeAsync    = "async"
	ModeRedirect = "redirect"
)

type Client struct {
	cfg  Config
	http *http.Client
}

func New(cfg Config) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &Client{
		cfg:  cfg,
		http: &http.Client{Timeout: cfg.Timeout},
	}
}

func (c *Client) Name() string { return c.cfg.Name }

func (c *Client) Authorize(ctx context.Context, req psp.AuthorizeRequest) (*psp.Response, error) {
	return c.post(ctx, "/v1/charges/authorize", req.IdempotencyKey, chargeRequest{
		TransactionID: req.TransactionID.String(),
		AmountMinor:   req.Amount.Amount(),
		Currency:      string(req.Amount.Currency()),
		ReturnURL:     req.ReturnURL,
	})
}

func (c *Client) Capture(ctx context.Context, req psp.CaptureRequest) (*psp.Response, error) {
	return c.post(ctx, "/v1/charges/capture", req.IdempotencyKey, chargeRequest{
		TransactionID: req.TransactionID.String(),
		Reference:     req.ProviderReference,
		AmountMinor:   req.Amount.Amount(),
		Currency:      string(req.Amount.Currency()),
	})
}

func (c *Client) Refund(ctx context.Context, req psp.RefundRequest) (*psp.Response, error) {
	return c.post(ctx, "/v1/charges/refund", req.IdempotencyKey, chargeRequest{
		TransactionID: req.TransactionID.String(),
		Reference:     req.ProviderReference,
		AmountMinor:   req.Amount.Amount(),
		Currency:      string(req.Amount.Currency()),
	})
}

func (c *Client) Void(ctx context.Context, req psp.VoidRequest) (*psp.Response, error) {
	return c.post(ctx, "/v1/charges/void", req.IdempotencyKey, chargeRequest{
		TransactionID: req.TransactionID.String(),
		Reference:     req.ProviderReference,
	})
}

func (c *Client) GetStatus(ctx context.Context, req psp.StatusRequest) (*psp.Response, error) {
	endpoint := c.cfg.BaseURL + "/v1/charges/status?idempotency_key=" + url.QueryEscape(req.IdempotencyKey)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, psp.NewError(c.cfg.Name, psp.ClassUnknown, "", "build status request", err)
	}
	return c.do(httpReq)
}

func (c *Client) post(ctx context.Context, path, idempotencyKey string, body chargeRequest) (*psp.Response, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, psp.NewError(c.cfg.Name, psp.ClassUnknown, "", "marshal request", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+path, bytes.NewReader(payload))
	if err != nil {
		return nil, psp.NewError(c.cfg.Name, psp.ClassUnknown, "", "build request", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", idempotencyKey)
	httpReq.Header.Set("X-Sim-Mode", c.cfg.Mode)

	return c.do(httpReq)
}

func (c *Client) do(httpReq *http.Request) (*psp.Response, error) {
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, c.classifyTransportError(err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		// The request was sent and may well have been processed; a failure to
		// read the reply says nothing about what the provider did.
		return nil, psp.NewError(c.cfg.Name, psp.ClassNetworkError, "", "read response body", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, c.classifyHTTPError(resp.StatusCode, raw)
	}

	var body chargeResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, psp.NewError(c.cfg.Name, psp.ClassUnknown, "", "decode response", err)
	}

	status := mapStatus(body.Status)

	// A provider that answers 200 with a failure status has declined. That is a
	// decision, not an error, so it must land in a terminal class and never be
	// retried.
	if status == psp.StatusFailed {
		return nil, psp.NewError(c.cfg.Name, psp.ClassDeclined, body.RawCode, "provider declined the charge", nil)
	}

	amount := money.Money{}
	if body.Currency != "" {
		amount, err = money.New(body.AmountMinor, money.Currency(body.Currency))
		if err != nil {
			return nil, psp.NewError(c.cfg.Name, psp.ClassUnknown, "", "provider returned an unusable amount", err)
		}
	}

	return &psp.Response{
		Status:            status,
		ProviderReference: body.Reference,
		Amount:            amount,
		RedirectURL:       body.RedirectURL,
		RawStatus:         body.RawStatus,
		RawCode:           body.RawCode,
	}, nil
}

// classifyTransportError decides what a failure to complete the exchange means.
//
// Everything here is ambiguous by construction: the request may have reached
// the provider and been processed, and the reply lost. None of these classes
// permit a retry without first confirming the outcome.
func (c *Client) classifyTransportError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return psp.NewError(c.cfg.Name, psp.ClassTimeout, "", "provider did not respond in time", err)
	}
	if errors.Is(err, context.Canceled) {
		return psp.NewError(c.cfg.Name, psp.ClassTimeout, "", "request cancelled before a reply arrived", err)
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return psp.NewError(c.cfg.Name, psp.ClassTimeout, "", "provider did not respond in time", err)
	}

	return psp.NewError(c.cfg.Name, psp.ClassNetworkError, "", "transport failure", err)
}

// classifyHTTPError maps status codes onto the normalized taxonomy.
//
// The important line is the 5xx case. A 500 is *ambiguous*, not a failure: the
// provider may have recorded the charge and then fallen over while replying.
// Treating it as a clean failure and retrying is the classic double-charge bug,
// so it is classed as Unknown, which forces the caller through GetStatus.
func (c *Client) classifyHTTPError(status int, raw []byte) error {
	var body errorResponse
	_ = json.Unmarshal(raw, &body)

	switch {
	case status == http.StatusTooManyRequests:
		return psp.NewError(c.cfg.Name, psp.ClassRateLimited, body.Code, body.Message, nil)

	case status == http.StatusServiceUnavailable:
		// The provider explicitly refused to service the request, so nothing
		// happened and a later retry is safe.
		return psp.NewError(c.cfg.Name, psp.ClassUnavailable, body.Code, body.Message, nil)

	case status >= 500:
		return psp.NewError(c.cfg.Name, psp.ClassUnknown, body.Code,
			fmt.Sprintf("provider returned %d; outcome unknown", status), nil)

	case status == http.StatusUnprocessableEntity, status == http.StatusPaymentRequired:
		return psp.NewError(c.cfg.Name, mapDeclineCode(body.Code), body.Code, body.Message, nil)

	default:
		// Other 4xx are our fault — a malformed request — and repeating it
		// unchanged will fail identically.
		return psp.NewError(c.cfg.Name, psp.ClassInvalidInstrument, body.Code,
			fmt.Sprintf("provider rejected the request with %d: %s", status, body.Message), nil)
	}
}
