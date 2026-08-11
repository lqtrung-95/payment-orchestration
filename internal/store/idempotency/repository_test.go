package idempotency_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/store/idempotency"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
)

const (
	merchant  = "m_idem"
	lockTTL   = 30 * time.Second
	recordTTL = 24 * time.Hour
)

func fingerprintOf(body string) []byte {
	return idempotency.Fingerprint("POST", "/v1/payments", []byte(body))
}

func claim(t *testing.T, db *postgres.DB, repo *idempotency.Repository, key string, fp []byte) idempotency.ClaimResult {
	t.Helper()

	var result idempotency.ClaimResult
	err := db.WithTx(context.Background(), func(ctx context.Context, tx pgx.Tx) error {
		var err error
		result, err = repo.Claim(ctx, tx, merchant, key, fp)
		return err
	})
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	return result
}

func complete(t *testing.T, db *postgres.DB, repo *idempotency.Repository, key string, token uuid.UUID, status int, body string) {
	t.Helper()

	err := completeWithToken(db, repo, key, token, status, body)
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
}

func completeWithToken(db *postgres.DB, repo *idempotency.Repository, key string, token uuid.UUID, status int, body string) error {
	return db.WithTx(context.Background(), func(ctx context.Context, tx pgx.Tx) error {
		return repo.Complete(ctx, tx, merchant, key, token, status, []byte(body), nil)
	})
}

func TestFirstClaimAcquires(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := idempotency.NewRepository(lockTTL, recordTTL)

	got := claim(t, db, repo, "key-1", fingerprintOf(`{"amount":100}`))
	if got.Outcome != idempotency.OutcomeAcquired {
		t.Errorf("outcome = %v, want Acquired", got.Outcome)
	}
}

// Race 1: an identical request is still running. The second caller must be told
// to wait, never allowed to start a second attempt — this is the case that
// produces double charges.
func TestConcurrentDuplicateIsToldToRetry(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := idempotency.NewRepository(lockTTL, recordTTL)
	fp := fingerprintOf(`{"amount":100}`)

	claim(t, db, repo, "key-inflight", fp)

	got := claim(t, db, repo, "key-inflight", fp)
	if got.Outcome != idempotency.OutcomeInFlight {
		t.Errorf("outcome = %v, want InFlight", got.Outcome)
	}
}

// Race 2: retry after the original succeeded. The stored response is replayed
// rather than the work being repeated.
func TestRetryAfterSuccessReplaysResponse(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := idempotency.NewRepository(lockTTL, recordTTL)
	fp := fingerprintOf(`{"amount":100}`)

	first := claim(t, db, repo, "key-done", fp)
	complete(t, db, repo, "key-done", first.Record.ClaimToken, 201, `{"id":"abc","state":"created"}`)

	got := claim(t, db, repo, "key-done", fp)
	if got.Outcome != idempotency.OutcomeReplay {
		t.Fatalf("outcome = %v, want Replay", got.Outcome)
	}
	if got.Record.ResponseStatus != 201 {
		t.Errorf("replayed status = %d, want 201", got.Record.ResponseStatus)
	}

	// Byte-for-byte, not merely equivalent JSON. Storing the body in a column
	// type that normalises — JSONB reorders keys and rewrites whitespace —
	// would pass an equivalence check while breaking any client that verifies a
	// signature over the response it received.
	const want = `{"id":"abc","state":"created"}`
	if string(got.Record.ResponseBody) != want {
		t.Errorf("replayed body = %s, want exactly %s", got.Record.ResponseBody, want)
	}
}

// Race 3: retry after the original failed definitively. The failure is replayed
// too. A caller that received a decline and retries the same key is entitled to
// that decline, not to a fresh attempt against the instrument.
func TestRetryAfterFailureReplaysFailure(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := idempotency.NewRepository(lockTTL, recordTTL)
	fp := fingerprintOf(`{"amount":100}`)

	first := claim(t, db, repo, "key-failed", fp)
	complete(t, db, repo, "key-failed", first.Record.ClaimToken, 422, `{"error":"card_declined"}`)

	got := claim(t, db, repo, "key-failed", fp)
	if got.Outcome != idempotency.OutcomeReplay {
		t.Fatalf("outcome = %v, want Replay", got.Outcome)
	}
	if got.Record.ResponseStatus != 422 {
		t.Errorf("replayed status = %d, want 422", got.Record.ResponseStatus)
	}
}

// Race 4: the same key with a different body. Replaying the first response
// would silently discard this payment, so it is refused outright.
func TestKeyReuseWithDifferentBodyIsRejected(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := idempotency.NewRepository(lockTTL, recordTTL)

	first := claim(t, db, repo, "key-reused", fingerprintOf(`{"amount":100}`))
	complete(t, db, repo, "key-reused", first.Record.ClaimToken, 201, `{"id":"abc"}`)

	got := claim(t, db, repo, "key-reused", fingerprintOf(`{"amount":999}`))
	if got.Outcome != idempotency.OutcomeFingerprintMismatch {
		t.Errorf("outcome = %v, want FingerprintMismatch", got.Outcome)
	}
}

// Reformatting an identical request must not look like a different one, or a
// client that re-serialises its JSON on retry would be refused.
func TestFingerprintIgnoresJSONFormatting(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := idempotency.NewRepository(lockTTL, recordTTL)

	first := claim(t, db, repo, "key-format", fingerprintOf(`{"amount":100,"currency":"USD"}`))
	complete(t, db, repo, "key-format", first.Record.ClaimToken, 201, `{"id":"abc"}`)

	// Same content: reordered keys and added whitespace.
	got := claim(t, db, repo, "key-format", fingerprintOf("{\n  \"currency\": \"USD\",\n  \"amount\": 100\n}"))
	if got.Outcome != idempotency.OutcomeReplay {
		t.Errorf("outcome = %v, want Replay — reformatting is not a different request", got.Outcome)
	}
}

// A claim whose owner died mid-request must not block that key forever, and the
// displaced owner must not be able to write its result afterwards. The second
// half is the fencing property: without it, a process that stalled past the
// lock TTL and then woke up would overwrite the outcome of the request that
// legitimately replaced it.
func TestStaleClaimIsTakenOverAndFencesOutOldOwner(t *testing.T) {
	db := testsupport.FreshDB(t)
	fp := fingerprintOf(`{"amount":100}`)

	// A lock TTL of zero makes any existing claim immediately stale, modelling
	// an owner that died without completing.
	repo := idempotency.NewRepository(0, recordTTL)

	original := claim(t, db, repo, "key-stale", fp)
	takeover := claim(t, db, repo, "key-stale", fp)

	if takeover.Outcome != idempotency.OutcomeAcquired {
		t.Fatalf("outcome = %v, want Acquired — a stale claim must be takeable", takeover.Outcome)
	}
	if takeover.Record.ClaimToken == original.Record.ClaimToken {
		t.Fatal("takeover reused the original claim token, so the old owner is not fenced out")
	}

	// The displaced owner is refused.
	err := completeWithToken(db, repo, "key-stale", original.Record.ClaimToken, 201, `{"id":"stale"}`)
	if !errors.Is(err, idempotency.ErrClaimLost) {
		t.Errorf("displaced owner Complete error = %v, want ErrClaimLost", err)
	}

	// The current owner succeeds, and its result is what a retry will see.
	if err := completeWithToken(db, repo, "key-stale", takeover.Record.ClaimToken, 201, `{"id":"current"}`); err != nil {
		t.Fatalf("current owner Complete returned error: %v", err)
	}

	replayed := claim(t, db, repo, "key-stale", fp)
	if replayed.Outcome != idempotency.OutcomeReplay {
		t.Fatalf("outcome = %v, want Replay", replayed.Outcome)
	}
	if !strings.Contains(string(replayed.Record.ResponseBody), "current") {
		t.Errorf("replayed body = %s, want the current owner's response", replayed.Record.ResponseBody)
	}
}

// The headline guarantee: many concurrent requests carrying one key produce
// exactly one execution. Ownership is decided by the unique constraint inside
// the transaction — a read-then-write would let two callers both conclude they
// may proceed.
func TestConcurrentClaimsElectExactlyOneWinner(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := idempotency.NewRepository(lockTTL, recordTTL)
	fp := fingerprintOf(`{"amount":4200,"currency":"USD"}`)

	const callers = 100
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		acquired int
		inFlight int
		other    int
	)

	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			var result idempotency.ClaimResult
			err := db.WithTx(context.Background(), func(ctx context.Context, tx pgx.Tx) error {
				var err error
				result, err = repo.Claim(ctx, tx, merchant, "key-race", fp)
				return err
			})

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				// A serialization failure is an acceptable loss: the caller
				// retries and finds the winner's claim. What matters is that it
				// did not acquire.
				other++
			case result.Outcome == idempotency.OutcomeAcquired:
				acquired++
			case result.Outcome == idempotency.OutcomeInFlight:
				inFlight++
			default:
				other++
			}
		}()
	}
	close(start)
	wg.Wait()

	if acquired != 1 {
		t.Fatalf("acquired = %d, want exactly 1 (in-flight %d, other %d)", acquired, inFlight, other)
	}
	if acquired+inFlight+other != callers {
		t.Errorf("accounted for %d of %d callers", acquired+inFlight+other, callers)
	}

	var rows int
	if err := db.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM idempotency_keys WHERE merchant_id = $1 AND key = 'key-race'`, merchant,
	).Scan(&rows); err != nil {
		t.Fatalf("count keys: %v", err)
	}
	if rows != 1 {
		t.Errorf("idempotency rows = %d, want 1", rows)
	}
}

// Keys are scoped per merchant, so one merchant's key can never collide with
// another's — nor can a merchant probe for whether a value is in use elsewhere.
func TestKeysAreScopedPerMerchant(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := idempotency.NewRepository(lockTTL, recordTTL)
	fp := fingerprintOf(`{"amount":100}`)
	ctx := context.Background()

	claim(t, db, repo, "shared-key", fp)

	var result idempotency.ClaimResult
	err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		result, err = repo.Claim(ctx, tx, "m_other", "shared-key", fp)
		return err
	})
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if result.Outcome != idempotency.OutcomeAcquired {
		t.Errorf("outcome = %v, want Acquired — another merchant's key must not block", result.Outcome)
	}
}
