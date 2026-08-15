// Package testsupport provides shared helpers for tests that need real
// infrastructure.
//
// These tests run against a live Postgres rather than a mock. The behaviour
// under test — deferred constraint triggers, unique-constraint arbitration
// between concurrent writers, row locking — exists only in the database. A mock
// would assert that the test's own model of Postgres is self-consistent, which
// is not the property anyone cares about.
package testsupport

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// NewDB connects to the test database, skipping the test when none is
// configured or reachable. Skipping rather than failing keeps `go test ./...`
// usable on a machine with no stack running, while CI always provides one.
func NewDB(t *testing.T) *postgres.DB {
	t.Helper()

	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		dsn = os.Getenv("POSTGRES_DSN")
	}
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := postgres.New(ctx, config.Postgres{
		DSN:             dsn,
		MaxConns:        25,
		MinConns:        2,
		MaxConnLifetime: time.Hour,
		MaxConnIdleTime: 30 * time.Minute,
		ConnectTimeout:  5 * time.Second,
		// Well above the production setting. The suite's slowest statement is
		// the TRUNCATE between cases, which takes an ACCESS EXCLUSIVE lock on
		// every table and is dominated by filesystem work rather than by rows —
		// all of these tables are empty when it runs. On a loaded container
		// runtime that can stall for tens of seconds, and failing there teaches
		// people to rerun the suite instead of reading it.
		StatementTimeout:  60 * time.Second,
		HealthCheckPeriod: 30 * time.Second,
	})
	if err != nil {
		t.Skipf("postgres unreachable at %s: %v", dsn, err)
	}

	t.Cleanup(db.Close)
	return db
}

// Truncate empties every table.
//
// TRUNCATE rather than DELETE: the ledger and audit tables carry append-only
// triggers that reject DELETE by design, and TRUNCATE bypasses row-level
// triggers. It also resets identity sequences, which keeps failure output
// readable across runs.
//
// This is global, so two packages truncating the same database concurrently
// will destroy each other's fixtures. Go runs test packages in parallel by
// default, so integration tests must be invoked with -p 1; the test targets in
// the Makefile and CI do exactly that. The alternative — a database per package
// cloned from a template — is not worth its complexity at this size.
func Truncate(t *testing.T, db *postgres.DB) {
	t.Helper()

	const query = `
		TRUNCATE
			recon_breaks,
			recon_runs,
			settlement_rows,
			settlement_files,
			fx_rate_locks,
			webhook_events_raw,
			transaction_state_changes,
			postings,
			journal_entries,
			idempotency_keys,
			outbox,
			processed_events,
			payment_transactions,
			ledger_accounts
		RESTART IDENTITY CASCADE`

	// Generous, because this is fixture setup rather than behaviour under test.
	// TRUNCATE takes an ACCESS EXCLUSIVE lock on every table listed, and the
	// list grows with each phase; a tight deadline turns a loaded machine into
	// a failing test suite, which teaches people to rerun rather than to look.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := db.Pool().Exec(ctx, query); err != nil {
		t.Fatalf("truncate test tables: %v", err)
	}
}

// FreshDB returns a connected, empty database and empties it again on cleanup,
// so a failing test leaves nothing behind for the next one to trip over.
func FreshDB(t *testing.T) *postgres.DB {
	t.Helper()

	db := NewDB(t)
	Truncate(t, db)
	t.Cleanup(func() { Truncate(t, db) })
	return db
}
