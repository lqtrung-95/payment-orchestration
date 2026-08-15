package testsupport

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/migrations"
)

// NewRouter returns a Router over n genuinely separate physical databases.
//
// Separate databases rather than separate schemas, because the point being
// tested is what Postgres refuses to do: there is no transaction spanning two
// databases and no query that joins across them. A schema-per-shard setup would
// let a careless join succeed and the test would prove nothing about the thing
// sharding actually costs.
//
// Shard 0 is the ordinary test database. The rest are created on demand beside
// it and migrated to the current version, so this needs no compose or CI change
// — anywhere the other integration tests run, this one does too.
func NewRouter(t *testing.T, n int) *postgres.Router {
	t.Helper()

	base := testDSN()
	if base == "" {
		t.Skip("POSTGRES_DSN not set; skipping integration test")
	}

	dsns := make([]string, 0, n)
	dsns = append(dsns, base)
	for i := 1; i < n; i++ {
		dsn, name := shardDSN(t, base, i)
		createDatabase(t, base, name)
		migrateUp(t, dsn)
		dsns = append(dsns, dsn)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	router, err := postgres.NewRouter(ctx, testPoolConfig(base), dsns)
	if err != nil {
		t.Skipf("shard databases unreachable: %v", err)
	}
	t.Cleanup(router.Close)
	return router
}

// FreshRouter returns a Router whose every shard is empty, and empties them
// again on cleanup.
func FreshRouter(t *testing.T, n int) *postgres.Router {
	t.Helper()

	router := NewRouter(t, n)
	truncateAll(t, router)
	t.Cleanup(func() { truncateAll(t, router) })
	return router
}

func truncateAll(t *testing.T, router *postgres.Router) {
	t.Helper()
	for _, db := range router.Shards() {
		Truncate(t, db)
	}
}

// shardDSN derives shard i's DSN from the base one by suffixing the database
// name. Deriving rather than configuring keeps the test's databases obviously
// disposable: anything named `<base>_shardN` was created by a test run.
func shardDSN(t *testing.T, base string, i int) (dsn, dbName string) {
	t.Helper()

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse base dsn: %v", err)
	}
	dbName = fmt.Sprintf("%s_shard%d", strings.TrimPrefix(u.Path, "/"), i)
	u.Path = "/" + dbName
	return u.String(), dbName
}

func createDatabase(t *testing.T, adminDSN, name string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	defer pool.Close()

	// CREATE DATABASE cannot run inside a transaction and has no IF NOT EXISTS,
	// so a duplicate is tolerated rather than avoided. The name is derived from
	// the DSN this process was given, not from anything a test supplies, so
	// there is no injectable component to quote around.
	_, err = pool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name))
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create shard database %s: %v", name, err)
	}
}

func migrateUp(t *testing.T, dsn string) {
	t.Helper()

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		t.Fatalf("load embedded migrations: %v", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, dsn)
	if err != nil {
		t.Fatalf("open migrator: %v", err)
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate shard database: %v", err)
	}
}

func testDSN() string {
	if dsn := os.Getenv("POSTGRES_TEST_DSN"); dsn != "" {
		return dsn
	}
	return os.Getenv("POSTGRES_DSN")
}

func testPoolConfig(dsn string) config.Postgres {
	return config.Postgres{
		DSN:               dsn,
		MaxConns:          10,
		MinConns:          1,
		MaxConnLifetime:   time.Hour,
		MaxConnIdleTime:   30 * time.Minute,
		ConnectTimeout:    5 * time.Second,
		StatementTimeout:  60 * time.Second,
		HealthCheckPeriod: 30 * time.Second,
	}
}
