package config

import (
	"strings"
	"testing"
	"time"
)

// setMinimalEnv provides only the values that have no default, so each test can
// layer the specific override it cares about on top.
func setMinimalEnv(t *testing.T) {
	t.Helper()
	t.Setenv("POSTGRES_DSN", "postgres://user:pass@localhost:5432/db?sslmode=disable")
}

func TestLoadAppliesDefaults(t *testing.T) {
	setMinimalEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.Env != "development" {
		t.Errorf("Env = %q, want development", cfg.Env)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP.Addr = %q, want :8080", cfg.HTTP.Addr)
	}
	if cfg.HTTP.RequestTimeout != 30*time.Second {
		t.Errorf("HTTP.RequestTimeout = %v, want 30s", cfg.HTTP.RequestTimeout)
	}
	if got, want := cfg.Kafka.Brokers, []string{"localhost:9092"}; len(got) != 1 || got[0] != want[0] {
		t.Errorf("Kafka.Brokers = %v, want %v", got, want)
	}
}

func TestLoadRequiresDSN(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() succeeded without POSTGRES_DSN, want error")
	}
	if !strings.Contains(err.Error(), "POSTGRES_DSN") {
		t.Errorf("error %q does not name the missing key", err)
	}
}

// Every misconfigured value should surface in one error. Reporting only the
// first turns configuration debugging into a fix-restart-repeat loop.
func TestLoadReportsAllErrorsAtOnce(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")
	t.Setenv("HTTP_READ_TIMEOUT", "not-a-duration")
	t.Setenv("REDIS_POOL_SIZE", "not-an-int")
	t.Setenv("LOG_LEVEL", "verbose")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() succeeded with four invalid values, want error")
	}

	for _, key := range []string{"POSTGRES_DSN", "HTTP_READ_TIMEOUT", "REDIS_POOL_SIZE", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s:\n%v", key, err)
		}
	}
}

func TestLoadParsesOverrides(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("HTTP_ADDR", ":9999")
	t.Setenv("HTTP_REQUEST_TIMEOUT", "2s")
	t.Setenv("POSTGRES_MAX_CONNS", "50")
	t.Setenv("KAFKA_BROKERS", "a:9092, b:9092 ,c:9092")
	t.Setenv("LOG_ADD_SOURCE", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if !cfg.IsProduction() {
		t.Error("IsProduction() = false, want true")
	}
	if cfg.HTTP.Addr != ":9999" {
		t.Errorf("HTTP.Addr = %q, want :9999", cfg.HTTP.Addr)
	}
	if cfg.HTTP.RequestTimeout != 2*time.Second {
		t.Errorf("HTTP.RequestTimeout = %v, want 2s", cfg.HTTP.RequestTimeout)
	}
	if cfg.Postgres.MaxConns != 50 {
		t.Errorf("Postgres.MaxConns = %d, want 50", cfg.Postgres.MaxConns)
	}
	if !cfg.Log.AddSource {
		t.Error("Log.AddSource = false, want true")
	}

	// Whitespace around comma-separated entries is trimmed rather than
	// producing broker addresses that fail to dial with a confusing error.
	want := []string{"a:9092", "b:9092", "c:9092"}
	if len(cfg.Kafka.Brokers) != len(want) {
		t.Fatalf("Kafka.Brokers = %v, want %v", cfg.Kafka.Brokers, want)
	}
	for i := range want {
		if cfg.Kafka.Brokers[i] != want[i] {
			t.Errorf("Kafka.Brokers[%d] = %q, want %q", i, cfg.Kafka.Brokers[i], want[i])
		}
	}
}

func TestLoadRejectsUnknownEnum(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("APP_ENV", "prod")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() accepted APP_ENV=prod, want error")
	}
	if !strings.Contains(err.Error(), "APP_ENV") {
		t.Errorf("error %q does not name APP_ENV", err)
	}
}
