// Package config loads and validates service configuration from the environment.
//
// Configuration is env-first and fail-fast: a malformed or missing value aborts
// boot with every problem listed at once. A payment service that starts with a
// half-valid config and discovers the gap under load is far worse than one that
// refuses to start.
package config

import "time"

type Config struct {
	Env      string
	HTTP     HTTP
	Postgres Postgres
	Redis    Redis
	Kafka    Kafka
	Log      Log
	PSP      PSP
}

type PSP struct {
	// SimulatorURL is the fault-injecting provider simulator. It runs as its own
	// process so it can be killed mid-flow.
	SimulatorURL string

	// DefaultProvider names the adapter used when a transaction does not pick
	// one. Routing rules arrive in a later phase.
	DefaultProvider string

	// Timeout bounds a single provider call. It is deliberately short relative
	// to the HTTP request timeout: a hung provider must surface as a timeout
	// this service classifies and recovers from, not as the caller giving up
	// first and leaving the outcome unexamined.
	Timeout time.Duration
}

type HTTP struct {
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
	// RequestTimeout bounds a single handler. Payment endpoints call external
	// PSPs, so an unbounded handler leaks goroutines and connection slots when
	// a provider hangs.
	RequestTimeout time.Duration
}

type Postgres struct {
	DSN string
	// MaxConns caps the pool. Sized against Postgres max_connections divided by
	// the number of service instances; oversizing turns a traffic spike into
	// connection exhaustion at the database rather than backpressure here.
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	ConnectTimeout    time.Duration
	StatementTimeout  time.Duration
	HealthCheckPeriod time.Duration
}

type Redis struct {
	Addr         string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
}

type Kafka struct {
	Brokers []string
	// ClientID identifies this service in broker-side metrics and logs.
	ClientID    string
	DialTimeout time.Duration
}

type Log struct {
	Level  string
	Format string
	// AddSource attaches file:line to every record. Useful locally, noisy and
	// measurably expensive in production.
	AddSource bool
}

// Load reads configuration from the environment, applying defaults suited to
// local development. Every value is overridable so the same binary runs in CI
// and in a deployed environment without a rebuild.
func Load() (*Config, error) {
	l := &loader{}

	cfg := &Config{
		Env: l.oneOf("APP_ENV", "development", "development", "test", "staging", "production"),

		HTTP: HTTP{
			Addr:            l.str("HTTP_ADDR", ":8080"),
			ReadTimeout:     l.duration("HTTP_READ_TIMEOUT", 10*time.Second),
			WriteTimeout:    l.duration("HTTP_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:     l.duration("HTTP_IDLE_TIMEOUT", 60*time.Second),
			ShutdownTimeout: l.duration("HTTP_SHUTDOWN_TIMEOUT", 20*time.Second),
			RequestTimeout:  l.duration("HTTP_REQUEST_TIMEOUT", 30*time.Second),
		},

		Postgres: Postgres{
			DSN:               l.required("POSTGRES_DSN"),
			MaxConns:          int32(l.int("POSTGRES_MAX_CONNS", 20)),
			MinConns:          int32(l.int("POSTGRES_MIN_CONNS", 2)),
			MaxConnLifetime:   l.duration("POSTGRES_MAX_CONN_LIFETIME", time.Hour),
			MaxConnIdleTime:   l.duration("POSTGRES_MAX_CONN_IDLE_TIME", 30*time.Minute),
			ConnectTimeout:    l.duration("POSTGRES_CONNECT_TIMEOUT", 5*time.Second),
			StatementTimeout:  l.duration("POSTGRES_STATEMENT_TIMEOUT", 10*time.Second),
			HealthCheckPeriod: l.duration("POSTGRES_HEALTH_CHECK_PERIOD", 30*time.Second),
		},

		Redis: Redis{
			Addr:         l.str("REDIS_ADDR", "localhost:6379"),
			Password:     l.str("REDIS_PASSWORD", ""),
			DB:           l.int("REDIS_DB", 0),
			DialTimeout:  l.duration("REDIS_DIAL_TIMEOUT", 5*time.Second),
			ReadTimeout:  l.duration("REDIS_READ_TIMEOUT", 3*time.Second),
			WriteTimeout: l.duration("REDIS_WRITE_TIMEOUT", 3*time.Second),
			PoolSize:     l.int("REDIS_POOL_SIZE", 20),
		},

		Kafka: Kafka{
			Brokers:     l.csv("KAFKA_BROKERS", []string{"localhost:9092"}),
			ClientID:    l.str("KAFKA_CLIENT_ID", "payment-orchestrator"),
			DialTimeout: l.duration("KAFKA_DIAL_TIMEOUT", 10*time.Second),
		},

		Log: Log{
			Level:     l.oneOf("LOG_LEVEL", "info", "debug", "info", "warn", "error"),
			Format:    l.oneOf("LOG_FORMAT", "json", "json", "text"),
			AddSource: l.bool("LOG_ADD_SOURCE", false),
		},

		PSP: PSP{
			SimulatorURL:    l.str("PSP_SIMULATOR_URL", "http://localhost:9090"),
			DefaultProvider: l.str("PSP_DEFAULT_PROVIDER", "psp-sync-sim"),
			Timeout:         l.duration("PSP_TIMEOUT", 5*time.Second),
		},
	}

	if err := l.err(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// IsProduction reports whether the service is running with production
// semantics. Used to gate development-only affordances such as the fault
// injection admin API introduced in a later phase.
func (c *Config) IsProduction() bool { return c.Env == "production" }
