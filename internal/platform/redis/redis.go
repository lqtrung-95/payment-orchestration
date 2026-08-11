// Package redis owns the Redis client used for caching and fast-path lookups.
//
// Redis is treated throughout this service as an optimization, never a source
// of truth. Every value cached here must be re-derivable from Postgres, and
// every correctness guarantee (idempotency, webhook deduplication) is enforced
// by a database constraint with Redis only short-circuiting the common case.
package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
)

type Client struct {
	rdb *goredis.Client
}

// New builds and verifies a Redis client, returning only after a successful
// PING so that a healthy return value means Redis is genuinely reachable.
func New(ctx context.Context, cfg config.Redis) (*Client, error) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolSize:     cfg.PoolSize,
	})

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &Client{rdb: rdb}, nil
}

// Raw exposes the underlying client for callers that need commands beyond the
// helpers on this type.
func (c *Client) Raw() *goredis.Client { return c.rdb }

func (c *Client) Close() error { return c.rdb.Close() }

func (c *Client) Ping(ctx context.Context) error {
	if err := c.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	return nil
}
