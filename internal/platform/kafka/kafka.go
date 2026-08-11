// Package kafka owns the Kafka client used for the asynchronous payment
// backbone.
//
// This phase establishes connectivity only. Producers, consumer groups, the
// transactional outbox relay, and the tiered retry ladder arrive in a later
// phase; keeping the client construction isolated here means those additions
// do not need to revisit connection or health-check concerns.
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
)

type Client struct {
	cl *kgo.Client
}

// New builds and verifies a Kafka client. Ping performs a metadata round trip
// against the seed brokers, so a healthy return value means the cluster is
// genuinely reachable rather than merely resolvable.
func New(ctx context.Context, cfg config.Kafka) (*Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DialTimeout(cfg.DialTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("ping kafka: %w", err)
	}

	return &Client{cl: cl}, nil
}

// Raw exposes the underlying client for produce and consume paths.
func (c *Client) Raw() *kgo.Client { return c.cl }

func (c *Client) Close() { c.cl.Close() }

func (c *Client) Ping(ctx context.Context) error {
	if err := c.cl.Ping(ctx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}
	return nil
}
