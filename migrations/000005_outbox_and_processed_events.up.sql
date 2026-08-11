-- Transactional outbox and consumer-side deduplication.
--
-- The outbox row is written in the same database transaction as the domain
-- change it describes. That atomicity is the whole point: publishing to a broker
-- inside the transaction would emit events for work that later rolls back, and
-- publishing after the commit loses events whenever the process dies in the gap.

CREATE TYPE outbox_status AS ENUM ('pending', 'published', 'failed');

CREATE TABLE outbox (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Stable identity that travels with the message into the broker and is what
    -- consumers deduplicate on. Distinct from the row id, which is local to this
    -- table and meaningless to anything downstream.
    event_id      UUID NOT NULL UNIQUE,

    aggregate_id  UUID NOT NULL,

    -- Becomes the Kafka partition key. Partitioning by merchant guarantees
    -- ordering per merchant, which is the only ordering this system needs;
    -- global ordering across all merchants is neither achievable nor useful.
    partition_key TEXT NOT NULL,

    topic         TEXT NOT NULL,
    payload       JSONB NOT NULL,

    status        outbox_status NOT NULL DEFAULT 'pending',
    attempts      INT NOT NULL DEFAULT 0,
    last_error    TEXT,

    -- Lets a failed publish back off rather than spinning. The claim query only
    -- picks up rows that are due.
    available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Drives the publisher's claim query: the oldest due, pending rows first.
CREATE INDEX outbox_claimable_idx ON outbox (available_at, id)
    WHERE status = 'pending';

CREATE INDEX outbox_aggregate_idx ON outbox (aggregate_id);

-- Consumer-side deduplication.
--
-- Kafka gives at-least-once delivery; exactly-once *delivery* does not exist.
-- Exactly-once *effect* does, and this table is where it comes from: the
-- primary key rejects a second attempt at the same event, inside the same
-- transaction as the work itself.
--
-- Keyed by consumer group as well as event, because two independent consumers
-- legitimately both process the same event and neither should block the other.
CREATE TABLE processed_events (
    event_id       UUID NOT NULL,
    consumer_group TEXT NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (event_id, consumer_group)
);

-- Supports pruning old records; the table would otherwise grow without bound.
CREATE INDEX processed_events_processed_at_idx ON processed_events (processed_at);
