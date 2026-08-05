CREATE TABLE outbox_event (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(150) NOT NULL,
    event_type VARCHAR(150) NOT NULL,
    event_version INTEGER NOT NULL DEFAULT 1,

    destination VARCHAR(200) NOT NULL,
    partition_key VARCHAR(200),

    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,

    status VARCHAR(30) NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,

    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,

    locked_at TIMESTAMPTZ,
    locked_by VARCHAR(200),
    published_at TIMESTAMPTZ,
    last_error TEXT
);

CREATE INDEX idx_outbox_event_polling
    ON outbox_event (status, available_at, created_at);

CREATE INDEX idx_outbox_event_recovery
    ON outbox_event (status, locked_at)
    WHERE status = 'PROCESSING';
