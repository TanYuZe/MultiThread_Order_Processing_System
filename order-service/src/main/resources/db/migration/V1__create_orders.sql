-- ─────────────────────────────────────────────────────────────────────────────
-- V1: Orders schema
-- Managed by Flyway. DO NOT modify this file after it has been applied.
-- Create a new Vx__ migration for schema changes.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE orders (
    id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          UUID          NOT NULL,
    status           VARCHAR(20)   NOT NULL,
    total_amount     NUMERIC(12,2) NOT NULL CHECK (total_amount > 0),
    currency         CHAR(3)       NOT NULL DEFAULT 'USD',
    idempotency_key  VARCHAR(64)   NOT NULL,
    version          BIGINT        NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_orders_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT chk_orders_status CHECK (status IN ('PENDING','CONFIRMED','CANCELLED','FAILED'))
);

CREATE INDEX idx_orders_user_id          ON orders (user_id);
CREATE INDEX idx_orders_status_created   ON orders (status, created_at DESC);

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE order_items (
    id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID          NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id  UUID          NOT NULL,
    quantity    INT           NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(12,2) NOT NULL CHECK (unit_price > 0)
);

CREATE INDEX idx_order_items_order_id ON order_items (order_id);

CREATE TABLE outbox_events (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type   VARCHAR(50)  NOT NULL,
    aggregate_id     UUID         NOT NULL,
    event_type       VARCHAR(100) NOT NULL,
    payload          JSONB        NOT NULL,
    published        BOOLEAN      NOT NULL DEFAULT FALSE,
    publish_attempts INT          NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_unpublished ON outbox_events (created_at ASC) WHERE published = FALSE;
CREATE INDEX idx_outbox_aggregate   ON outbox_events (aggregate_type, aggregate_id);
