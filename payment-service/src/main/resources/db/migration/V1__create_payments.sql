-- ─────────────────────────────────────────────────────────────────────────────
-- V1: Payments schema
--
-- KEY LEARNING FOCUS:
--   The UNIQUE constraint on idempotency_key is the ground-truth guard
--   against duplicate charges. It is atomic — cannot be raced.
--   Redis is a performance cache on top, not a replacement for this constraint.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE payments (
    id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id         UUID          NOT NULL,
    idempotency_key  VARCHAR(64)   NOT NULL,
    status           VARCHAR(20)   NOT NULL DEFAULT 'PENDING',
    amount           NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    currency         CHAR(3)       NOT NULL DEFAULT 'USD',
    attempt_count    INT           NOT NULL DEFAULT 0,
    last_attempt_at  TIMESTAMPTZ,
    authorized_at    TIMESTAMPTZ,
    failure_reason   TEXT,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_payments_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT chk_payments_status
        CHECK (status IN ('PENDING','AUTHORIZED','FAILED','REFUNDED'))
);

CREATE INDEX idx_payments_order_id ON payments (order_id);

-- Partial index: only PENDING payments are queried by the retry scheduler.
-- As payments are authorized/failed, they fall out of this index automatically.
CREATE INDEX idx_payments_pending_retry
    ON payments (last_attempt_at ASC)
    WHERE status = 'PENDING';
