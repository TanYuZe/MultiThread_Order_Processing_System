-- ─────────────────────────────────────────────────────────────────────────────
-- V1: Notifications schema
--
-- KEY LEARNING FOCUS:
--   Partial index on status='PENDING' keeps the poller query fast.
--   As notifications are sent/killed, they exit the index — no bloat.
--   The outbox_events table here serves notification-originated events.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE notifications (
    id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        UUID         NOT NULL,
    channel        VARCHAR(20)  NOT NULL,
    subject        VARCHAR(255),
    body           TEXT         NOT NULL,
    status         VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    attempt_count  INT          NOT NULL DEFAULT 0,
    max_attempts   INT          NOT NULL DEFAULT 3,
    last_error     TEXT,
    scheduled_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    sent_at        TIMESTAMPTZ,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_notifications_channel
        CHECK (channel IN ('EMAIL','SMS','PUSH')),
    CONSTRAINT chk_notifications_status
        CHECK (status IN ('PENDING','SENT','FAILED','DEAD'))
);

-- Primary poller query: find PENDING notifications ready for delivery
CREATE INDEX idx_notifications_pending_scheduled
    ON notifications (scheduled_at ASC)
    WHERE status = 'PENDING';

-- User notification history
CREATE INDEX idx_notifications_user_id_created
    ON notifications (user_id, created_at DESC);
