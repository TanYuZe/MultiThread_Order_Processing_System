-- ─────────────────────────────────────────────────────────────────────────────
-- V1: Inventory schema
--
-- KEY LEARNING FOCUS:
--   The `version` column enables JPA @Version optimistic locking.
--   The CHECK constraint is the DB-level safety net against negative stock.
--   Flyway manages this — never ALTER TABLE manually.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE inventory (
    product_id         UUID          PRIMARY KEY,
    product_name       VARCHAR(255)  NOT NULL,
    total_quantity     INT           NOT NULL CHECK (total_quantity >= 0),
    reserved_quantity  INT           NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0),
    version            BIGINT        NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT chk_inventory_no_oversell
        CHECK (total_quantity >= reserved_quantity)
);

-- Partial index: only index rows that have available stock.
-- Makes reservation queries fast even with millions of products.
CREATE INDEX idx_inventory_available
    ON inventory (product_id)
    WHERE (total_quantity - reserved_quantity) > 0;

-- Seed some test products (can be deleted for production):
INSERT INTO inventory (product_id, product_name, total_quantity)
VALUES
    ('00000000-0000-0000-0000-000000000001', 'Test Product A', 1000),
    ('00000000-0000-0000-0000-000000000002', 'Flash Sale Item', 10),
    ('00000000-0000-0000-0000-000000000003', 'Test Product C', 500);
