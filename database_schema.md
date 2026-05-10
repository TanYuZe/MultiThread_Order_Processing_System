# Database Schema — High-Scale Order & Payment Processing Simulation Platform

All tables reside in a single PostgreSQL database `platform_db`. Each service owns its tables — no cross-service joins. Services query only their own tables; cross-domain data access is done through APIs or Kafka events.

---

## 1. Schema DDL (Flyway-managed)

### Migration: V1__create_orders.sql

```sql
CREATE EXTENSION IF NOT EXISTS "pgcrypto";  -- for gen_random_uuid()

-- ─────────────────────────────────────────────────────────────────────────────
-- orders
-- Aggregate root for the Order bounded context.
-- Uses optimistic locking via the `version` column.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE orders (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          UUID         NOT NULL,
    status           VARCHAR(20)  NOT NULL,         -- PENDING | CONFIRMED | CANCELLED | FAILED
    total_amount     NUMERIC(12,2) NOT NULL CHECK (total_amount > 0),
    currency         CHAR(3)      NOT NULL DEFAULT 'USD',
    idempotency_key  VARCHAR(64)  NOT NULL,
    version          BIGINT       NOT NULL DEFAULT 0,   -- JPA @Version field
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_orders_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT chk_orders_status CHECK (status IN ('PENDING','CONFIRMED','CANCELLED','FAILED'))
);

-- Lookup orders for a user (user profile page, order history)
CREATE INDEX idx_orders_user_id
    ON orders (user_id);

-- Admin queries by status + time range (e.g., "show all PENDING orders today")
CREATE INDEX idx_orders_status_created_at
    ON orders (status, created_at DESC);

-- Trigger to auto-update updated_at on every row modification
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
```

### Migration: V2__create_order_items.sql

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- order_items
-- Line items belonging to an order.
-- Append-only once the order is created — no updates to items.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE order_items (
    id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID          NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id  UUID          NOT NULL,
    quantity    INT           NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(12,2) NOT NULL CHECK (unit_price > 0)
);

-- Most common query: "give me all items for this order"
CREATE INDEX idx_order_items_order_id
    ON order_items (order_id);
```

### Migration: V3__create_inventory.sql

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- inventory
-- Stock management. This table is the focal point of all concurrency demos.
--
-- LOCKING STRATEGIES demonstrated:
--   1. No lock        → oversell (v1_broken)
--   2. @Version       → optimistic locking (v2_optimistic)
--   3. SELECT FOR UPDATE → pessimistic locking (v3_pessimistic)
--   4. Redis DECRBY   → atomic at cache layer (v4_redis)
--
-- The check constraint prevents reserved_quantity from exceeding total_quantity,
-- providing a DB-level safety net as a last resort.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE inventory (
    product_id         UUID         PRIMARY KEY,
    product_name       VARCHAR(255) NOT NULL,
    total_quantity     INT          NOT NULL CHECK (total_quantity >= 0),
    reserved_quantity  INT          NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0),
    version            BIGINT       NOT NULL DEFAULT 0,   -- JPA @Version
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_inventory_available
        CHECK (total_quantity >= reserved_quantity)
);

-- Covering index for the reservation query pattern:
--   SELECT * FROM inventory WHERE product_id = ? AND (total_quantity - reserved_quantity) >= ?
-- The partial index filters to rows with available stock — most reads hit this index.
CREATE INDEX idx_inventory_available
    ON inventory (product_id)
    WHERE (total_quantity - reserved_quantity) > 0;

CREATE TRIGGER trg_inventory_updated_at
    BEFORE UPDATE ON inventory
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
```

### Migration: V4__create_payments.sql

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- payments
-- Records every payment attempt. The idempotency_key UNIQUE constraint is
-- the last line of defense against duplicate charges when client retries.
--
-- IDEMPOTENCY FLOW:
--   1. Client generates idempotency_key = UUID (stored client-side for retries)
--   2. First request: INSERT → succeeds → payment processed
--   3. Retry request: INSERT fails with UniqueViolation → return existing record
--   4. Redis also caches the result to avoid the DB roundtrip on hot retries
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE payments (
    id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id         UUID          NOT NULL REFERENCES orders(id),
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

-- Lookup all payments for an order
CREATE INDEX idx_payments_order_id
    ON payments (order_id);

-- Scheduled job selects PENDING payments older than 10 minutes for retry
-- Partial index is smaller and faster — only indexes the rows we care about
CREATE INDEX idx_payments_pending_retry
    ON payments (last_attempt_at)
    WHERE status = 'PENDING';
```

### Migration: V5__create_notifications.sql

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- notifications
-- Outbound notification records. The notification service polls this table
-- and updates status. After max_attempts, status transitions to 'DEAD'
-- and the record is published to the DLQ topic.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE notifications (
    id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        UUID         NOT NULL,
    channel        VARCHAR(20)  NOT NULL,       -- EMAIL | SMS | PUSH
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

-- The primary query: "give me all PENDING notifications due for processing"
-- Partial index on status='PENDING' keeps the index small as sent notifications pile up
CREATE INDEX idx_notifications_pending_scheduled
    ON notifications (scheduled_at ASC)
    WHERE status = 'PENDING';

-- Per-user notification history
CREATE INDEX idx_notifications_user_id_created
    ON notifications (user_id, created_at DESC);
```

### Migration: V6__create_outbox.sql

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- outbox_events
-- Implements the Transactional Outbox Pattern.
--
-- PROBLEM SOLVED:
--   Without the outbox, services do:
--     BEGIN; save order; COMMIT; publish to Kafka  ← DUAL WRITE
--   If the app crashes between COMMIT and kafka publish, the event is lost.
--   The consumer never receives orders.created, payment never happens.
--
-- SOLUTION:
--   BEGIN; save order; INSERT INTO outbox_events; COMMIT  ← single transaction
--   A separate poller reads unpublished rows and publishes to Kafka.
--   On publish success: UPDATE outbox_events SET published=true.
--   On crash: poller restarts, republishes (Kafka consumer must be idempotent).
--
-- TRADEOFF:
--   Extra DB roundtrip per event. Delivery latency adds ~1s (poller interval).
--   This is acceptable for eventual consistency. For synchronous flows, use
--   Kafka transactions instead.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE outbox_events (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(50)  NOT NULL,   -- ORDER | PAYMENT | INVENTORY
    aggregate_id    UUID         NOT NULL,
    event_type      VARCHAR(100) NOT NULL,   -- e.g., ORDER_CREATED, PAYMENT_AUTHORIZED
    payload         JSONB        NOT NULL,
    published       BOOLEAN      NOT NULL DEFAULT FALSE,
    publish_attempts INT         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- The poller query: SELECT * FROM outbox_events WHERE published=FALSE ORDER BY created_at
-- Partial index keeps it fast — only unpublished rows are indexed
CREATE INDEX idx_outbox_unpublished
    ON outbox_events (created_at ASC)
    WHERE published = FALSE;

-- For debugging: "what events have been published for order X?"
CREATE INDEX idx_outbox_aggregate
    ON outbox_events (aggregate_type, aggregate_id);
```

---

## 2. Entity Relationship Diagram

```
  orders (1) ──────────────── (N) order_items
     │                              │
     │                              │ product_id (FK to inventory.product_id)
     │                              ▼
     │                          inventory
     │
     └──────────────── (1) payments
                            (one payment per order attempt;
                             re-auth creates new payment record)

  outbox_events  (no FK — decoupled by design)
  notifications  (no FK to orders — notification service owns this)
```

---

## 3. Locking Strategy Deep Dive

### 3.1 Optimistic Locking (@Version)

Used for: Order status transitions, inventory updates under normal load.

**How it works in JPA:**
```java
@Entity
public class OrderJpaEntity {
    @Id
    private UUID id;

    @Version                // Hibernate manages this field
    private Long version;   // reads version=5, updates WHERE version=5, increments to 6

    private String status;
}
```

**SQL generated by Hibernate:**
```sql
-- READ:
SELECT id, status, version FROM orders WHERE id = ?
-- Returns: id=abc, status=PENDING, version=5

-- UPDATE (includes version in WHERE clause):
UPDATE orders
   SET status = 'CONFIRMED', version = 6, updated_at = now()
 WHERE id = 'abc' AND version = 5;
-- If 0 rows updated → ObjectOptimisticLockingFailureException → retry
```

**When it fails (race condition):**
```
T1: SELECT version=5, status=PENDING
T2: SELECT version=5, status=PENDING
T1: UPDATE WHERE version=5 → succeeds → version becomes 6
T2: UPDATE WHERE version=5 → 0 rows → throws OptimisticLockException
T2: Retry → SELECT version=6, status=CONFIRMED → cannot transition
```

**Retry pattern:**
```java
@Retryable(
    retryFor = ObjectOptimisticLockingFailureException.class,
    maxAttempts = 3,
    backoff = @Backoff(delay = 50, multiplier = 2)
)
public void confirmOrder(UUID orderId) {
    var order = orderRepository.findById(orderId).orElseThrow();
    order.confirm();  // throws if wrong state
    orderRepository.save(order);
}
```

### 3.2 Pessimistic Locking (SELECT FOR UPDATE)

Used for: Flash sale inventory reservation where optimistic retries are too expensive.

**JPA Query:**
```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT i FROM InventoryJpaEntity i WHERE i.productId = :productId")
Optional<InventoryJpaEntity> findByProductIdForUpdate(@Param("productId") UUID productId);
```

**SQL generated:**
```sql
SELECT product_id, total_quantity, reserved_quantity, version
  FROM inventory
 WHERE product_id = ?
   FOR UPDATE;    -- row-level write lock, other writers block here
```

**SKIP LOCKED variant (Flash sale without thundering herd):**
```sql
SELECT product_id, total_quantity, reserved_quantity
  FROM inventory
 WHERE product_id = ?
   FOR UPDATE SKIP LOCKED;  -- skip rows already locked → process other products
```

**When to use SKIP LOCKED:**
In a flash sale, many orders queue up. Without SKIP LOCKED, all waiting threads form a queue behind the lock. With SKIP LOCKED, a thread that cannot acquire the lock immediately moves on — useful when processing a batch of different products concurrently.

### 3.3 Advisory Locks (Application-Level Coordination)

Used for: Ensuring only one Outbox Poller runs per service instance in a multi-instance deployment.

```sql
-- Acquire advisory lock before polling (non-blocking trylock variant)
SELECT pg_try_advisory_lock(12345);  -- returns true if acquired, false if taken

-- Release on shutdown
SELECT pg_advisory_unlock(12345);
```

### 3.4 Transaction Isolation Levels

```
READ COMMITTED (default for all services):
  ─ Each statement sees data committed before the statement began
  ─ Sufficient for optimistic locking (version check is within the same transaction)
  ─ Flash sale inventory: use REPEATABLE READ if you need consistent reads within a transaction

REPEATABLE READ (inventory reservation — pessimistic path):
  ─ All reads within the transaction see a consistent snapshot
  ─ Prevents phantom reads for multi-item orders
  ─ JPA: @Transactional(isolation = Isolation.REPEATABLE_READ)
```

---

## 4. Index Strategy

| Table | Index | Type | Purpose |
|---|---|---|---|
| orders | `pk_orders_id` | B-tree | PK lookup |
| orders | `uq_orders_idempotency_key` | B-tree UNIQUE | Idempotency enforcement |
| orders | `idx_orders_user_id` | B-tree | User order history |
| orders | `idx_orders_status_created_at` | B-tree composite | Admin status queries |
| order_items | `idx_order_items_order_id` | B-tree | Fetch items by order |
| inventory | `idx_inventory_available` | Partial B-tree | Reservation queries (available stock only) |
| payments | `uq_payments_idempotency_key` | B-tree UNIQUE | Prevent duplicate charge |
| payments | `idx_payments_order_id` | B-tree | Payment lookup by order |
| payments | `idx_payments_pending_retry` | Partial B-tree | Retry scheduler query |
| notifications | `idx_notifications_pending_scheduled` | Partial B-tree | Pending notification poller |
| notifications | `idx_notifications_user_id_created` | B-tree composite | User notification history |
| outbox_events | `idx_outbox_unpublished` | Partial B-tree | Outbox poller query |
| outbox_events | `idx_outbox_aggregate` | B-tree composite | Debug: events per aggregate |

### Index Pitfalls Demonstrated

**Over-indexing (Task 6.4):**
Adding an index on every column slows down writes — every INSERT/UPDATE must also update all indexes. The project has a deliberate "over-indexed" branch that shows the impact on write throughput under load.

**Missing index on hot path (Task 6.4):**
The payment retry scheduler queries `payments WHERE status='PENDING'`. Without the partial index, this is a full table scan when payments grow to millions of rows. The load test demonstrates this degradation.

---

## 5. Query Patterns for Each Concurrency Scenario

### Oversell Race Condition (Task 3.2)

**Broken (no lock):**
```sql
-- T1 and T2 both execute simultaneously:
SELECT total_quantity, reserved_quantity FROM inventory WHERE product_id = 'X';
-- Both see: total=1, reserved=0 → available=1

-- Both proceed to reserve:
UPDATE inventory SET reserved_quantity = 1 WHERE product_id = 'X';
-- T1 succeeds, T2 also succeeds → reserved_quantity ends at 1
-- But TWO orders were confirmed! Oversell!
```

**Fixed (pessimistic):**
```sql
-- T1 acquires row lock:
SELECT * FROM inventory WHERE product_id = 'X' FOR UPDATE;
-- T2 blocks here until T1 releases the lock

-- T1: total=1, reserved=0 → reserves
UPDATE inventory SET reserved_quantity = 1 WHERE product_id = 'X';
COMMIT;  -- T2 now unblocks

-- T2: total=1, reserved=1 → available=0 → cannot reserve → order rejected
```

### Deadlock Demo (Task 3.3)

```sql
-- Thread T1: processing Order #1 (buys Product A then Product B)
BEGIN;
SELECT * FROM inventory WHERE product_id = 'A' FOR UPDATE;  -- acquires lock on A
-- ... T2 starts here ...
SELECT * FROM inventory WHERE product_id = 'B' FOR UPDATE;  -- BLOCKS (T2 holds B)

-- Thread T2: processing Order #2 (buys Product B then Product A)
BEGIN;
SELECT * FROM inventory WHERE product_id = 'B' FOR UPDATE;  -- acquires lock on B
SELECT * FROM inventory WHERE product_id = 'A' FOR UPDATE;  -- BLOCKS (T1 holds A)

-- PostgreSQL deadlock detector wakes up after ~1s:
-- Kills T2 with: ERROR: deadlock detected
-- T1 completes, T2 must retry
```

**Fix — consistent lock ordering:**
```sql
-- Both T1 and T2 must lock products in UUID sort order:
SELECT * FROM inventory
 WHERE product_id IN ('A', 'B')
 ORDER BY product_id        -- deterministic order eliminates deadlock
   FOR UPDATE;
```

---

## 6. Outbox Pattern — Event Schema (JSONB payload)

```json
{
  "eventId": "evt-uuid-123",
  "occurredAt": "2026-05-10T14:00:00Z",
  "data": {
    "orderId": "ord-uuid-456",
    "userId": "usr-uuid-789",
    "totalAmount": 99.99,
    "currency": "USD",
    "items": [
      { "productId": "prd-uuid-001", "quantity": 2, "unitPrice": 49.99 }
    ]
  }
}
```

Event types per aggregate:

| aggregate_type | event_type | Kafka Topic | Consumer |
|---|---|---|---|
| ORDER | ORDER_CREATED | orders.created | payment-svc, inventory-svc |
| ORDER | ORDER_CANCELLED | orders.cancelled | inventory-svc, notif-svc |
| PAYMENT | PAYMENT_AUTHORIZED | payments.authorized | order-svc, notif-svc |
| PAYMENT | PAYMENT_FAILED | payments.failed | order-svc, notif-svc |
| INVENTORY | STOCK_RESERVED | inventory.reserved | (monitoring only) |
| INVENTORY | STOCK_RELEASED | inventory.released | (monitoring only) |
