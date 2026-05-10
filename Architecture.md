# Architecture — High-Scale Order & Payment Processing Simulation Platform

---

## 1. High-Level System Architecture

```
                              ┌─────────────────────────────────────────────────────┐
                              │                  EXTERNAL CLIENTS                   │
                              │       (Gatling load tests / curl / Postman)         │
                              └───────────────────────┬─────────────────────────────┘
                                                      │  HTTP/REST
                                                      ▼
                              ┌─────────────────────────────────────────────────────┐
                              │                  API GATEWAY                        │
                              │         (Spring Cloud Gateway :8080)                │
                              │  ┌──────────────────────────────────────────────┐   │
                              │  │  Rate Limiter (Redis sliding window)         │   │
                              │  │  Correlation ID injection                    │   │
                              │  │  JWT validation (stub)                       │   │
                              │  └──────────────────────────────────────────────┘   │
                              └────┬────────────┬────────────┬─────────────┬────────┘
                                   │            │            │             │
                          /orders  │   /payments│  /inventory│    /notify  │
                                   ▼            ▼            ▼             ▼
                 ┌─────────────────┐  ┌──────────────┐  ┌───────────┐  ┌──────────────┐
                 │  ORDER SERVICE  │  │   PAYMENT    │  │ INVENTORY │  │ NOTIFICATION │
                 │  :8081          │  │   SERVICE    │  │  SERVICE  │  │   SERVICE    │
                 │                 │  │   :8082      │  │  :8083    │  │   :8084      │
                 └────────┬────────┘  └──────┬───────┘  └─────┬─────┘  └──────┬───────┘
                          │                  │                 │               │
                          └──────────────────┴─────────────────┴───────────────┘
                                                      │
                          ┌───────────────────────────┼───────────────────────────┐
                          │                           │                           │
                          ▼                           ▼                           ▼
               ┌─────────────────┐        ┌──────────────────┐        ┌──────────────────┐
               │   POSTGRESQL    │        │     APACHE       │        │      REDIS       │
               │   :5432         │        │     KAFKA        │        │      :6379       │
               │                 │        │     :9092        │        │                  │
               │  orders         │        │                  │        │  idempotency     │
               │  order_items    │        │  orders.*        │        │  inventory cache │
               │  payments       │        │  payments.*      │        │  rate limiter    │
               │  inventory      │        │  inventory.*     │        │  dist locks      │
               │  notifications  │        │  notifications.* │        │                  │
               │  outbox_events  │        │                  │        │                  │
               └─────────────────┘        └──────────────────┘        └──────────────────┘
                          │
                          ▼
               ┌─────────────────┐        ┌──────────────────┐        ┌──────────────────┐
               │ METRICS SERVICE │        │   PROMETHEUS     │        │     GRAFANA      │
               │  :8085          │◄───────│   :9090          │◄───────│     :3000        │
               │ /actuator/      │        │  scrapes every   │        │  dashboards:     │
               │  prometheus     │        │  15s             │        │  overview        │
               └─────────────────┘        └──────────────────┘        │  jvm             │
                                                                       │  kafka           │
                                                                       │  database        │
                                                                       └──────────────────┘
```

---

## 2. Request Flow: Order Creation (Happy Path)

This is the most important flow to understand — it touches every service and demonstrates the threading model.

```
Client                API Gateway        Order Service         Inventory Service
  │                       │                   │                      │
  │──── POST /orders ─────▶│                   │                      │
  │                       │                   │                      │
  │                [inject correlationId]      │                      │
  │                [check rate limit]          │                      │
  │                       │──── route ────────▶│                      │
  │                       │                   │                      │
  │                       │            [Tomcat/Virtual Thread]        │
  │                       │            [validate request]             │
  │                       │            [check idempotency key]        │
  │                       │            [begin DB transaction]         │
  │                       │            [save order PENDING]           │
  │                       │            [save outbox event]            │
  │                       │            [commit transaction]           │
  │                       │                   │─── gRPC/HTTP ────────▶│
  │                       │                   │                [reserve stock]
  │                       │                   │                [SELECT FOR UPDATE]
  │                       │                   │                [update reserved_qty]
  │                       │                   │◄── reservation id ───│
  │                       │                   │                      │
  │                       │            [Outbox Poller — scheduled]    │
  │                       │            [read unpublished events]      │
  │                       │            [publish to Kafka]             │
  │                       │            [mark published=true]          │
  │                       │                   │
  │◄──── 202 Accepted ───│◄─── response ────│
  │       {orderId}       │                   │
  │                       │                   │

                    Payment Service           Notification Service
                         │                          │
                         │◄── orders.created ───────│ (Kafka consumer)
                         │                          │
                   [authorize payment]              │
                   [call external gateway]          │
                   [async, CompletableFuture]        │
                   [save payment record]             │
                   [publish payments.authorized]     │
                         │                          │
                         │──── payments.authorized ─▶│
                                                    │
                                              [send confirmation]
                                              [email/SMS/push]
                                              [update notification status]
```

---

## 3. Thread Flow Diagram

Understanding WHICH thread executes WHICH code is the heart of this project.

```
HTTP Request Arrives
        │
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│  TOMCAT THREAD POOL (or Virtual Thread per request in Java 21)        │
│                                                                       │
│  Thread: VirtualThread-1 (or http-nio-8081-exec-3)                   │
│                                                                       │
│  ①  OrderController.createOrder()          [same thread]             │
│  ②  OrderApplicationService.createOrder()  [same thread]             │
│  ③  InventoryClient.reserveStock()         [same thread — BLOCKS]    │
│  ④  OrderRepository.save()                 [same thread — BLOCKS]    │
│  ⑤  Return HTTP 202                        [same thread]             │
│                                                                       │
│  With platform threads: step ③ or ④ blocking → thread is wasted     │
│  With virtual threads: thread parks cheaply → OS thread is freed     │
└───────────────────────────────────────────────────────────────────────┘
        │
        │  After response sent:
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│  SCHEDULED TASK THREAD (Outbox Poller)                                │
│                                                                       │
│  Thread: scheduling-1                                                 │
│                                                                       │
│  ⑥  OutboxPoller.pollAndPublish()  [every 1 second]                  │
│  ⑦  KafkaTemplate.send()           [non-blocking]                    │
│  ⑧  Mark outbox row published=true                                   │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
        │
        │  Kafka message received by Payment Service:
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│  KAFKA LISTENER CONTAINER THREAD (Payment Service)                    │
│                                                                       │
│  Thread: kafka-consumer-1-C-1  (one per partition, by default)       │
│                                                                       │
│  ⑨  PaymentKafkaConsumer.handleOrderCreated()                        │
│  ⑩  → offload to ASYNC EXECUTOR (CompletableFuture.supplyAsync)      │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
        │
        │  Async execution:
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│  ASYNC TASK EXECUTOR (Payment Service)                                │
│                                                                       │
│  Thread: payment-async-1                                              │
│                                                                       │
│  ⑪  PaymentService.authorize()                                       │
│      .thenApplyAsync(this::capturePayment)                            │
│      .thenApplyAsync(this::publishResult)                             │
│      .exceptionally(this::handleFailure)                              │
│      .orTimeout(10, SECONDS)                                          │
│                                                                       │
│  NOTE: MDC correlationId MUST be manually propagated here             │
│  because child threads don't inherit MDC from parent                  │
└───────────────────────────────────────────────────────────────────────┘

BROKEN (Task 4.5 demo):
  CompletableFuture.supplyAsync(() -> {
      // correlationId is null here — MDC context was lost!
      log.info("Processing payment...");  // no correlationId in log
  });

FIXED:
  var correlationId = MDC.get("correlationId");
  CompletableFuture.supplyAsync(() -> {
      MDC.put("correlationId", correlationId);
      try {
          log.info("Processing payment...");  // correlationId present
      } finally {
          MDC.clear();
      }
  });
```

---

## 4. Kafka Topic Topology

```
                           ┌─────────────────────────────────────────────────────┐
                           │                    KAFKA CLUSTER                    │
                           │                                                     │
  Order Service            │   TOPIC: orders.created (12 partitions)            │
  (Producer) ─────────────▶│   Key: orderId (ensures partition affinity)         │
                           │                                                     │
                           │   TOPIC: orders.cancelled (12 partitions)           │
  Order Service ──────────▶│   Key: orderId                                      │
                           │                                                     │
                           │   TOPIC: payments.authorized (12 partitions)        │
  Payment Service ────────▶│   Key: orderId                                      │
                           │                                                     │
                           │   TOPIC: payments.failed (12 partitions)            │
  Payment Service ────────▶│   Key: orderId                                      │
                           │                                                     │
                           │   TOPIC: inventory.reserved (12 partitions)         │
  Inventory Service ──────▶│   Key: productId                                    │
                           │                                                     │
                           │   TOPIC: inventory.released (12 partitions)         │
  Inventory Service ──────▶│   Key: productId                                    │
                           │                                                     │
                           │   TOPIC: notifications.pending (12 partitions)      │
  All services ───────────▶│   Key: userId (ensures ordering per user)           │
                           │                                                     │
                           │   TOPIC: notifications.dlq (1 partition)            │
  Notification Service ───▶│   Key: notificationId                               │
                           └──────────────────────┬──────────────────────────────┘
                                                  │
                    ┌─────────────────────────────┼──────────────────────┐
                    │                             │                      │
                    ▼                             ▼                      ▼
         ┌──────────────────┐          ┌──────────────────┐   ┌──────────────────┐
         │  Payment Service │          │ Inventory Service │   │  Notification    │
         │  Consumer Group: │          │  Consumer Group: │   │  Service         │
         │  payment-svc-grp │          │  inventory-grp   │   │  Consumer Group: │
         │                  │          │                  │   │  notif-svc-grp   │
         │  Subscribes to:  │          │  Subscribes to:  │   │                  │
         │  orders.created  │          │  orders.created  │   │  Subscribes to:  │
         │                  │          │  orders.cancelled│   │  payments.*      │
         │  Publishes to:   │          │                  │   │  orders.*        │
         │  payments.*      │          │  Publishes to:   │   │                  │
         │                  │          │  inventory.*     │   │  Publishes to:   │
         └──────────────────┘          └──────────────────┘   │  notifications.* │
                                                              └──────────────────┘

Consumer Group Offsets (monitored by metrics-service):
  kafka.consumer.lag{topic="orders.created", group="payment-svc-grp"}
  kafka.consumer.lag{topic="notifications.pending", group="notif-svc-grp"}

  Alert: lag > 10,000 on any topic/group
```

### DLQ Flow
```
notifications.pending
        │
        ▼
  [attempt 1 — fails]
  [wait 5s]
  [attempt 2 — fails]
  [wait 25s]
  [attempt 3 — fails]
        │
        ▼
notifications.dlq
        │
        ▼
  [DLQ Consumer logs alert]
  [updates notification.status = 'DEAD']
  [emits metric: notifications.dead_letter.total++]
```

---

## 5. Redis Usage Map

```
                           ┌──────────────────────────────────────────┐
                           │               REDIS :6379                │
                           │                                          │
                           │  ┌────────────────────────────────────┐  │
  Payment Service          │  │  IDEMPOTENCY KEYS                  │  │
  (write on first request) │  │  Key:   idempotency:{key}          │  │
  (read on retry)    ──────▶  │  Value: {paymentId, status}        │  │
                           │  │  TTL:   24 hours                   │  │
                           │  │  Type:  String                     │  │
                           │  └────────────────────────────────────┘  │
                           │                                          │
                           │  ┌────────────────────────────────────┐  │
  Inventory Service        │  │  INVENTORY STOCK CACHE             │  │
  (read-through)     ──────▶  │  Key:   inventory:{productId}      │  │
  (write-behind)           │  │  Value: {available, reserved}      │  │
                           │  │  TTL:   5 minutes                  │  │
                           │  │  Type:  Hash                       │  │
                           │  └────────────────────────────────────┘  │
                           │                                          │
                           │  ┌────────────────────────────────────┐  │
  API Gateway              │  │  RATE LIMITER (Sliding Window)     │  │
  (Lua script)       ──────▶  │  Key:   ratelimit:{userId}         │  │
                           │  │  Value: sorted set of timestamps   │  │
                           │  │  TTL:   1 minute                   │  │
                           │  │  Type:  ZSet                       │  │
                           │  │  Limit: 100 req/min per user       │  │
                           │  └────────────────────────────────────┘  │
                           │                                          │
                           │  ┌────────────────────────────────────┐  │
  Inventory Service        │  │  DISTRIBUTED LOCK (Flash Sale)     │  │
  (SETNX on reservation)   │  │  Key:   lock:inventory:{productId} │  │
                     ──────▶  │  Value: {instanceId}:{threadId}    │  │
                           │  │  TTL:   5 seconds (auto-expire)    │  │
                           │  │  Library: Redisson RLock           │  │
                           │  └────────────────────────────────────┘  │
                           │                                          │
                           └──────────────────────────────────────────┘

CACHE STAMPEDE scenario (Task 6.6):
  ① inventory:{productId} TTL expires
  ② 10,000 concurrent requests find cache MISS
  ③ All 10,000 hit PostgreSQL simultaneously
  ④ DB CPU → 100%, connection pool exhausted

FIX — Probabilistic Early Expiration:
  ① Key still has 30s TTL remaining
  ② Each reader runs: rand() < delta * beta * log(ttl_remaining)
  ③ Lucky reader refreshes cache early, before stampede
  ④ Others get cache HIT

FIX — Mutex Lock:
  ① Cache MISS → try SETNX lock:inventory:{id}
  ② Winner fetches from DB, populates cache, releases lock
  ③ Losers wait briefly, then get cache HIT
```

---

## 6. Database Interaction Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                    POSTGRESQL :5432                                  │
│                                                                     │
│  ┌─────────────┐   ┌──────────────┐   ┌───────────┐               │
│  │   orders    │   │  order_items │   │ inventory │               │
│  │             │   │              │   │           │               │
│  │ @Version ──▶│   │              │   │ @Version  │               │
│  │ (optimistic)│   │              │   │ (optimistic│               │
│  └──────┬──────┘   └──────┬───────┘   └─────┬─────┘               │
│         │                │                  │                     │
│  ┌──────▼──────┐   ┌──────▼───────┐   ┌─────▼─────┐               │
│  │  payments   │   │ notifications│   │  outbox   │               │
│  │             │   │              │   │  _events  │               │
│  │ idempotency_│   │ status index │   │           │               │
│  │ key (UNIQUE)│   │ scheduled_at │   │ published │               │
│  └─────────────┘   └──────────────┘   └───────────┘               │
│                                                                     │
└────────────┬──────────────────────────────────────────────────────┘
             │
             │  Connection Pools (HikariCP — one pool per service):
             │
    ┌─────────┴──────────────────────────────────────────────┐
    │                                                        │
    ▼                                                        ▼
┌──────────────────────┐                        ┌──────────────────────┐
│  Order Service Pool  │                        │  Payment Service Pool│
│  max=20, min=5       │                        │  max=20, min=5       │
│  timeout=3s          │                        │  timeout=3s          │
└──────────────────────┘                        └──────────────────────┘

Transaction Boundaries:
─────────────────────────────────────────────────────────────────
  Order Creation:
    BEGIN
      INSERT INTO orders (status=PENDING)
      INSERT INTO order_items
      INSERT INTO outbox_events (published=false)  ← same transaction!
    COMMIT
  ← Outbox poller reads and publishes AFTER commit (no dual-write risk)

  Inventory Reservation — Pessimistic (Flash Sale):
    BEGIN
      SELECT * FROM inventory
        WHERE product_id = ?
        FOR UPDATE            ← row-level write lock
      UPDATE inventory
        SET reserved_quantity = reserved_quantity + ?
        WHERE total_quantity - reserved_quantity >= ?
    COMMIT

  Inventory Reservation — Optimistic (Normal Load):
    BEGIN
      SELECT * FROM inventory WHERE product_id = ?  -- reads version=5
      -- application checks availability
      UPDATE inventory
        SET reserved_quantity = reserved_quantity + ?, version = 6
        WHERE product_id = ? AND version = 5        -- CAS
      -- if 0 rows updated → someone else modified it → retry
    COMMIT

Locking Decision Matrix:
  ┌──────────────┬────────────────────┬──────────────────────────────┐
  │ Scenario     │ Lock Type          │ Reason                       │
  ├──────────────┼────────────────────┼──────────────────────────────┤
  │ Normal order │ Optimistic (@Ver.) │ Low contention, high perf    │
  │ Flash sale   │ Pessimistic (SKIP  │ High contention, retries are │
  │              │ LOCKED)            │ too expensive                │
  │ Payment auth │ None (idempotency  │ External system, no shared   │
  │              │ key in Redis)      │ mutable state to lock        │
  │ Report batch │ No lock (read-only)│ Historical data, immutable   │
  └──────────────┴────────────────────┴──────────────────────────────┘
```

---

## 7. Concurrency Bug Map (Visual)

```
WHERE BUGS LIVE IN THE CODEBASE:

order-service/
  └── infrastructure/
      └── persistence/
          ├── v1_broken/
          │   └── UnsafeOrderStatusTransition.java   ← Race condition (Task 2.2)
          └── v2_fixed/
              └── SafeOrderStatusTransition.java      ← @Version + retry

  └── application/
      ├── v1_broken/
      │   └── UnsafeRequestCounter.java              ← Visibility bug (Task 2.3)
      └── v2_fixed/
          └── SafeRequestCounter.java                ← AtomicLong

inventory-service/
  └── application/
      ├── v1_broken/
      │   ├── UnsafeInventoryService.java            ← Read-modify-write race (Task 3.2)
      │   └── DeadlockInventoryService.java          ← Deadlock demo (Task 3.3)
      └── v2_fixed/
          ├── PessimisticInventoryService.java       ← SELECT FOR UPDATE
          ├── OptimisticInventoryService.java        ← @Version
          └── RedisInventoryService.java             ← DECRBY atomic

payment-service/
  └── application/
      ├── v1_broken/
      │   ├── BlockingPaymentService.java            ← Thread starvation (Task 4.6)
      │   └── NoBackoffPaymentRetry.java             ← Retry storm (Task 4.4)
      └── v2_fixed/
          ├── AsyncPaymentService.java               ← CompletableFuture
          └── ExponentialBackoffRetry.java           ← Backoff + jitter

notification-service/
  └── infrastructure/
      └── kafka/
          ├── v1_broken/
          │   └── BlockingNotificationConsumer.java  ← Consumer lag (Task 5.2)
          └── v2_fixed/
              └── AsyncNotificationConsumer.java     ← Bounded async dispatch
```
