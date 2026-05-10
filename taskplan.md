# Task Plan — High-Scale Order & Payment Processing Simulation Platform

All tasks follow this rule: **implement the broken version first, observe the failure, then implement the fix.** Never skip straight to the solution — the learning is in experiencing the failure.

Complexity ratings: **S** (< 2h), **M** (2-4h), **L** (4-8h), **XL** (1-2 days)

---

## EPIC 1: Foundation & Infrastructure

**Goal:** Get the full infrastructure stack running locally. Every subsequent epic depends on this foundation being solid.

---

### Task 1.1 — Maven Multi-Module Project Scaffold

**Description:** Create the parent POM and all module skeletons. The project must compile end-to-end with `mvn clean package -DskipTests` before any feature work begins.

**Subtasks:**
- 1.1.1 Create root `pom.xml` with `<modules>` for all 8 modules, Java 21 compiler config, Spring Boot BOM import
- 1.1.2 Create `platform-common/pom.xml` — no Spring Boot starter, only utility libs (logstash-logback-encoder, mapstruct, lombok)
- 1.1.3 Create `platform-domain/pom.xml` — zero framework dependencies (pure Java)
- 1.1.4 Create `order-service/pom.xml`, `payment-service/pom.xml`, `inventory-service/pom.xml`, `notification-service/pom.xml`, `metrics-service/pom.xml` — each with spring-boot-starter-web, spring-boot-starter-data-jpa, spring-boot-starter-actuator, spring-kafka
- 1.1.5 Create `api-gateway/pom.xml` with spring-cloud-starter-gateway
- 1.1.6 Create `load-test/pom.xml` with Gatling Maven plugin (not a Spring Boot app)
- 1.1.7 Verify: `mvn clean package -DskipTests` passes for all modules

**Learning Outcome:** Multi-module Maven projects — dependency management, BOM imports, module isolation.

**Concurrency Concept:** N/A (infrastructure).

**How to Test:** `mvn clean package -DskipTests -pl order-service` compiles without error.

**Failure Mode:** Missing BOM import causes conflicting Spring Boot dependency versions.

**Acceptance Criteria:**
- All modules compile cleanly
- `mvn dependency:tree -pl order-service` shows no version conflicts
- No circular module dependencies

**Complexity:** M

---

### Task 1.2 — Docker Compose Infrastructure Stack

**Description:** Create `docker/docker-compose.yml` that starts PostgreSQL, Redis, Kafka (with Zookeeper), Prometheus, and Grafana with a single `docker compose up -d` command.

**Subtasks:**
- 1.2.1 Add PostgreSQL 16 service with init script (`docker/postgres/init.sql` creates `platform_db`)
- 1.2.2 Add Redis 7 service
- 1.2.3 Add Zookeeper + Kafka 3.7 service with `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`
- 1.2.4 Add Prometheus service with `docker/prometheus/prometheus.yml` scraping all services at `:808x/actuator/prometheus`
- 1.2.5 Add Grafana service with provisioned data source and dashboard directories
- 1.2.6 Add health checks for each container so dependent services wait for dependencies
- 1.2.7 Verify: `docker compose up -d && docker compose ps` shows all services healthy

**Learning Outcome:** Production-like local environment. Understanding service dependencies and startup order.

**Concurrency Concept:** N/A.

**How to Test:** `docker compose ps` shows all containers as `healthy`. `psql -h localhost -U platform -d platform_db -c "\dt"` connects successfully.

**Failure Mode:** Kafka starts before Zookeeper is ready → `kafka.common.KafkaException`.

**Acceptance Criteria:**
- All 6 services start and report healthy
- Prometheus UI at `http://localhost:9090` shows targets
- Grafana at `http://localhost:3000` (admin/admin) is accessible

**Complexity:** M

---

### Task 1.3 — Flyway Database Migrations

**Description:** Set up Flyway in each service that owns database tables. Migrations run automatically on application startup.

**Subtasks:**
- 1.3.1 Add `flyway-core` dependency to order-service, payment-service, inventory-service, notification-service
- 1.3.2 Configure `spring.flyway.locations=classpath:db/migration` in `application.yml`
- 1.3.3 Create `V1__create_orders.sql`, `V2__create_order_items.sql` in order-service
- 1.3.4 Create `V3__create_inventory.sql` in inventory-service
- 1.3.5 Create `V4__create_payments.sql` in payment-service
- 1.3.6 Create `V5__create_notifications.sql`, `V6__create_outbox_events.sql` in notification-service (outbox is owned by notification service as it's the primary event publisher)
- 1.3.7 Note: outbox_events is used by all services — each service has its own outbox table (or a shared schema partition)
- 1.3.8 Verify: start order-service → logs show `Successfully applied 2 migrations`

**Learning Outcome:** Idempotent, version-controlled schema changes. Understanding Flyway checksum enforcement.

**Concurrency Concept:** N/A.

**How to Test:** Drop and recreate DB, restart service → migrations re-apply correctly.

**Failure Mode:** Modifying a committed migration file causes Flyway checksum failure on restart.

**Acceptance Criteria:**
- `flyway_schema_history` table shows all migrations with `success=true`
- All tables and indexes exist per `database_schema.md`

**Complexity:** S

---

### Task 1.4 — Structured Logging with Correlation ID

**Description:** Implement JSON-format structured logging with a correlation ID that flows through every log line — including across thread boundaries and Kafka hops.

**Subtasks:**
- 1.4.1 Add `logstash-logback-encoder` to `platform-common`
- 1.4.2 Create `logback-spring.xml` in each service: JSON encoder with fields: `timestamp`, `level`, `service`, `correlationId`, `threadName`, `threadType`, `userId`, `orderId`, `operation`, `durationMs`, `message`
- 1.4.3 Create `CorrelationIdFilter` (Servlet filter) in platform-common: reads `X-Correlation-Id` header or generates UUID, stores in `MDC.put("correlationId", id)`
- 1.4.4 Create `MdcTaskDecorator` — wraps `Runnable`/`Callable` to copy MDC context to child threads (critical for `CompletableFuture` and `@Async`)
- 1.4.5 Register `MdcTaskDecorator` in `ThreadPoolConfig` for all async executors
- 1.4.6 Create `KafkaCorrelationIdInterceptor` — produces correlation ID as Kafka message header; consumer reads it back into MDC before processing
- 1.4.7 Verify: make an API call with `X-Correlation-Id: test-123` → trace the same ID through order-service logs, Kafka consumer logs in payment-service, notification-service logs

**Learning Outcome:** `ThreadLocal` and `MDC`, context propagation across async boundaries, why child threads lose parent context.

**Concurrency Concept:** `ThreadLocal` — MDC uses ThreadLocal internally; demonstrates that ThreadLocal is NOT inherited by child threads automatically.

**How to Test:**
```bash
curl -H "X-Correlation-Id: abc-123" -X POST http://localhost:8080/api/orders ...
grep "abc-123" logs/order-service.log  # appears in all related log lines
grep "abc-123" logs/payment-service.log  # appears in consumer log
```

**Failure Mode:** Without `MdcTaskDecorator`, async threads log with `correlationId=null`. Without Kafka interceptor, the ID is lost at the service boundary.

**Acceptance Criteria:**
- Every log line has `correlationId` populated
- Same ID appears across all services for one logical request
- Log format is valid JSON (validate with `jq`)

**Complexity:** M

---

### Task 1.5 — Micrometer + Prometheus + Grafana Baseline

**Description:** Wire up Micrometer in every service, expose `/actuator/prometheus`, configure Prometheus scrape, and create a baseline Grafana dashboard.

**Subtasks:**
- 1.5.1 Add `micrometer-registry-prometheus` to all services
- 1.5.2 Enable `management.endpoints.web.exposure.include=health,info,prometheus,metrics` in `application.yml`
- 1.5.3 Create custom metrics in platform-common: `OrderMetrics`, `PaymentMetrics`, `InventoryMetrics` (Counter, Timer, Gauge wrappers)
- 1.5.4 Configure Prometheus `scrape_configs` in `prometheus.yml` for all service endpoints
- 1.5.5 Create Grafana dashboard `overview.json` with panels: request rate, error rate, P50/P95/P99 latency, JVM heap
- 1.5.6 Create Grafana dashboard `jvm.json` with JVM heap, thread counts, GC pause duration, class loading
- 1.5.7 Verify: `curl http://localhost:8081/actuator/prometheus` returns Prometheus text format metrics

**Learning Outcome:** Production observability from day one. Without metrics, you're flying blind during load tests.

**Concurrency Concept:** Thread pool metrics — you'll observe thread pool exhaustion in Grafana before you understand why it happens.

**How to Test:** Generate a few orders via curl → Prometheus shows `orders_created_total` incrementing.

**Failure Mode:** Micrometer not on classpath causes `NoSuchBeanDefinitionException` for `MeterRegistry`.

**Acceptance Criteria:**
- All services appear as green targets in Prometheus
- Grafana shows live data for all services
- Custom metrics visible in `http://localhost:9090/graph`

**Complexity:** M

---

## EPIC 2: Order Service — Core + Concurrency Bugs

**Goal:** Build the full Order Service with clean architecture, then deliberately introduce and fix two concurrency bugs.

---

### Task 2.1 — Order Service REST API + Domain Model

**Description:** Implement the full Order Service: controller, application service, domain model, JPA entities, repository. Orders can be created, retrieved, and cancelled.

**Subtasks:**
- 2.1.1 Create domain model: `Order`, `OrderItem`, `OrderStatus` enum in `platform-domain`
- 2.1.2 Create `OrderJpaEntity` with `@Version`, `@OneToMany` to `OrderItemJpaEntity`
- 2.1.3 Create `OrderJpaRepository extends JpaRepository`
- 2.1.4 Create `OrderRepositoryAdapter` implementing domain `OrderRepository` interface (anti-corruption layer)
- 2.1.5 Create `CreateOrderCommand`, `OrderApplicationService.createOrder()`
- 2.1.6 Create `OrderController` with: `POST /api/orders`, `GET /api/orders/{id}`, `DELETE /api/orders/{id}` (cancel)
- 2.1.7 Add request validation (`@Valid`, `@NotNull`, `@Positive`)
- 2.1.8 Add global exception handler (`@RestControllerAdvice`) with problem-detail responses
- 2.1.9 Write integration test with `@SpringBootTest` + Testcontainers (PostgreSQL)
- 2.1.10 Verify: `curl -X POST http://localhost:8081/api/orders -d '...'` returns `202 Accepted` with orderId

**Learning Outcome:** Clean layered architecture, domain-driven design, JPA relationship mapping.

**Concurrency Concept:** `@Version` field is on the entity now — you'll use it in Task 2.2.

**How to Test:** Integration test creates 10 orders sequentially, verifies all 10 in DB.

**Failure Mode:** Lazy-loading `OrderItem` outside of transaction → `LazyInitializationException`.

**Acceptance Criteria:**
- All CRUD operations work via REST
- Integration test passes with real PostgreSQL (Testcontainers)
- Error responses follow RFC 7807 (problem-detail format)

**Complexity:** M

---

### Task 2.2 — [BROKEN → FIXED] Non-Atomic Order Status Transition

**Description:** Demonstrate the check-then-act race condition in order state transitions. A concurrent `confirmOrder` and `cancelOrder` for the same order can both succeed, leaving the order in an inconsistent state.

**Subtasks:**

**Part A — Implement the broken version:**
- 2.2.1 Create `v1_broken/UnsafeOrderStatusService.java`:
  ```java
  public void confirmOrder(UUID orderId) {
      Order order = orderRepo.findById(orderId);
      if (order.getStatus() == OrderStatus.PENDING) {  // read
          Thread.sleep(50);  // simulate processing delay — race window
          order.setStatus(OrderStatus.CONFIRMED);        // write
          orderRepo.save(order);
      }
  }
  ```
- 2.2.2 Write `UnsafeOrderStatusRaceTest` — 2 threads: one confirms, one cancels the same order simultaneously. Assert that both cannot succeed.
- 2.2.3 Run test — observe it fails (both threads succeed, impossible state)

**Part B — Fix with optimistic locking:**
- 2.2.4 Create `v2_fixed/SafeOrderStatusService.java` using `@Version` + Spring Retry
- 2.2.5 Add `@Retryable(retryFor = ObjectOptimisticLockingFailureException.class, maxAttempts = 3)`
- 2.2.6 Run the same concurrent test — now exactly one thread wins, the other retries and finds the state already changed

**Part C — Observe in Grafana:**
- 2.2.7 Add metric: `order.status.transition.retries` counter — visible in Grafana during load test
- 2.2.8 Run `ConcurrentStatusTransitionSimulation.scala` with 500 VUs — compare retry rate broken vs fixed

**Learning Outcome:**
- Why `if (check) { act }` is never thread-safe in a multi-threaded environment
- How optimistic locking uses the DB as a coordination point
- Why `@Transactional` alone does NOT prevent this (READ COMMITTED allows the race)

**Concurrency Concept:** Race condition (check-then-act), optimistic locking, `@Version`.

**How to Test:**
```bash
# Run broken version test:
mvn test -pl order-service -Dtest=UnsafeOrderStatusRaceTest
# Expected: test FAILS (race condition allows invalid state)

# Switch to fixed version:
mvn test -pl order-service -Dtest=SafeOrderStatusRaceTest
# Expected: test PASSES (one winner, one retry)
```

**Failure Mode:** Forgetting `@Transactional` on the fixed version → optimistic lock check happens but transaction isn't committed atomically.

**Acceptance Criteria:**
- Broken test demonstrates the race (at least 1 in 5 runs produces invalid state)
- Fixed test always produces consistent state (100 consecutive runs pass)
- Grafana shows retry metric during load test

**Complexity:** M

---

### Task 2.3 — [BROKEN → FIXED] Non-Thread-Safe Request Counter

**Description:** A singleton service bean maintains a request counter using a plain `int`. Under concurrent load, increments are lost. Fix with `AtomicLong`, then `LongAdder`, and benchmark the difference.

**Subtasks:**
- 2.3.1 Create `v1_broken/UnsafeRequestCounter.java` using `private int count = 0; count++;`
- 2.3.2 Write `UnsafeCounterTest`: 100 threads each increment 1000 times, assert total == 100,000. Run 10 times — it fails most times.
- 2.3.3 Create `v2_fixed/AtomicRequestCounter.java` using `AtomicLong.incrementAndGet()`
- 2.3.4 Create `v3_fixed/LongAdderCounter.java` using `LongAdder.increment()` + `sum()`
- 2.3.5 Write JMH benchmark comparing `int++` vs `synchronized int++` vs `AtomicLong` vs `LongAdder` under 16 threads
- 2.3.6 Document results: LongAdder wins under high contention (striped cells reduce CAS collisions)

**Learning Outcome:**
- `int++` is not atomic — it's 3 JVM instructions (load, increment, store)
- `AtomicLong` uses Compare-And-Swap — correct but contended under high load
- `LongAdder` stripes the counter across cells — best for high-write, infrequent-read scenarios

**Concurrency Concept:** Atomic operations, CAS, memory visibility, `volatile`.

**How to Test:**
```bash
mvn test -pl order-service -Dtest=UnsafeCounterTest  # fails
mvn test -pl order-service -Dtest=AtomicCounterTest   # passes
```

**Failure Mode:** `volatile int count` does NOT fix the problem — volatile ensures visibility but not atomicity. Many engineers make this mistake.

**Acceptance Criteria:**
- Broken test fails at least 8 out of 10 runs
- AtomicLong test passes 100/100 runs
- JMH benchmark results documented in `benchmark_results/counter_benchmark.md`

**Complexity:** M

---

### Task 2.4 — Batch Order Processing with ForkJoinPool

**Description:** Generate a daily order report processing 100,000 historical orders. Demonstrate the performance difference between sequential, parallel stream, and ForkJoinPool-based processing.

**Subtasks:**
- 2.4.1 Create `OrderReportService` that loads orders in pages and enriches each with calculated fields
- 2.4.2 Implement `v1_sequential`: process with a standard `stream().map()` — measure wall time
- 2.4.3 Implement `v2_parallel_stream`: use `parallelStream()` on the common ForkJoinPool — measure wall time
- 2.4.4 Demonstrate the problem with `v2`: common ForkJoinPool is shared with all parallel streams in the JVM — running the report blocks other parallel operations
- 2.4.5 Implement `v3_dedicated_pool`: `new ForkJoinPool(cores - 1)` dedicated pool, submit task explicitly
- 2.4.6 Implement `v4_recursive_task`: custom `RecursiveTask<List<OrderSummary>>` that splits at 10,000 orders
- 2.4.7 Benchmark all four approaches, document in `benchmark_results/batch_benchmark.md`
- 2.4.8 Show how to monitor ForkJoinPool via Micrometer (active threads, steal count, queue size)

**Learning Outcome:**
- ForkJoinPool work-stealing algorithm
- Why using the common pool is dangerous in server applications
- `RecursiveTask` vs `parallelStream` — when each is appropriate
- CPU-bound parallelism ceiling: adding more threads than cores hurts performance

**Concurrency Concept:** ForkJoinPool, RecursiveTask, parallel streams, work-stealing.

**How to Test:** Run `OrderReportBenchmarkTest` with 100,000 synthetic orders, verify all 4 approaches produce identical results.

**Failure Mode:** Using `parallelStream()` inside a Kafka listener starves the consumer thread, causing consumer lag.

**Acceptance Criteria:**
- Dedicated pool approach is fastest (for CPU-bound work)
- All approaches produce same output (correctness)
- ForkJoinPool metrics visible in Grafana during batch run

**Complexity:** L

---

### Task 2.5 — Virtual Threads for Order Validation

**Description:** The order service calls an external address validation service (simulated with a slow mock). Demonstrate platform thread exhaustion vs virtual thread scalability.

**Subtasks:**
- 2.5.1 Create `MockAddressValidationClient` that sleeps 200ms (simulating slow external HTTP call)
- 2.5.2 Configure `v1_platform_threads`: `spring.threads.virtual.enabled=false`, `server.tomcat.threads.max=10` (deliberately small)
- 2.5.3 Run load test: 50 concurrent order creation requests → 40 requests timeout (only 10 threads available)
- 2.5.4 Configure `v2_virtual_threads`: `spring.threads.virtual.enabled=true`
- 2.5.5 Run same load test: 50 concurrent requests → all complete (virtual threads park during the 200ms sleep, OS threads are reused)
- 2.5.6 Show in Grafana: JVM thread count (platform=10 stuck, virtual=shows ~200 carriers reused)
- 2.5.7 Explain the "pinning" problem: if code inside virtual thread calls synchronized or JNI, it pins the carrier thread

**Learning Outcome:**
- Virtual threads are NOT faster — they have the same throughput for CPU work
- Virtual threads shine for I/O-bound workloads (they park cheaply)
- Pinning: synchronized blocks inside virtual threads can re-create the starvation problem

**Concurrency Concept:** Virtual Threads (Java 21), platform threads, OS thread pinning, I/O-bound vs CPU-bound.

**How to Test:**
```bash
# With platform threads (maxThreads=10):
mvn gatling:test -pl load-test -Dsimulation=VirtualThreadSimulation -Dthreads=false
# → Many 503s expected

# With virtual threads:
mvn gatling:test -pl load-test -Dsimulation=VirtualThreadSimulation -Dthreads=virtual
# → All succeed
```

**Failure Mode:** Virtual threads still starve if code uses `synchronized` — use `ReentrantLock` instead for lock-heavy code with virtual threads.

**Acceptance Criteria:**
- Platform thread test shows >= 40% error rate at 50 concurrency
- Virtual thread test shows 0% error rate at 50 concurrency
- Demonstrated via Grafana JVM thread count panel

**Complexity:** M

---

## EPIC 3: Inventory Service — Race Conditions + Locking

**Goal:** Build the Inventory Service and implement the full progression of concurrency fixes for the oversell problem — the most important lesson in this project.

---

### Task 3.1 — Inventory Service REST API + Domain Model

**Description:** Implement Inventory Service with product stock management. Standard CRUD for stock levels, query for available stock.

**Subtasks:**
- 3.1.1 Create `Inventory` domain entity, `InventoryJpaEntity` with `@Version`
- 3.1.2 Create `InventoryController`: `GET /api/inventory/{productId}`, `PUT /api/inventory/{productId}/stock`
- 3.1.3 Create `InventoryApplicationService.reserveStock()` and `releaseStock()`
- 3.1.4 Create Kafka consumer for `orders.created` → reserve stock; `orders.cancelled` → release stock
- 3.1.5 Integration test with Testcontainers (PostgreSQL + Kafka)

**Learning Outcome:** N/A (baseline — needed for the concurrency demos in Tasks 3.2-3.5).

**Complexity:** S

---

### Task 3.2 — [BROKEN → FIXED] Inventory Oversell Race Condition

**Description:** The central lesson of this project. Implement four progressively safer inventory reservation strategies and benchmark their behavior under concurrent flash sale load.

**Subtasks:**

**v1_broken — No lock:**
- 3.2.1 Implement bare read-modify-write: read quantity, check, update if available
- 3.2.2 Load test: 100 concurrent buyers, 1 item in stock → verify oversell (multiple buyers confirm)

**v2_synchronized — Java-level lock:**
- 3.2.3 Wrap in `synchronized(this)` — prevents race within single JVM
- 3.2.4 Load test: works for single instance, BUT if two service instances run, synchronized doesn't help (no shared JVM lock)
- 3.2.5 Document: why synchronized is insufficient for distributed systems

**v3_pessimistic — DB-level lock:**
- 3.2.6 Use `@Lock(PESSIMISTIC_WRITE)` → `SELECT FOR UPDATE`
- 3.2.7 Load test: no oversell even with multiple instances, but throughput drops (serial at DB level)
- 3.2.8 Measure: throughput at 100 concurrent buyers

**v4_optimistic — Version-based:**
- 3.2.9 Use `@Version` + retry on `ObjectOptimisticLockingFailureException`
- 3.2.10 Load test: higher throughput than pessimistic for LOW contention, but under flash sale high contention → retry storm
- 3.2.11 Measure: retry rate under 100% contention

**v5_redis — Atomic Redis counter:**
- 3.2.12 Use `DECRBY` Lua script: atomic decrement if result >= 0, else reject
- 3.2.13 Load test: highest throughput, no DB lock contention, no retries
- 3.2.14 Discuss tradeoff: Redis is single-threaded per command → serialize at Redis level, not DB

**Benchmark Summary (document in `benchmark_results/inventory_benchmark.md`):**
| Strategy | Throughput | Consistency | Multi-instance | Complexity |
|---|---|---|---|---|
| No lock | High | BROKEN | N/A | Low |
| synchronized | Medium | Single JVM only | No | Low |
| Pessimistic | Low-Medium | Correct | Yes | Medium |
| Optimistic | Medium-High | Correct (low contention) | Yes | Medium |
| Redis atomic | High | Correct | Yes | High |

**Learning Outcome:**
- Database is the authoritative source — all locking strategies ultimately serialize at DB or Redis
- synchronized only works within a single JVM process — useless in distributed systems
- Optimistic locking is best for LOW contention; pessimistic is best for HIGH contention
- Redis atomic operations (DECRBY) are the production choice for flash sales

**Concurrency Concept:** Race condition, synchronized, pessimistic locking, optimistic locking, atomic Redis operations, distributed systems.

**How to Test:**
```bash
mvn gatling:test -pl load-test -Dsimulation=FlashSaleSimulation -Dstrategy=v1_broken
# → DB shows negative stock

mvn gatling:test -pl load-test -Dsimulation=FlashSaleSimulation -Dstrategy=v5_redis
# → No oversell, highest throughput
```

**Failure Mode:** Forgetting to handle `ObjectOptimisticLockingFailureException` — unhandled exception causes 500 errors instead of graceful retry.

**Acceptance Criteria:**
- v1_broken produces oversell on every run with 100 concurrent buyers
- v3_pessimistic, v4_optimistic, v5_redis never produce oversell
- Benchmark results documented with actual measured throughput numbers

**Complexity:** L

---

### Task 3.3 — [BROKEN → FIXED] Deadlock Demonstration

**Description:** Artificially introduce a deadlock between inventory reservation and payment lock acquisition. Observe PostgreSQL deadlock detection, then fix with consistent lock ordering.

**Subtasks:**
- 3.3.1 Create `DeadlockInventoryPaymentTest` that spawns two threads:
  - T1: locks inventory row A, then payment row 1
  - T2: locks payment row 1, then inventory row A
- 3.3.2 Run test, observe `org.springframework.dao.CannotAcquireLockException` (PostgreSQL kills one transaction)
- 3.3.3 Parse the PostgreSQL log for `deadlock detected` message and explain the cycle
- 3.3.4 Fix: both threads must lock resources in the same order (sort UUIDs, lock lowest first)
- 3.3.5 Demonstrate `ReentrantLock.tryLock(timeout)` as an alternative — fail fast rather than deadlock

**Learning Outcome:** Deadlock conditions (mutual exclusion, hold-and-wait, no preemption, circular wait), PostgreSQL deadlock detection (not JVM-level), consistent lock ordering as prevention.

**Concurrency Concept:** Deadlock, `ReentrantLock`, lock ordering.

**How to Test:**
```bash
mvn test -pl inventory-service -Dtest=DeadlockTest
# → Observe CannotAcquireLockException in test output
# → Fix and re-run → no exception
```

**Complexity:** M

---

### Task 3.4 — ReadWriteLock for Inventory Reads

**Description:** Inventory is read 100x more than it is written (browse product catalogue vs. purchase). Using `synchronized` on reads unnecessarily blocks concurrent readers. Demonstrate with `ReentrantReadWriteLock`.

**Subtasks:**
- 3.4.1 Create in-memory inventory cache (`ConcurrentHashMap<UUID, InventorySnapshot>`)
- 3.4.2 `v1_synchronized`: read and write both use `synchronized(this)` — readers block each other
- 3.4.3 `v2_read_write_lock`: reads use `readLock()` (concurrent), writes use `writeLock()` (exclusive)
- 3.4.4 JMH benchmark: 90% read / 10% write mix, 16 threads → ReadWriteLock shows ~8x throughput improvement
- 3.4.5 Demonstrate starvation: if writes are continuous, readers can starve. Use `new ReentrantReadWriteLock(true)` (fair mode) to fix.

**Learning Outcome:** When reads dominate, ReadWriteLock dramatically outperforms synchronized. Fair mode prevents write starvation but reduces throughput slightly.

**Concurrency Concept:** `ReentrantReadWriteLock`, read/write lock, starvation, fairness.

**Complexity:** M

---

### Task 3.5 — Flash Sale with Redis Distributed Lock

**Description:** In a multi-instance deployment, the Java-level `synchronized` is useless. Demonstrate using Redis `SETNX` as a distributed lock for cross-instance inventory reservation during a flash sale.

**Subtasks:**
- 3.5.1 Start two instances of inventory-service (different ports, same DB + Redis)
- 3.5.2 Without distributed lock: both instances concurrently reserve, oversell occurs despite pessimistic DB lock (race before lock acquisition)
- 3.5.3 Implement Redisson `RLock` distributed lock: `lock.tryLock(3, 5, TimeUnit.SECONDS)`
- 3.5.4 The watchdog: Redisson automatically renews lock TTL if processing takes longer than expected
- 3.5.5 Test with two instances + Gatling flash sale simulation
- 3.5.6 Show in Redis: `KEYS lock:inventory:*` shows the active lock during the flash sale

**Learning Outcome:** Distributed systems don't share memory — Java locks are useless. Redis distributed lock is the standard solution. TTL prevents deadlock if the lock holder crashes.

**Concurrency Concept:** Distributed locking, Redis SETNX, Redisson, TTL-based lock expiry, watchdog renewal.

**Complexity:** L

---

## EPIC 4: Payment Service — Async + Idempotency

**Goal:** Build the Payment Service with a fully async CompletableFuture pipeline and robust idempotency, then demonstrate and fix thread starvation and retry storms.

---

### Task 4.1 — Payment Service Domain + Kafka Integration

**Description:** Create Payment Service with domain model, REST API, and Kafka consumer for `orders.created`.

**Subtasks:**
- 4.1.1 Create `Payment` domain, `PaymentJpaEntity` with `idempotencyKey` unique constraint
- 4.1.2 Create `PaymentController`: `POST /api/payments/authorize`, `GET /api/payments/{id}`
- 4.1.3 Create Kafka consumer: `orders.created` → `initiatePayment()`
- 4.1.4 Create `MockPaymentGatewayClient` with configurable delay (`GATEWAY_DELAY_MS` env var) and failure rate (`GATEWAY_FAILURE_RATE`)
- 4.1.5 Integration test

**Complexity:** M

---

### Task 4.2 — CompletableFuture Payment Pipeline

**Description:** Refactor synchronous payment processing into an async pipeline with proper timeout, error handling, and fallback.

**Subtasks:**
- 4.2.1 `v1_blocking`: sequential synchronous calls — auth → capture → save → publish
- 4.2.2 `v2_completable_future`: chain with `supplyAsync`, `thenApplyAsync`, `thenCompose`, `exceptionally`
- 4.2.3 Add `orTimeout(10, SECONDS)` — prevents hanging indefinitely
- 4.2.4 Add `exceptionally` fallback: on timeout, set payment status to FAILED, publish `payments.failed`
- 4.2.5 Demonstrate `thenApply` vs `thenApplyAsync` — which thread executes the continuation?
  - `thenApply`: executes on the thread that completes the future (may be the caller thread!)
  - `thenApplyAsync`: always executes on the executor's thread pool
- 4.2.6 Show MDC propagation problem: `correlationId` is null in async continuations — fix with `MdcTaskDecorator`

**Learning Outcome:**
- `CompletableFuture` composability and error handling
- Thread execution model: which thread runs which stage
- Why `thenApplyAsync` is safer in server applications (avoids blocking caller threads)
- MDC context propagation across CompletableFuture chains

**Concurrency Concept:** `CompletableFuture`, async composition, timeout, `ThreadLocal`, `Executor`.

**Complexity:** M

---

### Task 4.3 — Idempotency — Duplicate Payment Prevention

**Description:** Implement bulletproof idempotency so that duplicate payment requests (from client retries) never result in double charges.

**Subtasks:**
- 4.3.1 `v1_no_idempotency`: no deduplication → concurrent retries cause multiple charges
- 4.3.2 `v2_db_idempotency`: rely on `UNIQUE(idempotency_key)` constraint in PostgreSQL
  - First request: INSERT succeeds → process payment
  - Retry: INSERT throws `DataIntegrityViolationException` → fetch and return existing payment
- 4.3.3 `v3_redis_cache`: Redis `SETNX` check before DB → avoids DB roundtrip on hot retries
  - Race condition: two retries arrive simultaneously, both miss Redis, both try DB INSERT → only one succeeds, other reads existing
- 4.3.4 Write `ConcurrentIdempotencyTest`: 10 threads submit same payment simultaneously → exactly 1 payment created, all 10 threads return same response
- 4.3.5 Show the subtle race in v3: Redis SETNX is NX (set if not exists) — atomic. But Redis → DB window still has a race if Redis crashes. The DB UNIQUE constraint is the ground truth.

**Learning Outcome:** Idempotency is a distributed systems problem. The correct solution has two layers: fast Redis check + authoritative DB constraint. Neither alone is sufficient.

**Concurrency Concept:** Idempotency, race conditions, distributed state, Redis SETNX, DB constraints as last-resort safety net.

**Complexity:** M

---

### Task 4.4 — [BROKEN → FIXED] Payment Retry Storm

**Description:** When the external payment gateway recovers after an outage, all pending payments retry simultaneously — overwhelming the gateway and causing it to fail again.

**Subtasks:**
- 4.4.1 `v1_no_backoff`: failed payments retry immediately on a fixed 5-second scheduler → 1000 payments hit gateway simultaneously
- 4.4.2 Simulate gateway recovery: `GATEWAY_FAILURE_RATE=1.0` → set to `0.0` after 30s → observe 503 spike
- 4.4.3 `v2_exponential_backoff`: retry delay = `min(base * 2^attempt, maxDelay)` — spreads retries over time
- 4.4.4 `v3_jitter`: add `random(0, delay)` to backoff — prevents multiple instances from synchronizing retries
- 4.4.5 `v4_circuit_breaker`: Resilience4j `CircuitBreaker` wraps gateway calls
  - CLOSED → after 5 failures → OPEN (fast-fail 10s) → HALF-OPEN → test one call → CLOSED
- 4.4.6 Show in Grafana: retry rate, circuit breaker state, gateway call rate — before and after fix

**Learning Outcome:** Retry storms are a real production incident pattern. Jitter is the key insight that prevents synchronized retry bursts. Circuit breakers protect downstream services during recovery.

**Concurrency Concept:** Backpressure, exponential backoff, jitter, circuit breaker pattern.

**Complexity:** M

---

### Task 4.5 — ThreadLocal for Payment Context Propagation

**Description:** Demonstrate why `ThreadLocal` is needed for request-scoped context and the pitfall of ThreadLocal in thread pools (context leaking between requests).

**Subtasks:**
- 4.5.1 Create `PaymentContext` stored in `ThreadLocal`: `userId`, `paymentId`, `startTime`
- 4.5.2 Show correct usage: `ThreadLocal.withInitial()`, always `remove()` in finally block
- 4.5.3 `v1_leak`: forget to call `remove()` → next request reuses thread → sees stale payment context from previous request
- 4.5.4 Demonstrate: submit 10 requests, observe request 2 sometimes logs payment ID from request 1
- 4.5.5 Fix: always clear in finally, or use `InheritableThreadLocal` where child thread context is needed
- 4.5.6 Show how MDC (used in Task 1.4) is built on ThreadLocal — and why the same pitfall applies

**Learning Outcome:** ThreadLocal leaks are a real production bug — they cause security issues (user A sees user B's data) and incorrect behavior. Must always clean up in finally.

**Concurrency Concept:** `ThreadLocal`, thread pool context reuse, memory leaks.

**Complexity:** S

---

### Task 4.6 — [BROKEN → FIXED] Thread Starvation from Blocking HTTP Call

**Description:** The payment gateway call blocks for 10 seconds. With 200 Tomcat threads and 201 concurrent requests, the 201st request times out. The server is effectively dead.

**Subtasks:**
- 4.6.1 Configure `server.tomcat.threads.max=20`, `GATEWAY_DELAY_MS=10000`
- 4.6.2 Send 21 concurrent requests → observe 21st waits or times out
- 4.6.3 Dump threads: `jcmd <pid> Thread.print` → all 20 threads in `WAITING` on socket read
- 4.6.4 Fix A — `@Async` with dedicated executor: offload gateway call to `payment-io-pool` (virtual thread pool), return immediately from HTTP handler
- 4.6.5 Fix B — `spring.threads.virtual.enabled=true`: Tomcat uses virtual threads, parking on socket doesn't consume OS thread
- 4.6.6 Show in Grafana: `thread.pool.active` drops to near 0 with virtual threads (OS threads are reused)

**Learning Outcome:** Thread starvation is the most common production incident for I/O-heavy services. Virtual threads (Java 21) eliminate the problem for most cases. For older Java, @Async with a bounded executor is the fix.

**Concurrency Concept:** Thread starvation, virtual threads, `@Async`, I/O-bound blocking.

**Complexity:** M

---

## EPIC 5: Notification Service — Kafka + Backpressure

**Goal:** Build the Notification Service and demonstrate how slow consumers cause Kafka lag and how to control backpressure.

---

### Task 5.1 — Notification Service + Kafka Consumer

**Description:** Baseline notification service consuming from multiple topics and dispatching to mock email/SMS/push channels.

**Subtasks:**
- 5.1.1 Create `NotificationJpaEntity`, `NotificationRepository`
- 5.1.2 Create `@KafkaListener` for `payments.authorized`, `orders.cancelled` → create notification records
- 5.1.3 Create `MockEmailChannel`, `MockSmsChannel`, `MockPushChannel` with configurable delay
- 5.1.4 Integration test with Testcontainers Kafka

**Complexity:** M

---

### Task 5.2 — [BROKEN → FIXED] Consumer Lag from Slow Processing

**Description:** The SMS channel takes 2 seconds per message. The producer sends 100 messages/second. Consumer falls behind by 100 messages every second — lag grows unboundedly.

**Subtasks:**
- 5.2.1 `v1_synchronous_consumer`: Kafka consumer thread calls `smsChannel.send()` directly → 2s per message → 100-msg lag grows to infinity
- 5.2.2 Observe in Grafana: `kafka.consumer.lag` rising continuously
- 5.2.3 `v2_async_dispatch`: consumer thread reads message, submits to async executor, returns immediately
- 5.2.4 Problem: unbounded async executor queue → OOM if SMS channel stays slow indefinitely
- 5.2.5 `v3_bounded_queue`: `ThreadPoolExecutor` with `capacity=500`, `CallerRunsPolicy` → backpressure applied to consumer thread when queue is full
- 5.2.6 Show `CallerRunsPolicy` in action: when queue is full, the Kafka consumer thread itself processes the task — effectively pausing offset commit until queue drains
- 5.2.7 Tune `max.poll.records=10` to reduce batch size — consumer fetches fewer messages per poll, giving processing time to catch up

**Learning Outcome:** The consumer thread is precious — never block it with slow processing. But unbounded async dispatch trades lag for OOM. Bounded queue + CallerRunsPolicy is the production pattern. `max.poll.records` tuning controls how much work you commit to per poll cycle.

**Concurrency Concept:** Backpressure, `ThreadPoolExecutor` rejection policies, bounded queues, Kafka consumer concurrency.

**Complexity:** M

---

### Task 5.3 — Dead Letter Queue (DLQ)

**Description:** After 3 failed notification delivery attempts, the notification moves to a DLQ topic. A separate consumer monitors the DLQ and alerts on accumulation.

**Subtasks:**
- 5.3.1 Implement retry counting in notification processor: if `attempt_count >= max_attempts`, publish to `notifications.dlq`
- 5.3.2 Create `DlqMonitorConsumer` that reads from `notifications.dlq` and logs alerts + increments `notifications.dead_letter.total` counter
- 5.3.3 Create Grafana alert: `notifications.dead_letter.total` > 100 → warning
- 5.3.4 Simulate: set `MockSmsChannel` to always fail → watch DLQ fill up → observe Grafana alert

**Learning Outcome:** DLQ is the production pattern for handling poison messages. Without a DLQ, one bad message can block the consumer (if using sequential processing) or retry forever (wasting resources).

**Concurrency Concept:** Kafka consumer error handling, DLQ pattern, backpressure.

**Complexity:** M

---

### Task 5.4 — [BROKEN → FIXED] Kafka Consumer Thread Pool Exhaustion

**Description:** The notification deserializer makes a blocking external call (fetches user preferences from another service). This blocks the Kafka consumer thread — all partitions assigned to this consumer stop making progress.

**Subtasks:**
- 5.4.1 Add `BlockingUserPreferenceFetcher` called during deserialization/preprocessing
- 5.4.2 Observe: Kafka consumer lag grows for ALL partitions even though only one message is slow
- 5.4.3 Fix: move the blocking call out of the consumer thread into an async step using `CompletableFuture`
- 5.4.4 Alternative fix: increase `concurrency` in `@KafkaListener` → more consumer threads → more resilience to slow messages

**Complexity:** M

---

### Task 5.5 — Retry Queue with Exponential Backoff Headers

**Description:** Implement Kafka-native retry queues using message headers to track attempt count and schedule next retry time.

**Subtasks:**
- 5.5.1 Create retry topics: `notifications.retry.5s`, `notifications.retry.25s`, `notifications.retry.125s`
- 5.5.2 On failure: calculate next delay, publish to appropriate retry topic with header `retry-after: <timestamp>`
- 5.5.3 Retry consumers only process messages after `retry-after` timestamp (sleep if early — but careful: this blocks the consumer thread; use pause/resume instead)
- 5.5.4 After max retries, route to DLQ

**Learning Outcome:** Kafka retry queues are the standard production pattern for retry with backoff. Spring Kafka's `RetryableTopic` annotation automates this pattern.

**Complexity:** M

---

## EPIC 6: Load Testing + Production Debugging

**Goal:** Tie everything together by generating realistic load, reading thread dumps, analyzing metrics, and identifying bottlenecks.

---

### Task 6.1 — Gatling Simulation: 10,000 Concurrent Order Creations

**Description:** Write a Gatling simulation that ramps up to 10,000 virtual users creating orders concurrently. Observe where the system breaks first.

**Subtasks:**
- 6.1.1 Write `OrderCreationSimulation.scala`: ramp from 0 to 10,000 VUs over 60s, each VU creates 1 order
- 6.1.2 Run simulation: `mvn gatling:test -pl load-test -Dsimulation=OrderCreationSimulation`
- 6.1.3 Identify bottleneck #1: HikariCP connection pool exhausted → `SQLTransientConnectionException`
- 6.1.4 Fix: tune `maximum-pool-size` to match concurrent order creation rate
- 6.1.5 Identify bottleneck #2: Kafka producer queue fills up → `TimeoutException`
- 6.1.6 Document: at what VU count does each bottleneck appear?
- 6.1.7 Generate Gatling HTML report: analyze P99, error distribution, throughput over time

**Learning Outcome:** Real systems fail in multiple places simultaneously. Fixing one bottleneck reveals the next. This is normal — production performance tuning is iterative.

**Complexity:** L

---

### Task 6.2 — Gatling Simulation: Flash Sale

**Description:** Simulate a flash sale: 5,000 users simultaneously try to buy the last 10 units of a product.

**Subtasks:**
- 6.2.1 Write `FlashSaleSimulation.scala`: pre-seed inventory with 10 units, 5,000 VUs all fire simultaneously (no ramp)
- 6.2.2 Run with v1_broken strategy → observe DB shows negative inventory
- 6.2.3 Run with v5_redis strategy → exactly 10 orders confirmed, 4,990 rejected gracefully (HTTP 409 Conflict)
- 6.2.4 Measure throughput and latency for each strategy
- 6.2.5 Capture Gatling HTML report, document strategy comparison

**Complexity:** L

---

### Task 6.3 — Thread Dump Analysis Guide

**Description:** Write a practical guide to reading thread dumps. Use real thread dumps generated during load tests.

**Subtasks:**
- 6.3.1 Generate thread dump during thread pool exhaustion scenario: `jcmd <pid> Thread.print > dumps/exhaustion-dump.txt`
- 6.3.2 Generate thread dump during deadlock: `jcmd <pid> Thread.print > dumps/deadlock-dump.txt`
- 6.3.3 Generate thread dump during healthy state: `dumps/healthy-dump.txt`
- 6.3.4 Write analysis guide in `docs/thread-dump-analysis.md` explaining:
  - Thread states: RUNNABLE, BLOCKED, WAITING, TIMED_WAITING
  - How to spot thread pool exhaustion (all threads same stack frame)
  - How to spot deadlock (JVM reports "Found 1 deadlock" at bottom)
  - How to spot lock contention (many BLOCKED threads waiting for same monitor)
- 6.3.5 Annotate the real dump files with explanatory comments

**Complexity:** M

---

### Task 6.4 — Slow Query Analysis

**Description:** Identify queries that slow down under load, analyze with `EXPLAIN ANALYZE`, and fix with indexes or query rewrites.

**Subtasks:**
- 6.4.1 Enable PostgreSQL slow query log: `log_min_duration_statement = 100` (log queries > 100ms)
- 6.4.2 Run order creation load test, collect slow query log
- 6.4.3 For each slow query, run `EXPLAIN ANALYZE` and explain the execution plan
- 6.4.4 Demonstrate full table scan on `payments WHERE status='PENDING'` without partial index
- 6.4.5 Add partial index, re-run → sequential scan replaced by index scan, query time drops 100x
- 6.4.6 Show HikariCP connection pool metrics in Grafana: `db.connection.pool.active` approaching `maximum-pool-size` is the precursor to `SQLTransientConnectionException`

**Complexity:** M

---

### Task 6.5 — Grafana Dashboard — Full Observability

**Description:** Build out the complete Grafana dashboards so that every production scenario from `production_scenarios.md` is visible in metrics.

**Subtasks:**
- 6.5.1 Dashboard: `overview.json` — order rate, payment success rate, error rate, P50/P95/P99 latency
- 6.5.2 Dashboard: `jvm.json` — heap used, GC pause count, GC pause duration, thread count, thread pool active/queued/rejected
- 6.5.3 Dashboard: `kafka.json` — consumer lag per topic/group, messages per second, partition distribution
- 6.5.4 Dashboard: `database.json` — HikariCP active/idle/total connections, query duration by operation, lock wait events
- 6.5.5 Dashboard: `redis.json` — hit ratio, memory used, connected clients, evicted keys
- 6.5.6 Configure Grafana alerts for critical thresholds (see ProjectPlan.md monitoring section)

**Complexity:** M

---

### Task 6.6 — Redis Cache Stampede Simulation + Fix

**Description:** Demonstrate the cache stampede problem and implement two fixes: probabilistic early expiration and mutex-based cache population.

**Subtasks:**
- 6.6.1 `v1_stampede`: cache TTL=5s, load test at 1000 req/s → every 5s, all requests hit DB simultaneously (DB CPU spikes to 100%)
- 6.6.2 Observe in Grafana: `cache.hit.ratio` drops to 0 every 5s, DB query duration spikes
- 6.6.3 `v2_probabilistic`: XFetch algorithm — each reader probabilistically refreshes early based on remaining TTL
- 6.6.4 `v3_mutex_lock`: on cache miss, only one thread (per Redis key) fetches from DB; others wait briefly then get cache hit
- 6.6.5 Document tradeoffs: probabilistic gives slightly stale data occasionally; mutex adds latency on cache miss

**Complexity:** M
