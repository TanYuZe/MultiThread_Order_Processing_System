# High-Scale Order & Payment Processing Simulation Platform
## Project Plan — Production Backend Engineering Mentorship

---

## Vision & Learning Objectives

This project is a deliberate, structured learning environment designed to transform a mid-level backend engineer into a production-ready engineer who can:

- Diagnose and fix concurrency bugs under production load
- Design thread-safe systems from first principles
- Reason about distributed system failures before they happen
- Read thread dumps, analyze GC logs, and interpret Prometheus metrics
- Understand the WHY behind every architectural decision

The method is intentional exposure: every concurrency concept is first demonstrated with a **broken implementation** that fails under realistic load, then fixed with a **production-grade solution**, benchmarked, and monitored.

---

## Domain Overview

The system simulates a high-volume transactional backend — conceptually a hybrid of Stripe (payments) and Amazon Checkout (orders + inventory). Five bounded contexts collaborate through events:

### 1. Order Service
Manages the lifecycle of customer orders. Responsible for:
- Creating orders with idempotency guarantees
- Transitioning order status through a state machine (PENDING → CONFIRMED → CANCELLED | FAILED)
- Publishing domain events to trigger downstream processing
- Batch report generation (ForkJoinPool, parallel streams)

**Primary concurrency challenge:** Non-atomic status transitions cause lost updates under concurrent modification. Solved with optimistic locking (`@Version`) and retry.

### 2. Payment Service
Handles payment authorization with external gateway integration. Responsible for:
- Authorizing payments asynchronously (CompletableFuture pipelines)
- Enforcing idempotency — duplicate requests must never double-charge
- Retrying failed authorizations with exponential backoff + jitter
- Circuit breaking when the external gateway is degraded

**Primary concurrency challenge:** Retry storms collapse the external gateway when it recovers. Idempotency key races allow duplicate charges. Thread starvation occurs when blocking HTTP calls land on the Tomcat pool.

### 3. Inventory Service
Manages product stock with strict oversell prevention. Responsible for:
- Reserving stock at order creation time
- Releasing stock when orders are cancelled
- Supporting flash sale scenarios (high contention, microsecond windows)
- Caching availability in Redis

**Primary concurrency challenge:** The classic read-modify-write race: two threads read quantity=1, both deduct, final quantity is -1. Progressively fixed through synchronized → pessimistic lock → optimistic lock → Redis atomic counter.

### 4. Notification Service
Delivers asynchronous notifications across channels (email, SMS, push). Responsible for:
- Consuming Kafka events and dispatching notifications
- Managing retry queues with backpressure awareness
- Routing to dead-letter queue after max attempts
- Rate limiting per channel

**Primary concurrency challenge:** Slow delivery causes consumer lag. Without backpressure control, the consumer thread pool exhausts and the JVM runs out of memory buffering pending notifications.

### 5. Metrics Service
Aggregates observability signals into a queryable dashboard. Responsible for:
- Exposing thread pool stats (active, queued, rejected)
- Tracking Kafka consumer lag per topic/partition
- Surfacing HikariCP connection pool utilization
- Providing a `/metrics` endpoint for Prometheus scrape

---

## Concurrency Curriculum Map

Every concept below has a corresponding broken implementation in a clearly marked package (`v1_broken`) and a fixed implementation (`v2_fixed`) with a comparative benchmark.

| Concept | Service | Broken Pattern | Fix | Trigger Command |
|---|---|---|---|---|
| **Race Condition** | Inventory | `read qty → if qty > 0 → qty--` without lock | `SELECT FOR UPDATE` / `@Version` | `FlashSaleSimulation.scala` |
| **Check-Then-Act** | Order | `if (status == PENDING) { status = CONFIRMED }` | Optimistic lock + retry | `ConcurrentOrderUpdateTest` |
| **Deadlock** | Payment + Inventory | Lock payment → lock inventory (T1); lock inventory → lock payment (T2) | Consistent lock ordering | `DeadlockSimulation.scala` |
| **Thread Starvation** | Payment | Blocking HTTP call on Tomcat thread | Virtual thread / async executor | `SlowGatewaySimulation.scala` |
| **Visibility Bug** | Order | `boolean isRunning = true` in worker, no `volatile` | `volatile boolean isRunning` | `VisibilityBugTest` |
| **Non-thread-safe Counter** | Metrics | `int requestCount++` in singleton | `LongAdder` / `AtomicLong` | `ConcurrentCounterTest` |
| **synchronized** | Inventory | Demonstration of intrinsic locking, monitor contention | Baseline comparison | Unit test |
| **ReentrantLock** | Inventory | `synchronized` causes priority inversion | `ReentrantLock` with `tryLock(timeout)` | Unit test |
| **ReadWriteLock** | Inventory | `synchronized` on read-heavy path bottlenecks writes | `ReentrantReadWriteLock` | Load test with 90% reads |
| **ThreadLocal** | All | `correlationId` lost across async hops | `MDC` + explicit propagation to child threads | Integration test |
| **CompletableFuture** | Payment | Sequential blocking calls: auth → capture → notify | `thenComposeAsync` pipeline with timeout | `PaymentPipelineBenchmark` |
| **Virtual Threads** | Payment | Platform threads blocked on I/O, pool exhausts at 200 connections | `Thread.ofVirtual()` / `spring.threads.virtual.enabled=true` | `VirtualThreadSimulation.scala` |
| **ForkJoinPool** | Order | Sequential batch processing of 10k orders (1 thread) | `ForkJoinPool` with `RecursiveTask` + parallel streams | `BatchBenchmark` |
| **Backpressure** | Notification | Consumer processes 100/s, producer sends 10k/s → OOM | `max.poll.records` + async dispatch + bounded queue | Kafka producer script |
| **Optimistic Locking** | Order, Inventory | No versioning → lost updates | `@Version` + `ObjectOptimisticLockingFailureException` retry | `ConcurrentUpdateTest` |
| **Pessimistic Locking** | Inventory | Flash sale: optimistic lock causes too many retries | `@Lock(PESSIMISTIC_WRITE)` + `SELECT FOR UPDATE SKIP LOCKED` | `FlashSaleSimulation.scala` |
| **Atomic Operations** | Metrics | `synchronized` increment is too coarse | `LongAdder` for high-contention counters | Benchmark |
| **Distributed Lock** | Inventory | Multiple service instances both reserve same stock | Redis `SETNX` / Redisson `RLock` | Multi-instance load test |

---

## Parallelism Concepts

### CPU-Bound vs IO-Bound

The project deliberately demonstrates both failure modes:

**IO-Bound (Payment Service):**
Platform threads block during HTTP calls to the payment gateway. With 200 Tomcat threads and a 30s gateway response time, the server is unavailable after 200 concurrent requests. Virtual threads (Java 21) eliminate this ceiling — millions of virtual threads can park on IO without consuming OS threads.

```
Platform threads: 200 concurrent users → server dead
Virtual threads:  200,000 concurrent users → server healthy
```

**CPU-Bound (Order Batch Processing):**
Generating a report over 1 million historical orders is CPU-bound. ForkJoinPool splits the dataset recursively, processes chunks in parallel, and merges results. Parallel streams provide a simpler API for the same parallelism pattern.

```java
// Bad: sequential
orders.stream().map(this::enrich).collect(toList());  // 45 seconds

// Better: parallel stream (uses common ForkJoinPool)
orders.parallelStream().map(this::enrich).collect(toList());  // 8 seconds

// Best: dedicated ForkJoinPool to avoid starving other tasks
var pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors() - 1);
pool.submit(() -> orders.parallelStream().map(this::enrich).collect(toList())).get();
```

### Worker Pools

The system intentionally demonstrates what happens when you misconfigure thread pools:

| Pool | Default Size | Demonstrates |
|---|---|---|
| Tomcat (HTTP) | 200 platform / unlimited virtual | Starvation vs virtual thread advantage |
| Async Task Executor | core=10, max=50, queue=100 | Queue overflow → `TaskRejectedException` |
| Kafka Consumer | 1 per partition | Blocking consumer → lag accumulation |
| ForkJoinPool (batch) | `availableProcessors - 1` | CPU parallelism ceiling |

---

## Production Failure Scenarios Overview

| # | Scenario | Service | Trigger | Observable Signal |
|---|---|---|---|---|
| 1 | Thread Pool Exhaustion | Payment | Slow DB query | 503 errors, all threads WAITING |
| 2 | Inventory Oversell | Inventory | Flash sale | Negative stock in DB |
| 3 | Payment Deadlock | Payment + Inventory | Concurrent auth+reserve | `LockAcquisitionException` |
| 4 | Kafka Retry Storm | Notification | SMS API down + recovery | Consumer lag → DB pool exhaustion |
| 5 | Redis Cache Stampede | Inventory | Cache TTL expiry | DB CPU 100% |
| 6 | GC Pressure | Order | Large batch in-memory | STW pause → P99 spike |
| 7 | Visibility Bug | Order | Non-volatile flag | Worker loop never exits |
| 8 | Slow API Cascade | Payment | 30s gateway response | Cascading 503s |
| 9 | Idempotency Race | Payment | Concurrent retries | Duplicate charge |
| 10 | Task Starvation | Notification | Shared thread pool | Notification delay unbounded |

Full runbooks with logs, metrics, thread dumps, and fixes are in `production_scenarios.md`.

---

## Scaling Considerations

### Horizontal Scaling
All services are stateless — no in-process session state. Session affinity is not required. Horizontal scaling is achieved by adding instances behind the API Gateway.

**Stateless requirements enforced by design:**
- No `static` mutable state in service beans
- Correlation IDs generated per request, not stored on thread
- Idempotency state stored in Redis / PostgreSQL, not JVM heap

### Kafka Partitioning Strategy
- `orders.*` topics: partitioned by `orderId` — all events for one order go to one partition, guaranteeing consumer ordering
- `notifications.*` topics: partitioned by `userId` — notifications for one user arrive in order
- `payments.*` topics: partitioned by `orderId`
- Partition count: 12 (allows up to 12 consumer instances per group)

### Redis Clustering
- Inventory cache uses Redis cluster mode — stock counts are distributed, no single-key hotspot
- Idempotency keys use consistent hashing — same key always goes to same shard
- Distributed locks use `Redisson` with watchdog renewal — no clock-skew-related lock expiry

### Database Connection Pool Sizing
Rule of thumb: `pool_size = (cores * 2) + effective_spindle_count`
- 4-core host, SSD storage → `pool_size = 9`
- Intentionally misconfigured at `pool_size = 200` in the broken scenario to demonstrate connection starvation

---

## Monitoring Strategy

### Structured Logging
All logs are emitted as JSON via Logback + `logstash-logback-encoder`. Every log line includes:
- `correlationId` — propagated via MDC across thread hops
- `threadName` and `threadType` (virtual / platform)
- `userId`, `orderId`, `paymentId` (where applicable)
- `durationMs` for timed operations
- `operation` — machine-readable operation name

This format makes logs directly queryable in ELK / Grafana Loki without parsing.

### Metrics (Micrometer → Prometheus)
Every service exposes `/actuator/prometheus`. Key metrics:

| Metric | Alert Threshold |
|---|---|
| `thread.pool.queue.size` > 80 | Page — task queue near capacity |
| `kafka.consumer.lag` > 10,000 | Page — consumer falling behind |
| `db.connection.pool.active` > 18 (of 20) | Warn — connection pool near exhaustion |
| `orders.processing.duration` p99 > 2s | Warn — latency degradation |
| `inventory.reservation.failures` rate > 5% | Warn — potential oversell |
| `payment.authorization.attempts{status=failed}` > 10% | Page — payment failure spike |

### Grafana Dashboards
Located in `docker/grafana/provisioning/dashboards/`:
- `overview.json` — system health at a glance (all services)
- `jvm.json` — heap, GC pauses, thread counts
- `kafka.json` — consumer lag, throughput, partition distribution
- `database.json` — HikariCP pool, query latency, lock wait time

### Thread Dump Analysis
When thread pool exhaustion is suspected:
```bash
# Get PID
jcmd

# Print thread dump (no signal, works on containers)
jcmd <pid> Thread.print > thread-dump.txt

# Look for patterns:
# - Many threads in WAITING on java.util.concurrent.locks or socket
# - All threads sharing same stack frame → shared bottleneck
# - BLOCKED threads → lock contention or deadlock

# Detect deadlocks specifically:
grep -A5 "BLOCKED" thread-dump.txt
grep "deadlock" thread-dump.txt  # JVM reports deadlocks at the bottom
```

---

## Project Timeline Estimate

| Epic | Description | Estimate |
|---|---|---|
| 1 | Foundation & Infrastructure | 2 weeks |
| 2 | Order Service + Race Conditions | 2 weeks |
| 3 | Inventory Service + Locking | 2 weeks |
| 4 | Payment Service + Async | 2 weeks |
| 5 | Notification Service + Kafka | 1.5 weeks |
| 6 | Load Testing + Debugging | 1.5 weeks |
| **Total** | | **~11 weeks** |

This is a learning project — the timeline assumes active learning, not feature production speed. Each task should be read, implemented, broken deliberately, observed, fixed, and verified before moving on.

---

## Repository Structure

```
MultiThread_Order_Processing_System/
├── ProjectPlan.md                  ← this file
├── Architecture.md
├── database_schema.md
├── taskplan.md
├── production_scenarios.md
│
├── pom.xml                         ← parent POM (Java 21, Spring Boot 3.3.x)
├── platform-common/                ← shared: DTOs, exceptions, logging, correlation ID
├── platform-domain/                ← pure domain model (no Spring deps)
├── order-service/
├── payment-service/
├── inventory-service/
├── notification-service/
├── metrics-service/
├── api-gateway/
├── load-test/                      ← Gatling simulations
└── docker/
    ├── docker-compose.yml
    ├── prometheus/
    │   └── prometheus.yml
    └── grafana/
        └── provisioning/
```
