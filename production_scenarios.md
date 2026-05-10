# Production Scenarios — High-Scale Order & Payment Processing Simulation Platform

These are production-grade incident runbooks. Each scenario describes a real failure mode that has occurred in high-traffic systems. For each scenario: how to reproduce it in this project, what the symptoms look like, what metrics and logs to inspect, the root cause, and the fix.

---

## Scenario 1: Thread Pool Exhaustion

### Overview
All threads in the Tomcat (or async executor) thread pool are blocked waiting for a slow database query. New requests queue up, then time out with 503 Service Unavailable.

### How to Reproduce
```bash
# 1. Set HikariCP max pool size to 5 (in application.yml):
#    spring.datasource.hikari.maximum-pool-size: 5
#    spring.datasource.hikari.connection-timeout: 3000

# 2. Add artificial slow query to OrderRepository (ONLY in v1_broken):
#    SELECT pg_sleep(10);  -- 10 second block

# 3. Send 10 concurrent order creation requests:
curl -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" \
  -d '{"userId":"...","items":[...],"idempotencyKey":"..."}' &
# Repeat 10 times
```

### Symptoms
- HTTP requests time out after 3 seconds with `503 Service Unavailable`
- Error response body: `{"error": "Service temporarily unavailable"}`
- Response times spike from 50ms to 3000ms (connection timeout value)

### Logs to Inspect
```json
{"level":"ERROR","service":"order-service","threadName":"http-nio-8081-exec-1",
 "message":"Unable to acquire JDBC Connection",
 "exception":"com.zaxxer.hikari.pool.HikariPool$PoolInitializationException: 
   Connection is not available, request timed out after 3000ms"}
```

```json
{"level":"WARN","service":"order-service","threadName":"HikariPool-OrderService housekeeper",
 "message":"HikariPool-OrderService - Thread starvation or clock leap detected (true)."}
```

### Metrics to Inspect
```promql
# Prometheus queries to run during the incident:

# Active DB connections (should never hit maximum-pool-size):
hikaricp_connections_active{pool="HikariPool-OrderService"}

# Connection acquisition time (spikes when pool is exhausted):
hikaricp_connections_acquire_seconds{quantile="0.99"}

# Pending connection requests (threads waiting for a connection):
hikaricp_connections_pending{pool="HikariPool-OrderService"}

# API error rate (rises sharply):
rate(http_server_requests_seconds_count{status="503"}[1m])
```

### Grafana Panels
- "HikariCP Active Connections" panel → flatlines at 5 (pool max)
- "API P99 Latency" panel → spikes to 3000ms (timeout value)
- "Error Rate by Status" panel → 503s appear

### Thread Dump Analysis
```bash
jcmd $(pgrep -f order-service) Thread.print > thread-dump-exhaustion.txt
grep -A 20 "WAITING" thread-dump-exhaustion.txt
```

Expected output — ALL threads waiting for a DB connection:
```
"http-nio-8081-exec-1" #23 daemon prio=5 os_prio=0 tid=0x00007f... nid=0x... waiting on condition
   java.lang.Thread.State: WAITING (parking)
      at sun.misc.Unsafe.park(Native Method)
      at com.zaxxer.hikari.util.ConcurrentBag.borrow(ConcurrentBag.java:...)
      at com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:...)
      at com.zaxxer.hikari.HikariDataSource.getConnection(HikariDataSource.java:...)
```

All 5 active threads show this same stack → pool is exhausted, all callers are parked.

### Root Cause
```
HikariCP max connections = 5
Slow query holds connection for 10 seconds
After 5 concurrent requests: pool exhausted
Requests 6-N wait up to connection-timeout (3s), then throw
```

The fundamental problem: connection hold time × concurrent requests > pool size.

### Fix
```yaml
# application.yml — production sizing:
spring:
  datasource:
    hikari:
      maximum-pool-size: 20        # formula: (cores * 2) + 1
      minimum-idle: 5
      connection-timeout: 3000     # fail fast if pool is truly exhausted
      idle-timeout: 600000
      max-lifetime: 1800000

# Fix the slow query (add index, optimize JOIN, add pagination):
# EXPLAIN ANALYZE SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC
# → add index: CREATE INDEX idx_orders_user_id ON orders(user_id)
```

```java
// Additional fix: use virtual threads (Java 21) so blocked threads don't consume OS threads:
// application.yml:
spring.threads.virtual.enabled: true
// Now "blocking" threads are virtual — they park cheaply, OS thread is reused
```

### Verify Fix
```bash
# Re-run 10 concurrent requests:
for i in {1..10}; do curl -s -o /dev/null -w "%{http_code}\n" -X POST ... & done
# All should return 202, not 503

# Grafana: hikaricp_connections_active should stay well below maximum-pool-size
```

---

## Scenario 2: Inventory Oversell Race Condition

### Overview
Two (or more) users simultaneously purchase the last unit of a product. Without proper locking, both requests read `available=1`, both proceed to reserve, and inventory drops to -1. Physical goods cannot be manufactured retroactively.

### How to Reproduce
```bash
# 1. Seed inventory: product X with total_quantity=1, reserved_quantity=0
# 2. Use the v1_broken inventory service (no locking)
# 3. Run FlashSaleSimulation with 100 concurrent buyers:
mvn gatling:test -pl load-test -Dsimulation=FlashSaleSimulation \
  -Dproduct=X -Dquantity=1 -Dusers=100

# 4. Check DB after:
psql -c "SELECT total_quantity, reserved_quantity FROM inventory WHERE product_id='X'"
# Expected (broken): reserved_quantity = 2 (or more), total_quantity = 1 → oversold!
```

### Symptoms
- Multiple `ORDER_CONFIRMED` Kafka events for the same product when stock = 1
- `reserved_quantity > total_quantity` in the inventory table
- Customer support tickets: "I ordered successfully but item is out of stock"

### Logs to Inspect
```json
{"level":"INFO","service":"inventory-service","threadName":"kafka-consumer-1-C-1",
 "productId":"X","operation":"RESERVE_STOCK","reservedQty":1,"available":0,
 "message":"Stock reserved successfully"}
```
```json
{"level":"INFO","service":"inventory-service","threadName":"kafka-consumer-1-C-2",
 "productId":"X","operation":"RESERVE_STOCK","reservedQty":1,"available":0,
 "message":"Stock reserved successfully"}
```
Both threads see `available=0` AFTER their reservation succeeds — but both also succeed. That means two reservations occurred for 1 available unit.

### Metrics to Inspect
```promql
# Reservation success rate per product (should not exceed total_quantity):
increase(inventory_reservation_success_total{productId="X"}[1m])
# → shows 2 when stock was 1 (impossible)

# Constraint violation events:
increase(inventory_constraint_violations_total[1m])
# → should be 0 (the CHECK constraint prevents the final DB update from going negative)
# → if it's > 0, data integrity is at risk
```

### Root Cause
```sql
-- Both threads execute this simultaneously:
T1: SELECT total_quantity=1, reserved_quantity=0  -- available=1
T2: SELECT total_quantity=1, reserved_quantity=0  -- available=1

-- Both see available > 0, both proceed:
T1: UPDATE inventory SET reserved_quantity=1 WHERE product_id='X'  -- succeeds
T2: UPDATE inventory SET reserved_quantity=1 WHERE product_id='X'  -- also succeeds!
-- Neither thread knows the other thread ran. Both commit successfully.
-- PostgreSQL CHECK constraint (total >= reserved) is NOT violated because
-- each individual UPDATE is within bounds — the race happens in the application layer.
```

### Fix (progression — see Task 3.2)
```java
// v3 — Pessimistic locking (best for flash sales):
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT i FROM InventoryJpaEntity i WHERE i.productId = :productId")
Optional<InventoryJpaEntity> findByProductIdForUpdate(@Param("productId") UUID productId);

// SQL: SELECT ... FROM inventory WHERE product_id=? FOR UPDATE
// T2 blocks at the SELECT until T1 commits, then reads the updated reserved_quantity
```

### Verify Fix
```bash
# With pessimistic locking:
mvn gatling:test -pl load-test -Dsimulation=FlashSaleSimulation \
  -Dproduct=X -Dquantity=1 -Dusers=100 -Dstrategy=pessimistic

# Exactly 1 SUCCESS, 99 CONFLICT responses
psql -c "SELECT total_quantity, reserved_quantity FROM inventory WHERE product_id='X'"
# Expected: total_quantity=1, reserved_quantity=1 → no oversell
```

---

## Scenario 3: Deadlock in Payment Processing

### Overview
Two payment processing threads lock resources in opposite orders. Thread 1 holds inventory lock, waits for payment lock. Thread 2 holds payment lock, waits for inventory lock. PostgreSQL detects the cycle and kills one transaction.

### How to Reproduce
```bash
# Use the DeadlockPaymentService (v1_broken):
# It processes multi-item orders and locks inventory rows in arbitrary (non-deterministic) order.

# Run DeadlockSimulation with 2 concurrent orders that share overlapping products:
mvn gatling:test -pl load-test -Dsimulation=DeadlockSimulation
```

### Symptoms
- Intermittent `500 Internal Server Error` on order creation
- Error: `could not serialize access due to concurrent update` or `deadlock detected`
- Transaction success rate drops from 100% to ~50% under concurrent load

### Logs to Inspect
```json
{"level":"ERROR","service":"inventory-service","threadName":"async-exec-2",
 "orderId":"ord-001","operation":"RESERVE_STOCK",
 "message":"Transaction rolled back due to deadlock",
 "exception":"org.springframework.dao.CannotAcquireLockException: could not obtain lock on row in relation 'inventory'; 
   nested exception: org.postgresql.util.PSQLException: ERROR: deadlock detected
   Detail: Process 12345 waits for ShareLock on transaction 67890; blocked by process 11111.
   Process 11111 waits for ShareLock on transaction 12345; blocked by process 12345.
   Hint: See server log for query details."}
```

### Metrics to Inspect
```promql
# Deadlock event counter (must be 0 in healthy systems):
increase(inventory_reservation_failures_total{reason="DEADLOCK"}[5m])

# Transaction rollback rate:
increase(spring_transaction_rollbacks_total[1m])
```

### PostgreSQL Server Log
```
2026-05-10 14:23:11 UTC [12345]: ERROR:  deadlock detected
2026-05-10 14:23:11 UTC [12345]: DETAIL:  Process 12345 waits for ShareLock on transaction 67890;
    blocked by process 11111.
    Process 11111 waits for ShareLock on transaction 12345; blocked by process 12345.
2026-05-10 14:23:11 UTC [12345]: HINT:  See server log for query details.
2026-05-10 14:23:11 UTC [12345]: CONTEXT:  while updating tuple (0,17) in relation "inventory"
```

### Root Cause
```
Order #1 (products: A, B) — Thread T1:
  1. LOCK inventory row for product A  ✓
  2. Trying to LOCK inventory row for product B  ← BLOCKED (T2 holds B)

Order #2 (products: B, A) — Thread T2:
  1. LOCK inventory row for product B  ✓
  2. Trying to LOCK inventory row for product A  ← BLOCKED (T1 holds A)

Circular dependency → deadlock.
PostgreSQL detects cycle after deadlock_timeout (default 1s) and kills T2.
T2's transaction is rolled back → 500 error.
```

### Fix
```java
// Always lock inventory rows in a deterministic order (sort product IDs):
public void reserveMultipleProducts(List<UUID> productIds, ...) {
    // Sort product IDs so all threads lock in the same order
    var sortedIds = productIds.stream()
        .sorted()  // UUID natural ordering — deterministic
        .collect(toList());

    for (UUID productId : sortedIds) {
        inventoryRepo.findByProductIdForUpdate(productId);  // lock in sorted order
        // ... reserve stock
    }
}
```

Alternative: use `SELECT ... FOR UPDATE SKIP LOCKED` + retry — each thread grabs whichever row is available, skips locked ones.

### Verify Fix
```bash
mvn test -pl inventory-service -Dtest=DeadlockTest
# → 0 CannotAcquireLockException in 1000 concurrent reservation attempts
```

---

## Scenario 4: Kafka Consumer Lag / Retry Storm

### Overview
The external SMS API goes down for 5 minutes. 50,000 notifications accumulate in the retry queue. When the API recovers, all 50,000 retry simultaneously — overloading both the SMS gateway and the application's database connection pool.

### How to Reproduce
```bash
# 1. Set MockSmsChannel failure rate to 100%:
export MOCK_SMS_FAILURE_RATE=1.0

# 2. Generate 50,000 notifications over 5 minutes:
mvn gatling:test -pl load-test -Dsimulation=NotificationLoadSimulation -Dusers=50000

# 3. Restore SMS API (failure rate → 0%):
export MOCK_SMS_FAILURE_RATE=0.0

# 4. Watch the retry storm: all 50,000 retry simultaneously
```

### Symptoms
- Kafka `kafka.consumer.lag` metric spikes to 50,000
- When SMS API recovers: DB CPU → 100%, HikariCP connection pool exhausted
- `SQLTransientConnectionException` in notification-service logs
- SMS API returns 429 (Too Many Requests) as retry storm overwhelms it

### Logs to Inspect
```json
{"level":"WARN","service":"notification-service","threadName":"notification-retry-1",
 "kafka.consumer.lag":48723,"topic":"notifications.retry.5s",
 "message":"Consumer lag is critical — processing is behind production rate"}

{"level":"ERROR","service":"notification-service","threadName":"async-notif-7",
 "notificationId":"notif-001","attempt":1,
 "message":"SMS delivery failed",
 "exception":"SmsGatewayException: 503 Service Unavailable"}

{"level":"ERROR","service":"notification-service","threadName":"async-notif-12",
 "message":"Unable to acquire JDBC Connection after 3000ms — connection pool exhausted"}
```

### Metrics to Inspect
```promql
# Consumer lag per topic (alert at > 10,000):
kafka_consumer_group_lag{topic="notifications.pending", group="notif-svc-grp"}

# Retry attempt rate (spikes during storm):
rate(notification_delivery_attempts_total{status="RETRY"}[1m])

# DB connection pool under retry storm:
hikaricp_connections_active{pool="HikariPool-NotifService"} / 
hikaricp_connections_max{pool="HikariPool-NotifService"}
# → approaches 1.0 during storm
```

### Root Cause
```
Phase 1 (SMS down): notifications fail → retry after 5s → fail again → retry after 25s → ...
Phase 2 (SMS down for 5 min): 50,000 notifications all have retry-after = "5 minutes from failure"
Phase 3 (SMS up): all 50,000 retry-after timestamps expire simultaneously
Phase 4 (storm): 50,000 notifications hit DB to update attempt_count, hit SMS API to deliver
                  → DB pool exhausted, SMS API overloaded → all fail again → storm repeats
```

### Fix
```java
// 1. Exponential backoff with jitter:
private Duration calculateRetryDelay(int attemptCount) {
    long baseDelayMs = 5_000;
    long maxDelayMs = 300_000;  // 5 minutes max
    long exponential = (long) (baseDelayMs * Math.pow(2, attemptCount));
    long capped = Math.min(exponential, maxDelayMs);
    long jitter = ThreadLocalRandom.current().nextLong(capped / 2);  // ±50% jitter
    return Duration.ofMillis(capped + jitter);
}

// 2. Circuit breaker around SMS gateway:
@CircuitBreaker(name = "sms-gateway", fallbackMethod = "smsCircuitBreakerFallback")
public void sendSms(String recipient, String body) {
    smsGatewayClient.send(recipient, body);
}

// 3. Rate limiter on retry consumer (max 100 retries/second):
// spring.kafka.listener.concurrency: 3    ← limit consumer threads
// max.poll.records: 10                    ← limit messages per poll
```

### Verify Fix
```bash
# With jitter + circuit breaker, retry rate should be bounded:
# SMS API down → recovers → retry rate ramps up gradually, NOT instantaneous spike
promql: rate(notification_delivery_attempts_total{status="RETRY"}[1m])
# → should show gradual ramp, not a sudden 50,000/min spike
```

---

## Scenario 5: Redis Cache Stampede

### Overview
The inventory stock cache for a popular product expires. 10,000 concurrent product page views all experience a cache miss simultaneously and hit PostgreSQL — saturating the database.

### How to Reproduce
```bash
# 1. Cache TTL set to 10 seconds (deliberately short for demo)
# 2. Generate 10,000 concurrent inventory reads, timed to align with TTL expiry:
mvn gatling:test -pl load-test -Dsimulation=CacheStampedeSimulation -Dproduct=X

# Watch the 10s mark: DB CPU spikes to 100%
```

### Symptoms
- Every 10 seconds, a brief but severe DB CPU spike
- API P99 latency spikes from 5ms (cache hit) to 500ms+ (DB query) every 10 seconds
- `cache.hit.ratio` drops to 0 every 10 seconds for exactly 1 spike duration

### Logs to Inspect
```json
{"level":"DEBUG","service":"inventory-service","threadName":"http-nio-8083-exec-5",
 "productId":"X","operation":"GET_AVAILABILITY","cacheHit":false,
 "message":"Cache miss — fetching from database"}
```
```
10,000 of these lines appearing in the same second = stampede
```

### Metrics to Inspect
```promql
# Cache hit ratio (drops to 0 during stampede):
rate(cache_gets_total{result="hit",cache="inventory"}[10s]) /
rate(cache_gets_total{cache="inventory"}[10s])

# DB query rate spikes at TTL expiry:
rate(spring_data_repository_invocations_seconds_count{repository="InventoryJpaRepository"}[10s])

# Latency during stampede vs normal:
histogram_quantile(0.99, rate(http_server_requests_seconds_bucket{uri="/api/inventory/*"}[10s]))
```

### Root Cause
```
T=0s:    Cache populated for product X, TTL=10s
T=9.99s: 10,000 requests → cache HIT, 5ms response
T=10.00s: Cache EXPIRES
T=10.00s: 10,000 requests → ALL cache MISS → ALL hit PostgreSQL simultaneously
          PostgreSQL handles 100 queries/s normally, now receives 10,000 queries in <1s
          → connection pool exhausted → SQLTransientConnectionException
T=10.01s: First thread gets DB result, populates cache
T=10.01s: Other 9,999 threads ALSO populate cache (redundant)
T=10.01s: Cache is warm again for another 10s
```

### Fix Option A — Probabilistic Early Expiration (XFetch)
```java
public Optional<InventorySnapshot> getAvailability(UUID productId) {
    var cached = redisTemplate.opsForValue().get("inventory:" + productId);
    if (cached != null) {
        long ttlRemaining = redisTemplate.getExpire("inventory:" + productId, SECONDS);
        double beta = 1.0;  // tuning parameter (1.0 = standard)
        double delta = 0.1; // expected recomputation time in seconds
        // Probabilistically decide to refresh early:
        if (Math.random() <= delta * beta * Math.log(ttlRemaining)) {
            return refreshFromDatabase(productId);  // lucky thread refreshes early
        }
        return Optional.of(cached);  // other threads use cache normally
    }
    return refreshFromDatabase(productId);
}
```

### Fix Option B — Mutex Lock (Simpler, Slightly Higher Latency)
```java
public Optional<InventorySnapshot> getAvailability(UUID productId) {
    var cached = redisTemplate.opsForValue().get("inventory:" + productId);
    if (cached != null) return Optional.of(cached);

    // Acquire distributed lock — only one thread fetches from DB:
    var lock = redissonClient.getLock("cache-refresh:inventory:" + productId);
    try {
        if (lock.tryLock(2, 5, SECONDS)) {
            try {
                // Double-check after acquiring lock (another thread may have populated):
                cached = redisTemplate.opsForValue().get("inventory:" + productId);
                if (cached != null) return Optional.of(cached);
                return refreshFromDatabase(productId);
            } finally {
                lock.unlock();
            }
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
    // Lock timeout → return stale or empty
    return Optional.empty();
}
```

---

## Scenario 6: GC Pressure from Large In-Memory Batch

### Overview
The batch order report loads 1 million order records into a `List<Order>` in the JVM heap. The GC cannot collect quickly enough, causing Stop-the-World pauses of 2-5 seconds. All incoming requests freeze during GC.

### How to Reproduce
```bash
# 1. Set heap size to 512MB (small, to make GC pressure visible quickly):
#    JAVA_OPTS="-Xms512m -Xmx512m -XX:+PrintGCDetails"
# 2. Trigger batch report with 1M orders:
curl -X POST http://localhost:8081/api/orders/report?range=all
# 3. Watch GC log and JVM dashboard in Grafana
```

### Symptoms
- API response times freeze periodically (all requests take 2-5s instead of normal 50ms)
- JVM GC log shows long STW pauses: `[GC pause (G1 Evacuation Pause) 2341ms]`
- Heap usage graph shows sawtooth pattern with slow downward slope (GC cannot keep up)
- Memory usage approaches `-Xmx` value → `OutOfMemoryError: Java heap space`

### Logs to Inspect
```json
{"level":"WARN","service":"order-service","threadName":"scheduling-1",
 "operation":"BATCH_REPORT","recordsLoaded":1000000,
 "message":"Batch report loaded all records into memory — heap pressure likely"}
```

GC log (enable with `-Xlog:gc*:file=gc.log`):
```
[GC(10)] [Eden: 400MB(400MB)->0B(400MB) Survivors: 50MB->50MB Heap: 480MB(512MB)->450MB(512MB)]
[GC(10)] pause time: 2341.2ms  ← 2.3 second STW pause
```

### Metrics to Inspect
```promql
# JVM heap usage (approaching Xmx is bad):
jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"}
# Alert when > 0.8

# GC pause duration (STW):
rate(jvm_gc_pause_seconds_sum{action="end of major GC"}[1m])
# Alert when p99 > 500ms

# GC frequency (too frequent = heap too small):
rate(jvm_gc_pause_seconds_count[1m])
# Alert when > 1/s
```

### Root Cause
```java
// BAD: loads all records at once
public List<OrderSummary> generateReport() {
    List<Order> allOrders = orderRepo.findAll();  // 1M records → ~2GB heap
    return allOrders.stream().map(this::summarize).collect(toList());
}
// At peak: 1M Order objects in memory simultaneously
// G1GC must scan entire heap to collect → long STW
```

### Fix
```java
// GOOD: stream with pagination — only N records in memory at a time
public void generateReport(ReportWriter writer) {
    int pageSize = 1000;
    int page = 0;
    Page<Order> current;
    do {
        current = orderRepo.findAll(PageRequest.of(page++, pageSize, Sort.by("createdAt")));
        current.getContent().stream()
            .map(this::summarize)
            .forEach(writer::write);  // write immediately, don't accumulate
    } while (current.hasNext());
}

// ALSO GOOD: JPA ScrollableResults (true server-side cursor)
// ALSO GOOD: Spring Data JPA Stream<T> (stateless cursor under the hood)
```

Additional JVM tuning:
```bash
# Use G1GC with region-based collection (default Java 17+):
JAVA_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=16m"

# Or ZGC for sub-millisecond pauses (Java 21):
JAVA_OPTS="-XX:+UseZGC -XX:ZCollectionInterval=5"
```

---

## Scenario 7: Non-Volatile Shared State Visibility Bug

### Overview
A background worker thread sets `running = false` to signal shutdown. The main thread's loop never sees the update — it continues running forever. JVM CPU optimization caches the variable in a register, not flushing to main memory.

### How to Reproduce
```java
// v1_broken/UnsafeWorkerLoop.java:
public class UnsafeWorkerLoop {
    private boolean running = true;  // NOT volatile

    public void run() {
        while (running) {           // main thread reads from register — never sees update
            processNextOrder();
        }
    }

    public void stop() {
        running = false;            // worker thread writes to its own register
        // No guarantee main thread ever sees this change!
    }
}
```

```bash
# Run VisibilityBugTest:
mvn test -pl order-service -Dtest=VisibilityBugTest
# → Test hangs forever (main thread loop never exits)
# → Add -Djvm.opts=-Xint to disable JIT → bug may disappear (JIT is the optimizer)
```

### Symptoms
- Service does not shut down gracefully on SIGTERM
- Worker thread consumes CPU indefinitely after stop() is called
- Thread dump shows: thread in RUNNABLE state in `processNextOrder()` loop, not exiting

### Root Cause
```
JVM (with JIT optimization) is allowed to:
  - Cache `running` in a CPU register (no memory read on each loop iteration)
  - Reorder instructions (if no happens-before relationship)

Without `volatile`, the JVM does not guarantee that:
  - The write to `running = false` in the stop() thread is VISIBLE to the run() thread
  - There is a happens-before edge between the write and subsequent reads

This is the Java Memory Model (JMM) — not a bug, but intended behavior for performance.
```

### Fix
```java
// v2_fixed/SafeWorkerLoop.java:
public class SafeWorkerLoop {
    private volatile boolean running = true;  // volatile = happens-before guarantee

    public void run() {
        while (running) {    // each iteration reads from main memory — sees the update
            processNextOrder();
        }
    }

    public void stop() {
        running = false;    // write is flushed to main memory immediately
    }
}

// ALSO CORRECT:
private final AtomicBoolean running = new AtomicBoolean(true);
// AtomicBoolean internally uses volatile + CAS
```

**Why volatile doesn't fix count++:**
`volatile` guarantees visibility (reads see the latest write) but NOT atomicity.
`count++` is 3 operations: read → increment → write. Between the read and the write, another thread can read the old value. `volatile` ensures the read sees fresh data, but the race between read and write remains.

---

## Scenario 8: Slow External Payment API Cascade Failure

### Overview
The external payment gateway degrades to 30-second response times. All Tomcat threads hang waiting for the HTTP response. The entire order service becomes unresponsive — even endpoints that don't touch payments return 503.

### How to Reproduce
```bash
# Set payment gateway to respond after 30s:
export GATEWAY_DELAY_MS=30000

# Set Tomcat max threads to 20 (visible failure threshold):
# server.tomcat.threads.max: 20 (in application.yml, v1_broken profile)

# Send 25 concurrent payment requests:
for i in {1..25}; do
  curl -s -X POST http://localhost:8082/api/payments/authorize ... &
done

# Now check a health endpoint (should be instant, unrelated to payments):
curl http://localhost:8082/actuator/health
# → Response time: 30+ seconds (all threads are blocked on payment calls!)
```

### Symptoms
- `/actuator/health` returns slowly or times out (unrelated to payment logic)
- All Tomcat threads show `WAITING` on socket read in thread dump
- `thread.pool.active` metric flatlines at Tomcat max threads
- Downstream services waiting for order confirmation also time out → cascading failure

### Logs to Inspect
```json
{"level":"ERROR","service":"payment-service","threadName":"http-nio-8082-exec-5",
 "paymentId":"pay-001","operation":"AUTHORIZE_PAYMENT","elapsedMs":30005,
 "message":"Payment gateway request timed out",
 "exception":"java.net.SocketTimeoutException: Read timed out"}
```
```json
{"level":"ERROR","service":"order-service","threadName":"http-nio-8081-exec-3",
 "orderId":"ord-001","operation":"AWAIT_PAYMENT_CONFIRMATION",
 "message":"Payment service is unavailable — circuit breaker OPEN"}
```

### Root Cause
```
Payment gateway: responds in 30s
Tomcat threads: 20 (max)
Concurrent requests: 25

After 20 requests: all Tomcat threads are blocked on payment HTTP socket
Requests 21-25: wait for a thread to free up
Even /actuator/health can't respond — Tomcat has no free thread!

This is thread starvation from I/O blocking on platform threads.
```

### Fix — Layer 1: HTTP Client Timeout
```java
// Always configure socket timeout — NEVER let it be infinite:
RestClient.builder()
    .baseUrl(gatewayUrl)
    .requestFactory(factory -> {
        var f = new SimpleClientHttpRequestFactory();
        f.setConnectTimeout(Duration.ofSeconds(3));
        f.setReadTimeout(Duration.ofSeconds(10));  // max wait for gateway
    })
    .build();
```

### Fix — Layer 2: Circuit Breaker
```java
@CircuitBreaker(
    name = "payment-gateway",
    fallbackMethod = "gatewayUnavailableFallback"
)
public PaymentResult authorize(AuthorizeRequest request) {
    return gatewayClient.authorize(request);  // throws → circuit opens
}

private PaymentResult gatewayUnavailableFallback(AuthorizeRequest request, Exception ex) {
    // Don't fail the whole order — set payment to PENDING for retry
    return PaymentResult.pending(request.getPaymentId(), "Gateway temporarily unavailable");
}
```

### Fix — Layer 3: Virtual Threads (Java 21)
```yaml
# application.yml:
spring.threads.virtual.enabled: true
```
Virtual threads park on blocking I/O without consuming OS threads. Even with 10,000 concurrent payment calls, the JVM uses only a handful of carrier threads. `/actuator/health` always has a thread available.

---

## Scenario 9: Idempotency Key Collision Under Concurrent Retries

### Overview
A client's payment request times out (gateway takes too long). The client retries. Two retry requests arrive simultaneously — both miss the Redis idempotency check before either has populated it. Both proceed to create a payment record. The customer is charged twice.

### How to Reproduce
```bash
# 1. Set Redis to simulate intermittent unavailability (or just disable the Redis check):
# 2. Send 10 concurrent requests with the same idempotency key:
for i in {1..10}; do
  curl -X POST http://localhost:8080/api/payments/authorize \
    -H "Idempotency-Key: idem-key-123" \
    -d '{"amount": 99.99, "orderId": "ord-001"}' &
done

# 3. Check: how many payment records were created?
psql -c "SELECT COUNT(*) FROM payments WHERE order_id = 'ord-001'"
# v1_no_idempotency: might show 2, 3, or more
# v3_db_constraint: always shows exactly 1
```

### Symptoms
- Customer bank statement shows 2 charges for the same order
- Two payment records with same `order_id` in payments table
- `payments.idempotency_key` column does NOT have UNIQUE constraint (v1_broken)

### Root Cause
```
T1: GET redis:idem-key-123  → nil (cache miss)
T2: GET redis:idem-key-123  → nil (cache miss)  ← race window

T1: POST gateway (proceeds)
T2: POST gateway (proceeds, race not yet resolved)

T1: SET redis:idem-key-123 {paymentId: "pay-001"}
T2: SET redis:idem-key-123 {paymentId: "pay-002"}  ← overwrites T1!

T1: INSERT INTO payments (idempotency_key='idem-key-123', ...)  ← succeeds
T2: INSERT INTO payments (idempotency_key='idem-key-123', ...)  ← ALSO succeeds (no constraint!)

Result: 2 payment records, 2 gateway charges
```

### Fix
```java
// Layer 1: DB UNIQUE constraint is the ground truth (cannot be raced):
// ALTER TABLE payments ADD CONSTRAINT uq_payments_idempotency_key UNIQUE (idempotency_key);

// Layer 2: Redis SETNX to avoid DB roundtrip on hot retries:
public Payment authorizeWithIdempotency(String idempotencyKey, AuthorizeRequest req) {
    // Try Redis first (fast path):
    var cached = redis.get("idem:" + idempotencyKey);
    if (cached != null) return cached;

    // Try DB (slower but authoritative):
    var existing = paymentRepo.findByIdempotencyKey(idempotencyKey);
    if (existing.isPresent()) {
        redis.setex("idem:" + idempotencyKey, 86400, existing.get());  // re-populate cache
        return existing.get();
    }

    // New payment — the DB UNIQUE constraint is the final guard:
    try {
        var payment = processNewPayment(idempotencyKey, req);
        redis.setex("idem:" + idempotencyKey, 86400, payment);
        return payment;
    } catch (DataIntegrityViolationException e) {
        // Race: another thread got here first — read their result:
        return paymentRepo.findByIdempotencyKey(idempotencyKey)
            .orElseThrow(() -> new IllegalStateException("Payment missing after conflict", e));
    }
}
```

**Key insight:** Redis `GET`/`SET` is NOT atomic. The UNIQUE constraint in PostgreSQL is the only truly atomic guard. Redis is a performance optimization on top, not a replacement.

---

## Scenario 10: Thread Starvation in Shared Thread Pool

### Overview
High-priority payment processing tasks and low-priority notification tasks share the same `@Async` thread pool. Payment tasks consume all 50 threads continuously. Notification tasks queue up indefinitely — some users never receive their order confirmation email.

### How to Reproduce
```bash
# 1. Configure a single shared thread pool (v1_broken):
#    spring.task.execution.pool.core-size: 50
#    spring.task.execution.pool.max-size: 50
#    (Both PaymentService and NotificationService use @Async with no specific executor)

# 2. Generate continuous payment load at 50 req/s (saturates the pool):
mvn gatling:test -pl load-test -Dsimulation=PaymentLoadSimulation -Drps=50

# 3. While payment load is running, trigger notifications:
curl -X POST http://localhost:8084/api/notifications/test -d '{"userId":"user-001"}'

# 4. Wait 5 minutes, check if notification was sent:
psql -c "SELECT status, sent_at FROM notifications WHERE user_id='user-001'"
# → status=PENDING, sent_at=NULL (never processed!)
```

### Symptoms
- Order confirmation emails not sent despite successful orders
- `notifications.pending` Kafka topic lag grows
- `notification.processing.queue.size` metric grows unboundedly
- Thread dump: all 50 threads are in `PaymentService.authorize()` — no thread for notifications

### Logs to Inspect
```json
{"level":"WARN","service":"notification-service","threadName":"scheduling-1",
 "operation":"NOTIFICATION_QUEUE_CHECK","queueSize":12456,
 "message":"Notification queue depth is above threshold — processing is starved"}
```

No notification processing log lines appear during the payment load — starvation confirmed.

### Metrics to Inspect
```promql
# Notification queue depth (should be near 0):
notification_queue_size

# Notification processing rate (should match creation rate):
rate(notifications_processed_total[1m])

# Thread pool active by pool name:
executor_active_threads{name="applicationTaskExecutor"}
# → stuck at 50 (all payment threads), notification threads never get scheduled
```

### Root Cause
```
Single thread pool: 50 threads
Payment tasks: continuous 50 req/s → use all 50 threads continuously
Notification tasks: submitted but never executed (queue keeps growing)

Java's default ThreadPoolExecutor uses FIFO ordering — no priority.
In practice, payment tasks arrive faster than notification tasks drain,
so notification tasks always sit at the back of the queue.
This is "starvation" — a thread does make progress, eventually,
but the effective starvation is the notification queue growing without bound.
```

### Fix
```java
// SEPARATE thread pools for different workload types:

@Configuration
public class ThreadPoolConfig {

    @Bean("paymentExecutor")
    public Executor paymentExecutor() {
        var executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(20);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("payment-async-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        return executor;
    }

    @Bean("notificationExecutor")
    public Executor notificationExecutor() {
        var executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(1000);
        executor.setThreadNamePrefix("notif-async-");
        return executor;
    }
}

// Usage — specify the pool explicitly:
@Async("paymentExecutor")
public CompletableFuture<PaymentResult> authorizeAsync(...) { ... }

@Async("notificationExecutor")
public CompletableFuture<Void> sendAsync(...) { ... }
```

**Key insight:** Separate thread pools provide isolation between workload types. A payment traffic spike can no longer starve notification processing. The tradeoff is more threads overall, but this is acceptable — notification threads are mostly idle (I/O-bound, low arrival rate).

### Verify Fix
```bash
# With separate thread pools:
# 1. Start payment load (saturates paymentExecutor)
# 2. Trigger notification
# 3. Check notification sent within 5 seconds
psql -c "SELECT status, sent_at FROM notifications WHERE user_id='user-001'"
# → status=SENT, sent_at=<timestamp> (delivered despite payment load)
```

---

## Quick Reference: Incident Correlation

| Symptom | Likely Scenario | First Metric to Check |
|---|---|---|
| All requests returning 503 | Thread Pool Exhaustion (#1) | `hikaricp_connections_pending` |
| Negative inventory after flash sale | Oversell Race (#2) | DB: `SELECT reserved_quantity > total_quantity` |
| Intermittent 500 on multi-item orders | Deadlock (#3) | PostgreSQL log: `deadlock detected` |
| Email/SMS delivery delayed hours | Starvation (#10) | `notification_queue_size` |
| DB CPU spike every N seconds | Cache Stampede (#5) | `cache_gets_total{result="hit"}` ratio |
| API freezes every ~N minutes | GC Pressure (#6) | `jvm_gc_pause_seconds_sum` |
| Duplicate charge on retry | Idempotency Race (#9) | `SELECT COUNT(*) FROM payments WHERE order_id=?` |
| API unresponsive after gateway degradation | Cascade Failure (#8) | `circuit_breaker_state` |
| Worker process never stops | Visibility Bug (#7) | Thread dump: RUNNABLE in infinite loop |
| Payment lag spike on SMS API recovery | Retry Storm (#4) | `kafka_consumer_group_lag` |
