package com.platform.simulation

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._
import scala.util.Random

/**
 * SIMULATION: High-Concurrency Order Creation
 *
 * Demonstrates: thread pool exhaustion, DB connection pool saturation,
 *               Kafka producer backpressure under 10,000 concurrent users.
 *
 * Run with:
 *   mvn gatling:test -pl load-test -Dsimulation=com.platform.simulation.OrderCreationSimulation
 *
 * View report: load-test/target/gatling-results/<timestamp>/index.html
 *
 * Expected bottlenecks (fix progressively — see taskplan.md Task 6.1):
 *   1. HikariCP pool exhaustion → SQLTransientConnectionException → 503
 *   2. Kafka producer queue timeout → message loss
 *   3. GC pressure at sustained 10k/s
 */
class OrderCreationSimulation extends Simulation {

  val baseUrl: String = sys.env.getOrElse("BASE_URL", "http://localhost:8080")

  val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")
    .header("X-Correlation-Id", session => java.util.UUID.randomUUID().toString)

  val productIds = List(
    "00000000-0000-0000-0000-000000000001",
    "00000000-0000-0000-0000-000000000003"
  )

  def createOrderBody(): String = {
    val userId = java.util.UUID.randomUUID().toString
    val productId = productIds(Random.nextInt(productIds.size))
    val idempotencyKey = java.util.UUID.randomUUID().toString
    s"""
    {
      "userId": "$userId",
      "idempotencyKey": "$idempotencyKey",
      "currency": "USD",
      "items": [
        {
          "productId": "$productId",
          "quantity": 1,
          "unitPrice": 49.99
        }
      ]
    }
    """
  }

  val createOrder = scenario("Create Order")
    .exec(
      http("POST /api/orders")
        .post("/api/orders")
        .body(StringBody(_ => createOrderBody()))
        .check(status.in(202, 409))  // 202 = created, 409 = duplicate idempotency key
    )

  // Ramp from 0 to 1000 users over 30s, hold for 60s
  // Change to 10000 for full stress test (EPIC 6 Task 6.1)
  val targetUsers: Int = sys.env.getOrElse("TARGET_USERS", "1000").toInt
  val rampDuration: Int = sys.env.getOrElse("RAMP_SECONDS", "30").toInt
  val holdDuration: Int = sys.env.getOrElse("HOLD_SECONDS", "60").toInt

  setUp(
    createOrder.inject(
      rampUsers(targetUsers).during(rampDuration.seconds),
      constantUsersPerSec(targetUsers / 10).during(holdDuration.seconds)
    )
  ).protocols(httpProtocol)
    .assertions(
      global.successfulRequests.percent.gte(95),   // fail if > 5% error rate
      global.responseTime.percentile(99).lte(2000) // fail if P99 > 2s
    )
}
