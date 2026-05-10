package com.platform.simulation

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

/**
 * SIMULATION: Flash Sale — Inventory Race Condition
 *
 * Demonstrates: inventory oversell race condition, locking strategies under load.
 *
 * HOW TO USE:
 *   Step 1 — Run with UNSAFE strategy, observe oversell:
 *     INVENTORY_STRATEGY=UNSAFE mvn gatling:test -pl load-test \
 *       -Dsimulation=com.platform.simulation.FlashSaleSimulation
 *     Then: psql -c "SELECT * FROM inventory WHERE product_id='00000000-0000-0000-0000-000000000002'"
 *     Expected: reserved_quantity > 10 (OVERSOLD!)
 *
 *   Step 2 — Run with PESSIMISTIC strategy, verify no oversell:
 *     INVENTORY_STRATEGY=PESSIMISTIC mvn gatling:test -pl load-test \
 *       -Dsimulation=com.platform.simulation.FlashSaleSimulation
 *     Expected: reserved_quantity = 10, 4990 orders REJECTED with 409
 *
 *   Step 3 — Run with REDIS strategy, compare throughput:
 *     INVENTORY_STRATEGY=REDIS mvn gatling:test -pl load-test \
 *       -Dsimulation=com.platform.simulation.FlashSaleSimulation
 *     Expected: same correctness, higher throughput than PESSIMISTIC
 *
 * See taskplan.md Task 3.2 and production_scenarios.md Scenario 2 for details.
 */
class FlashSaleSimulation extends Simulation {

  val baseUrl: String = sys.env.getOrElse("BASE_URL", "http://localhost:8080")
  val flashSaleProductId: String = "00000000-0000-0000-0000-000000000002"  // stock=10
  val totalUsers: Int = sys.env.getOrElse("FLASH_SALE_USERS", "5000").toInt

  val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")

  val buyFlashSaleItem = scenario("Flash Sale Purchase")
    .exec(
      http("POST /api/orders (flash sale)")
        .post("/api/orders")
        .body(StringBody(_ => {
          val userId = java.util.UUID.randomUUID().toString
          val idempotencyKey = java.util.UUID.randomUUID().toString
          s"""
          {
            "userId": "$userId",
            "idempotencyKey": "$idempotencyKey",
            "currency": "USD",
            "items": [
              {
                "productId": "$flashSaleProductId",
                "quantity": 1,
                "unitPrice": 9.99
              }
            ]
          }
          """
        }))
        // Accept both 202 (won the race) and 409 (lost — out of stock)
        // Any 5xx is a test failure (indicates a bug, not expected behavior)
        .check(status.in(202, 409))
    )

  setUp(
    // ALL users fire simultaneously — no ramp-up. This is the flash sale moment.
    buyFlashSaleItem.inject(
      atOnceUsers(totalUsers)
    )
  ).protocols(httpProtocol)
    .assertions(
      // No 5xx errors allowed — oversell should fail with business-level 409, not crash
      global.failedRequests.count.is(0)
    )
}
