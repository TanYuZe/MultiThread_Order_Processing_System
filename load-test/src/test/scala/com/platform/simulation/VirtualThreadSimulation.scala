package com.platform.simulation

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

/**
 * SIMULATION: Virtual Thread vs Platform Thread Comparison
 *
 * Demonstrates: thread starvation on platform threads vs virtual thread scalability.
 *
 * See taskplan.md Task 2.5 for setup instructions.
 *
 * HOW TO USE:
 *   Step 1 — Platform threads (starvation expected):
 *     # Set in application.yml: spring.threads.virtual.enabled=false
 *     # Set: server.tomcat.threads.max=10
 *     mvn gatling:test -pl load-test -Dsimulation=com.platform.simulation.VirtualThreadSimulation
 *     Expected: >= 80% error rate (only 10 threads, gateway takes 200ms each)
 *
 *   Step 2 — Virtual threads:
 *     # Set in application.yml: spring.threads.virtual.enabled=true
 *     mvn gatling:test -pl load-test -Dsimulation=com.platform.simulation.VirtualThreadSimulation
 *     Expected: 0% error rate (virtual threads park during sleep, not consuming OS threads)
 *
 *   The order validation endpoint simulates an external call with configurable delay.
 *   Set VALIDATION_DELAY_MS env var in order-service to control the delay.
 */
class VirtualThreadSimulation extends Simulation {

  val baseUrl: String = sys.env.getOrElse("BASE_URL", "http://localhost:8080")
  val concurrentUsers: Int = sys.env.getOrElse("CONCURRENT_USERS", "50").toInt

  val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")

  val validateAddress = scenario("Address Validation (IO-bound)")
    .exec(
      http("POST /api/orders/validate-address")
        .post("/api/orders/validate-address")
        .body(StringBody("""{"street":"123 Main St","city":"Springfield","zip":"12345"}"""))
        .check(status.in(200, 202))
    )

  setUp(
    validateAddress.inject(
      atOnceUsers(concurrentUsers)
    )
  ).protocols(httpProtocol)
    .assertions(
      // With platform threads (10 max): this will FAIL
      // With virtual threads: this will PASS
      global.successfulRequests.percent.gte(99),
      global.responseTime.percentile(99).lte(500)
    )
}
