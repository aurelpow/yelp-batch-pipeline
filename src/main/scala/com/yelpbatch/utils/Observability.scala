package com.yelpbatch.utils

import org.slf4j.{LoggerFactory, MDC}
import scala.util.{Failure, Success, Try}

object Observability {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Logs a metric in a structured format (JSON-like) for log scrapers.
   */
  def trackMetric(name: String, value: Double, tags: Map[String, String] = Map.empty): Unit = {
    // In a real setup, this would push to StatsD/Prometheus.
    // Here we log a structured event that agents can parse.
    val tagString = tags.map { case (k, v) => s""""$k":"$v"""" }.mkString(",")
    logger.info(s"""{"metric":"$name", "value":$value, "tags":{$tagString}}""")
  }

    // Circuit Breaker State
  private val failureCount = new java.util.concurrent.atomic.AtomicInteger(0)
  private val lastFailureTime = new java.util.concurrent.atomic.AtomicLong(0)
  private val failureThreshold = 5
  private val resetTimeout = 60000 // 1 minute

  /**
   * Circuit Breaker Logic
   * Prevents execution if too many failures occurred recently.
   */
  def withCircuitBreaker[T](fn: => T): T = {
    val failures = failureCount.get()
    val lastFail = lastFailureTime.get()
    val now = System.currentTimeMillis()

    if (failures >= failureThreshold) {
      if (now - lastFail > resetTimeout && lastFailureTime.compareAndSet(lastFail, now)) {
        // Half-open: try once
        try {
          logger.info("Circuit Breaker: Half-open, attempting operation...")
          val result = fn
          // Success: Reset
          failureCount.set(0)
          logger.info("Circuit Breaker: Closed (Recovered)")
          result
        } catch {
          case e: Exception =>
            lastFailureTime.set(System.currentTimeMillis())
            logger.warn("Circuit Breaker: Open (Probe failed)")
            throw new RuntimeException("Circuit Breaker: Open (Probe failed)", e)
        }
      } else {
        throw new RuntimeException(s"Circuit Breaker: Open. Skipping operation. (Failures: $failures)")
      }
    } else {
      try {
        val result = fn
        // Success: Reset if we had some failures but not enough to trip
        if (failures > 0) failureCount.set(0)
        result
      } catch {
        case e: Exception =>
          val newCount = failureCount.incrementAndGet()
          lastFailureTime.set(now)
          logger.warn(s"Operation failed. Failure count: $newCount/$failureThreshold")
          throw e
      }
    }
  }

  /**
   * Retry Logic (Resilience)
   * Retries a block of code 'maxRetries' times with exponential backoff.
   * @param fn The block of code to execute
   * @param maxRetries Maximum number of retries
   * @param delayMs Initial delay in milliseconds
   * @tparam T Return type of the block
   * @return Result of type T
   */
  @scala.annotation.tailrec
  def withRetry[T](fn: => T, maxRetries: Int = 3, delayMs: Long = 2000, attempt: Int = 1): T = {
    Try(fn) match {
      case Success(result) => result
      case Failure(e) if attempt <= maxRetries =>
        logger.warn(s"Operation failed (Attempt $attempt/$maxRetries). Retrying in ${delayMs}ms. Error: ${e.getMessage}")
        try {
          Thread.sleep(delayMs)
        } catch {
          case ie: InterruptedException =>
            Thread.currentThread().interrupt()
            throw ie
        }
        withRetry(fn, maxRetries, delayMs * 2, attempt + 1)
      case Failure(e) =>
        throw e
    }
  }

  /**
   * Helper to manage MDC Context safely
   */
  def withMdc[T](context: Map[String, String])(block: => T): T = {
    val previousContext = Option(MDC.getCopyOfContextMap)
    context.foreach { case (k, v) => MDC.put(k, v) }
    try {
      block
    } finally {
      // Restore previous context or clear if it was empty
      previousContext match {
        case Some(map) => MDC.setContextMap(map)
        case None => MDC.clear()
      }
    }
  }
}
