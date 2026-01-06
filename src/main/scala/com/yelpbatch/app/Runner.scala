package com.yelpbatch.app

import com.typesafe.config.Config
import com.yelpbatch.bronze.BronzeIngest
import com.yelpbatch.gold.factreviewtip.FactReviewTipBusinessAgg
import com.yelpbatch.gold.businesspopularity.BusinessPopularityAgg
import com.yelpbatch.silver.SilverIngest
import com.yelpbatch.utils.Observability
import org.apache.spark.sql.SparkSession
import org.slf4j.{LoggerFactory, MDC}

object Runner {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null

    try {
      // 1. Argument Parsing
      val jobArgs = JobArguments.parse(args)

      // 2. Setup Global Logging Context (MDC)
      // All logs from this point will have process, env, and run_date attached
      MDC.put("process", jobArgs.process)
      MDC.put("env", jobArgs.env)
      MDC.put("run_date", jobArgs.runDate.mkString("|"))

      logger.info("Starting YelpBatch Application...")

      // 3. Driver Registration
      try {
        Class.forName("org.postgresql.Driver")
        logger.info("[OK] PostgreSQL JDBC driver loaded successfully")
      } catch {
        case e: ClassNotFoundException =>
          throw new RuntimeException("[ERROR] PostgreSQL driver not found in classpath", e)
      }

      // 4. Config Loading (Validates config automatically)
      val (rawConfig, _) = AppConfig.loadTyped(jobArgs.env)

      // 5. Spark Session Creation
      spark = createSparkSession(jobArgs, rawConfig)

      // 6. Job Execution
      val startTime = System.currentTimeMillis()
      runJob(spark, jobArgs, rawConfig)
      val duration = System.currentTimeMillis() - startTime

      // 7. Observability: Success Metric
      Observability.trackMetric("job_duration_ms", duration)
      Observability.trackMetric("job_status", 1) // 1 = Success

    } catch {
      case e: Exception =>
        logger.error(s"!!! Job Failed: ${e.getMessage}", e)
        Observability.trackMetric("job_status", 0) // 0 = Failure
        // Hook for metrics/alerting
        sendAlert(e)
        sys.exit(1)
    } finally {
      // 8. Cleanup
      if (spark != null) {
        logger.info("Stopping Spark Session...")
        spark.stop()
      }
      MDC.clear() // Clean up thread local
    }
  }

  private def createSparkSession(jobArgs: JobArguments, config: Config): SparkSession = {
    val sparkBuilder = SparkSession.builder()
      .appName(s"YelpBatch-${jobArgs.process}")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")

    // Read tuning values from raw config
    val timeParserPolicy = config.getString("tuning.legacyTimeParserPolicy")
    val executorHeartbeat = config.getString("tuning.executorHeartbeat")

    val configuredBuilder = sparkBuilder
      .config("spark.sql.legacy.timeParserPolicy", timeParserPolicy)
      .config("spark.executor.heartbeatInterval", executorHeartbeat)

    if (jobArgs.env == "local") {
      configuredBuilder
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    } else {
      configuredBuilder.getOrCreate()
    }
  }

  private def runJob(spark: SparkSession, jobArgs: JobArguments, appConfig: Config): Unit = {
    jobArgs.process match {
      case p if p == "bronze_ingest" && jobArgs.skipBronze =>
        logger.info("Skipping Bronze Ingest as per --skip_bronze flag.")

      case "bronze_ingest" =>
        BronzeIngest.run(spark, appConfig, jobArgs.tablesOpt)

      case "silver_ingest" =>
        jobArgs.runDate.foreach(d => SilverIngest.run(spark, appConfig, d, jobArgs.fullLoad, jobArgs.tablesOpt))

      case "gold_fact_review_tip" =>
        jobArgs.runDate.foreach(d =>
          FactReviewTipBusinessAgg.run(
            spark, appConfig, d, jobArgs.forceMonth, jobArgs.skipDaily, jobArgs.dryRun,
            jobArgs.postGreSQLUser.getOrElse(""), jobArgs.postGreSQLPassword.getOrElse("")
          )
        )

      case "gold_business_popularity" =>
        jobArgs.runDate.foreach(d =>
          BusinessPopularityAgg.run(
            spark, appConfig, d, jobArgs.postGreSQLUser.getOrElse(""), jobArgs.postGreSQLPassword.getOrElse("")
          )
        )

      case other =>
        throw new IllegalArgumentException(s"Unknown process: $other")
    }
  }

  private def sendAlert(e: Exception): Unit = {
    // Placeholder for alerting logic (e.g., PagerDuty, Slack, Email, Datadog)
    logger.error(s"ALERT: Job failure detected. Reason: ${e.getMessage}")
  }
}