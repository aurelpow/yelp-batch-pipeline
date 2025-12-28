package com.yelpbatch.app

import com.typesafe.config.Config
import com.yelpbatch.bronze.BronzeIngest
import com.yelpbatch.gold.factreviewtip.FactReviewTipBusinessAgg
import com.yelpbatch.gold.businesspopularity.BusinessPopularityAgg
import com.yelpbatch.silver.SilverIngest
import org.apache.spark.sql.SparkSession

object Runner {
  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null

    try {
      // 1. Driver Registration
      try {
        Class.forName("org.postgresql.Driver")
        println("✓ PostgreSQL JDBC driver loaded successfully")
      } catch {
        case e: ClassNotFoundException =>
          throw new RuntimeException("✗ PostgreSQL driver not found in classpath", e)
      }

      // 2. Argument Parsing
      val jobArgs = JobArguments.parse(args)

      // 3. Config Loading (Validates config automatically)
      val (rawConfig, typedConfig) = AppConfig.loadTyped(jobArgs.env)

      // 4. Spark Session Creation
      spark = createSparkSession(jobArgs, typedConfig)

      // 5. Job Execution
      runJob(spark, jobArgs, rawConfig)

    } catch {
      case e: Exception =>
        System.err.println(s"!!! Job Failed: ${e.getMessage}")
        e.printStackTrace()
        // Hook for metrics/alerting
        sendAlert(e)
        sys.exit(1)
    } finally {
      // 6. Cleanup
      if (spark != null) {
        println("Stopping Spark Session...")
        spark.stop()
      }
    }
  }

  def createSparkSession(jobArgs: JobArguments, appConfig: AppConfig): SparkSession = {
    val sparkBuilder = SparkSession.builder()
      .appName(s"YelpBatch-${jobArgs.process}")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")

    // Read tuning values from typed config
    val timeParserPolicy = appConfig.tuning.legacyTimeParserPolicy
    val executorHeartbeat = appConfig.tuning.executorHeartbeat

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

  def runJob(spark: SparkSession, jobArgs: JobArguments, appConfig: Config): Unit = {
    jobArgs.process match {
      case p if p == "bronze_ingest" && jobArgs.skipBronze =>
        println("Skipping Bronze Ingest as per --skip_bronze flag.")

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

  def sendAlert(e: Exception): Unit = {
    // Placeholder for alerting logic (e.g., PagerDuty, Slack, Email, Datadog)
    System.err.println(s"ALERT: Job failure detected. Reason: ${e.getMessage}")
  }
}