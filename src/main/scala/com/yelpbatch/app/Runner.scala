package com.yelpbatch.app

import com.yelpbatch.bronze.BronzeIngest
import com.yelpbatch.gold.factreviewtip.FactReviewTipBusinessAgg
import com.yelpbatch.gold.businesspopularity.BusinessPopularityAgg
import com.yelpbatch.silver.SilverIngest
import com.yelpbatch.utils.DateUtils
import org.apache.spark.sql.SparkSession

object Runner {
  def main(args: Array[String]): Unit = {
    // Force PostgreSQL driver registration BEFORE SparkSession creation
    try {
      Class.forName("org.postgresql.Driver")
      println("✓ PostgreSQL JDBC driver loaded successfully")
    } catch {
      case e: ClassNotFoundException =>
        System.err.println("✗ PostgreSQL driver not found in classpath")
        throw e
    }
    // Parse command-line arguments into a map
    val m = args.sliding(2,2).collect{case Array(k,v)=>k.stripPrefix("--")->v}.toMap
    val process: String = m.getOrElse("process", sys.error("Missing --process"))
    val env: String = m.getOrElse("env",sys.error("Missing --env"))
    val runDateOpt: Option[String] = m.get("run_date")            // YYYY-MM-DD (single day)
    val startOpt: Option[String] = m.get("start_date")          // YYYY-MM-DD
    val endOpt: Option[String] = m.get("end_date")            // YYYY-MM-DD
    val tablesOpt:Option[String]  = m.get("tables") // comma-separated table names
    val forceMonth: Option[String] = m.get("force_monthly")       // YYYY-MM
    val postGresqlUser: Option[String] = m.get("pg_user")       // PostgreSQL user
    val postGresqlPassword: Option[String] = m.get("pg_password") // PostgreSQL password

    // Helper function to parse boolean flags
    def getBooleanFlag(key: String): Boolean = {
      m.get(key).exists(v => v.toLowerCase == "true")
    }

    val skipDaily: Boolean = getBooleanFlag("skip_daily") // for gold processes
    val dryRun: Boolean    = getBooleanFlag("dry_run") // for gold processes
    val fullLoad: Boolean  = getBooleanFlag("full_load") // for silver_ingest
    val skipBronze: Boolean = getBooleanFlag("skip_bronze") // for bronze_ingest

    // Load environment-specific config
    val appConfig = AppConfig.load(env)

    // derive the execution dates
    val runDates: Seq[String] = {
      (runDateOpt, startOpt, endOpt) match {
        case (Some(d), None, None) => Seq(DateUtils.normalizeDate(d))
        case (None, Some(s), Some(e)) => DateUtils.dateRange(s, e)
        case _ => sys.error("Provide either --run_date YYYY-MM-DD or --start_date & --end_date")
      }
    }

    // Normalize tables argument if they came with square brackets (e.g., from JSON)
    def parseTables(tablesStr: String): Option[String] = {
      if (tablesStr == null) return None
      val trimmed = tablesStr.trim
      // remove surrounding brackets if present
      val inside = if (trimmed.startsWith("[") && trimmed.endsWith("]")) trimmed.substring(1, trimmed.length - 1) else trimmed
      // unify quotes by replacing single quotes with double quotes
      val unified = inside.replace('\'', '"')
      // split on commas, trim and strip surrounding double quotes
      val items = unified
        .split(",")
        .map(_.trim)
        .map(_.stripPrefix("\"").stripSuffix("\""))
        .filter(_.nonEmpty)
      if (items.isEmpty) None else Some(items.mkString(","))
    }

    // Normalize tables argument
    val tablesCsvOpt: Option[String] = tablesOpt.flatMap(parseTables)

    val sparkBuilder = SparkSession.builder()
      .appName(s"YelpBatch-$process")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore")  // For local filesystem
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")

    // Read tuning values from application config with safe defaults.
    val timeParserPolicy: String =
      try {
        if (appConfig.hasPath("tuning.timeParserPolicy")) appConfig.getString("tuning.timeParserPolicy") else "LEGACY"
      } catch { case _: Throwable => "LEGACY" }

    val executorHeartbeat: String =
      try {
        if (appConfig.hasPath("tuning.executorHeartbeatInterval")) appConfig.getString("tuning.executorHeartbeatInterval") else "60s"
      } catch { case _: Throwable => "60s" }

    val configuredBuilder: SparkSession.Builder = sparkBuilder
      .config("spark.sql.legacy.timeParserPolicy", timeParserPolicy)
      .config("spark.executor.heartbeatInterval", executorHeartbeat)

    // Local-only configuration (won't affect production)
    val finalBuilder: SparkSession.Builder = if (env == "local") {
      sparkBuilder
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1") // Force binding to localhost
    } else {
      configuredBuilder
    }
    // Create Spark session
    val spark = finalBuilder.getOrCreate()

    process match {
      case p if  p == "bronze_ingest" && skipBronze  =>
        println("Skipping Bronze Ingest as per --skip_bronze flag.")
      case "bronze_ingest" =>
        BronzeIngest.run(spark,appConfig, tablesCsvOpt)
      case "silver_ingest" =>
        runDates.foreach(d => SilverIngest.run(spark,appConfig, d,fullLoad, tablesCsvOpt))
      case "gold_fact_review_tip" =>
        runDates.foreach(d =>
          FactReviewTipBusinessAgg.run(
            spark,
            appConfig,
            d,
            forceMonth,
            skipDaily,
            dryRun,
            postGresqlUser.getOrElse(""),
            postGresqlPassword.getOrElse("")
          )
        )
      case "gold_business_popularity" =>
        runDates.foreach(d =>
          BusinessPopularityAgg.run(
            spark,
            appConfig,
            d,
            postGresqlUser.getOrElse(""),
            postGresqlPassword.getOrElse("")
          )
        )
      case other => sys.error(s"Unknown process: $other")
    }
    // Close the Spark session
    spark.stop()
  }
}