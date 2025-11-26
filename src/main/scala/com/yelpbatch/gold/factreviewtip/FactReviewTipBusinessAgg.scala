package com.yelpbatch.gold.factreviewtip

// Imports spark packages
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.yelpbatch.utils.{DateUtils, Granularity, IOUtils, PostgreSQLWriter, RawColumns, tableNames, transformColumns}

object FactReviewTipBusinessAgg {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /**
   * RUN: Main execution flow for Fact Review & Tip aggregation
   * Computes daily and/or monthly metrics based on runDate
   *
   * @param spark              SparkSession
   * @param config             application config
   * @param runDateStr         run date string YYYY-MM-DD
   * @param forceMonthOpt      optional YYYY-MM to force monthly run
   * @param skipDaily          if true, skip daily computation
   * @param dryRun             if true, skip writes
   * @param postGresqlUser     PostgreSQL username
   * @param postGresqlPassword PostgreSQL password
   */
  def run(
           spark: SparkSession,
           config: Config,
           runDateStr: String,
           forceMonthOpt: Option[String],
           skipDaily: Boolean,
           dryRun: Boolean,
           postGresqlUser: String = "",
           postGresqlPassword: String = ""
         ): Unit = {

    // Validate and normalize date
    require(runDateStr.nonEmpty, "runDate is required: yyyy-MM-dd")
    val runDate = DateUtils.normalizeDate(runDateStr)
    val isEom = DateUtils.isEndOfMonthStr(runDate)
    val ym = DateUtils.toYearMonth(runDate)
    val monthStart = DateUtils.toMonthStart(runDate)
    val monthEnd = DateUtils.toMonthEnd(runDate)

    logger.info(s"[FactReviewTipBusinessAgg] Starting execution for runDate=$runDate (isEom=$isEom, ym=$ym)")

    // Get paths from config
    val silverDir: String = config.getString("paths.silverDir")
    val goldPath = config.getString("paths.gold.factReviewTipMetrics")

    // Apply Spark configurations
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", config.getString("tuning.partitionOverwriteMode"))
    spark.conf.set("spark.sql.shuffle.partitions", config.getInt("tuning.shufflePartitions").toString)
    spark.conf.set("spark.sql.files.maxPartitionBytes", config.getString("tuning.maxPartitionBytes"))
    spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols",
      config.getInt("delta.stats.numIndexedCols").toString)
    spark.conf.set("spark.sql.legacy.timeParserPolicy", config.getString("tuning.legacyTimeParserPolicy"))

    // DAILY: Always compute daily metrics unless skipDaily is set
    if (!skipDaily) {
      logger.info(s"[FactReviewTipBusinessAgg] Computing daily metrics for $runDate")
      // READ: Load data from Silver layer
      val dataFrames: Map[String, DataFrame] = readData(
        spark = spark,
        silverPath = silverDir,
        dateStart = runDate,
        dateEnd = runDate
      )
      // TRANSFORM daily metrics
      val dailyMetrics: DataFrame = transform(
        dataframes = dataFrames,
        runDate = runDate,
        granularity = Granularity.Daily
      )
      // PERSIST: Write results to Gold layer
      persist(
        spark = spark,
        df = dailyMetrics,
        outputPath = goldPath,
        hostPostGres = config.getString("postgresql.host"),
        portPostGres = config.getInt("postgresql.port"),
        databasePostGres = config.getString("postgresql.database"),
        postGresEnabled = config.getBoolean("postgresql.enabled"),
        userPostGres = postGresqlUser,
        passwordPostGres = postGresqlPassword
      )
      logger.info(s"[FactReviewTipBusinessAgg] Execution completed successfully for " +
        s"runDate=$runDate, granularity=${Granularity.Daily}")
    }

    // MONTHLY: Compute monthly metrics if runDate is end-of-month or forceMonthOpt matches
    val monthlyToRun = isEom || forceMonthOpt.contains(ym)

    if (monthlyToRun) {
      logger.info(s"[FactReviewTipBusinessAgg] Computing monthly metrics for $ym")

      // READ monthly data
      val monthlyData: Map[String, DataFrame] = readData(
        spark = spark,
        silverPath = silverDir,
        dateStart = monthStart,
        dateEnd = monthEnd
      )

      // TRANSFORM monthly metrics
      val monthlyMetrics: DataFrame = transform(
        dataframes = monthlyData,
        runDate = runDate,
        granularity = Granularity.Monthly
      )

      // PERSIST monthly metrics
      persist(
        spark = spark,
        df = monthlyMetrics,
        outputPath = goldPath,
        hostPostGres = config.getString("postgresql.host"),
        portPostGres = config.getInt("postgresql.port"),
        databasePostGres = config.getString("postgresql.database"),
        postGresEnabled = config.getBoolean("postgresql.enabled"),
        userPostGres = postGresqlUser,
        passwordPostGres = postGresqlPassword
      )
      logger.info(s"[FactReviewTipBusinessAgg] Execution completed successfully for " +
        s"runDate=$runDate, granularity=${Granularity.Monthly}")
    }
  }

  /**
   * READ: Load all required tables from Silver layer with date filtering
   *
   * @param spark      SparkSession
   * @param silverPath base path to Silver layer
   * @param dateStart  start date for filtering (first day of month)
   * @param dateEnd    end date for filtering (last day of month)
   * @return Map of table name -> DataFrame
   */
  private def readData(
                        spark: SparkSession,
                        silverPath: String,
                        dateStart: String,
                        dateEnd: String
                      ): Map[String, DataFrame] = {
    // Define tables to read
    val tablesToRead = Seq(tableNames.Business, tableNames.Review, tableNames.Tip)

    // READ: Load all tables from Delta and return a Map[String, DataFrame]
    val dataFrames: Map[String, DataFrame] = tablesToRead.map { table =>
      logger.info(s"Reading table: $table for date range [$dateStart, $dateEnd]")
      table -> IOUtils.readDelta(
        spark = spark,
        path = s"$silverPath/${table}_snapshot",
        columns = FactReviewTipBusinessUtils.selectColumns(table),
        conditions = FactReviewTipBusinessUtils.filterConditions(table, dateStart, dateEnd)
      )
    }.toMap

    logger.info(s"Successfully loaded ${dataFrames.size} tables")
    dataFrames
  }

  /** TRANSFORM: Compute metrics from loaded DataFrames
   *
   * @param dataframes  Map of table name -> DataFrame
   * @param runDate     run date string YYYY-MM-DD
   * @param granularity Granularity (Daily or Monthly)
   * @return DataFrame with computed metrics
   */
  private def transform(
                         dataframes: Map[String, DataFrame],
                         runDate: String,
                         granularity: Int
                       ): DataFrame = {

    // Combine review and tip metrics
    val combinedDf = FactReviewTipBusinessUtils.getMetrics(
      runDate = runDate,
      reviewDF = dataframes(tableNames.Review),
      tipDF = dataframes(tableNames.Tip),
      granularity = granularity
    )
    combinedDf
  }

  /** PERSIST: Write output in wide format (one row per business_id + measure)
   *
   * @param spark             SparkSession used for writing data
   * @param df                DataFrame to be persisted
   * @param outputPath        Destination path for the output data
   * @param hostPostGres      PostgreSQL host
   * @param portPostGres      PostgreSQL port
   * @param databasePostGres  PostgreSQL database name
   * @param postGresEnabled   Whether PostgreSQL write is enabled
   * @param userPostGres      PostgreSQL username
   * @param passwordPostGres  PostgreSQL password
   */
  private def persist(
                       spark: SparkSession,
                       df: DataFrame,
                       outputPath: String,
                       hostPostGres: String = "",
                       portPostGres: Int = 5432,
                       databasePostGres: String = "",
                       postGresEnabled: Boolean = false,
                       userPostGres: String = "",
                       passwordPostGres: String = ""
                     ): Unit = {
    logger.info(s"[FactReviewTipBusinessAgg] Writing data to Gold layer")

    // Add ingestion timestamp
    val dfWithTs: DataFrame = df
      .withColumn(transformColumns.goldIngestTs,
        current_timestamp()
      )
    // Define key columns for upsert (business_id + period_month)
    val keyCols: Seq[String] = Seq(RawColumns.businessId,
      transformColumns.day,
      transformColumns.granularity,
      ColNames.measure)
    val partitionCols: Seq[String] = Seq(transformColumns.day, transformColumns.granularity)

    // Write to PostgreSQL if enabled
    if (postGresEnabled) {
      logger.info(s"[FactReviewTipBusinessAgg] Upserting data to PostgreSQL database at $hostPostGres:$portPostGres/$databasePostGres")
      val jdbcUrl = PostgreSQLWriter.getJDBCUrl(
        host = hostPostGres,
        port = portPostGres,
        database = databasePostGres
      )

      // Use upsert to handle duplicates (delete existing records for the same day/period_month, then insert)
      PostgreSQLWriter.upsertToPostgreSQL(
        spark = spark,
        df = dfWithTs,
        jdbcUrl = jdbcUrl,
        tableName = "gold.fact_review_tip_metrics_wide",
        user = userPostGres,
        password = passwordPostGres,
        deleteKeys = Seq("day", "period_month", "granularity"),  // Delete existing records for this day, period, and granularity
        enabled = postGresEnabled
      )

      logger.info("Successfully upserted to " +
        s"PostgreSQL table gold.fact_review_tip_metrics_wide")
    }
    else {
      // Upsert into Delta table
      IOUtils.upsertDeltaByKey(
        spark = spark,
        df = dfWithTs,
        outputPath = outputPath,
        keyCols = keyCols,
        partitionColumns = partitionCols
      )
      logger.info(s"Successfully wrote to $outputPath")
    }
  }
}