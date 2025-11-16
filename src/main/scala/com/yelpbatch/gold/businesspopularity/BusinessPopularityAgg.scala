package com.yelpbatch.gold.businesspopularity

import com.typesafe.config.Config
import com.yelpbatch.gold.businesspopularity.BusinessPopularityAgg.{persist, readData, transform}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import com.yelpbatch.utils.{DateUtils, IOUtils, RawColumns, tableNames, transformColumns,Granularity}

object BusinessPopularityAgg {

  // Logger
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /**
   * RUN: Main execution flow for Business Popularity aggregation
   * Only runs when runDate is end-of-month
   *
   * @param spark   SparkSession
   * @param config  application configuration
   * @param runDate date to process (YYYY-MM-DD)
   */
  def run(
           spark: SparkSession,
           config: Config,
           runDate: String
         ): Unit = {

    // Validate required parameters
    require(runDate.nonEmpty, "runDate is required: yyyy-MM-dd")

    // Check if runDate is end of month - skip if not
    if (!DateUtils.isEndOfMonthStr(runDate)) {
      logger.info(s"[BusinessPopularityAgg] Skipping execution: $runDate is not end-of-month")
      return
    }

    logger.info(s"[BusinessPopularityAgg] Starting execution for end-of-month date: $runDate")

    // Compute date range (first day to last day of month)
    val firstDayOfMonth: String = DateUtils.toMonthStart(runDate)
    val lastDayOfMonth: String = runDate // runDate is already validated as end-of-month
    val periodMonth: String = DateUtils.toYearMonth(runDate)

    logger.info(s"[BusinessPopularityAgg] Processing month: $periodMonth [$firstDayOfMonth to $lastDayOfMonth]")

    // Get paths from config
    val silverDir: String = config.getString("paths.silverDir")
    val goldDir: String = config.getString("paths.goldDir")

    // Apply Spark configurations
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", config.getString("tuning.partitionOverwriteMode"))
    spark.conf.set("spark.sql.shuffle.partitions", config.getInt("tuning.shufflePartitions").toString)
    spark.conf.set("spark.sql.files.maxPartitionBytes", config.getString("tuning.maxPartitionBytes"))
    spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols",
      config.getInt("delta.stats.numIndexedCols").toString)
    spark.conf.set("spark.sql.legacy.timeParserPolicy", config.getString("tuning.legacyTimeParserPolicy"))

    // READ: Load data from Silver layer
    val dataFrames: Map[String, DataFrame] = readData(
      spark = spark,
      silverPath = silverDir,
      dateStart = firstDayOfMonth,
      dateEnd = lastDayOfMonth
    )

    // TRANSFORM: Compute business popularity metrics
    val popularityDF: DataFrame = transform(
      dataFrames = dataFrames,
      dateEnd = lastDayOfMonth,
      periodMonth = periodMonth
    )

    // PERSIST: Write results to Gold layer
    persist(
      spark = spark,
      df = popularityDF,
      goldPath = goldDir
    )
    logger.info(s"[BusinessPopularityAgg] Execution completed successfully for $periodMonth")
  }

  /**
   * READ: Load all required tables from Silver layer with date filtering
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


    val tablesToRead = Seq(tableNames.Business, tableNames.Review, tableNames.Tip, tableNames.Checkin)

    // READ: Load all tables from Delta and return a Map[String, DataFrame]
    val dataFrames: Map[String, DataFrame] = tablesToRead.map { table =>
      logger.info(s"[BusinessPopularityAgg] Reading table: $table for date range [$dateStart, $dateEnd]")
      table -> IOUtils.readDelta(
        spark = spark,
        path = s"$silverPath/${table}_snapshot",
        columns = BusinessPopularityUtils.selectColumns(table),
        conditions = BusinessPopularityUtils.filterConditions(table, dateStart, dateEnd)
      )
    }.toMap

    logger.info(s"[BusinessPopularityAgg] Successfully loaded ${dataFrames.size} tables")
    dataFrames
  }

  /**
   * TRANSFORM: Compute business popularity metrics from loaded DataFrames
   *
   * @param dataFrames Map of table name -> DataFrame (from readData)
   * @param dateEnd    end date for recency calculation
   * @param periodMonth period in YYYY-MM format
   * @return DataFrame with business popularity metrics and scores
   */
  private def transform(
                         dataFrames: Map[String, DataFrame],
                         dateEnd: String,
                         periodMonth: String
                       ): DataFrame = {

    logger.info("Starting transformation: aggregating metrics")

    // Aggregate review data
    val reviewAggDF: DataFrame = dataFrames(tableNames.Review)
      .groupBy(RawColumns.businessId)
      .agg(
        count("*").alias(BusinessPopularityColumns.reviewCount),
        avg(RawColumns.stars).alias(BusinessPopularityColumns.avgReviewStars),
        max(RawColumns.date).alias(BusinessPopularityColumns.lastReviewDate),
        min(RawColumns.date).alias(BusinessPopularityColumns.firstReviewDate)
      )

    // Aggregate checkin data
    val checkinAggDF: DataFrame = dataFrames(tableNames.Checkin)
      .groupBy(RawColumns.businessId)
      .agg(count("*").as(BusinessPopularityColumns.checkinCount))

    // Aggregate tip data
    val tipAggDF: DataFrame = dataFrames(tableNames.Tip)
      .groupBy(RawColumns.businessId)
      .agg(sum(RawColumns.complimentCount).as(BusinessPopularityColumns.tipComplimentCount))

    // Join all aggregated data together
    val businessPopularityDF: DataFrame = dataFrames(tableNames.Business)
      .join(reviewAggDF, Seq(RawColumns.businessId), "left")
      .join(checkinAggDF, Seq(RawColumns.businessId), "left")
      .join(tipAggDF, Seq(RawColumns.businessId), "left")
      .na.fill(0, Seq(
        BusinessPopularityColumns.reviewCount,
        BusinessPopularityColumns.avgReviewStars,
        BusinessPopularityColumns.checkinCount,
        BusinessPopularityColumns.tipComplimentCount
      ))

    logger.info("[BusinessPopularityAgg] Applying min-max normalization to metrics")

    // Apply Normalization (Min-Max Scaling) to popularity metrics
    val metricsToNormalize: Seq[(String, String)] = Seq(
      (BusinessPopularityColumns.avgReviewStars, BusinessPopularityColumns.normStars),
      (BusinessPopularityColumns.reviewCount, BusinessPopularityColumns.normReviewCount),
      (BusinessPopularityColumns.checkinCount, BusinessPopularityColumns.normCheckinCount),
      (BusinessPopularityColumns.tipComplimentCount, BusinessPopularityColumns.normTipCompliments)
    )

    val normalizedDF: DataFrame = metricsToNormalize.foldLeft(businessPopularityDF) {
      case (df, (sourceCol, targetCol)) =>
        BusinessPopularityUtils.minMaxNormalize(df, sourceCol, targetCol)
    }

    logger.info("[BusinessPopularityAgg] Computing recency boost and popularity score")

    // Recency boost: exponential decay based on days since lastReviewDate
    val daysSince: Column = when(col(BusinessPopularityColumns.lastReviewDate).isNull, lit(3650))
      .otherwise(datediff(to_date(lit(dateEnd)), to_date(col(BusinessPopularityColumns.lastReviewDate))))

    val recencyBoost: Column = exp(-daysSince.cast("double") / lit(30.0))

    val withRecencyDF: DataFrame = normalizedDF
      .withColumn(BusinessPopularityColumns.recencyBoost, recencyBoost)

    // Calculate final popularity score
    val resultDF: DataFrame = withRecencyDF.withColumn(
      BusinessPopularityColumns.popularityScore,
      col(BusinessPopularityColumns.normStars) * BusinessPopularityWeights.wStars +
        col(BusinessPopularityColumns.normReviewCount) * BusinessPopularityWeights.wReviewCount +
        col(BusinessPopularityColumns.recencyBoost) * BusinessPopularityWeights.wRecency +
        (col(BusinessPopularityColumns.normCheckinCount) + col(BusinessPopularityColumns.normTipCompliments)) *
          (BusinessPopularityWeights.wCheckTip / 2.0)
    )

    // Compute city rank based on popularity score
    val w: WindowSpec = Window.partitionBy(RawColumns.city).orderBy(col(BusinessPopularityColumns.popularityScore).desc)
    val finalDf: DataFrame = resultDF
      .withColumn(BusinessPopularityColumns.cityRank, rank().over(w))
      .select(
        lit(dateEnd).alias(transformColumns.day),
        lit(periodMonth).alias(BusinessPopularityColumns.periodMonth),
        lit(Granularity.Monthly).alias(transformColumns.granularity),
        col(RawColumns.businessId),
        col(RawColumns.name),
        col(RawColumns.city),
        col(RawColumns.state),
        col(RawColumns.categories),
        col(BusinessPopularityColumns.popularityScore),
        col(BusinessPopularityColumns.cityRank)
      )
    logger.info(s"[BusinessPopularityAgg] Transformation complete, total businesses: ${finalDf.count()}")
    finalDf
  }

  /**
   * PERSIST: Write business popularity results to Gold layer
   *
   * @param spark       SparkSession
   * @param df          DataFrame with business popularity metrics
   * @param goldPath    base path to Gold layer
   */
  private def persist(
                       spark: SparkSession,
                       df: DataFrame,
                       goldPath: String
                     ): Unit = {

    logger.info(s"[BusinessPopularityAgg] Writing business popularity data to Gold layer")

    val outputPath = s"$goldPath/business_popularity"

    // Add ingestion timestamp
    val dfWithTs: DataFrame = df
      .withColumn(transformColumns.goldIngestTs,
        current_timestamp()
      )

    // Define key columns for upsert (business_id + period_month)
    val keyCols: Seq[String] = Seq(RawColumns.businessId, transformColumns.day, transformColumns.granularity)
    val partitionCols: Seq[String] = Seq(transformColumns.day, transformColumns.granularity)

    // Upsert into Delta table
    IOUtils.upsertDeltaByKey(
      spark = spark,
      df = dfWithTs,
      outputPath = outputPath,
      keyCols = keyCols,
      partitionColumns = partitionCols
    )

    logger.info(s"[BusinessPopularityAgg] Successfully wrote ${df.count()} records to $outputPath")
  }
}