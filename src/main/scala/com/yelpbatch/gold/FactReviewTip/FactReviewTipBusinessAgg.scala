package com.yelpbatch.gold.FactReviewTip

// Imports spark packages
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import com.yelpbatch.utils.IOUtils
import com.yelpbatch.utils.{DateUtils, Granularity, RawColumns}

object FactReviewTipBusinessAgg {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /** Main entry from Runner */
  def run(
           spark: SparkSession,
           runDateStr: String,
           forceMonthOpt: Option[String],
           skipDaily: Boolean,
           dryRun: Boolean
         ): Unit = {
    logger.info(s"Entering FactReviewTipBusinessAgg.run for runDate=$runDateStr skipDaily=$skipDaily dryRun=$dryRun")

    val defaultCfg = ConfigFactory.parseResources("app.local.conf").resolve()
    val cfg = ConfigFactory.load().withFallback(defaultCfg).resolve()

    def required(key: String): String =
      if (cfg.hasPath(key)) cfg.getString(key)
      else throw new RuntimeException(s"Missing config key: $key - add it to application.conf or app.local.conf")

    val paths = (
      required("paths.silver.review"),
      required("paths.silver.tip"),
      required("paths.gold.factReviewTipMetrics")
    )

    logger.info(s"Configured paths -> review=$paths._1 tip=$paths._2 gold=${paths._3}")

    val runDate = DateUtils.normalizeDate(runDateStr)
    val isEom = DateUtils.isEndOfMonthStr(runDate)
    val ym = DateUtils.toYearMonth(runDate)
    logger.info(s"Computed runDate=$runDate isEom=$isEom ym=$ym")

    // Always compute daily metrics unless skipDaily is set
    if (!skipDaily) {
      logger.info(s"Computing daily metrics for $runDate")
      val daily = computeDaily(spark, runDate, paths)
      if (!dryRun) {
        logger.info(s"Writing daily metrics to $paths._3")
      writeWide(spark, daily, paths._3)
      } else logger.info("Dry run enabled - skipping write")
    }

    // Compute monthly metrics if runDate is end-of-month or forceMonthOpt matches
    val monthlyToRun = isEom || forceMonthOpt.contains(ym)
    if (monthlyToRun) {
      logger.info(s"Computing monthly metrics for $ym")
      val monthly = computeMonthly(spark, ym, paths)
      if (!dryRun) {
        logger.info(s"Writing monthly metrics to $paths._3")
        writeWide(spark, monthly, paths._3)
      } else logger.info("Dry run enabled - skipping monthly write")
    }
    logger.info("Exiting FactReviewTipBusinessAgg.run")
  }

  // helper to aggregate review metrics or tip metrics
  private def aggMetrics(df: DataFrame, metrics: Seq[Column]): DataFrame =
    df.groupBy(ColNames.businessId)
      .agg(array(metrics: _*).as("metrics"))
      .select(col(ColNames.businessId), explode(col("metrics")).as("m"))
      .select(
        col(ColNames.businessId),
        col("m.measure").as(ColNames.measure),
        col("m.units").cast("double").as(ColNames.units)
      )

  // tips (note count and coalesce)
  private val tipMetrics = Seq(
    struct(lit(MeasureFactReviewTip.tipCount).as("measure"),
      count(lit(1)).cast("double").as("units")),
    struct(lit(MeasureFactReviewTip.complimentCountSum).as("measure"),
      sum(coalesce(col("compliment_count"), lit(0))).cast("double").as("units")),
    struct(lit(MeasureFactReviewTip.distinctUsersTip).as("measure"),
      countDistinct(col(ColNames.userID)).cast("double").as("units"))
  )


  // reviews
  private val reviewMetrics = Seq(
    struct(lit(MeasureFactReviewTip.reviewCount).as("measure"),
      countDistinct(col("review_id")).cast("double").as("units")),
    struct(lit(MeasureFactReviewTip.distinctUsersReview).as("measure"),
      countDistinct(col(ColNames.userID)).cast("double").as("units")),
    struct(lit(MeasureFactReviewTip.avgStars).as("measure"),
      avg(col("stars")).cast("double").as("units")),
    struct(lit(MeasureFactReviewTip.totalUseful).as("measure"),
      sum(col("useful")).cast("double").as("units")),
    struct(lit(MeasureFactReviewTip.totalFunny).as("measure"),
      sum(col("funny")).cast("double").as("units")),
    struct(lit(MeasureFactReviewTip.totalCool).as("measure"),
      sum(col("cool")).cast("double").as("units"))
  )

  // compute daily metrics
  private def computeDaily(
                            spark: SparkSession,
                            runDate: String,
                            paths: (String, String, String)
                          ): DataFrame = {
    val (reviewPath, tipPath, _) = paths

    // Load Inputs and filter them (review, tip)
    val reviewDF = spark.read.format("delta").load(reviewPath)
      .filter(to_date(col(RawColumns.date)) === lit(runDate))
    val tipDF = spark.read.format("delta").load(tipPath)
      .filter(to_date(col(RawColumns.date)) === lit(runDate))

    val combined =
      aggMetrics(reviewDF, reviewMetrics)
        .unionByName(aggMetrics(tipDF, tipMetrics))
        .withColumns(Map(
        (ColNames.date, lit(runDate)),
        (ColNames.periodMonth, lit(DateUtils.toYearMonth(runDate))),
        (ColNames.granularity, lit(Granularity.Daily)),
        (ColNames.DtAudModification, current_timestamp().cast(TimestampType))))
      .select(
        col(ColNames.businessId),
        col(ColNames.date),
        col(ColNames.periodMonth),
        col(ColNames.granularity),
        col(ColNames.measure),
        col(ColNames.units),
        col(ColNames.DtAudModification)
      )
    combined
  }

  private def computeMonthly(
                              spark: SparkSession,
                              runDate: String,
                              paths: (String, String, String)
                            ): DataFrame = {
    val (reviewPath, tipPath, _) = paths
    val monthStart = DateUtils.toMonthStart(runDate)
    val monthEnd = DateUtils.toMonthEnd(runDate)
    // Load Inputs and filter them (review, tip)
    val reviewDF = spark.read.format("delta").load(reviewPath)
      .filter(col("date").between(lit(monthStart), lit(monthEnd)))
    val tipDF = spark.read.format("delta").load(tipPath)
      .filter(col("date").between(lit(monthStart), lit(monthEnd)))

    val combined =
      aggMetrics(reviewDF, reviewMetrics)
        .unionByName(aggMetrics(tipDF, tipMetrics))
        .withColumns(Map(
          (ColNames.date, lit(runDate)),
          (ColNames.periodMonth, lit(DateUtils.toYearMonth(runDate))),
          (ColNames.granularity, lit(Granularity.Daily)),
          (ColNames.DtAudModification, current_timestamp().cast(TimestampType))))
        .select(
          col(ColNames.businessId),
          col(ColNames.date),
          col(ColNames.periodMonth),
          col(ColNames.granularity),
          col(ColNames.measure),
          col(ColNames.units),
          col(ColNames.DtAudModification)
        )
    combined
  }
  // Write output in wide format (one row per business_id + measure)
  private def writeWide(spark: SparkSession, df: DataFrame, outputPath: String): Unit = {
    // choose compound key to avoid duplicates for same business/month/measure/granularity
    val keys = Seq("business_id", "period_month", "measure", "granularity")
    IOUtils.upsertDeltaByKey(spark, df, outputPath, keys)
  }
}