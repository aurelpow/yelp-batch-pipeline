package com.yelpbatch.gold.factreviewtip

// Imports spark packages
import com.typesafe.config.{Config}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import com.yelpbatch.utils.{IOUtils,DateUtils, Granularity, RawColumns }

object FactReviewTipBusinessAgg {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /** Main entry from Runner
   *
   * @param spark         : SparkSession
   * @param config        : application config
   * @param runDateStr    : run date string YYYY-MM-DD
   * @param forceMonthOpt : optional YYYY-MM to force monthly run
   * @param skipDaily     : if true, skip daily computation
   * @param dryRun        : if true, skip writes
   * */
  def run(
           spark: SparkSession,
           config: Config,
           runDateStr: String,
           forceMonthOpt: Option[String],
           skipDaily: Boolean,
           dryRun: Boolean
         ): Unit = {
    logger.info(s"Entering FactReviewTipBusinessAgg.run for runDate=$runDateStr skipDaily=$skipDaily dryRun=$dryRun")

    // Load paths from config
    val paths = (
      config.getString("paths.silver.review"),
      config.getString("paths.silver.tip"),
      config.getString("paths.gold.factReviewTipMetrics")
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
      val monthly = computeMonthly(spark, runDate, paths)
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
        col("m").getField(ColNames.measure).as(ColNames.measure),
        col("m").getField(ColNames.units).cast("double").as(ColNames.units)
      )

  // tips (note count and coalesce)
  private val tipMetrics = Seq(
    struct(lit(MeasureFactReviewTip.tipCount).as(ColNames.measure),
      count(lit(1)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.complimentCountSum).as(ColNames.measure),
      sum(coalesce(col(RawColumns.complimentCount), lit(0))).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.distinctUsersTip).as(ColNames.measure),
      countDistinct(col(ColNames.userID)).cast("double").as(ColNames.units))
  )

  // reviews
  private val reviewMetrics = Seq(
    struct(lit(MeasureFactReviewTip.reviewCount).as(ColNames.measure),
      countDistinct(col(RawColumns.reviewId)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.distinctUsersReview).as(ColNames.measure),
      countDistinct(col(ColNames.userID)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.avgStars).as(ColNames.measure),
      avg(col(RawColumns.stars)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.totalUseful).as(ColNames.measure),
      sum(col(RawColumns.useful)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.totalFunny).as(ColNames.measure),
      sum(col(RawColumns.funny)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.totalCool).as(ColNames.measure),
      sum(col(RawColumns.cool)).cast("double").as(ColNames.units))
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
      .filter(col(ColNames.date).between(lit(monthStart), lit(monthEnd)))
    val tipDF = spark.read.format("delta").load(tipPath)
      .filter(col(ColNames.date).between(lit(monthStart), lit(monthEnd)))

    val combined =
      aggMetrics(reviewDF, reviewMetrics)
        .unionByName(aggMetrics(tipDF, tipMetrics))
        .withColumns(Map(
          (ColNames.date, lit(runDate)),
          (ColNames.periodMonth, lit(DateUtils.toYearMonth(runDate))),
          (ColNames.granularity, lit(Granularity.Monthly)),
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
    val keys = Seq(ColNames.date, ColNames.businessId, ColNames.measure, ColNames.granularity)
    IOUtils.upsertDeltaByKey(spark, df, outputPath, keys, Seq(ColNames.date, ColNames.granularity))
  }
}