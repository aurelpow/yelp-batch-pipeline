package com.yelpbatch.gold.factreviewtip

import com.yelpbatch.utils.{DateUtils, RawColumns, tableNames, transformColumns}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object FactReviewTipBusinessUtils {

  private def aggMetrics(df: DataFrame, metrics: Seq[Column]): DataFrame =
    df.groupBy(RawColumns.businessId)
      .agg(array(metrics: _*).as("metrics"))
      .select(col(RawColumns.businessId), explode(col("metrics")).as("m"))
      .select(
        col(RawColumns.businessId),
        col("m").getField(ColNames.measure).as(ColNames.measure),
        col("m").getField(ColNames.units).cast("double").as(ColNames.units)
      )

  // tips (note count and coalesce)
  private val tipMetrics: Seq[Column] = Seq(
    struct(lit(MeasureFactReviewTip.tipCount).as(ColNames.measure),
      count(lit(1)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.complimentCountSum).as(ColNames.measure),
      sum(coalesce(col(RawColumns.complimentCount), lit(0))).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.distinctUsersTip).as(ColNames.measure),
      countDistinct(col(RawColumns.userId)).cast("double").as(ColNames.units))
  )

  // reviews
  private val reviewMetrics: Seq[Column] = Seq(
    struct(lit(MeasureFactReviewTip.reviewCount).as(ColNames.measure),
      countDistinct(col(RawColumns.reviewId)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.distinctUsersReview).as(ColNames.measure),
      countDistinct(col(RawColumns.userId)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.avgStars).as(ColNames.measure),
      avg(col(RawColumns.stars)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.totalUseful).as(ColNames.measure),
      sum(col(RawColumns.useful)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.totalFunny).as(ColNames.measure),
      sum(col(RawColumns.funny)).cast("double").as(ColNames.units)),
    struct(lit(MeasureFactReviewTip.totalCool).as(ColNames.measure),
      sum(col(RawColumns.cool)).cast("double").as(ColNames.units))
  )

  /**
   * Get Metrics for FactReviewTipBusiness
   * @param runDate The run date in YYYY-MM-DD format
   * @param reviewDF DataFrame containing review data
   * @param tipDF DataFrame containing tip data
   * @param granularity Granularity level (e.g., daily, monthly)
   * @return DataFrame with computed metrics
   */
  def getMetrics(runDate: String,
                              reviewDF: DataFrame,
                              tipDF: DataFrame,
                              granularity: Int,
                            ): DataFrame = {
    val combined =
      aggMetrics(reviewDF, reviewMetrics)
        .unionByName(aggMetrics(tipDF, tipMetrics))
        .withColumns(Map(
          (transformColumns.day, lit(runDate)),
          (transformColumns.periodMonth, lit(DateUtils.toYearMonth(runDate))),
          (transformColumns.granularity, lit(granularity))))
        .select(
          col(transformColumns.day),
          col(transformColumns.periodMonth),
          col(transformColumns.granularity),
          col(RawColumns.businessId),
          col(ColNames.measure),
          col(ColNames.units)
        )
    combined
  }

  /** Generate filter conditions based on table and date range
   * @param table table name
   * @param dateStart start date for filtering
   * @param dateEnd end date for filtering
   * @return sequence of Column conditions
   */
  def filterConditions(table: String,
                       dateStart: String,
                       dateEnd: String
                      ): Seq[Column] = table match {
    case tableNames.Business => Seq(col(transformColumns.day) === dateEnd, col(RawColumns.isOpen) === lit(1))
    case tableNames.Review => Seq(col(RawColumns.date).between(dateStart, dateEnd))
    case tableNames.Tip => Seq(col(RawColumns.date).between(dateStart, dateEnd))
    case _ => throw new IllegalArgumentException(s"Unknown table name: $table")
  }

  def selectColumns(table: String
                   ): Seq[String] = table match {
    case tableNames.Business =>
      Seq(RawColumns.businessId, RawColumns.name, RawColumns.city, RawColumns.state, RawColumns.categories)
    case tableNames.Review =>
      Seq(RawColumns.businessId, RawColumns.reviewId, RawColumns.userId,RawColumns.date, RawColumns.stars,
        RawColumns.useful, RawColumns.funny, RawColumns.cool)
    case tableNames.Tip =>
      Seq(RawColumns.businessId, RawColumns.userId, RawColumns.date, RawColumns.complimentCount)
    case _ => throw new IllegalArgumentException(s"Unknown table name: $table")
  }
}
