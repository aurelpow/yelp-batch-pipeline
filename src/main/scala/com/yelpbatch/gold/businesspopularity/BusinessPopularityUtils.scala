package com.yelpbatch.gold.businesspopularity

import com.yelpbatch.utils.{RawColumns, tableNames, transformColumns}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._

object BusinessPopularityUtils {

  def minMaxNormalize(df: DataFrame, colName: String, outName: String): DataFrame = {
    val stats = df.agg(min(col(colName)).as("minv"), max(col(colName)).as("maxv")).collect().head
    val minv = stats.getAs[Any]("minv")
    val maxv = stats.getAs[Any]("maxv")
    if (minv == null || maxv == null || minv == maxv) {
      df.withColumn(outName, lit(0.0))
    } else {
      df.withColumn(outName, (col(colName).cast("double") - lit(minv.toString.toDouble)) / (lit(maxv.toString.toDouble) - lit(minv.toString.toDouble)))
    }
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
    case tableNames.Checkin => Seq(col(transformColumns.checkinDate).between(dateStart, dateEnd))
    case _ => throw new IllegalArgumentException(s"Unknown table name: $table")
  }

  def selectColumns(table: String
                   ): Seq[String] = table match {
    case tableNames.Business =>
      Seq(RawColumns.businessId, RawColumns.name, RawColumns.city, RawColumns.state, RawColumns.categories)
    case tableNames.Review =>
      Seq(RawColumns.businessId, RawColumns.date, RawColumns.stars)
    case tableNames.Tip =>
      Seq(RawColumns.businessId, RawColumns.date, RawColumns.complimentCount)
    case tableNames.Checkin =>
      Seq(RawColumns.businessId, transformColumns.checkinDate)
    case _ => throw new IllegalArgumentException(s"Unknown table name: $table")
  }
}