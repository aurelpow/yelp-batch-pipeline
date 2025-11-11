package com.yelpbatch.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.yelpbatch.utils.{RawColumns, transformColumns}

object SilverTransforms {

  /**
   * Transform business table with data quality rules
   * @param bronzeTable: input bronze DataFrame
   * @return transformed DataFrame
   */
  def transformBusinessTable(bronzeTable: DataFrame): DataFrame = {
    bronzeTable
      .filter(col(RawColumns.businessId).isNotNull && col(RawColumns.businessId) =!= "")
      .withColumn(RawColumns.name, trim(col(RawColumns.name)))
      .withColumn(RawColumns.city, trim(initcap(col(RawColumns.city))))
      .withColumn(RawColumns.state, upper(trim(col(RawColumns.state))))
      .withColumn(RawColumns.categories, trim(col(RawColumns.categories)))
      // Data quality rules
      .withColumn(RawColumns.stars, when(col(RawColumns.stars) < 1 || col(RawColumns.stars) > 5, null)
        .otherwise(col(RawColumns.stars)).cast("integer"))
      .withColumn(RawColumns.reviewCount, when(col(RawColumns.reviewCount) < 0, 0)
        .otherwise(col(RawColumns.reviewCount)).cast("integer"))
      .withColumn(RawColumns.latitude, when(col(RawColumns.latitude) < -90 || col(RawColumns.latitude) > 90, null)
        .otherwise(col(RawColumns.latitude)).cast("double"))
      .withColumn(RawColumns.longitude, when(col(RawColumns.longitude) < -180 || col(RawColumns.longitude) > 180, null)
        .otherwise(col(RawColumns.latitude)).cast("double"))
      .withColumn(RawColumns.isOpen, col(RawColumns.isOpen).cast("boolean"))
      // Select final columns for business fact table
      .select(
        RawColumns.businessId,
        RawColumns.name,
        RawColumns.city,
        RawColumns.state,
        RawColumns.latitude,
        RawColumns.longitude,
        RawColumns.stars,
        RawColumns.reviewCount,
        RawColumns.isOpen,
        RawColumns.categories,
        transformColumns.ingestTs
    )
  }

  /**
   * Transform user table with data quality rules
   */
  def transformUserTable(bronzeTable: DataFrame): DataFrame = {
    bronzeTable
      .filter(col(RawColumns.userId).isNotNull && col(RawColumns.userId) =!= "")
      .withColumn(RawColumns.name, trim(col(RawColumns.name)))
      .withColumn(RawColumns.yelpingSince, to_date(col(RawColumns.yelpingSince), "yyyy-MM-dd"))
      .withColumn(RawColumns.friends, when(col(RawColumns.friends).isNull || col(RawColumns.friends) === "None", "")
        .otherwise(col(RawColumns.friends)))
      .withColumn(transformColumns.friendCount,
        when(col(RawColumns.friends) === "" || col(RawColumns.friends).isNull, 0)
          .otherwise(size(split(col(RawColumns.friends), ","))))
      // Data quality rules
      .withColumn(RawColumns.reviewCount, when(col(RawColumns.reviewCount) < 0, 0).otherwise(col(RawColumns.reviewCount)))
      .withColumn(RawColumns.fans, when(col(RawColumns.fans) < 0, 0).otherwise(col(RawColumns.fans)))
      .withColumn(RawColumns.averageStars,
        when(col(RawColumns.averageStars) < 1.0 || col(RawColumns.averageStars) > 5.0, null)
          .otherwise(col(RawColumns.averageStars)))
      .withColumn(RawColumns.useful, when(col(RawColumns.useful) < 0, 0).otherwise(col(RawColumns.useful)))
      .withColumn(RawColumns.funny, when(col(RawColumns.funny) < 0, 0).otherwise(col(RawColumns.funny)))
      .withColumn(RawColumns.cool, when(col(RawColumns.cool) < 0, 0).otherwise(col(RawColumns.cool)))
      .drop(RawColumns.friends) // Remove raw friends field, keep only count
      .select(
        RawColumns.userId,
        RawColumns.name,
        RawColumns.yelpingSince,
        RawColumns.reviewCount,
        RawColumns.fans,
        RawColumns.averageStars,
        RawColumns.useful,
        RawColumns.funny,
        RawColumns.cool,
        transformColumns.friendCount,
        transformColumns.ingestTs
      )
  }

  /**
   * Transform review table with data quality rules
   */
  def transformReviewTable(bronzeTable: DataFrame): DataFrame = {
    bronzeTable
      .filter(
        col(RawColumns.reviewId).isNotNull && col(RawColumns.reviewId) =!= "" &&
          col(RawColumns.userId).isNotNull && col(RawColumns.userId) =!= "" &&
          col(RawColumns.businessId).isNotNull && col(RawColumns.businessId) =!= ""
      )
      // Parse timestamp, then cast to date; fall back to plain date if needed
      .withColumn(
        RawColumns.date,
        coalesce(
          to_date(to_timestamp(col(RawColumns.date), "yyyy-MM-dd HH:mm:ss")),
          to_date(col(RawColumns.date), "yyyy-MM-dd")
        )
      )
      .withColumn(RawColumns.stars,
        when(col(RawColumns.stars) < 1 || col(RawColumns.stars) > 5, null)
          .otherwise(col(RawColumns.stars).cast("integer")))
      .withColumn(RawColumns.text, trim(col(RawColumns.text)))
      // Data quality rules
      .withColumn(RawColumns.useful, when(col(RawColumns.useful) < 0, 0).otherwise(col(RawColumns.useful)))
      .withColumn(RawColumns.funny, when(col(RawColumns.funny) < 0, 0).otherwise(col(RawColumns.funny)))
      .withColumn(RawColumns.cool, when(col(RawColumns.cool) < 0, 0).otherwise(col(RawColumns.cool)))
      .select(
        RawColumns.reviewId,
        RawColumns.userId,
        RawColumns.businessId,
        RawColumns.stars,
        RawColumns.useful,
        RawColumns.funny,
        RawColumns.cool,
        RawColumns.date,
        RawColumns.text,
        transformColumns.ingestTs
      )
  }

  /**
   * Transform checkin table with data quality rules
   */
  def transformCheckinTable(bronzeTable: DataFrame): DataFrame = {
    bronzeTable
      .filter(col(RawColumns.businessId).isNotNull && col(RawColumns.businessId) =!= "")
      .withColumn(RawColumns.date, explode(split(col(RawColumns.date), ",\\s*")))
      .filter(col(RawColumns.date) =!= "" && col(RawColumns.date).isNotNull)
      .withColumn(transformColumns.checkinDate,
        when(col(RawColumns.date).contains(" "),
          to_timestamp(col(RawColumns.date), "yyyy-MM-dd HH:mm:ss"))
          .otherwise(
            to_timestamp(concat(col(RawColumns.date), lit(" 00:00:00")), "yyyy-MM-dd HH:mm:ss")))
      .filter(col(transformColumns.checkinDate).isNotNull)
      .drop(RawColumns.date)
      .select(
        RawColumns.businessId,
        transformColumns.checkinDate,
        transformColumns.ingestTs
      )
  }

  /**
   * Transform tip table with data quality rules
   */
  def transformTipTable(bronzeTable: DataFrame): DataFrame = {
    bronzeTable
      .filter(
        col(RawColumns.userId).isNotNull && col(RawColumns.userId) =!= "" &&
          col(RawColumns.businessId).isNotNull && col(RawColumns.businessId) =!= ""
      )
      .withColumn(RawColumns.text, trim(col(RawColumns.text)))
      .withColumn(RawColumns.date, to_timestamp(col(RawColumns.date), "yyyy-MM-dd HH:mm:ss"))
      .withColumn(RawColumns.complimentCount,
        when(col(RawColumns.complimentCount) < 0, 0)
          .otherwise(col(RawColumns.complimentCount).cast("integer")))
      .select(
        RawColumns.userId,
        RawColumns.businessId,
        RawColumns.text,
        RawColumns.date,
        RawColumns.complimentCount,
        transformColumns.ingestTs
      )
  }
}
