package com.yelpbatch.bronze
import com.yelpbatch.utils.{RawColumns, tableNames}
import org.apache.spark.sql.types._
object rawSchemas {

  private val business: StructType = StructType(Seq(
    StructField(RawColumns.businessId, StringType, nullable = false),
    StructField(RawColumns.name,        StringType, true),
    StructField(RawColumns.city,        StringType, true),
    StructField(RawColumns.state,       StringType, true),
    StructField(RawColumns.latitude,    DoubleType, true),
    StructField(RawColumns.longitude,   DoubleType, true),
    StructField(RawColumns.stars,       DoubleType, true),
    StructField(RawColumns.reviewCount,IntegerType, true),
    StructField(RawColumns.isOpen,     IntegerType, true),
    StructField(RawColumns.categories,  StringType,  true)
  ))

  private val user: StructType = StructType(Seq(
    StructField(RawColumns.userId,       StringType, false),
    StructField(RawColumns.name,          StringType, true),
    StructField(RawColumns.yelpingSince, StringType, true), // cast to date later
    StructField(RawColumns.reviewCount,  IntegerType, true),
    StructField(RawColumns.fans,          IntegerType, true),
    StructField(RawColumns.averageStars, DoubleType,  true),
    StructField(RawColumns.useful,        IntegerType, true),
    StructField(RawColumns.funny,         IntegerType, true),
    StructField(RawColumns.cool,          IntegerType, true),
    StructField(RawColumns.friends,       StringType,  true)
  ))

  private val review: StructType = StructType(Seq(
    StructField(RawColumns.reviewId,  StringType, false),
    StructField(RawColumns.userId,    StringType, true),
    StructField(RawColumns.businessId,StringType, true),
    StructField(RawColumns.stars,      DoubleType, true),
    StructField(RawColumns.useful,     IntegerType, true),
    StructField(RawColumns.funny,      IntegerType, true),
    StructField(RawColumns.cool,       IntegerType, true),
    StructField(RawColumns.date,       StringType,  true), // cast to date later
    StructField(RawColumns.text,       StringType,  true)
  ))

  private val checkin: StructType = StructType(Seq(
    StructField(RawColumns.businessId, StringType, false),
    StructField(RawColumns.date,        StringType, true) // explode later
  ))

  private val tip: StructType = StructType(Seq(
    StructField(RawColumns.userId,     StringType, true),
    StructField(RawColumns.businessId, StringType, true),
    StructField(RawColumns.text,        StringType, true),
    StructField(RawColumns.date,        StringType, true),   // cast to timestamp
    StructField(RawColumns.complimentCount, IntegerType, true)
  ))
  /** Helper to get schema by table name */
  def getSchema(tableName: String): StructType = {
    tableName.toLowerCase match {
      case tableNames.Business => business
      case tableNames.User => user
      case tableNames.Review => review
      case tableNames.Checkin => checkin
      case tableNames.Tip => tip
      case other => throw new IllegalArgumentException(s"No schema defined for $other")
    }
  }
}