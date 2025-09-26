package com.yelpbatch
import org.apache.spark.sql.types._
object Schemas {

  val business: StructType = StructType(Seq(
    StructField("business_id", StringType, nullable = false),
    StructField("name",        StringType, true),
    StructField("city",        StringType, true),
    StructField("state",       StringType, true),
    StructField("latitude",    DoubleType, true),
    StructField("longitude",   DoubleType, true),
    StructField("stars",       DoubleType, true),
    StructField("review_count",IntegerType, true),
    StructField("is_open",     IntegerType, true),
    StructField("categories",  StringType,  true)
  ))

  val user: StructType = StructType(Seq(
    StructField("user_id",       StringType, false),
    StructField("name",          StringType, true),
    StructField("yelping_since", StringType, true), // cast to date later
    StructField("review_count",  IntegerType, true),
    StructField("fans",          IntegerType, true),
    StructField("average_stars", DoubleType,  true),
    StructField("useful",        IntegerType, true),
    StructField("funny",         IntegerType, true),
    StructField("cool",          IntegerType, true),
    StructField("friends",       StringType,  true)
  ))

  val review: StructType = StructType(Seq(
    StructField("review_id",  StringType, false),
    StructField("user_id",    StringType, true),
    StructField("business_id",StringType, true),
    StructField("stars",      DoubleType, true),
    StructField("useful",     IntegerType, true),
    StructField("funny",      IntegerType, true),
    StructField("cool",       IntegerType, true),
    StructField("date",       StringType,  true), // cast to date later
    StructField("text",       StringType,  true)
  ))

  val checkin: StructType = StructType(Seq(
    StructField("business_id", StringType, false),
    StructField("date",        StringType, true) // explode later
  ))

  val tip: StructType = StructType(Seq(
    StructField("user_id",     StringType, true),
    StructField("business_id", StringType, true),
    StructField("text",        StringType, true),
    StructField("date",        StringType, true),   // cast to timestamp
    StructField("compliment_count", IntegerType, true)
  ))
  /** Helper to get schema by table name */
  def getSchema(tableName: String): StructType = {
    tableName.toLowerCase match {
      case "business" => business
      case "user" => user
      case "review" => review
      case "checkin" => checkin
      case "tip" => tip
      case other => throw new IllegalArgumentException(s"No schema defined for $other")
    }
  }
}