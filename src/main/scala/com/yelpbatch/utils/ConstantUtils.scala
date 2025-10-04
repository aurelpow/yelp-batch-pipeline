package com.yelpbatch.utils

/** Granularity codes */
object Granularity {
  val Daily:    Int = 0
  val Monthly:  Int = 2
}

object RawColumns {
  val userId: String = "user_id"
  val businessId: String = "business_id"
  val date = "date"
  val ingestionDate = "ingestion_date"
  val sourceFile    = "source_file"
}