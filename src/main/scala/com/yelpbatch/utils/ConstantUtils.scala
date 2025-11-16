package com.yelpbatch.utils

/** Granularity codes */
object Granularity {
  val Daily:    Int = 0
  val Monthly:  Int = 2
}

/** Centralized raw column names to avoid hardcoding strings everywhere */
object RawColumns {
  val userId: String = "user_id"
  val businessId: String = "business_id"
  val name: String = "name"
  val city: String = "city"
  val state: String = "state"
  val latitude: String = "latitude"
  val longitude: String = "longitude"
  val isOpen: String = "is_open"
  val categories: String = "categories"
  val yelpingSince: String = "yelping_since"
  val reviewCount: String = "review_count"
  val fans: String = "fans"
  val averageStars: String = "average_stars"
  val friends: String = "friends"
  val reviewId: String = "review_id"
  val date: String = "date"
  val text: String = "text"
  val complimentCount: String = "compliment_count"
  val stars: String = "stars"
  val useful: String = "useful"
  val funny: String = "funny"
  val cool: String = "cool"
}

object tableNames {
  val Business: String = "business"
  val User: String = "user"
  val Review: String = "review"
  val Checkin: String = "checkin"
  val Tip: String = "tip"
}

object transformColumns {
  val ingestTs: String = "_ingest_ts"
  val ingestDate: String = "ingest_date"
  val silverIngestTs: String = "_silver_ingest_ts"
  val checkinDate: String = "checkin_date"
  val friendCount: String = "friend_count"
  val day: String = "day"
  val goldIngestTs: String = "_gold_ingest_ts"
  val granularity: String = "granularity"
  val periodMonth: String = "period_month"
}