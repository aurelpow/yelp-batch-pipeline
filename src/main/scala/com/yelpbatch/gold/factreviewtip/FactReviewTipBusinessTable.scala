package com.yelpbatch.gold.factreviewtip

/** Centralized names to avoid hardcoding strings everywhere */
object ColNames {
  // Keys
  val businessId = "business_id"
  val userID = "user_id"
  val granularity = "granularity" // 0 daily, 2 monthly
  val periodMonth = "period_month" // YYYY-MM
  val date = "date" // original date column from source
  val measure = "measure" // measure ids column
  val units = "units" // measure values column
  val DtAudModification = "dt_aud_modification"
}

/** Measure codes  */
object MeasureFactReviewTip {
  // Measure mapping to columns (wide format)
  val reviewCount: Int =  1
  val tipCount: Int = 2
  val complimentCountSum: Int = 3
  val avgStars: Int = 4
  val totalUseful: Int = 5
  val totalFunny: Int = 6
  val totalCool: Int = 7
  val distinctUsersReview: Int = 8
  val distinctUsersTip: Int = 9
}