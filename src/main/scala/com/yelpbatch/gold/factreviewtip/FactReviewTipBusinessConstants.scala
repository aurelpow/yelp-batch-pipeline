package com.yelpbatch.gold.factreviewtip

/** Centralized names to avoid hardcoding strings everywhere */
object ColNames {
  val measure = "measure" // measure ids column
  val units = "units" // measure values column
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