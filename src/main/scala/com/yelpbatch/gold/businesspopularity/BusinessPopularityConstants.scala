package com.yelpbatch.gold.businesspopularity

import com.yelpbatch.utils.tableNames

object BusinessPopularityColumns {
  val reviewCount: String = "review_count"
  val avgReviewStars: String = "avg_review_stars"
  val lastReviewDate: String = "last_review_date"
  val firstReviewDate: String = "first_review_date"
  val checkinCount: String = "checkin_count"
  val tipComplimentCount: String = "tip_compliment_count"
  val popularityScore: String = "popularity_score"
  val normStars: String = "norm_stars"
  val normReviewCount: String = "norm_review_count"
  val normCheckinCount: String = "norm_checkin_count"
  val normTipCompliments: String = "norm_tip_compliments"
  val recencyBoost: String = "recency_boost"
  val cityRank: String = "city_rank"
  val periodMonth: String = "period_month"
}

object BusinessPopularityWeights {
  val wStars = 0.50
  val wReviewCount = 0.25
  val wRecency = 0.15
  val wCheckTip = 0.10
}