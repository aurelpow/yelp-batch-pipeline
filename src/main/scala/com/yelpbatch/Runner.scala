package com.yelpbatch

import com.yelpbatch.utils.DateUtils
import com.yelpbatch.gold.FactReviewTip.FactReviewTipBusinessAgg
import org.apache.spark.sql.SparkSession

object Runner {
  def main(args: Array[String]): Unit = {
    val m = args.sliding(2,2).collect{case Array(k,v)=>k.stripPrefix("--")->v}.toMap
    val process    = m.getOrElse("process", sys.error("Missing --process"))
    val env        = m.getOrElse("env","local")
    val runDateOpt = m.get("run_date")            // YYYY-MM-DD (single day)
    val startOpt   = m.get("start_date")          // YYYY-MM-DD
    val endOpt     = m.get("end_date")            // YYYY-MM-DD
    val forceMonth = m.get("force_monthly")       // YYYY-MM
    val skipDaily  = m.contains("skip_daily")
    val dryRun     = m.contains("dry_run")

    // derive the execution dates
    val runDates: Seq[String] =
      (runDateOpt, startOpt, endOpt) match {
        case (Some(d), None, None) => Seq(DateUtils.normalizeDate(d))
        case (None, Some(s), Some(e)) => DateUtils.dateRange(s, e)
        case _ => sys.error("Provide either --run_date YYYY-MM-DD or --start_date & --end_date")
      }

    val spark = SparkSession.builder()
      .appName(s"YelpBatch-$process")
      .getOrCreate()

    process match {
      case "fact_review_tip_metrics_wide" =>
        runDates.foreach { d =>
          FactReviewTipBusinessAgg.run(spark, d, forceMonth, skipDaily, dryRun)
        }
      case "bronze_ingest" => BronzeIngest.run(spark, runDates.head)
      case "silver_ingest" => SilverIngest.run(spark, runDates.head)
      case other => sys.error(s"Unknown process: $other")
    }

    spark.stop()
  }
}
