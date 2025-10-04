package com.yelpbatch.utils

import java.util.Properties
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object IOUtils {

  /** Write DataFrame as Delta with optional partitioning and writer options */
  def writeDelta(
                  df: DataFrame,
                  outputPath: String,
                  mode: String = "append",
                  partitionBy: Seq[String] = Seq.empty,
                  options: Map[String, String] = Map.empty
                ): Unit = {
    var writer = df.write.format("delta").mode(mode)
    if (partitionBy.nonEmpty) writer = writer.partitionBy(partitionBy: _*)
    options.foreach { case (k, v) => writer = writer.option(k, v) }
    writer.save(outputPath)
  }

  /** Idempotent upsert (merge) into Delta by key columns. Creates target if missing. */
  def upsertDeltaByKey(
                        spark: SparkSession,
                        df: DataFrame,
                        outputPath: String,
                        keyCols: Seq[String]
                      ): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fsPath = new org.apache.hadoop.fs.Path(outputPath)
    val hfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    if (hfs.exists(fsPath)) {
      val deltaTable = DeltaTable.forPath(spark, outputPath)
      val joinCondition = keyCols.map(k => s"target.$k = source.$k").mkString(" AND ")
      deltaTable.as("target")
        .merge(df.as("source"), joinCondition)
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    } else {
      // create initial table
      df.write
        .format("delta")
        .mode("overwrite")
        .save(outputPath)
    }
  }

  /**
   * Register a Delta path as a metastore table and create/replace a view.
   * Use in Databricks / Spark with a metastore so SQL consumers can query.
   *
   * tableName: fully qualified metastore name (e.g. db.schema.table) or simple name
   * viewName: convenience view name for analysts
   */
  def registerDeltaTableAndView(
                                 spark: SparkSession,
                                 deltaPath: String,
                                 tableName: String,
                                 viewName: String
                               ): Unit = {
    // create table if missing
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING DELTA LOCATION '$deltaPath'")
    // optional view for easier consumption
    spark.sql(s"CREATE OR REPLACE VIEW $viewName AS SELECT * FROM $tableName")
  }
}
