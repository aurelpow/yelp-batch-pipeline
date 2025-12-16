package com.yelpbatch.silver

import com.typesafe.config.Config
import com.yelpbatch.utils.{IOUtils, RawColumns}
import io.delta.tables.DeltaTable
// Import com.yelpbatch.utils objects
import com.yelpbatch.utils.{tableNames, transformColumns}
// Import spark packages
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SilverIngest - transform Bronze -> Silver (daily-partitioned outputs)
 *
 * This object handles the transformation of data from the Bronze layer to the Silver layer
 * in a Delta Lake architecture. The Silver layer is partitioned by day, and only the requested
 * day partition is written during each run.
 *
 * Key Features:
 * - Bronze remains a full source scan: The Bronze layer contains raw data, and this process
 *   reads the entire table or a filtered subset based on the run date.
 * - Silver writes ONLY the requested day partition: The Silver layer stores cleaned and
 *   transformed data, partitioned by day, at the following path:
 *   -   data/silver/<"table">_snapshot/day=YYYY-MM-DD
 */
object SilverIngest {

  // Logger
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /** Main entry from Runner
   *
   * @param spark     : SparkSession
   * @param config    : application config
   * @param runDate   : run date string YYYY-MM-DD
   * @param fullLoad  : if true, process full table without date filtering
   * @param tablesOpt : optional comma-separated table names to process
   * */
  def run(spark: SparkSession,
          config: Config,
          runDate: String,
          fullLoad: Boolean,
          tablesOpt: Option[String] = None): Unit = {
    // Validate required parameters
    require(runDate.nonEmpty, "runDate is required: yyyy-MM-dd")

    val bronzeDir = config.getString("paths.bronzeDir")
    val silverDir = config.getString("paths.silverDir")

    // Get tables from parameter or config
    val tablesCsv = tablesOpt.getOrElse(config.getString("app.tables"))
    val inputTables = tablesCsv.split(",").map(_.trim).filter(_.nonEmpty).distinct.toSeq

    // Apply Spark configurations
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", config.getString("tuning.partitionOverwriteMode"))
    spark.conf.set("spark.sql.shuffle.partitions", config.getInt("tuning.shufflePartitions").toString)
    spark.conf.set("spark.sql.files.maxPartitionBytes", config.getString("tuning.maxPartitionBytes"))
    spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols",
      config.getInt("delta.stats.numIndexedCols").toString)
    spark.conf.set("spark.sql.legacy.timeParserPolicy", config.getString("tuning.legacyTimeParserPolicy"))

    val maxRecsPerFile = if (config.hasPath("writer.maxRecordsPerFile"))
      config.getInt("writer.maxRecordsPerFile").toString else "50000"

    val hfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    logger.info(s"Processing tables: ${inputTables.mkString(", ")} for date: $runDate")

    inputTables.foreach { tableName =>
      processTable(spark, hfs, bronzeDir, silverDir, tableName, runDate, maxRecsPerFile, fullLoad)
    }

    logger.info("All tables processed successfully.")
  }

  /**
   * Process a single table from Bronze to Silver layer with daily partitioning
   */
  private def processTable(
                            spark: SparkSession,

                            hfs: FileSystem,
                            bronzeDir: String,
                            silverDir: String,
                            tableName: String,
                            runDate: String,
                            maxRecsPerFile: String,
                            fullLoad: Boolean,
                          ): Unit = {

    val inputPath = s"$bronzeDir/$tableName"
    val outputPath = s"$silverDir/${tableName}_snapshot"

    if (!hfs.exists(new Path(inputPath))) {
      logger.info(s"[$tableName] SKIP: bronze table not found -> $inputPath")
      return
    }

    logger.info(s"[$tableName] Processing bronze table: $inputPath")

    // Read bronze table
    val bronzeTable = spark.read.format("delta").load(inputPath)

    // Filter by date first for event-based tables to reduce data volume before transformation
    val filteredBronze = if (!fullLoad) {
      filterOrSnapshotByDate(tableName, bronzeTable, runDate)
    } else {
      bronzeTable
    }

    // Apply transformations only on filtered data
    val silverTable = transformTable(tableName, filteredBronze)
      .withColumn(transformColumns.silverIngestTs, current_timestamp())
      .withColumn(transformColumns.day, lit(runDate))

    if (silverTable.isEmpty) {
      logger.info(s"[$tableName] No data for date: $runDate")
      return
    }

    // Insert or overwrite the daily partition in Silver
    IOUtils.writeDelta(
      df = silverTable,
      outputPath = outputPath,
      mode = "overwrite",
      partitionBy = Seq(transformColumns.day),
      options = Map("maxRecordsPerFile" -> maxRecsPerFile)
    )
    logger.info(s"[$tableName] Wrote day=$runDate to $outputPath")
  }

  /**
   * Apply transformations based on table type
   */
  private def transformTable(tableName: String, bronzeTable: DataFrame): DataFrame = {
    tableName.toLowerCase match {
      case tableNames.Business => SilverTransforms.transformBusinessTable(bronzeTable)
      case tableNames.User => SilverTransforms.transformUserTable(bronzeTable)
      case tableNames.Review => SilverTransforms.transformReviewTable(bronzeTable)
      case tableNames.Checkin => SilverTransforms.transformCheckinTable(bronzeTable)
      case tableNames.Tip => SilverTransforms.transformTipTable(bronzeTable)
      case _ =>
        logger.info(s"[$tableName] WARN: No transformation rules defined, copying as-is")
        bronzeTable
    }
  }

  /**
   * Filter event tables by date or snapshot dimension tables for a specific date
   */
  private def filterOrSnapshotByDate(tableName: String, table: DataFrame, runDate: String): DataFrame = {
    tableName.toLowerCase match {
      case tableNames.Review =>
        table.filter(to_date(col(RawColumns.date)) === lit(runDate))
      case tableNames.Tip =>
        table.filter(to_date(col(RawColumns.date)) === lit(runDate))
      case tableNames.Business | tableNames.User | tableNames.Checkin =>
        // For dimension tables, take snapshot as of runDate
        table
      case _ =>
        table
    }
  }
}