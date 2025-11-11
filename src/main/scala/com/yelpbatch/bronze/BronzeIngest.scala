package com.yelpbatch.bronze

import com.typesafe.config.Config
import com.yelpbatch.utils.{IOUtils, transformColumns}
import org.apache.spark.sql.functions.{col, current_timestamp, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * BronzeIngest - Spark application for ingesting raw Yelp dataset JSON files
 * into a Delta Lake bronze layer as part of a data lakehouse architecture.
 *
 * Input:
 *   - Yelp JSON files by table (e.g., yelp_academic_dataset_business.json)
 * Output:
 *   - Delta tables partitioned by `_ingest_date` under `bronzeDir/<table>`
 *
 * Usage:
 *   - Processes all tables listed in configuration, or a specific table via command line argument.
 */
object BronzeIngest {

  // Logger
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /** Main entry from Runner
   *
   * @param spark         : SparkSession
   * @param cfg        : application config
   * @param tablesOpt    : optional comma-separated table names to process
   * */
  def run(spark: SparkSession, cfg: Config, tablesOpt: Option[String] = None): Unit = {

    // Extract config values
    val mongoEnabled = cfg.getBoolean("mongodb.enabled")
    val mongoUri = if (mongoEnabled) cfg.getString("mongodb.uri") else ""
    val mongoDatabase = if (mongoEnabled) cfg.getString("mongodb.database") else ""
    val srcDir = cfg.getString("paths.rawDir")
    val bronzeDir = cfg.getString("paths.bronzeDir")
    val writeMode = "overwrite"
    val maxRecordsPerFile = cfg.getString("writer.maxRecordsPerFile")

    // Load tables to process
    val tablesCsv = tablesOpt.getOrElse(cfg.getString("app.tables"))
    val tablesToProcess = tablesCsv.split(",").map(_.trim).filter(_.nonEmpty).toSeq

    tablesToProcess.foreach { table =>
      logger.info(s"[$table] Starting ingestion...")

      // Read from MongoDB or filesystem based on config
      val rawDataTry: Try[DataFrame] = if (mongoEnabled) {
        logger.info(s"[$table] Reading from MongoDB: ${mongoDatabase}.$table")
        IOUtils.readFromMongoDG(
          spark,
          mongoUri = mongoUri,
          database = mongoDatabase,
          collection = table,
          schema = Some(rawSchemas.getSchema(table))
        )
      } else {
        val inputPath = s"$srcDir/yelp_academic_dataset_${table}.json"
        logger.info(s"[$table] Reading from filesystem: $inputPath")
        IOUtils.readJsonFromFileSystem(
          spark,
          inputPath,
          schema = Some(rawSchemas.getSchema(table))
        )
      }

      rawDataTry match {
        case Success(raw) =>
          // Add ingestion metadata columns
          val withMeta = raw
            .withColumn(transformColumns.ingestTs, current_timestamp())
            .withColumn(transformColumns.ingestDate, to_date(col(transformColumns.ingestTs)))

          val recordCount = withMeta.count()
          logger.info(s"[$table] Processing $recordCount records")

          // Write to Delta Lake bronze layer
          val outputPath = s"$bronzeDir/$table"
          logger.info(s"[$table] Writing to Delta : $outputPath (mode=$writeMode)")

          IOUtils.writeDelta(
            df = withMeta,
            outputPath = outputPath,
            mode = writeMode,
            partitionBy = Seq(transformColumns.ingestDate),
            options = Map("maxRecordsPerFile" -> maxRecordsPerFile)
          )

          logger.info(s"[$table] ✓ Success")
        case Failure(ex) =>
          logger.error(s"[$table] ✗ SKIP: ${ex.getMessage}")
      }
    }
    logger.info("[BronzeIngest] All done.")
  }
}