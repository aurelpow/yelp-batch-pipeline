package com.yelpbatch.bronze

import com.typesafe.config.Config
import com.yelpbatch.utils.{IOUtils, Observability, transformColumns}
import org.apache.spark.sql.functions.{col, current_timestamp, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer

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
    val mongoEnabled: Boolean = cfg.getBoolean("mongodb.enabled")
    val mongoUri: String = if (mongoEnabled) cfg.getString("mongodb.uri") else ""
    val mongoDatabase: String = if (mongoEnabled) cfg.getString("mongodb.database") else ""
    val srcDir: String = cfg.getString("paths.rawDir")
    val bronzeDir: String = cfg.getString("paths.bronzeDir")
    val writeMode: String = "overwrite"
    val maxRecordsPerFile: String = cfg.getString("writer.maxRecordsPerFile")

    // Failure Strategy: fail-fast, fail-at-end (default), skip-and-warn
    val failureStrategy: String = if (cfg.hasPath("app.failureStrategy"))
      cfg.getString("app.failureStrategy") else "fail-at-end"

    // Load tables to process
    val tablesCsv: String = tablesOpt.getOrElse(cfg.getString("app.tables"))
    val tablesToProcess: Seq[String] = tablesCsv.split(",").map(_.trim).filter(_.nonEmpty).toSeq

    // Track errors for fail-at-end strategy
    val errors = ListBuffer[String]()

    tablesToProcess.foreach { table =>
      // 1. Structured Logging: Add 'table' to MDC context for this block
      Observability.withMdc(Map("table" -> table)) {

        logger.info("Starting ingestion...")

        val processResult = Try {
          // 2. Resilience: Wrap I/O in Retry Logic & Circuit Breaker
          // If Mongo/File read fails transiently, it will retry 3 times.
          // If it fails persistently, Circuit Breaker will open to prevent cascading failures.
          val raw: DataFrame = Observability.withCircuitBreaker {
            Observability.withRetry {
              if (mongoEnabled) {
                logger.info(s"Reading from MongoDB: ${mongoDatabase}.$table")
                IOUtils.readFromMongoDG(
                  spark,
                  mongoUri = mongoUri,
                  database = mongoDatabase,
                  collection = table,
                  schema = Some(rawSchemas.getSchema(table))
                ).get
              } else {
                val inputPath: String = s"$srcDir/yelp_academic_dataset_${table}.json"
                logger.info(s"Reading from filesystem: $inputPath")
                IOUtils.readJsonFromFileSystem(
                  spark,
                  inputPath,
                  schema = Some(rawSchemas.getSchema(table))
                ).get
              }
            }
          }

          // Transformation: Add ingestion metadata columns
          val withMeta: DataFrame = raw
            .withColumn(transformColumns.ingestTs, current_timestamp())
            .withColumn(transformColumns.ingestDate, to_date(col(transformColumns.ingestTs)))

          // Write to Delta Lake bronze layer
          val outputPath: String = s"$bronzeDir/$table"
          logger.info(s"Writing to Delta : $outputPath (mode=$writeMode)")

          // Retry on write as well (e.g. S3/HDFS temporary unavailability)
          Observability.withCircuitBreaker {
            Observability.withRetry {
              IOUtils.writeDelta(
                df = withMeta,
                outputPath = outputPath,
                mode = writeMode,
                partitionBy = Seq(transformColumns.ingestDate),
                options = Map("maxRecordsPerFile" -> maxRecordsPerFile)
              )
            }
          }
        }

        // Handle Result based on Strategy
        processResult match {
          case Success(_) =>
            logger.info("Ingestion Success")
            // 3. Observability: Track success metric
            Observability.trackMetric("bronze_ingest_success", 1, Map("table" -> table))

          case Failure(ex) =>
            val errorMsg = s"Failed: ${ex.getMessage}"
            logger.error(errorMsg, ex)
            Observability.trackMetric("bronze_ingest_failure", 1, Map("table" -> table))

            failureStrategy match {
              case "fail-fast" =>
                throw ex // Rethrow to stop the entire job immediately
              case "skip-and-warn" =>
                logger.warn("Skipping due to error, continuing with next table.")
              case _ => errors += errorMsg // fail-at-end
            }
        }
      }
    }
    // If using fail-at-end, throw exception if any errors occurred
    if (failureStrategy == "fail-at-end" && errors.nonEmpty) {
      throw new RuntimeException(s"Bronze Ingest completed with errors:\n${errors.mkString("\n")}")
    }

    logger.info("[BronzeIngest] All done.")
  }
}