package com.yelpbatch.utils

import java.util.Properties
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import scala.util.Try

object IOUtils {
  // Logger
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /** Write DataFrame as Delta with optional partitioning and writer options
   *
   * @param df          : DataFrame to write
   * @param outputPath  : target Delta path
   * @param mode        : write mode (default "append")
   * @param partitionBy : optional partition columns
   * @param options     : additional writer options as key-value map
   * */
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

  /**
   * Idempotent upsert (merge) into Delta by key columns. Creates target if missing.
   *
   * @param spark            : SparkSession
   * @param df               : incoming DataFrame to upsert
   * @param outputPath       : target Delta path
   * @param keyCols          : columns to use as keys for matching
   * @param partitionColumns : optional partition columns for new Delta table
   * */
  def upsertDeltaByKey(
                        spark: SparkSession,
                        df: DataFrame,
                        outputPath: String,
                        keyCols: Seq[String],
                        partitionColumns: Seq[String] = Seq.empty
                      ): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fsPath = new org.apache.hadoop.fs.Path(outputPath)
    val hfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    // validate key columns exist in the incoming DataFrame
    val missingKeys = keyCols.filterNot(df.columns.contains)
    if (missingKeys.nonEmpty) {
      throw new IllegalArgumentException(s"Missing key columns in DataFrame: ${missingKeys.mkString(",")}")
    }

    // only use partition columns that actually exist in the DataFrame
    val actualPartitionCols = partitionColumns.filter(df.columns.contains)
    if (partitionColumns.nonEmpty && actualPartitionCols.isEmpty) {
      // warn but continue: writing without partitioning is safer than failing
      org.slf4j.LoggerFactory.getLogger(getClass).warn(
        s"Requested partition columns ${partitionColumns.mkString(",")} not present in DataFrame; writing without partitioning"
      )
    }

    // decide whether to include overwriteSchema based on session partition overwrite mode
    val partitionOverwriteMode = spark.conf.getOption("spark.sql.sources.partitionOverwriteMode").getOrElse("static")
    val includeOverwriteSchema = partitionOverwriteMode.toLowerCase != "dynamic"

    if (hfs.exists(fsPath)) {
      // try to perform merge upsert into existing delta table
      try {
        val deltaTable = io.delta.tables.DeltaTable.forPath(spark, outputPath)
        val joinCondition = keyCols.map(k => s"target.$k = source.$k").mkString(" AND ")
        deltaTable.as("target")
          .merge(df.as("source"), joinCondition)
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()
        org.slf4j.LoggerFactory.getLogger(getClass).info(s"Upsert complete into existing Delta at $outputPath")
      } catch {
        case ex: Exception =>
          // if it's not a Delta table or merge failed, fall back to overwriting (safe recovery)
          logger.warn(s"Could not open Delta at $outputPath for merge: ${ex.getMessage}. Falling back to overwrite create.")

          var writer = df.write.format("delta").mode("overwrite")
          if (includeOverwriteSchema) writer = writer.option("overwriteSchema", "true")
          if (actualPartitionCols.nonEmpty) writer = writer.partitionBy(actualPartitionCols: _*)
          writer.save(outputPath)

          logger.info(s"Wrote DataFrame to $outputPath with mode=overwrite (fallback)")
      }
    } else {
      // create initial delta table (target missing)

      logger.info(s"Target path $outputPath does not exist. Creating initial Delta table.")
      // ensure parent directory exists
      Option(fsPath.getParent).foreach(p => if (!hfs.exists(p)) hfs.mkdirs(p))

      var writer = df.write.format("delta").mode("overwrite")
      if (includeOverwriteSchema) writer = writer.option("overwriteSchema", "true")
      if (actualPartitionCols.nonEmpty) writer = writer.partitionBy(actualPartitionCols: _*)
      writer.save(outputPath)

      logger.info(s"Created new Delta at $outputPath partitionedBy=${actualPartitionCols.mkString(",")}")
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

  /**
   * Read from MongoDB with error handling
   *
   * @param spark       : SparkSession
   * @param mongoUri    : MongoDB connection URI
   * @param database    : database name
   * @param collection  : collection name
   * @param schema      : optional StructType schema
   * @param readOptions : additional read options as key-value map
   * @return Try[DataFrame] - use .get, .getOrElse(), or pattern matching
   */
  def readFromMongoDG(
                       spark: SparkSession,
                       mongoUri: String,
                       database: String,
                       collection: String,
                       schema: Option[StructType] = None,
                       readOptions: Map[String, String] = Map.empty
                     ): Try[DataFrame] = {

    Try {
      // Validate inputs
      require(mongoUri != null && mongoUri.trim.nonEmpty, "MongoDB URI cannot be empty")
      require(database != null && database.trim.nonEmpty, "Database name cannot be empty")
      require(collection != null && collection.trim.nonEmpty, "Collection name cannot be empty")
      require(spark != null, "SparkSession cannot be null")

      logger.info(s"Configuring MongoDB Reader: URI=$mongoUri, DB=$database, Coll=$collection")

      // Build reader
      var reader = spark.read
        .format("mongodb")
        .option("spark.mongodb.read.connection.uri", mongoUri)
        .option("spark.mongodb.read.database", database)
        .option("spark.mongodb.read.collection", collection)

      // Apply options and schema
      readOptions.foreach { case (k, v) => reader = reader.option(k, v) }
      schema.foreach(s => reader = reader.schema(s))

      reader.load()
    }
  }

  /**
   * Read JSON from filesystem with optional schema
   *
   * @param spark  : SparkSession
   * @param path   : file path or directory
   * @param schema : optional StructType schema
   * @return Try[DataFrame] - use .get, .getOrElse(), or pattern matching
   */
  def readJsonFromFileSystem(
                              spark: SparkSession,
                              path: String,
                              schema: Option[org.apache.spark.sql.types.StructType] = None
                            ): Try[DataFrame] = {

    Try {
      // Validate inputs
      require(spark != null, "SparkSession cannot be null")
      require(path != null && path.trim.nonEmpty, "Path cannot be empty")

      // Build reader
      var reader = spark.read

      // If schema is provided, use it; otherwise infer
      schema match {
        case Some(s) => reader.schema(s).json(path)
        case None => reader.json(path)
      }
    }
  }

  /**
   * Read Delta table with optional column selection and filtering
   *
   * @param spark      : SparkSession
   * @param path       : Delta table path
   * @param columns    : columns to select (if empty, select all)
   * @param conditions : optional filter conditions (SQL expressions)
   * @return DataFrame with selected columns and applied filters
   */
  def readDelta(
                 spark: SparkSession,
                 path: String,
                 columns: Seq[String],
                 conditions: Seq[Column] = Seq.empty
               ): DataFrame = {
    val reader = spark.read.format("delta")
    val df: DataFrame = reader.load(path)

    // apply filters if any
    val filtered: DataFrame = if (conditions.nonEmpty) df.filter(conditions.reduce(_ && _)) else df

    // select requested columns (if none provided, return all)
    if (columns.nonEmpty) filtered.select(columns.map(col): _*) else filtered
  }
}